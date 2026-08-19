"""
fetch_broadwayworld_history.py
================================
BroadwayWorld가 공식으로 제공하는 "Export Complete Show History to Excel"
기능(grossesshowexcel.php)을 이용해 쇼별 전체 주간 이력을 받는다.
이건 스크래핑이 아니라 사이트가 명시적으로 제공하는 다운로드 버튼과
동일한 엔드포인트를 호출하는 것 - League 페이지의 "복사·재배포 금지"
문구와 달리 BroadwayWorld는 이 기능 자체를 사용자에게 노출해놓았음.

또한 쇼 개별 페이지(grosses/{slug})에서 First Preview / Opening Date /
Closing Date를 긁어와서 KOPIS의 perf_start_date/perf_end_date에 대응하는
정확한 값을 얻는다. 장르(Musical/Play)는 주간 그로스 리스트 페이지
(grosses.php)에서 각 쇼 링크의 title 속성으로 얻는다 - 이 스크립트는
장르 매칭까지 한 번에 처리한다.

Usage:
  python fetch_broadwayworld_history.py --slugs WICKED HAMILTON --out-dir ./data
  # 또는 broadway_targets.csv의 title을 슬러그로 자동 변환해서 일괄 처리
  python fetch_broadwayworld_history.py --targets broadway_targets.csv --out-dir ./data
"""
import argparse
import io
import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.broadwayworld.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}


def slugify(title):
    """'The Book of Mormon' -> 'THE-BOOK-OF-MORMON' 형태로 변환 시도.
    실제 슬러그가 이 규칙과 다른 쇼들이 있을 수 있어서(예: 특수문자 처리),
    안 맞으면 show_search()로 재시도한다."""
    s = title.upper().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


def find_show_slug(title, session):
    """grosses.php 검색으로 실제 슬러그를 찾는다 (slugify 추정이 틀렸을 때 대비)."""
    resp = session.get(f"{BASE}/grosses.php", headers=HEADERS, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    target_norm = re.sub(r"[^A-Z0-9]", "", title.upper())
    for a in soup.select("a[href^='/grosses/']"):
        if re.sub(r"[^A-Z0-9]", "", a.get_text(strip=True).upper()) == target_norm:
            return a["href"].split("/grosses/")[-1]
    return None


def fetch_show_meta(slug, session):
    """개별 쇼 페이지에서 First Preview / Opening Date / Closing Date 추출."""
    resp = session.get(f"{BASE}/grosses/{slug}", headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    meta = {}
    for label, key in [("First Preview", "first_preview"), ("Opening Date", "opening_date"), ("Closing Date", "closing_date")]:
        m = re.search(rf"{label}\s*\n?\s*([\d/]+|Currently Running)", text)
        if m:
            meta[key] = m.group(1)
    return meta


def fetch_show_excel(slug, session):
    """공식 엑셀 export 엔드포인트 호출. xlsx 바이너리를 pandas로 바로 파싱."""
    url = f"{BASE}/grossesshowexcel.php"
    params = {"show": slug, "all": "on", "year": "0"}
    resp = session.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    try:
        df = pd.read_excel(io.BytesIO(resp.content))
        df["show_slug"] = slug
        return df
    except Exception as e:
        print(f"  [{slug}] 엑셀 파싱 실패: {e} (응답이 xlsx가 아닐 수 있음 - 로그인 필요 여부 확인 필요)")
        return None


def fetch_genre_map(session):
    """grosses.php 최신 주 테이블에서 쇼별 장르(title 속성)를 긁어옴.
    현재 상연 중인 쇼만 잡히므로, 종영작 장르는 각 쇼 개별 페이지에서 보강 필요."""
    resp = session.get(f"{BASE}/grosses.php", headers=HEADERS, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    genre_map = {}
    for a in soup.select("a[href^='/grosses/']"):
        title_attr = a.get("title", "")
        if title_attr in ("Musical", "Play"):
            slug = a["href"].split("/grosses/")[-1]
            genre_map[slug] = title_attr
    return genre_map


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="직접 지정할 쇼 슬러그 목록 (예: WICKED HAMILTON)")
    ap.add_argument("--targets", help="broadway_targets.csv 경로 (title 컬럼에서 자동 변환)")
    ap.add_argument("--out-dir", default="./data")
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument(
        "--meta-only", action="store_true",
        help="장르+개막/폐막일만 받고 주간 엑셀 export는 건너뜀 "
             "(이미 Playbill/tacookson 주간 데이터가 있을 때 - 훨씬 빠름, 쇼당 요청 1~2회)"
    )
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    session = requests.Session()

    titles = []
    if args.slugs:
        titles = args.slugs
    elif args.targets:
        targets = pd.read_csv(args.targets)
        titles = targets["title"].drop_duplicates().tolist()
    else:
        ap.error("--slugs 또는 --targets 중 하나는 필요해요")

    print(f"총 {len(titles)}개 쇼 처리 예정 (모드: {'메타만' if args.meta_only else '메타+주간이력'})")
    print("장르 맵 가져오는 중 (현재 상연작 기준)...")
    genre_map = fetch_genre_map(session)
    time.sleep(args.sleep)

    all_weekly, all_meta = [], []
    for i, title in enumerate(titles, 1):
        slug = slugify(title)
        # slugify 추정이 틀렸으면 검색으로 재시도
        test = session.head(f"{BASE}/grosses/{slug}", headers=HEADERS, timeout=15)
        if test.status_code != 200:
            found = find_show_slug(title, session)
            if found:
                slug = found
            else:
                print(f"[{i}/{len(titles)}] '{title}' -> 슬러그 못 찾음, 스킵")
                continue

        print(f"[{i}/{len(titles)}] '{title}' -> slug={slug}")

        meta = fetch_show_meta(slug, session)
        meta["title"] = title
        meta["slug"] = slug
        meta["genre"] = genre_map.get(slug, "")
        all_meta.append(meta)
        time.sleep(args.sleep)

        if not args.meta_only:
            df = fetch_show_excel(slug, session)
            if df is not None:
                all_weekly.append(df)
            time.sleep(args.sleep)

    if all_meta:
        meta_df = pd.DataFrame(all_meta)
        meta_path = os.path.join(args.out_dir, "broadwayworld_show_meta.csv")
        meta_df.to_csv(meta_path, index=False, encoding="utf-8-sig")
        print(f"\n메타(개막/폐막일/장르) 저장: {meta_path} ({len(meta_df)}개 쇼)")
        missing_genre = meta_df["genre"].eq("").sum()
        if missing_genre:
            print(f"  참고: {missing_genre}개 쇼는 장르를 못 찾음 (종영작이라 현재 상연 리스트에 없을 가능성 - 개별 페이지 보강 필요)")

    if all_weekly:
        weekly_df = pd.concat(all_weekly, ignore_index=True)
        weekly_path = os.path.join(args.out_dir, "broadwayworld_weekly_full.csv")
        weekly_df.to_csv(weekly_path, index=False, encoding="utf-8-sig")
        print(f"주간 이력 저장: {weekly_path} ({len(weekly_df)}행)")
    elif not args.meta_only:
        print("\n수집된 주간 데이터가 없어요 - 엑셀 export 응답 형식을 확인해주세요.")


if __name__ == "__main__":
    main()
