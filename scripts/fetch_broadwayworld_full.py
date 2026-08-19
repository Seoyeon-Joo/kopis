"""
fetch_broadwayworld_full.py
=============================
원본 grosses.csv(주간 흥행 데이터, 컬럼: show/theatre/...)의 고유 쇼 이름을 기준으로,
BroadwayWorld에서 아래 정보를 긁어온다.

  - 개막일(opening_date) / 폐막일(closing_date) / 첫 프리뷰(first_preview)
  - 장르(genre): Musical/Play - 현재 상연작은 grosses.php 목록에서, 종영작은
    쇼 개별 페이지의 <title> 태그에 "Musical"이 들어있는지로 보조 판별
  - 캐스트(cast): 배우 이름 + 배역
  - 창작진(creative_team): 연출/안무/무대디자인 등 Production Team 크레딧

*** 중요 ***
이 스크립트는 raw HTML을 실제로 본 적은 있지만(WICKED 1개 쇼), 나머지 1,100여 개
쇼가 전부 동일한 HTML 구조라는 보장은 없어요. 캐스트/창작진 파싱은 <a href="/people/...">
링크 패턴에 기반한 휴리스틱이라, 처음 몇 개 쇼 결과를 사람이 직접 확인해서 이상한 값이
섞이는지 체크하는 걸 강력히 권장해요. (원작자 주석: 재공연이 많은 쇼는 캐스트가 시즌별로
누적되어 나올 수 있어 원본 오리지널 캐스트만 보고 싶다면 이후 결과에서 필터링이 필요할 수 있음)

Usage:
  python fetch_broadwayworld_full.py --raw broadway_targets.csv --out-dir data --sleep 1.0
  python fetch_broadwayworld_full.py --shows "Wicked" "Hamilton" --out-dir .  # 소수 테스트용
"""
import argparse
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

PERSON_RE = re.compile(r"^/people/(?!character/)[^/]+/?$")
CHARACTER_RE = re.compile(r"^/people/character/([^/]+)-(\d+)/?$")
SHOWID_RE = re.compile(r"showid=(\d+)")

ROLE_KEYWORDS = [
    "Director", "Choreographer", "Music Director", "Musical Director", "Orchestrator",
    "Composer", "Lyricist", "Book", "Producer", "Scenic Designer", "Set Designer",
    "Costume Designer", "Lighting Designer", "Sound Designer", "Projection Designer",
    "Hair", "Wig Designer", "Make-Up Designer", "Special Effects Designer",
    "Flying Effects", "Musical Staging", "Casting", "General Manager",
]


def slugify(title):
    s = title.upper().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


def find_show_slug(title, session, grosses_html_cache):
    if grosses_html_cache["soup"] is None:
        resp = session.get(f"{BASE}/grosses.php", headers=HEADERS, timeout=20)
        grosses_html_cache["soup"] = BeautifulSoup(resp.text, "html.parser")
    soup = grosses_html_cache["soup"]
    target_norm = re.sub(r"[^A-Z0-9]", "", title.upper())
    for a in soup.select("a[href^='/grosses/']"):
        if re.sub(r"[^A-Z0-9]", "", a.get_text(strip=True).upper()) == target_norm:
            return a["href"].split("/grosses/")[-1]
    return None


def fetch_genre_map(session):
    """현재 상연작만 잡힘 - 종영작은 별도 휴리스틱(<title> 태그) 필요"""
    resp = session.get(f"{BASE}/grosses.php", headers=HEADERS, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    genre_map = {}
    for a in soup.select("a[href^='/grosses/']"):
        t = a.get("title", "")
        if t in ("Musical", "Play"):
            genre_map[a["href"].split("/grosses/")[-1]] = t
    return genre_map


def fetch_show_page(slug, session):
    """개별 쇼 그로스 페이지 -> 날짜/showid 추출"""
    resp = session.get(f"{BASE}/grosses/{slug}", headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        return None, {}
    soup = BeautifulSoup(resp.text, "html.parser")

    showid = None
    for a in soup.find_all("a", href=True):
        m = SHOWID_RE.search(a["href"])
        if m:
            showid = m.group(1)
            break
        m2 = re.search(r"/shows/[\w-]+-(\d+)(?:/|\.html)", a["href"])
        if m2:
            showid = m2.group(1)
            break

    text = soup.get_text("\n", strip=True)
    meta = {}
    for label, key in [("First Preview", "first_preview"), ("Opening Date", "opening_date"), ("Closing Date", "closing_date")]:
        m = re.search(rf"{label}\s*\n?\s*([\d/]+|Currently Running)", text)
        if m:
            meta[key] = m.group(1)

    return showid, meta


def genre_from_title_tag(soup):
    title_tag = soup.find("title")
    if not title_tag:
        return ""
    t = title_tag.get_text()
    if re.search(r"\bMusical\b", t, re.IGNORECASE):
        return "Musical"
    if re.search(r"\bPlay\b", t, re.IGNORECASE):
        return "Play"
    return ""


def fetch_cast_and_creative(showid, session):
    """cast.php?showid=X 페이지에서 캐스트 + Production Team(창작진) 추출.
    휴리스틱: <a href="/people/이름/"> 다음에 <a href="/people/character/역할-showid/">가
    오면 캐스트, ROLE_KEYWORDS에 해당하는 텍스트가 오면 창작진으로 분류."""
    if not showid:
        return [], [], ""

    resp = session.get(f"{BASE}/shows/cast.php", headers=HEADERS, params={"showid": showid}, timeout=20)
    if resp.status_code != 200:
        return [], [], ""
    soup = BeautifulSoup(resp.text, "html.parser")
    genre_guess = genre_from_title_tag(soup)

    cast_entries = []   # (actor, [roles])
    creative_entries = []  # (name, role)

    all_tags = soup.find_all(["a", "strong", "b"])
    current_person = None
    current_roles = []

    def flush_person():
        if current_person and current_roles:
            cast_entries.append((current_person, list(current_roles)))

    for tag in all_tags:
        if tag.name == "a" and tag.get("href"):
            href = tag["href"]
            if PERSON_RE.match(href):
                flush_person()
                current_person = tag.get_text(strip=True)
                current_roles = []
            elif CHARACTER_RE.match(href):
                role = CHARACTER_RE.match(href).group(1).replace("-", " ")
                if current_person:
                    current_roles.append(role)
        elif tag.name in ("strong", "b"):
            role_text = tag.get_text(strip=True)
            if any(kw.lower() in role_text.lower() for kw in ROLE_KEYWORDS) and current_person:
                creative_entries.append((current_person, role_text))
                current_person = None  # 창작진은 role 1개당 1줄이라 바로 리셋

    flush_person()

    return cast_entries, creative_entries, genre_guess


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", help="원본 grosses.csv 경로 (show 컬럼에서 고유 쇼 이름 추출)")
    ap.add_argument("--shows", nargs="*", help="직접 지정할 쇼 이름 목록 (테스트용)")
    ap.add_argument("--out-dir", default="./data")
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--limit", type=int, default=None, help="테스트용 처리 개수 제한")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    session = requests.Session()

    if args.shows:
        titles = args.shows
    elif args.raw:
        df = pd.read_csv(args.raw, sep=None, engine="python", encoding="utf-8-sig")
        df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
        if "show" not in df.columns:
            print(f"'show' 컬럼을 못 찾았어요. 실제 컬럼: {list(df.columns)}")
            raise SystemExit(1)
        titles = df["show"].drop_duplicates().tolist()
    else:
        ap.error("--raw 또는 --shows 중 하나는 필요해요")

    if args.limit:
        titles = titles[: args.limit]

    print(f"총 {len(titles)}개 쇼 처리 예정")
    genre_map = fetch_genre_map(session)
    grosses_cache = {"soup": None}
    time.sleep(args.sleep)

    rows = []
    for i, title in enumerate(titles, 1):
        slug = slugify(title)
        test = session.head(f"{BASE}/grosses/{slug}", headers=HEADERS, timeout=15)
        if test.status_code != 200:
            found = find_show_slug(title, session, grosses_cache)
            if found:
                slug = found
            else:
                print(f"[{i}/{len(titles)}] '{title}' -> 슬러그 못 찾음, 스킵")
                continue

        showid, meta = fetch_show_page(slug, session)
        time.sleep(args.sleep)

        cast_entries, creative_entries, genre_guess = fetch_cast_and_creative(showid, session)
        time.sleep(args.sleep)

        genre = genre_map.get(slug, "") or genre_guess

        cast_str = "; ".join(f"{name} as {', '.join(roles)}" for name, roles in cast_entries)
        creative_str = "; ".join(f"{name} ({role})" for name, role in creative_entries)

        row = {
            "title": title,
            "slug": slug,
            "showid": showid,
            "genre": genre,
            "first_preview": meta.get("first_preview", ""),
            "opening_date": meta.get("opening_date", ""),
            "closing_date": meta.get("closing_date", ""),
            "cast": cast_str,
            "creative_team": creative_str,
        }
        rows.append(row)
        print(f"[{i}/{len(titles)}] '{title}' -> slug={slug}, showid={showid}, "
              f"genre={genre}, cast={len(cast_entries)}명, creative={len(creative_entries)}명")

    if rows:
        out_df = pd.DataFrame(rows)
        out_path = os.path.join(args.out_dir, "broadwayworld_full.csv")
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n저장 완료: {out_path} ({len(out_df)}행)")
    else:
        print("\n수집된 데이터가 없어요.")


if __name__ == "__main__":
    main()
