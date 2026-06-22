"""
KOPIS (공연예술통합전산망) 전체 데이터 수집기
API KEY: 880584ecb36e4fec940c44b18d309d94

수집 항목:
  [DB 검색] 공연목록, 공연상세, 공연시설목록, 공연시설상세,
            기획/제작사 목록, 수상작 목록, 축제 목록, 원·창작자 목록
  [예매통계] 예매상황판, 기간별/장르별/시간대별/가격대별 통계
  [공연통계] 기간별/지역별/장르별/공연별/시설별/가격대별 통계
"""

import requests
import xml.etree.ElementTree as ET
import csv
import os
import time
from datetime import datetime, timedelta

# ── 설정 ──────────────────────────────────────────────────────────────────────
API_KEY    = "880584ecb36e4fec940c44b18d309d94"
BASE_URL   = "http://www.kopis.or.kr/openApi/restful"
OUTPUT_DIR = r"C:\Users\82106\Desktop\Claude\kopis_data"

# 통계 조회 기간 (최근 3년)
STAT_END   = datetime.today().strftime("%Y%m%d")
STAT_START = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y%m%d")

# 공연목록 조회 기간
PRF_START  = "20100101"
PRF_END    = datetime.today().strftime("%Y%m%d")

ROWS_PER_PAGE = 100
SLEEP_SEC     = 0.3   # API 과부하 방지

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 공통 유틸 ─────────────────────────────────────────────────────────────────
def fetch_xml(url: str, params: dict):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return ET.fromstring(r.content)
        except Exception as e:
            print(f"  [재시도 {attempt+1}/3] {e}")
            time.sleep(2)
    return None


def elem_to_dict(elem) -> dict:
    return {child.tag: (child.text or "").strip() for child in elem}


def save_csv(rows: list, filename: str):
    if not rows:
        print(f"  → 데이터 없음: {filename}")
        return
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  → {len(rows):,}행 저장: {path}")


def fetch_pages(endpoint: str, base_params: dict, item_tag: str = "db") -> list:
    """페이지네이션을 자동 처리하여 전체 데이터 반환"""
    all_rows, page = [], 1
    while True:
        params = {**base_params, "cpage": page, "rows": ROWS_PER_PAGE}
        root = fetch_xml(f"{BASE_URL}/{endpoint}", params)
        if root is None:
            break
        items = root.findall(item_tag)
        if not items:
            break
        all_rows.extend(elem_to_dict(it) for it in items)
        print(f"    페이지 {page}: {len(items)}건 (누적 {len(all_rows):,}건)")
        if len(items) < ROWS_PER_PAGE:
            break
        page += 1
        time.sleep(SLEEP_SEC)
    return all_rows


# ══════════════════════════════════════════════════════════════════════════════
# 1. DB 검색
# ══════════════════════════════════════════════════════════════════════════════

def collect_performances():
    """공연목록 전체 수집"""
    print("\n[1/8] 공연목록 수집 중...")
    params = {"service": API_KEY, "stdate": PRF_START, "eddate": PRF_END}
    rows = fetch_pages("pblprfr", params)
    save_csv(rows, "01_공연목록.csv")
    return [r["mt20id"] for r in rows if r.get("mt20id")]


def collect_performance_detail(mt20ids: list):
    """공연상세 수집 (공연목록에서 얻은 ID 사용)"""
    print(f"\n[2/8] 공연상세 수집 중 (총 {len(mt20ids):,}건)...")
    all_rows = []
    for i, mid in enumerate(mt20ids, 1):
        root = fetch_xml(f"{BASE_URL}/pblprfr/{mid}", {"service": API_KEY})
        if root is not None:
            items = root.findall("db")
            for it in items:
                row = elem_to_dict(it)
                # 중첩 styurls / relates 태그 평탄화
                styurls = it.find("styurls")
                if styurls is not None:
                    row["styurls"] = "|".join(
                        (u.text or "").strip() for u in styurls.findall("styurl")
                    )
                relates = it.find("relates")
                if relates is not None:
                    parts = []
                    for rel in relates.findall("relate"):
                        relatenm = (rel.findtext("relatenm") or "").strip()
                        relateurl = (rel.findtext("relateurl") or "").strip()
                        parts.append(f"{relatenm}:{relateurl}")
                    row["relates"] = "|".join(parts)
                all_rows.append(row)
        if i % 100 == 0:
            print(f"    {i:,}/{len(mt20ids):,} 완료")
        time.sleep(SLEEP_SEC)
    save_csv(all_rows, "02_공연상세.csv")


def collect_venues():
    """공연시설목록 수집"""
    print("\n[3/8] 공연시설목록 수집 중...")
    params = {"service": API_KEY}
    rows = fetch_pages("prfplc", params)
    save_csv(rows, "03_공연시설목록.csv")
    return [r["mt10id"] for r in rows if r.get("mt10id")]


def collect_venue_detail(mt10ids: list):
    """공연시설상세 수집"""
    print(f"\n[4/8] 공연시설상세 수집 중 (총 {len(mt10ids):,}건)...")
    all_rows = []
    for i, vid in enumerate(mt10ids, 1):
        root = fetch_xml(f"{BASE_URL}/prfplc/{vid}", {"service": API_KEY})
        if root is not None:
            all_rows.extend(elem_to_dict(it) for it in root.findall("db"))
        if i % 200 == 0:
            print(f"    {i:,}/{len(mt10ids):,} 완료")
        time.sleep(SLEEP_SEC)
    save_csv(all_rows, "04_공연시설상세.csv")


def collect_producers():
    """기획/제작사 목록"""
    print("\n[5/8] 기획/제작사 목록 수집 중...")
    params = {"service": API_KEY}
    rows = fetch_pages("mnfct", params)
    save_csv(rows, "05_기획제작사목록.csv")


def collect_awards():
    """수상작 목록"""
    print("\n[6/8] 수상작 목록 수집 중...")
    params = {"service": API_KEY}
    rows = fetch_pages("award", params)
    save_csv(rows, "06_수상작목록.csv")


def collect_festivals():
    """축제 목록"""
    print("\n[7/8] 축제 목록 수집 중...")
    params = {"service": API_KEY, "stdate": PRF_START, "eddate": PRF_END}
    rows = fetch_pages("festival", params)
    save_csv(rows, "07_축제목록.csv")


def collect_creators():
    """원·창작자 목록"""
    print("\n[8/8] 원·창작자 목록 수집 중...")
    params = {"service": API_KEY}
    rows = fetch_pages("prfstf", params)
    save_csv(rows, "08_원창작자목록.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 2. 예매통계
# ══════════════════════════════════════════════════════════════════════════════

def _boxoffice_rows(ststype: str, extra: dict = None) -> list:
    """
    예매상황판/통계 공통 수집
    ststype: D(일별), W(주별), M(월별), Y(연별)
    date: 기준일 (YYYYMMDD)
    """
    rows, params_base = [], {"service": API_KEY, "ststype": ststype}
    if extra:
        params_base.update(extra)

    # 월별로 순환하며 수집
    cur = datetime.strptime(STAT_START[:6] + "01", "%Y%m%d")
    end = datetime.strptime(STAT_END[:6] + "01", "%Y%m%d")
    while cur <= end:
        date_str = cur.strftime("%Y%m%d")
        root = fetch_xml(f"{BASE_URL}/boxoffice", {**params_base, "date": date_str})
        if root is not None:
            items = root.findall("db")
            for it in items:
                row = elem_to_dict(it)
                row["_query_date"] = date_str
                rows.append(row)
        # 다음 달
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
        time.sleep(SLEEP_SEC)
    return rows


def collect_booking_stats():
    print("\n[예매통계 1/5] 예매상황판 수집 중...")
    rows = _boxoffice_rows("D")
    save_csv(rows, "09_예매상황판_일별.csv")

    print("\n[예매통계 2/5] 기간별 통계 수집 중...")
    rows = _boxoffice_rows("W")
    save_csv(rows, "10_예매통계_기간별(주별).csv")

    print("\n[예매통계 3/5] 장르별 통계 수집 중...")
    rows = _boxoffice_rows("M")
    save_csv(rows, "11_예매통계_월별.csv")

    # 장르코드별 수집
    genre_codes = {
        "AAAA": "연극", "BBBC": "뮤지컬", "BBBD": "오페라",
        "BBBE": "클래식", "BBBF": "무용", "BBGG": "국악",
        "CCCA": "서양음악(클래식)", "EEEA": "서커스/마술", "GGGA": "복합"
    }
    genre_rows = []
    for code, name in genre_codes.items():
        r = _boxoffice_rows("M", {"catecode": code})
        for row in r:
            row["genre_code"] = code
            row["genre_name"] = name
        genre_rows.extend(r)
    save_csv(genre_rows, "12_예매통계_장르별.csv")

    print("\n[예매통계 4/5] 시간대별 통계 — 별도 API 미제공(상황판 내 포함)")
    print("\n[예매통계 5/5] 가격대별 통계 — 별도 API 미제공(상황판 내 포함)")


# ══════════════════════════════════════════════════════════════════════════════
# 3. 공연통계
# ══════════════════════════════════════════════════════════════════════════════

def _stat_pages(endpoint: str, extra: dict = None) -> list:
    """공연통계 공통 수집 (연도별 분할)"""
    rows = []
    # 연도별로 나눠 수집
    start_year = int(STAT_START[:4])
    end_year   = int(STAT_END[:4])
    for year in range(start_year, end_year + 1):
        stdate = f"{year}0101"
        eddate = f"{year}1231"
        params = {"service": API_KEY, "stdate": stdate, "eddate": eddate}
        if extra:
            params.update(extra)
        yr_rows = fetch_pages(endpoint, params)
        for r in yr_rows:
            r["_year"] = str(year)
        rows.extend(yr_rows)
        print(f"    {year}년: {len(yr_rows):,}건")
    return rows


def collect_performance_stats():
    print("\n[공연통계 1/6] 기간별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsqrst"), "13_공연통계_기간별.csv")

    print("\n[공연통계 2/6] 지역별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsarea"), "14_공연통계_지역별.csv")

    print("\n[공연통계 3/6] 장르별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsgnre"), "15_공연통계_장르별.csv")

    print("\n[공연통계 4/6] 공연별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsperformance"), "16_공연통계_공연별.csv")

    print("\n[공연통계 5/6] 공연 시설별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsvenue"), "17_공연통계_시설별.csv")

    print("\n[공연통계 6/6] 가격대별 통계 수집 중...")
    save_csv(_stat_pages("prfsttsticket"), "18_공연통계_가격대별.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("KOPIS 전체 데이터 수집 시작")
    print(f"  출력 폴더: {OUTPUT_DIR}")
    print(f"  통계 조회 기간: {STAT_START} ~ {STAT_END}")
    print("=" * 60)

    # ── DB 검색 ────────────────────────────────────────────────
    mt20ids = collect_performances()          # 01 공연목록
    collect_performance_detail(mt20ids)      # 02 공연상세
    mt10ids = collect_venues()               # 03 공연시설목록
    collect_venue_detail(mt10ids)            # 04 공연시설상세
    collect_producers()                      # 05 기획/제작사
    collect_awards()                         # 06 수상작
    collect_festivals()                      # 07 축제
    collect_creators()                       # 08 원·창작자

    # ── 예매통계 ────────────────────────────────────────────────
    collect_booking_stats()                  # 09~12

    # ── 공연통계 ────────────────────────────────────────────────
    collect_performance_stats()              # 13~18

    print("\n" + "=" * 60)
    print("수집 완료! 저장 위치:", OUTPUT_DIR)
    print("=" * 60)


if __name__ == "__main__":
    main()
