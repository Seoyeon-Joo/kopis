"""
Phase 1 (병렬): 통계 수집
Usage: python scripts/fetch_stats.py <type>
Types: booking | performance
"""
import sys, os, csv, time, requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

API_KEY  = os.environ["KOPIS_API_KEY"]
BASE_URL = "http://www.kopis.or.kr/openApi/restful"
OUT_DIR  = "output"

TODAY      = datetime.today()
STAT_START = (TODAY - timedelta(days=365 * 3)).replace(day=1)

GENRE_CODES = {
    "AAAA": "연극",     "BBBC": "뮤지컬",  "BBBD": "오페라",
    "BBBE": "클래식",   "BBBF": "무용",    "BBGG": "국악",
    "EEEA": "서커스/마술",
}

os.makedirs(OUT_DIR, exist_ok=True)


def fetch_xml(url, params):
    for attempt in range(3):
        try:
            r = requests.get(url, params={**params, "service": API_KEY}, timeout=30)
            r.raise_for_status()
            return ET.fromstring(r.content)
        except Exception as e:
            print(f"  [retry {attempt+1}] {e}", flush=True)
            time.sleep(2)
    return None


def to_rows(root):
    return [{c.tag: (c.text or "").strip() for c in it} for it in root.findall("db")]


def fetch_pages(endpoint, base_params):
    rows, page = [], 1
    while True:
        root = fetch_xml(f"{BASE_URL}/{endpoint}", {**base_params, "cpage": page, "rows": 100})
        if root is None:
            break
        items = to_rows(root)
        if not items:
            break
        rows.extend(items)
        if len(items) < 100:
            break
        page += 1
        time.sleep(0.2)
    return rows


def save_csv(rows, filename):
    if not rows:
        print(f"  데이터 없음: {filename}", flush=True)
        return
    path = os.path.join(OUT_DIR, filename)
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  저장: {path} ({len(rows):,}행)", flush=True)


def month_iter(start, end):
    """start~end 월의 첫째날을 순서대로 yield"""
    cur = start.replace(day=1)
    while cur <= end:
        yield cur
        cur = cur.replace(month=cur.month % 12 + 1, year=cur.year + (1 if cur.month == 12 else 0))


# ── 예매통계 ─────────────────────────────────────────────────────────────────

def collect_booking():
    print("=== 예매통계 수집 시작 ===", flush=True)
    rows_daily, rows_weekly, rows_monthly, rows_genre = [], [], [], []

    for cur in month_iter(STAT_START, TODAY):
        date_str = cur.strftime("%Y%m%d")

        for ststype, bucket in [("day", rows_daily), ("week", rows_weekly), ("month", rows_monthly)]:
            root = fetch_xml(f"{BASE_URL}/boxoffice", {"ststype": ststype, "date": date_str})
            if root:
                for r in to_rows(root):
                    r["_query_date"] = date_str
                    bucket.append(r)

        for code, name in GENRE_CODES.items():
            root = fetch_xml(
                f"{BASE_URL}/boxoffice",
                {"ststype": "month", "date": date_str, "catecode": code},
            )
            if root:
                for r in to_rows(root):
                    r.update({"_query_date": date_str, "genre_code": code, "genre_name": name})
                    rows_genre.append(r)

        print(f"  {date_str} 완료", flush=True)
        time.sleep(0.3)

    save_csv(rows_daily,   "09_예매상황판_일별.csv")
    save_csv(rows_weekly,  "10_예매통계_기간별.csv")
    save_csv(rows_monthly, "11_예매통계_월별.csv")
    save_csv(rows_genre,   "12_예매통계_장르별.csv")


# ── 공연통계 ─────────────────────────────────────────────────────────────────

PERF_STAT_ENDPOINTS = [
    ("prfsttsqrst",        "13_공연통계_기간별.csv"),
    ("prfsttsarea",        "14_공연통계_지역별.csv"),
    ("prfsttsgnre",        "15_공연통계_장르별.csv"),
    ("prfsttsperformance", "16_공연통계_공연별.csv"),
    ("prfsttsvenue",       "17_공연통계_시설별.csv"),
    ("prfsttsticket",      "18_공연통계_가격대별.csv"),
]


def collect_performance():
    print("=== 공연통계 수집 시작 ===", flush=True)
    start_year = STAT_START.year
    end_year   = TODAY.year

    for endpoint, outfile in PERF_STAT_ENDPOINTS:
        all_rows = []
        for year in range(start_year, end_year + 1):
            rows = fetch_pages(
                endpoint,
                {"stdate": f"{year}0101", "eddate": f"{year}1231"},
            )
            for r in rows:
                r["_year"] = str(year)
            all_rows.extend(rows)
            print(f"  {endpoint} {year}년: {len(rows):,}행", flush=True)
        save_csv(all_rows, outfile)


def main():
    kind = sys.argv[1]
    {"booking": collect_booking, "performance": collect_performance}[kind]()


if __name__ == "__main__":
    main()
