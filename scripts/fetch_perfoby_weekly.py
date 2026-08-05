"""
data/16_공연통계_공연별_주간_전체_with_runtime.csv 를 이어서 채우는 스크립트.

기존 파일의 week_end_date 최대값을 읽어서, 그 다음 날부터 "어제"까지를
7일 단위 주간 구간으로 쪼개 perfoStatsPerfoByList(sql_type="week")를 호출하고,
02_공연상세.csv에서 러닝타임(runtime_min)을 조인해 기존 파일에 이어붙인다.

data1~data12 매핑(2026-06-26 세션에서 검증 완료, fetch_graphql_stats.py의
perfoby 쿼리와 동일한 필드셋을 사용):
  data1=rank, data2=title, data3=genre, data4=perf_start_date,
  data5=perf_end_date, data6=venue_name, data7=num_showings,
  data8=ticket_sales_qty, data9=ticket_sales_amount,
  data11=perf_id, data12=company_id   (data10은 응답에 없음)

Usage:
  python fetch_perfoby_weekly.py
  python fetch_perfoby_weekly.py --weeks-back 8   # 기존 파일이 없을 때 초기 수집 구간
"""
import argparse
import csv
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests

URL = "https://kopis.or.kr:9001/api/prs/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://kopis.or.kr/por/stats/perfo/perfoStatsPerfoBy.do",
    "User-Agent": "Mozilla/5.0",
}

DETAIL_API_BASE = "http://www.kopis.or.kr/openApi/restful/pblprfr"

TARGET_PATH = "data/16_공연통계_공연별_주간_전체_with_runtime.csv"
DETAIL_PATH = "data/02_공연상세.csv"
TARGETS_ENRICHED_PATH = "data/targets_enriched.csv"
NEW_PERF_CANDIDATES_PATH = "output/16_new_perf_candidates.csv"

FIELDNAMES = [
    "rank", "title", "genre", "perf_start_date", "perf_end_date",
    "venue_name", "runtime_min", "num_showings", "ticket_sales_qty",
    "ticket_sales_amount", "perf_id", "company_id",
    "week_start_date", "week_end_date",
]

QUERY = """query GetPerfoStatsPerfoByList($startDate: String!, $endDate: String!, $sql_type: String!, $prfrNm: String, $curPage: Int!, $pageSize: Int!) {
  perfoStatsPerfoByList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type, prfrNm: $prfrNm, curPage: $curPage, pageSize: $pageSize) {
    result { data1 data2 data3 data4 data5 data6 data7 data8 data9 data11 data12 totalCnt __typename }
    curDate postDate searchDate __typename
  }
}"""


def gql(variables, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(
                URL,
                json={"operationName": "GetPerfoStatsPerfoByList", "query": QUERY, "variables": variables},
                headers=HEADERS,
                timeout=20,
            )
            data = r.json()
            if "errors" in data:
                print(f"  [에러] {variables}: {data['errors'][0]['message']}", flush=True)
                return None
            return data["data"]["perfoStatsPerfoByList"]["result"]
        except Exception as e:
            print(f"  [retry {attempt + 1}] {variables}: {e}", flush=True)
            time.sleep(2)
    return None


def _parse_runtime_minutes(raw):
    """'2시간 10분' / '90분' -> 분 단위 정수. pipeline.py의 동일 함수와 로직 통일."""
    if not raw:
        return None
    s = str(raw)
    h = re.search(r"(\d+)\s*시간", s)
    m = re.search(r"(\d+)\s*분", s)
    if not h and not m:
        return None
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


def load_detail_df(detail_path):
    """02_공연상세.csv를 통째로 읽어서 pandas DataFrame으로 반환 (없으면 None)."""
    if not os.path.isfile(detail_path):
        print(f"  경고: {detail_path} 없음", flush=True)
        return None
    import pandas as pd
    return pd.read_csv(detail_path, dtype=str, encoding="utf-8-sig", low_memory=False)


def build_runtime_lookup(detail_df):
    """DataFrame에서 perf_id -> runtime_min 매핑을 만든다."""
    lookup = {}
    if detail_df is None:
        return lookup
    for _, r in detail_df.iterrows():
        pid = r.get("mt20id")
        if not pid or pid in lookup:
            continue
        lookup[pid] = _parse_runtime_minutes(r.get("prfruntime"))
    print(f"  러닝타임 조회표: {len(lookup):,}개 공연", flush=True)
    return lookup


def fetch_missing_details(missing_ids, api_key):
    """02_공연상세.csv에 아직 없는 perf_id들을 KOPIS 공연상세 API로 직접 채운다.
    fetch_details.py와 동일한 엔드포인트/파싱 로직."""
    rows = []
    if not api_key:
        print("  KOPIS_API_KEY 없음 - 신규 공연상세 보강 건너뜀 (runtime_min 비워둠)", flush=True)
        return rows

    print(f"  02_공연상세.csv에 없는 신규 공연 {len(missing_ids):,}개 상세 조회 중...", flush=True)
    for i, mt20id in enumerate(missing_ids):
        for attempt in range(3):
            try:
                r = requests.get(f"{DETAIL_API_BASE}/{mt20id}", params={"service": api_key}, timeout=30)
                r.raise_for_status()
                root = ET.fromstring(r.content)
                for it in root.findall("db"):
                    row = {c.tag: (c.text or "").strip() for c in it}
                    styurls = it.find("styurls")
                    if styurls is not None:
                        row["styurls"] = "|".join((u.text or "").strip() for u in styurls.findall("styurl"))
                    rows.append(row)
                break
            except Exception as e:
                print(f"    [retry {attempt + 1}] {mt20id}: {e}", flush=True)
                time.sleep(2)
        if (i + 1) % 50 == 0:
            print(f"    {i + 1:,}/{len(missing_ids):,} 처리", flush=True)
        time.sleep(0.2)
    print(f"  신규 공연상세 {len(rows):,}건 확보", flush=True)
    return rows


def week_windows(start, end):
    """start~end를 7일 단위(겹침 없음)로 쪼갠다."""
    cur = start
    while cur <= end:
        w_end = min(cur + timedelta(days=6), end)
        yield cur, w_end
        cur = w_end + timedelta(days=1)


def determine_start_date(existing_rows, weeks_back):
    if existing_rows:
        max_end = max(r["week_end_date"] for r in existing_rows)
        return datetime.strptime(max_end, "%Y%m%d") + timedelta(days=1)
    return datetime.today() - timedelta(weeks=weeks_back)


def load_existing(path):
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_new_perf_candidates(new_rows, targets_enriched_path, out_path):
    """이번에 새로 잡힌 공연 중 targets_enriched.csv(영상 수집 대상)에 아직
    없는 것들만 골라서, pipeline.py sync-new-performances가 바로 먹을 수 있는
    01_공연목록.csv 스키마(mt20id/prfnm/genrenm/prfpdfrom/prfpdto/fcltynm)로
    저장한다. 후속 워크플로 스텝에서 sync-new-performances를 호출해 이 파일을
    targets_enriched.csv/work_groups.json에 반영한다."""
    existing_ids = set()
    if os.path.isfile(targets_enriched_path):
        import pandas as pd
        existing_ids = set(pd.read_csv(targets_enriched_path, dtype=str, encoding="utf-8-sig")["perf_id"])

    seen = {}
    for r in new_rows:
        pid = r["perf_id"]
        if not pid or pid in existing_ids or pid in seen:
            continue
        seen[pid] = {
            "mt20id": pid,
            "prfnm": r["title"],
            "genrenm": r["genre"],
            "prfpdfrom": r["perf_start_date"],
            "prfpdto": r["perf_end_date"],
            "fcltynm": r["venue_name"],
        }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if not seen:
        print("  영상 수집 대상(targets_enriched.csv)에 없는 신규 공연 없음", flush=True)
        # 후속 스텝이 파일 존재 여부로 분기하기 쉽도록 빈 파일은 만들지 않음
        if os.path.isfile(out_path):
            os.remove(out_path)
        return

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["mt20id", "prfnm", "genrenm", "prfpdfrom", "prfpdto", "fcltynm"])
        w.writeheader()
        w.writerows(seen.values())
    print(f"  영상 수집 대상 후보 {len(seen):,}건 -> {out_path}", flush=True)


def collect(target_path, detail_path, weeks_back):
    existing_rows = load_existing(target_path)
    print(f"기존 행 수: {len(existing_rows):,}", flush=True)

    start = determine_start_date(existing_rows, weeks_back)
    yesterday = datetime.today() - timedelta(days=1)
    if start > yesterday:
        print(f"이미 최신 상태 (다음 시작일 {start.strftime('%Y-%m-%d')} > 어제). 수집할 신규 구간 없음.", flush=True)
        return

    print(f"수집 구간: {start.strftime('%Y-%m-%d')} ~ {yesterday.strftime('%Y-%m-%d')}", flush=True)

    detail_df = load_detail_df(detail_path)
    runtime_lookup = build_runtime_lookup(detail_df)

    new_rows = []
    for w_start, w_end in week_windows(start, yesterday):
        s, e = w_start.strftime("%Y%m%d"), w_end.strftime("%Y%m%d")
        print(f"  {s} ~ {e} 수집 중...", flush=True)
        page = 1
        while True:
            result = gql({
                "startDate": s, "endDate": e, "sql_type": "week",
                "prfrNm": "", "curPage": page, "pageSize": 100,
            })
            if not result:
                break
            for r in result:
                perf_id = r.get("data11", "")
                new_rows.append({
                    "rank": r.get("data1", ""),
                    "title": r.get("data2", ""),
                    "genre": r.get("data3", ""),
                    "perf_start_date": r.get("data4", ""),
                    "perf_end_date": r.get("data5", ""),
                    "venue_name": r.get("data6", ""),
                    "runtime_min": None,  # 아래서 채움
                    "num_showings": r.get("data7", ""),
                    "ticket_sales_qty": r.get("data8", ""),
                    "ticket_sales_amount": r.get("data9", ""),
                    "perf_id": perf_id,
                    "company_id": r.get("data12", ""),
                    "week_start_date": s,
                    "week_end_date": e,
                })
            print(f"    page {page}: +{len(result)}건", flush=True)
            if len(result) < 100:
                break
            page += 1
            time.sleep(0.2)
        time.sleep(0.3)

    print(f"신규 행: {len(new_rows):,}", flush=True)
    if not new_rows:
        print("완료! (신규 데이터 없음, 기존 파일 그대로 둠)", flush=True)
        return

    # 02_공연상세.csv에 없는 신규 공연은 API로 직접 보강 (누락된 runtime_min 최소화)
    missing_ids = sorted({r["perf_id"] for r in new_rows if r["perf_id"] and r["perf_id"] not in runtime_lookup})
    if missing_ids:
        api_key = os.environ.get("KOPIS_API_KEY")
        detail_rows = fetch_missing_details(missing_ids, api_key)
        if detail_rows:
            import pandas as pd
            new_detail_df = pd.DataFrame(detail_rows)
            if detail_df is not None:
                combined_detail = pd.concat([detail_df, new_detail_df], ignore_index=True)
            else:
                combined_detail = new_detail_df
            combined_detail = combined_detail.drop_duplicates(subset=["mt20id"], keep="last")
            combined_detail.to_csv(detail_path, index=False, encoding="utf-8-sig")
            print(f"  {detail_path} 갱신: {len(combined_detail):,}행 (신규 {len(new_detail_df):,}건 추가)", flush=True)
            for row in detail_rows:
                pid = row.get("mt20id")
                if pid:
                    runtime_lookup[pid] = _parse_runtime_minutes(row.get("prfruntime"))

    for r in new_rows:
        r["runtime_min"] = runtime_lookup.get(r["perf_id"], "")
        if r["runtime_min"] is None:
            r["runtime_min"] = ""

    write_new_perf_candidates(new_rows, TARGETS_ENRICHED_PATH, NEW_PERF_CANDIDATES_PATH)

    # 기존 + 신규 합치고, (perf_id, week_start_date, week_end_date) 기준 중복 제거(신규 우선)
    combined = {}
    for r in existing_rows:
        combined[(r["perf_id"], r["week_start_date"], r["week_end_date"])] = r
    for r in new_rows:
        combined[(r["perf_id"], r["week_start_date"], r["week_end_date"])] = r

    all_rows = list(combined.values())
    all_rows.sort(key=lambda r: (r["week_start_date"], r["week_end_date"], r.get("rank", "")))

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"완료! {target_path} 총 {len(all_rows):,}행 (기존 {len(existing_rows):,} + 신규 {len(new_rows):,}, 중복 제거 후)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default=TARGET_PATH)
    ap.add_argument("--detail", default=DETAIL_PATH)
    ap.add_argument("--weeks-back", type=int, default=8, help="기존 파일이 없을 때 초기 수집 구간(주)")
    args = ap.parse_args()
    collect(args.target, args.detail, args.weeks_back)


if __name__ == "__main__":
    main()
