"""
data/16_공연통계_공연별_일별_전체.csv 를 이어서 채우는 스크립트 (일간 버전).

fetch_perfoby_weekly.py(주간 자동화)와는 완전히 별개 파일/별개 워크플로우.
이쪽은 KOPIS GraphQL perfoStatsPerfoByList를 sql_type="day"로, 하루씩 개별
호출해서 공연 x 일 단위의 세밀한 패널을 쌓는다. 요일 정렬 문제 자체가
생길 수 없다 (하루는 항상 하루라서 "주간 경계 어긋남" 같은 게 없음).

data1~data12 매핑은 주간 버전과 동일 (2026-06-26 세션에서 검증):
  data1=rank, data2=title, data3=genre, data4=perf_start_date,
  data5=perf_end_date, data6=venue_name, data7=num_showings,
  data8=ticket_sales_qty, data9=ticket_sales_amount,
  data11=perf_id, data12=company_id   (data10은 응답에 없음)

기존 scripts/fetch_graphql_stats.py의 collect_perfoby()가 이미 같은 방식으로
sql_type="day" 호출을 하고 있었지만, 그건 청크 병렬 백필용(한 번 돌리고
끝)이었다. 이 스크립트는 그 로직을 "마지막 저장일 다음날 ~ 어제까지 매일
이어붙이는" 증분 수집용으로 재구성한 것.

용도: 이 파일은 targets_enriched.csv/build-targets에 자동으로 연결되지
않는다 (그 역할은 여전히 fetch_perfoby_weekly.py + pipeline.py build-targets가
담당). 순수하게 "공연 x 일" 단위 패널이 필요할 때(예: 논문 회귀분석의
일별 DV) 쓰는 별도 데이터 소스다. 필요하면 나중에 이 파일을 주 단위로
groupby해서 16_..._with_runtime.csv 검증용으로 대조해볼 수도 있다.

Usage:
  python fetch_perfoby_daily.py
  python fetch_perfoby_daily.py --days-back 60   # 기존 파일이 없을 때 초기 수집 구간(일)
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests

URL = "https://kopis.or.kr:9001/api/prs/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://kopis.or.kr/por/stats/perfo/perfoStatsPerfoBy.do",
    "User-Agent": "Mozilla/5.0",
}

TARGET_PATH = "data/16_공연통계_공연별_일별_전체.csv"
DETAIL_PATH = "data/02_공연상세.csv"

FIELDNAMES = [
    "date", "rank", "title", "genre", "perf_start_date", "perf_end_date",
    "venue_name", "runtime_min", "num_showings", "ticket_sales_qty",
    "ticket_sales_amount", "perf_id", "company_id",
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


def build_runtime_lookup(detail_path):
    """02_공연상세.csv에서 perf_id -> runtime_min 매핑을 만든다 (없으면 빈 dict)."""
    lookup = {}
    if not os.path.isfile(detail_path):
        print(f"  경고: {detail_path} 없음 - runtime_min 비워둠", flush=True)
        return lookup
    import pandas as pd
    df = pd.read_csv(detail_path, dtype=str, encoding="utf-8-sig", low_memory=False)
    for _, r in df.iterrows():
        pid = r.get("mt20id")
        if not pid or pid in lookup:
            continue
        lookup[pid] = _parse_runtime_minutes(r.get("prfruntime"))
    print(f"  러닝타임 조회표: {len(lookup):,}개 공연", flush=True)
    return lookup


def day_range(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def determine_start_date(existing_rows, days_back):
    if existing_rows:
        max_date = max(r["date"] for r in existing_rows)
        return datetime.strptime(max_date, "%Y%m%d") + timedelta(days=1)
    return datetime.today() - timedelta(days=days_back)


def load_existing(path):
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def collect(target_path, detail_path, days_back):
    existing_rows = load_existing(target_path)
    print(f"기존 행 수: {len(existing_rows):,}", flush=True)

    start = determine_start_date(existing_rows, days_back)
    yesterday = datetime.today() - timedelta(days=1)
    if start > yesterday:
        print(f"이미 최신 상태 (다음 시작일 {start.strftime('%Y-%m-%d')} > 어제). 수집할 신규 날짜 없음.", flush=True)
        return

    print(f"수집 구간: {start.strftime('%Y-%m-%d')} ~ {yesterday.strftime('%Y-%m-%d')} ({(yesterday-start).days + 1}일)", flush=True)

    runtime_lookup = build_runtime_lookup(detail_path)

    new_rows = []
    for day in day_range(start, yesterday):
        date_str = day.strftime("%Y%m%d")
        page = 1
        day_count = 0
        while True:
            result = gql({
                "startDate": date_str, "endDate": date_str, "sql_type": "day",
                "prfrNm": "", "curPage": page, "pageSize": 100,
            })
            if not result:
                break
            for r in result:
                perf_id = r.get("data11", "")
                new_rows.append({
                    "date": date_str,
                    "rank": r.get("data1", ""),
                    "title": r.get("data2", ""),
                    "genre": r.get("data3", ""),
                    "perf_start_date": r.get("data4", ""),
                    "perf_end_date": r.get("data5", ""),
                    "venue_name": r.get("data6", ""),
                    "runtime_min": runtime_lookup.get(perf_id, "") or "",
                    "num_showings": r.get("data7", ""),
                    "ticket_sales_qty": r.get("data8", ""),
                    "ticket_sales_amount": r.get("data9", ""),
                    "perf_id": perf_id,
                    "company_id": r.get("data12", ""),
                })
                day_count += len(result)
            if len(result) < 100:
                break
            page += 1
            time.sleep(0.2)
        print(f"  {date_str}: +{day_count}건", flush=True)
        time.sleep(0.3)

    print(f"신규 행: {len(new_rows):,}", flush=True)
    if not new_rows:
        print("완료! (신규 데이터 없음, 기존 파일 그대로 둠)", flush=True)
        return

    # 기존 + 신규 합치고, (perf_id, date) 기준 중복 제거(신규 우선)
    combined = {}
    for r in existing_rows:
        combined[(r["perf_id"], r["date"])] = r
    for r in new_rows:
        combined[(r["perf_id"], r["date"])] = r

    all_rows = list(combined.values())
    all_rows.sort(key=lambda r: (r["date"], r.get("rank", "")))

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
    ap.add_argument("--days-back", type=int, default=60, help="기존 파일이 없을 때 초기 수집 구간(일)")
    args = ap.parse_args()
    collect(args.target, args.detail, args.days_back)


if __name__ == "__main__":
    main()
