"""
KOPIS GraphQL 통계 수집 스크립트 (GitHub Actions용)

Usage:
  python fetch_graphql_stats.py summary <quick|full>
      -> 가벼운 7종 요약표 (종합/지역/장르/가격대×2/시간대/장르(예매)) 수집
  python fetch_graphql_stats.py perfoby <chunk_index> <total_chunks> <quick|full>
      -> 무거운 "공연별 일별" 데이터를 날짜 구간으로 쪼개서 수집 (matrix 병렬용)
"""
import os, sys, csv, time
from datetime import datetime, timedelta
import requests

URL = "https://kopis.or.kr:9001/api/prs/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://kopis.or.kr/por/stats/perfo/perfoStatsPerfoBy.do",
    "User-Agent": "Mozilla/5.0",
}
OUT_DIR = "output_graphql"
os.makedirs(OUT_DIR, exist_ok=True)

TODAY = datetime.today()


def get_range(mode):
    if mode == "full":
        return TODAY - timedelta(days=365 * 3), TODAY
    return TODAY - timedelta(days=30), TODAY


def gql(operation_name, query, variables, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(
                URL,
                json={"operationName": operation_name, "query": query, "variables": variables},
                headers=HEADERS,
                timeout=20,
            )
            data = r.json()
            if "errors" in data:
                print(f"  [에러] {operation_name} {variables}: {data['errors'][0]['message']}", flush=True)
                return None
            return data["data"]
        except Exception as e:
            print(f"  [retry {attempt+1}] {operation_name}: {e}", flush=True)
            time.sleep(2)
    return None


def save_csv(rows, filename):
    if not rows:
        print(f"  데이터 없음, 건너뜀: {filename}", flush=True)
        return
    path = os.path.join(OUT_DIR, filename)
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  저장: {filename} ({len(rows):,}행)", flush=True)


def six_month_chunks(start, end):
    chunks = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=180), end)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def day_range(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# ──────────────────────────────────────────────────────────────────────────
# SUMMARY (가벼운 7종) - 6개월 단위로 쪼개서 호출
# ──────────────────────────────────────────────────────────────────────────

def collect_summary(start, today):
    print("[1/7] 종합통계", flush=True)
    q = """query GetTotal($startDate: String!, $endDate: String!) {
      perfoStatsTotalList(startDate: $startDate, endDate: $endDate) {
        result { data1 data2 data3 data4 __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetTotal", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d")})
        if d:
            for r in d["perfoStatsTotalList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "13b_종합통계.csv")

    print("[2/7] 지역별(공연통계)", flush=True)
    q = """query GetArea($startDate: String!, $endDate: String!, $col: String!, $order: String!) {
      perfoStatsAreaList(startDate: $startDate, endDate: $endDate, col: $col, order: $order) {
        result { data1 data2 data3 data4 data5 data6 data7 data8 data9 data10 data11 data12 data13 data14 data15 data16 data17 data18 data19 data20 data26 stdgCpsggCd __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetArea", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "col": "amt", "order": "desc"})
        if d:
            for r in d["perfoStatsAreaList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "14_공연통계_지역별.csv")

    print("[3/7] 장르별(공연통계)", flush=True)
    q = """query GetCate($startDate: String!, $endDate: String!, $col: String!, $order: String!, $sql_type: String!, $genre_code: String) {
      perfoStatsCateList(startDate: $startDate, endDate: $endDate, col: $col, order: $order, sql_type: $sql_type, genre_code: $genre_code) {
        result { data1 data3 data4 data5 data6 data7 data8 data9 data10 data11 data12 data13 data14 data15 data16 data17 data18 __typename }
        curDate postDate searchDate totalCnt __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetCate", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "col": "amt", "order": "desc", "sql_type": "month", "genre_code": ""})
        if d:
            for r in d["perfoStatsCateList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "15_공연통계_장르별.csv")

    print("[4/7] 가격대별(공연통계)", flush=True)
    q = """query GetPrice($startDate: String!, $endDate: String!, $sql_type: String!) {
      perfoStatsPriceList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type) {
        result { genreCode genreCodeNm prcFreeAdslNocs prcK30LwrAdslNocs prcK30K50AdslNocs prcK50K70AdslNocs prcK70K100AdslNocs prcK100K150AdslNocs prcK150UpAdslNocs tcktngSumQty totNtssNmrsSm totNtssAmountSm __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetPrice", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "sql_type": "month"})
        if d:
            for r in d["perfoStatsPriceList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "18_공연통계_가격대별.csv")

    print("[5/7] 장르별(예매통계)", flush=True)
    q = """query GetBstCate($genre_code: String, $startDate: String!, $endDate: String!, $col: String!, $order: String!, $resultType: String!) {
      bstStatsCateList(genre_code: $genre_code, startDate: $startDate, endDate: $endDate, col: $col, order: $order, resultType: $resultType) {
        result { data1 data4 data5 data6 data7 data8 data10 data13 data14 data15 data16 data17 data18 __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetBstCate", q, {"genre_code": "", "startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "col": "genreCode", "order": "asc", "resultType": "1"})
        if d:
            for r in d["bstStatsCateList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "12_예매통계_장르별.csv")

    print("[6/7] 시간대별(예매통계)", flush=True)
    q = """query GetBstTime($startDate: String!, $endDate: String!, $sql_type: String!, $col: String!, $order: String!, $resultType: String!) {
      bstStatsTimeList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type, col: $col, order: $order, resultType: $resultType) {
        result { data1 data3 data4 data5 data8 data9 data13 data14 data15 data16 data17 data18 data19 __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetBstTime", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "sql_type": "day", "col": "date", "order": "asc", "resultType": "1"})
        if d:
            for r in d["bstStatsTimeList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "예매통계_시간대별.csv")

    print("[7/7] 가격대별(예매통계)", flush=True)
    q = """query GetBstPrice($startDate: String!, $endDate: String!, $sql_type: String!, $resultType: String!) {
      bstStatsPriceList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type, resultType: $resultType) {
        result { genreCode genreCodeNm prcFreeAdslNocs prcK30LwrAdslNocs prcK30K50AdslNocs prcK50K70AdslNocs prcK70K100AdslNocs prcK100K150AdslNocs prcK150UpAdslNocs tcktngSumQty totNtssNmrsSm totNtssAmountSm __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetBstPrice", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "sql_type": "day", "resultType": "1"})
        if d:
            for r in d["bstStatsPriceList"]["result"]:
                r["_period_start"], r["_period_end"] = s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
                rows.append(r)
        time.sleep(0.3)
    save_csv(rows, "예매통계_가격대별.csv")

    print("[+] 기간별 일별 (공연통계, perfoStatsDateList)", flush=True)
    q = """query GetDate($startDate: String!, $endDate: String!, $sql_type: String!, $col: String!, $order: String!) {
      perfoStatsDateList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type, col: $col, order: $order) {
        result { data1 data2 data3 data4 data5 data6 data7 data8 data9 data12 data13 data14 data15 data16 data17 data18 data19 __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for s, e in six_month_chunks(start, today):
        d = gql("GetDate", q, {"startDate": s.strftime("%Y%m%d"), "endDate": e.strftime("%Y%m%d"), "sql_type": "day", "col": "date", "order": "asc"})
        if d:
            rows.extend(d["perfoStatsDateList"]["result"])
        time.sleep(0.3)
    save_csv(rows, "13_공연통계_기간별_일별.csv")


# ──────────────────────────────────────────────────────────────────────────
# PERFOBY (공연별 일별) - 날짜 구간을 청크로 쪼개서 병렬 처리
# ──────────────────────────────────────────────────────────────────────────

def collect_perfoby(start, today, chunk_index, total_chunks):
    total_days = (today - start).days + 1
    days_per_chunk = total_days // total_chunks + 1
    chunk_start = start + timedelta(days=days_per_chunk * chunk_index)
    chunk_end = min(chunk_start + timedelta(days=days_per_chunk - 1), today)
    if chunk_start > today:
        print(f"  청크 {chunk_index}: 범위 밖, 건너뜀", flush=True)
        save_csv([], f"16_공연통계_공연별_일별_chunk{chunk_index}.csv")
        return

    print(f"  청크 {chunk_index}/{total_chunks}: {chunk_start.strftime('%Y-%m-%d')} ~ {chunk_end.strftime('%Y-%m-%d')}", flush=True)

    q = """query GetPerfoStatsPerfoByList($startDate: String!, $endDate: String!, $sql_type: String!, $prfrNm: String, $curPage: Int!, $pageSize: Int!) {
      perfoStatsPerfoByList(startDate: $startDate, endDate: $endDate, sql_type: $sql_type, prfrNm: $prfrNm, curPage: $curPage, pageSize: $pageSize) {
        result { data1 data2 data3 data4 data5 data6 data7 data8 data9 data11 data12 totalCnt __typename }
        curDate postDate searchDate __typename
      }
    }"""
    rows = []
    for day in day_range(chunk_start, chunk_end):
        date_str = day.strftime("%Y%m%d")
        page = 1
        while True:
            d = gql("GetPerfoStatsPerfoByList", q, {
                "startDate": date_str, "endDate": date_str, "sql_type": "day",
                "prfrNm": "", "curPage": page, "pageSize": 100,
            })
            if not d:
                break
            result = d["perfoStatsPerfoByList"]["result"]
            if not result:
                break
            for r in result:
                r["_date"] = date_str
                rows.append(r)
            if len(result) < 100:
                break
            page += 1
            time.sleep(0.2)
        time.sleep(0.2)
    save_csv(rows, f"16_공연통계_공연별_일별_chunk{chunk_index}.csv")


def main():
    kind = sys.argv[1]
    if kind == "summary":
        mode = sys.argv[2] if len(sys.argv) > 2 else "quick"
        start, today = get_range(mode)
        print(f"=== summary | {mode} | {start.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')} ===", flush=True)
        collect_summary(start, today)
    elif kind == "perfoby":
        chunk_index, total_chunks = int(sys.argv[2]), int(sys.argv[3])
        mode = sys.argv[4] if len(sys.argv) > 4 else "quick"
        start, today = get_range(mode)
        print(f"=== perfoby | {mode} | chunk {chunk_index}/{total_chunks} ===", flush=True)
        collect_perfoby(start, today, chunk_index, total_chunks)
    else:
        print("Usage: python fetch_graphql_stats.py [summary <quick|full>] | [perfoby <chunk> <total> <quick|full>]")
        sys.exit(1)
    print("완료!", flush=True)


if __name__ == "__main__":
    main()
