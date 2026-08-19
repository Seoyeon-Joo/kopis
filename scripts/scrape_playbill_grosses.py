"""
scrape_playbill_grosses.py
============================
tacookson/data의 grosses.csv가 2020-03-08에서 끊기는데, playbill.com/grosses는
2026년 현재까지 데이터가 계속 있음(2020-03~2021-08 코로나 셧다운 기간만 공백).
이 스크립트는 그 공백 이후 데이터를 이어붙이기 위한 스크래퍼.

URL 패턴 확인 완료: https://playbill.com/grosses?week=YYYY-MM-DD
(예: ?week=2026-07-26 -> Week 9, Week's Total $34,864,840.00 로 정확히 그 주
 데이터가 서버에서 렌더링되어 나오는 걸 실제로 확인함. JSON API 우회 필요 없음.)

각 쇼 이름은 playbill.com/production/gross?production=<UUID> 링크를 갖고 있어서,
이 UUID를 안정적인 perf_id로 바로 쓸 수 있음 (재공연도 UUID가 달라서 구분 가능).

*** 주의: 실제 <table> 태그 구조(클래스명 등)는 raw HTML을 직접 못 받아온 상태에서
텍스트 레이아웃만 보고 추정한 파서예요. 처음 몇 주치 결과를 꼭 grosses.csv 형식과
비교해서 컬럼이 밀리지 않았는지 확인하세요. 안 맞으면 이 페이지를 브라우저에서
'페이지 소스 보기'로 열어서 <table> 부분의 실제 태그 구조를 저한테 보내주시면
바로 고쳐드릴게요. ***

Usage:
  python scrape_playbill_grosses.py --start 2021-08-08 --end 2026-08-09 \
      --out playbill_grosses_recent.csv
"""
import argparse
import os
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
import pandas as pd

URL_TEMPLATE = "https://playbill.com/grosses?week={week}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html",
}

PRODUCTION_LINK_RE = re.compile(r"/production/gross\?production=([0-9a-fA-F-]+)")
MONEY_RE = re.compile(r"-?\$[\d,]+\.\d{2}")
NUMBER_RE = re.compile(r"-?[\d,]+(?:\.\d+)?%?")


def week_range(start_date, end_date):
    """start_date부터 end_date까지 7일 간격 주차(일요일 시작 가정)를 생성."""
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=7)


def _split_stacked(text):
    """'7,548 1,026' 처럼 공백으로 붙은 두 숫자를 분리.
    쇼 표는 This Week Gross/Potential Gross, Avg Ticket/Top Ticket,
    Seats Sold/Seats in Theatre, Perfs/Previews 가 한 셀에 두 줄로 쌓여있음."""
    parts = text.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return text, None


def parse_html_table(html, week):
    """grosses?week=... 페이지의 표를 파싱.
    각 행 구조(확인됨): [Show+Theatre(링크 포함)] [This Week Gross\nPotential Gross]
    [Diff $] [Avg Ticket\nTop Ticket] [Seats Sold\nSeats in Theatre]
    [Perfs\nPreviews] [% Cap] [Diff % cap]"""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    table = soup.find("table")
    if not table:
        print(f"  [{week}] 테이블을 못 찾음 - 페이지 구조가 바뀌었을 수 있음")
        return rows

    body_rows = table.find_all("tr")
    for tr in body_rows:
        cells = tr.find_all("td")
        if not cells or len(cells) < 7:
            continue  # 헤더 행 등 스킵

        # 1번째 셀: 쇼 이름(+ production UUID 링크) + 극장 이름
        first_cell = cells[0]
        link = first_cell.find("a", href=PRODUCTION_LINK_RE)
        production_id = None
        show = None
        if link:
            m = PRODUCTION_LINK_RE.search(link.get("href", ""))
            production_id = m.group(1) if m else None
            show = link.get_text(strip=True)
        cell_text_lines = list(first_cell.stripped_strings)
        theatre = cell_text_lines[-1] if len(cell_text_lines) > 1 else None
        if show is None and cell_text_lines:
            show = cell_text_lines[0]

        gross_text = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""
        this_week_gross, potential_gross = _split_stacked(gross_text)

        diff_dollar = cells[2].get_text(strip=True) if len(cells) > 2 else None

        ticket_text = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
        avg_ticket, top_ticket = _split_stacked(ticket_text)

        seats_text = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""
        seats_sold, seats_in_theatre = _split_stacked(seats_text)

        perf_text = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""
        performances, previews = _split_stacked(perf_text)

        pct_cap = cells[6].get_text(strip=True) if len(cells) > 6 else None
        diff_pct_cap = cells[7].get_text(strip=True) if len(cells) > 7 else None

        if not show:
            continue

        rows.append({
            "week_ending": week,
            "production_id": production_id,
            "show": show,
            "theatre": theatre,
            "this_week_gross": this_week_gross,
            "potential_gross": potential_gross,
            "diff_dollar": diff_dollar,
            "avg_ticket_price": avg_ticket,
            "top_ticket_price": top_ticket,
            "seats_sold": seats_sold,
            "seats_in_theatre": seats_in_theatre,
            "performances": performances,
            "previews": previews,
            "pct_capacity": pct_cap,
            "diff_pct_cap": diff_pct_cap,
        })
    return rows


def fetch_week(week, session, retries=3):
    week_str = week.strftime("%Y-%m-%d")
    url = URL_TEMPLATE.format(week=week_str)

    for attempt in range(retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return parse_html_table(resp.text, week_str)
        except Exception as e:
            print(f"  [retry {attempt+1}] {week_str}: {e}")
            time.sleep(2)
    return []


def determine_start(existing_path, fallback_start):
    """기존 CSV가 있으면 그 파일의 최대 week_ending 다음 주부터 시작.
    (fetch_perfoby_weekly.py의 determine_start_date()와 동일한 설계)"""
    import os
    if not existing_path or not os.path.isfile(existing_path):
        return fallback_start
    df = pd.read_csv(existing_path, usecols=["week_ending"])
    if df.empty:
        return fallback_start
    max_week = pd.to_datetime(df["week_ending"]).max()
    return max_week + timedelta(days=7)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (--append-to 사용 시 생략 가능)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (생략하면 오늘 기준 최신 완료 주)")
    ap.add_argument("--out", default="playbill_grosses_recent.csv")
    ap.add_argument("--append-to", default=None,
                     help="기존 CSV 경로. 지정하면 그 파일의 마지막 주 다음부터 자동으로 이어서 수집하고,"
                          " 끝나면 기존 데이터 + 신규 데이터를 합쳐 같은 경로에 다시 씀.")
    ap.add_argument("--sleep", type=float, default=1.0, help="요청 사이 대기 시간(초) - 서버 부담 줄이기용")
    args = ap.parse_args()

    if args.append_to:
        fallback = datetime.strptime(args.start, "%Y-%m-%d") if args.start else datetime(2021, 8, 8)
        start = determine_start(args.append_to, fallback)
        out_path = args.append_to
    else:
        if not args.start:
            ap.error("--start가 필요해요 (또는 --append-to 사용)")
        start = datetime.strptime(args.start, "%Y-%m-%d")
        out_path = args.out

    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.today() - timedelta(days=1)

    if start > end:
        print(f"수집할 신규 주 없음 (다음 시작일 {start.date()} > 종료일 {end.date()})")
        return

    session = requests.Session()
    all_rows = []
    weeks = list(week_range(start, end))
    print(f"{len(weeks)}개 주차 수집 시작")

    for i, week in enumerate(weeks, 1):
        rows = fetch_week(week, session)
        all_rows.extend(rows)
        print(f"[{i}/{len(weeks)}] {week.strftime('%Y-%m-%d')}: {len(rows)}건")
        time.sleep(args.sleep)

    if all_rows:
        df_new = pd.DataFrame(all_rows)
        if args.append_to and os.path.isfile(args.append_to):
            df_existing = pd.read_csv(args.append_to)
            df = pd.concat([df_existing, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=["week_ending", "production_id"], keep="last")
        else:
            df = df_new
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n완료: 신규 {len(df_new)}행 수집, 총 {len(df)}행 -> {out_path}")
    else:
        print("\n수집된 신규 데이터가 없어요. URL 패턴/파싱 로직을 다시 확인해주세요.")


if __name__ == "__main__":
    main()
