# -*- coding: utf-8 -*-
"""
pipeline.py
===========
KOPIS 공연 YouTube 축약콘텐츠 수집 파이프라인 전체를 파일 하나로.
서브커맨드 4개로 단계를 나눈다 (기능은 이전 5개 파일과 동일, 합치기만 함):

  python pipeline.py build-targets  --stats ... --season ... --out-dir data/
  python pipeline.py collect        --targets ... --groups ... --api-keys ... --shard-index 0 --shard-count 20
  python pipeline.py merge          --base-dir data/youtube_targeted --targets data/targets_enriched.csv --out-dir data/youtube_targeted
  python pipeline.py qa             --videos ... --catalog ... --out ...

=== 핵심 설계 (대화에서 검증된 내용 요약) ===
- 게이트: keep = (작품명 텍스트매칭) AND (날짜매칭 OR 장소매칭)
  master_merged.csv 50,523건 검증: Precision 87.0%, Recall 45.9%
  (오탐 최소화 요청에 맞춰 recall을 희생하고 precision을 최대화)
- matched_title "기관명, 작품명" 파싱 버그, description NaN 버그 수정 반영
- 시즌 그룹(초연/재연 등)은 대표 제목으로 1회만 검색 -> 결과를 날짜 근접도로
  멤버별(개별 perf_id) 재배분. KOPIS 카탈로그 4,093개 전부 커버됨(검증완료).
- excluded_video_ids.txt는 선택사항 - 없으면 그냥 빈 걸로 취급하고 진행.
- QA(institution cross-check): 인라인 게이트만으로 못 잡는 "같은 기관의
  다른 작품" 오배정(예: 유령↔퉁소소리)을 merge 이후 별도로 잡아낸다.
- post_end 표기: 공연 종료일 이후 업로드된 영상은 해당 perf_id의 티켓
  판매에 인과적으로 영향을 줄 수 없다는 판단에 따라(2026-07-30 대화 결정),
  merge 단계에서 매 행마다 `is_post_end` 컬럼을 붙이고 gated/dropped를
  각각 "종료일 이전"과 "종료일 이후" 4개 파일로 분리해 저장한다. 원본
  전체(all_videos.csv)는 shard 파일에서 언제든 재생성 가능해 기본적으로는
  더 이상 만들지 않는다 (원하면 --emit-raw로 켤 수 있음).
"""
import argparse
import random
import csv
import glob
import json
import os
import re
import sys
import time
from collections import defaultdict

import pandas as pd

API_BASE = "https://www.googleapis.com/youtube/v3"


# =============================================================================
# 공통 신호 계산 (구 signal_utils.py)
# =============================================================================

def normalize(s):
    if s is None:
        return ""
    if isinstance(s, float) and pd.isna(s):
        return ""
    s = re.sub(r"\[.*?\]", "", str(s))
    s = re.sub(r"[^\w가-힣]", "", s)
    return s.lower()


def parse_names(raw):
    """'김민준, 임혜란, 이태훈 등' 같은 prfcast/prfcrew 원문을 이름 리스트로 파싱.
    끝의 '등' 접미사는 제거. 2글자 미만 토큰(오탐 위험 큼)은 버림."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    s = str(raw).strip()
    s = re.sub(r"\s*등\s*$", "", s)
    names = [n.strip() for n in re.split(r"[,、·/]", s) if n.strip()]
    return [n for n in names if len(n) >= 2]


def split_institution_and_work(raw_title):
    """'기관명, 작품명' / '...: 작품명' -> (기관명, 작품명). 마지막 세그먼트가 작품명."""
    if raw_title is None or (isinstance(raw_title, float) and pd.isna(raw_title)):
        return None, ""
    t = re.sub(r"\[.*?\]", "", str(raw_title)).strip()
    parts = [p.strip() for p in re.split(r"[,:：]", t) if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return None, (parts[0] if parts else t)


def venue_core(venue_name):
    if venue_name is None or (isinstance(venue_name, float) and pd.isna(venue_name)):
        return ""
    v = re.sub(r"\[.*?\]", "", str(venue_name)).strip()
    return re.split(r"\s+", v)[0] if v else ""


QUOTE_PATTERN = re.compile(r"['\"‘’“”「」〈》《〉]([^'\"‘’“”「」〈》《〉]{1,25})['\"‘’“”「」〈》《〉]")


def quoted_exact_match(work_title, combined_raw_text):
    work_raw = (work_title or "").strip()
    if not work_raw:
        return False
    quotes = QUOTE_PATTERN.findall(combined_raw_text or "")
    wn = normalize(work_raw)
    return any(normalize(q) == wn for q in quotes)


NEWS_CHANNELS = {
    "YTN", "KBS News", "SBS 뉴스", "연합뉴스TV", "KBS전주",
    "한경arteTV", "채널A News", "MBC뉴스",
}


def _parse_dt(value):
    from datetime import datetime
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "year"):
        return value.replace(tzinfo=None) if getattr(value, "tzinfo", None) else value
    s = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt) + 2] if "T" in s else s[:10], fmt)
        except ValueError:
            continue
    dt = pd.to_datetime(s, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.tz_localize(None).to_pydatetime()


def _date_within_buffer(published_at, perf_start, perf_end, buffer_days=90):
    from datetime import timedelta
    if not published_at or not perf_start or not perf_end:
        return False
    pub, start, end = _parse_dt(published_at), _parse_dt(perf_start), _parse_dt(perf_end)
    if pub is None or start is None or end is None:
        return False
    return start - timedelta(days=buffer_days) <= pub <= end + timedelta(days=buffer_days)


def _load_perf_end_dates(targets_path):
    """targets_enriched.csv에서 perf_id -> perf_end_date 매핑을 만든다.
    merge 단계에서 videos.csv(matched_perf_id, published_at만 있음)와
    조인해 종료일 이후 업로드분을 판별하는 데 쓴다."""
    end_dates = {}
    with open(targets_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            pid = row.get("perf_id")
            end = row.get("perf_end_date")
            if pid and end:
                end_dates[pid] = end
    return end_dates


def _is_post_end(published_at, perf_end_date):
    """공연 종료일 이후 업로드된 영상인지 판별. 날짜를 못 읽으면(둘 중
    하나라도 파싱 실패) 판단 불가로 보고 False(=종료일 이전과 동일하게 취급,
    즉 기존 gated/dropped 파일에서 빠지지 않게) 처리한다."""
    pub, end = _parse_dt(published_at), _parse_dt(perf_end_date)
    if pub is None or end is None:
        return False
    return pub > end


SHORT_TITLE_LEN = 3  # 정규화 후 이 미만 길이 제목은 "짧고 흔한 제목"으로 간주


def compute_signals(perf, video_title, description, channel_name, published_at):
    """keep = text_match AND (date_match OR venue_match) (뉴스 채널은 date_match까지 필수).
    단, 제목이 짧고 흔한 단어(예: '봄', '결', '판')인 경우 substring 매칭만으로는
    오탐 위험이 너무 커서, 기관명 또는 제작사명이 함께 나와야 text_match로 인정한다
    (quote_hit은 창작자가 의도적으로 따옴표/괄호로 작품명을 특정한 것이므로 예외)."""
    inst_name, work_title = split_institution_and_work(perf.get("title"))
    title_core = normalize(work_title)[:6]
    inst_core = normalize(inst_name) if inst_name else ""
    venue_norm = normalize(venue_core(perf.get("venue_name")))
    company_norm = normalize(perf.get("company_name") or "")

    video_title = video_title or ""
    description = description or ""  # description NaN 버그 수정
    combined_raw = f"{video_title} {description}"
    combined_text = normalize(combined_raw)

    date_match = _date_within_buffer(published_at, perf.get("perf_start_date"), perf.get("perf_end_date"))
    venue_match = len(venue_norm) >= 2 and venue_norm in combined_text
    inst_match = len(inst_core) >= 4 and inst_core in combined_text
    company_match = len(company_norm) >= 2 and company_norm in combined_text
    # raw_hit: 길이 제한 없는 순수 substring 존재 여부 (1글자 제목도 검사는 함).
    # substring_hit: 기존 호환용 컬럼 - 2글자 미만은 단독으로는 절대 신뢰 안 함.
    raw_hit = len(title_core) >= 1 and title_core in combined_text
    substring_hit = len(title_core) >= 2 and raw_hit
    quote_hit = quoted_exact_match(work_title, combined_raw)

    # 배우 이름만으로는 "이 공연 영상"이라는 근거가 안 됨(그 배우가 다른 작품에도
    # 출연했을 수 있음) - 그래서 actor_match는 keep 판정에 절대 안 쓰고 참고용
    # 컬럼으로만 남긴다. 검색 쿼리를 넓히는 용도/시즌 재배정 보조 용도로만 사용.
    cast = perf.get("cast") or []
    actor_match = any(len(name) >= 2 and normalize(name) in combined_text for name in cast)

    if len(title_core) < SHORT_TITLE_LEN:
        # 짧고 흔한 제목(1~2글자): raw_hit만으로는 불안정 -> 기관/제작사
        # co-occurrence 필수. 1글자 제목도 이 경로로는 통과 가능
        # (quote_hit은 명시적 인용이라 그 자체로 신뢰).
        text_match = quote_hit or (raw_hit and (inst_match or company_match))
    else:
        text_match = substring_hit or quote_hit

    is_news = channel_name in NEWS_CHANNELS
    # 뉴스 채널이거나(다른 소재 혼입 위험) 반복 행사명(is_recurring_title,
    # 매년 같은 이름으로 재개최되는 지역 축제/시리즈 - venue_match만으로는
    # "어느 해" 영상인지 구분이 안 됨)이면 date_match를 필수로 요구.
    require_date = is_news or bool(perf.get("is_recurring_title"))
    keep = (text_match and date_match) if require_date else (text_match and (date_match or venue_match))

    return {
        "work_title": work_title, "inst_name": inst_name,
        "substring_hit": substring_hit, "quote_hit": quote_hit,
        "inst_match": inst_match, "company_match": company_match,
        "venue_match": venue_match, "date_match": date_match,
        "actor_match": actor_match, "text_match": text_match,
        "is_news": is_news, "keep": keep,
    }


# =============================================================================
# 1단계: build-targets (구 build_targets.py)
# =============================================================================

def _load_detail_enrichment(path):
    """02_공연상세.csv(mt20id 단위, perf_id당 여러 행 있을 수 있음)에서
    캐스트/스태프/제작사 정보를 perf_id 1행으로 정리해서 반환.
    파일이 없으면(경로 미지정 등) None -> 호출부에서 캐스트 강화 없이 진행."""
    if not path or not os.path.isfile(path):
        return None
    cols_wanted = ["mt20id", "prfcast", "prfcrew", "entrpsnm", "entrpsnmP", "entrpsnmA", "fcltynm"]
    detail = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    cols = [c for c in cols_wanted if c in detail.columns]
    detail = detail[cols].drop_duplicates("mt20id", keep="last")
    detail = detail.rename(columns={"mt20id": "perf_id"})

    company_cols = [c for c in ["entrpsnm", "entrpsnmP", "entrpsnmA"] if c in detail.columns]
    if company_cols:
        detail["company_name"] = detail[company_cols].bfill(axis=1).iloc[:, 0]
    else:
        detail["company_name"] = ""

    detail["cast_names"] = detail.get("prfcast", "").apply(parse_names) if "prfcast" in detail.columns else [[]] * len(detail)
    detail["crew_names"] = detail.get("prfcrew", "").apply(parse_names) if "prfcrew" in detail.columns else [[]] * len(detail)
    # 스태프 이름은 캐스트에서 제외 (배우/스태프 이름이 겹치면 오탐 위험)
    detail["cast_names"] = detail.apply(
        lambda r: [n for n in r["cast_names"] if n not in set(r["crew_names"])], axis=1
    )
    return detail[["perf_id", "company_name", "cast_names", "crew_names"]]


def cmd_build_targets(args):
    stats = pd.read_csv(args.stats)
    season = pd.read_csv(args.season)
    detail_enrich = _load_detail_enrichment(args.detail)
    if detail_enrich is not None:
        print(f"공연상세 캐스트/제작사 정보 병합: {len(detail_enrich)}건 "
              f"(캐스트 있는 공연 {(detail_enrich['cast_names'].apply(len) > 0).sum()}건)")
    else:
        print("⚠️  --detail 파일이 없어서 캐스트/제작사 강화 없이 진행해요 (기존 방식과 동일)")

    static_cols = ["perf_id", "title", "genre", "perf_start_date", "perf_end_date",
                   "venue_name", "runtime_min", "company_id"]
    static = stats.drop_duplicates("perf_id")[static_cols]
    sales = stats.groupby("perf_id")["ticket_sales_qty"].sum().rename("total_ticket_sales_qty")

    merged = static.merge(sales, on="perf_id").merge(
        season[["perf_id", "season_match_status", "work_group_key", "season_rank"]],
        on="perf_id", how="left",
    )
    if detail_enrich is not None:
        merged = merged.merge(detail_enrich, on="perf_id", how="left")
        merged["company_name"] = merged["company_name"].fillna("")
        merged["cast_names"] = merged["cast_names"].apply(lambda v: v if isinstance(v, list) else [])
    else:
        merged["company_name"] = ""
        merged["cast_names"] = [[]] * len(merged)

    # season_group_size 컬럼은 신뢰 불가 (원본 후보그룹 크기라 최종 매칭과 다름) -> 재계산
    actual_size = (
        merged[merged["season_match_status"] == "matched"]
        .groupby("work_group_key")["perf_id"].transform("count")
    )
    merged["actual_group_size"] = 1
    merged.loc[merged["season_match_status"] == "matched", "actual_group_size"] = actual_size
    merged["runtime_missing"] = merged["runtime_min"].isna()

    # 시즌그룹(초연/재연)으로 공식 매칭되지 않은 공연 중, "같은 작품명 + 같은
    # 극장"이 서로 다른 perf_id/날짜로 반복되면 "매년 같은 장소에서 재개최되는
    # 지역 축제/시리즈"일 가능성이 큼(예: 흥으로 잇는 세상 - 원주치악예술관에서
    # 해마다 재개최). 이런 경우는 극장 이름만으론 "어느 해"인지 구분이 안 되므로
    # date_match를 필수로 요구한다.
    #
    # 주의: "같은 작품명"만 보고 판단하면 안 됨 - 시지프스[함안]/시지프스[고양]
    # 처럼 같은 제목이 여러 도시를 도는 투어 공연도 여기 걸려버리는데, 이건
    # 도시마다 극장이 다르므로 venue_match가 오히려 훌륭한 구분 신호임(그
    # 도시 극장 이름이 언급되면 그 도시 공연이 맞다는 뜻). 그래서 "극장까지
    # 같아야" 반복행사로 간주하고, 극장이 다르면(투어) venue_match를 그대로
    # 유효한 신호로 남겨둔다.
    standalone = merged[merged["season_match_status"] != "matched"].copy()
    standalone["_work_title_norm"] = standalone["title"].apply(
        lambda t: normalize(split_institution_and_work(t)[1])
    )
    standalone["_venue_norm"] = standalone["venue_name"].apply(lambda v: normalize(venue_core(v)))
    recurrence_count = standalone.groupby(["_work_title_norm", "_venue_norm"])["perf_id"].transform("nunique")
    recurring_perf_ids = set(standalone.loc[recurrence_count >= 2, "perf_id"])
    merged["is_recurring_title"] = merged["perf_id"].isin(recurring_perf_ids)
    n_recurring = merged["is_recurring_title"].sum()
    if n_recurring:
        print(f"반복 행사명 감지(같은 작품명+같은 극장 반복): {n_recurring}건 "
              f"-> 날짜매칭 필수로 게이트 강화 (투어 공연은 극장이 달라서 제외됨)")

    merged = merged.sort_values("total_ticket_sales_qty", ascending=False)

    # 티켓 판매량 상위 10%는 "인기작"으로 표시 - 검색 결과 자체가 넘쳐서
    # (50개 초과, order=relevance 상위권 경쟁이 치열해서) 놓치기 쉬운 공연들.
    # 이런 공연은 collect 단계에서 max_pages를 늘리고 order=both를 강제
    # 적용해서 더 깊게 판다 (자원을 필요한 곳에 몰아주는 방식).
    if len(merged) > 0:
        threshold = merged["total_ticket_sales_qty"].quantile(0.9)
        merged["is_high_demand"] = merged["total_ticket_sales_qty"] >= threshold
        print(f"인기작(티켓판매량 상위 10%) 감지: {merged['is_high_demand'].sum()}건 "
              f"(판매량 {threshold:,.0f}건 이상) -> 검색 시 더 깊게 탐색하도록 표시")
    else:
        merged["is_high_demand"] = False

    os.makedirs(args.out_dir, exist_ok=True)
    targets_path = os.path.join(args.out_dir, "targets_enriched.csv")
    # CSV에는 리스트를 파이프(|)로 join해서 저장 (배우 이름 자체엔 콤마/파이프가 안 들어가므로 안전)
    out_df = merged.copy()
    out_df["cast_names"] = out_df["cast_names"].apply(lambda v: "|".join(v))
    out_df = out_df.astype(str)

    # [2026-08-12] build-targets가 매번 --stats(16번 파일) 기준으로 완전히
    # 새로 쓰다 보니, 16번 파일이 아직 못 따라잡은 신규 공연(sync-new-performances로
    # targets_enriched.csv에 먼저 들어온 것들)이 다음 build-targets 실행 때마다
    # 통째로 지워지는 문제가 있었음(16번 파일 수집이 며칠 지연되면 특히 심함).
    # 그래서 이번 stats에 없는 기존 perf_id는 그대로 보존하고, 있는 것만 최신
    # 통계로 갱신(replace)한다 - "replace 알고 있는 것 + 보존 모르는 것"으로 변경.
    n_from_stats = len(out_df)
    n_preserved = 0
    if os.path.isfile(targets_path):
        prev = pd.read_csv(targets_path, dtype=str, encoding="utf-8-sig")
        stats_ids = set(out_df["perf_id"])
        preserved = prev[~prev["perf_id"].isin(stats_ids)]
        n_preserved = len(preserved)
        if n_preserved:
            out_df = pd.concat([out_df, preserved], ignore_index=True)

    out_df.to_csv(targets_path, index=False, encoding="utf-8-sig")
    if n_preserved:
        print(f"타겟 파일 저장: {targets_path} (16번 통계 기준 {n_from_stats}건 갱신 + "
              f"아직 통계에 안 잡힌 기존 공연 {n_preserved}건 보존 = 총 {len(out_df)}건)")
    else:
        print(f"타겟 파일 저장: {targets_path} ({len(out_df)}건)")

    groups = {}
    matched = merged[merged["season_match_status"] == "matched"]
    for key, g in matched.groupby("work_group_key"):
        if len(g) < 2:
            continue
        members = [{
            "perf_id": row["perf_id"], "title": row["title"], "venue_name": row["venue_name"],
            "perf_start_date": str(row["perf_start_date"]), "perf_end_date": str(row["perf_end_date"]),
            "season_rank": row["season_rank"], "genre": row["genre"],
            "company_name": row.get("company_name", ""), "cast": row.get("cast_names", []),
            "is_high_demand": bool(row.get("is_high_demand", False)),
        } for _, row in g.iterrows()]
        rep = min(g["title"], key=len)
        groups[key] = {"representative_title": rep, "members": members}

    groups_path = os.path.join(args.out_dir, "work_groups.json")
    with open(groups_path, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)
    print(f"그룹 윈도우 저장: {groups_path} ({len(groups)}개 그룹, "
          f"{sum(len(v['members']) for v in groups.values())}개 공연 포함)")


# =============================================================================
# 1.5단계: sync-new-performances (주간 증분 수집용, 신규 공연만 추가)
# =============================================================================
#
# 기존 4,093개 카탈로그(및 그 시즌 매칭 825/3,268 결과)는 절대 재계산하지 않고
# 그대로 둔 채, KOPIS에 새로 올라온 공연만 targets_enriched.csv/work_groups.json에
# 덧붙인다. 시즌 그룹핑(초연/재연 매칭)은 원래 스크립트가 남아있지 않아 완전히
# 재현할 수는 없어서, 검증 결과 "완전 재현 74.2%, 오매칭(false merge) 사실상 없음"인
# 보수적 규칙(제목 정규화 + (제작사 일치 OR 캐스트 자카드 유사도 >= 0.4))만 쓴다.
# 애매하면 그냥 단독(unmatched)으로 남긴다 - 이 프로젝트 전체의 기본 철학과 동일하게
# recall보다 precision(오탐 없음)을 우선한다.

def _strip_trailing_brackets(title):
    """'회란기 [광주]' -> '회란기', '다이어리 [대구 (앵콜)]' -> '다이어리'.
    끝에 붙은 대괄호/괄호 블록을 (중첩돼도) 반복해서 제거."""
    t = (title or "").strip()
    prev = None
    while prev != t:
        prev = t
        t = re.sub(r"[\[(][^\[\]()]*[\])]\s*$", "", t).strip()
    return t


def _cast_jaccard(a, b):
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _parse_runtime_minutes(raw):
    """'2시간 10분' / '1시간 30분' / '90분' -> 분 단위 정수. 파싱 실패 시 None."""
    if not raw:
        return None
    s = str(raw)
    h = re.search(r"(\d+)\s*시간", s)
    m = re.search(r"(\d+)\s*분", s)
    if not h and not m:
        return None
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


def _safe_str(x):
    """pandas가 빈 CSV 필드를 NaN(float)으로 읽어들이는 경우까지 포함해서
    항상 문자열로 안전하게 변환한다. (2026-08-05: mt13id가 비어있는 신규
    공연상세 행에서 'float' object has no attribute 'strip' 크래시 발생해 추가)"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def cmd_sync_new_performances(args):
    existing = pd.read_csv(args.existing_targets, dtype=str, encoding="utf-8-sig")
    existing_ids = set(existing["perf_id"])

    groups = {}
    if args.existing_groups and os.path.isfile(args.existing_groups):
        with open(args.existing_groups, encoding="utf-8") as f:
            groups = json.load(f)

    perf_list = pd.read_csv(args.perf_list, dtype=str, encoding="utf-8-sig")
    new_list = perf_list[~perf_list["mt20id"].isin(existing_ids)].drop_duplicates("mt20id")
    if new_list.empty:
        print("새로 추가된 공연 없음 - targets_enriched.csv/work_groups.json 변경 없음.")
        return

    # 캐스트/제작사 강화 (build-targets와 동일한 헬퍼 재사용)
    detail_enrich = _load_detail_enrichment(args.detail)
    enrich_map = {}
    if detail_enrich is not None:
        for _, r in detail_enrich.iterrows():
            enrich_map[r["perf_id"]] = {"company_name": r["company_name"], "cast_names": r["cast_names"]}

    # 러닝타임/제작사코드는 02_공연상세 원본에서 별도로 뽑음 (mt13id 앞부분 = company_id)
    detail_extra = {}
    if args.detail and os.path.isfile(args.detail):
        raw = pd.read_csv(args.detail, dtype=str, encoding="utf-8-sig", low_memory=False)
        for _, r in raw.iterrows():
            pid = r.get("mt20id")
            if not pid or pid in detail_extra:
                continue
            mt13 = _safe_str(r.get("mt13id"))
            detail_extra[pid] = {
                "runtime_min": _parse_runtime_minutes(r.get("prfruntime")),
                "company_id": mt13.split("-")[0] if mt13 else "",
            }

    new_rows = []
    for _, r in new_list.iterrows():
        pid = r["mt20id"]
        extra = detail_extra.get(pid, {})
        enrich = enrich_map.get(pid, {"company_name": "", "cast_names": []})
        new_rows.append({
            "perf_id": pid,
            "title": r.get("prfnm", ""),
            "genre": r.get("genrenm", ""),
            "perf_start_date": _safe_str(r.get("prfpdfrom")).replace(".", "-"),
            "perf_end_date": _safe_str(r.get("prfpdto")).replace(".", "-"),
            "venue_name": r.get("fcltynm", ""),
            "runtime_min": extra.get("runtime_min"),
            "company_id": extra.get("company_id", ""),
            "company_name": enrich.get("company_name", "") or "",
            "cast_names_list": enrich.get("cast_names", []) or [],
        })

    matched_count = 0

    # 1) 기존 시즌 그룹에 재연/삼연으로 편입되는지 먼저 확인
    for row in new_rows:
        norm_title = normalize(_strip_trailing_brackets(row["title"]))
        best_key, best_group = None, None
        if norm_title:
            for key, g in groups.items():
                rep_norm = normalize(_strip_trailing_brackets(g.get("representative_title", "")))
                if rep_norm != norm_title:
                    continue
                rep_company = next((m.get("company_name") for m in g["members"] if m.get("company_name")), "")
                rep_cast = next((m.get("cast") for m in g["members"] if m.get("cast")), [])
                company_ok = bool(row["company_name"]) and bool(rep_company) and \
                    normalize(row["company_name"]) == normalize(rep_company)
                cast_ok = _cast_jaccard(row["cast_names_list"], rep_cast) >= 0.4
                if company_ok or cast_ok:
                    best_key, best_group = key, g
                    break
        if best_group is not None:
            next_rank = max((m.get("season_rank") or 0) for m in best_group["members"]) + 1
            best_group["members"].append({
                "perf_id": row["perf_id"], "title": row["title"], "venue_name": row["venue_name"],
                "perf_start_date": row["perf_start_date"], "perf_end_date": row["perf_end_date"],
                "season_rank": next_rank, "genre": row["genre"],
                "company_name": row["company_name"], "cast": row["cast_names_list"],
                "is_high_demand": False,
            })
            row["season_match_status"] = "matched"
            row["work_group_key"] = best_key
            row["season_rank"] = next_rank
            matched_count += 1
        else:
            row["season_match_status"] = "unmatched"
            row["work_group_key"] = ""
            row["season_rank"] = ""

    # 2) 아직 안 묶인 것들끼리 이번 배치 안에서 새 그룹 형성 시도
    #    (예: 이번 주에 같은 작품이 여러 지역 투어로 동시에 새로 올라온 경우)
    unmatched = [r for r in new_rows if r["season_match_status"] == "unmatched"]
    by_title = defaultdict(list)
    for r in unmatched:
        nt = normalize(_strip_trailing_brackets(r["title"]))
        if nt:
            by_title[nt].append(r)

    new_group_seq = 0
    for norm_title, rows in by_title.items():
        if len(rows) < 2:
            continue
        n = len(rows)
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for i in range(n):
            for j in range(i + 1, n):
                same_company = bool(rows[i]["company_name"]) and bool(rows[j]["company_name"]) and \
                    normalize(rows[i]["company_name"]) == normalize(rows[j]["company_name"])
                similar_cast = _cast_jaccard(rows[i]["cast_names_list"], rows[j]["cast_names_list"]) >= 0.4
                if same_company or similar_cast:
                    union(i, j)

        clusters = defaultdict(list)
        for i in range(n):
            clusters[find(i)].append(rows[i])

        for cluster_rows in clusters.values():
            if len(cluster_rows) < 2:
                continue  # 끝내 짝을 못 찾은 건 단독(unmatched)으로 남김
            cluster_rows.sort(key=lambda r: r["perf_start_date"] or "")
            new_group_seq += 1
            key = f"{norm_title}::new_{new_group_seq}"
            groups[key] = {"representative_title": min((r["title"] for r in cluster_rows), key=len), "members": []}
            for rank, r in enumerate(cluster_rows, start=1):
                groups[key]["members"].append({
                    "perf_id": r["perf_id"], "title": r["title"], "venue_name": r["venue_name"],
                    "perf_start_date": r["perf_start_date"], "perf_end_date": r["perf_end_date"],
                    "season_rank": rank, "genre": r["genre"],
                    "company_name": r["company_name"], "cast": r["cast_names_list"],
                    "is_high_demand": False,
                })
                r["season_match_status"] = "matched"
                r["work_group_key"] = key
                r["season_rank"] = rank
                matched_count += 1

    # 3) targets_enriched.csv에 append (ticket_sales/is_high_demand는 다음 전체
    #    stats 재수집 전까지는 알 수 없으므로 0/False로 시작 - collect 단계
    #    로직상 필수값이 아니라 안전함)
    append_df = pd.DataFrame([{
        "perf_id": r["perf_id"], "title": r["title"], "genre": r["genre"],
        "perf_start_date": r["perf_start_date"], "perf_end_date": r["perf_end_date"],
        "venue_name": r["venue_name"], "runtime_min": r["runtime_min"], "company_id": r["company_id"],
        "total_ticket_sales_qty": 0,
        "season_match_status": r["season_match_status"], "work_group_key": r["work_group_key"],
        "season_rank": r["season_rank"], "company_name": r["company_name"],
        "cast_names": "|".join(r["cast_names_list"]),
        "actual_group_size": 1, "runtime_missing": r["runtime_min"] in (None, ""),
        "is_recurring_title": False, "is_high_demand": False,
    } for r in new_rows])

    combined = pd.concat([existing, append_df], ignore_index=True)
    os.makedirs(os.path.dirname(args.out_targets) or ".", exist_ok=True)
    combined.to_csv(args.out_targets, index=False, encoding="utf-8-sig")

    os.makedirs(os.path.dirname(args.out_groups) or ".", exist_ok=True)
    with open(args.out_groups, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)

    print(f"새 공연 {len(new_rows)}건 발견 (그중 시즌 그룹 매칭 {matched_count}건, "
          f"단독 {len(new_rows) - matched_count}건 - 애매하면 안전하게 단독으로 남김)")
    print(f"타겟 파일 갱신: {args.out_targets} (총 {len(combined)}건)")
    print(f"그룹 파일 갱신: {args.out_groups}")
    print("주의: 티켓판매량/인기작 여부는 아직 반영 안 됨 - 다음 build-targets 전체 재실행 때 채워짐.")


# =============================================================================
# 2단계: collect (구 youtube_collect_targeted.py)
# =============================================================================

class PerMinuteRateLimitError(Exception):
    pass


class DailyQuotaExceededError(Exception):
    pass


class InvalidApiKeyError(Exception):
    """quota 초과가 아니라 키 자체가 무효한 경우 (삭제/비활성화된 키, 오타 등).
    재시도해봐야 소용없으니 바로 다음 키로 넘어가야 함."""
    pass


def robust_get(url, params, max_retries=3, timeout=20):
    import requests  # collect 커맨드에서만 필요 (prepare/merge/qa는 pandas만 있으면 됨)
    backoff = 3
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout):
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff)
            backoff *= 2
            continue
        if resp.status_code == 200:
            return resp
        if resp.status_code == 400:
            body = resp.text
            if "api key not valid" in body.lower() or "api_key_invalid" in body.lower():
                raise InvalidApiKeyError(body[:300])
            resp.raise_for_status()
        if resp.status_code == 403:
            body = resp.text
            if "per minute" in body.lower() or "rate limit exceeded" in body.lower():
                raise PerMinuteRateLimitError(body[:300])
            if "quota" in body.lower():
                raise DailyQuotaExceededError(body[:300])
            raise RuntimeError(f"403: {body[:300]}")
        if resp.status_code == 429:
            # 초당/분당 과다 요청. Retry-After 있으면 그만큼(최대 30초로 상한), 없으면 백오프.
            wait = backoff
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = max(wait, float(retry_after))
                except ValueError:
                    pass
            wait = min(wait, 45)  # 한 번에 너무 오래 안 기다리게 상한
            if attempt == max_retries - 1:
                raise PerMinuteRateLimitError(f"429 재시도 소진: {resp.text[:200]}")
            print(f"    [429] {wait:.0f}초 대기 후 재시도 ({attempt + 1}/{max_retries})", flush=True)
            time.sleep(wait)
            backoff *= 2
            continue
        if resp.status_code >= 500:
            if attempt == max_retries - 1:
                resp.raise_for_status()
            time.sleep(backoff)
            backoff *= 2
            continue
        resp.raise_for_status()
    raise RuntimeError("robust_get: 재시도 소진")


class KeyRotator:
    def __init__(self, keys):
        if not keys:
            raise ValueError("이 shard에 배정된 API 키가 없어요")
        self.keys = keys
        self.idx = 0

    @property
    def exhausted(self):
        """모든 키를 다 써봤는지(quota 소진이든 무효한 키든) - 호출부는 이걸
        먼저 확인해서, exhausted면 아예 API를 다시 안 부르고 바로 종료해야 함."""
        return self.idx >= len(self.keys)

    @property
    def current(self):
        # exhausted 상태에서 호출되면 IndexError 대신 None을 반환 (크래시 방지).
        # 호출부는 exhausted를 먼저 체크하는 게 정상 경로지만, 혹시 놓치더라도
        # 여기서 한 번 더 안전망 역할을 함.
        if self.exhausted:
            return None
        return self.keys[self.idx]

    def rotate(self):
        self.idx += 1
        if self.idx >= len(self.keys):
            return False
        print(f"  키 전환: {self.idx + 1}/{len(self.keys)}번째 키로 전환", flush=True)
        return True


def iter_date_windows(start_date, end_date, pre_buffer_days=30, post_buffer_days=120, window_days=30):
    """
    공연(또는 그룹 전체) 기간 - pre_buffer_days ~ 기간 + post_buffer_days 범위를
    window_days 단위로 쪼개서 (publishedAfter, publishedBefore) RFC3339 튜플을
    순서대로 반환한다. order=relevance/date 둘 다 사실상 상위 결과 위주로만
    반환되는 한계를, 기간을 잘게 쪼개 각 구간마다 별도로 검색해서 보완한다.
    파싱 실패 시 빈 리스트(호출부에서 "윈도우 없이 기존 방식대로" 처리).
    """
    from datetime import datetime, timedelta

    def _parse(s):
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.strptime(s[:10].replace(".", "-").replace("/", "-"), "%Y-%m-%d")
            except ValueError:
                continue
        return None

    start = _parse(start_date)
    end = _parse(end_date) or start
    if not start:
        return []

    cursor = start - timedelta(days=pre_buffer_days)
    limit = end + timedelta(days=post_buffer_days)
    from datetime import timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # start/end가 naive라 맞춰서 비교
    if limit > now:
        limit = now  # 미래 날짜는 검색해봐야 결과가 있을 수 없고 API 에러 유발 가능성도 있음
    windows = []
    while cursor < limit:
        chunk_end = min(cursor + timedelta(days=window_days), limit)
        windows.append((cursor.strftime("%Y-%m-%dT00:00:00Z"), chunk_end.strftime("%Y-%m-%dT00:00:00Z")))
        cursor = chunk_end
    return windows


def _fetch_search_page(rotator, params, query, per_minute_wait, fail_log_path=None, unit_id=None):
    """검색 1페이지(최대 50개)를 키 재시도/전환과 함께 가져온다.

    핵심 원칙: **429(분당 제한)는 절대 포기하지 않는다.** 이건 영구적인 문제가
    아니라 "지금 당장 너무 빨리 요청해서" 생기는 일시적 제한이라, 충분히
    기다리면 반드시 풀린다. 그래서 429는 키를 계속 돌려가며 무한정 재시도하고,
    한 바퀴(모든 키)를 다 돌았는데도 안 풀리면 대기 시간을 점점 늘려가며
    (최대 5분) 다시 처음부터 돈다 - "시간이 걸리더라도 결국은 찾는다"는
    원칙. 안전장치로 20바퀴(수백 번 시도)까지만 허용하고, 그 이상은 정말
    비정상 상황(예: API 자체 장애)으로 보고 그때는 포기한다.

    반면 quota 소진/무효한 키/파라미터 에러(400)는 아무리 기다려도 저절로
    안 풀리는 문제라 - 이런 것들은 기존처럼 빠르게 판단해서 처리한다.

    반환:
      (None, None)             - 이 shard의 키를 전부 시도했는데 다 안 됨
                                  (quota소진/전부 무효, 또는 429가 20바퀴를
                                  돌아도 안 풀리는 이상 상황). 호출부는
                                  지금까지 모은 페이지만이라도 쓰고 멈춰야 함.
      ([], None)                - 이 페이지 자체는 실패(400 등)했지만
                                  전체를 멈출 이유는 아님 - 그냥 이 페이지만 포기.
      (items, next_page_token)  - 정상 (items가 빈 리스트일 수도 있음, 정상).

    fail_log_path: 결국 포기한 검색어를 여기에 기록 (recall 손실 지점을
                   나중에 확인할 수 있게).
    """
    import requests
    if rotator.exhausted:
        return None, None

    MAX_FULL_CYCLES = 3  # 이 이상은 API 자체 장애 등 비정상 상황으로 간주
    # (기존 20에서 축소 - 실측 결과 20바퀴까지 기다리는 동안 IP 기반으로 보이는
    # 분당 제한이 전혀 안 풀리는 경우가 흔해서, 검색어 하나가 shard의 남은
    # 시간 예산을 통째로 먹어버리는 사고가 반복 확인됨. 3바퀴(최소 52+70+88=210초
    # 대기, 키 37개 기준 최대 111회 시도)까지만 기다리고 포기 -> failed_queries.txt에
    # 기록해서 retry-failed 단계(별도 40분 예산, 다른 시간대)에서 다시 시도하게 함.
    rate_limit_hits_this_cycle = 0
    full_cycles = 0

    while True:
        if rotator.exhausted:
            return None, None
        p = dict(params, key=rotator.current)
        try:
            resp = robust_get(f"{API_BASE}/search", p)
            data = resp.json()
            return data.get("items", []), data.get("nextPageToken")
        except PerMinuteRateLimitError:
            rate_limit_hits_this_cycle += 1
            print(f"  [429/분당제한] '{query}' - 키 전환 ({rate_limit_hits_this_cycle}번째 시도, "
                  f"{full_cycles + 1}바퀴째)", flush=True)
            if rotator.rotate():
                # 키를 바꿔도 같은 프로젝트 밑에 묶여있으면 quota가 공유될 수 있어서
                # 즉시 재요청하면 또 걸리기 쉬움 - 짧게라도 텀을 두고 재시도
                time.sleep(1.5)
                continue
            # 한 바퀴(모든 키)를 다 돌았는데도 안 풀림 -> 포기하지 않고, 대신
            # 바퀴를 돌 때마다 대기 시간을 늘려가며(최대 5분) 처음부터 다시 시도
            full_cycles += 1
            if full_cycles >= MAX_FULL_CYCLES:
                print(f"  {MAX_FULL_CYCLES}바퀴를 다 돌아도 안 풀려요 - 비정상 상황으로 보고 포기합니다.", flush=True)
                break
            wait = min(per_minute_wait * (1 + full_cycles * 0.5), 300)
            print(f"  {full_cycles}바퀴째 - {wait:.0f}초 대기 후 처음 키로 재시도 (포기 안 함).", flush=True)
            time.sleep(wait)
            rotator.idx = 0
            rate_limit_hits_this_cycle = 0
        except DailyQuotaExceededError:
            if not rotator.rotate():
                print("  이 shard에 배정된 키를 모두 소진했어요.", flush=True)
                return None, None
        except InvalidApiKeyError:
            print("  [무효한 키] 유효하지 않아요 - 다음 키로 전환", flush=True)
            if not rotator.rotate():
                print("  이 shard에 배정된 키를 모두 시도했는데 다 무효해요.", flush=True)
                return None, None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # robust_get이 자체 재시도(3번)를 이미 다 써보고도 못 붙은 경우 -
            # 네트워크 순단은 quota/키 문제와 달리 완전히 별개 원인이라, 이
            # 페이지만 건너뛰고 계속 진행한다 (여기서 못 잡으면 shard 전체가
            # 죽는 심각한 문제였음 - 실제로 재현 확인함).
            print(f"  [네트워크 오류] '{query}': {e} - 이 페이지만 건너뜀", flush=True)
            return [], None
        except requests.exceptions.HTTPError as e:
            # 400 등 재시도해도 소용없는(파라미터 자체가 문제인) 에러는 이 페이지
            # 1건만 건너뛰고 계속 진행한다 - shard 전체가 죽으면 안 됨.
            status = e.response.status_code if e.response is not None else "?"
            body = e.response.text[:200] if e.response is not None else str(e)
            print(f"  [HTTP {status}] '{query}': {body} - 이 페이지만 건너뜀", flush=True)
            return [], None

    print(f"  [포기] '{query}' {MAX_FULL_CYCLES}바퀴를 다 돌아도 실패, 건너뜀", flush=True)
    if fail_log_path:
        try:
            os.makedirs(os.path.dirname(fail_log_path) or ".", exist_ok=True)
            # 탭/줄바꿈으로 컬럼을 구분하는 포맷이라, 제목에 이런 문자가 섞여
            # 들어오면(예전에 KOPIS 원본에 제어문자 섞인 사례가 실제 있었음)
            # 줄이 깨져서 retry-failed 파싱이 틀어질 수 있음 - 미리 안전하게 치환.
            def _safe(s):
                return (s or "").replace("\t", " ").replace("\n", " ").replace("\r", " ")
            with open(fail_log_path, "a", encoding="utf-8") as f:
                f.write("\t".join([
                    _safe(unit_id), _safe(query), _safe(params.get("order", "relevance")),
                    _safe(params.get("videoDuration", "")), _safe(params.get("publishedAfter", "")),
                    _safe(params.get("publishedBefore", "")),
                ]) + "\n")
        except Exception:
            pass  # 로그 실패는 전체 흐름을 막을 이유가 아님
    return [], None


def search_with_retry(rotator, query, max_results=50, per_minute_wait=35,
                       order="relevance", video_duration=None,
                       published_after=None, published_before=None,
                       max_pages=10, fail_log_path=None, unit_id=None):
    """검색 결과를 페이지네이션으로 가져온다. 1페이지(최대 50개)가 꽉 차면
    2페이지째도 마저 가져오고, 2페이지도 꽉 차면 3페이지째... 이런 식으로
    "결과가 50개 미만으로 나올 때까지" 또는 max_pages(기본 10, 최대 500개)에
    닿을 때까지 이어서 가져온다.

    결과가 50개가 안 되는(대다수일 것으로 예상되는) 쿼리는 추가 비용 없이
    1페이지로 끝나고, 정말 결과가 넘치는 인기 검색어만 추가 페이지 비용
    (페이지당 100 units)을 더 쓰는 구조라 quota를 필요한 곳에만 쓴다.

    order: "relevance" 또는 "date" (date를 병행하면 인기/조회수가 낮아
    relevance 상위에 안 뜨는 영상도 잡을 수 있음)
    video_duration: None 또는 "short" (쇼츠 누락 방지용 별도 패스에 사용)
    published_after/published_before: RFC3339 문자열 (날짜 윈도우 검색용)
    max_pages: 안전장치 - 아무리 꽉 차도 이 페이지 수 이상은 더 안 가져옴
               (기본 10페이지=최대 500개. 극단적으로 인기 많은 검색어 하나가
               quota를 통째로 먹는 것을 방지)
    fail_log_path: 여러 키를 다 돌려봐도 결국 포기한 검색어를 기록할 파일 경로
                   (recall 손실 지점을 나중에 확인/재시도할 수 있게)
    """
    base = {"part": "snippet", "q": query, "type": "video",
            "maxResults": min(max_results, 50), "relevanceLanguage": "ko", "order": order}
    if video_duration:
        base["videoDuration"] = video_duration
    if published_after:
        base["publishedAfter"] = published_after
    if published_before:
        base["publishedBefore"] = published_before

    if rotator.exhausted:
        return None  # 이전 쿼리에서 이미 모든 키를 다 써봤음 - 재시도해봐야 소용없음

    all_items = []
    next_page_token = None
    for _ in range(max_pages):
        params = dict(base)
        if next_page_token:
            params["pageToken"] = next_page_token
        items, token = _fetch_search_page(rotator, params, query, per_minute_wait,
                                           fail_log_path=fail_log_path, unit_id=unit_id)
        if items is None:
            # 키 전부 소진 - 이미 모은 페이지가 있으면 그거라도 살려서 반환,
            # 첫 페이지부터 안 됐으면 기존과 동일하게 None(shard 전체 종료 신호)
            return all_items if all_items else None
        all_items.extend(items)
        if len(items) < 50 or not token:
            break  # 결과가 덜 찼거나 다음 페이지가 없으면 여기서 끝
        next_page_token = token
        time.sleep(0.3)  # 페이지 사이에도 살짝 텀
    return all_items



def _get_with_rotation(rotator, url, params_base, max_key_tries=6, label=""):
    """robust_get을 키 로테이션과 함께 감싸는 공통 헬퍼.
    - PerMinuteRateLimitError(429): 다른 키로 전환해서 재시도 (search 쪽처럼
      끝까지 포기 안 하진 않지만, 최소 max_key_tries번은 시도)
    - DailyQuotaExceededError/InvalidApiKeyError: 다음 키로 전환해서 재시도
    - 네트워크 순단(ConnectionError/Timeout): 이 호출만 건너뛰고 None 반환
    - 그 외 예상 못 한 HTTP 에러: 이 호출만 건너뛰고 None 반환 (전체가 죽으면 안 됨)
    - 키를 다 써도 안 되면 None 반환
    """
    import requests
    if rotator.exhausted:
        return None
    for _ in range(min(len(rotator.keys), max_key_tries)):
        params = dict(params_base, key=rotator.current)
        try:
            return robust_get(url, params)
        except PerMinuteRateLimitError:
            print(f"  [{label}] [429/분당제한] 키 전환", flush=True)
            if rotator.rotate():
                time.sleep(1.5)
                continue
            print(f"  [{label}] 이 shard에 배정된 키를 모두 돌려봤는데도 막혀요.", flush=True)
            return None
        except DailyQuotaExceededError:
            if not rotator.rotate():
                print(f"  [{label}] 이 shard에 배정된 키를 모두 소진했어요.", flush=True)
                return None
        except InvalidApiKeyError:
            if not rotator.rotate():
                print(f"  [{label}] 이 shard에 배정된 키를 모두 시도했는데 다 무효해요.", flush=True)
                return None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # robust_get 자체 재시도(3번)를 다 써보고도 못 붙은 네트워크 순단 -
            # HTTPError와 다른 예외 타입이라 안 잡으면 그대로 크래시났었음(실제 발견함).
            print(f"  [{label}] [네트워크 오류] {e} - 이 호출만 건너뜀", flush=True)
            return None
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            body = e.response.text[:200] if e.response is not None else str(e)
            print(f"  [{label}] [HTTP {status}] {body} - 이 호출만 건너뜀", flush=True)
            return None
    return None


def videos_list(rotator, video_ids):
    out = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        params = {"part": "contentDetails,statistics", "id": ",".join(chunk)}
        resp = _get_with_rotation(rotator, f"{API_BASE}/videos", params, label="videos.list")
        if resp is None:
            continue  # 이 50개 묶음은 건너뛰고 나머지는 계속 진행
        for item in resp.json().get("items", []):
            out[item["id"]] = item
    return out


def channels_list(rotator, channel_ids, cache):
    """채널 구독자수/영상수. 이미 캐시에 있는 채널은 다시 안 부름 (같은 채널이
    여러 영상에 겹치는 경우가 많아서 quota 절약)."""
    new_ids = [c for c in dict.fromkeys(channel_ids) if c and c not in cache]
    for i in range(0, len(new_ids), 50):
        chunk = new_ids[i:i + 50]
        params = {"part": "statistics", "id": ",".join(chunk)}
        resp = _get_with_rotation(rotator, f"{API_BASE}/channels", params, label="channels.list")
        if resp is None:
            continue
        for item in resp.json().get("items", []):
            cache[item["id"]] = item.get("statistics", {})
    return cache


GENRE_HASHTAG_PREFIXES = ["연극", "뮤지컬", "무용", "발레", "오페라", "국악", "콘서트", "클래식"]


def build_queries(title, genre, venue_name, is_group=False, inst_name=None,
                   cast_names=None, company_name=None):
    title = (title or "").strip()
    if not title:
        return []
    venue_c = venue_core(venue_name)
    queries = [
        title, f"{title} 쇼츠", f"{title} 리뷰", f"{title} 하이라이트", f"{title} 커튼콜",
        f"{title} 직캠", f"{title} 무대인사", f"{title} 실황",
        f"{title} 프레스콜", f"{title} 연습영상", f"{title} 인터뷰",
        f"{title} 트레일러", f"{title} 뮤직비디오",
        f"{title} 티저", f"{title} 브이로그", f"{title} 비하인드", f"{title} 메이킹", f"{title} 쇼케이스",
        # 논문 IV(축약/스포일러 콘텐츠) 자체가 연구 대상이라 이 카테고리는 반드시 폭넓게 잡아야 함
        f"{title} 결말", f"{title} 스포", f"{title} 요약",
    ]
    if venue_c and venue_c != title:
        queries.append(f"{title} {venue_c}")
    if "뮤지컬" in (genre or ""):
        queries.append(f"{title} 넘버")
        queries.append(f"{title} musical")  # 해외 진출작/글로벌 팬 대상 영어 홍보 콘텐츠 대비
    if "무용" in (genre or "") or "발레" in (genre or ""):
        queries.append(f"{title} 공연")
        # 실사례 확인: KOPIS 제목은 "레미제라블 [과천]"인데 실제 창작발레
        # 버전은 "춤으로 모두 무용! <창작발레 레미제라블>"처럼 검색/구매 시
        # "창작발레"/"창작무용"이라는 표현을 앞세워 부르는 경우가 많음
        queries.append(f"창작발레 {title}")
        queries.append(f"창작무용 {title}")
    # 실사례 확인("청사초롱 불 밝혀라"): 정식 공연 전에 투자/관객 반응을 보려고
    # 낭독공연(리딩쇼케이스) 형태로 먼저 올리는 경우가 흔함 - 별도 쿼리 필요
    queries.append(f"{title} 낭독공연")
    queries.append(f"{title} 낭독회")
    queries.append(f"{title} 풀버전")
    queries.append(f"{title} 발췌")
    queries.append(f"{title} 갈라")
    queries.append(f"{title} 개막")
    queries.append(f"{title} 폐막")
    queries.append(f"{title} 앙코르")
    # 기관명 쿼리: 그룹(초연/재연)이든 단독 공연이든 상관없이 항상 포함.
    # 특히 "봄"/"결"/"판"처럼 짧고 흔한 제목은 기관명 없이는 검색 자체가
    # 무관한 결과로 뒤덮여서, 이 쿼리가 사실상 유일한 실효 검색어가 됨.
    if inst_name:
        queries.append(f"{inst_name} {title}")
    if company_name and company_name != inst_name:
        queries.append(f"{title} {company_name}")
    # 배우명은 상위 2명까지만 - 제목과 함께 검색하므로("{제목} {배우명}") 그
    # 배우가 나온 "다른" 작품 영상이 섞일 위험은 낮음(제목 텍스트도 같이 검색됨).
    # 검색 결과를 넓히는 용도일 뿐, 매칭 게이트는 여전히 제목 텍스트 기준.
    for name in (cast_names or [])[:2]:
        queries.append(f"{title} {name}")
    # 실사용자들이 "#연극유령"처럼 장르+제목을 띄어쓰기 없이 붙여 해시태그로
    # 쓰는 경우가 많음 -> 그 표기 그대로도 쿼리에 추가 (예: "연극유령")
    for prefix in GENRE_HASHTAG_PREFIXES:
        if prefix in (genre or ""):
            queries.append(f"{prefix}{title}")
            break
    return list(dict.fromkeys(queries))


def assign_to_member(published_at, members, video_text=""):
    pub = _parse_dt(published_at)
    video_text_norm = normalize(video_text) if video_text else ""

    def _cast_hit_member():
        """캐스트 이름이 영상 텍스트에 등장하는 멤버를 찾는다. 여러 멤버가 걸리면
        (배우 재출연 등) 애매하므로 단독으로 걸리는 경우만 신뢰한다."""
        if not video_text_norm:
            return None
        hit_members = [
            m for m in members
            if any(len(n) >= 2 and normalize(n) in video_text_norm for n in (m.get("cast") or []))
        ]
        return hit_members[0] if len(hit_members) == 1 else None

    if pub is None:
        cast_hit = _cast_hit_member()
        if cast_hit:
            return cast_hit["perf_id"], "other_season"
        return members[0]["perf_id"], "unknown"

    date_hits = []
    for m in members:
        start, end = _parse_dt(m["perf_start_date"]), _parse_dt(m["perf_end_date"])
        if start and end and start <= pub <= end:
            date_hits.append(m)
    if len(date_hits) == 1:
        return date_hits[0]["perf_id"], "current_season"
    if len(date_hits) > 1:
        # 날짜 범위가 겹치는 멤버가 여럿이면(드묾) 캐스트로 재확인, 안 되면 첫 번째
        cast_hit = _cast_hit_member()
        if cast_hit and cast_hit in date_hits:
            return cast_hit["perf_id"], "current_season"
        return date_hits[0]["perf_id"], "current_season"

    # 날짜 범위 안 걸리는 경우: 캐스트 매칭을 날짜 최근접보다 먼저 확인
    # (초연/재연이 오래 전이어도 배우가 명시돼 있으면 그게 더 확실한 신호)
    cast_hit = _cast_hit_member()
    if cast_hit:
        return cast_hit["perf_id"], "other_season"

    best, best_dist = None, None
    for m in members:
        start, end = _parse_dt(m["perf_start_date"]), _parse_dt(m["perf_end_date"])
        if start is None or end is None:
            continue
        dist = min(abs((pub - start).days), abs((pub - end).days))
        if best_dist is None or dist < best_dist:
            best, best_dist = m, dist
    if best is not None and best_dist is not None and best_dist <= 90:
        return best["perf_id"], "other_season"
    return members[0]["perf_id"], "unknown"


def _load_csv_rows(path):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _load_id_set(path):
    """path가 없거나 안 넘어오면 빈 set. excluded_video_ids.txt는 선택사항."""
    if not path or not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def _append_line(path, text):
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def _write_csv_row(path, fieldnames, row):
    is_new = not os.path.isfile(path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def build_search_units(targets, groups):
    """검색 단위 생성. 그룹(멤버 2개 이상)은 1개 단위, 나머지는 perf_id 단위.
    -> KOPIS 카탈로그 전체가 어떤 형태로든 반드시 커버됨(초연/재연 포함)."""
    grouped_perf_ids = set()
    units = []
    for key, g in groups.items():
        if len(g["members"]) < 2:
            continue
        for m in g["members"]:
            grouped_perf_ids.add(m["perf_id"])
        # 쿼리에 쓸 캐스트: 대표작(첫 멤버) 캐스트 우선, 없으면 다른 멤버에서 보충
        rep_cast = next((m.get("cast") for m in g["members"] if m.get("cast")), [])
        rep_company = next((m.get("company_name") for m in g["members"] if m.get("company_name")), "")
        # 그룹 내 멤버 중 하나라도 인기작이면 그룹 전체를 인기작 취급
        # (초연이 대박났으면 재연 검색도 결과가 넘칠 가능성이 높음)
        is_high_demand = any(m.get("is_high_demand") for m in g["members"])
        units.append({
            "unit_id": f"group::{key}", "is_group": True,
            "title": g["representative_title"], "genre": None,
            "venue_name": g["members"][0]["venue_name"], "members": g["members"],
            "inst_name": split_institution_and_work(g["members"][0]["title"])[0],
            "cast_names": rep_cast, "company_name": rep_company,
            "is_high_demand": is_high_demand,
        })
    for row in targets:
        if row["perf_id"] in grouped_perf_ids:
            continue
        cast = (row.get("cast_names") or "").split("|") if row.get("cast_names") else []
        is_recurring = str(row.get("is_recurring_title", "")).strip().lower() == "true"
        is_high_demand = str(row.get("is_high_demand", "")).strip().lower() == "true"
        units.append({
            "unit_id": f"perf::{row['perf_id']}", "is_group": False,
            "title": row["title"], "genre": row.get("genre"), "venue_name": row.get("venue_name"),
            "members": [{
                "perf_id": row["perf_id"], "title": row["title"], "venue_name": row.get("venue_name"),
                "perf_start_date": row.get("perf_start_date"), "perf_end_date": row.get("perf_end_date"),
                "genre": row.get("genre"), "cast": cast, "company_name": row.get("company_name", ""),
                "is_recurring_title": is_recurring,
            }],
            "inst_name": None,
            "cast_names": cast, "company_name": row.get("company_name", ""),
            "is_high_demand": is_high_demand,
        })
    return units


def _parse_iso8601_duration(s):
    if not s:
        return ""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s)
    if not m:
        return ""
    h, mi, se = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mi * 60 + se


VIDEO_ROW_FIELDNAMES = [
    "video_id", "video_title", "description", "channel_id", "channel_name",
    "channel_subscriber_count", "channel_video_count",
    "published_at", "duration_sec", "video_format",
    "view_count", "like_count", "comment_count", "video_url",
    "matched_perf_id", "matched_title", "matched_genre", "season_match",
    "substring_hit", "quote_hit", "venue_match", "date_match", "actor_match", "company_match", "is_news",
    "gate_keep",
]


def _process_search_items(items, unit, seen_ids, excluded_ids):
    """검색 결과 items를 성과(perf_id) 배정 + 신호 계산까지 마쳐서
    (video_id, snippet, target_perf_id, member, season_match, signals) 튜플
    리스트로 반환한다. cmd_collect와 retry-failed가 공유해서 쓴다."""
    kept = []
    skipped_no_id = 0
    for item in items:
        # type=video로 요청해도 YouTube API가 드물게 videoId 없는 항목을
        # 섞어 보내는 경우가 실제로 있음(삭제/지역제한 등) - 여기서 죽으면
        # 이 shard가 그때까지 모은 데이터를 통째로 날리므로 건너뛰고 계속함.
        vid = item.get("id", {}).get("videoId")
        if not vid:
            skipped_no_id += 1
            continue
        if vid in seen_ids or vid in excluded_ids:
            continue
        seen_ids.add(vid)
        sn = item["snippet"]
        video_text = f"{sn.get('title', '')} {sn.get('description', '')}"
        target_perf_id, season_match = (
            assign_to_member(sn.get("publishedAt"), unit["members"], video_text=video_text)
            if unit["is_group"] else (unit["members"][0]["perf_id"], "current_season")
        )
        member = next(m for m in unit["members"] if m["perf_id"] == target_perf_id)
        signals = compute_signals(
            perf={"title": member["title"], "venue_name": member["venue_name"],
                  "perf_start_date": member["perf_start_date"], "perf_end_date": member["perf_end_date"],
                  "cast": member.get("cast"), "company_name": member.get("company_name"),
                  "is_recurring_title": member.get("is_recurring_title", False)},
            video_title=sn.get("title", ""), description=sn.get("description", ""),
            channel_name=sn.get("channelTitle", ""), published_at=sn.get("publishedAt"),
        )
        # 여기서 discard하지 않음 - keep 여부는 gate_keep 컬럼에만 기록하고
        # 실제 필터링은 apply-gate 서브커맨드에서 별도로 수행한다.
        kept.append((vid, sn, target_perf_id, member, season_match, signals))
    if skipped_no_id:
        print(f"  [videoId 없음] {skipped_no_id}건 건너뜀 (API가 이상 항목을 섞어 보냄)", flush=True)
    return kept


def _write_kept_rows(kept, rotator, channel_cache, videos_path):
    """kept 튜플 리스트를 videos.list/channels.list로 메타데이터까지 채워서
    videos_path에 이어쓴다 (cmd_collect와 retry-failed가 공유). 갱신된
    channel_cache를 반환한다 (호출부가 이어서 재사용할 수 있도록)."""
    if not kept:
        return channel_cache
    meta = videos_list(rotator, [v[0] for v in kept])
    channel_cache = channels_list(rotator, [sn.get("channelId", "") for _, sn, *_ in kept], channel_cache)
    for vid, sn, target_perf_id, member, season_match, signals in kept:
        m = meta.get(vid, {})
        ch_id = sn.get("channelId", "")
        ch_stats = channel_cache.get(ch_id, {})
        duration_sec = _parse_iso8601_duration(m.get("contentDetails", {}).get("duration", ""))
        _write_csv_row(videos_path, VIDEO_ROW_FIELDNAMES, {
            "video_id": vid, "video_title": sn.get("title", ""),
            "description": sn.get("description", ""), "channel_id": ch_id,
            "channel_name": sn.get("channelTitle", ""),
            "channel_subscriber_count": ch_stats.get("subscriberCount", ""),
            "channel_video_count": ch_stats.get("videoCount", ""),
            "published_at": sn.get("publishedAt", ""),
            "duration_sec": duration_sec,
            "video_format": "shorts" if (duration_sec and duration_sec <= 60) else "general",
            "view_count": m.get("statistics", {}).get("viewCount", ""),
            "like_count": m.get("statistics", {}).get("likeCount", ""),
            "comment_count": m.get("statistics", {}).get("commentCount", ""),
            "video_url": f"https://www.youtube.com/watch?v={vid}",
            "matched_perf_id": target_perf_id, "matched_title": member["title"],
            "matched_genre": member.get("genre", ""),
            "season_match": season_match, "substring_hit": signals["substring_hit"],
            "quote_hit": signals["quote_hit"], "venue_match": signals["venue_match"],
            "date_match": signals["date_match"], "actor_match": signals["actor_match"],
            "company_match": signals["company_match"],
            "is_news": signals["is_news"],
            "gate_keep": signals["keep"],
        })
    return channel_cache


def cmd_collect(args):
    start_time = time.time()

    def _time_up():
        return (time.time() - start_time) / 60 >= args.time_budget_minutes

    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    rotator = KeyRotator(keys)

    targets = _load_csv_rows(args.targets)
    groups = {}
    if args.groups and os.path.isfile(args.groups):
        with open(args.groups, encoding="utf-8") as f:
            groups = json.load(f)
    excluded_ids = _load_id_set(args.excluded_ids)  # 없으면 빈 set, 정상 진행
    processed = _load_id_set(args.state_file)

    units = build_search_units(targets, groups)
    if args.shard_count:
        units = [u for i, u in enumerate(units) if i % args.shard_count == args.shard_index]
    units = [u for u in units if u["unit_id"] not in processed]
    if args.limit:
        units = units[:args.limit]

    print(f"이번 shard 처리 대상: {len(units)}개 검색 단위 ({len(keys)}개 키 배정됨), "
          f"시간 예산 {args.time_budget_minutes}분", flush=True)

    videos_path = os.path.join(args.out_dir, "videos.csv")
    channel_cache = {}  # channel_id -> statistics dict. shard 전체에서 재사용해 quota 절약.

    total_kept = 0
    orders_by_strategy = {"relevance": ["relevance"], "date": ["date"], "both": ["relevance", "date"]}
    orders = orders_by_strategy[args.order_strategy]

    for unit in units:
        # GitHub Actions의 job 시간 제한(보통 6시간)에 걸려 강제 종료되면, 그
        # 시점까지 모은 데이터를 커밋하는 단계까지 통째로 못 돌 위험이 있다.
        # 그래서 그보다 여유 있게 스스로 먼저 멈춰서, 뒤에 있는 커밋 단계가
        # 정상적으로 실행되게 한다 (429가 오래 지속되는 유닛 때문에 시간이
        # 예상보다 훨씬 오래 걸릴 수 있어서 특히 중요함).
        if _time_up():
            print(f"시간 예산({args.time_budget_minutes}분) 소진 - 안전하게 종료합니다 "
                  f"(다음 실행에서 이어감).", flush=True)
            break

        parsed_inst, parsed_work_title = split_institution_and_work(unit["title"])
        query_title = parsed_work_title or unit["title"]
        query_inst = unit.get("inst_name") or parsed_inst
        queries = build_queries(query_title, unit["genre"], unit["venue_name"],
                                 is_group=unit["is_group"], inst_name=query_inst,
                                 cast_names=unit.get("cast_names"), company_name=unit.get("company_name"))

        # 인기작(티켓판매량 상위 10%)은 검색 결과 자체가 넘칠 가능성이 높아서
        # (order=relevance 상위권 경쟁이 치열함) 자원을 더 몰아준다:
        # order=both를 강제 적용하고, 페이지도 평소보다 2배 더 받아온다.
        is_high_demand = unit.get("is_high_demand", False)
        unit_orders = orders_by_strategy["both"] if is_high_demand else orders
        unit_max_pages = args.max_pages * 2 if is_high_demand else args.max_pages

        windows = [(None, None)]
        if args.use_date_windows:
            starts = [m.get("perf_start_date") for m in unit["members"] if m.get("perf_start_date")]
            ends = [m.get("perf_end_date") for m in unit["members"] if m.get("perf_end_date")]
            if starts and ends:
                dw = iter_date_windows(
                    min(starts), max(ends), pre_buffer_days=args.pre_buffer_days,
                    post_buffer_days=args.post_buffer_days, window_days=args.window_days,
                )
                if dw:
                    windows = dw

        # 검색 콜 목록: (query, order, video_duration, published_after, published_before)
        calls = [(q, order, None, pa, pb) for q in queries for order in unit_orders for pa, pb in windows]
        if args.include_shorts_pass:
            # 쇼츠 누락 방지용 별도 패스는 대표 제목 하나로, 날짜 윈도우 없이 1회만
            calls.append((unit["title"], "relevance", "short", None, None))

        print(f"[{unit['unit_id']}] 쿼리 {len(queries)}개 x 검색콜 {len(calls)}개 "
              f"(order={'both(인기작)' if is_high_demand else args.order_strategy}, "
              f"windows={len(windows) if args.use_date_windows else 0}, "
              f"shorts_pass={args.include_shorts_pass}, max_pages={unit_max_pages})", flush=True)
        seen_ids, kept = set(), []
        unit_start_time = time.time()
        unit_completed = True

        for q, order, video_duration, pub_after, pub_before in calls:
            if _time_up():
                print(f"시간 예산 소진 - '{unit['unit_id']}' 처리 도중이지만 안전하게 중단합니다.", flush=True)
                print("  (이 유닛은 미완료 처리라 처리기록에 안 남기고, 지금까지 모은 것만 저장 -> 다음 실행에서 이 유닛부터 다시 처리)", flush=True)
                channel_cache = _write_kept_rows(kept, rotator, channel_cache, videos_path)
                total_kept += len(kept)
                print(f"완료(시간 예산으로 조기 종료). 이 shard 총 수집: {total_kept}건", flush=True)
                return
            if time.time() - unit_start_time >= args.max_seconds_per_unit:
                # 429가 이 유닛 안에서 계속 안 풀려서(IP 기반 제한으로 보임) 검색어
                # 하나하나가 몇 분씩 걸리는 상황 - shard의 남은 시간 예산을 이
                # 유닛 하나가 다 먹어버리기 전에 포기하고 다음 유닛으로 넘어간다.
                # processed_units.txt에 안 남기므로 다음 실행에서 이 유닛은 처음부터
                # 다시 처리됨 (지금까지 모은 부분 결과는 아래에서 저장은 함).
                print(f"  이 유닛 처리 시간이 {args.max_seconds_per_unit}초를 넘었어요 - "
                      f"429가 계속 안 풀리는 것 같아 포기하고 다음 유닛으로 넘어갑니다.", flush=True)
                unit_completed = False
                break
            items = search_with_retry(
                rotator, q, args.max_videos_per_query,
                order=order, video_duration=video_duration,
                published_after=pub_after, published_before=pub_before,
                max_pages=unit_max_pages,
                fail_log_path=os.path.join(args.out_dir, "failed_queries.txt"),
                unit_id=unit["unit_id"],
            )
            if items is None:
                print("전체 키 소진 - 지금까지 이 유닛에서 모은 것만 저장하고 종료 (다음 실행에서 이어감)", flush=True)
                channel_cache = _write_kept_rows(kept, rotator, channel_cache, videos_path)
                total_kept += len(kept)
                print(f"완료(quota 소진으로 조기 종료). 이 shard 총 수집: {total_kept}건", flush=True)
                return
            # 고정 간격이면 20개 shard의 요청 타이밍이 우연히 겹치기 쉬워서,
            # 살짝 무작위성을 줘서 서로 어긋나게 함 (같은 프로젝트 밑에 여러
            # 키가 몰려있으면 quota가 공유될 수 있어서 이게 은근히 도움이 됨)
            time.sleep(args.query_delay + random.uniform(0, args.query_delay * 0.5))
            kept.extend(_process_search_items(items, unit, seen_ids, excluded_ids))

        channel_cache = _write_kept_rows(kept, rotator, channel_cache, videos_path)

        total_kept += len(kept)
        gate_pass = sum(1 for *_, signals in kept if signals["keep"])
        print(f"  -> 이 유닛 수집: {len(kept)}건 (게이트통과 참고치: {gate_pass}건) (누적수집: {total_kept}건)", flush=True)

        if unit_completed:
            _append_line(args.state_file, unit["unit_id"])
        else:
            print(f"  ('{unit['unit_id']}'은 미완료 처리라 처리기록에 안 남김 - 다음 실행에서 처음부터 다시 처리)", flush=True)

    print(f"완료. 이 shard 총 수집(게이트 미적용, 전량 저장): {total_kept}건", flush=True)
    fail_log = os.path.join(args.out_dir, "failed_queries.txt")
    if os.path.isfile(fail_log):
        with open(fail_log, encoding="utf-8") as f:
            n_failed = sum(1 for _ in f)
        print(f"참고: 키를 다 돌려봐도 실패해서 포기한 검색어 {n_failed}건 -> {fail_log}", flush=True)


# =============================================================================
# 4.5단계: apply-gate (신규) - 수집과 분리된 필터링 단계
# =============================================================================
# cmd_collect()는 더 이상 게이트에서 discard하지 않고 검색된 걸 전량 저장한다.
# 실제 "쓸만한 영상만 추리기"는 여기서 별도로 수행해서, 언제든 다른 기준으로
# 재필터링할 수 있게 한다 (원본 raw 데이터는 항상 보존됨).
#
# --mode strict (기본): 기존 collect 내장 게이트와 동일한 기준
#   (text_match AND (date_match OR venue_match), 뉴스채널은 date_match 필수)
# --mode text-only: 날짜/장소 매칭 없이 text_match(substring_hit OR quote_hit)만
#   있으면 통과. 시즌그룹(초연/재연)이 원래 기간과 멀리 떨어진 시점에 영상이
#   올라온 경우를 놓치지 않기 위한 완화 모드. 뉴스채널은 여전히 date_match 필수
#   (뉴스는 다른 소재가 섞일 확률이 높아서 이 기준까지 풀면 오탐이 급증함).

def cmd_retry_failed(args):
    """collect 중 20바퀴를 다 돌아도 실패해서 failed_queries.txt에 남은
    검색어들을 다시 시도한다. cmd_collect와 동일한 끈질긴 재시도 로직
    (search_with_retry)을 그대로 쓰고, 성공한 결과는 같은 처리 과정
    (_process_search_items/_write_kept_rows)을 거쳐 videos.csv에 이어쓴다.

    failed_queries.txt는 이번 라운드 기준으로 다시 쓴다 - 이번에 성공한 건
    빠지고, 이번에도 실패한 건 다시 남아서 다음 라운드에 또 시도할 수 있다.
    """
    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    rotator = KeyRotator(keys)

    targets = _load_csv_rows(args.targets)
    groups = {}
    if args.groups and os.path.isfile(args.groups):
        with open(args.groups, encoding="utf-8") as f:
            groups = json.load(f)
    units_by_id = {u["unit_id"]: u for u in build_search_units(targets, groups)}

    if not os.path.isfile(args.failed_queries):
        print(f"실패 기록 파일이 없어요({args.failed_queries}) - 재시도할 게 없습니다.")
        return

    with open(args.failed_queries, encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f if ln.strip()]
    print(f"재시도 대상 검색어: {len(lines)}건")
    if not lines:
        return

    excluded_ids = _load_id_set(args.excluded_ids)
    channel_cache = {}
    seen_ids = set()
    videos_path = args.out or os.path.join(os.path.dirname(os.path.abspath(args.failed_queries)), "videos.csv")

    # 실패 로그를 일단 비움 - 이번 라운드에서 다시 실패하는 것만 search_with_retry가
    # 자동으로 다시 채워넣게 됨(fail_log_path로 같은 파일을 넘기므로).
    open(args.failed_queries, "w", encoding="utf-8").close()

    still_failed = []
    recovered_count = 0
    skipped_unknown_unit = 0
    start_time = time.time()

    for idx, line in enumerate(lines, 1):
        if (time.time() - start_time) / 60 >= args.time_budget_minutes:
            print(f"시간 예산({args.time_budget_minutes}분) 소진 - 나머지는 다음 라운드로 넘깁니다.", flush=True)
            still_failed.extend(lines[idx - 1:])
            break

        parts = line.split("\t")
        if len(parts) < 6:
            print(f"[{idx}/{len(lines)}] 예전 포맷이라 재시도 불가, 그대로 보존: {line[:60]}")
            still_failed.append(line)
            continue
        unit_id, query, order, video_duration, pub_after, pub_before = parts[:6]
        video_duration = video_duration or None
        pub_after = pub_after or None
        pub_before = pub_before or None

        unit = units_by_id.get(unit_id)
        if unit is None:
            print(f"[{idx}/{len(lines)}] unit_id '{unit_id}' 못 찾음(카탈로그 변경됐을 수 있음) - 건너뜀")
            skipped_unknown_unit += 1
            continue

        if rotator.exhausted:
            print("키를 모두 소진했어요 - 나머지는 다음 라운드로 넘깁니다.")
            still_failed.extend(lines[idx - 1:])
            break

        print(f"[{idx}/{len(lines)}] '{query}' 재시도 중...", flush=True)
        items = search_with_retry(
            rotator, query, args.max_videos_per_query,
            order=order, video_duration=video_duration,
            published_after=pub_after, published_before=pub_before,
            max_pages=args.max_pages,
            fail_log_path=args.failed_queries,  # 이번에도 실패하면 여기에 자동으로 다시 기록됨
            unit_id=unit_id,
        )
        if not items:  # None(키 소진) 또는 []([]는 이미 fail_log에 기록됨) 둘 다 이번엔 못 건짐
            continue

        kept = _process_search_items(items, unit, seen_ids, excluded_ids)
        channel_cache = _write_kept_rows(kept, rotator, channel_cache, videos_path)
        recovered_count += len(kept)
        time.sleep(args.query_delay)

    if still_failed:
        with open(args.failed_queries, "a", encoding="utf-8") as f:
            for line in still_failed:
                f.write(line + "\n")

    print(f"\n완료: 복구된 영상 {recovered_count}건 -> {videos_path}")
    if skipped_unknown_unit:
        print(f"알 수 없는 unit_id라 건너뛴 항목: {skipped_unknown_unit}건")
    if os.path.isfile(args.failed_queries) and os.path.getsize(args.failed_queries) > 0:
        with open(args.failed_queries, encoding="utf-8") as f:
            remaining = sum(1 for _ in f)
        print(f"여전히 실패로 남은 검색어: {remaining}건 (다음 라운드에서 다시 재시도 가능)")
    else:
        print("실패로 남은 검색어 없음 - 전부 복구했거나 처음부터 없었어요.")


def cmd_apply_gate(args):
    videos = pd.read_csv(args.videos, low_memory=False)

    if args.mode == "strict":
        # NaN(예: 스키마가 다른 예전 데이터가 섞여있는 경우)이 bool로 바뀌면
        # True 취급돼버려서 "게이트 통과 안 한 것"이 잘못 통과될 수 있음 -
        # fillna(False)로 명시적으로 걸러냄.
        keep = videos["gate_keep"].fillna(False).astype(bool)
    else:  # text-only
        text_match = videos["substring_hit"].fillna(False).astype(bool) | videos["quote_hit"].fillna(False).astype(bool)
        news_ok = (~videos["is_news"].fillna(False).astype(bool)) | videos["date_match"].fillna(False).astype(bool)
        keep = text_match & news_ok

    kept_df = videos[keep]
    dropped_df = videos[~keep]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    kept_df.to_csv(args.out, index=False, encoding="utf-8-sig")
    if args.dropped_out:
        dropped_df.to_csv(args.dropped_out, index=False, encoding="utf-8-sig")

    print(f"mode={args.mode}: 전체 {len(videos)}건 중 {len(kept_df)}건 통과, {len(dropped_df)}건 제외")
    print(f"통과분 -> {args.out}")
    if args.dropped_out:
        print(f"제외분 -> {args.dropped_out} (나중에 재검토 가능하도록 보존)")


# =============================================================================
# 3단계: merge (구 merge_results.py)
# =============================================================================

def cmd_merge(args):
    # videos.csv가 100MB 넘어가면 GitHub Actions에서 gzip 압축해서
    # videos.csv.gz로 커밋하므로, 둘 다 찾아야 함. 같은 shard에 두 파일이
    # 동시에 있으면(압축 직후 재실행 등) .gz를 우선한다(더 최신 상태).
    csv_files = glob.glob(os.path.join(args.base_dir, "shard_*", "videos.csv"))
    gz_files = glob.glob(os.path.join(args.base_dir, "shard_*", "videos.csv.gz"))
    gz_dirs = {os.path.dirname(f) for f in gz_files}
    files = sorted(gz_files + [f for f in csv_files if os.path.dirname(f) not in gz_dirs])
    print(f"shard 결과 파일 {len(files)}개 발견 (압축 {len(gz_files)}개 포함)")
    if not files:
        print("합칠 파일이 없어요.")
        return

    # perf_id -> perf_end_date 매핑 (공연 종료일 이후 업로드분을 판별하기 위함).
    # 종료 이후엔 그 공연 자체의 매출이 더 이상 존재하지 않으므로, 그런
    # 영상은 티켓 판매에 인과적으로 영향을 줄 수 없다는 판단(2026-07-30
    # 대화에서 확정)에 따라 gated/dropped와는 별도로 표기 + 분리한다.
    end_dates = _load_perf_end_dates(args.targets)
    print(f"perf_end_date 매핑 {len(end_dates)}건 로드 ({args.targets})")

    # 예전엔 20개 shard를 pd.read_csv로 전부 동시에 메모리에 올린 뒤
    # pd.concat으로 한 번 더 복사본을 만들었음 -> shard가 쌓일수록 러너
    # 메모리(7GB)를 넘겨서 hosted runner가 죽는 원인이 됨(2026-07-30 확인).
    # 지금은 파일을 하나씩만 열어서 한 줄씩 흘려보내며 쓰기 때문에, 메모리
    # 사용량이 "shard 전체 크기"가 아니라 "video_id 집합 + 파일 하나 크기"
    # 수준으로 유지됨. season_match / perf_id 집계도 스트리밍 중에 같이 계산.
    #
    # 예전엔 여기서 (게이트 미적용) all_videos.csv를 통째로 썼다가 별도
    # apply-gate 단계에서 다시 읽어 gated/dropped로 나눴음. all_videos.csv는
    # 800MB대라 gzip으로도 100MB를 못 넘기고(GitHub 커밋 불가), shard 원본
    # 데이터로부터 언제든 재생성 가능해서 별도 산출물로 보존할 실익이 없다고
    # 판단(2026-07-30) -> 기본적으로 더 이상 만들지 않고, merge 단계에서
    # 바로 gate_keep + is_post_end 기준 4개 파일로 나눠서 스트리밍으로 쓴다.
    # (원본 전체가 정말 필요하면 --emit-raw로 all_videos.csv도 추가로 남길 수 있음)
    seen_ids = set()
    total_rows = 0
    kept_rows = 0
    season_counts = {}
    perf_ids_with_video = set()
    fieldnames = None
    bucket_counts = {"gated": 0, "dropped": 0, "gated_post_end": 0, "dropped_post_end": 0}

    def _open_reader(path):
        if path.endswith(".gz"):
            import gzip
            return gzip.open(path, "rt", newline="", encoding="utf-8-sig")
        return open(path, "r", newline="", encoding="utf-8-sig")

    os.makedirs(args.out_dir, exist_ok=True)
    out_paths = {
        "gated": os.path.join(args.out_dir, "all_videos_gated.csv"),
        "dropped": os.path.join(args.out_dir, "all_videos_dropped.csv"),
        "gated_post_end": os.path.join(args.out_dir, "all_videos_gated_post_end.csv"),
        "dropped_post_end": os.path.join(args.out_dir, "all_videos_dropped_post_end.csv"),
    }
    tmp_paths = {k: p + ".tmp" for k, p in out_paths.items()}
    raw_path = os.path.join(args.out_dir, "all_videos.csv")
    raw_tmp_path = raw_path + ".tmp"

    out_files = {}
    writers = {}
    raw_f, raw_writer = None, None
    try:
        for k, p in tmp_paths.items():
            out_files[k] = open(p, "w", newline="", encoding="utf-8-sig")
        if args.emit_raw:
            raw_f = open(raw_tmp_path, "w", newline="", encoding="utf-8-sig")

        for f in files:
            if os.path.getsize(f) == 0:
                continue
            with _open_reader(f) as in_f:
                reader = csv.DictReader(in_f)
                if reader.fieldnames is None:
                    continue
                if fieldnames is None:
                    fieldnames = reader.fieldnames
                    out_fieldnames = fieldnames + ["is_post_end"]
                    for k, out_f2 in out_files.items():
                        writers[k] = csv.DictWriter(out_f2, fieldnames=out_fieldnames)
                        writers[k].writeheader()
                    if raw_f is not None:
                        raw_writer = csv.DictWriter(raw_f, fieldnames=out_fieldnames)
                        raw_writer.writeheader()
                for row in reader:
                    total_rows += 1
                    vid = row.get("video_id")
                    if vid in seen_ids:
                        continue
                    seen_ids.add(vid)
                    kept_rows += 1
                    if "season_match" in row:
                        season_counts[row["season_match"]] = season_counts.get(row["season_match"], 0) + 1
                    pid = row.get("matched_perf_id")
                    if pid:
                        perf_ids_with_video.add(pid)

                    is_post_end = _is_post_end(row.get("published_at"), end_dates.get(pid))
                    row["is_post_end"] = is_post_end
                    gate_keep = str(row.get("gate_keep", "")).strip().lower() == "true"

                    if gate_keep and not is_post_end:
                        bucket = "gated"
                    elif gate_keep and is_post_end:
                        bucket = "gated_post_end"
                    elif (not gate_keep) and not is_post_end:
                        bucket = "dropped"
                    else:
                        bucket = "dropped_post_end"
                    bucket_counts[bucket] += 1
                    writers[bucket].writerow(row)
                    if raw_writer is not None:
                        raw_writer.writerow(row)
    except Exception:
        # 실패 시 중간 파일이 남아 다음 실행에 잘못 쓰이지 않게 정리
        for k, out_f2 in out_files.items():
            out_f2.close()
        if raw_f is not None:
            raw_f.close()
        for p in list(tmp_paths.values()) + [raw_tmp_path]:
            if os.path.exists(p):
                os.remove(p)
        raise
    finally:
        for k, out_f2 in out_files.items():
            out_f2.close()
        if raw_f is not None:
            raw_f.close()

    if fieldnames is None:
        print("모든 shard 파일이 비어있어요.")
        for p in list(tmp_paths.values()) + [raw_tmp_path]:
            if os.path.exists(p):
                os.remove(p)
        return

    for k, p in tmp_paths.items():
        os.replace(p, out_paths[k])
    if args.emit_raw:
        os.replace(raw_tmp_path, raw_path)

    print(f"videos: {total_rows} -> {kept_rows}건 (중복 {total_rows - kept_rows}건 제거)")
    if season_counts:
        print("\nseason_match 분포:")
        for k, v in sorted(season_counts.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}")
    if perf_ids_with_video:
        print(f"\nperf_id 커버리지: {len(perf_ids_with_video)}개 공연에 영상 있음")

    n_post_end = bucket_counts["gated_post_end"] + bucket_counts["dropped_post_end"]
    pct_post_end = (n_post_end / kept_rows * 100) if kept_rows else 0.0
    print(f"\n종료일 이후(is_post_end=True) 업로드분: {n_post_end}건 ({pct_post_end:.1f}%)")
    print("\n저장:")
    print(f"  게이트 통과 (종료일 이전) -> {out_paths['gated']} ({bucket_counts['gated']}건)")
    print(f"  게이트 제외 (종료일 이전) -> {out_paths['dropped']} ({bucket_counts['dropped']}건)")
    print(f"  게이트 통과 (종료일 이후) -> {out_paths['gated_post_end']} ({bucket_counts['gated_post_end']}건)")
    print(f"  게이트 제외 (종료일 이후) -> {out_paths['dropped_post_end']} ({bucket_counts['dropped_post_end']}건)")
    if args.emit_raw:
        print(f"  원본 전체(비게이트, --emit-raw) -> {raw_path} ({kept_rows}건)")


# =============================================================================
# 4단계: qa (구 check_perf_id_mismatches.py)
# =============================================================================

def cmd_qa(args):
    videos = pd.read_csv(args.videos, low_memory=False)
    catalog = pd.read_csv(args.catalog)

    catalog["inst_name"], catalog["work_title"] = zip(*catalog["title"].apply(split_institution_and_work))
    catalog["work_norm"] = catalog["work_title"].apply(normalize)
    inst_works = {}
    for inst, g in catalog[catalog["inst_name"].notna()].groupby("inst_name"):
        works = list(zip(g["perf_id"], g["work_title"], g["work_norm"]))
        if len(works) > 1:
            inst_works[inst] = works
    print(f"같은 기관이 여러 작품을 올린 경우: {len(inst_works)}개 기관")

    videos["inst_name"], videos["work_title"] = zip(*videos["matched_title"].apply(split_institution_and_work))
    videos["combined_raw"] = videos["video_title"].fillna("") + " " + videos["description"].fillna("")
    videos["work_norm"] = videos["work_title"].apply(normalize)

    def find_mismatch(row, high_conf_only):
        inst = row["inst_name"]
        if not inst or inst not in inst_works:
            return None
        current = row["work_norm"]
        if high_conf_only:
            quotes = {normalize(q) for q in QUOTE_PATTERN.findall(row["combined_raw"])}
            hits = [(pid, wt) for pid, wt, wn in inst_works[inst]
                    if wn and len(wn) >= 2 and wn != current and wn in quotes]
        else:
            text_norm = normalize(row["combined_raw"])
            hits = [(pid, wt) for pid, wt, wn in inst_works[inst]
                    if wn and len(wn) >= 2 and wn != current and wn in text_norm]
        return hits or None

    videos["high_confidence_other_work"] = videos.apply(lambda r: find_mismatch(r, True), axis=1)
    videos["low_confidence_other_work"] = videos.apply(lambda r: find_mismatch(r, False), axis=1)

    report = videos[
        videos["high_confidence_other_work"].notna() | videos["low_confidence_other_work"].notna()
    ][["video_id", "matched_perf_id", "matched_title", "video_title",
       "high_confidence_other_work", "low_confidence_other_work", "video_url"]].copy()
    report["confidence"] = report["high_confidence_other_work"].apply(lambda x: "high" if x else "low")

    report.to_csv(args.out, index=False, encoding="utf-8-sig")
    n_high = (report["confidence"] == "high").sum()
    print(f"perf_id 오배정 의심: 총 {len(report)}건 (고신뢰 {n_high}건) -> {args.out}")
    print("고신뢰 건은 반드시 수동 검토 후 matched_perf_id를 정정하세요.")


# =============================================================================
# 5단계(선택): 채널 기반 보완 수집
# =============================================================================
# search.list(100 units/호출)는 relevance/date 정렬 상위권 위주로만 반환되기
# 때문에, 아무리 쿼리/윈도우를 늘려도 "그 채널엔 있지만 검색엔 안 뜨는" 영상이
# 남을 수 있다. 채널이 특정되면 playlistItems.list(1 unit/50개)로 그 채널의
# 업로드를 전량 가져올 수 있어 훨씬 저렴하고 누락이 없다.
# 매칭(perf_id 배정)은 여기서 하지 않는다 - compute_signals()를 그대로 재사용해
# merge 이후 별도 스텝으로 처리하거나, 이 출력을 collect의 videos.csv와 같은
# 스키마로 맞춰 merge에 합류시킨다.

OFFICIAL_CHANNEL_MARKERS = [
    "공식", "official", "컴퍼니", "씨어터", "시어터", "극장", "아트센터",
    "무용단", "합창단", "관현악단", "예술단", "오케스트라", "국립", "시립",
]


def _channel_confidence(channel_title, channel_description, match_key):
    """채널명이 검색에 쓴 제작사/극장명과 얼마나 강하게 일치하는지 + 공식 채널
    표지(컴퍼니/씨어터/공식 등)가 있는지로 신뢰도를 매긴다.
    high면 사람이 한 건씩 안 봐도 바로 채택해도 안전한 수준으로 간주."""
    title_norm = normalize(channel_title)
    key_norm = normalize(match_key)
    name_match = bool(key_norm) and key_norm in title_norm
    has_marker = any(m.lower() in (channel_title + channel_description).lower() for m in OFFICIAL_CHANNEL_MARKERS)

    if name_match and has_marker:
        return "high"
    if name_match:
        return "medium"
    return "low"


def cmd_discover_channels(args):
    """
    targets_enriched.csv 전체에서 고유 제작사명(company_name)/극장명(venue_name)을
    뽑아서 YouTube search.list(type=channel)로 공식 채널 후보를 자동 탐색한다.

    기존 suggest-channels는 "이미 verified된 영상"에서 자주 등장하는 채널만
    뽑았기 때문에, 애초에 검색으로 한 번도 안 걸린 채널(제작사가 자기
    채널에만 올리고 우리 검색 쿼리로는 안 걸리는 경우)은 원천적으로 후보에도
    못 들어갔다. 이 커맨드는 "우리가 찾은 영상" 기준이 아니라 "카탈로그에
    있는 제작사/극장 이름 자체"에서 출발하므로, 한 번도 못 찾은 제작사의
    채널도 후보로 잡을 수 있다.

    search.list(type=channel)은 100 units/호출이라 search.list(video)와 비용은
    같지만, 고유 기관/극장 이름 개수만큼만 호출되므로(공연 4,093개가 아니라
    제작사/극장 수백 개 단위) 전체 대비 비용은 크지 않다.

    confidence=high인 행은 채널명이 제작사/극장명과 강하게 일치하고 공식 채널
    표지(컴퍼니/씨어터/공식 등)까지 있는 경우라, 사람이 한 건씩 다 안 봐도
    바로 채택해도 안전한 수준으로 본다. medium/low만 사람이 골라서 보면
    검수 부담이 크게 줄어든다.
    """
    targets = _load_csv_rows(args.targets)
    names = set()
    for row in targets:
        company = (row.get("company_name") or "").strip()
        venue_c = venue_core(row.get("venue_name") or "")
        if company:
            names.add(("company", company))
        if venue_c:
            names.add(("venue", venue_c))
    print(f"고유 제작사/극장명 {len(names)}개 (제작사 {sum(1 for k,_ in names if k=='company')}개, "
          f"극장 {sum(1 for k,_ in names if k=='venue')}개)")

    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    rotator = KeyRotator(keys)

    processed = _load_id_set(args.state_file) if args.state_file else set()
    todo = [(kind, name) for kind, name in sorted(names) if f"{kind}::{name}" not in processed]
    print(f"이번 실행 대상: {len(todo)}개 (이미 처리됨 {len(names) - len(todo)}개 건너뜀), "
          f"시간 예산 {args.time_budget_minutes}분")

    out_exists = os.path.isfile(args.out)
    fieldnames = ["scope", "match_key", "channel_id", "channel_title", "channel_description", "confidence"]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    start_time = time.time()
    total_found = 0

    for idx, (kind, name) in enumerate(todo, 1):
        # 이름이 수백~수천 개면 오래 걸릴 수 있어서, collect/channel-crawl과
        # 동일하게 GitHub Actions 6시간 제한 전에 스스로 멈춘다. 이름 하나
        # 끝날 때마다 바로 저장(append)하는 구조라 중간에 멈춰도 안전함.
        if (time.time() - start_time) / 60 >= args.time_budget_minutes:
            print(f"시간 예산({args.time_budget_minutes}분) 소진 - 안전하게 중단 (다음 실행에서 이어감)", flush=True)
            break

        params = {"part": "snippet", "q": name, "type": "channel", "maxResults": 3}
        resp = _get_with_rotation(rotator, f"{API_BASE}/search", params, label="discover-channels")
        rows = []
        if resp is None:
            print(f"[{idx}/{len(todo)}] '{name}' 검색 실패 - 건너뛰고 계속 진행", flush=True)
        else:
            for item in resp.json().get("items", []):
                sn = item.get("snippet", {})
                title, desc = sn.get("title", ""), sn.get("description", "") or ""
                rows.append({
                    # scope/match_key: channel_allowlist.csv와 바로 호환되는 컬럼명.
                    # scope 기본값은 검색에 쓴 종류(company/venue)를 그대로 채워두지만,
                    # 사람이 검수할 때 "이 채널은 사실 여러 작품을 다루는 허브다"
                    # 싶으면 scope를 hub로 바꿔줘야 함 (자동 판별 불가 - 검색만으론
                    # 그 채널이 한 작품 전용인지 여러 작품을 다루는지 알 수 없음).
                    "scope": kind, "match_key": name,
                    "channel_id": item.get("id", {}).get("channelId", ""),
                    "channel_title": title,
                    "channel_description": desc[:200],
                    "confidence": _channel_confidence(title, desc, name),
                })

        if rows:
            with open(args.out, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not out_exists:
                    writer.writeheader()
                    out_exists = True
                writer.writerows(rows)
            total_found += len(rows)
        if args.state_file:
            _append_line(args.state_file, f"{kind}::{name}")

        if idx % 50 == 0:
            print(f"[{idx}/{len(todo)}] 진행 중... (누적 후보 {total_found}개)", flush=True)
        time.sleep(0.2)

    # 최종 요약 - 중복 제거는 여기서 한 번만(파일 전체를 다시 읽어서 확인)
    if os.path.isfile(args.out):
        df = pd.read_csv(args.out, low_memory=False).drop_duplicates(subset="channel_id")
        n_high = (df["confidence"] == "high").sum() if len(df) else 0
    else:
        df, n_high = pd.DataFrame(), 0
    print(f"\n이번 실행 신규 후보: {total_found}개 (누적 파일 기준 고유 채널 {len(df)}개) -> {args.out}")
    print(f"confidence=high {n_high}건은 채널명이 제작사/극장명과 강하게 일치 + 공식 표지까지 있어서")
    print("바로 채택해도 안전해요. medium/low만 사람이 골라서 확인하면 검수 부담이 줄어들어요.")
    print("특히 인터파크/국립극장처럼 여러 작품을 다루는 '허브' 채널이면")
    print("scope 컬럼을 hub로 바꿔주세요 (그래야 매칭 단계에서 전체 카탈로그를")
    print("후보로 놓고 판정함 - company/venue로 두면 그 하나만 후보가 됨).")


def cmd_suggest_channels(args):
    """merge 산출물(all_videos_gated.csv 등)에서 channel_id별 등장 횟수를 세어 크롤링 후보를 뽑는다.
    사람이 채널명을 보고 제작사/극장 공식 채널인지 직접 검수해야 한다."""
    videos = pd.read_csv(args.videos, low_memory=False)
    counts = videos.groupby(["channel_id", "channel_name"]).size().reset_index(name="video_count_in_data")
    counts = counts[counts["video_count_in_data"] >= args.min_count].sort_values(
        "video_count_in_data", ascending=False
    )
    counts.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"채널 후보 {len(counts)}개 -> {args.out}")
    print("⚠️  자동 추출된 '후보'예요. 팬 채널/개인 채널이 섞여있을 수 있으니 크롤링 전에")
    print("   채널명을 보고 제작사/극장 공식 채널이 맞는지 직접 확인해주세요.")


def _uploads_playlist_ids(rotator, channel_ids):
    result = {}
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i + 50]
        params = {"part": "contentDetails", "id": ",".join(chunk)}
        resp = _get_with_rotation(rotator, f"{API_BASE}/channels", params, label="uploads-playlist")
        if resp is None:
            continue
        for item in resp.json().get("items", []):
            uploads = item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
            if uploads:
                result[item["id"]] = uploads
    return result


def _iter_playlist_video_ids(rotator, playlist_id, published_after=None):
    """playlistItems.list 전체 페이징 (1 unit/페이지). 재생목록은 보통 업로드일
    역순이라, published_after보다 오래된 항목이 나오기 시작하면 그 지점에서 멈춘다."""
    video_ids = []
    next_page_token = None
    while True:
        params = {"part": "contentDetails", "playlistId": playlist_id, "maxResults": 50}
        if next_page_token:
            params["pageToken"] = next_page_token
        resp = _get_with_rotation(rotator, f"{API_BASE}/playlistItems", params, label="playlistItems")
        if resp is None:
            break  # 이 재생목록은 여기까지만 (지금까지 모은 video_ids는 보존)
        data = resp.json()
        for item in data.get("items", []):
            cd = item.get("contentDetails", {})
            vid, published_at = cd.get("videoId"), cd.get("videoPublishedAt", "")
            if not vid:
                continue
            if published_after and published_at and published_at < published_after:
                return video_ids
            video_ids.append(vid)
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
        time.sleep(0.1)
    return video_ids


def cmd_channel_crawl(args):
    start_time = time.time()
    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    rotator = KeyRotator(keys)

    allowlist = _load_csv_rows(args.channel_allowlist)
    channel_ids = [row["channel_id"] for row in allowlist if row.get("channel_id")]
    processed = _load_id_set(args.state_file) if args.state_file else set()
    channel_ids = [c for c in channel_ids if c not in processed]
    print(f"채널 {len(channel_ids)}개 대상 크롤링 시작 ({len(keys)}개 키 배정됨), "
          f"시간 예산 {args.time_budget_minutes}분")

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, "channel_videos_raw.csv")
    fieldnames = ["video_id", "video_title", "description", "channel_id", "channel_name",
                  "published_at", "duration_sec", "view_count", "like_count", "comment_count",
                  "video_url", "source_type"]

    uploads_map = _uploads_playlist_ids(rotator, channel_ids)
    print(f"업로드 재생목록 확인: {len(uploads_map)}/{len(channel_ids)}개 채널")

    total_written = 0
    for idx, (channel_id, playlist_id) in enumerate(uploads_map.items(), 1):
        # channel-crawl도 collect처럼 채널이 많거나 영상이 아주 많은 채널이
        # 섞여있으면 오래 걸릴 수 있어서, GitHub Actions 6시간 제한에 걸려
        # 강제종료되기 전에 스스로 멈춘다. 채널 단위로 즉시 저장(append)하는
        # 구조라(아래 참고), 여기서 멈춰도 이미 처리한 채널의 데이터는 안전함.
        if (time.time() - start_time) / 60 >= args.time_budget_minutes:
            print(f"시간 예산({args.time_budget_minutes}분) 소진 - 안전하게 중단 (다음 실행에서 이어감)", flush=True)
            break

        print(f"[{idx}/{len(uploads_map)}] channel={channel_id} 재생목록 순회 중...", flush=True)
        vids = _iter_playlist_video_ids(rotator, playlist_id, published_after=args.published_after)
        print(f"  {len(vids)}개 영상 발견 - 메타데이터 조회 중...", flush=True)

        meta = videos_list(rotator, vids)
        rows = []
        for vid, m in meta.items():
            sn = m.get("snippet", {})
            cd = m.get("contentDetails", {})
            st = m.get("statistics", {})
            rows.append({
                "video_id": vid, "video_title": sn.get("title", ""), "description": sn.get("description", ""),
                "channel_id": sn.get("channelId", ""), "channel_name": sn.get("channelTitle", ""),
                "published_at": sn.get("publishedAt", ""),
                "duration_sec": _parse_iso8601_duration(cd.get("duration", "")),
                "view_count": st.get("viewCount", ""), "like_count": st.get("likeCount", ""),
                "comment_count": st.get("commentCount", ""), "video_url": f"https://www.youtube.com/watch?v={vid}",
                "source_type": "channel_crawl",
            })

        # 채널 하나 끝날 때마다 바로 저장 - 중간에 시간예산/quota로 멈춰도
        # 이미 처리한 채널의 데이터는 절대 사라지지 않음.
        file_exists = os.path.isfile(out_path)
        with open(out_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        total_written += len(rows)
        if args.state_file:
            _append_line(args.state_file, channel_id)

    print(f"\n완료: 이번 실행에서 {total_written}개 -> {out_path}")
    print("이 파일은 아직 특정 공연과 매칭되지 않은 원본이에요.")
    print("scripts/pipeline.py match-channel-videos 로 이어서 매칭해주세요.")


# =============================================================================
# 유틸리티: check-keys - API 키 목록 전체 유효성 검사
# =============================================================================
_CHECK_KEYS_TEST_VIDEO_ID = "dQw4w9WgXcQ"  # 아무 공개 영상 ID면 됨 (part=id만 요청, 1 unit)


def _check_one_key(key, timeout=15):
    """키 하나로 최소 비용(1 unit) 호출을 날려서 상태를 판정.
    반환값: ('ok'|'invalid'|'quota_exceeded'|'error', 상세메시지)"""
    import requests
    params = {"part": "id", "id": _CHECK_KEYS_TEST_VIDEO_ID, "key": key}
    try:
        resp = robust_get(f"{API_BASE}/videos", params, max_retries=1, timeout=timeout)
        return "ok", f"{len(resp.json().get('items', []))}건 응답"
    except InvalidApiKeyError as e:
        return "invalid", str(e)[:150]
    except DailyQuotaExceededError as e:
        return "quota_exceeded", str(e)[:150]
    except PerMinuteRateLimitError as e:
        return "rate_limited", str(e)[:150]
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = e.response.text[:150] if e.response is not None else str(e)
        return "error", f"HTTP {status}: {body}"
    except Exception as e:
        return "error", str(e)[:150]


def cmd_check_keys(args):
    """
    669개 키 전체를 각각 1 unit짜리 호출로 검사해서 상태를 분류한다.
    - ok: 정상
    - invalid: 키 자체가 무효 (삭제된 프로젝트, API 비활성화, 오타 등) -> 목록에서 빼야 함
    - quota_exceeded: 오늘 이미 이 키의 quota를 다 씀 (내일이면 다시 정상일 수 있음)
    - rate_limited: 순간적으로 막힘 (검사 자체를 너무 빨리 돌려서 그럴 수도 있음, 재검사 권장)
    - error: 그 외 예상 못 한 응답

    키 하나당 quota 1 unit만 쓰므로 669개를 다 검사해도 669 units밖에 안 듦
    (search.list 1회 100 units보다 훨씬 저렴).
    """
    if args.keys_file:
        with open(args.keys_file, encoding="utf-8") as f:
            keys = [line.strip() for line in f if line.strip()]
    else:
        keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    print(f"키 {len(keys)}개 검사 시작 (키당 1 unit)")

    rows = []
    for idx, key in enumerate(keys, 1):
        status, detail = _check_one_key(key)
        masked = key[:6] + "..." + key[-4:] if len(key) > 12 else "***"
        rows.append({"key_index": idx, "key_masked": masked, "status": status, "detail": detail})
        if status != "ok":
            print(f"[{idx}/{len(keys)}] {masked} -> {status} ({detail})", flush=True)
        if idx % 50 == 0:
            print(f"[{idx}/{len(keys)}] 진행 중...", flush=True)
        time.sleep(args.delay)

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"\n=== 결과 요약 ({len(keys)}개 검사) ===")
    print(df["status"].value_counts().to_string())
    print(f"\n상세 결과 -> {args.out}")

    invalid_count = (df["status"] == "invalid").sum()
    if invalid_count:
        print(f"\n⚠️  invalid로 나온 {invalid_count}개는 실제로 죽은 키일 가능성이 커요.")
        print("   669개 키 목록(비밀 저장소 secrets.YOUTUBE_API_KEYS)에서 제거를 고려해보세요.")
    quota_count = (df["status"] == "quota_exceeded").sum()
    if quota_count:
        print(f"ℹ️  quota_exceeded {quota_count}개는 오늘 이미 다 쓴 것뿐이라, 정상 키일 수 있어요")
        print("   (내일 다시 검사하면 ok로 나올 가능성 높음 - 죽은 키로 오해하지 마세요).")


def _flush_match_rows(rows, path):
    """rows를 path에 이어쓰고(append), 헤더는 파일이 없을 때만 씀. 빈 리스트면 아무것도 안 함."""
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_exists = os.path.isfile(path)
    df = pd.DataFrame(rows)
    df.to_csv(path, mode="a", header=not file_exists, index=False, encoding="utf-8-sig")


def cmd_match_channel_videos(args):
    """
    channel-crawl의 raw 산출물을 실제 공연(perf_id)과 매칭한다.

    채널마다 후보 범위가 다르다는 게 핵심:
    - scope=company: channel_allowlist.csv의 match_key(제작사명)와 같은
      company_name을 가진 공연들만 후보로 놓고 판정 (예: 레미제라블코리아
      채널 -> 레미제라블 관련 공연들만).
    - scope=venue: match_key(극장명 핵심어)와 같은 venue_core를 가진 공연들만
      후보 (예: 특정 소극장 채널 -> 그 극장에서 열린 공연들만).
    - scope=hub: 후보를 좁히지 않고 카탈로그 전체(4,093개)를 다 후보로 놓고
      판정. 인터파크(월요라이브), 국립극장, 혜화로운공연생활처럼 여러 작품을
      넘나드는 채널에 씀. API 호출은 없고 순수 문자열 매칭이라(compute_signals
      는 네트워크를 안 씀) 영상 수 x 4,093건이어도 CPU로는 충분히 빠름.

    각 영상에 대해 후보 전체에 compute_signals()를 돌려서 text_match인
    후보만 남기고, 여러 개 남으면 날짜 최근접으로 하나를 고른다(assign_to_member
    와 동일한 원칙). 아예 매칭 안 되면 --unmatched-out에 별도 저장해서
    나중에 재검토할 수 있게 한다(버려지지 않음 - collect 단계와 동일한 원칙).
    """
    raw = pd.read_csv(args.raw_videos, low_memory=False)
    allowlist = pd.read_csv(args.channel_allowlist, dtype=str, low_memory=False).fillna("")
    targets = _load_csv_rows(args.targets)

    # 이 명령은 channel-crawl처럼 "이어서 처리"하는 게 아니라 매번 raw_videos.csv
    # 전체를 다시 매칭하는 구조라, 실행할 때마다 결과가 새로 나와야 함.
    # (아래에서 중간저장을 위해 append 모드를 쓰므로, 시작할 때 기존 출력을
    # 지워두지 않으면 재실행할 때마다 중복이 계속 쌓임.)
    for path in [args.out, args.unmatched_out, args.ambiguous_out]:
        if path and os.path.isfile(path):
            os.remove(path)

    channel_scope = {}
    for _, row in allowlist.iterrows():
        cid = row.get("channel_id", "")
        if cid:
            channel_scope[cid] = {"scope": row.get("scope", "hub"), "match_key": row.get("match_key", "")}

    by_company, by_venue = {}, {}
    for row in targets:
        cn = (row.get("company_name") or "").strip()
        if cn:
            by_company.setdefault(cn, []).append(row)
        vc = venue_core(row.get("venue_name") or "")
        if vc:
            by_venue.setdefault(vc, []).append(row)

    matched_rows, unmatched_rows, ambiguous_rows = [], [], []
    for _, vrow in raw.iterrows():
        cid = vrow.get("channel_id", "")
        info = channel_scope.get(cid, {"scope": "hub", "match_key": ""})
        scope, match_key = info["scope"], info["match_key"]

        if scope == "company":
            candidates = by_company.get(match_key, [])
        elif scope == "venue":
            candidates = by_venue.get(match_key, [])
        else:  # hub (또는 allowlist에 없는 채널의 기본값) -> 카탈로그 전체
            candidates = targets

        text_match_hits = []  # (cand, signals) - text_match=True인 후보 전부
        for cand in candidates:
            signals = compute_signals(
                perf={"title": cand["title"], "venue_name": cand.get("venue_name"),
                      "perf_start_date": cand.get("perf_start_date"), "perf_end_date": cand.get("perf_end_date"),
                      "company_name": cand.get("company_name")},
                video_title=vrow.get("video_title", ""), description=vrow.get("description", ""),
                channel_name=vrow.get("channel_name", ""), published_at=vrow.get("published_at"),
            )
            if signals["text_match"]:
                text_match_hits.append((cand, signals))

        if not text_match_hits:
            unmatched_rows.append(dict(vrow))
            continue

        # 서로 다른 "작품"이 동시에 걸리면(예: 프레스콜 영상에 여러 작품 배우가
        # 같이 나옴) 억지로 하나를 고르지 않는다 - 잘못 배정하는 것보다
        # ambiguous로 빼서 사람이 보는 게 안전함. 같은 작품의 여러 시즌(초연/
        # 재연)만 겹치는 건 애매한 게 아니므로(작품은 동일) 그대로 날짜
        # 최근접으로 진행한다.
        distinct_titles = {normalize(s["work_title"]) for _, s in text_match_hits}
        if len(distinct_titles) > 1:
            ambiguous_rows.append({
                **dict(vrow),
                "candidate_titles": " | ".join(sorted({c["title"] for c, _ in text_match_hits})),
                "candidate_perf_ids": " | ".join(sorted({c["perf_id"] for c, _ in text_match_hits})),
            })
            continue

        best_row, best_signals, best_dist = None, None, None
        for cand, signals in text_match_hits:
            pub = _parse_dt(vrow.get("published_at"))
            start = _parse_dt(cand.get("perf_start_date"))
            dist = abs((pub - start).days) if (pub and start) else 999999
            if best_dist is None or dist < best_dist:
                best_row, best_signals, best_dist = cand, signals, dist

        out_row = dict(vrow)
        out_row.update({
            "matched_perf_id": best_row["perf_id"], "matched_title": best_row["title"],
            "matched_genre": best_row.get("genre", ""),
            "substring_hit": best_signals["substring_hit"], "quote_hit": best_signals["quote_hit"],
            "venue_match": best_signals["venue_match"], "date_match": best_signals["date_match"],
            "actor_match": best_signals["actor_match"], "company_match": best_signals["company_match"],
            "is_news": best_signals["is_news"], "gate_keep": best_signals["keep"],
        })
        matched_rows.append(out_row)

        # hub 채널(카탈로그 전체 4,093개와 비교)은 영상이 많으면 오래 걸릴 수
        # 있어서, 다른 명령들과 일관되게 주기적으로 중간 저장한다 (API 호출은
        # 없어 quota 위험은 없지만, 오래 걸리다 중간에 끊기면 매칭 작업
        # 전체가 날아가는 건 똑같으므로).
        if len(matched_rows) >= 200:
            _flush_match_rows(matched_rows, args.out)
            matched_rows = []
        if unmatched_rows and args.unmatched_out and len(unmatched_rows) >= 200:
            _flush_match_rows(unmatched_rows, args.unmatched_out)
            unmatched_rows = []
        if ambiguous_rows and args.ambiguous_out and len(ambiguous_rows) >= 200:
            _flush_match_rows(ambiguous_rows, args.ambiguous_out)
            ambiguous_rows = []

    # 마지막으로 남은 것들 마저 저장
    _flush_match_rows(matched_rows, args.out)
    if args.unmatched_out:
        _flush_match_rows(unmatched_rows, args.unmatched_out)
    if args.ambiguous_out:
        _flush_match_rows(ambiguous_rows, args.ambiguous_out)

    print(f"매칭됨: 파일 {args.out} 확인")
    if args.unmatched_out:
        print(f"매칭 안 됨(카탈로그에 없는 작품일 수도 있음, 보존됨) -> {args.unmatched_out}")
    if args.ambiguous_out:
        print(f"서로 다른 작품이 동시에 걸려서 보류(재검토 필요) -> {args.ambiguous_out}")
    print("gate_keep 컬럼은 여기서도 discard용이 아니라 참고용이에요 - apply-gate로 이어서 필터링하세요.")


# =============================================================================
# CLI
# =============================================================================

def main():
    ap = argparse.ArgumentParser(description="KOPIS YouTube 축약콘텐츠 수집 파이프라인")
    sub = ap.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("build-targets", help="1단계: 타겟/그룹 파일 생성")
    p1.add_argument("--stats", required=True)
    p1.add_argument("--season", required=True)
    p1.add_argument("--detail", default=None,
                     help="02_공연상세.csv 경로 (선택, 있으면 캐스트/제작사 정보로 쿼리·시즌배정 강화)")
    p1.add_argument("--out-dir", required=True)
    p1.set_defaults(func=cmd_build_targets)

    p1b = sub.add_parser("sync-new-performances",
                          help="1.5단계(주간 증분): KOPIS에 새로 올라온 공연만 targets_enriched.csv/"
                               "work_groups.json에 추가 (기존 4,093개는 안 건드림)")
    p1b.add_argument("--existing-targets", required=True, help="기존 targets_enriched.csv")
    p1b.add_argument("--existing-groups", required=True, help="기존 work_groups.json")
    p1b.add_argument("--perf-list", required=True, help="이번 주 새로 받은 01_공연목록.csv")
    p1b.add_argument("--detail", default=None, help="이번 주 새로 받은 02_공연상세.csv (선택, 있으면 캐스트/제작사/러닝타임 채움)")
    p1b.add_argument("--out-targets", required=True)
    p1b.add_argument("--out-groups", required=True)
    p1b.set_defaults(func=cmd_sync_new_performances)

    p2 = sub.add_parser("collect", help="2단계: YouTube 수집 (shard 하나 분량)")
    p2.add_argument("--targets", required=True)
    p2.add_argument("--groups", default=None)
    p2.add_argument("--excluded-ids", default=None, help="선택사항. 없으면 그냥 진행")
    p2.add_argument("--api-keys", required=True)
    p2.add_argument("--max-videos-per-query", type=int, default=50,
                     help="search.list는 1~50개 요청이든 quota가 동일(100 units)하므로 기본 50")
    p2.add_argument("--max-pages", type=int, default=10,
                     help="한 쿼리당 최대 몇 페이지까지 이어받을지 (페이지가 꽉 찰 때만 다음 페이지 요청, "
                          "기본 10페이지=최대 500개, 인기작은 2배(20페이지=1000개) 적용됨)")
    p2.add_argument("--out-dir", default="./output_targeted")
    p2.add_argument("--state-file", default=None)
    p2.add_argument("--shard-index", type=int, default=None)
    p2.add_argument("--shard-count", type=int, default=None)
    p2.add_argument("--query-delay", type=float, default=1.5)
    p2.add_argument("--limit", type=int, default=None)
    p2.add_argument("--time-budget-minutes", type=float, default=300,
                     help="이 시간(분)이 지나면 GitHub Actions의 job 시간제한(보통 6시간=360분)에 "
                          "걸려 강제종료되기 전에 스스로 안전하게 멈춤 (기본 300분=5시간, "
                          "같은 job 안에서 뒤이어 도는 retry-failed 몫까지 감안한 여유)")
    p2.add_argument("--max-seconds-per-unit", type=int, default=600,
                     help="검색 단위(공연) 하나에 쓸 수 있는 최대 처리 시간(초). 429가 계속 "
                          "안 풀려서 이 시간을 넘기면 그 유닛은 포기하고 다음 유닛으로 넘어감 "
                          "(처리기록에 안 남겨서 다음 실행에서 처음부터 재시도됨). 검색어 하나가 "
                          "shard 전체 시간 예산을 다 먹어버리는 걸 막는 안전장치 (기본 600초=10분)")
    p2.add_argument(
        "--order-strategy", choices=["relevance", "date", "both"], default="relevance",
        help="relevance만(기본) / date만 / both(quota 2배, 커버리지 최우선일 때)",
    )
    p2.add_argument("--use-date-windows", action="store_true",
                     help="공연(그룹) 기간+버퍼를 월 단위로 쪼개 publishedAfter/Before 검색 (quota 크게 증가)")
    p2.add_argument("--window-days", type=int, default=30)
    p2.add_argument("--pre-buffer-days", type=int, default=30, help="개막 전 버퍼(티저/예고편)")
    p2.add_argument("--post-buffer-days", type=int, default=120, help="폐막 후 버퍼(리뷰/스포/결말)")
    p2.add_argument("--include-shorts-pass", action="store_true", default=True,
                     help="videoDuration=short 별도 패스 (기본 켜짐, 비용 낮음)")
    p2.add_argument("--no-shorts-pass", dest="include_shorts_pass", action="store_false")
    p2.set_defaults(func=cmd_collect)

    p2b = sub.add_parser("retry-failed",
                          help="2.5단계: collect에서 20바퀴 다 돌아도 실패한 검색어 재시도")
    p2b.add_argument("--failed-queries", required=True, help="collect가 남긴 failed_queries.txt 경로")
    p2b.add_argument("--targets", required=True)
    p2b.add_argument("--groups", default=None)
    p2b.add_argument("--api-keys", required=True)
    p2b.add_argument("--excluded-ids", default=None)
    p2b.add_argument("--out", default=None, help="복구된 영상을 이어쓸 videos.csv 경로 (기본: failed_queries.txt와 같은 폴더)")
    p2b.add_argument("--max-videos-per-query", type=int, default=50)
    p2b.add_argument("--max-pages", type=int, default=10)
    p2b.add_argument("--query-delay", type=float, default=4)
    p2b.add_argument("--time-budget-minutes", type=float, default=40,
                      help="이 시간(분)이 지나면 안전하게 중단(같은 job 안에서 앞서 도는 "
                           "collect 몫까지 합쳐서 GitHub Actions 6시간 제한 안에 들어오게)")
    p2b.set_defaults(func=cmd_retry_failed)

    p3 = sub.add_parser("merge", help="3단계: shard 결과 병합 (게이트+종료일 기준 4개 파일로 바로 분리)")
    p3.add_argument("--base-dir", required=True)
    p3.add_argument("--targets", required=True, help="targets_enriched.csv 경로 (perf_end_date 조인용)")
    p3.add_argument("--out-dir", required=True)
    p3.add_argument("--emit-raw", action="store_true",
                     help="게이트 적용 전 원본 전체(all_videos.csv)도 추가로 남김 (기본 off, "
                          "재생성 가능한 800MB대 파일이라 보통 불필요)")
    p3.set_defaults(func=cmd_merge)

    p4 = sub.add_parser("qa", help="4단계: 기관 교차검증 QA")
    p4.add_argument("--videos", required=True)
    p4.add_argument("--catalog", required=True)
    p4.add_argument("--out", required=True)
    p4.set_defaults(func=cmd_qa)

    p4b = sub.add_parser(
        "apply-gate",
        help="4.5단계(선택): strict 외 다른 기준(text-only)으로 재필터링하고 싶을 때만 사용. "
             "기본 strict 게이트는 이제 merge 단계에서 바로 4개 파일로 나뉘어 나오므로 "
             "보통은 이 커맨드를 따로 돌릴 필요 없음.",
    )
    p4b.add_argument("--videos", required=True,
                      help="gate_keep 컬럼 포함 CSV (merge --emit-raw로 만든 all_videos.csv 등)")
    p4b.add_argument("--mode", choices=["strict", "text-only"], default="strict")
    p4b.add_argument("--out", required=True, help="통과분 저장 경로")
    p4b.add_argument("--dropped-out", default=None, help="제외분 저장 경로 (선택, 나중에 재검토용)")
    p4b.set_defaults(func=cmd_apply_gate)

    p5 = sub.add_parser("suggest-channels", help="5단계(선택): 기존 영상 데이터에서 채널 크롤링 후보 추출")
    p5.add_argument("--videos", required=True, help="merge 산출물 경로 (예: all_videos_gated.csv)")
    p5.add_argument("--min-count", type=int, default=3)
    p5.add_argument("--out", required=True)
    p5.set_defaults(func=cmd_suggest_channels)

    p5b = sub.add_parser("discover-channels",
                          help="5단계(선택, 권장): 카탈로그 전체 제작사/극장명으로 공식 채널 자동 탐색")
    p5b.add_argument("--targets", required=True, help="targets_enriched.csv 경로")
    p5b.add_argument("--api-keys", required=True)
    p5b.add_argument("--out", required=True)
    p5b.add_argument("--state-file", default=None, help="처리 완료한 이름(scope::match_key) 기록 파일 (재실행 시 이어감)")
    p5b.add_argument("--time-budget-minutes", type=float, default=300,
                      help="GitHub Actions 6시간 제한 전에 스스로 안전하게 멈춤 (기본 300분)")
    p5b.set_defaults(func=cmd_discover_channels)

    p6 = sub.add_parser("channel-crawl", help="5단계(선택): allowlist 채널 업로드 전체 수집")
    p6.add_argument("--channel-allowlist", required=True, help="channel_id 컬럼 포함 CSV (사람이 검수한 확정 목록)")
    p6.add_argument("--api-keys", required=True)
    p6.add_argument("--published-after", default=None, help="RFC3339, 예: 2023-01-01T00:00:00Z (선택)")
    p6.add_argument("--out-dir", default="./output_channels")
    p6.add_argument("--state-file", default=None, help="처리 완료한 channel_id 기록 파일 (재실행 시 이어감)")
    p6.add_argument("--time-budget-minutes", type=float, default=300,
                     help="GitHub Actions 6시간 제한 전에 스스로 안전하게 멈춤 (기본 300분)")
    p6.set_defaults(func=cmd_channel_crawl)

    p6b = sub.add_parser("match-channel-videos",
                          help="5단계(선택): channel-crawl 결과를 실제 공연과 매칭")
    p6b.add_argument("--raw-videos", required=True, help="channel-crawl이 만든 channel_videos_raw.csv")
    p6b.add_argument("--channel-allowlist", required=True, help="scope/match_key 컬럼 포함된 확정 allowlist")
    p6b.add_argument("--targets", required=True, help="targets_enriched.csv")
    p6b.add_argument("--out", required=True)
    p6b.add_argument("--unmatched-out", default=None)
    p6b.add_argument("--ambiguous-out", default=None, help="서로 다른 작품이 동시에 걸린 영상 보류함")
    p6b.set_defaults(func=cmd_match_channel_videos)

    p7 = sub.add_parser("check-keys", help="유틸: API 키 목록 전체 유효성 검사 (키당 1 unit)")
    p7.add_argument("--api-keys", default=None, help="콤마로 구분된 키 목록")
    p7.add_argument("--keys-file", default=None, help="한 줄에 키 하나씩 있는 파일 (--api-keys 대신 사용 가능)")
    p7.add_argument("--delay", type=float, default=0.3, help="키 검사 사이 대기시간(초)")
    p7.add_argument("--out", default="key_check_result.csv")
    p7.set_defaults(func=cmd_check_keys)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
