# -*- coding: utf-8 -*-
"""
pipeline.py
===========
KOPIS 공연 YouTube 축약콘텐츠 수집 파이프라인 전체를 파일 하나로.
서브커맨드 4개로 단계를 나눈다 (기능은 이전 5개 파일과 동일, 합치기만 함):

  python pipeline.py build-targets  --stats ... --season ... --out-dir data/
  python pipeline.py collect        --targets ... --groups ... --api-keys ... --shard-index 0 --shard-count 20
  python pipeline.py merge          --base-dir data/youtube_targeted --out-dir data/youtube_targeted
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
"""
import argparse
import csv
import glob
import json
import os
import re
import sys
import time

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

    os.makedirs(args.out_dir, exist_ok=True)
    targets_path = os.path.join(args.out_dir, "targets_enriched.csv")
    # CSV에는 리스트를 파이프(|)로 join해서 저장 (배우 이름 자체엔 콤마/파이프가 안 들어가므로 안전)
    out_df = merged.copy()
    out_df["cast_names"] = out_df["cast_names"].apply(lambda v: "|".join(v))
    out_df.to_csv(targets_path, index=False, encoding="utf-8-sig")
    print(f"타겟 파일 저장: {targets_path} ({len(merged)}건)")

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
        } for _, row in g.iterrows()]
        rep = min(g["title"], key=len)
        groups[key] = {"representative_title": rep, "members": members}

    groups_path = os.path.join(args.out_dir, "work_groups.json")
    with open(groups_path, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)
    print(f"그룹 윈도우 저장: {groups_path} ({len(groups)}개 그룹, "
          f"{sum(len(v['members']) for v in groups.values())}개 공연 포함)")


# =============================================================================
# 2단계: collect (구 youtube_collect_targeted.py)
# =============================================================================

class PerMinuteRateLimitError(Exception):
    pass


class DailyQuotaExceededError(Exception):
    pass


def robust_get(url, params, max_retries=3, timeout=20):
    import requests  # collect 커맨드에서만 필요 (prepare/merge/qa는 pandas만 있으면 됨)
    backoff = 2
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
    def current(self):
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
    windows = []
    while cursor < limit:
        chunk_end = min(cursor + timedelta(days=window_days), limit)
        windows.append((cursor.strftime("%Y-%m-%dT00:00:00Z"), chunk_end.strftime("%Y-%m-%dT00:00:00Z")))
        cursor = chunk_end
    return windows


def search_with_retry(rotator, query, max_results=15, per_minute_wait=15,
                       order="relevance", video_duration=None,
                       published_after=None, published_before=None):
    """429/분당제한이 뜨면 같은 키로 오래 기다리기보다, 배정된 다른 키로 바로
    전환해서 시도한다 (이 shard가 여러 개 키를 갖고 있는 걸 활용). 그래도 막히면
    그때 짧게 대기 후 재시도. 최대 min(len(키), 6)개 키까지 돌려본다.

    order: "relevance" 또는 "date" (date를 병행하면 인기/조회수가 낮아
    relevance 상위에 안 뜨는 영상도 잡을 수 있음)
    video_duration: None 또는 "short" (쇼츠 누락 방지용 별도 패스에 사용)
    published_after/published_before: RFC3339 문자열 (날짜 윈도우 검색용)
    """
    base = {"part": "snippet", "q": query, "type": "video",
            "maxResults": max_results, "relevanceLanguage": "ko", "order": order}
    if video_duration:
        base["videoDuration"] = video_duration
    if published_after:
        base["publishedAfter"] = published_after
    if published_before:
        base["publishedBefore"] = published_before
    max_key_tries = min(len(rotator.keys), 6)
    for key_try in range(max_key_tries):
        params = dict(base, key=rotator.current)
        try:
            resp = robust_get(f"{API_BASE}/search", params)
            return resp.json().get("items", [])
        except PerMinuteRateLimitError:
            print(f"  [429/분당제한] '{query}' - 키 전환 시도 ({key_try + 1}/{max_key_tries})", flush=True)
            if rotator.rotate():
                continue  # 다른 키로 바로 재시도 (대기 없이)
            # 이 shard 키를 전부 써봤는데도 막힘 -> 그제서야 짧게 대기하고 처음 키로 복귀
            print(f"  키를 모두 돌려봤는데도 막혀요. {per_minute_wait}초 대기 후 처음 키로 재시도.", flush=True)
            time.sleep(per_minute_wait)
            rotator.idx = 0
        except DailyQuotaExceededError:
            if not rotator.rotate():
                print("  이 shard에 배정된 키를 모두 소진했어요.", flush=True)
                return None
    print(f"  [포기] '{query}' 재시도 실패, 건너뜀", flush=True)
    return []


def videos_list(rotator, video_ids):
    out = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        params = {"part": "contentDetails,statistics", "id": ",".join(chunk), "key": rotator.current}
        try:
            resp = robust_get(f"{API_BASE}/videos", params)
        except DailyQuotaExceededError:
            if not rotator.rotate():
                break
            params["key"] = rotator.current
            resp = robust_get(f"{API_BASE}/videos", params)
        for item in resp.json().get("items", []):
            out[item["id"]] = item
    return out


def channels_list(rotator, channel_ids, cache):
    """채널 구독자수/영상수. 이미 캐시에 있는 채널은 다시 안 부름 (같은 채널이
    여러 영상에 겹치는 경우가 많아서 quota 절약)."""
    new_ids = [c for c in dict.fromkeys(channel_ids) if c and c not in cache]
    for i in range(0, len(new_ids), 50):
        chunk = new_ids[i:i + 50]
        params = {"part": "statistics", "id": ",".join(chunk), "key": rotator.current}
        try:
            resp = robust_get(f"{API_BASE}/channels", params)
        except DailyQuotaExceededError:
            if not rotator.rotate():
                break
            params["key"] = rotator.current
            resp = robust_get(f"{API_BASE}/channels", params)
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
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def _write_csv_row(path, fieldnames, row):
    is_new = not os.path.isfile(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
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
        units.append({
            "unit_id": f"group::{key}", "is_group": True,
            "title": g["representative_title"], "genre": None,
            "venue_name": g["members"][0]["venue_name"], "members": g["members"],
            "inst_name": split_institution_and_work(g["members"][0]["title"])[0],
            "cast_names": rep_cast, "company_name": rep_company,
        })
    for row in targets:
        if row["perf_id"] in grouped_perf_ids:
            continue
        cast = (row.get("cast_names") or "").split("|") if row.get("cast_names") else []
        is_recurring = str(row.get("is_recurring_title", "")).strip().lower() == "true"
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


def cmd_collect(args):
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

    print(f"이번 shard 처리 대상: {len(units)}개 검색 단위 ({len(keys)}개 키 배정됨)")

    videos_path = os.path.join(args.out_dir, "videos.csv")
    fieldnames = [
        "video_id", "video_title", "description", "channel_id", "channel_name",
        "channel_subscriber_count", "channel_video_count",
        "published_at", "duration_sec", "video_format",
        "view_count", "like_count", "comment_count", "video_url",
        "matched_perf_id", "matched_title", "matched_genre", "season_match",
        "substring_hit", "quote_hit", "venue_match", "date_match", "actor_match", "company_match", "is_news",
        "gate_keep",
    ]
    channel_cache = {}  # channel_id -> statistics dict. shard 전체에서 재사용해 quota 절약.

    total_kept = 0
    orders_by_strategy = {"relevance": ["relevance"], "date": ["date"], "both": ["relevance", "date"]}
    orders = orders_by_strategy[args.order_strategy]

    for unit in units:
        parsed_inst, parsed_work_title = split_institution_and_work(unit["title"])
        query_title = parsed_work_title or unit["title"]
        query_inst = unit.get("inst_name") or parsed_inst
        queries = build_queries(query_title, unit["genre"], unit["venue_name"],
                                 is_group=unit["is_group"], inst_name=query_inst,
                                 cast_names=unit.get("cast_names"), company_name=unit.get("company_name"))

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
        calls = [(q, order, None, pa, pb) for q in queries for order in orders for pa, pb in windows]
        if args.include_shorts_pass:
            # 쇼츠 누락 방지용 별도 패스는 대표 제목 하나로, 날짜 윈도우 없이 1회만
            calls.append((unit["title"], "relevance", "short", None, None))

        print(f"[{unit['unit_id']}] 쿼리 {len(queries)}개 x 검색콜 {len(calls)}개 "
              f"(order={args.order_strategy}, windows={len(windows) if args.use_date_windows else 0}, "
              f"shorts_pass={args.include_shorts_pass})", flush=True)
        seen_ids, kept = set(), []

        def _process_items(items):
            for item in items:
                vid = item["id"]["videoId"]
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
                # (검색으로 찾은 건 일단 다 저장해야 recall 손실이 안 생김)
                kept.append((vid, sn, target_perf_id, member, season_match, signals))

        for q, order, video_duration, pub_after, pub_before in calls:
            items = search_with_retry(
                rotator, q, args.max_videos_per_query,
                order=order, video_duration=video_duration,
                published_after=pub_after, published_before=pub_before,
            )
            if items is None:
                print("전체 키 소진 - 스크립트 종료 (다음 실행에서 이어감)", flush=True)
                return
            time.sleep(args.query_delay)
            _process_items(items)

        if kept:
            meta = videos_list(rotator, [v[0] for v in kept])
            channel_cache = channels_list(rotator, [sn.get("channelId", "") for _, sn, *_ in kept], channel_cache)
            for vid, sn, target_perf_id, member, season_match, signals in kept:
                m = meta.get(vid, {})
                ch_id = sn.get("channelId", "")
                ch_stats = channel_cache.get(ch_id, {})
                duration_sec = _parse_iso8601_duration(m.get("contentDetails", {}).get("duration", ""))
                _write_csv_row(videos_path, fieldnames, {
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

        total_kept += len(kept)
        gate_pass = sum(1 for *_, signals in kept if signals["keep"])
        print(f"  -> 이 유닛 수집: {len(kept)}건 (게이트통과 참고치: {gate_pass}건) (누적수집: {total_kept}건)", flush=True)

        _append_line(args.state_file, unit["unit_id"])

    print(f"완료. 이 shard 총 수집(게이트 미적용, 전량 저장): {total_kept}건", flush=True)


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

def cmd_apply_gate(args):
    videos = pd.read_csv(args.videos, low_memory=False)

    if args.mode == "strict":
        keep = videos["gate_keep"].astype(bool)
    else:  # text-only
        text_match = videos["substring_hit"].astype(bool) | videos["quote_hit"].astype(bool)
        news_ok = (~videos["is_news"].astype(bool)) | videos["date_match"].astype(bool)
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
    files = sorted(glob.glob(os.path.join(args.base_dir, "shard_*", "videos.csv")))
    print(f"shard 결과 파일 {len(files)}개 발견")
    if not files:
        print("합칠 파일이 없어요.")
        return

    dfs = [pd.read_csv(f, low_memory=False) for f in files if os.path.getsize(f) > 0]
    if not dfs:
        print("모든 shard 파일이 비어있어요.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset="video_id", keep="first")
    print(f"videos: {before} -> {len(merged)}건 (중복 {before - len(merged)}건 제거)")
    print("\nseason_match 분포:")
    print(merged["season_match"].value_counts())
    print(f"\nperf_id 커버리지: {merged['matched_perf_id'].nunique()}개 공연에 영상 있음")

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, "all_videos.csv")
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n저장: {out_path}")


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

    rows = []
    for idx, (kind, name) in enumerate(sorted(names), 1):
        params = {"part": "snippet", "q": name, "type": "channel", "maxResults": 3, "key": rotator.current}
        try:
            resp = robust_get(f"{API_BASE}/search", params)
        except DailyQuotaExceededError:
            if not rotator.rotate():
                print(f"[{idx}/{len(names)}] 키 전부 소진 - 지금까지 결과만 저장하고 종료")
                break
            params["key"] = rotator.current
            resp = robust_get(f"{API_BASE}/search", params)
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
        if idx % 50 == 0:
            print(f"[{idx}/{len(names)}] 진행 중...", flush=True)
        time.sleep(0.2)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df = pd.DataFrame(rows).drop_duplicates(subset="channel_id")
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    n_high = (df["confidence"] == "high").sum() if len(df) else 0
    print(f"\n채널 후보 {len(rows)}개(고유 기관/극장 {len(names)}개 기준 검색) -> {args.out}")
    print(f"confidence=high {n_high}건은 채널명이 제작사/극장명과 강하게 일치 + 공식 표지까지 있어서")
    print("바로 채택해도 안전해요. medium/low만 사람이 골라서 확인하면 검수 부담이 줄어들어요.")
    print("특히 인터파크/국립극장처럼 여러 작품을 다루는 '허브' 채널이면")
    print("scope 컬럼을 hub로 바꿔주세요 (그래야 매칭 단계에서 전체 카탈로그를")
    print("후보로 놓고 판정함 - company/venue로 두면 그 하나만 후보가 됨).")


def cmd_suggest_channels(args):
    """기존 all_videos.csv에서 channel_id별 등장 횟수를 세어 크롤링 후보를 뽑는다.
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
        params = {"part": "contentDetails", "id": ",".join(chunk), "key": rotator.current}
        resp = robust_get(f"{API_BASE}/channels", params)
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
        params = {"part": "contentDetails", "playlistId": playlist_id, "maxResults": 50, "key": rotator.current}
        if next_page_token:
            params["pageToken"] = next_page_token
        resp = robust_get(f"{API_BASE}/playlistItems", params)
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
    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    rotator = KeyRotator(keys)

    allowlist = _load_csv_rows(args.channel_allowlist)
    channel_ids = [row["channel_id"] for row in allowlist if row.get("channel_id")]
    print(f"채널 {len(channel_ids)}개 대상 크롤링 시작 ({len(keys)}개 키 배정됨)")

    uploads_map = _uploads_playlist_ids(rotator, channel_ids)
    print(f"업로드 재생목록 확인: {len(uploads_map)}/{len(channel_ids)}개 채널")

    all_video_ids = []
    for idx, (channel_id, playlist_id) in enumerate(uploads_map.items(), 1):
        print(f"[{idx}/{len(uploads_map)}] channel={channel_id} 재생목록 순회 중...", flush=True)
        vids = _iter_playlist_video_ids(rotator, playlist_id, published_after=args.published_after)
        print(f"  {len(vids)}개 영상 발견")
        all_video_ids.extend(vids)

    unique_ids = list(dict.fromkeys(all_video_ids))
    print(f"\n고유 영상 {len(unique_ids)}개 메타데이터 조회 중...")
    meta = videos_list(rotator, unique_ids)

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, "channel_videos_raw.csv")
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
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n완료: {len(rows)}개 -> {out_path}")
    print("이 파일은 아직 특정 공연과 매칭되지 않은 원본이에요.")
    print("scripts/pipeline.py match-channel-videos 로 이어서 매칭해주세요.")


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

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    pd.DataFrame(matched_rows).to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"매칭됨: {len(matched_rows)}건 -> {args.out}")
    if args.unmatched_out:
        os.makedirs(os.path.dirname(args.unmatched_out) or ".", exist_ok=True)
        pd.DataFrame(unmatched_rows).to_csv(args.unmatched_out, index=False, encoding="utf-8-sig")
        print(f"매칭 안 됨(카탈로그에 없는 작품일 수도 있음, 보존됨): {len(unmatched_rows)}건 -> {args.unmatched_out}")
    if args.ambiguous_out:
        os.makedirs(os.path.dirname(args.ambiguous_out) or ".", exist_ok=True)
        pd.DataFrame(ambiguous_rows).to_csv(args.ambiguous_out, index=False, encoding="utf-8-sig")
        print(f"서로 다른 작품이 동시에 걸려서 보류(재검토 필요): {len(ambiguous_rows)}건 -> {args.ambiguous_out}")
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

    p2 = sub.add_parser("collect", help="2단계: YouTube 수집 (shard 하나 분량)")
    p2.add_argument("--targets", required=True)
    p2.add_argument("--groups", default=None)
    p2.add_argument("--excluded-ids", default=None, help="선택사항. 없으면 그냥 진행")
    p2.add_argument("--api-keys", required=True)
    p2.add_argument("--max-videos-per-query", type=int, default=15)
    p2.add_argument("--out-dir", default="./output_targeted")
    p2.add_argument("--state-file", default=None)
    p2.add_argument("--shard-index", type=int, default=None)
    p2.add_argument("--shard-count", type=int, default=None)
    p2.add_argument("--query-delay", type=float, default=1.5)
    p2.add_argument("--limit", type=int, default=None)
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

    p3 = sub.add_parser("merge", help="3단계: shard 결과 병합")
    p3.add_argument("--base-dir", required=True)
    p3.add_argument("--out-dir", required=True)
    p3.set_defaults(func=cmd_merge)

    p4 = sub.add_parser("qa", help="4단계: 기관 교차검증 QA")
    p4.add_argument("--videos", required=True)
    p4.add_argument("--catalog", required=True)
    p4.add_argument("--out", required=True)
    p4.set_defaults(func=cmd_qa)

    p4b = sub.add_parser("apply-gate", help="4.5단계: 수집과 분리된 필터링 (raw는 항상 보존)")
    p4b.add_argument("--videos", required=True, help="merge 단계 산출물 (gate_keep 컬럼 포함된 all_videos.csv)")
    p4b.add_argument("--mode", choices=["strict", "text-only"], default="strict")
    p4b.add_argument("--out", required=True, help="통과분 저장 경로")
    p4b.add_argument("--dropped-out", default=None, help="제외분 저장 경로 (선택, 나중에 재검토용)")
    p4b.set_defaults(func=cmd_apply_gate)

    p5 = sub.add_parser("suggest-channels", help="5단계(선택): 기존 영상 데이터에서 채널 크롤링 후보 추출")
    p5.add_argument("--videos", required=True, help="all_videos.csv 경로")
    p5.add_argument("--min-count", type=int, default=3)
    p5.add_argument("--out", required=True)
    p5.set_defaults(func=cmd_suggest_channels)

    p5b = sub.add_parser("discover-channels",
                          help="5단계(선택, 권장): 카탈로그 전체 제작사/극장명으로 공식 채널 자동 탐색")
    p5b.add_argument("--targets", required=True, help="targets_enriched.csv 경로")
    p5b.add_argument("--api-keys", required=True)
    p5b.add_argument("--out", required=True)
    p5b.set_defaults(func=cmd_discover_channels)

    p6 = sub.add_parser("channel-crawl", help="5단계(선택): allowlist 채널 업로드 전체 수집")
    p6.add_argument("--channel-allowlist", required=True, help="channel_id 컬럼 포함 CSV (사람이 검수한 확정 목록)")
    p6.add_argument("--api-keys", required=True)
    p6.add_argument("--published-after", default=None, help="RFC3339, 예: 2023-01-01T00:00:00Z (선택)")
    p6.add_argument("--out-dir", default="./output_channels")
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

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
