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


def compute_signals(perf, video_title, description, channel_name, published_at):
    """keep = (substring_hit OR quote_hit) AND (date_match OR venue_match)
    (뉴스 채널은 date_match까지 필수). Precision 87.0% / Recall 45.9% 검증됨."""
    inst_name, work_title = split_institution_and_work(perf.get("title"))
    title_core = normalize(work_title)[:6]
    inst_core = normalize(inst_name) if inst_name else ""
    venue_norm = normalize(venue_core(perf.get("venue_name")))

    video_title = video_title or ""
    description = description or ""  # description NaN 버그 수정
    combined_raw = f"{video_title} {description}"
    combined_text = normalize(combined_raw)

    date_match = _date_within_buffer(published_at, perf.get("perf_start_date"), perf.get("perf_end_date"))
    venue_match = len(venue_norm) >= 2 and venue_norm in combined_text
    inst_match = len(inst_core) >= 4 and inst_core in combined_text
    substring_hit = len(title_core) >= 3 and title_core in combined_text
    quote_hit = quoted_exact_match(work_title, combined_raw)

    text_match = substring_hit or quote_hit
    is_news = channel_name in NEWS_CHANNELS
    keep = (text_match and date_match) if is_news else (text_match and (date_match or venue_match))

    return {
        "work_title": work_title, "inst_name": inst_name,
        "substring_hit": substring_hit, "quote_hit": quote_hit,
        "inst_match": inst_match, "venue_match": venue_match, "date_match": date_match,
        "is_news": is_news, "keep": keep,
    }


# =============================================================================
# 1단계: build-targets (구 build_targets.py)
# =============================================================================

def cmd_build_targets(args):
    stats = pd.read_csv(args.stats)
    season = pd.read_csv(args.season)

    static_cols = ["perf_id", "title", "genre", "perf_start_date", "perf_end_date",
                   "venue_name", "runtime_min", "company_id"]
    static = stats.drop_duplicates("perf_id")[static_cols]
    sales = stats.groupby("perf_id")["ticket_sales_qty"].sum().rename("total_ticket_sales_qty")

    merged = static.merge(sales, on="perf_id").merge(
        season[["perf_id", "season_match_status", "work_group_key", "season_rank"]],
        on="perf_id", how="left",
    )

    # season_group_size 컬럼은 신뢰 불가 (원본 후보그룹 크기라 최종 매칭과 다름) -> 재계산
    actual_size = (
        merged[merged["season_match_status"] == "matched"]
        .groupby("work_group_key")["perf_id"].transform("count")
    )
    merged["actual_group_size"] = 1
    merged.loc[merged["season_match_status"] == "matched", "actual_group_size"] = actual_size
    merged["runtime_missing"] = merged["runtime_min"].isna()
    merged = merged.sort_values("total_ticket_sales_qty", ascending=False)

    os.makedirs(args.out_dir, exist_ok=True)
    targets_path = os.path.join(args.out_dir, "targets_enriched.csv")
    merged.to_csv(targets_path, index=False, encoding="utf-8-sig")
    print(f"타겟 파일 저장: {targets_path} ({len(merged)}건)")

    groups = {}
    matched = merged[merged["season_match_status"] == "matched"]
    for key, g in matched.groupby("work_group_key"):
        if len(g) < 2:
            continue
        members = [{
            "perf_id": row["perf_id"], "title": row["title"], "venue_name": row["venue_name"],
            "perf_start_date": str(row["perf_start_date"]), "perf_end_date": str(row["perf_end_date"]),
            "season_rank": row["season_rank"],
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


def search_with_retry(rotator, query, max_results=15, per_minute_wait=20):
    base = {"part": "snippet", "q": query, "type": "video",
            "maxResults": max_results, "relevanceLanguage": "ko"}
    for attempt in range(3):
        params = dict(base, key=rotator.current)
        try:
            resp = robust_get(f"{API_BASE}/search", params)
            return resp.json().get("items", [])
        except PerMinuteRateLimitError:
            print(f"  [분당제한] '{query}' - {per_minute_wait}초 대기 후 재시도 ({attempt + 1}/3)", flush=True)
            time.sleep(per_minute_wait)
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


def build_queries(title, genre, venue_name, is_group=False, inst_name=None):
    title = (title or "").strip()
    if not title:
        return []
    venue_c = venue_core(venue_name)
    queries = [title, f"{title} 쇼츠", f"{title} 리뷰", f"{title} 하이라이트", f"{title} 커튼콜"]
    if venue_c and venue_c != title:
        queries.append(f"{title} {venue_c}")
    if "뮤지컬" in (genre or ""):
        queries.append(f"{title} 넘버")
    if "무용" in (genre or "") or "발레" in (genre or ""):
        queries.append(f"{title} 공연")
    if is_group and inst_name:
        queries.append(f"{inst_name} {title}")
    return list(dict.fromkeys(queries))


def assign_to_member(published_at, members):
    pub = _parse_dt(published_at)
    if pub is None:
        return members[0]["perf_id"], "unknown"
    for m in members:
        start, end = _parse_dt(m["perf_start_date"]), _parse_dt(m["perf_end_date"])
        if start and end and start <= pub <= end:
            return m["perf_id"], "current_season"
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
        units.append({
            "unit_id": f"group::{key}", "is_group": True,
            "title": g["representative_title"], "genre": None,
            "venue_name": g["members"][0]["venue_name"], "members": g["members"],
            "inst_name": split_institution_and_work(g["members"][0]["title"])[0],
        })
    for row in targets:
        if row["perf_id"] in grouped_perf_ids:
            continue
        units.append({
            "unit_id": f"perf::{row['perf_id']}", "is_group": False,
            "title": row["title"], "genre": row.get("genre"), "venue_name": row.get("venue_name"),
            "members": [{
                "perf_id": row["perf_id"], "title": row["title"], "venue_name": row.get("venue_name"),
                "perf_start_date": row.get("perf_start_date"), "perf_end_date": row.get("perf_end_date"),
            }],
            "inst_name": None,
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
        "published_at", "duration_sec", "view_count", "video_url",
        "matched_perf_id", "matched_title", "season_match",
        "substring_hit", "quote_hit", "venue_match", "date_match",
    ]

    for unit in units:
        queries = build_queries(unit["title"], unit["genre"], unit["venue_name"],
                                 is_group=unit["is_group"], inst_name=unit["inst_name"])
        print(f"[{unit['unit_id']}] 쿼리 {len(queries)}개", flush=True)
        seen_ids, kept = set(), []

        for q in queries:
            items = search_with_retry(rotator, q, args.max_videos_per_query)
            if items is None:
                print("전체 키 소진 - 스크립트 종료 (다음 실행에서 이어감)")
                return
            time.sleep(args.query_delay)

            for item in items:
                vid = item["id"]["videoId"]
                if vid in seen_ids or vid in excluded_ids:
                    continue
                seen_ids.add(vid)
                sn = item["snippet"]
                target_perf_id, season_match = (
                    assign_to_member(sn.get("publishedAt"), unit["members"])
                    if unit["is_group"] else (unit["members"][0]["perf_id"], "current_season")
                )
                member = next(m for m in unit["members"] if m["perf_id"] == target_perf_id)
                signals = compute_signals(
                    perf={"title": member["title"], "venue_name": member["venue_name"],
                          "perf_start_date": member["perf_start_date"], "perf_end_date": member["perf_end_date"]},
                    video_title=sn.get("title", ""), description=sn.get("description", ""),
                    channel_name=sn.get("channelTitle", ""), published_at=sn.get("publishedAt"),
                )
                if not signals["keep"]:
                    continue
                kept.append((vid, sn, target_perf_id, member, season_match, signals))

        if kept:
            meta = videos_list(rotator, [v[0] for v in kept])
            for vid, sn, target_perf_id, member, season_match, signals in kept:
                m = meta.get(vid, {})
                _write_csv_row(videos_path, fieldnames, {
                    "video_id": vid, "video_title": sn.get("title", ""),
                    "description": sn.get("description", ""), "channel_id": sn.get("channelId", ""),
                    "channel_name": sn.get("channelTitle", ""), "published_at": sn.get("publishedAt", ""),
                    "duration_sec": _parse_iso8601_duration(m.get("contentDetails", {}).get("duration", "")),
                    "view_count": m.get("statistics", {}).get("viewCount", ""),
                    "video_url": f"https://www.youtube.com/watch?v={vid}",
                    "matched_perf_id": target_perf_id, "matched_title": member["title"],
                    "season_match": season_match, "substring_hit": signals["substring_hit"],
                    "quote_hit": signals["quote_hit"], "venue_match": signals["venue_match"],
                    "date_match": signals["date_match"],
                })

        _append_line(args.state_file, unit["unit_id"])

    print("완료.")


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
# CLI
# =============================================================================

def main():
    ap = argparse.ArgumentParser(description="KOPIS YouTube 축약콘텐츠 수집 파이프라인")
    sub = ap.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("build-targets", help="1단계: 타겟/그룹 파일 생성")
    p1.add_argument("--stats", required=True)
    p1.add_argument("--season", required=True)
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

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
