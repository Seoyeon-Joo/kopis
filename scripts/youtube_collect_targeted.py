# -*- coding: utf-8 -*-
"""
youtube_collect_targeted.py
============================
기존 youtube_collect.py의 헬퍼 함수를 그대로 재사용하면서,
"영상이 부족한 공연만" 타겟으로 추가 수집하는 스크립트.

기존 youtube_collect.py 대비 달라진 점
--------------------------------------
1. 입력이 전체 공연 리스트가 아니라 missing_or_low_coverage_perfs.csv
   (verified 영상 0~2개인 공연만 모아둔 우선순위 리스트)
2. 공연 제목 하나로만 검색하지 않고 쿼리 변형을 여러 개 만들어서 검색
   (쇼츠/리뷰/하이라이트/커튼콜/극장명/넘버 등)
3. videoDuration=short 파라미터로 한 번 더 검색해서 쇼츠 누락을 줄임
4. excluded_video_ids.txt(이미 verified/review/delete에 있는 video_id)를
   API 호출 전에 걸러서 quota 낭비를 막음
5. 수집된 영상에 signal_scorer.py의 점수화 로직을 바로 적용해서
   suggested_status, signal_score, flag_reason 컬럼을 붙여서 출력
   (review 큐에 넣기 전에 1차로 걸러지게)

사용법 (기존 워크플로우와 동일한 인자 체계 유지)
--------------------------------------------
    python youtube_collect_targeted.py \
        --targets data/missing_or_low_coverage_perfs.csv \
        --excluded-ids data/excluded_video_ids.txt \
        --api-key "$API_KEY" \
        --shard-index 0 --shard-count 62 \
        --out-dir data/youtube_targeted/shard_0 \
        --state-file data/youtube_targeted/shard_0/processed_perf_ids.txt \
        --max-videos-per-query 15 \
        --max-comments-per-video 30
"""

import os
import sys
import csv
import time
import argparse

import youtube_collect as yc
from signal_scorer import score_row, normalize_venue_core
from net_utils import robust_get


class PerMinuteRateLimitError(Exception):
    """일일 quota가 아니라 '분당' 요청 제한에 걸렸을 때 (대기 후 재시도 가능)"""
    pass


def search_videos_ext(api_key, query, max_results=15, video_duration=None, max_retries=3):
    """
    yc.search_videos와 동일하지만 videoDuration 파라미터를 추가로 받는다.
    (any / short / medium / long — 여기서는 주로 'short'를 추가 호출에 사용)

    '분당' rate limit(PerMinuteRateLimitError)은 일일 quota 소진과 다르게,
    잠깐 대기했다가 같은 요청을 재시도하면 대부분 풀린다. 최대 max_retries번
    재시도하고, 그래도 안 되면 포기하고 빈 리스트를 반환한다(shard 전체를
    죽이지 않음).
    """
    params = {
        "key": api_key,
        "q": query,
        "part": "snippet",
        "type": "video",
        "maxResults": min(max_results, 50),
        "relevanceLanguage": "ko",
        "regionCode": "KR",
        "order": "relevance",
    }
    if video_duration:
        params["videoDuration"] = video_duration

    video_ids = []
    next_page_token = None
    while len(video_ids) < max_results:
        if next_page_token:
            params["pageToken"] = next_page_token

        attempt = 0
        while True:
            resp = robust_get(f"{yc.API_BASE}/search", params=params, timeout=20)
            data = resp.json()
            if "error" not in data:
                break
            msg = data["error"].get("message", "")
            if "per minute" in msg.lower() or "per 100 seconds" in msg.lower():
                attempt += 1
                if attempt > max_retries:
                    print(f"  [분당 한도] '{query}': {max_retries}번 재시도해도 안 풀림, 이 쿼리는 건너뜀")
                    return video_ids[:max_results]
                wait = 20 * attempt
                print(f"  [분당 한도] '{query}': {wait}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(wait)
                continue
            if "quota" in msg.lower():
                # 분당 한도가 아닌 진짜 일일 quota 소진
                raise yc.QuotaExceededError(msg)
            print(f"  [search 오류] '{query}' ({video_duration}): {msg}")
            return video_ids[:max_results]

        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        next_page_token = data.get("nextPageToken")
        if not next_page_token or len(video_ids) >= max_results:
            break
        time.sleep(0.5)

    return video_ids[:max_results]


def load_target_performances(csv_path):
    """ missing_or_low_coverage_perfs.csv 로딩 (perf_id, title, genre, venue_name,
        perf_start_date, perf_end_date, total_ticket_sales_qty, verified_video_count) """
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    # 판매량 큰 공연부터 (흥행작일수록 놓친 영상 있을 확률/영향 둘 다 큼)
    rows.sort(key=lambda r: float(r.get("total_ticket_sales_qty") or 0), reverse=True)
    return rows


def load_excluded_ids(path):
    if not path or not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def build_queries(perf):
    title = (perf.get("title") or "").strip()
    if not title:
        return []
    venue_core = normalize_venue_core(perf.get("venue_name", ""))
    genre = perf.get("genre", "")

    queries = [
        title,
        f"{title} 쇼츠",
        f"{title} 리뷰",
        f"{title} 하이라이트",
        f"{title} 커튼콜",
    ]
    if venue_core and venue_core != title:
        queries.append(f"{title} {venue_core}")
    if "뮤지컬" in genre:
        queries.append(f"{title} 넘버")
    if "무용" in genre or "발레" in genre:
        queries.append(f"{title} 공연")

    # 중복 제거(순서 유지)
    return list(dict.fromkeys(queries))


def main():
    ap = argparse.ArgumentParser(description="저조 매칭 공연 타겟 추가 YouTube 수집")
    ap.add_argument("--targets", required=True, help="missing_or_low_coverage_perfs.csv 경로")
    ap.add_argument("--excluded-ids", required=True, help="excluded_video_ids.txt 경로")
    ap.add_argument("--api-key", default=os.environ.get("YOUTUBE_API_KEY"))
    ap.add_argument("--max-videos-per-query", type=int, default=15)
    ap.add_argument("--max-comments-per-video", type=int, default=30)
    ap.add_argument("--out-dir", default="./output_targeted")
    ap.add_argument("--state-file", default=None)
    ap.add_argument("--shard-index", type=int, default=None)
    ap.add_argument("--shard-count", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None, help="이번 실행에서 처리할 공연 수 제한 (quota 관리용)")
    args = ap.parse_args()

    if not args.api_key:
        print("API 키가 필요해요. --api-key 인자나 YOUTUBE_API_KEY 환경변수를 설정해주세요.")
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)

    performances = load_target_performances(args.targets)
    excluded_ids = load_excluded_ids(args.excluded_ids)
    print(f"타겟 공연 {len(performances)}개, 제외 video_id {len(excluded_ids)}개 로드")

    if args.shard_count:
        before = len(performances)
        performances = yc.partition_shard(performances, args.shard_index, args.shard_count)
        print(f"shard {args.shard_index}/{args.shard_count}: {before}개 중 {len(performances)}개 담당")

    if args.state_file:
        processed = yc.load_processed_ids(args.state_file)
        before = len(performances)
        performances = [p for p in performances if p["perf_id"] not in processed]
        print(f"이미 처리된 공연 {before - len(performances)}개 제외, 남은 공연 {len(performances)}개")

    if args.limit:
        performances = performances[: args.limit]

    print(f"이번 실행에서 처리할 공연: {len(performances)}개")

    perf_video_map = []
    done_perf_ids = []
    quota_hit = False

    for idx, perf in enumerate(performances, 1):
        queries = build_queries(perf)
        print(f"[{idx}/{len(performances)}] '{perf['title']}' — 쿼리 {len(queries)}개")

        found_ids = []
        try:
            for q in queries:
                found_ids.extend(
                    search_videos_ext(args.api_key, q, max_results=args.max_videos_per_query)
                )
                time.sleep(1.2)  # 쿼리마다 살짝 쉬어서 분당 한도에 안 걸리게
            # 짧은 콘텐츠(쇼츠) 누락 방지용 별도 검색 (원제목만, videoDuration=short)
            found_ids.extend(
                search_videos_ext(
                    args.api_key, perf["title"], max_results=args.max_videos_per_query, video_duration="short"
                )
            )
            time.sleep(1.2)
        except yc.QuotaExceededError:
            print("  -> 일일 quota 초과. 지금까지 모은 데이터를 저장하고 종료할게요.")
            quota_hit = True
            break

        # 이미 확인된 video_id는 제외 (quota 절약 + 중복 방지)
        new_ids = [v for v in dict.fromkeys(found_ids) if v not in excluded_ids]
        skipped = len(set(found_ids)) - len(new_ids)
        if skipped:
            print(f"  이미 확인된 영상 {skipped}건 제외, 신규 후보 {len(new_ids)}건")

        done_perf_ids.append(perf["perf_id"])
        perf_video_map.append((perf, new_ids))
        time.sleep(0.1)

    all_video_ids = []
    for _, vids in perf_video_map:
        all_video_ids.extend(vids)
    unique_video_ids = list(dict.fromkeys(all_video_ids))

    print(f"\n고유 신규 영상 {len(unique_video_ids)}개 메타데이터 조회 중...")
    video_detail_list = yc.get_video_details(args.api_key, unique_video_ids)
    video_detail_by_id = {d["video_id"]: d for d in video_detail_list}

    unique_channel_ids = list(dict.fromkeys(d["channel_id"] for d in video_detail_list if d["channel_id"]))
    channel_info = yc.get_channel_details(args.api_key, unique_channel_ids)

    comments_by_video = {}
    if args.max_comments_per_video > 0:
        for vid in unique_video_ids:
            comments_by_video[vid] = yc.get_comments(args.api_key, vid, max_comments=args.max_comments_per_video)
            time.sleep(0.05)

    all_videos = []
    all_comments = []

    for perf, video_ids in perf_video_map:
        for vid in video_ids:
            base = video_detail_by_id.get(vid)
            if not base:
                continue
            row = dict(base)
            ch = channel_info.get(row["channel_id"], {})
            row.update(
                {
                    "channel_subscriber_count": ch.get("subscriber_count", ""),
                    "channel_video_count": ch.get("channel_video_count", ""),
                    "matched_perf_id": perf["perf_id"],
                    "matched_title": perf["title"],
                    "matched_genre": perf.get("genre", ""),
                    "matched_venue": perf.get("venue_name", ""),
                    "matched_perf_start": perf.get("perf_start_date", ""),
                    "matched_perf_end": perf.get("perf_end_date", ""),
                    "video_type": "",
                    "source_type": "",
                    "notes": "",
                }
            )
            # 사전 스코어링 바로 적용 -> 사람이 review할 때 signal_score/flag_reason부터 봄
            row.update(score_row(row))
            all_videos.append(row)

        if args.max_comments_per_video > 0:
            for vid in video_ids:
                all_comments.extend(comments_by_video.get(vid, []))

    candidates_path = os.path.join(args.out_dir, "candidates.csv")
    comments_path = os.path.join(args.out_dir, "comments.csv")

    if args.state_file:
        yc.append_csv(candidates_path, all_videos)
        yc.append_csv(comments_path, all_comments)
        yc.append_processed_ids(args.state_file, done_perf_ids)
    else:
        yc.write_csv(candidates_path, all_videos)
        yc.write_csv(comments_path, all_comments)

    print(f"\n완료: 후보 영상 {len(all_videos)}개 -> {candidates_path}")
    print(f"완료: 댓글 {len(all_comments)}개 -> {comments_path}")
    if quota_hit:
        sys.exit(0)


if __name__ == "__main__":
    main()
