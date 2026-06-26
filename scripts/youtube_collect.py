"""
공연 관련 YouTube 영상 데이터 수집 스크립트
=========================================

기능
----
1. 공연 리스트(CSV)를 입력받아 작품명으로 YouTube 검색
2. 검색된 영상마다 메타데이터 수집:
   제목, 설명("더보기" 전문), 채널명/채널ID, 업로드일, 영상 길이,
   조회수, 좋아요수, 댓글수, 영상 URL
3. 영상별 댓글(상위 N개) 수집
4. 채널별 구독자 수 수집
5. video_type / source_type / notes 컬럼은 빈 칸으로 남겨서
   나중에 수동으로 trailer / condensed / full_broadcast / other 등으로 코딩

준비물
------
1. Google Cloud Console (console.cloud.google.com) 에서:
   - 새 프로젝트 생성 (또는 기존 프로젝트 사용)
   - "YouTube Data API v3" 활성화
   - API 키 발급 (사용자 인증 정보 > API 키 만들기)
2. 발급받은 키를 환경변수로 등록:
     export YOUTUBE_API_KEY="발급받은키"
   (GitHub Actions에서 쓸 거면 레포 Settings > Secrets에 YOUTUBE_API_KEY로 등록)

실행 예시
--------
   python youtube_collect.py --input performances_sample.csv --out-dir ./output

출력
----
- videos.csv   : 영상 단위 메타데이터 (공연 매칭 정보 포함)
- comments.csv : 영상별 댓글 (video_id 로 videos.csv와 JOIN)

Quota 주의사항
--------------
YouTube Data API 기본 일일 할당량은 10,000 units 예요.
- search.list  : 1회 호출(결과 50개 이하 한 페이지) = 100 units  ← 제일 비쌈
- videos.list / channels.list / commentThreads.list : 1회 호출 = 1 unit
즉 공연 1개당 기본 약 100 units 이므로, 하루에 처리 가능한 공연 수는
대략 100개 정도예요(영상/채널/댓글 호출 비용은 미미함).
공연이 그보다 많으면 --limit 으로 잘라서 여러 날에 나눠 돌리거나,
입력 CSV 자체를 사전에 후보군만 추려서 넣어주세요.
"""

import os
import re
import sys
import csv
import time
import argparse
import requests

API_BASE = "https://www.googleapis.com/youtube/v3"
DURATION_PATTERN = re.compile(
    r"PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?"
)


def iso8601_duration_to_seconds(duration: str) -> int:
    """ 'PT1H2M10S' 같은 ISO8601 길이 문자열을 초 단위로 변환 """
    match = DURATION_PATTERN.match(duration or "")
    if not match:
        return 0
    parts = match.groupdict()
    hours = int(parts["hours"] or 0)
    minutes = int(parts["minutes"] or 0)
    seconds = int(parts["seconds"] or 0)
    return hours * 3600 + minutes * 60 + seconds


class QuotaExceededError(Exception):
    pass


def search_videos(api_key, query, max_results=30):
    """
    search.list 호출 -> videoId 리스트 반환
    한 페이지(최대 50개) = 100 units. max_results를 50 넘게 주면
    페이지를 추가로 넘기면서 그만큼 100 units씩 더 소모돼요.
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

    video_ids = []
    next_page_token = None

    while len(video_ids) < max_results:
        if next_page_token:
            params["pageToken"] = next_page_token
        resp = requests.get(f"{API_BASE}/search", params=params, timeout=20)
        data = resp.json()

        if "error" in data:
            msg = data["error"].get("message", "")
            print(f"  [search 오류] '{query}': {msg}")
            if "quota" in msg.lower():
                raise QuotaExceededError(msg)
            break

        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        next_page_token = data.get("nextPageToken")
        if not next_page_token or len(video_ids) >= max_results:
            break
        time.sleep(0.2)

    return video_ids[:max_results]


def get_video_details(api_key, video_ids):
    """ videos.list 호출 -> 영상별 메타데이터 dict 리스트 (50개씩 배치) """
    results = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        params = {
            "key": api_key,
            "id": ",".join(batch),
            "part": "snippet,contentDetails,statistics",
        }
        resp = requests.get(f"{API_BASE}/videos", params=params, timeout=20)
        data = resp.json()

        if "error" in data:
            print(f"  [videos 오류] {data['error'].get('message')}")
            continue

        for item in data.get("items", []):
            sn = item.get("snippet", {})
            cd = item.get("contentDetails", {})
            st = item.get("statistics", {})
            duration_sec = iso8601_duration_to_seconds(cd.get("duration", ""))

            results.append(
                {
                    "video_id": item["id"],
                    "video_title": sn.get("title", ""),
                    "description": sn.get("description", ""),
                    "channel_id": sn.get("channelId", ""),
                    "channel_name": sn.get("channelTitle", ""),
                    "published_at": sn.get("publishedAt", ""),
                    "duration_sec": duration_sec,
                    "duration_min": round(duration_sec / 60, 2),
                    "is_shorts_guess": duration_sec > 0 and duration_sec <= 60,
                    "view_count": st.get("viewCount", ""),
                    "like_count": st.get("likeCount", ""),
                    "comment_count": st.get("commentCount", ""),
                    "video_url": f"https://www.youtube.com/watch?v={item['id']}",
                }
            )
        time.sleep(0.2)
    return results


def get_channel_details(api_key, channel_ids):
    """ channels.list 호출 -> {channel_id: {subscriber_count, channel_video_count}} """
    channel_ids = list(set(c for c in channel_ids if c))
    info = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        params = {
            "key": api_key,
            "id": ",".join(batch),
            "part": "statistics",
        }
        resp = requests.get(f"{API_BASE}/channels", params=params, timeout=20)
        data = resp.json()

        if "error" in data:
            print(f"  [channels 오류] {data['error'].get('message')}")
            continue

        for item in data.get("items", []):
            st = item.get("statistics", {})
            hidden = st.get("hiddenSubscriberCount", False)
            info[item["id"]] = {
                "subscriber_count": "" if hidden else st.get("subscriberCount", ""),
                "channel_video_count": st.get("videoCount", ""),
            }
        time.sleep(0.2)
    return info


def get_comments(api_key, video_id, max_comments=30):
    """
    commentThreads.list 호출 -> 댓글 리스트
    댓글이 비활성화된 영상이면 에러가 나는데, 조용히 빈 리스트를 반환해요.
    """
    comments = []
    params = {
        "key": api_key,
        "videoId": video_id,
        "part": "snippet",
        "maxResults": min(max_comments, 100),
        "order": "relevance",
        "textFormat": "plainText",
    }
    try:
        resp = requests.get(f"{API_BASE}/commentThreads", params=params, timeout=20)
        data = resp.json()
        if "error" in data:
            return comments  # 댓글 비활성화 / 댓글 없음 등
        for item in data.get("items", [])[:max_comments]:
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append(
                {
                    "video_id": video_id,
                    "comment_id": item["id"],
                    "author": top.get("authorDisplayName", ""),
                    "text": top.get("textDisplay", ""),
                    "like_count": top.get("likeCount", ""),
                    "published_at": top.get("publishedAt", ""),
                }
            )
    except Exception as e:
        print(f"  [comments 오류] {video_id}: {e}")
    return comments


def load_performances(csv_path, limit=None):
    """
    공연 CSV를 읽어서 perf_id 기준으로 중복 제거하고,
    ticket_sales_qty 컬럼이 있으면 총 판매량 기준 내림차순 정렬.
    (큰 흥행작부터 처리해야 YouTube 매칭 확률이 높아요)
    """
    rows_by_perf = {}
    ticket_sum = {}

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get("perf_id") or row.get("title")
            if pid not in rows_by_perf:
                rows_by_perf[pid] = {
                    "perf_id": row.get("perf_id", ""),
                    "title": row.get("title", ""),
                    "genre": row.get("genre", ""),
                    "venue_name": row.get("venue_name", ""),
                    "perf_start_date": row.get("perf_start_date", ""),
                    "perf_end_date": row.get("perf_end_date", ""),
                }
            try:
                ticket_sum[pid] = ticket_sum.get(pid, 0) + int(row.get("ticket_sales_qty", 0))
            except (ValueError, TypeError):
                pass

    performances = list(rows_by_perf.values())
    if ticket_sum:
        performances.sort(key=lambda p: ticket_sum.get(p["perf_id"], 0), reverse=True)

    if limit:
        performances = performances[:limit]
    return performances


def write_csv(path, rows):
    if not rows:
        print(f"  (저장할 데이터 없음: {path})")
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path, rows):
    """ 기존 파일이 있으면 이어붙이고, 없으면 새로 만들어요 (여러 날에 나눠 돌릴 때 사용) """
    if not rows:
        print(f"  (저장할 데이터 없음: {path})")
        return
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def load_processed_ids(state_path):
    """ 이미 처리한 perf_id 목록을 불러와요 (없으면 빈 set) """
    if not os.path.isfile(state_path):
        return set()
    with open(state_path, encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def append_processed_ids(state_path, perf_ids):
    """ 처리 완료한 perf_id를 상태 파일에 기록해요 (다음 실행에서 건너뛰기용) """
    with open(state_path, "a", encoding="utf-8") as f:
        for pid in perf_ids:
            f.write(f"{pid}\n")


def main():
    parser = argparse.ArgumentParser(description="공연 관련 YouTube 영상/댓글/채널 데이터 수집")
    parser.add_argument("--input", required=True, help="공연 리스트 CSV 경로")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("YOUTUBE_API_KEY"),
        help="YouTube Data API v3 키 (기본값: YOUTUBE_API_KEY 환경변수)",
    )
    parser.add_argument("--limit", type=int, default=None, help="처리할 공연 수 제한 (quota 관리용)")
    parser.add_argument("--max-videos-per-perf", type=int, default=30, help="공연당 최대 검색 영상 수")
    parser.add_argument("--max-comments-per-video", type=int, default=30, help="영상당 최대 댓글 수")
    parser.add_argument("--out-dir", default="./output", help="출력 폴더")
    parser.add_argument(
        "--state-file",
        default=None,
        help="처리 완료한 perf_id를 기록할 파일 (지정하면 다음 실행에서 자동으로 건너뜀)",
    )
    args = parser.parse_args()

    if not args.api_key:
        print("API 키가 필요해요. --api-key 인자나 YOUTUBE_API_KEY 환경변수를 설정해주세요.")
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)
    performances = load_performances(args.input, limit=None)  # 전체를 먼저 불러오고 아래서 거름

    if args.state_file:
        processed = load_processed_ids(args.state_file)
        before = len(performances)
        performances = [p for p in performances if p["perf_id"] not in processed]
        print(f"이미 처리된 공연 {before - len(performances)}개 제외, 남은 공연 {len(performances)}개")

    if args.limit:
        performances = performances[: args.limit]

    print(f"이번 실행에서 처리할 공연: {len(performances)}개 (총 판매량 기준 내림차순)")

    estimated_units = len(performances) * 100
    print(f"예상 search.list 비용: 약 {estimated_units} units (일일 한도 10,000)")
    if estimated_units > 9000:
        print("⚠️  quota를 거의 다 쓸 것 같아요. --limit으로 줄이는 걸 권장해요.")

    all_videos = []
    all_comments = []
    done_perf_ids = []
    quota_hit = False

    for idx, perf in enumerate(performances, 1):
        title = perf["title"]
        print(f"[{idx}/{len(performances)}] '{title}' 검색 중...")

        try:
            video_ids = search_videos(args.api_key, title, max_results=args.max_videos_per_perf)
        except QuotaExceededError:
            print("  -> 일일 quota를 초과했어요. 지금까지 모은 데이터를 저장하고 종료할게요.")
            quota_hit = True
            break

        done_perf_ids.append(perf["perf_id"])

        if not video_ids:
            print("  검색 결과 없음")
            continue

        details = get_video_details(args.api_key, video_ids)

        # 공연 매칭 정보 + 수동 코딩용 빈 컬럼 추가
        for d in details:
            d.update(
                {
                    "matched_perf_id": perf["perf_id"],
                    "matched_title": perf["title"],
                    "matched_genre": perf["genre"],
                    "matched_venue": perf["venue_name"],
                    "matched_perf_start": perf["perf_start_date"],
                    "matched_perf_end": perf["perf_end_date"],
                    "video_type": "",   # trailer / condensed / full_broadcast / other
                    "source_type": "",  # 프레스콜 / 온라인콜 / 공식MV / 팬채널 / 리뷰채널 등
                    "notes": "",
                }
            )

        # 채널 구독자수
        channel_ids = [d["channel_id"] for d in details]
        channel_info = get_channel_details(args.api_key, channel_ids)
        for d in details:
            ch = channel_info.get(d["channel_id"], {})
            d["channel_subscriber_count"] = ch.get("subscriber_count", "")
            d["channel_video_count"] = ch.get("channel_video_count", "")

        # 댓글
        for d in details:
            cmts = get_comments(args.api_key, d["video_id"], max_comments=args.max_comments_per_video)
            all_comments.extend(cmts)
            time.sleep(0.1)

        all_videos.extend(details)
        time.sleep(0.3)

    videos_path = os.path.join(args.out_dir, "videos.csv")
    comments_path = os.path.join(args.out_dir, "comments.csv")

    if args.state_file:
        # 여러 날에 나눠 돌리는 모드 -> 이어붙이기 + 처리완료 perf_id 기록
        append_csv(videos_path, all_videos)
        append_csv(comments_path, all_comments)
        append_processed_ids(args.state_file, done_perf_ids)
    else:
        # 단발성 실행 -> 새로 덮어쓰기
        write_csv(videos_path, all_videos)
        write_csv(comments_path, all_comments)

    print(f"\n완료: 영상 {len(all_videos)}개 -> {videos_path}")
    print(f"완료: 댓글 {len(all_comments)}개 -> {comments_path}")
    if args.state_file:
        print(f"이번 실행에서 처리 완료한 공연: {len(done_perf_ids)}개 (상태 파일: {args.state_file})")
    if quota_hit:
        sys.exit(0)  # quota 초과는 실패가 아니라 "오늘 분량 끝" 이므로 정상 종료 처리


if __name__ == "__main__":
    main()
