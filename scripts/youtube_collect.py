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

Quota 주의사항
--------------
YouTube Data API 기본 일일 할당량은 10,000 units 예요.
- search.list  : 1회 호출(결과 50개 이하 한 페이지) = 100 units  ← 제일 비쌈
- videos.list / channels.list / commentThreads.list : 1회 호출 = 1 unit
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
            pid = row.get("perf_id") or
