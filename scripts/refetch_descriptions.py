#!/usr/bin/env python3
"""
refetch_descriptions.py

YouTube search.list API로 수집되어 description이 ~100자로 잘려있는 CSV를
videos.list API로 재조회해서 전체 description으로 채워주는 스크립트.

scripts/net_utils.py 의 robust_get을 재사용하므로, 이 파일은
kopis 레포의 scripts/ 폴더 안에 같이 두고 실행해야 함.

사용법:
    export YOUTUBE_API_KEYS="key1,key2,key3,..."   # 쉼표로 구분된 키 목록 (기존 파이프라인과 동일)
    python refetch_descriptions.py \
        --input all_videos_gated_post_end.csv \
        --output all_videos_gated_post_end_full.csv \
        --cache description_cache.json

특징:
    - video_id 기준으로 중복 제거 후 조회 (같은 영상 여러 번 호출 안 함)
    - videos.list는 한 번에 최대 50개 id를 배치로 조회 (search.list보다 quota 100배 저렴)
    - API 키 여러 개를 순환 사용, 403(quotaExceeded)이 뜨면 다음 키로 자동 전환
    - 네트워크 일시 오류는 net_utils.robust_get의 지수 백오프로 처리
    - 중간 결과를 JSON 캐시에 저장 -> 스크립트가 중단돼도 이어서 실행 가능
    - 삭제/비공개 전환된 영상은 결과에서 빠지므로, 그런 video_id는
      원래 description(잘린 snippet)을 그대로 유지함 (데이터 유실 아님)
"""

import argparse
import csv
import json
import os
import sys
import time

from net_utils import robust_get

API_BASE = "https://www.googleapis.com/youtube/v3/videos"
BATCH_SIZE = 50  # videos.list 최대 id 개수
SLEEP_BETWEEN_CALLS = 0.05  # 초당 호출 제한 여유


def load_api_keys():
    raw = os.environ.get("YOUTUBE_API_KEYS", "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    if not keys:
        sys.exit(
            "환경변수 YOUTUBE_API_KEYS 가 비어있습니다. "
            "예: export YOUTUBE_API_KEYS=\"AIzaSy...,AIzaSy...\""
        )
    return keys


class KeyRotator:
    """API 키를 순환하며, quota 초과(403) 시 다음 키로 넘어감."""

    def __init__(self, keys):
        self.keys = keys
        self.idx = 0
        self.exhausted = set()

    def current(self):
        if len(self.exhausted) >= len(self.keys):
            sys.exit("모든 API 키의 quota가 소진되었습니다. 내일 다시 시도하거나 키를 추가하세요.")
        return self.keys[self.idx]

    def rotate(self):
        self.exhausted.add(self.idx)
        self.idx = (self.idx + 1) % len(self.keys)
        while self.idx in self.exhausted and len(self.exhausted) < len(self.keys):
            self.idx = (self.idx + 1) % len(self.keys)


def fetch_batch(video_ids, rotator, max_retries=5):
    """video_ids(최대 50개)에 대해 videos.list 호출, {video_id: description} 반환."""
    ids_param = ",".join(video_ids)
    for attempt in range(max_retries):
        key = rotator.current()
        params = {
            "part": "snippet",
            "id": ids_param,
            "key": key,
            "maxResults": 50,
        }
        resp = robust_get(API_BASE, params=params, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            result = {}
            for item in data.get("items", []):
                vid = item["id"]
                desc = item.get("snippet", {}).get("description", "")
                result[vid] = desc
            return result

        if resp.status_code == 403:
            body = resp.text
            if "quotaExceeded" in body or "dailyLimitExceeded" in body:
                print(f"  [키 {rotator.idx} quota 소진, 다음 키로 전환]", file=sys.stderr)
                rotator.rotate()
                continue
            print(f"  [403 에러, 배치 스킵] {body[:200]}", file=sys.stderr)
            return {}

        if resp.status_code == 400:
            print(f"  [400 에러, 배치 스킵] {resp.text[:200]}", file=sys.stderr)
            return {}

        print(f"  [HTTP {resp.status_code} 에러, 재시도 {attempt+1}/{max_retries}] {resp.text[:200]}", file=sys.stderr)
        time.sleep(2 ** attempt)

    print(f"  [배치 실패, 스킵] {video_ids[:3]}...", file=sys.stderr)
    return {}


def load_cache(cache_path):
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache_path, cache):
    if not cache_path:
        return
    tmp = cache_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    os.replace(tmp, cache_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="원본 CSV (video_id, description 컬럼 필요)")
    ap.add_argument("--output", required=True, help="description이 채워진 결과 CSV 경로")
    ap.add_argument("--cache", default="description_cache.json", help="중간 결과 캐시 파일 (이어하기용)")
    ap.add_argument("--dry-run", action="store_true", help="실제 API 호출 없이 대상 개수만 확인")
    args = ap.parse_args()

    with open(args.input, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    if "video_id" not in fieldnames or "description" not in fieldnames:
        sys.exit("입력 CSV에 video_id / description 컬럼이 필요합니다.")

    unique_ids = sorted({r["video_id"] for r in rows if r["video_id"]})
    print(f"전체 행: {len(rows)} / 고유 video_id: {len(unique_ids)}")

    cache = load_cache(args.cache)
    todo = [vid for vid in unique_ids if vid not in cache]
    print(f"이미 캐시됨: {len(unique_ids) - len(todo)} / 새로 조회 필요: {len(todo)}")

    if args.dry_run:
        print(f"[dry-run] 필요한 API 호출 수(배치 {BATCH_SIZE}개씩): {(len(todo) + BATCH_SIZE - 1) // BATCH_SIZE}")
        return

    if todo:
        rotator = KeyRotator(load_api_keys())
        batches = [todo[i:i + BATCH_SIZE] for i in range(0, len(todo), BATCH_SIZE)]
        for i, batch in enumerate(batches, 1):
            result = fetch_batch(batch, rotator)
            for vid in batch:
                # API 응답에 없으면(삭제/비공개) None으로 표시해서 원본 유지
                cache[vid] = result.get(vid)
            if i % 20 == 0 or i == len(batches):
                print(f"  진행: {i}/{len(batches)} 배치 완료 ({len(cache)} / {len(unique_ids)} 캐시됨)")
                save_cache(args.cache, cache)
            time.sleep(SLEEP_BETWEEN_CALLS)
        save_cache(args.cache, cache)

    # 원본 rows에 병합
    filled = 0
    kept_original = 0
    for r in rows:
        vid = r["video_id"]
        new_desc = cache.get(vid)
        if new_desc is not None and new_desc != "":
            r["description"] = new_desc
            filled += 1
        else:
            kept_original += 1  # 삭제/비공개 등으로 못 가져온 경우 기존 값 유지

    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"완료: {filled}개 행 description 갱신, {kept_original}개 행 기존값 유지(조회 실패/삭제된 영상)")
    print(f"결과 저장: {args.output}")


if __name__ == "__main__":
    main()
