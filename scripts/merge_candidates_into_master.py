# -*- coding: utf-8 -*-
"""
merge_candidates_into_master.py
================================
data/youtube_targeted/shard_*/candidates.csv 에 흩어진 신규 후보 영상들을
suggested_status(수집 시점에 signal_scorer가 이미 매긴 값) 기준으로
기존 마스터 파일에 정식으로 합친다.

분류 기준
--------
- verified_candidate       -> data/all_videos_verified.csv 에 추가
- delete_candidate         -> data/videos_to_delete_from_targeted.csv 에 추가
                              (기존 videos_to_delete_part1~3.csv는 이미 25MB
                              한도에 가까워서 안 건드리고, 신규 파일로 분리)
- no_strong_signal         -> data/videos_to_review.csv 에 추가
                              (기존 파일과 다르게, 여긴 "새로 발견된 영상이라
                              아예 판단 근거가 없는 것"이라 사람이 봐야 함)

댓글(comments.csv)은 이번엔 다루지 않는다 (나중에 별도 처리).

중복 방지
--------
이미 all_videos_verified.csv / videos_to_review.csv / videos_to_delete*.csv에
있는 video_id는 다시 추가하지 않는다 (excluded_video_ids.txt 활용).

사용법
------
    python scripts/merge_candidates_into_master.py --data-dir data
"""

import os
import csv
import glob
import argparse


def load_excluded_ids(data_dir):
    path = os.path.join(data_dir, "excluded_video_ids.txt")
    if not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def load_master_columns(data_dir):
    """ 기존 all_videos_verified.csv의 컬럼 순서를 기준 스키마로 사용 """
    path = os.path.join(data_dir, "all_videos_verified.csv")
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        return next(reader)


def load_candidates(data_dir):
    pattern = os.path.join(data_dir, "youtube_targeted", "shard_*", "candidates.csv")
    files = sorted(glob.glob(pattern))
    rows = []
    for path in files:
        try:
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows.extend(reader)
        except Exception as e:
            print(f"[경고] {path} 읽기 실패: {e}")
    return files, rows


def append_rows(path, rows, fieldnames):
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description="타겟 수집 후보를 마스터 파일에 병합")
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()

    excluded_ids = load_excluded_ids(args.data_dir)
    master_cols = load_master_columns(args.data_dir)
    files, candidates = load_candidates(args.data_dir)

    print(f"shard 후보 파일 {len(files)}개, 총 {len(candidates)}행 로드")

    # video_id 기준 중복 제거 (여러 shard가 같은 영상을 찾았을 수 있음)
    seen = set()
    deduped = []
    for row in candidates:
        vid = row.get("video_id")
        if not vid or vid in seen or vid in excluded_ids:
            continue
        seen.add(vid)
        deduped.append(row)

    print(f"중복/기존 제외 후 신규 후보: {len(deduped)}건")

    to_verified, to_review, to_delete = [], [], []
    for row in deduped:
        status = row.get("suggested_status", "")
        if status == "verified_candidate":
            to_verified.append(row)
        elif status == "delete_candidate":
            to_delete.append(row)
        else:  # no_strong_signal 포함, 판단 근거 부족한 건 전부 review로
            to_review.append(row)

    print(f"  -> verified: {len(to_verified)}건")
    print(f"  -> review: {len(to_review)}건")
    print(f"  -> delete: {len(to_delete)}건")

    for row in to_verified:
        row["correction_note"] = "youtube_collect_targeted + signal_scorer: 신규 verified"
    for row in to_review:
        row["correction_note"] = "youtube_collect_targeted: 신규 발견, 신호 부족으로 review 필요"
    for row in to_delete:
        row["correction_note"] = "youtube_collect_targeted + signal_scorer: 신규 delete 후보"

    append_rows(os.path.join(args.data_dir, "all_videos_verified.csv"), to_verified, master_cols)
    append_rows(os.path.join(args.data_dir, "videos_to_review.csv"), to_review, master_cols)
    append_rows(os.path.join(args.data_dir, "videos_to_delete_from_targeted.csv"), to_delete, master_cols)

    # excluded_video_ids.txt도 갱신 (다음 수집 라운드에서 또 안 걸리게)
    new_excluded = excluded_ids | seen
    with open(os.path.join(args.data_dir, "excluded_video_ids.txt"), "w", encoding="utf-8") as f:
        for vid in sorted(new_excluded):
            f.write(vid + "\n")

    print(f"\n완료. excluded_video_ids.txt: {len(excluded_ids)} -> {len(new_excluded)}건")

    # 파일 크기 경고 (GitHub 웹 업로드 25MB 한도 참고용)
    verified_path = os.path.join(args.data_dir, "all_videos_verified.csv")
    if os.path.isfile(verified_path):
        size_mb = os.path.getsize(verified_path) / (1024 * 1024)
        if size_mb > 20:
            print(f"[주의] all_videos_verified.csv가 {size_mb:.1f}MB예요. 25MB 넘기 전에 분할을 고려하세요.")


if __name__ == "__main__":
    main()
