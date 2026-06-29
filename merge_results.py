"""
shard_0, shard_1, ... shard_N 폴더에 흩어진 videos.csv / comments.csv를
각각 하나의 all_videos.csv / all_comments.csv로 합치는 스크립트

사용법:
    pip install pandas
    python merge_results.py --base-dir data/youtube --out-dir data/youtube

주의:
- 같은 영상이 여러 공연(performance)에 매칭된 경우, 영상은 매칭된 횟수만큼
  여러 행으로 나올 수 있어요. 이건 의도된 동작이에요 (한 행 = 한 (공연, 영상) 쌍).
- 혹시 같은 shard를 중복 실행해서 (video_id, matched_perf_id) 쌍이
  완전히 똑같이 두 번 들어간 경우는 따로 경고만 출력해요 (자동으로 지우지 않음 -
  의도된 중복인지 실수인지는 직접 판단하시는 게 안전해서요).
"""

import argparse
import glob
import os
import pandas as pd


def merge_one_file_type(base_dir, filename, output_path):
    pattern = os.path.join(base_dir, "shard_*", filename)
    paths = sorted(glob.glob(pattern))

    if not paths:
        print(f"합칠 파일을 못 찾았어요: {pattern}")
        return None

    dfs = []
    empty_count = 0
    for p in paths:
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            empty_count += 1
            continue
        if df.empty:
            empty_count += 1
            continue
        shard_name = os.path.basename(os.path.dirname(p))
        df["source_shard"] = shard_name
        dfs.append(df)

    if not dfs:
        print(f"{filename}: 찾은 shard {len(paths)}개 중 데이터가 있는 게 하나도 없어요.")
        return None

    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(
        f"{filename}: shard {len(paths)}개 중 데이터 있는 {len(dfs)}개 "
        f"(빈 파일 {empty_count}개) -> 총 {len(merged)}행 -> {output_path}"
    )
    return merged


def main():
    parser = argparse.ArgumentParser(description="shard별 videos.csv/comments.csv를 하나로 합치기")
    parser.add_argument("--base-dir", default="data/youtube", help="shard_* 폴더들이 있는 상위 경로")
    parser.add_argument("--out-dir", default="data/youtube", help="합쳐진 결과를 저장할 경로")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    videos = merge_one_file_type(
        args.base_dir, "videos.csv", os.path.join(args.out_dir, "all_videos.csv")
    )
    merge_one_file_type(
        args.base_dir, "comments.csv", os.path.join(args.out_dir, "all_comments.csv")
    )

    if videos is not None and "video_id" in videos.columns and "matched_perf_id" in videos.columns:
        dup_count = videos.duplicated(subset=["video_id", "matched_perf_id"]).sum()
        if dup_count:
            print(
                f"\n참고: (video_id, matched_perf_id) 기준으로 완전히 똑같은 행이 "
                f"{dup_count}건 있어요. (같은 shard를 두 번 실행했거나 비슷한 이유일 수 있어요.) "
                f"필요하면 직접 확인 후 제거하세요: "
                f"all_videos.csv를 ['video_id','matched_perf_id'] 기준으로 drop_duplicates"
            )

        print(f"\n장르별 매칭 영상 행 수:")
        if "matched_genre" in videos.columns:
            print(videos["matched_genre"].value_counts(dropna=False).to_string())

        unique_perfs = videos["matched_perf_id"].nunique()
        print(f"\n영상이 1개 이상 매칭된 고유 공연 수: {unique_perfs}개")


if __name__ == "__main__":
    main()
