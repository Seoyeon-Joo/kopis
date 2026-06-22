"""
Phase 3: 청크 병합 + data/ 폴더 정리
- artifacts/  : GitHub Actions 다운로드 경로
- data/       : 최종 CSV 저장 경로
"""
import os, csv, glob
from collections import defaultdict

ARTIFACTS_DIR = "artifacts"
OUT_DIR       = "data"
os.makedirs(OUT_DIR, exist_ok=True)


def read_csv(path):
    try:
        with open(path, encoding="utf-8-sig", errors="ignore") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        print(f"  [오류] 읽기 실패 {path}: {e}")
        return []


def write_csv(rows, path):
    if not rows:
        print(f"  데이터 없음, 건너뜀: {path}")
        return
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    size_kb = os.path.getsize(path) / 1024
    print(f"  저장: {os.path.basename(path)}  ({len(rows):,}행, {size_kb:.0f} KB)")


def main():
    all_csvs = glob.glob(os.path.join(ARTIFACTS_DIR, "**", "*.csv"), recursive=True)
    print(f"발견된 CSV 파일: {len(all_csvs)}개\n")

    # 청크 파일 vs 단일 파일 분류
    chunk_groups = defaultdict(list)
    direct_files = {}   # base_name → path (중복은 마지막 것 사용)

    for path in sorted(all_csvs):
        base = os.path.basename(path)
        if "_chunk" in base:
            prefix = base.split("_chunk")[0]   # e.g. "02_공연상세"
            chunk_groups[prefix].append(path)
        else:
            # .txt IDs 파일 무시, ids_ 파일 무시
            if base.endswith(".csv"):
                direct_files[base] = path

    # 청크 병합
    for prefix, files in sorted(chunk_groups.items()):
        print(f"[병합] {prefix}  ({len(files)}개 청크)")
        rows = []
        for f in sorted(files):
            rows.extend(read_csv(f))
        write_csv(rows, os.path.join(OUT_DIR, f"{prefix}.csv"))

    # 단일 파일 복사
    for base, path in sorted(direct_files.items()):
        print(f"[복사] {base}")
        rows = read_csv(path)
        write_csv(rows, os.path.join(OUT_DIR, base))

    print(f"\n완료! 최종 파일: {OUT_DIR}/")
    for f in sorted(os.listdir(OUT_DIR)):
        size = os.path.getsize(os.path.join(OUT_DIR, f))
        print(f"  {f:45s}  {size/1024:>8.0f} KB")


if __name__ == "__main__":
    main()
