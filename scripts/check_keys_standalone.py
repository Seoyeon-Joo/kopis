# -*- coding: utf-8 -*-
"""
check_keys_standalone.py
=========================
기존 파이프라인(scripts/pipeline.py, .github/workflows/pipeline.yml)과
완전히 독립된 1회성 스크립트. 아무것도 안 바꾸고 그냥 이 파일 하나로 실행.

키 하나당 videos.list(part=id) 1 unit짜리 호출로 상태를 검사한다:
- ok             : 정상
- invalid        : 키 자체가 무효 (삭제된 프로젝트/API 비활성화/오타 등) -> 진짜 죽은 키
- quota_exceeded : 오늘 이미 이 키의 quota를 다 씀 (죽은 키 아님, 내일이면 정상일 수 있음)
- error          : 그 외 예상 못 한 응답

사용법
------
    python check_keys_standalone.py --api-keys "키1,키2,키3,..."
    python check_keys_standalone.py --keys-file keys.txt   # 한 줄에 키 하나씩
"""

import argparse
import csv
import sys
import time

import requests

API_BASE = "https://www.googleapis.com/youtube/v3"
TEST_VIDEO_ID = "dQw4w9WgXcQ"  # 아무 공개 영상 ID (part=id만 요청, 1 unit)


def check_one_key(key, timeout=15):
    """키 하나를 검사해서 (status, detail) 튜플 반환."""
    params = {"part": "id", "id": TEST_VIDEO_ID, "key": key}
    try:
        resp = requests.get(f"{API_BASE}/videos", params=params, timeout=timeout)
    except (requests.ConnectionError, requests.Timeout) as e:
        return "error", f"네트워크 오류: {e}"

    if resp.status_code == 200:
        return "ok", f"{len(resp.json().get('items', []))}건 응답"

    body = resp.text
    if resp.status_code == 400 and (
        "api key not valid" in body.lower() or "api_key_invalid" in body.lower()
    ):
        return "invalid", body[:150]

    if resp.status_code == 403 and "quota" in body.lower():
        return "quota_exceeded", body[:150]

    if resp.status_code == 403 and (
        "per minute" in body.lower() or "rate limit exceeded" in body.lower()
    ):
        return "rate_limited", body[:150]

    return "error", f"HTTP {resp.status_code}: {body[:150]}"


def main():
    ap = argparse.ArgumentParser(description="YouTube API 키 목록 전체 유효성 검사 (독립 실행)")
    ap.add_argument("--api-keys", default=None, help="콤마로 구분된 키 목록")
    ap.add_argument("--keys-file", default=None, help="한 줄에 키 하나씩 있는 파일")
    ap.add_argument("--delay", type=float, default=0.3, help="키 검사 사이 대기시간(초)")
    ap.add_argument("--out", default="key_check_result.csv")
    ap.add_argument("--valid-keys-out", default="valid_keys.txt",
                    help="사용 가능한 키만 콤마로 이어붙인 텍스트 파일 경로")
    args = ap.parse_args()

    if args.keys_file:
        with open(args.keys_file, encoding="utf-8") as f:
            keys = [line.strip() for line in f if line.strip()]
    elif args.api_keys:
        keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    else:
        print("--api-keys 또는 --keys-file 중 하나는 필요해요.")
        sys.exit(1)

    print(f"키 {len(keys)}개 검사 시작 (키당 1 unit, 총 {len(keys)} units 사용)")

    rows = []
    for idx, key in enumerate(keys, 1):
        status, detail = check_one_key(key)
        masked = key[:6] + "..." + key[-4:] if len(key) > 12 else "***"
        rows.append({"key_index": idx, "key_masked": masked, "status": status, "detail": detail})
        if status != "ok":
            print(f"[{idx}/{len(keys)}] {masked} -> {status} ({detail})", flush=True)
        if idx % 50 == 0:
            print(f"[{idx}/{len(keys)}] 진행 중...", flush=True)
        time.sleep(args.delay)

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["key_index", "key_masked", "status", "detail"])
        writer.writeheader()
        writer.writerows(rows)

    # 사용 가능한 키만 콤마로 이어붙인 텍스트 파일 - GitHub Secrets에 바로 붙여넣기용.
    # invalid(진짜 죽은 키)/error(원인 불명)만 빼고, quota_exceeded/rate_limited는
    # 일시적인 상태일 뿐 키 자체는 멀쩡하므로 포함시킴.
    USABLE_STATUSES = {"ok", "quota_exceeded", "rate_limited"}
    usable_keys = [k for k, r in zip(keys, rows) if r["status"] in USABLE_STATUSES]
    with open(args.valid_keys_out, "w", encoding="utf-8") as f:
        f.write(",".join(usable_keys))

    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    print(f"\n=== 결과 요약 ({len(keys)}개 검사) ===")
    for status, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}개")
    print(f"\n상세 결과 -> {args.out}")
    print(f"사용 가능한 키만 콤마로 이어붙인 파일({len(usable_keys)}개) -> {args.valid_keys_out}")
    print("   (이 파일 내용을 그대로 복사해서 GitHub Secrets에 붙여넣으면 돼요)")

    if counts.get("invalid"):
        print(f"\n⚠️  invalid {counts['invalid']}개는 실제로 죽은 키예요. 키 목록에서 제거를 고려하세요.")
    if counts.get("quota_exceeded"):
        print(f"ℹ️  quota_exceeded {counts['quota_exceeded']}개는 오늘 이미 다 쓴 것뿐이라 죽은 키가 아니에요.")
        print("   내일 다시 검사하면 ok로 나올 가능성이 높아요.")


if __name__ == "__main__":
    main()
