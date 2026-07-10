# -*- coding: utf-8 -*-
"""
signal_scorer.py
=================
영상 title/description에서 구조적 신호(날짜, 장소, 해시태그)를 뽑아서
matched_perf_* 정보와 대조한 뒤 신뢰도 점수를 매기는 스크립트.

목적
----
1. 지금까지 텍스트 유사도(단어 겹침)만으로 verified/review/delete를 나누던 걸
   보완해서, 공연 영상 특유의 구조(날짜/장소/해시태그 클러스터)를 자동으로
   점수화한다.
2. 기존 all_videos_verified.csv / videos_to_review.csv / videos_to_delete.csv를
   다시 스코어링해서, "점수는 낮은데 verified로 들어간 것" / "점수는 높은데
   delete/review에 있는 것"을 찾아 재검토 대상으로 뽑아낸다.
3. 앞으로 신규 수집분에도 그대로 적용해서 review 큐에 들어가는 양 자체를
   줄인다.

사용법
------
    python signal_scorer.py \
        --input all_videos_verified.csv videos_to_review.csv videos_to_delete.csv \
        --out scored_videos.csv

출력 컬럼 (기존 컬럼 + 아래 추가)
--------------------------------
    date_signal        : "match" / "mismatch" / "none"
    venue_signal       : "match" / "mismatch" / "none"
    hashtag_categories : 몇 개 카테고리(장르/맥락/팬덤)에 해시태그가 있었는지 (0~3)
    is_condensed_official : True/False (공식 축약 콘텐츠 시리즈 여부)
    signal_score       : 종합 점수 (대략 -3 ~ +6)
    suggested_status   : "verified" / "review" / "delete" (스크립트의 제안, 최종 판단은 사람)
    flag_reason        : 왜 이 점수/제안이 나왔는지 요약
"""

import re
import csv
import argparse
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# 1. 날짜 파싱
# ---------------------------------------------------------------------------

DATE_RANGE_PATTERNS = [
    # 2024.03.10 ~ 2024.04.02 / 2024-03-10~2024-04-02
    re.compile(
        r"(?P<y1>20\d{2})[.\-/](?P<m1>\d{1,2})[.\-/](?P<d1>\d{1,2})\s*[~\-–]\s*"
        r"(?:(?P<y2>20\d{2})[.\-/])?(?P<m2>\d{1,2})[.\-/](?P<d2>\d{1,2})"
    ),
    # 2024.03.10 (연도 있는 단일 날짜)
    re.compile(r"(?P<y1>20\d{2})[.\-/](?P<m1>\d{1,2})[.\-/](?P<d1>\d{1,2})"),
]

# 연도 없이 "3.10~4.02" 같은 경우, published_at의 연도를 기준으로 보정
SHORT_RANGE_PATTERN = re.compile(
    r"(?<!\d)(?P<m1>\d{1,2})[.\-/](?P<d1>\d{1,2})\s*[~\-–]\s*(?P<m2>\d{1,2})[.\-/](?P<d2>\d{1,2})(?!\d)"
)


def _safe_date(y, m, d):
    try:
        return datetime(int(y), int(m), int(d)).date()
    except (ValueError, TypeError):
        return None


def extract_date_range(text, fallback_year=None):
    """
    설명란 텍스트에서 날짜(범위 포함)를 최대한 뽑아낸다.
    반환: (start_date, end_date) 튜플의 리스트 (못 찾으면 빈 리스트)
    단일 날짜만 있으면 (date, date)로 반환.
    """
    if not text:
        return []

    found = []

    for pat in DATE_RANGE_PATTERNS:
        for m in pat.finditer(text):
            gd = m.groupdict()
            y1 = gd.get("y1")
            m1, d1 = gd.get("m1"), gd.get("d1")
            y2 = gd.get("y2") or y1
            m2, d2 = gd.get("m2"), gd.get("d2")
            start = _safe_date(y1, m1, d1)
            if m2 and d2:
                end = _safe_date(y2, m2, d2)
            else:
                end = start
            if start:
                found.append((start, end or start))

    if not found and fallback_year:
        for m in SHORT_RANGE_PATTERN.finditer(text):
            gd = m.groupdict()
            start = _safe_date(fallback_year, gd["m1"], gd["d1"])
            end = _safe_date(fallback_year, gd["m2"], gd["d2"])
            if start:
                found.append((start, end or start))

    return found


def date_signal(description, matched_start, matched_end, published_at, buffer_days=90):
    """
    설명란에서 뽑은 날짜(들)이 matched_perf_start~end와 겹치거나
    buffer_days 이내로 인접하면 'match', 완전히 동떨어져 있으면 'mismatch',
    설명란에 날짜 정보 자체가 없으면 'none'.
    """
    fallback_year = None
    if published_at:
        try:
            fallback_year = int(str(published_at)[:4])
        except ValueError:
            pass

    ranges = extract_date_range(description, fallback_year=fallback_year)
    if not ranges:
        return "none", None

    try:
        p_start = datetime.strptime(str(matched_start), "%Y-%m-%d").date()
        p_end = datetime.strptime(str(matched_end), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return "none", None

    window_start = p_start - timedelta(days=buffer_days)
    window_end = p_end + timedelta(days=buffer_days)

    for start, end in ranges:
        if start <= window_end and end >= window_start:
            return "match", (start, end)

    return "mismatch", ranges[0]


# ---------------------------------------------------------------------------
# 2. 장소 파싱
# ---------------------------------------------------------------------------

def normalize_venue_core(venue_name):
    """
    '두산아트센터 연강홀' -> '두산아트센터' 처럼 공연장 핵심 고유명사만 추출.
    괄호/공백 뒤 상세관 이름(홀/극장/씨어터 등)은 잘라낸다.
    """
    if not venue_name:
        return ""
    v = re.sub(r"\[.*?\]", "", venue_name)  # [서울] 같은 지역 표기 제거
    v = v.strip()
    # 첫 공백 전까지를 핵심 명칭으로 (대부분 '기관명 + 관 이름' 구조)
    core = re.split(r"\s+", v)[0]
    return core


def venue_signal(text, matched_venue):
    if not matched_venue:
        return "none"
    core = normalize_venue_core(matched_venue)
    if not core or len(core) < 2:
        return "none"
    if core in (text or ""):
        return "match"
    return "none"  # 명시적으로 다른 장소가 나오는 경우까지 잡으려면 별도 극장 DB 필요


# ---------------------------------------------------------------------------
# 3. 해시태그 클러스터
# ---------------------------------------------------------------------------

GENRE_TAGS = {
    "#뮤지컬", "#연극", "#무용", "#발레", "#현대무용", "#국악", "#오페라",
    "#클래식", "#오케스트라", "#교향악단", "#합창", "#합창단", "#판소리",
    "#서커스", "#마술", "#복합", "#대중음악", "#콘서트",
}
CONTEXT_TAGS = {
    "#공연", "#공연정보", "#공연스타그램", "#관극", "#프레스콜", "#커튼콜",
    "#티켓오픈", "#예매", "#공연추천", "#공연리뷰", "#무대", "#캐스팅",
}
FAN_CONDENSED_TAGS = {
    "#뮤덕", "#관극일지", "#입덕", "#넘버추천", "#일분뮤지컬", "#1분뮤지컬",
    "#하이라이트", "#넘버소개", "#음악추천", "#연극덕후", "#뮤지컬추천",
}

UNRELATED_TAGS = {
    "#브이로그", "#맛집", "#일상", "#여행", "#리뷰", "#언박싱", "#운세", "#사주",
}

CONDENSED_SERIES_MARKERS = [
    "일분뮤지컬", "1분뮤지컬", "분뮤지컬", "오늘의 넘버", "넘버 소개", "넘버소개",
    "하이라이트 모음", "쇼츠로 보는", "초 요약", "초요약",
]
# '출처'/'공식' 단독은 클래식 채널 boilerplate 설명(공연 문의, 저작권 안내 등)에도
# 흔히 등장해서 오탐이 많다. 반드시 '축약/요약/큐레이션 콘텐츠'라는 취지가
# 분명한 구체적 표현만 인정한다.
OFFICIAL_SOURCE_MARKERS = [
    "콘텐츠 제작사", "제작사 라이브", "공식 하이라이트", "공식 요약",
    "넘버 소개", "오늘의 넘버",
]


def extract_hashtags(text):
    if not text:
        return set()
    return set(re.findall(r"#[가-힣A-Za-z0-9_]+", text))


def hashtag_signal(text):
    tags = extract_hashtags(text)
    categories_hit = 0
    if tags & GENRE_TAGS:
        categories_hit += 1
    if tags & CONTEXT_TAGS:
        categories_hit += 1
    if tags & FAN_CONDENSED_TAGS:
        categories_hit += 1
    unrelated_hit = bool(tags & UNRELATED_TAGS) and not (tags & GENRE_TAGS)
    return categories_hit, unrelated_hit


PERFORMANCE_KEYWORDS = ["뮤지컬", "연극", "무용", "발레", "오페라", "국악", "공연", "커튼콜", "프레스콜", "넘버"]


def is_condensed_official(title, description):
    """
    '출처'/'공식' 같은 단어는 방송사 다큐/뉴스에도 흔히 나오므로 그것만으로는
    인정하지 않는다. 아래 둘 중 하나일 때만 공식 축약 콘텐츠로 인정:
      (a) '일분뮤지컬' 류의 구체적 시리즈 마커가 있음 (그 자체로 충분히 특이함)
      (b) 출처/공식 마커 + 공연 관련 키워드(뮤지컬/연극/공연 등)가 함께 있음
    """
    combined = f"{title or ''} {description or ''}"
    has_series_marker = any(m in combined for m in CONDENSED_SERIES_MARKERS)
    if has_series_marker:
        return True
    has_source_marker = any(m.lower() in combined.lower() for m in OFFICIAL_SOURCE_MARKERS)
    has_perf_keyword = any(k in combined for k in PERFORMANCE_KEYWORDS)
    return has_source_marker and has_perf_keyword


QUOTE_PAIRS = [("'", "'"), ('"', '"'), ("「", "」"), ("『", "』"), ("《", "》"), ("〈", "〉")]


def extract_core_title(matched_title):
    """
    '국립발레단, 호두까기인형 [서울 서초]' -> '호두까기인형'
    '마리 퀴리 [안동]' -> '마리 퀴리'
    콤마(,)가 있으면 기관명을 제외한 뒤쪽(작품명)을 우선 사용.
    """
    if not matched_title:
        return ""
    t = re.sub(r"\[.*?\]", "", matched_title).strip()
    if "," in t:
        t = t.split(",", 1)[1].strip()
    return t


def explicit_title_quote_match(title, description, matched_title):
    """
    영상 제목/설명에 작품명이 따옴표나 괄호로 명시적으로 인용됐는지 확인.
    예: 뮤지컬 '마리퀴리', [마리퀴리], 연극「사의찬미」
    단순 텍스트 겹침보다 훨씬 강한 신호(의도적으로 그 작품을 가리킨다는 뜻).
    """
    core = extract_core_title(matched_title)
    if not core or len(core) < 2:
        return False
    combined = f"{title or ''} {description or ''}"
    core_nospace = core.replace(" ", "")
    for open_q, close_q in QUOTE_PAIRS:
        pattern = re.escape(open_q) + r"\s*" + re.escape(core_nospace) + r"\s*" + re.escape(close_q)
        if re.search(pattern, combined.replace(" ", "")):
            return True
    # 대괄호 [작품명] 형태도 확인
    if re.search(r"\[\s*" + re.escape(core_nospace) + r"\s*\]", combined.replace(" ", "")):
        return True
    return False


# ---------------------------------------------------------------------------
# 4. 종합 스코어링
# ---------------------------------------------------------------------------

def score_row(row):
    title = row.get("video_title", "")
    desc = row.get("description", "")
    combined = f"{title}\n{desc}"

    d_sig, d_range = date_signal(
        desc,
        row.get("matched_perf_start"),
        row.get("matched_perf_end"),
        row.get("published_at"),
    )
    v_sig = venue_signal(combined, row.get("matched_venue"))
    cat_hit, unrelated_hit = hashtag_signal(combined)
    condensed = is_condensed_official(title, desc)
    quoted_match = explicit_title_quote_match(title, desc, row.get("matched_title"))

    score = 0
    reasons = []

    if quoted_match:
        score += 2
        reasons.append("작품명이 따옴표/괄호로 명시적으로 인용됨")

    if d_sig == "match":
        score += 2
        reasons.append("날짜 일치")
    elif d_sig == "mismatch":
        score -= 2
        reasons.append("날짜 불일치(다른 회차 가능성)")

    if v_sig == "match":
        score += 2
        reasons.append("장소 일치")

    score += cat_hit  # 0~3
    if cat_hit >= 2:
        reasons.append(f"해시태그 {cat_hit}개 카테고리 일치")

    if unrelated_hit:
        score -= 3
        reasons.append("무관 해시태그 발견")

    if condensed:
        score += 1
        reasons.append("공식 축약 콘텐츠 시리즈로 추정")

    # 여기서는 3분류를 강제로 나누지 않는다. "신호가 없으면 기존 판정을 건드리지 않는다"가
    # 기본 원칙. 뚜렷한 근거(강한 양성 / 강한 음성)가 있을 때만 재검토 후보로 표시한다.
    if condensed:
        suggested = "verified_candidate"  # 공식 축약 시리즈는 delete/review에 있으면 복구 후보
    elif score >= 3:
        suggested = "verified_candidate"  # 날짜+장소+해시태그 등 강한 양성 근거
    elif unrelated_hit or score <= -2:
        suggested = "delete_candidate"    # 무관 해시태그 또는 날짜 불일치 등 강한 음성 근거
    else:
        suggested = "no_strong_signal"    # 신호 부족 -> 기존 판정 유지, 재검토 대상 아님

    return {
        "date_signal": d_sig,
        "venue_signal": v_sig,
        "hashtag_categories": cat_hit,
        "is_condensed_official": condensed,
        "explicit_title_quote_match": quoted_match,
        "signal_score": score,
        "suggested_status": suggested,
        "flag_reason": "; ".join(reasons) if reasons else "신호 없음",
    }


# ---------------------------------------------------------------------------
# 5. 실행부
# ---------------------------------------------------------------------------

def load_rows(paths):
    rows = []
    for p in paths:
        with open(p, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["_source_file"] = p
                rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser(description="영상 설명란 구조 신호 기반 사전 스코어링")
    ap.add_argument("--input", nargs="+", required=True, help="스코어링할 CSV 파일(들)")
    ap.add_argument("--out", default="scored_videos.csv")
    ap.add_argument(
        "--flag-mismatches-only",
        action="store_true",
        help="기존 파일의 match_status/폴더와 signal 제안이 다른 행만 출력",
    )
    args = ap.parse_args()

    rows = load_rows(args.input)
    print(f"총 {len(rows)}행 로드")

    out_rows = []
    for row in rows:
        sig = score_row(row)
        merged = dict(row)
        merged.update(sig)

        if args.flag_mismatches_only:
            # 원래 어느 파일에서 왔는지로 현재 상태 추정
            src = row["_source_file"].lower()
            if "verified" in src:
                current = "verified"
            elif "review" in src:
                current = "review"
            elif "delete" in src:
                current = "delete"
            else:
                current = "unknown"

            suggested = sig["suggested_status"]
            # 신호 부족(no_strong_signal)은 애초에 재검토 대상이 아님 -> 항상 skip
            if suggested == "no_strong_signal":
                continue
            # 강한 양성 근거인데 이미 verified면 재확인할 필요 없음 -> skip
            if suggested == "verified_candidate" and current == "verified":
                continue
            # 강한 음성 근거인데 이미 delete면 재확인할 필요 없음 -> skip
            if suggested == "delete_candidate" and current == "delete":
                continue

            merged["current_status"] = current

        out_rows.append(merged)

    print(f"출력 {len(out_rows)}행 -> {args.out}")
    if out_rows:
        fieldnames = list(out_rows[0].keys())
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(out_rows)


if __name__ == "__main__":
    main()
