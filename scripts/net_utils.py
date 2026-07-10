# -*- coding: utf-8 -*-
"""
net_utils.py
============
YouTube Data API 호출용 공용 네트워크 헬퍼.

GitHub Actions에서 62개 shard가 동시에 googleapis.com에 요청을 보내다 보면
'Connection reset by peer' 같은 일시적 네트워크 오류가 종종 난다.
requests.get을 직접 쓰면 이런 오류에 스크립트 전체가 죽어버리므로,
지수 백오프로 재시도하는 wrapper를 통해서만 호출하도록 한다.
"""

import time
import requests


def robust_get(url, params=None, timeout=20, max_retries=5, backoff_base=3):
    """
    requests.get을 감싸서 네트워크 오류(ConnectionError, Timeout 등) 발생 시
    지수 백오프로 재시도한다. HTTP 응답 자체는 그대로 반환하므로(에러 JSON
    포함), quota 초과 같은 API 레벨 오류 처리는 호출부에서 기존처럼 한다.

    max_retries번 재시도해도 안 되면 마지막 예외를 그대로 올린다.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return requests.get(url, params=params, timeout=timeout)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exc = e
            wait = backoff_base * attempt
            print(f"  [네트워크 오류] {type(e).__name__}, {wait}초 대기 후 재시도 ({attempt}/{max_retries})")
            time.sleep(wait)
    raise last_exc
