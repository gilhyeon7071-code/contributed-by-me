"""속도 제한 응답을 **실제로 알아보는지** (2026-09-22 실측 사고).

2026-09-22 10:36, 예약작업 `kis_intraday_e2e` 3회차가 잔고 조회에서 죽었다(rc=2).
응답은 `EGW00215 / '원장에서 허용 가능한 초당 거래건수를 초과하였습니다.'` 인데
감지 토큰이 `초당 거래건수 초과` 여서 **조사(를) 하나 때문에 안 맞았고**,
`EGW00215` 도 목록에 없어 **재시도가 한 번도 안 걸렸다.**
게다가 그 실패 경보는 텔레그램 토큰 두절(09-21 13:18~09-22 13:42) 때문에 사용자에게 안 갔다.

10-01 에는 아침 배치가 50종목을 연속 발주한다. 이 자리가 막히면 주문이 그냥 실패한다.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from kis_order_client import KISOrderClient as C  # noqa: E402


class _Resp:
    def __init__(self, code=500):
        self.status_code = code


REAL = {"rt_cd": "1", "msg_cd": "EGW00215",
        "msg1": "원장에서 허용 가능한 초당 거래건수를 초과하였습니다."}


def test_the_real_message_is_recognized():
    """09-22 에 실제로 온 본문 그대로."""
    assert C._is_rate_limited(_Resp(500), REAL) is True


def test_code_alone_is_enough():
    """문구가 바뀌어도 코드로 잡힌다 — 브로커 문구는 우리가 정하지 않는다."""
    assert C._is_rate_limited(_Resp(500), {"msg_cd": "EGW00215", "msg1": "알 수 없음"}) is True


@pytest.mark.parametrize("msg", [
    "초당 거래건수를 초과하였습니다",
    "초당 거래건수 초과",
    "초당 거래건수가 초과되었습니다",
])
def test_particles_do_not_break_detection(msg):
    """조사가 무엇이 붙든 걸려야 한다 — 그것 하나로 재시도가 통째로 죽었다."""
    assert C._is_rate_limited(_Resp(500), {"msg1": msg}) is True


@pytest.mark.parametrize("body", [
    {"msg_cd": "EGW00201", "msg1": "접근토큰 발급 잠시 후 다시 시도해주세요"},
    {"msg1": "Too Many Requests"},
    {"msg1": "rate limit exceeded"},
])
def test_other_known_rate_limits_still_caught(body):
    assert C._is_rate_limited(_Resp(500), body) is True


def test_429_is_caught_regardless_of_body():
    assert C._is_rate_limited(_Resp(429), {}) is True


@pytest.mark.parametrize("body", [
    {"rt_cd": "1", "msg_cd": "40310000", "msg1": "모의투자 주문가능금액이 부족합니다"},
    {"rt_cd": "1", "msg_cd": "APBK0013", "msg1": "장운영시간이 아닙니다"},
    {"msg1": ""},
    {},
])
def test_unrelated_errors_are_not_rate_limits(body):
    """아무거나 속도제한으로 보면 **진짜 실패를 재시도로 덮는다** — 그게 더 나쁘다."""
    assert C._is_rate_limited(_Resp(500), body) is False


# ---------------------------------------------------- 인식만으로는 부족하다 — **재시도가 걸려야** 한다
def _client(session, retries=4):
    import threading
    import types
    c = C.__new__(C)
    c._session = session
    c._rate_lock = threading.Lock()
    c._last_request_monotonic = 0.0
    c.cfg = types.SimpleNamespace(base_url="https://x", timeout_sec=5,
                                  rate_limit_retries=retries, rate_limit_backoff_sec=0.0,
                                  min_request_interval_sec=0.0)
    c._wait_rate_limit = lambda: None
    return c


class _Fake:
    def __init__(self, code, body):
        self.status_code, self._b, self.headers = code, body, {}

    def json(self):
        return self._b

    @property
    def text(self):
        import json
        return json.dumps(self._b)


class _Session:
    def __init__(self, fail_n):
        self.n, self.fail_n = 0, fail_n

    def request(self, **kw):
        self.n += 1
        if self.n <= self.fail_n:
            return _Fake(500, REAL)
        return _Fake(200, {"rt_cd": "0", "msg1": "정상처리"})


def test_request_retries_on_the_real_rate_limit():
    s = _Session(fail_n=2)
    body, _ = _client(s)._request_json("GET", "/t", headers={})
    assert s.n == 3 and body["rt_cd"] == "0"


def test_retries_are_bounded():
    """끝없이 재시도하면 장 시간을 다 쓴다 — 한도 뒤에는 실패로 낸다."""
    from kis_order_client import KISApiError
    s = _Session(fail_n=99)
    with pytest.raises(KISApiError):
        _client(s, retries=2)._request_json("GET", "/t", headers={})
    assert s.n == 3          # 최초 1 + 재시도 2
