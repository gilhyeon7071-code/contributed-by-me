# -*- coding: utf-8 -*-
"""시장 이벤트 게이트가 **관측 불가일 때 닫히는지** 고정한다.

[2026-09-12] 사이드카·서킷브레이커 점검에서 나온 것.

탐지기(`market_anomaly_detector.py`)는 정직하다 - 볼 수 있는 지수가 하나도 없으면
게이트 파일을 건드리지 않고 `valid=false / action=no_observation` 만 남긴다.
그런데 **소비자가 정직하지 않았다.** 진입 게이트가 파일 나이를 안 봐서
어제 이벤트는 as_of_ymd 불일치로 전부 걸러지고 -> 이벤트 0건 -> 통과가 됐다.

**폭락장에 WS 가 끊기는 것이 가장 그럴듯한 시나리오인데 그때 게이트가 열렸다.**
차단 비용은 없다 - 사이드카·CB 발동 구간에는 어차피 매매가 이루어지지 않는다.

그리고 게이트 파일이 없을 때 `market_event_level: "NORMAL"` 스텁을 만들던 자리도 같이 본다.
파일이 없다는 것은 "시장이 정상" 이 아니라 "모른다" 다.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

entry = pytest.importorskip("paper_engine.entry")


def _today() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def _status(tmp_path, monkeypatch, payload):
    """탐지기 상태 파일을 격리 파일로 갈아끼운다. 진짜 산출물을 건드리지 않는다."""
    p = tmp_path / "market_anomaly_detector_status_latest.json"
    if payload is not None:
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(entry, "MARKET_ANOMALY_STATUS_PATH", p, raising=False)
    return p


def _ok_status():
    return {"as_of_ymd": _today(), "valid": True,
            "observed": {"0001": {"change_pct": -1.75, "level": "NORMAL"}}}


def test_observed_normal_day_passes(tmp_path, monkeypatch):
    """정상 관측이면 막지 않는다. 매일 막히면 그 가드는 꺼진다."""
    _status(tmp_path, monkeypatch, _ok_status())
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": _today(), "market_event_level": "NORMAL"}, {})
    assert blocked is False, reason


def test_gate_file_from_another_day_blocks(tmp_path, monkeypatch):
    """어제 게이트는 오늘을 말해주지 않는다."""
    _status(tmp_path, monkeypatch, _ok_status())
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": "20260911", "market_event_level": "NORMAL"}, {})
    assert blocked is True and "gate_not_today" in reason, reason


def test_unknown_level_blocks(tmp_path, monkeypatch):
    """자리표시자(UNKNOWN)를 '정상' 으로 읽지 않는다."""
    _status(tmp_path, monkeypatch, _ok_status())
    for level in ("UNKNOWN", ""):
        blocked, reason = entry._market_event_observation_block(
            {"as_of_ymd": _today(), "market_event_level": level}, {})
        assert blocked is True and reason == "level_unknown", (level, reason)


def test_no_observation_blocks(tmp_path, monkeypatch):
    """**핵심.** 탐지기가 아무 지수도 못 봤으면 '이벤트 없음' 이 아니다."""
    _status(tmp_path, monkeypatch,
            {"as_of_ymd": _today(), "valid": False, "action": "no_observation",
             "observed": {}, "rejected": {"0001": "stale_source:age=900s>max=300s"}})
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": _today(), "market_event_level": "NORMAL"}, {})
    assert blocked is True and "no_observation" in reason, reason


def test_empty_observed_blocks(tmp_path, monkeypatch):
    """valid 깃발이 없어도 관측 목록이 비면 모르는 것이다."""
    _status(tmp_path, monkeypatch, {"as_of_ymd": _today(), "observed": {}})
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": _today(), "market_event_level": "NORMAL"}, {})
    assert blocked is True and reason == "no_observed_index", reason


def test_detector_status_missing_blocks(tmp_path, monkeypatch):
    """상태 파일을 못 읽으면 판정 불가다. 통과로 흡수하지 않는다."""
    _status(tmp_path, monkeypatch, None)
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": _today(), "market_event_level": "NORMAL"}, {})
    assert blocked is True and "unreadable" in reason, reason


def test_detector_status_from_another_day_blocks(tmp_path, monkeypatch):
    _status(tmp_path, monkeypatch, {"as_of_ymd": "20260911", "valid": True,
                                    "observed": {"0001": {}}})
    blocked, reason = entry._market_event_observation_block(
        {"as_of_ymd": _today(), "market_event_level": "NORMAL"}, {})
    assert blocked is True and "detector_status_not_today" in reason, reason


def test_switch_can_be_turned_off_and_is_declared(tmp_path, monkeypatch):
    """끌 수 있어야 하고, **설정에 선언돼 있어야 한다.** 선언 없는 값은 조용히 바뀐다."""
    _status(tmp_path, monkeypatch, None)
    blocked, _ = entry._market_event_observation_block(
        {"as_of_ymd": "20260101"}, {"require_market_observation": False})
    assert blocked is False

    from paper_engine.config import load_config
    eg = (load_config().get("p1_entry_policy") or {}).get("event_gate") or {}
    assert eg.get("require_market_observation") is True, \
        "설정에 선언돼 있지 않다 - 코드 기본값에만 있으면 조용히 바뀐다"


def test_autostub_no_longer_claims_normal():
    """게이트 파일이 없을 때 NORMAL 을 지어내던 자리. 소스로 고정한다."""
    import inspect
    src = inspect.getsource(entry._apply_p1_event_gate)
    assert '"market_event_level": "NORMAL"' not in src, \
        "자리표시자가 다시 NORMAL 을 주장한다"
    assert '"market_event_level": "UNKNOWN"' in src


def test_sidecar_is_not_a_block_reason():
    """**사이드카로 우리가 멈추면 안 된다.**

    제도(2026-09-12 사용자 제공 조문): 사이드카는 **프로그램매매 호가**의 효력만 5분 정지하고
    일반 투자자의 매수·매도 주문은 정상적으로 진행된다. 시장 전체가 서는 서킷브레이커와 다르다.

    우리 발주가 프로그램매매인가 - 실측으로 아니다.
      비차익 프로그램매매 기준은 15종목 이상 일괄인데,
      2026-09-11 제출원장 143행의 **분당 최대 4종목**, 09-07 은 6종목이다.
      max_new 는 4(19회)/15(6회)로 15는 상한이고 실제 발주는 그 아래다.

    따라서 SIDECAR 를 차단 사유로 두면 **시장은 도는데 우리만 서는** 틀린 차단이 된다.
    지금은 탐지기가 그 타입을 생산하지 않아 동작 변화가 0이지만,
    나중에 누가 사이드카를 구현할 때 전면 차단으로 잘못 배선되는 것을 막는다.
    """
    from paper_engine.config import load_config
    g = ((load_config().get("p1_entry_policy") or {}).get("event_gate") or {})         .get("explicit_market_event_guard") or {}
    block = [str(x).upper() for x in (g.get("block_event_types") or [])]
    wide = [str(x).upper() for x in (g.get("market_wide_event_types") or [])]
    assert "SIDECAR" not in block, block
    assert "SIDECAR" not in wide, wide
    # 서킷브레이커는 전체 매매 중단이므로 남아 있어야 한다
    assert "CIRCUIT_BREAKER" in block and "CIRCUIT_BREAKER" in wide
