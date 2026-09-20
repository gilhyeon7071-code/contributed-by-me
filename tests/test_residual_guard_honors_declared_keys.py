# -*- coding: utf-8 -*-
"""`intraday_residual_overnight_guard` 의 선언 키를 엔진이 **실제로 읽는지** 고정한다.

[2026-09-12] 미결 대장 C14. 세 키는 현재 동작을 정확히 기술했지만 제어하지 않았다.
검사기는 셋이 True 인지 검사했고 셋 다 True 라 아무도 눈치채지 못했다.
설정이 갖지 않은 제어권을 선언하면, 누가 False 로 바꿔도 동작은 그대로다 -
그 침묵이 이 시스템의 반복된 실패 형태다.
"""
from __future__ import annotations

import inspect
import pytest

ex = pytest.importorskip("paper_engine.exit")

_SRC = inspect.getsource(ex)


def test_apply_to_surge_is_read_not_just_declared():
    assert 'guard_cfg.get("apply_to_surge"' in _SRC
    assert "if _row_is_surge and not apply_to_surge:" in _SRC


def test_apply_to_non_surge_is_read_not_just_declared():
    assert 'guard_cfg.get("apply_to_non_surge"' in _SRC
    assert "if (not _row_is_surge) and not apply_to_non_surge:" in _SRC


def test_trigger_on_same_day_loss_fails_closed_rather_than_silently_ignored():
    """유일한 트리거라 의미를 임의로 만들지 않는다 - 알리고 가드는 계속 돈다."""
    assert 'guard_cfg.get("trigger_on_same_day_loss"' in _SRC
    assert "INTRADAY_RESIDUAL_GUARD_CONTRACT" in _SRC


def test_keys_are_surfaced_in_the_artifact():
    """산출물에 실효값이 실려야 사후에 대조할 수 있다."""
    for k in ("apply_to_surge", "apply_to_non_surge", "trigger_on_same_day_loss"):
        assert '"%s": %s,' % (k, k) in _SRC, "%s 가 산출물에 없다" % k


def test_validator_and_engine_agree_on_the_key_set():
    """검사기가 요구하는 키를 엔진이 전부 읽는다 - 한쪽만 바뀌면 FAIL."""
    import io
    from pathlib import Path
    v = io.open(Path(ex.__file__).resolve().parents[1]
                / "tools" / "validate_intraday_residual_overnight_guard_activation.py",
                encoding="utf-8").read()
    for k in ("enabled", "shadow_only", "exit_before_overnight",
              "trigger_on_same_day_loss", "apply_to_surge", "apply_to_non_surge", "scope"):
        assert '"%s"' % k in v, "검사기가 %s 를 안 본다" % k
        assert 'guard_cfg.get("%s"' % k in _SRC, "엔진이 %s 를 안 읽는다" % k


# ---------------------------------------------------------------------------
# 행동 시험. 소스 문자열 확인만으로는 "제어한다" 를 증명하지 못한다.
# **산출물 경로를 tmp 로 갈아끼운다** - 그러지 않으면 시험이 진짜 latest.json 을 덮는다
# (2026-09-11 에 실제로 그 사고를 냈다). feedback_check_where_the_tool_writes
# ---------------------------------------------------------------------------

def _shadow(tmp_path, monkeypatch, *, is_surge: bool, **guard):
    pd = pytest.importorskip("pandas")
    monkeypatch.setattr(ex, "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH",
                        tmp_path / "shadow.csv", raising=False)
    monkeypatch.setattr(ex, "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH",
                        tmp_path / "shadow.json", raising=False)
    cfg = {"intraday_residual_overnight_guard": dict(
        {"enabled": True, "shadow_only": True, "scope": "intraday_realtime"}, **guard)}
    pos = {"code": "123456", "entry_date": "20260601", "entry_price": 1000.0, "qty": 3,
           "entry_timing": "intraday_realtime", "entry_order_id": "BUY1",
           "source_order_id": "BUY1", "signal_date": "20260601"}
    if is_surge:
        pos["surge_type"] = "GAP_UP"
    # legacy 스키마는 **리스트**를 받는다 (common.py:1088 - 0=trade_id 1=code
    # 2=entry_date 4=exit_date 6=net_ret 8=exit_reason 9=note). dict 를 주면
    # 조용히 빈 행이 되어 후보 0건이 나오고, 그걸 "걸러졌다" 로 오독하게 된다
    trade = ["T1", "123456", "20260601", "", "20260601", "", -0.03, "", "TEST",
             "sell_qty=2;partial_exit=1"]
    out = ex._write_intraday_residual_overnight_guard_shadow(
        config=cfg, schema="legacy", trades_new=[trade],
        still_open=[pos], runtime_ymd="20260601")
    return out


def test_apply_to_non_surge_false_actually_suppresses(tmp_path, monkeypatch):
    on = _shadow(tmp_path, monkeypatch, is_surge=False, apply_to_non_surge=True)
    off = _shadow(tmp_path, monkeypatch, is_surge=False, apply_to_non_surge=False)
    assert on["candidates"] == 1, "기본(True)에서 후보가 잡혀야 한다"
    assert off["candidates"] == 0, "False 인데 후보가 그대로다 - 키가 제어하지 않는다"


def test_apply_to_surge_false_actually_suppresses(tmp_path, monkeypatch):
    on = _shadow(tmp_path, monkeypatch, is_surge=True, apply_to_surge=True)
    off = _shadow(tmp_path, monkeypatch, is_surge=True, apply_to_surge=False)
    assert on["candidates"] == 1
    assert off["candidates"] == 0


def test_default_behaviour_is_unchanged(tmp_path, monkeypatch):
    """키를 안 주면 예전과 같다 - 이번 변경으로 동작이 바뀌면 안 된다."""
    for surge in (True, False):
        out = _shadow(tmp_path, monkeypatch, is_surge=surge)
        assert out["candidates"] == 1
        assert out["apply_to_surge"] is True and out["apply_to_non_surge"] is True
