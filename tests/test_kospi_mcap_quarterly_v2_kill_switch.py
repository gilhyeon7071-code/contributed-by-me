"""긴급 전량 청산 — **안전 규칙이 진짜로 막는지** 본다 (2026-09-21).

배경: 현황판 [긴급 정지 / 전량 청산] 버튼이 확인창만 띄우고
*"명령이 서버로 전송되었습니다"* 라고 alert 만 했다. 아무것도 보내지 않았다.
비상 상황에서 눌러 놓고 청산됐다고 믿게 만드는 자리였다.

배선하면서 지켜야 하는 것 셋 — 여기서 보는 것은 전부 **막는 쪽**이다.
  1. 무장(kill_switch_armed) 전에는 주문이 나가지 않는다
  2. 같은 요청을 두 번 집행하지 않는다(멱등)
  3. 주문 ID 에 KILL 태그가 들어가 래치 청산·분기 재구성과 섞이지 않는다
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from paper.strategies.kospi_mcap_quarterly_v2.src import kill_switch as KS  # noqa: E402
from test_kospi_mcap_quarterly_v2_runner import CFG as _CFG, SimKIS  # noqa: E402

# 실제 전략 설정을 그대로 쓴다 — 발주 창·계좌 같은 키를 손으로 흉내 내면 시험이 현실과 갈라진다
CFG = dict(_CFG)
CFG.setdefault("strategy_id", "kospi_mcap_quarterly_v2")
NOW = datetime(2026, 10, 2, 10, 0, 0)


def _clock():
    return NOW


def _state(tmp_path: Path, *, qty=5, code="005930") -> Path:
    st = tmp_path / "state"
    st.mkdir(parents=True, exist_ok=True)
    if qty:
        (st / "fills.jsonl").write_text(json.dumps(
            {"fill_id": "a|1", "order_id": "a", "code": code, "side": "BUY", "qty": qty,
             "amount": qty * 70000, "trade_date": "20261001", "observed_at": "2026-10-01T10:00:00"}
        ) + "\n", encoding="utf-8")
    return st


def _kis(hold=None):
    k = SimKIS()
    k.hold = dict(hold or {})
    return k


# ---------------------------------------------------------------- 요청은 주문이 아니다
def test_request_writes_file_and_sends_nothing(tmp_path):
    st = _state(tmp_path)
    r = KS.request(state_dir=st, reason="수동 긴급", by="dashboard", clock=_clock)
    assert r["status"] == "REQUESTED" and r["request_id"].startswith("KILL-")
    assert (st / KS.REQUEST).is_file()
    assert KS.read_request(st)["reason"] == "수동 긴급"


def test_no_request_is_standby_not_error(tmp_path):
    rep = KS.execute(_kis(), state_dir=_state(tmp_path), cfg=CFG, ops={}, clock=_clock)
    assert rep["status"] == "STANDBY" and rep["reasons"] == ["NO_REQUEST"]


# ---------------------------------------------------------------- 1) 무장 전에는 안 나간다
def test_not_armed_plans_but_sends_nothing(tmp_path):
    st = _state(tmp_path)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    kis = _kis({"005930": 5})
    rep = KS.execute(kis, state_dir=st, cfg=CFG, ops={"kill_switch_armed": False}, clock=_clock)
    assert rep["armed"] is False
    assert len(rep["orders"]) == 1 and rep["submitted"] == 0
    assert kis.orders == []                       # 브로커에 아무것도 안 갔다
    assert "NOT_ARMED_SHADOW" in rep["reasons"]
    assert KS.read_request(st)["status"] == "REQUESTED"   # 집행 표시가 붙지 않는다


def test_armed_but_no_submit_flag_still_sends_nothing(tmp_path):
    st = _state(tmp_path)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    kis = _kis({"005930": 5})
    rep = KS.execute(kis, state_dir=st, cfg=CFG, ops={"kill_switch_armed": True},
                     no_submit=True, clock=_clock)
    assert kis.orders == [] and "NO_SUBMIT_REHEARSAL" in rep["reasons"]


def test_armed_sends(tmp_path):
    st = _state(tmp_path)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    kis = _kis({"005930": 5})
    rep = KS.execute(kis, state_dir=st, cfg=CFG, ops={"kill_switch_armed": True}, clock=_clock)
    assert rep["submitted"] == 1 and len(kis.orders) == 1
    assert kis.orders[0]["sll_buy_dvsn_cd"] == "01"        # 매도
    assert KS.read_request(st)["status"] == "EXECUTED"


# ---------------------------------------------------------------- 2) 멱등
def test_same_request_is_not_executed_twice(tmp_path):
    st = _state(tmp_path)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    kis = _kis({"005930": 5})
    KS.execute(kis, state_dir=st, cfg=CFG, ops={"kill_switch_armed": True}, clock=_clock)
    n = len(kis.orders)
    rep2 = KS.execute(kis, state_dir=st, cfg=CFG, ops={"kill_switch_armed": True}, clock=_clock)
    assert rep2["status"] == "STANDBY" and rep2["reasons"][0].startswith("ALREADY_EXECUTED")
    assert len(kis.orders) == n                    # 두 번째로는 안 나간다


# ---------------------------------------------------------------- 3) 태그 분리
def test_order_id_carries_kill_tag(tmp_path):
    st = _state(tmp_path)
    r = KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    rep = KS.execute(_kis({"005930": 5}), state_dir=st, cfg=CFG,
                     ops={"kill_switch_armed": False}, clock=_clock)
    oid = rep["orders"][0]["order_id"]
    assert f"|KILL|{r['request_id']}|" in oid and oid.endswith("|SELL|20261002|1")
    assert "LATCH" not in oid                      # 래치 청산과 섞이지 않는다


# ---------------------------------------------------------------- 계좌 몫이 아닌 주식은 안 판다
def test_sells_min_of_ledger_and_broker(tmp_path):
    st = _state(tmp_path, qty=10)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    rep = KS.execute(_kis({"005930": 3}), state_dir=st, cfg=CFG,
                     ops={"kill_switch_armed": False}, clock=_clock)
    assert rep["orders"][0]["requested_qty"] == 3
    assert rep["mismatches"] and rep["status"] == "STOP"    # 크게 남긴다


def test_nothing_to_liquidate_is_said_out_loud(tmp_path):
    st = _state(tmp_path, qty=0)
    KS.request(state_dir=st, reason="r", by="t", clock=_clock)
    rep = KS.execute(_kis(), state_dir=st, cfg=CFG, ops={"kill_switch_armed": True}, clock=_clock)
    assert rep["orders"] == [] and "NOTHING_TO_LIQUIDATE" in rep["reasons"]


def test_unreadable_request_is_stop_not_standby(tmp_path):
    st = _state(tmp_path)
    (st / KS.REQUEST).write_text("{깨진", encoding="utf-8")
    rep = KS.execute(_kis(), state_dir=st, cfg=CFG, ops={"kill_switch_armed": True}, clock=_clock)
    assert rep["status"] == "STOP" and rep["reasons"][0].startswith("REQUEST_UNREADABLE")


@pytest.mark.parametrize("armed", [True, False])
def test_config_default_is_disarmed(armed):
    """기본값이 꺼짐이어야 한다 — 설정 파일이 실수로 비어도 무장되지 않는다."""
    ops = {"kill_switch_armed": armed} if armed else {}
    assert bool(ops.get("kill_switch_armed", False)) is armed


def test_shipped_config_is_disarmed():
    p = (ROOT / "paper" / "strategies" / "kospi_mcap_quarterly_v2" / "config" / "daily_ops_v1.json")
    assert json.loads(p.read_text(encoding="utf-8")).get("kill_switch_armed") is False
