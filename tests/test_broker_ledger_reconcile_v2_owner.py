"""broker_ledger_reconcile — V2(분기 시총) 원장을 소유자로 인식하는가. 브로커·원장은 전부 가짜, 쓰기는 tmp 로 격리."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import broker_ledger_reconcile as B  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(B, "EQUITY_LEDGER", tmp_path / "account_equity_ledger.csv")
    monkeypatch.setattr(B, "V2_FILLS", tmp_path / "fills.jsonl")
    monkeypatch.setattr(B, "read_fills_net", lambda: {"ok": True, "net": {}})
    monkeypatch.setattr(B, "read_topn_positions", lambda: {"ok": True, "positions": {}, "closed": True})
    state = {"positions": {}}
    broker = {"holdings": {}}
    monkeypatch.setattr(B, "read_paper_state", lambda: {"ok": True, "positions": state["positions"]})
    monkeypatch.setattr(B, "read_broker", lambda mock: {"ok": True, "mode": "mock", "holdings": broker["holdings"],
                                                        "cash": 1_000_000.0, "values": {}})

    def v2_fills(rows):
        (tmp_path / "fills.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")

    return {"state": state, "broker": broker, "v2_fills": v2_fills, "tmp": tmp_path}


def _run():
    out = B.reconcile(mock=True)
    return out, B.build_alert_text(out)


def test_before_first_rebalance_is_quiet(world):
    out, alert = _run()
    assert out["status"] == "MATCH" and alert is None
    assert out["sources"]["v2_positions"]["reason"].startswith("V2 체결 없음")


def test_v2_holdings_are_owned_and_quiet(world):
    world["v2_fills"]([{"code": "005930", "side": "BUY", "qty": 10}, {"code": "000660", "side": "BUY", "qty": 3},
                       {"code": "000660", "side": "SELL", "qty": 1}])
    world["broker"]["holdings"] = {"005930": 10, "000660": 2}
    out, alert = _run()
    assert out["status"] == "MATCH" and alert is None
    assert {o["code"]: o["owner"] for o in out["ownership"]} == {"005930": "kospi_mcap_quarterly_v2",
                                                                  "000660": "kospi_mcap_quarterly_v2"}


def test_someone_sold_v2_shares_alarms(world):
    """v41.1 청산·카나리아 체결 등으로 계좌가 V2 원장보다 적어지면 운다."""
    world["v2_fills"]([{"code": "005930", "side": "BUY", "qty": 10}])
    world["broker"]["holdings"] = {"005930": 9}
    out, alert = _run()
    assert out["status"] == "GAP" and out["summary"]["qty_mismatch"] == 1 and alert is not None


def test_unknown_holding_still_alarms(world):
    world["v2_fills"]([{"code": "005930", "side": "BUY", "qty": 10}])
    world["broker"]["holdings"] = {"005930": 10, "123456": 5}
    out, alert = _run()
    assert out["ownership_summary"]["unattributed_codes"] == 1 and alert is not None


def test_v2_and_v41_claim_same_code_is_ambiguous(world):
    world["v2_fills"]([{"code": "005930", "side": "BUY", "qty": 10}])
    world["state"]["positions"] = {"005930": 4}
    world["broker"]["holdings"] = {"005930": 14}
    out, alert = _run()
    assert out["ownership"][0]["owner"] == "AMBIGUOUS" and alert is not None
    assert out["summary"]["gap_codes"] == 0  # 수량은 v41.1(4) + V2(10) = 14 로 맞다


def test_corrupt_v2_ledger_is_reported_not_silent(world):
    (world["tmp"] / "fills.jsonl").write_text("{not json\n", encoding="utf-8")
    assert B.read_v2_positions()["ok"] is False


def test_topn_owner_is_marked_closed():
    """[2026-09-21] 종결된 라운드가 소유자 표에서 살아 있는 것처럼 읽히면 안 된다.
    표는 지우지 않는다 — 과거 귀속과 '포지션이 다시 나타나면' 을 위해 남긴다."""
    e = B.EXIT_OWNERS["topn_stage1"]
    assert e.get("closed") is True and "ROUND_CLOSED" in e.get("closed_note", "")
    # 살아 있는 소유자는 표시가 없어야 한다
    assert not B.EXIT_OWNERS["kospi_mcap_quarterly_v2"].get("closed")
    assert not B.EXIT_OWNERS["paper_engine_v41_1"].get("closed")


def test_closed_round_is_excluded_from_reconcile(tmp_path, monkeypatch):
    """마커가 있으면 멈춘 positions.csv 를 대조하지 않는다 — '원장에만 6종목' 이 매일 뜨던 자리."""
    d = tmp_path / "topn"
    d.mkdir()
    (d / "positions.csv").write_text("code,qty\n005930,5\n", encoding="utf-8")
    monkeypatch.setattr(B, "TOPN_POSITIONS", d / "positions.csv")
    assert B.read_topn_positions()["positions"] == {"005930": 5}      # 마커 없으면 대조한다
    (d / "ROUND_CLOSED.json").write_text("{}", encoding="utf-8")
    out = B.read_topn_positions()
    assert out["positions"] == {} and out.get("closed") is True
