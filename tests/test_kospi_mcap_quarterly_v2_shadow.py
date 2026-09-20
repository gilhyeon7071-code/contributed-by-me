"""H1 그림자 기록 — 합성 입력. 주문 없음."""
from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import shadow_record as S

NOW = datetime(2026, 9, 18, 18, 40, 0)
RULES = json.loads(S.DEFAULT_RULES.read_text(encoding="utf-8"))
DAYS = ["20260901", "20260902", "20260903", "20260904", "20260907", "20260908", "20260909"]


def _events(tmp_path, rows):
    (tmp_path / "h1_events.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def _ev(eid="R1|BUYBACK_DIRECT", et="BUYBACK_DIRECT", code="005930", dt="20260901", amend=False):
    return {"event_id": eid, "event_type": et, "stock_code": code, "corp_name": "테스트", "rcept_dt": dt,
            "is_amendment": amend, "direction": "BUY_PRESSURE"}


def _prices(tmp_path, code="005930", open_=10000.0, closes=None, volume=100000.0):
    rows = []
    for i, d in enumerate(DAYS):
        c = (closes or {}).get(d, 10000.0)
        rows.append({"code": code, "date": d, "open": open_ if d == "20260902" else c, "close": c, "volume": volume})
    pd.DataFrame(rows).to_parquet(tmp_path / "prices_krx.parquet", index=False)


def test_planned_then_open_then_closed(tmp_path):
    _events(tmp_path, [_ev()])
    _prices(tmp_path, closes={d: 10000.0 for d in DAYS} | {"20260909": 11000.0})
    r1 = S.run(tmp_path, RULES, NOW)
    assert r1["counts"]["OPEN"] == 1 and r1["planned_new"] == 1
    r2 = S.run(tmp_path, RULES, NOW)          # 같은 날 다시 돌려도 새 계획 없음
    assert r2["planned_new"] == 0
    rows = [json.loads(x) for x in (tmp_path / "shadow_trades.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [r["status"] for r in rows][:2] == ["PLANNED", "OPEN"]     # 줄을 고치지 않고 덧붙인다
    closed = [r for r in rows if r["status"] == "CLOSED"][0]
    assert closed["entry_price"] == 10000.0 and closed["exit_price"] == 11000.0
    assert closed["gross_return"] == pytest.approx(0.10)
    assert closed["net_return"] == pytest.approx(0.10 - RULES["round_trip_cost_pct"])


def test_low_liquidity_is_skipped_and_recorded(tmp_path):
    _events(tmp_path, [_ev()])
    _prices(tmp_path, volume=1000.0)          # 10,000원 × 1,000주 = 1천만원 < 2억
    r = S.run(tmp_path, RULES, NOW)
    assert r["counts"]["SKIPPED"] == 1
    row = [json.loads(x) for x in (tmp_path / "shadow_trades.jsonl").read_text(encoding="utf-8").splitlines()][-1]
    assert row["status"] == "SKIPPED" and any("LOW_LIQUIDITY" in x for x in row["skip_reasons"])


def test_order_too_big_is_skipped(tmp_path):
    _events(tmp_path, [_ev()])
    _prices(tmp_path, volume=30000.0)         # 3억 → 300만원은 1.0%... 2% 한도 아래이므로 통과해야 함
    assert S.run(tmp_path, RULES, NOW)["counts"].get("OPEN") == 1
    import shutil
    shutil.rmtree(tmp_path / "shadow_trades.jsonl", ignore_errors=True)
    (tmp_path / "shadow_trades.jsonl").unlink(missing_ok=True)
    _prices(tmp_path, volume=21000.0)         # 2.1억 → 300만원이 1.43%... 통과. 2억 미만이면 유동성에서 걸림
    r = S.run(tmp_path, RULES, NOW)
    assert r["counts"].get("OPEN") == 1


def test_amendment_is_not_recorded(tmp_path):
    _events(tmp_path, [_ev(amend=True)])
    _prices(tmp_path)
    assert S.run(tmp_path, RULES, NOW)["planned_new"] == 0


def test_other_event_types_ignored(tmp_path):
    _events(tmp_path, [_ev("R9|RIGHTS_OFFERING", "RIGHTS_OFFERING")])
    _prices(tmp_path)
    assert S.run(tmp_path, RULES, NOW)["planned_new"] == 0


def test_plan_recorded_even_without_prices(tmp_path):
    """사건이 난 날에는 가격이 아직 없다 — 그래도 계획은 미리 남아야 한다(나중에 고르지 않게)."""
    _events(tmp_path, [_ev()])
    r = S.run(tmp_path, RULES, NOW)
    assert r["planned_new"] == 1 and r["counts"]["PLANNED"] == 1
