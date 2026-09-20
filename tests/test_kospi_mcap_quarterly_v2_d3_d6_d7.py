"""분기 시총(V2) D3 국면 · D6 선정 · D7 목표 포트폴리오 — 정상 통과, 이상마다 멈춤."""
from __future__ import annotations

import json
import math

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import targets as T

CFG = {
    "n_names": 5, "strategy_capital_krw": 10_000_000, "weight_cap": 0.20,
    "exposure_above": 1.0, "exposure_below": 0.5, "ma_window": 3, "min_names_for_cap": 5,
    "cap_sum_tolerance": 1e-10, "cost_reserve_pct": 0.0, "integerization_fail_pct": 0.10,
}


def _index(closes, last="20260930"):
    dates = [f"202609{d:02d}" for d in range(30 - len(closes) + 1, 31)]
    assert dates[-1] == last
    return pd.DataFrame({"date": dates, "close": closes})


# ---------- D3 ----------

def test_regime_above_below_and_equal_is_above():
    assert T.compute_regime(_index([100, 100, 130]), "20260930", CFG)["regime"] == "above"
    r = T.compute_regime(_index([130, 130, 100]), "20260930", CFG)
    assert r["regime"] == "below" and r["exposure"] == 0.5 and r["effective_from"] == "next_trading_day"
    eq = T.compute_regime(_index([100, 100, 100]), "20260930", CFG)
    assert eq["regime"] == "above" and eq["exposure"] == 1.0


def test_regime_stops_on_short_window_or_wrong_date():
    assert T.compute_regime(_index([100, 100]), "20260930", CFG)["status"] == "STOP"
    assert any(r.startswith("REGIME_INDEX_DATE") for r in T.compute_regime(_index([1, 2, 3]), "20261001", CFG)["reasons"])


# ---------- D6 ----------

def _universe(rows):
    return pd.DataFrame(rows, columns=["code", "name", "market_cap", "close"])


def test_select_top_order_and_ties():
    u = _universe([("000003", "c", 100, 10), ("000001", "a", 100, 10), ("000002", "b", 300, 10),
                   ("000004", "d", 50, 10), ("000005", "e", 40, 10), ("000006", "f", 30, 10), ("000007", "g", 0, 10)])
    sel, s = T.select_top(u, CFG)
    assert s["status"] == "OK" and list(sel["code"]) == ["000002", "000001", "000003", "000004", "000005"]
    assert list(sel["rank"]) == [1, 2, 3, 4, 5] and s["invalid_mcap_excluded"] == 1


def test_select_top_stops_when_short():
    sel, s = T.select_top(_universe([("000001", "a", 10, 1), ("000002", "b", 9, 1)]), CFG)
    assert s["status"] == "STOP" and s["reasons"][0].startswith("SELECTION_SHORT")


# ---------- D7 상한 ----------

def test_cap_weights_multi_round():
    w, rounds = T.cap_weights([50, 30, 10, 5, 5], 0.20)
    assert math.isclose(sum(w), 1.0, abs_tol=1e-12) and max(w) <= 0.2 + 1e-12
    assert rounds >= 2  # 첫 재배분 뒤 새로 넘는 종목이 생긴다


def test_cap_weights_no_cap_needed_keeps_raw():
    w, rounds = T.cap_weights([20, 20, 20, 20, 20], 0.20)
    assert rounds == 0 and all(math.isclose(x, 0.2) for x in w)


def test_cap_keeps_market_cap_proportion_among_uncapped():
    w, _ = T.cap_weights([90, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1], 0.20)
    assert math.isclose(w[1] / w[2], 4 / 3, rel_tol=1e-12)


def test_target_stops_with_fewer_than_five_names():
    sel = _universe([("00000%d" % i, "x", 10, 100) for i in range(4)])
    _, s = T.build_target_portfolio(sel, 1.0, CFG)
    assert s["status"] == "STOP" and s["reasons"][0].startswith("CAP_TOO_FEW_NAMES")


# ---------- D7 정수화 ----------

def _sel(prices, mcaps=None):
    mcaps = mcaps or [50, 30, 10, 5, 5]
    return _universe([(f"00000{i}", f"n{i}", m, p) for i, (m, p) in enumerate(zip(mcaps, prices))])


def test_integerization_respects_budget_cap_and_improves_error():
    tp, s = T.build_target_portfolio(_sel([1000, 1000, 1000, 1000, 1000]), 1.0, CFG)
    assert s["status"] == "OK", s["reasons"]
    assert s["invested"] <= s["target_budget"]
    assert (tp["actual_weight"] <= 0.20 + 1e-12).all()
    assert (tp["target_qty"] >= tp["floor_qty"]).all()


def test_integerization_adds_share_when_one_share_exceeds_target_but_reduces_error():
    # 시총 [30,30,15,15,5,5] -> 상한 뒤 비중 [.2,.2,.2,.2,.1,.1]. 마지막 종목 목표 10만원, 1주 15만원(상한 20만원 이내)
    cfg = dict(CFG, strategy_capital_krw=1_000_000)
    tp, s = T.build_target_portfolio(_sel([70_000] * 5 + [150_000], [30, 30, 15, 15, 5, 5]), 1.0, cfg)
    last = tp[tp["code"] == "000005"].iloc[0]
    assert last["basket_weight"] == pytest.approx(0.1)
    assert last["floor_qty"] == 0 and last["target_qty"] == 1  # 오차 10만 -> 5만, 예산·상한 이내


def test_floor_is_not_broken_by_float_error():
    cfg = dict(CFG, strategy_capital_krw=1_000_000)
    tp, _ = T.build_target_portfolio(_sel([10] * 6, [30, 30, 15, 15, 5, 5]), 1.0, cfg)
    assert tp[tp["code"] == "000004"].iloc[0]["floor_qty"] == 10_000  # 100000/10


def test_integerization_does_not_add_share_beyond_account_cap():
    # 같은 구조에서 1주 25만원이면 오차는 줄어도(10만 -> 15만, 안 줄어듦) 추가 안 함; 상한 20만원도 넘음
    cfg = dict(CFG, strategy_capital_krw=1_000_000)
    tp, _ = T.build_target_portfolio(_sel([10, 10, 10, 10, 10, 250_000], [30, 30, 15, 15, 5, 5]), 1.0, cfg)
    assert tp[tp["code"] == "000005"].iloc[0]["target_qty"] == 0


def test_integerization_fails_when_zero_qty_weight_too_large():
    cfg = dict(CFG, strategy_capital_krw=1_000_000)
    tp, s = T.build_target_portfolio(_sel([10, 10, 10, 10_000_000, 10_000_000]), 1.0, cfg)
    assert s["status"] == "STOP" and any("zero_qty_target_weight" in r for r in s["reasons"])


def test_integerization_fails_on_cash_drift():
    cfg = dict(CFG, strategy_capital_krw=1_000_000)
    _, s = T.build_target_portfolio(_sel([150_000, 150_000, 150_000, 150_000, 150_000]), 1.0, cfg)
    assert any("cash_drift" in r for r in s["reasons"])


def test_exposure_half_halves_budget_and_account_cap():
    tp, s = T.build_target_portfolio(_sel([100, 100, 100, 100, 100]), 0.5, CFG)
    assert s["target_budget"] == pytest.approx(5_000_000) and s["intended_cash_by_exposure"] == pytest.approx(5_000_000)
    assert (tp["actual_weight"] <= 0.10 + 1e-12).all()


def test_cost_reserve_reduces_allocable():
    _, s = T.build_target_portfolio(_sel([100] * 5), 1.0, dict(CFG, cost_reserve_pct=0.005))
    assert s["allocable"] == pytest.approx(9_950_000)


def test_invalid_price_stops():
    _, s = T.build_target_portfolio(_sel([100, 100, 0, 100, 100]), 1.0, CFG)
    assert s["status"] == "STOP" and "PRICE_INVALID" in s["reasons"]


# ---------- 실행기: D1~D7 연결 ----------

def test_runner_end_to_end(tmp_path):
    from tests.test_kospi_mcap_quarterly_v2_d1_d2 import TH, frames, _write_cp949
    from paper.strategies.kospi_mcap_quarterly_v2.src import run_rebalance_plan as R
    p, b, f = frames()
    files = [_write_cp949(p, tmp_path / "a.csv"), _write_cp949(b, tmp_path / "b.csv"), _write_cp949(f, tmp_path / "c.csv")]
    th = tmp_path / "th.json"
    th.write_text(json.dumps(dict(TH, index_window=3)), encoding="utf-8")
    idx = tmp_path / "idx.csv"
    pd.DataFrame({"date": ["20260926", "20260929", "20260930"], "index_code": "2001", "index_name": "KOSPI200",
                  "close": ["100", "100", "130"], "fetched_at": "x"}).to_csv(idx, index=False)
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps(dict(CFG, n_names=3, min_names_for_cap=3, weight_cap=0.5)), encoding="utf-8")
    out = tmp_path / "out"
    res = R.run("20260930", files, out, index_csv=idx, thresholds_path=th, strategy_cfg_path=cfg, format_test=True)
    assert res["stage"] == "D7", res
    names = {x.name for x in out.iterdir()}
    assert {"selection_20260930.csv", "target_portfolio_20260930.csv", "target_20260930_summary.json",
            "target_log.jsonl"} <= names
    assert set(pd.read_csv(out / "selection_20260930.csv", dtype={"code": str})["code"]) == {"005930", "000660", "0120G0"}
