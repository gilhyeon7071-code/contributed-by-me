"""Build a separate research paper ledger from the six deferred frozen rules."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
TRADES = LOG / "frozen_positive_next_open_cost_trades_latest.csv"
COST_SUMMARY = LOG / "frozen_positive_next_open_cost_summary_latest.csv"
OUT_TRADES = LOG / "deferred_research_paper_ledger_trades_latest.csv"
OUT_DAILY = LOG / "deferred_research_paper_ledger_daily_latest.csv"
OUT_JSON = LOG / "deferred_research_paper_ledger_latest.json"
OUT_MD = LOG / "deferred_research_paper_ledger_latest.md"
REPORT = ROOT / "report_backtest_v41_1.py"


def load_report():
    import importlib.util

    spec = importlib.util.spec_from_file_location("paper_report", REPORT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load report module")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main() -> int:
    summary = pd.read_csv(COST_SUMMARY)
    ids = set(summary.loc[summary["cost_verdict"].eq("COST_NET_POSITIVE_TRAIN_HOLDOUT"), "frozen_id"].astype(str))
    t = pd.read_csv(TRADES, dtype={"code": str, "frozen_id": str})
    t = t[t["frozen_id"].isin(ids)].copy()
    if t.empty:
        raise SystemExit("no frozen deferred trades")
    t["code"] = t["code"].astype(str).str.zfill(6)
    t["signal_date"] = pd.to_datetime(t["date"], errors="coerce").dt.normalize()
    t["horizon_n"] = t["horizon"].str.extract(r"(\d+)")[0].astype(int)
    report = load_report()
    raw = report.load_data()
    px = raw[["code", "date", "open", "close", "price_session_index", "price_history_key"]].copy()
    px["code"] = px["code"].astype(str).str.zfill(6)
    px["date"] = pd.to_datetime(px["date"], errors="coerce").dt.normalize()
    px = px.drop_duplicates(["code", "date"])
    t = t.merge(px[["code", "date", "price_session_index"]].rename(columns={"date": "signal_date", "price_session_index": "signal_session_index"}), on=["code", "signal_date"], how="left", validate="many_to_one")
    t["entry_session_index"] = t["signal_session_index"] + 1
    t["exit_session_index"] = t["signal_session_index"] + t["horizon_n"]
    entry = px[["code", "price_session_index", "date", "open"]].rename(columns={"price_session_index": "entry_session_index", "date": "entry_date", "open": "entry_open"})
    exit_ = px[["code", "price_session_index", "date", "close"]].rename(columns={"price_session_index": "exit_session_index", "date": "exit_date", "close": "exit_close"})
    t = t.merge(entry, on=["code", "entry_session_index"], how="left", validate="many_to_one")
    t = t.merge(exit_, on=["code", "exit_session_index"], how="left", validate="many_to_one")
    t["entry_price_with_slippage"] = pd.to_numeric(t["entry_open"], errors="coerce") * 1.001
    t["exit_price_with_slippage"] = pd.to_numeric(t["exit_close"], errors="coerce") * 0.999
    # [2026-09-10] 슬리피지 0.001 을 위 두 줄에서 이미 양쪽에 곱했다.
    #   여기서 한 번 빼는 값에 남는 것은 **매도 거래세 0.002 뿐**이다.
    #   종전 0.005 는 왕복 0.700% 였다. 브로커 실측: 수수료 0 / 제세금 0.19723%.
    #   왕복 = 0.001*2 + 0.002 = 0.00400 (DEFAULT_FEE 와 동일). C6.
    SELL_TAX = 0.002
    t["recomputed_net_return"] = t["exit_price_with_slippage"].div(t["entry_price_with_slippage"]).sub(1.0) - SELL_TAX
    ret_col = [c for c in ["next_open_return_h1", "next_open_return_h2", "next_open_return_h5"] if c in t.columns]
    def stored(row):
        c = f"next_open_return_{row['horizon']}"
        return row.get(c)
    t["stored_net_return"] = t.apply(stored, axis=1)
    t["return_abs_diff"] = (pd.to_numeric(t["recomputed_net_return"], errors="coerce") - pd.to_numeric(t["stored_net_return"], errors="coerce")).abs()
    if t["return_abs_diff"].dropna().gt(1e-10).any():
        raise SystemExit("recomputed return mismatch")
    t["candidate_count_same_signal"] = t.groupby(["frozen_id", "signal_date"])["code"].transform("size")
    t["equal_weight"] = 1.0 / t["candidate_count_same_signal"]
    t["research_only"] = True
    t["operational_use"] = False
    t["paper_ledger_status"] = "RESEARCH_ONLY_ROUND_TRIP"
    daily = t.groupby(["frozen_id", "source", "scope", "horizon", "strategy", "signal", "regime", "signal_date"], as_index=False).agg(candidate_count=("code", "size"), basket_mean_net_return=("stored_net_return", "mean"), basket_mean_net_bps=("stored_net_return", lambda x: float(pd.to_numeric(x, errors="coerce").mean() * 10000)), positive_candidates=("stored_net_return", lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())))
    t.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "deferred_research_only_paper_ledger", "frozen_rules": int(t["frozen_id"].nunique()), "trade_rows": int(len(t)), "signal_dates": int(t["signal_date"].nunique()), "daily_basket_rows": int(len(daily)), "policy": {"allocation": "equal_weight_per_frozen_rule_and_signal_date", "entry": "next_actual_session_open", "exit": "horizon_actual_session_close", "slippage_each_side": 0.001, "round_trip_cost": 0.005, "capacity_cap": None, "overlap": "allowed_in_research_only"}, "recomputed_return_max_abs_diff": float(t["return_abs_diff"].max()), "research_only": True, "operational_use": False, "outputs": {"trades": str(OUT_TRADES), "daily": str(OUT_DAILY)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Research Paper Ledger", "", f"- frozen_rules: {payload['frozen_rules']}", f"- trade_rows: {payload['trade_rows']}", f"- signal_dates: {payload['signal_dates']}", f"- daily_basket_rows: {payload['daily_basket_rows']}", f"- recomputed_return_max_abs_diff: {payload['recomputed_return_max_abs_diff']}", "- research_only: true", "- operational_use: false", "", "## Policy", "- equal weight per frozen rule and signal date", "- next actual-session open entry", "- horizon actual-session close exit", "- no capacity cap; overlap allowed only in research"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
