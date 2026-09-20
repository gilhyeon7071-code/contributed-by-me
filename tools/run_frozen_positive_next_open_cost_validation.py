"""Research-only cost/next-open validation of frozen historical-positive combinations."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
AXIS = ROOT / "tools" / "build_new_method_strategy_axis_matrix.py"
AXIS_REPEAT = LOG / "new_method_strategy_axis_matrix_repeat_latest.csv"
LIB_REPEAT = LOG / "new_method_strategy_library_repeat_latest.csv"
OUT_TRADES = LOG / "frozen_positive_next_open_cost_trades_latest.csv"
OUT_SUMMARY = LOG / "frozen_positive_next_open_cost_summary_latest.csv"
OUT_JSON = LOG / "frozen_positive_next_open_cost_validation_latest.json"
OUT_MD = LOG / "frozen_positive_next_open_cost_validation_latest.md"

START = pd.Timestamp("2025-06-01")
END = pd.Timestamp("2026-06-30")
SLIPPAGE = 0.001
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX 로 쓰인다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
#   이 파일은 SLIPPAGE 를 진입·청산에 따로 곱한다(L74~75). COST 는 그 위에
#   한 번 빼는 값이므로 **남는 것은 매도 거래세 0.002 뿐**이다.
#   왕복 = 0.001*2(슬리피지) + 0.002(거래세) = 0.00400 으로 라이브와 같다.
COST = 0.002
PERIODS = {
    "P1_202506_202509": (pd.Timestamp("2025-06-01"), pd.Timestamp("2025-09-30")),
    "P2_202510_202512": (pd.Timestamp("2025-10-01"), pd.Timestamp("2025-12-31")),
    "P3_202601_202603": (pd.Timestamp("2026-01-01"), pd.Timestamp("2026-03-31")),
    "P4_202604_202606": (pd.Timestamp("2026-04-01"), pd.Timestamp("2026-06-30")),
}


def load_axis():
    spec = importlib.util.spec_from_file_location("axis_base", AXIS)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load axis base")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def frozen_rows() -> pd.DataFrame:
    frames = []
    for path, source in ((AXIS_REPEAT, "axis"), (LIB_REPEAT, "library")):
        if not path.exists():
            raise FileNotFoundError(path)
        x = pd.read_csv(path)
        x["source"] = source
        frames.append(x[x["evidence_state"].eq("TIME_ORDERED_REPEAT_POSITIVE")].copy())
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out.drop_duplicates(subset=["source", "scope", "horizon", "research_regime", "strategy_family", "signal_name", "strategy_name", "baseline_type"], keep="first")
    if out.empty:
        raise RuntimeError("no frozen positive combinations")
    return out


def add_next_open_returns(f: pd.DataFrame) -> pd.DataFrame:
    out = f.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    g = out.groupby("price_history_key", sort=False)
    entry_open = g["open"].shift(-1)
    entry_session = g["price_session_index"].shift(-1)
    entry_exact = entry_session.sub(out["price_session_index"]).eq(1)
    for label, days in (("h1", 1), ("h2", 2), ("h5", 5)):
        exit_close = g["close"].shift(-days)
        exit_session = g["price_session_index"].shift(-days)
        exact = entry_exact & exit_session.sub(out["price_session_index"]).eq(days)
        entry_px = entry_open * (1.0 + SLIPPAGE)
        exit_px = exit_close * (1.0 - SLIPPAGE)
        out[f"next_open_return_{label}"] = (exit_px.div(entry_px).sub(1.0) - COST).where(exact)
    return out


def make_rules(panel: pd.DataFrame, base) -> pd.DataFrame:
    out = base._strategy_features(panel)
    out = base._atomic_signals(out)
    g = out.groupby("price_history_key", sort=False)
    prev_high20 = g["close"].transform(lambda x: x.rolling(20, min_periods=20).max().shift(1))
    prev_high60 = g["close"].transform(lambda x: x.rolling(60, min_periods=60).max().shift(1))
    low14 = g["low"].transform(lambda x: x.rolling(14, min_periods=14).min())
    high14 = g["high"].transform(lambda x: x.rolling(14, min_periods=14).max())
    out["stoch_k14"] = 100 * (out["close"] - low14) / (high14 - low14 + 1e-9)
    out["stoch_d3"] = g["stoch_k14"].transform(lambda x: x.rolling(3, min_periods=3).mean())
    prev_k = g["stoch_k14"].shift(1)
    prev_d = g["stoch_d3"].shift(1)
    value_rank = out["value"].groupby(out["date"], sort=False).rank(method="first", pct=True)
    atr_rank = out["atr_pct"].groupby(out["date"], sort=False).rank(method="first", pct=True)
    out["strategy_MR_BOLLINGER"] = out["MR_BOLLINGER"].fillna(False)
    out["strategy_MA_CROSS_UP"] = out["MA_CROSS_UP"].fillna(False)
    out["strategy_BREAKOUT_252D"] = out["BREAKOUT_252D"].fillna(False)
    out["strategy_RSI_OVERSOLD"] = out["rsi14"].le(30).fillna(False)
    out["strategy_BOLLINGER_MR"] = out["MR_BOLLINGER"].fillna(False)
    out["strategy_Z_SCORE_MR"] = out["strategy_z20"].le(-1.5).fillna(False)
    out["strategy_STOCH_OVERSOLD_CROSS"] = (out["stoch_k14"].lt(20) & out["stoch_k14"].gt(out["stoch_d3"]) & prev_k.le(prev_d)).fillna(False)
    out["strategy_SIMPLE_MOMENTUM"] = out["RS_Q5"].fillna(False)
    out["strategy_MACD_GOLDEN"] = out["macd_golden"].fillna(False)
    out["strategy_VOLUME_BREAKOUT"] = (out["close"].gt(prev_high20) & out["V_ACCEL_Q5"]).fillna(False)
    out["strategy_SUPPORT_RESISTANCE_BREAK"] = out["close"].gt(prev_high60).fillna(False)
    out["strategy_MOMENTUM_LOW_VOL"] = (out["RS_Q5"] & atr_rank.le(0.20)).fillna(False)
    out["strategy_MULTIFACTOR_PROXY"] = (out["RS_Q5"] & value_rank.ge(0.80) & atr_rank.le(0.20)).fillna(False)
    out["strategy_SQUEEZE_VOLUME_BREAK"] = (out["close"].gt(prev_high20) & out["V_ACCEL_Q5"] & (out["close"].groupby(out["price_history_key"], sort=False).transform(lambda x: x.rolling(20, min_periods=20).std(ddof=0) / (x.rolling(20, min_periods=20).mean() + 1e-9)).groupby(out["date"], sort=False).rank(method="first", pct=True).le(0.20))).fillna(False)
    return out


def mask_for(row: pd.Series, panel: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=panel.index)
    scope = str(row.get("scope", ""))
    regime = row.get("research_regime")
    if pd.notna(regime) and str(regime) not in ("", "nan", "None"):
        mask &= panel["research_regime"].eq(str(regime))
    strategy = row.get("strategy_family") if pd.notna(row.get("strategy_family")) else row.get("strategy_name")
    if pd.notna(strategy) and str(strategy) not in ("", "nan", "None"):
        col = "strategy_" + str(strategy)
        if col not in panel:
            raise RuntimeError(f"missing strategy rule {col}")
        mask &= panel[col]
    signal = row.get("signal_name")
    if pd.notna(signal) and str(signal) not in ("", "nan", "None"):
        if str(signal) not in panel:
            raise RuntimeError(f"missing signal rule {signal}")
        mask &= panel[str(signal)]
    return mask


def period_for(d: pd.Series) -> pd.Series:
    p = pd.Series(pd.NA, index=d.index, dtype="object")
    for name, (start, end) in PERIODS.items():
        p.loc[d.between(start, end)] = name
    return p


def metric(x: pd.DataFrame, ret_col: str) -> dict[str, object]:
    x = x[x[ret_col].notna()].copy()
    if x.empty:
        return {"trade_rows": 0, "unique_dates": 0, "mean_net_bps": None, "median_net_bps": None, "win_rate": None, "profit_factor": None, "daily_mean_net_bps": None}
    r = x[ret_col].astype(float)
    daily = x.groupby("date")[ret_col].mean()
    pos, neg = float(r[r > 0].sum()), float(-r[r < 0].sum())
    return {"trade_rows": int(len(r)), "unique_dates": int(x["date"].nunique()), "mean_net_bps": float(r.mean() * 10000), "median_net_bps": float(r.median() * 10000), "win_rate": float((r > 0).mean()), "profit_factor": float(pos / neg) if neg > 0 else (999.0 if pos > 0 else None), "daily_mean_net_bps": float(daily.mean() * 10000)}


def main() -> int:
    base = load_axis()
    raw = base._load_report_module().load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    report = base._load_report_module()
    f = report.compute_factors(raw)
    f["date"] = pd.to_datetime(f["date"], errors="coerce").dt.normalize()
    f["research_regime"] = f["date"].map(report._assign_report_research_regime(f))
    f = make_rules(f, base)
    f = add_next_open_returns(f)
    panel = f[f["date"].between(START, END) & f["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"]) & pd.to_numeric(f["close"], errors="coerce").gt(0) & pd.to_numeric(f["value"], errors="coerce").gt(0)].copy()
    panel["period"] = period_for(panel["date"])
    frozen = frozen_rows()
    trade_rows = []
    summary_rows = []
    for i, row in frozen.reset_index(drop=True).iterrows():
        mask = mask_for(row, panel)
        horizon = str(row["horizon"])
        ret_col = f"next_open_return_{horizon}"
        q = panel.loc[mask & panel[ret_col].notna(), ["date", "code", "market", "research_regime", "period", ret_col]].copy()
        q.insert(0, "frozen_id", i)
        q.insert(1, "source", row.get("source"))
        q.insert(2, "scope", row.get("scope"))
        q.insert(3, "horizon", horizon)
        q.insert(4, "strategy", row.get("strategy_family") if pd.notna(row.get("strategy_family")) else row.get("strategy_name"))
        q.insert(5, "signal", row.get("signal_name"))
        q.insert(6, "regime", row.get("research_regime"))
        trade_rows.append(q)
        base_rec = {"frozen_id": i, "source": row.get("source"), "scope": row.get("scope"), "horizon": horizon, "strategy": row.get("strategy_family") if pd.notna(row.get("strategy_family")) else row.get("strategy_name"), "signal": row.get("signal_name"), "regime": row.get("research_regime"), "slippage": SLIPPAGE, "round_trip_cost": COST}
        for part, x in (("all", q), ("train", q[q["period"].isin(["P1_202506_202509", "P2_202510_202512", "P3_202601_202603"])]), ("holdout", q[q["period"].eq("P4_202604_202606")])):
            base_rec.update({f"{part}_{k}": v for k, v in metric(x.rename(columns={ret_col: "ret"}), "ret").items()})
        train_daily = [base_rec.get(f"train_{x}") for x in ("daily_mean_net_bps",)]
        hold = base_rec.get("holdout_daily_mean_net_bps")
        base_rec["cost_verdict"] = "COST_NET_POSITIVE_TRAIN_HOLDOUT" if train_daily[0] is not None and train_daily[0] > 0 and hold is not None and hold > 0 else "COST_NET_MIXED_OR_NEGATIVE"
        summary_rows.append(base_rec)
    trades = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    trades.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "frozen_positive_next_open_cost_validation", "window": {"start": str(START.date()), "end": str(END.date())}, "cost_model": {"entry": "next_actual_session_open_plus_slippage", "exit": "horizon_actual_session_close_minus_slippage", "slippage": SLIPPAGE, "round_trip_cost": COST}, "price_history_contract": integrity, "frozen_combinations": int(len(frozen)), "panel_rows": int(len(panel)), "trade_rows": int(len(trades)), "verdict_counts": summary["cost_verdict"].value_counts().to_dict(), "outputs": {"trades": str(OUT_TRADES), "summary": str(OUT_SUMMARY)}, "operational_change": False, "limitations": ["Frozen combinations were selected by prior close-to-close exploration; no new combinations were added.", "No capacity, fill rejection, or paper-runtime confirmation was modeled.", "This is a cost/entry timing research check, not an operating promotion decision."]}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    positive = summary[summary["cost_verdict"].eq("COST_NET_POSITIVE_TRAIN_HOLDOUT")]
    lines = ["# Frozen Positive Next-Open Cost Validation", "", f"- generated_at: {payload['generated_at']}", f"- frozen_combinations: {len(frozen)}", f"- panel_rows: {len(panel)}", f"- trade_rows: {len(trades)}", f"- cost_positive_train_holdout: {len(positive)}", "", "## Cost-positive combinations"]
    if positive.empty:
        lines.append("- none")
    else:
        for _, r in positive.sort_values("holdout_daily_mean_net_bps", ascending=False).iterrows():
            lines.append(f"- {r['source']} / {r['scope']} / {r['horizon']} / {r['strategy']} / {r['signal']} / {r['regime']}: train={float(r['train_daily_mean_net_bps']):.2f} bps, holdout={float(r['holdout_daily_mean_net_bps']):.2f} bps")
    lines.extend(["", "## Limitations", *[f"- {x}" for x in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
