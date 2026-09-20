"""Read-only Round 1 exploratory validation under STRATEGY_VALIDATION_GUARD V2."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
REPORT = ROOT / "report_backtest_v41_1.py"
POPULATION_MEMBERSHIP = ROOT / "_cache" / "krx_population_static_membership_latest.parquet"
OUT_PREFIX = "strategy_validation_round1_train_axis_summary_20260720"
TRAIN_START = pd.Timestamp("2020-01-02")
TRAIN_END = pd.Timestamp("2023-12-29")
SIGNAL_MIN_PRICE = 1_000.0
SIGNAL_MIN_VALUE = 1_000_000_000.0
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX 로 쓰인다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
FEE = 0.0
SLIPPAGE = 0.001
SELL_TAX = 0.002
MIN_SIGNAL_DATES = 30
MIN_CALENDAR_DAYS = 60
MIN_EPISODES = 2


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_report() -> Any:
    spec = importlib.util.spec_from_file_location("round1_report", REPORT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {REPORT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def coverage_block_count(dates: pd.Series) -> int:
    valid = pd.to_datetime(dates, errors="coerce").dropna()
    if valid.empty:
        return 0
    half_year = valid.dt.year.astype(str) + "-H" + np.where(valid.dt.month.le(6), "1", "2")
    return int(half_year.value_counts().ge(15).sum())


def bool_rank(frame: pd.DataFrame, column: str, ascending: bool) -> pd.Series:
    ranks = pd.to_numeric(frame[column], errors="coerce").groupby(frame["date"], sort=False).rank(
        method="first", pct=True, ascending=ascending
    )
    return ranks.le(0.20).fillna(False)


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def build_execution_panel(factors: pd.DataFrame) -> pd.DataFrame:
    work = factors.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["code"] = work["code"].astype(str).str.zfill(6)
    work = work.sort_values(["code", "date"], kind="mergesort").reset_index(drop=True)
    calendar = pd.DatetimeIndex(sorted(work["date"].dropna().unique()))
    calendar_pos = {day: i for i, day in enumerate(calendar)}
    work["_global_idx"] = work["date"].map(calendar_pos)
    work["entry_date"] = work["_global_idx"].map(
        lambda i: calendar[int(i) + 1] if pd.notna(i) and int(i) + 1 < len(calendar) else pd.NaT
    )
    work["exit_date_h5"] = work["_global_idx"].map(
        lambda i: calendar[int(i) + 5] if pd.notna(i) and int(i) + 5 < len(calendar) else pd.NaT
    )
    entry = work[["code", "date", "open"]].rename(columns={"date": "entry_date", "open": "entry_open"})
    exit_h5 = work[["code", "date", "close"]].rename(columns={"date": "exit_date_h5", "close": "exit_close_h5"})
    work = work.merge(entry, on=["code", "entry_date"], how="left", validate="many_to_one")
    work = work.merge(exit_h5, on=["code", "exit_date_h5"], how="left", validate="many_to_one")
    valid_execution = (work["entry_open"].gt(0) & work["exit_close_h5"].gt(0)).fillna(False).astype(bool)
    net_return = (
        (work["exit_close_h5"] * (1.0 - FEE - SLIPPAGE - SELL_TAX))
        / (work["entry_open"] * (1.0 + FEE + SLIPPAGE))
        - 1.0
    )
    work["net_return_h5"] = net_return.where(valid_execution)
    return work


def add_research_axes(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.sort_values(["code", "date"], kind="mergesort").copy()
    grouped = out.groupby("price_history_key", sort=False)
    ma20 = grouped["close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ma60 = grouped["close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    std20 = grouped["close"].transform(lambda x: x.rolling(20, min_periods=20).std(ddof=0))
    previous_ma20 = grouped["close"].transform(lambda x: x.rolling(20, min_periods=20).mean()).groupby(out["price_history_key"], sort=False).shift(1)
    previous_ma60 = grouped["close"].transform(lambda x: x.rolling(60, min_periods=60).mean()).groupby(out["price_history_key"], sort=False).shift(1)
    previous_high252 = grouped["close"].transform(lambda x: x.rolling(252, min_periods=60).max().shift(1))
    out["MR_BOLLINGER"] = ((out["close"] - ma20) / (std20 + 1e-9)).le(-1.5) & pd.to_numeric(out["rsi14"], errors="coerce").le(30.0)
    out["MA_CROSS_UP"] = ma20.gt(ma60) & previous_ma20.le(previous_ma60)
    out["BREAKOUT_252D"] = out["close"].gt(previous_high252)
    out["STRETCH_Q1"] = bool_rank(out, "stretch", ascending=True)
    out["RS_Q5"] = bool_rank(out, "rs", ascending=False)
    out["RS_SLOPE_Q5"] = bool_rank(out, "rs_slope", ascending=False)
    out["V_ACCEL_Q5"] = bool_rank(out, "v_accel", ascending=False)
    return out


def summarize(panel: pd.DataFrame, mask: pd.Series, axis: str, condition: str) -> dict[str, object]:
    selected = panel.loc[mask & panel["net_return_h5"].notna(), ["date", "net_return_h5"]].copy()
    baseline = panel.loc[panel["net_return_h5"].notna(), ["date", "net_return_h5"]].groupby("date", as_index=False)["net_return_h5"].mean().rename(columns={"net_return_h5": "baseline_net_return_h5"})
    if selected.empty:
        return {
            "axis": axis, "condition": condition, "candidate_rows": 0, "unique_signal_dates": 0,
            "calendar_span_days": 0, "coverage_half_year_blocks": 0, "mean_daily_net_h5_bps": None,
            "median_daily_net_h5_bps": None, "mean_daily_excess_h5_bps": None,
            "positive_daily_net_rate": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    daily = selected.groupby("date", as_index=False)["net_return_h5"].mean().rename(columns={"net_return_h5": "daily_net_return_h5"})
    daily = daily.merge(baseline, on="date", how="left", validate="one_to_one")
    daily["daily_excess_h5"] = daily["daily_net_return_h5"] - daily["baseline_net_return_h5"]
    unique_dates = int(len(daily))
    span = int((daily["date"].max() - daily["date"].min()).days) if unique_dates else 0
    coverage_blocks = coverage_block_count(daily["date"])
    status = "DESCRIPTIVE" if unique_dates >= MIN_SIGNAL_DATES and span >= MIN_CALENDAR_DAYS and coverage_blocks >= MIN_EPISODES else "DEFERRED_INSUFFICIENT_SAMPLE"
    return {
        "axis": axis,
        "condition": condition,
        "candidate_rows": int(len(selected)),
        "unique_signal_dates": unique_dates,
        "calendar_span_days": span,
        "coverage_half_year_blocks": coverage_blocks,
        "mean_daily_net_h5_bps": round(float(daily["daily_net_return_h5"].mean() * 10_000), 4),
        "median_daily_net_h5_bps": round(float(daily["daily_net_return_h5"].median() * 10_000), 4),
        "mean_daily_excess_h5_bps": round(float(daily["daily_excess_h5"].mean() * 10_000), 4),
        "positive_daily_net_rate": round(float((daily["daily_net_return_h5"] > 0).mean()), 6),
        "status": status,
    }


def regime_replay_check(report: Any, factors: pd.DataFrame, full_regime: pd.Series) -> list[dict[str, object]]:
    outcome: list[dict[str, object]] = []
    for cutoff in (pd.Timestamp("2021-12-31"), pd.Timestamp("2022-12-30"), pd.Timestamp("2023-12-29")):
        truncated = factors.loc[pd.to_datetime(factors["date"]).le(cutoff)].copy()
        replay = report._assign_report_research_regime(truncated)
        common = full_regime.index.intersection(replay.index)
        mismatch = int((full_regime.loc[common].astype(str) != replay.loc[common].astype(str)).sum())
        outcome.append({"cutoff": cutoff.strftime("%Y-%m-%d"), "compared_dates": int(len(common)), "mismatch_dates": mismatch, "status": "PASS" if mismatch == 0 else "FAIL"})
    return outcome


def main() -> int:
    report = load_report()
    print("[1/5] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] computing as-of factors and ex-ante regime")
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    full_regime = report._assign_report_research_regime(factors)
    factors["research_regime"] = factors["date"].map(full_regime)
    replay = regime_replay_check(report, factors, full_regime)
    if any(item["status"] != "PASS" for item in replay):
        raise RuntimeError(f"regime truncate-replay failed: {replay}")
    print("[3/5] building actual-session h5 execution panel")
    panel = build_execution_panel(factors)
    if not POPULATION_MEMBERSHIP.exists():
        raise RuntimeError(f"missing static population membership: {POPULATION_MEMBERSHIP}")
    membership = pd.read_parquet(POPULATION_MEMBERSHIP, columns=["as_of_date", "code", "market", "security_type"])
    membership["date"] = pd.to_datetime(membership["as_of_date"].astype(str), format="%Y%m%d", errors="coerce")
    membership["code"] = membership["code"].astype(str).str.zfill(6)
    membership["market"] = membership["market"].fillna("").astype(str).str.upper().str.strip()
    membership = membership.loc[
        membership["date"].between(TRAIN_START, TRAIN_END)
        & membership["market"].isin(["KOSPI", "KOSDAQ"])
        & membership["security_type"].fillna("").astype(str).str.upper().eq("COMMON"),
        ["date", "code", "market"],
    ].drop_duplicates(["date", "code"])
    if membership.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate static population membership key")
    panel = panel.merge(membership, on=["date", "code"], how="inner", validate="many_to_one", suffixes=("", "_membership"))
    eligible = (
        panel["date"].between(TRAIN_START, TRAIN_END)
        & pd.to_numeric(panel["open"], errors="coerce").gt(0)
        & pd.to_numeric(panel["close"], errors="coerce").ge(SIGNAL_MIN_PRICE)
        & pd.to_numeric(panel["value"], errors="coerce").ge(SIGNAL_MIN_VALUE)
    )
    panel = panel.loc[eligible].copy()
    if panel.empty:
        raise RuntimeError("empty registered research universe")
    print("[4/5] evaluating registered single axes")
    panel = add_research_axes(panel)
    rows: list[dict[str, object]] = []
    rows.append(summarize(panel, pd.Series(True, index=panel.index), "BASELINE", "ELIGIBLE_UNIVERSE"))
    for regime in ("BULL", "BEAR", "SIDEWAYS", "STRESS", "TRANSITION"):
        rows.append(summarize(panel, panel["research_regime"].eq(regime), "SITUATION", regime))
    for strategy in ("MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D"):
        rows.append(summarize(panel, panel[strategy].fillna(False), "STRATEGY_GRAMMAR", strategy))
    for signal in ("STRETCH_Q1", "RS_Q5", "RS_SLOPE_Q5", "V_ACCEL_Q5"):
        rows.append(summarize(panel, panel[signal].fillna(False), "ATOMIC_SIGNAL", signal))
    result = pd.DataFrame(rows).sort_values(["axis", "condition"], kind="mergesort").reset_index(drop=True)
    csv_path = LOG / f"{OUT_PREFIX}.csv"
    json_path = LOG / f"{OUT_PREFIX}.json"
    md_path = LOG / f"{OUT_PREFIX}.md"
    result.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_strategy_validation_round1_exploration_train_only",
        "plan": str(ROOT / "docs" / "exec-plans" / "active" / "20260720_strategy_validation_round1.md"),
        "price_history_contract": integrity,
        "contract": {
            "signal_window": [TRAIN_START.strftime("%Y-%m-%d"), TRAIN_END.strftime("%Y-%m-%d")],
            "markets": ["KOSPI", "KOSDAQ"], "signal_min_price_krw": SIGNAL_MIN_PRICE,
            "signal_min_daily_value_krw": SIGNAL_MIN_VALUE,
            "entry": "next actual global trading-session open", "exit": "h5 actual global trading-session close",
            "costs": {"fee_pct_each_side": FEE, "slippage_pct_each_side": SLIPPAGE, "sell_tax_pct": SELL_TAX},
            "primary_metric": "signal-date equal-weight candidate basket net h5 return",
            "minimum_cell": {"unique_signal_dates": MIN_SIGNAL_DATES, "calendar_span_days": MIN_CALENDAR_DAYS, "coverage_half_year_blocks": MIN_EPISODES},
            "axes": {"situation": ["BULL", "BEAR", "SIDEWAYS", "STRESS", "TRANSITION"], "strategy_grammar": ["MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D"], "atomic_signal": ["STRETCH_Q1", "RS_Q5", "RS_SLOPE_Q5", "V_ACCEL_Q5"]},
        },
        "input": {"eligible_rows": int(len(panel)), "eligible_signal_dates": int(panel["date"].nunique()), "population_membership_rows": int(len(membership)), "population_membership_hash": sha256(POPULATION_MEMBERSHIP), "generator_hash": sha256(Path(__file__)), "report_backtest_hash": sha256(REPORT)},
        "regime_truncate_replay": replay,
        "results": safe_records(result),
        "operational_change": False,
        "selection_or_promotion": "FORBIDDEN_IN_ROUND_1",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Strategy Validation Round 1 — Train Exploratory Axis Results", "", f"- generated_at: {payload['generated_at']}", f"- eligible_rows: {len(panel)}", f"- eligible_signal_dates: {panel['date'].nunique()}", "- selection_or_promotion: forbidden", "", "## Results", "", "```csv", result.to_csv(index=False), "```", "", "## Regime truncate-replay", "", "```csv", pd.DataFrame(replay).to_csv(index=False), "```", "", "## Interpretation limit", "", "- These are pre-registered single-axis exploratory results only. No Validation/OOS selection, axis conjunction, candidate-generator change, or operating application is permitted from this file."]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "csv": str(csv_path), "json": str(json_path), "md": str(md_path), "eligible_rows": int(len(panel))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())