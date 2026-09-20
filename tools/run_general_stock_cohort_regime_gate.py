"""Read-only exploratory check: does a market-regime gate explain the sign flip in the
UPTREND x HIGH_LIQUID structural cohort's absolute cost-adjusted return across periods?

Pre-registered before running (see .agent/PLANS.md 2026-07-21 entry):
- Population: existing structural cohort (in_structural_cohort flag), no new definition.
- Primary metric: cohort daily equal-weight cost-adjusted net return (bps), full cohort,
  no strength-decile (S10) filter.
- Primary holding period: h60. h20/h120 are diagnostic only.
- New axis: research_regime (BULL/BEAR/SIDEWAYS/STRESS/TRANSITION), reusing the
  already lookahead-audited _assign_report_research_regime from report_backtest_v41_1.py.
- Periods: the same four fixed partitions already used in the General Stock thread.
- Minimum cell: 30 unique signal dates, else DEFERRED_INSUFFICIENT_SAMPLE.
- Costs: BUY 0.6% / SELL 0.8%, unchanged from prior diagnostics.
- Exploratory only. No Gate/LOCK/order/candidate-generator change.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STRUCTURE_RUNNER_PATH = ROOT / "tools" / "run_general_stock_structure_outcome_map.py"
ROUND1_PATH = ROOT / "tools" / "run_strategy_validation_round1.py"
OUT_PREFIX = "general_stock_cohort_regime_gate_20260721"
# [2026-09-10] 라이브 설정에 맞춰 교정 (BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2(수수료) + 0.001*2(슬리피지) + 0.002(거래세) = 0.00400.
#   **과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
#   BUY_COST = 슬리피지만(0.001). SELL_COST = 슬리피지 + 거래세(0.001+0.002).
BUY_COST = 0.001
SELL_COST = 0.003
MIN_SIGNAL_DATES = 30
PRIMARY_HORIZON = 60
REGIMES = ("BULL", "BEAR", "SIDEWAYS", "STRESS", "TRANSITION")
REPLAY_CUTOFFS = (pd.Timestamp("2021-12-31"), pd.Timestamp("2022-12-30"), pd.Timestamp("2023-12-29"), pd.Timestamp("2024-12-30"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def regime_replay_check(report: Any, factors: pd.DataFrame, full_regime: pd.Series) -> list[dict[str, object]]:
    outcome: list[dict[str, object]] = []
    for cutoff in REPLAY_CUTOFFS:
        truncated = factors.loc[pd.to_datetime(factors["date"]).le(cutoff)].copy()
        replay = report._assign_report_research_regime(truncated)
        common = full_regime.index.intersection(replay.index)
        mismatch = int((full_regime.loc[common].astype(str) != replay.loc[common].astype(str)).sum())
        outcome.append({
            "cutoff": cutoff.strftime("%Y-%m-%d"),
            "compared_dates": int(len(common)),
            "mismatch_dates": mismatch,
            "status": "PASS" if mismatch == 0 else "FAIL",
        })
    return outcome


def add_net_returns(universe: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    out = universe.copy()
    for horizon in horizons:
        exit_column = f"exit_close_h{horizon}"
        net_column = f"net_return_h{horizon}"
        valid = out["entry_open"].gt(0) & out[exit_column].gt(0)
        out[net_column] = ((out[exit_column] * (1.0 - SELL_COST)) / (out["entry_open"] * (1.0 + BUY_COST)) - 1.0).where(valid)
    return out


def daily_basket(frame: pd.DataFrame, return_column: str) -> pd.DataFrame:
    return frame.groupby("date", as_index=False).agg(
        net_return=(return_column, "mean"),
        constituent_count=(return_column, "size"),
    )


def summarize_cell(daily: pd.DataFrame, partition: str, horizon: int, regime_label: str) -> dict[str, object]:
    if daily.empty:
        return {
            "partition": partition, "horizon": f"h{horizon}", "regime": regime_label,
            "signal_dates": 0, "candidate_rows": 0, "avg_constituents": None,
            "mean_daily_net_return_bps": None, "positive_daily_net_rate": None,
            "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    return {
        "partition": partition,
        "horizon": f"h{horizon}",
        "regime": regime_label,
        "signal_dates": int(len(daily)),
        "candidate_rows": int(daily["constituent_count"].sum()),
        "avg_constituents": round(float(daily["constituent_count"].mean()), 4),
        "mean_daily_net_return_bps": round(float(daily["net_return"].mean() * 10_000), 4),
        "positive_daily_net_rate": round(float(daily["net_return"].gt(0).mean()), 6),
        "status": "DESCRIPTIVE" if len(daily) >= MIN_SIGNAL_DATES else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    structure = load_module(STRUCTURE_RUNNER_PATH, "cohort_regime_gate_structure")
    round1 = load_module(ROUND1_PATH, "cohort_regime_gate_round1")
    report = round1.load_report()

    print("[1/6] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})

    print("[2/6] building structural cohort (UPTREND x HIGH_LIQUID, no strength filter)")
    universe = structure.build_forward_panel(raw)
    universe = structure.attach_point_in_time_universe(universe)
    universe = structure.add_observable_states(universe)
    # Same cohort definition as run_general_stock_strength_within_structure.py:add_strength_signal,
    # reused verbatim (no new population rule) but without the strength-decile filter.
    universe["in_structural_cohort"] = universe["stock_trend_state"].eq("UPTREND") & universe["liquidity_tier"].eq("HIGH_LIQUID")
    universe = add_net_returns(universe, structure.HORIZONS)

    print("[3/6] computing as-of factors and ex-ante regime (reused, lookahead-audited)")
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    full_regime = report._assign_report_research_regime(factors)
    replay = regime_replay_check(report, factors, full_regime)
    if any(item["status"] != "PASS" for item in replay):
        raise RuntimeError(f"regime truncate-replay failed: {replay}")

    print("[4/6] joining regime label onto cohort universe by date")
    universe["research_regime"] = universe["date"].map(full_regime)

    print("[5/6] summarizing cohort daily basket by partition x horizon x regime")
    rows: list[dict[str, object]] = []
    for partition, start, end in structure.PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in structure.HORIZONS:
            return_column = f"net_return_h{horizon}"
            cohort = part.loc[part["in_structural_cohort"] & part[return_column].notna()].copy()
            all_daily = daily_basket(cohort, return_column)
            rows.append(summarize_cell(all_daily, partition, horizon, "ALL_REGIMES"))
            for regime in REGIMES:
                reg_daily = daily_basket(cohort.loc[cohort["research_regime"].eq(regime)], return_column)
                rows.append(summarize_cell(reg_daily, partition, horizon, regime))
            non_bull_daily = daily_basket(cohort.loc[cohort["research_regime"].ne("BULL")], return_column)
            rows.append(summarize_cell(non_bull_daily, partition, horizon, "NON_BULL"))

    summary = pd.DataFrame(rows).sort_values(["partition", "horizon", "regime"], kind="mergesort").reset_index(drop=True)

    print("[6/6] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_cohort_regime_gate_exploration",
        "exploratory_only": True,
        "pre_registration": {
            "population": "existing UPTREND x HIGH_LIQUID structural cohort, no new definition",
            "primary_metric": "cohort daily equal-weight cost-adjusted net return (bps), full cohort, no strength-decile filter",
            "primary_horizon": f"h{PRIMARY_HORIZON}",
            "new_axis": "research_regime (BULL/BEAR/SIDEWAYS/STRESS/TRANSITION), reused from report_backtest_v41_1.py",
            "periods": "same four fixed partitions as prior General Stock thread",
            "min_cell_signal_dates": MIN_SIGNAL_DATES,
            "costs": {"buy_cost": BUY_COST, "sell_cost": SELL_COST},
        },
        "price_history_contract": integrity,
        "regime_truncate_replay": replay,
        "input": {
            "universe_rows": int(len(universe)),
            "universe_signal_dates": int(universe["date"].nunique()),
            "structure_runner_sha256": sha256(STRUCTURE_RUNNER_PATH),
            "round1_module_sha256": sha256(ROUND1_PATH),
            "runner_sha256": sha256(Path(__file__)),
        },
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_EXPLORATORY_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    primary = summary.loc[(summary["horizon"] == f"h{PRIMARY_HORIZON}")]
    lines = [
        "# Structural Cohort x Market-Regime Gate — Exploratory",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- primary: h{PRIMARY_HORIZON} daily equal-weight cohort net-return, split by research_regime",
        "- exploratory diagnostic only; no promotion or operating change",
        "",
        "## Primary horizon (h60) results",
        "",
        "```csv",
        primary.to_csv(index=False),
        "```",
        "",
        "## Regime truncate-replay",
        "",
        "```csv",
        pd.DataFrame(replay).to_csv(index=False),
        "```",
        "",
        "## Interpretation limit",
        "",
        "- This re-splits an already-defined cohort by an already-defined regime label; it is not a new signal or a new population.",
        "- It does not set a candidate cap, position size, stop, profit target, entry/exit rule, or operating policy.",
        "- BULL-only selection here is descriptive; confirming it requires a separate Validation/OOS round with the gate pre-registered before looking at Validation/OOS data.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
