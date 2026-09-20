"""Read-only cost-aware execution diagnostic for the exploratory structural S10 cohort."""
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
STRENGTH_RUNNER_PATH = ROOT / "tools" / "run_general_stock_strength_within_structure.py"
OUT_PREFIX = "general_stock_s10_execution_diagnostic_20260720"
# [2026-09-10] 라이브 설정에 맞춰 교정 (BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   BUY_COST = 슬리피지만(0.001). SELL_COST = 슬리피지 + 거래세(0.001+0.002).
#   왕복 0.400% = optimize_params_v41_1.DEFAULT_FEE 와 동일.
#   **과거 산출물은 더 비싼 세계(왕복 1.400%)의 값이라 새 실행과 직접 비교할 수 없다.**
BUY_COST = 0.001
SELL_COST = 0.003


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_strength_runner() -> Any:
    spec = importlib.util.spec_from_file_location("s10_execution_strength", STRENGTH_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {STRENGTH_RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


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


def summarize(valid: pd.DataFrame, return_column: str, partition: str) -> dict[str, object]:
    cohort = valid.loc[valid["in_structural_cohort"] & valid["strength_decile"].notna()].copy()
    s10 = cohort.loc[cohort["strength_decile"].eq(10)].copy()
    if s10.empty:
        return {
            "partition": partition, "horizon": return_column.replace("net_return_", ""), "signal_dates": 0,
            "candidate_rows": 0, "avg_constituents": None, "mean_daily_net_return_bps": None,
            "positive_daily_net_rate": None, "cohort_same_dates_net_bps": None,
            "incremental_net_bps_vs_cohort": None, "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    s10_daily = daily_basket(s10, return_column)
    cohort_daily = daily_basket(cohort, return_column).rename(columns={"net_return": "cohort_net_return"})
    joined = s10_daily.merge(cohort_daily[["date", "cohort_net_return"]], on="date", how="left", validate="one_to_one")
    return {
        "partition": partition,
        "horizon": return_column.replace("net_return_", ""),
        "signal_dates": int(len(joined)),
        "candidate_rows": int(joined["constituent_count"].sum()),
        "avg_constituents": round(float(joined["constituent_count"].mean()), 4),
        "mean_daily_net_return_bps": round(float(joined["net_return"].mean() * 10_000), 4),
        "positive_daily_net_rate": round(float(joined["net_return"].gt(0).mean()), 6),
        "cohort_same_dates_net_bps": round(float(joined["cohort_net_return"].mean() * 10_000), 4),
        "incremental_net_bps_vs_cohort": round(float((joined["net_return"] - joined["cohort_net_return"]).mean() * 10_000), 4),
        "status": "DESCRIPTIVE" if len(joined) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def main() -> int:
    strength = load_strength_runner()
    base = strength.load_base()
    print("[1/5] loading integrity-filtered price history")
    report = base.load_report()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] building structural S10 candidates and actual-session exits")
    universe = base.build_forward_panel(raw)
    universe = base.attach_point_in_time_universe(universe)
    universe = base.add_observable_states(universe)
    universe = strength.add_strength_signal(universe)
    universe = add_net_returns(universe, base.HORIZONS)
    print("[3/5] calculating daily equal-weight cost-adjusted baskets")
    rows: list[dict[str, object]] = []
    for partition, start, end in base.PARTITIONS:
        part = universe.loc[universe["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
        for horizon in base.HORIZONS:
            return_column = f"net_return_h{horizon}"
            valid = part.loc[part[return_column].notna()].copy()
            rows.append(summarize(valid, return_column, partition))
    print("[4/5] building fixed execution diagnostic summary")
    summary = pd.DataFrame(rows).sort_values(["partition", "horizon"], kind="mergesort").reset_index(drop=True)
    print("[5/5] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_s10_structural_execution_diagnostic",
        "exploratory_only": True,
        "price_history_contract": integrity,
        "selection": "prior exploratory UPTREND × HIGH_LIQUID structural cohort with same-date/same-market 20-session strength S10",
        "execution": "next actual global-session open entry; h20/h60/h120 actual-session close exit",
        "costs": {"buy_cost": BUY_COST, "sell_cost": SELL_COST, "formula": "(exit_close * (1 - SELL_COST)) / (entry_open * (1 + BUY_COST)) - 1"},
        "primary": "h60 mean daily equal-weight S10 net-return basket; compared to full structural cohort basket on same S10 dates",
        "input": {"universe_rows": int(len(universe)), "universe_signal_dates": int(universe["date"].nunique()), "strength_runner_sha256": sha256(STRENGTH_RUNNER_PATH), "runner_sha256": sha256(Path(__file__))},
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_EXPLORATORY_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    overall_h60 = summary.loc[(summary["partition"] == "ALL_AVAILABLE") & (summary["horizon"] == "h60")]
    lines = [
        "# S10 Structural Candidate Execution Diagnostic — Exploratory",
        "",
        f"- generated_at: {payload['generated_at']}",
        "- primary: h60 daily equal-weight net-return basket after next-session-open entry and fixed costs",
        "- exploratory diagnostic only; no promotion or operating change",
        "",
        "## Overall h60",
        "",
        "```csv",
        overall_h60.to_csv(index=False),
        "```",
        "",
        "## Interpretation Limit",
        "",
        "- This evaluates a selection chosen in prior exploration, so it is not an independent OOS confirmation.",
        "- It does not set a candidate cap, position size, stop, profit target, or operating policy.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path), "universe_rows": int(len(universe))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
