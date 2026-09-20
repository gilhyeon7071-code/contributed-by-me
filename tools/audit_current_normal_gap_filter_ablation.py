"""Read-only h5 ablation of the current normal entry gap-history guard.

The script intentionally freezes the current L0 base selector and current
entry_gap_risk_guard values. It does not change any operating policy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

import generate_candidates_v41_1 as gen


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
META_PATH = LOGS / "candidates_latest_meta.json"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _split_name(value: pd.Timestamp) -> str:
    if value <= pd.Timestamp("2023-12-29"):
        return "TRAIN_2020_2023"
    if value <= pd.Timestamp("2024-12-31"):
        return "VALIDATION_2024"
    return "OOS_2025_2026H1"


def _summary(frame: pd.DataFrame, cohort: str, split: str) -> dict[str, object]:
    part = frame.loc[frame["split"] == split].copy()
    if part.empty:
        return {
            "cohort": cohort,
            "split": split,
            "candidate_rows": 0,
            "signal_days": 0,
            "mean_daily_net_bps": None,
            "median_daily_net_bps": None,
            "positive_day_rate": None,
            "status": "DEFERRED_INSUFFICIENT_SAMPLE",
        }
    daily = part.groupby("date", as_index=False)["net_ret_h5"].mean()
    signal_days = int(len(daily))
    return {
        "cohort": cohort,
        "split": split,
        "candidate_rows": int(len(part)),
        "signal_days": signal_days,
        "mean_daily_net_bps": round(float(daily["net_ret_h5"].mean() * 10_000), 4),
        "median_daily_net_bps": round(float(daily["net_ret_h5"].median() * 10_000), 4),
        "positive_day_rate": round(float((daily["net_ret_h5"] > 0).mean()), 6),
        "status": "DESCRIPTIVE" if signal_days >= 20 else "DEFERRED_INSUFFICIENT_SAMPLE",
    }


def run(out_prefix: str) -> dict[str, object]:
    out_json = LOGS / f"{out_prefix}.json"
    out_csv = LOGS / f"{out_prefix}.csv"
    meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    params = dict(meta["chosen_params"])
    gap_cfg = dict(cfg["entry_gap_risk_guard"])
    fee = _float(cfg.get("fee_pct"), 0.005)
    slip = _float(cfg.get("slippage_pct"), 0.001)
    tax = _float(cfg.get("sell_tax_pct"), 0.002)

    print("[1/5] loading price data")
    raw = gen._load_data()
    print("[2/5] computing frozen current L0 features")
    prices, _, _ = gen._compute_factors(raw)
    prices = prices.copy()
    prices["code"] = prices["code"].astype(str).str.zfill(6)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["code", "date"]).reset_index(drop=True)

    print("[3/5] selecting the current L0 base population")
    candidates = gen._select_candidates(prices, params).copy()
    candidates["code"] = candidates["code"].astype(str).str.zfill(6)
    candidates["date"] = pd.to_datetime(candidates["date"])
    candidates = candidates.loc[candidates["date"].notna()].copy()

    print("[4/5] reconstructing the current gap-history guard")
    bars = prices[["code", "date", "open", "close"]].copy()
    bars["open"] = pd.to_numeric(bars["open"], errors="coerce")
    bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
    bars = bars.sort_values(["code", "date"]).reset_index(drop=True)
    prev_close = bars.groupby("code", sort=False)["close"].shift(1)
    bars["gap_pct"] = bars["open"] / prev_close - 1.0
    threshold = abs(_float(gap_cfg["down_gap_threshold_pct"]))
    # The first bar for each code has no previous close. It is a non-hit.
    bars["down_gap_hit"] = (bars["gap_pct"] <= -threshold).fillna(False).astype("int8")
    lookback = int(gap_cfg["lookback_sessions"])
    bars["recent_down_gap_count"] = bars.groupby("code", sort=False)["down_gap_hit"].transform(
        lambda series: series.rolling(lookback, min_periods=1).sum()
    )
    candidates = candidates.merge(
        bars[["code", "date", "recent_down_gap_count"]],
        on=["code", "date"],
        how="left",
        validate="many_to_one",
    )
    candidates["recent_down_gap_count"] = pd.to_numeric(
        candidates["recent_down_gap_count"], errors="coerce"
    ).fillna(0).astype("int64")
    candidates["gap_guard_block"] = candidates["recent_down_gap_count"] > int(gap_cfg["max_down_gap_count"])

    calendar = pd.DatetimeIndex(sorted(prices["date"].dropna().unique()))
    calendar_index = calendar.get_indexer(candidates["date"])
    candidates["calendar_idx"] = calendar_index
    candidates = candidates.loc[candidates["calendar_idx"] >= 0].copy()
    candidates["entry_date"] = [
        calendar[index + 1] if index + 1 < len(calendar) else pd.NaT
        for index in candidates["calendar_idx"].astype(int)
    ]
    candidates["exit_date_h5"] = [
        calendar[index + 5] if index + 5 < len(calendar) else pd.NaT
        for index in candidates["calendar_idx"].astype(int)
    ]
    entry = bars[["code", "date", "open"]].rename(columns={"date": "entry_date", "open": "entry_open"})
    exit_h5 = bars[["code", "date", "close"]].rename(columns={"date": "exit_date_h5", "close": "exit_close_h5"})
    candidates = candidates.merge(entry, on=["code", "entry_date"], how="left", validate="many_to_one")
    candidates = candidates.merge(exit_h5, on=["code", "exit_date_h5"], how="left", validate="many_to_one")
    evaluable = candidates.loc[candidates["entry_open"].gt(0) & candidates["exit_close_h5"].gt(0)].copy()
    evaluable["net_ret_h5"] = (
        evaluable["exit_close_h5"] * (1.0 - fee - slip - tax)
    ) / (evaluable["entry_open"] * (1.0 + fee + slip)) - 1.0
    evaluable["split"] = evaluable["date"].map(_split_name)

    print("[5/5] writing descriptive result")
    splits = ["TRAIN_2020_2023", "VALIDATION_2024", "OOS_2025_2026H1"]
    rows: list[dict[str, object]] = []
    for split in splits:
        rows.append(_summary(evaluable, "BASE_ALL", split))
        rows.append(_summary(evaluable.loc[~evaluable["gap_guard_block"]], "GAP_GUARD_PASS", split))
        rows.append(_summary(evaluable.loc[evaluable["gap_guard_block"]], "GAP_GUARD_BLOCK", split))
        pass_daily = evaluable.loc[(evaluable["split"] == split) & (~evaluable["gap_guard_block"])].groupby("date")["net_ret_h5"].mean()
        block_daily = evaluable.loc[(evaluable["split"] == split) & (evaluable["gap_guard_block"])].groupby("date")["net_ret_h5"].mean()
        common = pass_daily.index.intersection(block_daily.index)
        delta = pass_daily.loc[common] - block_daily.loc[common]
        rows.append(
            {
                "cohort": "PASS_MINUS_BLOCK_SAME_SIGNAL_DAYS",
                "split": split,
                "candidate_rows": None,
                "signal_days": int(len(common)),
                "mean_daily_net_bps": round(float(delta.mean() * 10_000), 4) if len(delta) else None,
                "median_daily_net_bps": round(float(delta.median() * 10_000), 4) if len(delta) else None,
                "positive_day_rate": round(float((delta > 0).mean()), 6) if len(delta) else None,
                "status": "DESCRIPTIVE" if len(common) >= 20 else "DEFERRED_INSUFFICIENT_SAMPLE",
            }
        )
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8-sig")
    payload: dict[str, object] = {
        "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_current_normal_gap_guard_ablation",
        "question": "Does the current entry_gap_risk_guard improve h5 next-actual-session-open to h5-close candidate quality within the frozen current L0 base selector?",
        "protocol": {
            "population": "current candidate generator L0 AND selector only; no sector/news/final-score additions",
            "date_range": [str(prices["date"].min().date()), str(prices["date"].max().date())],
            "entry": "next actual global trading session open",
            "exit": "h5 actual global trading session close",
            "primary_metric": "signal-date equal-weight basket net return, h5",
            "costs": {"fee_pct_each_side": fee, "slippage_pct_each_side": slip, "sell_tax_pct": tax},
            "splits": splits,
            "minimum_signal_days_for_descriptive_status": 20,
            "no_parameter_search": True,
        },
        "gap_guard": gap_cfg,
        "input": {
            "price_rows": int(len(prices)),
            "base_candidate_rows_before_h5_availability": int(len(candidates)),
            "evaluable_candidate_rows": int(len(evaluable)),
            "base_candidate_signal_dates": int(candidates["date"].nunique()),
            "evaluable_signal_dates": int(evaluable["date"].nunique()),
            "generator_hash": _sha256(ROOT / "generate_candidates_v41_1.py"),
            "paper_engine_hash": _sha256(ROOT / "paper_engine.py"),
            "candidate_meta_hash": _sha256(META_PATH),
        },
        "results": rows,
        "operational_change": False,
        "policy_change": False,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-prefix", default="current_normal_gap_filter_ablation_20260720")
    args = parser.parse_args()
    result = run(args.out_prefix)
    print(json.dumps({"status": result["status"], "input": result["input"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
