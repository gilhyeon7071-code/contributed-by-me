"""Regime/holding-strategy HPO for the nine entry requirements (read-only)."""

from __future__ import annotations

import argparse
import gc
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from optimize_regime_strategy_signal_requirements import (
    CORE_SIGNALS,
    GENERATOR,
    LOGS,
    OFFICIAL_COST,
    OFFICIAL_SLIPPAGE,
    REGIMES,
    ROBUST_RETURN_CAP,
    ROOT,
    SPLITS,
    STABLE,
    STRATEGIES,
    add_strategy_returns,
    load_module,
    markdown_table,
)


N_ITER = 160
SEED = 411
SIGNAL_NAMES = [name for name, _ in CORE_SIGNALS]
FACTOR_NAMES = [factor for _, factor in CORE_SIGNALS]
RANGES = {
    "rs_lim": np.round(np.arange(1.10, 3.001, 0.05), 4),
    "v_accel_lim": np.round(np.arange(1.00, 8.001, 0.10), 4),
    "stretch_max": np.round(np.arange(1.05, 1.301, 0.01), 4),
    "value_min": np.arange(30e9, 300e9 + 1, 5e9),
    "atr_max": np.round(np.arange(0.03, 0.251, 0.005), 4),
    "rsi_max": np.arange(55.0, 80.1, 2.5),
    "vol_close_corr_min": np.round(np.arange(-0.20, 0.401, 0.05), 4),
    "near_52w_high_gap_max": np.round(np.arange(0.03, 0.301, 0.01), 4),
    "min_listing_days": np.array([60.0, 126.0, 252.0, 504.0]),
}


def prepare() -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    module = load_module(GENERATOR, "regime_strategy_signal_hpo_generator")
    raw = module._load_data()
    integrity = dict(getattr(raw, "attrs", {}).get("price_history_integrity", {}) or {})
    raw_rows = len(raw)
    factors, latest, _ = module._compute_factors(raw)
    del raw
    gc.collect()
    factors = add_strategy_returns(factors)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    common = factors[FACTOR_NAMES].notna().all(axis=1)
    common &= factors["market_regime"].isin(REGIMES)
    common &= factors["date"].ge(SPLITS["train"][0])
    keep = [
        "date", "code", "market_regime", "macd_golden", *FACTOR_NAMES,
        *[f"strategy_ret_h{h}" for h in STRATEGIES],
    ]
    work = factors.loc[common, keep].copy()
    del factors
    gc.collect()
    stable_raw = json.loads(STABLE.read_text(encoding="utf-8-sig"))
    stable = module._normalize_params(stable_raw)
    meta = {
        "raw_rows": int(raw_rows), "analysis_rows": int(len(work)),
        "date_min": str(work["date"].min().date()),
        "date_max": str(work["date"].max().date()),
        "latest_factor_date": str(pd.Timestamp(latest).date()),
        "codes": int(work["code"].nunique()), "dates": int(work["date"].nunique()),
        "integrity": integrity,
    }
    return work, meta, stable


def candidates(stable: dict[str, Any]) -> list[dict[str, Any]]:
    rng = np.random.default_rng(SEED)
    out = []
    base = {
        "candidate_id": 0,
        "included": SIGNAL_NAMES,
        "require_macd": bool(float(stable.get("require_macd_golden", 0.0)) >= 0.5),
        **{key: float(stable[key]) for key in RANGES},
    }
    out.append(base)
    while len(out) < N_ITER:
        included = [name for name in SIGNAL_NAMES if rng.random() < 0.50]
        if not included:
            included = [SIGNAL_NAMES[int(rng.integers(0, len(SIGNAL_NAMES)))]]
        row = {
            "candidate_id": len(out),
            "included": included,
            "require_macd": bool(rng.random() < 0.25),
        }
        for key, values in RANGES.items():
            row[key] = float(values[int(rng.integers(0, len(values)))])
        out.append(row)
    return out


def requirement_mask(df: pd.DataFrame, p: dict[str, Any]) -> np.ndarray:
    mask = np.ones(len(df), dtype=bool)
    included = set(p["included"])
    if "rs_pass" in included:
        mask &= df["rs"].to_numpy() > p["rs_lim"]
    if "v_accel_pass" in included:
        mask &= df["v_accel"].to_numpy() > p["v_accel_lim"]
    if "stretch_pass" in included:
        mask &= df["stretch"].to_numpy() <= p["stretch_max"]
    if "value_pass" in included:
        mask &= df["value"].to_numpy() >= p["value_min"]
    if "atr_pass" in included:
        mask &= df["atr14_pct"].to_numpy() <= p["atr_max"]
    if "rsi_pass" in included:
        mask &= df["rsi14"].to_numpy() <= p["rsi_max"]
    if "volcorr_pass" in included:
        mask &= df["vol_close_corr20"].to_numpy() >= p["vol_close_corr_min"]
    if "high52_pass" in included:
        mask &= df["high_52w_gap"].to_numpy() <= p["near_52w_high_gap_max"]
    if "listing_pass" in included:
        mask &= df["listing_days"].to_numpy() >= p["min_listing_days"]
    if p["require_macd"]:
        mask &= df["macd_golden"].fillna(False).to_numpy(dtype=bool)
    return mask


def metrics(df: pd.DataFrame, target: str, selected: np.ndarray) -> dict[str, Any]:
    values = pd.to_numeric(df[target], errors="coerce").to_numpy(dtype=float)
    valid = selected & np.isfinite(values)
    values = values[valid]
    dates = df.loc[valid, "date"]
    if len(values) == 0:
        return {k: None for k in (
            "n", "dates", "mean_bps", "median_bps", "robust_mean_bps", "winrate",
            "profit_factor", "daily_robust_mean_bps", "daily_alpha_bps", "daily_mdd",
        )}
    robust = np.clip(values, -ROBUST_RETURN_CAP, ROBUST_RETURN_CAP)
    pos = values[values > 0].sum()
    neg = -values[values < 0].sum()
    codes, unique_dates = pd.factorize(dates, sort=True)
    count = np.bincount(codes, minlength=len(unique_dates)).astype(float)
    sums = np.bincount(codes, weights=robust, minlength=len(unique_dates))
    daily = sums / count
    all_values = pd.to_numeric(df[target], errors="coerce").to_numpy(dtype=float)
    all_valid = np.isfinite(all_values)
    all_robust = np.clip(all_values[all_valid], -ROBUST_RETURN_CAP, ROBUST_RETURN_CAP)
    all_codes, all_dates = pd.factorize(df.loc[all_valid, "date"], sort=True)
    all_count = np.bincount(all_codes, minlength=len(all_dates)).astype(float)
    all_sum = np.bincount(all_codes, weights=all_robust, minlength=len(all_dates))
    baseline_map = dict(zip(pd.Index(all_dates), all_sum / all_count))
    baseline = np.array([baseline_map[d] for d in pd.Index(unique_dates)])
    wealth = np.cumprod(1.0 + daily)
    peaks = np.maximum.accumulate(wealth)
    mdd = np.min(wealth / peaks - 1.0)
    return {
        "n": int(len(values)), "dates": int(len(unique_dates)),
        "mean_bps": float(values.mean() * 10000),
        "median_bps": float(np.median(values) * 10000),
        "robust_mean_bps": float(robust.mean() * 10000),
        "winrate": float((values > 0).mean()),
        "profit_factor": float(pos / neg) if neg > 0 else None,
        "daily_robust_mean_bps": float(daily.mean() * 10000),
        "daily_alpha_bps": float((daily - baseline).mean() * 10000),
        "daily_mdd": float(mdd),
    }


def split(df: pd.DataFrame, name: str) -> pd.DataFrame:
    start, end = SPLITS[name]
    return df[df["date"].between(start, end)].copy()


def prefixed(row: dict[str, Any], prefix: str) -> dict[str, Any]:
    return {f"{prefix}_{k}": v for k, v in row.items()}


def safe_num(row: dict[str, Any], key: str, default: float = -1e12) -> float:
    value = row.get(key)
    return float(value) if value is not None and pd.notna(value) else default


def robust_candidate(row: dict[str, Any]) -> bool:
    return bool(
        safe_num(row, "train_n", 0) >= 120
        and safe_num(row, "train_dates", 0) >= 30
        and safe_num(row, "val_n", 0) >= 60
        and safe_num(row, "val_dates", 0) >= 20
        and safe_num(row, "val_2024_n", 0) >= 15
        and safe_num(row, "val_2024_dates", 0) >= 10
        and safe_num(row, "val_2025_n", 0) >= 15
        and safe_num(row, "val_2025_dates", 0) >= 10
        and all(safe_num(row, f"{s}_daily_robust_mean_bps") > 0 for s in ("train", "val_2024", "val_2025"))
        and all(safe_num(row, f"{s}_daily_alpha_bps") > 0 for s in ("train", "val_2024", "val_2025"))
        and all(safe_num(row, f"{s}_profit_factor") > 1.0 for s in ("train", "val_2024", "val_2025"))
    )


def score(row: dict[str, Any]) -> float:
    alphas = [safe_num(row, f"{s}_daily_alpha_bps") for s in ("train", "val_2024", "val_2025")]
    pfs = [max(safe_num(row, f"{s}_profit_factor", 0.01), 0.01) for s in ("train", "val_2024", "val_2025")]
    means = [safe_num(row, f"{s}_daily_robust_mean_bps") for s in ("train", "val_2024", "val_2025")]
    complexity = len(row["included"]) + int(row["require_macd"])
    return float(min(alphas) + 0.25 * np.mean(alphas) + 0.25 * min(means) + 8.0 * np.log(min(pfs)) - 1.5 * complexity)


def run_hpo(work: pd.DataFrame, stable: dict[str, Any]):
    configs = candidates(stable)
    summary = []
    grid_rows = []
    roles = []
    checks = {"oos_evaluated_after_train_val_ranking": True, "iterations_per_cell": N_ITER}
    for regime in REGIMES:
        regime_rows = work[work["market_regime"].eq(regime)]
        for horizon, strategy in STRATEGIES.items():
            target = f"strategy_ret_h{horizon}"
            cell = regime_rows[regime_rows[target].notna()].copy()
            parts = {name: split(cell, name) for name in ("train", "val", "val_2024", "val_2025")}
            rows = []
            for config in configs:
                row = dict(config)
                for name, part in parts.items():
                    row.update(prefixed(metrics(part, target, requirement_mask(part, config)), name))
                row["robust_train_val"] = robust_candidate(row)
                row["selection_score"] = score(row)
                rows.append(row)
            ranked = sorted(rows, key=lambda r: (r["robust_train_val"], r["selection_score"]), reverse=True)
            robust_rows = [r for r in ranked if r["robust_train_val"]]
            sample_rows = [
                r for r in ranked
                if safe_num(r, "train_n", 0) >= 120
                and safe_num(r, "val_n", 0) >= 60
                and safe_num(r, "val_2024_n", 0) >= 15
                and safe_num(r, "val_2025_n", 0) >= 15
            ]
            selection_status = "ROBUST_TRAIN_VAL_CANDIDATE" if robust_rows else "NO_ROBUST_HPO_CANDIDATE"
            pool = robust_rows if robust_rows else (sample_rows if sample_rows else ranked)
            top = pool[:20]
            oos = split(cell, "oos")
            for rank, row in enumerate(top, 1):
                config = next(c for c in configs if c["candidate_id"] == row["candidate_id"])
                row.update(prefixed(metrics(oos, target, requirement_mask(oos, config)), "oos"))
                row["rank"] = rank
                row["regime"] = regime
                row["strategy"] = strategy
                row["horizon"] = horizon
                row["selection_status"] = selection_status
                row["included_text"] = "|".join(row["included"])
                grid_rows.append(row)
            best = top[0]
            oos_ok = bool(
                selection_status == "ROBUST_TRAIN_VAL_CANDIDATE"
                and safe_num(best, "oos_n", 0) >= 15
                and safe_num(best, "oos_dates", 0) >= 10
                and safe_num(best, "oos_daily_robust_mean_bps") > 0
                and safe_num(best, "oos_daily_alpha_bps") > 0
                and safe_num(best, "oos_profit_factor") > 1.0
            )
            verdict = "OOS_PASS" if oos_ok else ("OOS_FAIL" if selection_status.startswith("ROBUST") else selection_status)
            summary.append({
                "regime": regime, "strategy": strategy, "horizon": horizon,
                "selection_status": selection_status, "oos_verdict": verdict,
                "candidate_id": best["candidate_id"], "included": best["included"],
                "require_macd": best["require_macd"],
                **{key: best[key] for key in RANGES},
                "selection_score": best["selection_score"],
                **{key: best.get(key) for key in best if key.startswith(("train_", "val_", "oos_"))},
                "policy_effect": False,
            })
            near = top[:10]
            for signal in SIGNAL_NAMES + ["macd_confirm"]:
                freq = np.mean([
                    bool(r["require_macd"]) if signal == "macd_confirm" else signal in r["included"]
                    for r in near
                ])
                role = (
                    "UNRESOLVED" if not robust_rows else
                    "REQUIRED_CANDIDATE" if freq >= 0.999 else
                    "OPTIONAL_CANDIDATE" if freq >= 0.30 else
                    "EXCLUDED_FROM_NEAR_OPTIMAL"
                )
                roles.append({"regime": regime, "strategy": strategy, "signal": signal, "frequency": float(freq), "role": role})
            print(f"[HPO] {regime}/{strategy} status={selection_status} id={best['candidate_id']} oos={verdict}")
    return summary, grid_rows, roles, checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(LOGS))
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("[HPO] loading integrity-clean history")
    work, meta, stable = prepare()
    summary, grid_rows, roles, checks = run_hpo(work, stable)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS", "scope": "read_only_regime_strategy_signal_hpo",
        "policy_effect": False, "order_path_effect": False,
        "iterations_per_cell": N_ITER, "seed": SEED,
        "oos_used_for_selection": False,
        "data": meta, "summaries": summary, "calculation_checks": checks,
        "limitations": [
            "This is a bounded 160-candidate research HPO per cell, not live promotion.",
            "BULL/BEAR and H1/H2/H5 use the same definitions as the preceding subset search.",
            "OOS_FAIL or NO_ROBUST_HPO_CANDIDATE cannot be promoted.",
        ],
    }
    base = "regime_strategy_signal_hpo"
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    for p in (out / f"{base}_{stamp}.json", out / f"{base}_latest.json"):
        p.write_text(text, encoding="utf-8")
    frames = {
        "summary": pd.DataFrame(summary),
        "top_candidates": pd.DataFrame(grid_rows),
        "condition_roles": pd.DataFrame(roles),
    }
    for name, frame in frames.items():
        dated = out / f"{base}_{name}_{stamp}.csv"
        serial = frame.copy()
        if "included" in serial.columns:
            serial["included"] = serial["included"].map(lambda x: "|".join(x) if isinstance(x, list) else x)
        serial.to_csv(dated, index=False, encoding="utf-8-sig")
        (out / f"{base}_{name}_latest.csv").write_bytes(dated.read_bytes())
    compact = frames["summary"][["regime", "strategy", "selection_status", "oos_verdict", "included", "require_macd", "train_n", "val_n", "oos_n", "train_profit_factor", "val_profit_factor", "oos_profit_factor"]].copy()
    compact["included"] = compact["included"].map(lambda x: ",".join(x))
    md = "# Regime Strategy Signal HPO\n\n" + markdown_table(compact) + "\n\nNo live policy was changed.\n"
    (out / f"{base}_{stamp}.md").write_text(md, encoding="utf-8")
    (out / f"{base}_latest.md").write_text(md, encoding="utf-8")
    print(f"[HPO] complete rows={len(work)} cells={len(summary)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
