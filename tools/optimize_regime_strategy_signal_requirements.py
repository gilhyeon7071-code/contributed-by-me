"""Read-only regime x holding-strategy x signal requirement optimization.

The selector is trained without OOS data. 2026 rows are opened only after the
TRAIN/VAL winner has been fixed for each regime and holding horizon.
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"
GENERATOR = ROOT / "generate_candidates_v41_1.py"
STABLE = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"

CORE_SIGNALS = [
    ("rs_pass", "rs"),
    ("v_accel_pass", "v_accel"),
    ("stretch_pass", "stretch"),
    ("value_pass", "value"),
    ("atr_pass", "atr14_pct"),
    ("rsi_pass", "rsi14"),
    ("volcorr_pass", "vol_close_corr20"),
    ("high52_pass", "high_52w_gap"),
    ("listing_pass", "listing_days"),
]
SIGNALS = [name for name, _ in CORE_SIGNALS]
STRATEGIES = {1: "H1_SHORT", 2: "H2_SWING", 5: "H5_POSITION"}
REGIMES = ("BULL", "BEAR", "SIDEWAYS", "TRANSITION", "STRESS")
N_BITS = len(SIGNALS)
N_MASKS = 1 << N_BITS
CORE9_MASK = (1 << len(CORE_SIGNALS)) - 1
# [2026-09-10] 라이브 설정에 맞춰 교정 (BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2(수수료) + 0.001*2(슬리피지) + 0.002(거래세) = 0.00400.
#   **과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
#   OFFICIAL_SLIPPAGE 를 진입·청산에 이미 곱하므로(L99~101) OFFICIAL_COST 에
#   남는 것은 **매도 거래세 0.002 뿐**이다. 왕복 = 0.001*2 + 0.002 = 0.00400.
OFFICIAL_COST = 0.002
OFFICIAL_SLIPPAGE = 0.001
ROBUST_RETURN_CAP = 0.30

SPLITS = {
    "train": (pd.Timestamp("2021-01-01"), pd.Timestamp("2023-12-31")),
    "val": (pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")),
    "val_2024": (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")),
    "val_2025": (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31")),
    "oos": (pd.Timestamp("2026-01-01"), pd.Timestamp("2262-04-11")),
}


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def add_signal_flags(df: pd.DataFrame, p: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    out["rs_pass"] = out["rs"] > float(p["rs_lim"])
    out["v_accel_pass"] = out["v_accel"] > float(p["v_accel_lim"])
    out["stretch_pass"] = out["stretch"] <= float(p["stretch_max"])
    out["value_pass"] = out["value"] >= float(p["value_min"])
    out["atr_pass"] = out["atr14_pct"] <= float(p["atr_max"])
    out["rsi_pass"] = out["rsi14"] <= float(p["rsi_max"])
    out["volcorr_pass"] = out["vol_close_corr20"] >= float(p["vol_close_corr_min"])
    out["high52_pass"] = out["high_52w_gap"] <= float(p["near_52w_high_gap_max"])
    out["listing_pass"] = out["listing_days"] >= float(p["min_listing_days"])
    mask = np.zeros(len(out), dtype=np.uint16)
    for bit, signal in enumerate(SIGNALS):
        mask |= out[signal].fillna(False).to_numpy(dtype=np.uint16) << bit
    out["signal_mask"] = mask
    return out


def add_strategy_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Use next-session open entry and exact h-session close exit."""
    out = df.sort_values(["price_history_key", "date"]).copy()
    grouped = out.groupby("price_history_key", sort=False)
    entry_open = grouped["open"].shift(-1)
    entry_idx = grouped["price_session_index"].shift(-1)
    for horizon in STRATEGIES:
        exit_close = grouped["close"].shift(-horizon)
        exit_idx = grouped["price_session_index"].shift(-horizon)
        valid = (
            entry_open.gt(0)
            & exit_close.gt(0)
            & entry_idx.sub(out["price_session_index"]).eq(1)
            & exit_idx.sub(out["price_session_index"]).eq(horizon)
        )
        gross = (exit_close * (1.0 - OFFICIAL_SLIPPAGE)) / (
            entry_open * (1.0 + OFFICIAL_SLIPPAGE)
        ) - 1.0
        out[f"strategy_ret_h{horizon}"] = gross.where(valid) - OFFICIAL_COST
    return out


def assign_research_regime(factors: pd.DataFrame) -> pd.Series:
    """Research-only ex-ante regime from trend, volatility, and breadth."""
    d = factors.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.normalize()
    idx = d.groupby("date")["close"].mean().sort_index()
    ret20 = idx.pct_change(20)
    ma60 = idx.rolling(60, min_periods=60).mean()
    vol20 = idx.pct_change().rolling(20, min_periods=20).std()
    def _safe_breadth(g: pd.DataFrame) -> float:
        close = pd.to_numeric(g["close"], errors="coerce")
        ma = pd.to_numeric(g["ma60"], errors="coerce")
        valid = close.notna() & ma.notna()
        if not bool(valid.any()):
            return float("nan")
        return float((close[valid] > ma[valid]).mean())

    breadth = d.groupby("date").apply(_safe_breadth, include_groups=False)
    out = pd.DataFrame({"ret20": ret20, "ma60": ma60, "vol20": vol20, "breadth": breadth})
    out["above_ma60"] = idx > out["ma60"]
    regime = np.full(len(out), "TRANSITION", dtype=object)
    stress = (out["ret20"] <= -0.10) | ((out["vol20"] >= 0.04) & (out["ret20"] < 0))
    bull = (~stress) & out["above_ma60"] & (out["ret20"] >= 0.05) & (out["breadth"] >= 0.55)
    bear = (~stress) & (~out["above_ma60"]) & (out["ret20"] <= -0.02) & (out["breadth"] <= 0.45)
    sideways = (~stress) & (out["ret20"].abs() <= 0.05) & out["breadth"].between(0.40, 0.60, inclusive="both")
    regime[stress.fillna(False).to_numpy()] = "STRESS"
    regime[bull.fillna(False).to_numpy()] = "BULL"
    regime[bear.fillna(False).to_numpy()] = "BEAR"
    regime[sideways.fillna(False).to_numpy()] = "SIDEWAYS"
    return pd.Series(regime, index=out.index, name="market_regime_research")

def prepare() -> tuple[pd.DataFrame, dict[str, Any]]:
    module = load_module(GENERATOR, "regime_strategy_signal_optimizer_generator")
    raw = module._load_data()
    integrity = dict(getattr(raw, "attrs", {}).get("price_history_integrity", {}) or {})
    raw_rows = len(raw)
    factors, latest, _ = module._compute_factors(raw)
    del raw
    gc.collect()
    stable = json.loads(STABLE.read_text(encoding="utf-8-sig"))
    params = module._normalize_params(stable)
    factors = add_signal_flags(factors, params)
    factors = add_strategy_returns(factors)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    regime_map = assign_research_regime(factors)
    factors["market_regime"] = factors["date"].map(regime_map)

    required_factors = [factor for _, factor in CORE_SIGNALS]
    common = factors[required_factors].notna().all(axis=1)
    common &= factors["market_regime"].isin(REGIMES)
    common &= factors["date"].ge(SPLITS["train"][0])
    keep = [
        "date", "code", "market", "market_regime", "signal_mask",
        *[f"strategy_ret_h{h}" for h in STRATEGIES],
    ]
    work = factors.loc[common, keep].copy()
    del factors
    gc.collect()
    meta = {
        "raw_rows": int(raw_rows),
        "analysis_rows": int(len(work)),
        "date_min": str(work["date"].min().date()),
        "date_max": str(work["date"].max().date()),
        "latest_factor_date": str(pd.Timestamp(latest).date()),
        "codes": int(work["code"].nunique()),
        "dates": int(work["date"].nunique()),
        "integrity": integrity,
        "params": {name: params[name] for name in (
            "rs_lim", "v_accel_lim", "stretch_max", "value_min", "atr_max",
            "rsi_max", "vol_close_corr_min", "near_52w_high_gap_max", "min_listing_days",
        )},
    }
    return work, meta


def superset_zeta(values: np.ndarray) -> np.ndarray:
    out = values.copy()
    indexes = np.arange(N_MASKS)
    for bit in range(N_BITS):
        flag = 1 << bit
        low = indexes[(indexes & flag) == 0]
        out[..., low] += out[..., low | flag]
    return out


def metric_frame(df: pd.DataFrame, target: str) -> pd.DataFrame:
    columns = [
        "n", "dates", "mean_bps", "robust_mean_bps", "winrate", "profit_factor",
        "daily_mean_bps", "daily_robust_mean_bps", "daily_alpha_bps", "daily_mdd",
    ]
    if df.empty:
        return pd.DataFrame(
            {c: np.full(N_MASKS, np.nan) for c in columns},
            index=np.arange(N_MASKS),
        ).assign(n=0, dates=0)

    values = pd.to_numeric(df[target], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(values)
    values = values[valid]
    masks = df.loc[valid, "signal_mask"].to_numpy(dtype=np.int64)
    dates = df.loc[valid, "date"]
    robust = np.clip(values, -ROBUST_RETURN_CAP, ROBUST_RETURN_CAP)

    exact_n = np.bincount(masks, minlength=N_MASKS).astype(float)
    exact_sum = np.bincount(masks, weights=values, minlength=N_MASKS)
    exact_robust = np.bincount(masks, weights=robust, minlength=N_MASKS)
    exact_wins = np.bincount(masks, weights=(values > 0).astype(float), minlength=N_MASKS)
    exact_pos = np.bincount(masks, weights=np.where(values > 0, values, 0.0), minlength=N_MASKS)
    exact_neg = np.bincount(masks, weights=np.where(values < 0, -values, 0.0), minlength=N_MASKS)
    n = superset_zeta(exact_n)
    sums = superset_zeta(exact_sum)
    robust_sums = superset_zeta(exact_robust)
    wins = superset_zeta(exact_wins)
    pos = superset_zeta(exact_pos)
    neg = superset_zeta(exact_neg)

    date_code, unique_dates = pd.factorize(dates, sort=True)
    daily_n = np.zeros((len(unique_dates), N_MASKS), dtype=float)
    daily_sum = np.zeros_like(daily_n)
    daily_robust = np.zeros_like(daily_n)
    np.add.at(daily_n, (date_code, masks), 1.0)
    np.add.at(daily_sum, (date_code, masks), values)
    np.add.at(daily_robust, (date_code, masks), robust)
    daily_n = superset_zeta(daily_n)
    daily_sum = superset_zeta(daily_sum)
    daily_robust = superset_zeta(daily_robust)
    with np.errstate(divide="ignore", invalid="ignore"):
        daily_ret = np.divide(
            daily_sum, daily_n, out=np.full_like(daily_sum, np.nan), where=daily_n > 0
        )
        daily_robust_ret = np.divide(
            daily_robust, daily_n, out=np.full_like(daily_robust, np.nan), where=daily_n > 0
        )
    selected_days = (daily_n > 0).sum(axis=0)
    with warnings_suppressed():
        daily_mean = np.nanmean(daily_ret, axis=0)
        daily_robust_mean = np.nanmean(daily_robust_ret, axis=0)
        baseline = daily_robust_ret[:, [0]]
        daily_alpha = np.nanmean(daily_robust_ret - baseline, axis=0)
    held = np.nan_to_num(daily_robust_ret, nan=0.0)
    wealth = np.cumprod(1.0 + held, axis=0)
    peaks = np.maximum.accumulate(wealth, axis=0)
    drawdown = np.divide(
        wealth, peaks, out=np.ones_like(wealth), where=peaks > 0
    ) - 1.0
    mdd = drawdown.min(axis=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        return pd.DataFrame({
            "n": n.astype(int),
            "dates": selected_days.astype(int),
            "mean_bps": np.divide(sums, n, out=np.full(N_MASKS, np.nan), where=n > 0) * 10000,
            "robust_mean_bps": np.divide(robust_sums, n, out=np.full(N_MASKS, np.nan), where=n > 0) * 10000,
            "winrate": np.divide(wins, n, out=np.full(N_MASKS, np.nan), where=n > 0),
            "profit_factor": np.divide(pos, neg, out=np.full(N_MASKS, np.nan), where=neg > 0),
            "daily_mean_bps": daily_mean * 10000,
            "daily_robust_mean_bps": daily_robust_mean * 10000,
            "daily_alpha_bps": daily_alpha * 10000,
            "daily_mdd": mdd,
        }, index=np.arange(N_MASKS))


class warnings_suppressed:
    def __enter__(self):
        import warnings
        self._ctx = warnings.catch_warnings()
        self._ctx.__enter__()
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._ctx.__exit__(exc_type, exc, tb)


def prefixed(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    return frame.rename(columns={c: f"{prefix}_{c}" for c in frame.columns})


def signal_names(combo_id: int) -> list[str]:
    return [name for bit, name in enumerate(SIGNALS) if combo_id & (1 << bit)]


def direct_metrics(df: pd.DataFrame, target: str, combo_id: int) -> dict[str, Any]:
    selected = df[(df["signal_mask"].astype(int) & combo_id) == combo_id]
    values = pd.to_numeric(selected[target], errors="coerce").dropna()
    pos = values[values > 0].sum()
    neg = -values[values < 0].sum()
    return {
        "n": int(len(values)),
        "mean_bps": float(values.mean() * 10000) if len(values) else None,
        "median_bps": float(values.median() * 10000) if len(values) else None,
        "profit_factor": float(pos / neg) if neg > 0 else None,
    }


def split_rows(df: pd.DataFrame, name: str) -> pd.DataFrame:
    start, end = SPLITS[name]
    return df[df["date"].between(start, end)]


def selection_status(grid: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    eligible = (
        grid["combo_id"].gt(0)
        & grid["train_n"].ge(120) & grid["train_dates"].ge(30)
        & grid["val_n"].ge(60) & grid["val_dates"].ge(20)
        & grid["val_2024_n"].ge(15) & grid["val_2024_dates"].ge(10)
        & grid["val_2025_n"].ge(15) & grid["val_2025_dates"].ge(10)
        & grid["train_daily_robust_mean_bps"].gt(0)
        & grid["val_daily_robust_mean_bps"].gt(0)
        & grid["val_2024_daily_robust_mean_bps"].gt(0)
        & grid["val_2025_daily_robust_mean_bps"].gt(0)
        & grid["train_daily_alpha_bps"].gt(0)
        & grid["val_daily_alpha_bps"].gt(0)
        & grid["val_2024_daily_alpha_bps"].gt(0)
        & grid["val_2025_daily_alpha_bps"].gt(0)
        & grid["train_profit_factor"].gt(1.0)
        & grid["val_profit_factor"].gt(1.0)
        & grid["val_2024_profit_factor"].gt(1.0)
        & grid["val_2025_profit_factor"].gt(1.0)
    )
    robust = grid.loc[eligible].copy()
    if not robust.empty:
        return robust, "ROBUST_TRAIN_VAL_CANDIDATE"
    fallback = grid[
        grid["combo_id"].gt(0)
        & grid["train_n"].ge(120)
        & grid["val_n"].ge(60)
        & grid["val_2024_n"].ge(15)
        & grid["val_2025_n"].ge(15)
    ].copy()
    return fallback, "NO_ROBUST_COMBINATION"


def optimize(work: pd.DataFrame):
    summaries = []
    top_rows = []
    role_rows = []
    all_grids = []
    checks = {"oos_excluded_from_selection_score": True, "direct_recalculation": []}

    bit_counts = np.array([int(i).bit_count() for i in range(N_MASKS)])
    for regime in REGIMES:
        regime_rows = work[work["market_regime"].eq(regime)]
        for horizon, strategy in STRATEGIES.items():
            target = f"strategy_ret_h{horizon}"
            cell = regime_rows[regime_rows[target].notna()].copy()
            frames = {
                name: metric_frame(split_rows(cell, name), target)
                for name in SPLITS
            }
            grid = pd.concat(
                [prefixed(frames[name], name) for name in SPLITS], axis=1
            )
            grid["combo_id"] = np.arange(N_MASKS)
            grid["complexity"] = bit_counts
            stability_cols = [
                "train_daily_alpha_bps", "val_daily_alpha_bps",
                "val_2024_daily_alpha_bps", "val_2025_daily_alpha_bps",
            ]
            grid["worst_selection_alpha_bps"] = grid[stability_cols].min(axis=1)
            min_pf = grid[[
                "train_profit_factor", "val_profit_factor",
                "val_2024_profit_factor", "val_2025_profit_factor",
            ]].min(axis=1).clip(lower=0.01)
            grid["selection_score"] = (
                grid["worst_selection_alpha_bps"]
                + 0.25 * grid["val_daily_alpha_bps"]
                + 8.0 * np.log(min_pf)
                - 1.5 * grid["complexity"]
            )
            pool, status = selection_status(grid)
            if pool.empty:
                pool = grid[grid["combo_id"].gt(0)].copy()
                status = "INSUFFICIENT_SAMPLE"
            ranked = pool.sort_values(
                ["selection_score", "complexity", "val_n"],
                ascending=[False, True, False],
            )
            top = ranked.head(20).copy()
            best = top.iloc[0]
            combo_id = int(best["combo_id"])
            best_signals = signal_names(combo_id)

            oos_ok = bool(
                status == "ROBUST_TRAIN_VAL_CANDIDATE"
                and best["oos_n"] >= 15 and best["oos_dates"] >= 10
                and best["oos_daily_robust_mean_bps"] > 0
                and best["oos_daily_alpha_bps"] > 0
                and best["oos_profit_factor"] > 1.0
            )
            verdict = (
                "OOS_PASS" if oos_ok
                else ("OOS_FAIL" if status == "ROBUST_TRAIN_VAL_CANDIDATE" else status)
            )

            direct = {}
            for split_name in ("train", "val", "oos"):
                direct[split_name] = direct_metrics(
                    split_rows(cell, split_name), target, combo_id
                )
                aggregate_n = int(best[f"{split_name}_n"])
                aggregate_mean = float(best[f"{split_name}_mean_bps"])
                if (
                    direct[split_name]["n"] != aggregate_n
                    or not math.isclose(
                        direct[split_name]["mean_bps"], aggregate_mean,
                        rel_tol=1e-10, abs_tol=1e-8,
                    )
                ):
                    raise AssertionError(
                        f"direct metric mismatch {regime}/{strategy}/{split_name}"
                    )
            checks["direct_recalculation"].append({
                "regime": regime, "strategy": strategy,
                "combo_id": combo_id, "status": "PASS",
            })

            near = top.head(min(10, len(top)))
            for bit, signal in enumerate(SIGNALS):
                frequency = float(
                    ((near["combo_id"].astype(int) & (1 << bit)) > 0).mean()
                )
                if status != "ROBUST_TRAIN_VAL_CANDIDATE":
                    role = "UNRESOLVED"
                elif frequency >= 0.999:
                    role = "REQUIRED_CANDIDATE"
                elif frequency >= 0.30:
                    role = "OPTIONAL_CANDIDATE"
                else:
                    role = "EXCLUDED_FROM_NEAR_OPTIMAL"
                role_rows.append({
                    "regime": regime, "strategy": strategy, "horizon": horizon,
                    "signal": signal, "near_optimal_frequency": frequency,
                    "role": role, "selection_status": status,
                    "policy_effect": False,
                })

            baseline = grid.loc[grid["combo_id"].eq(0)].iloc[0]
            core9 = grid.loc[grid["combo_id"].eq(CORE9_MASK)].iloc[0]
            summaries.append({
                "regime": regime, "strategy": strategy, "horizon": horizon,
                "selection_status": status, "oos_verdict": verdict,
                "best_combo_id": combo_id, "best_conditions": best_signals,
                "best_core_conditions": [
                    s for s in best_signals
                ],
                "best_macd_required": False,
                "selection_score": float(best["selection_score"]),
                "train_n": int(best["train_n"]),
                "train_pf": float(best["train_profit_factor"]),
                "train_daily_mean_bps": float(
                    best["train_daily_robust_mean_bps"]
                ),
                "train_daily_alpha_bps": float(best["train_daily_alpha_bps"]),
                "val_n": int(best["val_n"]),
                "val_pf": float(best["val_profit_factor"]),
                "val_daily_mean_bps": float(best["val_daily_robust_mean_bps"]),
                "val_daily_alpha_bps": float(best["val_daily_alpha_bps"]),
                "oos_n": int(best["oos_n"]),
                "oos_pf": float(best["oos_profit_factor"]),
                "oos_daily_mean_bps": float(best["oos_daily_robust_mean_bps"]),
                "oos_daily_alpha_bps": float(best["oos_daily_alpha_bps"]),
                "oos_mdd": float(best["oos_daily_mdd"]),
                "train_median_bps": direct["train"]["median_bps"],
                "val_median_bps": direct["val"]["median_bps"],
                "oos_median_bps": direct["oos"]["median_bps"],
                "baseline_val_daily_mean_bps": float(
                    baseline["val_daily_robust_mean_bps"]
                ),
                "core9_val_n": int(core9["val_n"]),
                "core9_val_pf": (
                    float(core9["val_profit_factor"])
                    if pd.notna(core9["val_profit_factor"]) else None
                ),
                "macd_confirmation_evaluated": False,
                "policy_effect": False,
            })
            for rank, row in enumerate(top.itertuples(), 1):
                record = row._asdict()
                record.update({
                    "regime": regime, "strategy": strategy,
                    "horizon": horizon, "rank": rank,
                    "conditions": "|".join(signal_names(int(row.combo_id))),
                    "selection_status": status,
                })
                top_rows.append(record)
            grid.insert(0, "regime", regime)
            grid.insert(1, "strategy", strategy)
            grid.insert(2, "horizon", horizon)
            all_grids.append(grid)
            print(
                f"[OPT] {regime}/{strategy} status={status} "
                f"combo={combo_id} oos={verdict}"
            )

    return (
        summaries, top_rows, role_rows,
        pd.concat(all_grids, ignore_index=True), checks,
    )


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no rows)"
    clean = df.copy().where(pd.notna(df), "")
    cols = [str(c) for c in clean.columns]
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for row in clean.astype(str).itertuples(index=False, name=None):
        lines.append(
            "| " + " | ".join(str(v).replace("|", "/") for v in row) + " |"
        )
    return "\n".join(lines)


def write_markdown(
    path: Path, summary: pd.DataFrame, roles: pd.DataFrame
) -> None:
    compact = summary[[
        "regime", "strategy", "selection_status", "oos_verdict",
        "best_conditions", "train_n", "val_n", "oos_n",
        "val_pf", "oos_pf", "val_daily_mean_bps", "oos_daily_mean_bps",
    ]].copy()
    compact["best_conditions"] = compact["best_conditions"].map(
        lambda x: ",".join(x) if isinstance(x, list) else str(x)
    )
    role_compact = roles[
        roles["role"].isin(["REQUIRED_CANDIDATE", "OPTIONAL_CANDIDATE"])
    ].copy()
    lines = [
        "# Regime Strategy Signal Requirement Optimization",
        "",
        "## Decision summary",
        "",
        "Existing integrity-clean history is searched for best-fit condition subsets. OOS data is not used for selection.",
        "",
        markdown_table(compact),
        "",
        "## Required and optional candidates",
        "",
        markdown_table(role_compact),
        "",
        "## Method boundary",
        "",
        f"- TRAIN: {SPLITS['train'][0].date()} to {SPLITS['train'][1].date()}",
        f"- VAL: {SPLITS['val'][0].date()} to {SPLITS['val'][1].date()}",
        "- OOS: 2026-01-01 onward; never included in selection_score.",
        f"- Net return: next-session open entry, exact h-session close exit, cost={OFFICIAL_COST:.4f}, entry/exit slippage={OFFICIAL_SLIPPAGE:.4f} each.",
        f"- Robust selection caps individual net returns at +/-{ROBUST_RETURN_CAP:.0%}; raw PF and means are retained.",
        "- This output proposes requirement candidates only and does not alter live policy.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(LOGS))
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("[OPT] loading integrity-clean history")
    work, meta = prepare()
    summaries, top_rows, role_rows, grid, checks = optimize(work)
    summary_df = pd.DataFrame(summaries)
    top_df = pd.DataFrame(top_rows)
    role_df = pd.DataFrame(role_rows)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "scope": "read_only_regime_strategy_signal_requirement_optimization",
        "policy_effect": False,
        "order_path_effect": False,
        "definitions": {
            "regimes": list(REGIMES),
            "strategies": STRATEGIES,
            "core_signals": [name for name, _ in CORE_SIGNALS],
            "macd": "not included in the 9-condition combination search",
        },
        "splits": {
            k: [str(v[0].date()), str(v[1].date())]
            for k, v in SPLITS.items()
        },
        "cost_model": {
            "cost": OFFICIAL_COST,
            "entry_slippage": OFFICIAL_SLIPPAGE,
            "exit_slippage": OFFICIAL_SLIPPAGE,
        },
        "selection_contract": {
            "oos_used_for_selection": False,
            "full_combination_count_per_cell": N_MASKS - 1,
            "robust_return_cap": ROBUST_RETURN_CAP,
            "minimum_samples": {"train": 120, "val": 60, "val_year": 15},
        },
        "data": meta,
        "calculation_checks": checks,
        "summaries": summaries,
        "limitations": [
            "Regimes are research-only: aggregate trend, volatility, and breadth computed from information available on each signal date.",
            "H1/H2/H5 are holding-strategy proxies, not normal/split/surge execution classes.",
            "Combination search is observational and can still suffer multiple-testing selection bias.",
            "OOS_FAIL or NO_ROBUST_COMBINATION must not be promoted to live policy.",
        ],
    }
    base = "regime_strategy_signal_optimization"
    json_text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    for path in (
        out / f"{base}_{stamp}.json",
        out / f"{base}_latest.json",
    ):
        path.write_text(json_text, encoding="utf-8")
    outputs = {
        "summary": summary_df,
        "top_combinations": top_df,
        "condition_roles": role_df,
        "combination_grid": grid,
    }
    for name, frame in outputs.items():
        dated = out / f"{base}_{name}_{stamp}.csv"
        frame.to_csv(dated, index=False, encoding="utf-8-sig")
        (out / f"{base}_{name}_latest.csv").write_bytes(dated.read_bytes())
    dated_md = out / f"{base}_{stamp}.md"
    write_markdown(dated_md, summary_df, role_df)
    (out / f"{base}_latest.md").write_bytes(dated_md.read_bytes())
    print(
        f"[OPT] complete rows={len(work)} cells={len(summaries)} "
        f"grid_rows={len(grid)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())