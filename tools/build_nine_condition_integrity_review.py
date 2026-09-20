from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if not (ROOT / "generate_candidates_v41_1.py").exists():
    ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"
RISK = ROOT / "12_Risk_Controlled"
PRE_GENERATOR = ROOT / "backup" / "20260710_price_history_integrity_contract" / "20260710_131500" / "generate_candidates_v41_1.py"
CURRENT_GENERATOR = ROOT / "generate_candidates_v41_1.py"

CONDITIONS = {
    "rs_pass": "rs",
    "v_accel_pass": "v_accel",
    "stretch_pass": "stretch",
    "value_pass": "value",
    "atr_pass": "atr14_pct",
    "rsi_pass": "rsi14",
    "volcorr_pass": "vol_close_corr20",
    "high52_pass": "high_52w_gap",
    "listing_pass": "listing_days",
}
ROLES = {
    "momentum_breakout": ["rs_pass", "stretch_pass", "high52_pass"],
    "participation_liquidity": ["v_accel_pass", "value_pass", "volcorr_pass"],
    "risk_maturity": ["atr_pass", "rsi_pass", "listing_pass"],
}


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def add_passes(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    out = df.copy()
    out["rs_pass"] = out["rs"] > float(p["rs_lim"])
    out["v_accel_pass"] = out["v_accel"] > float(p["v_accel_lim"])
    out["stretch_pass"] = out["stretch"] < float(p["stretch_max"])
    out["value_pass"] = out["value"] > float(p["value_min"])
    out["atr_pass"] = out["atr14_pct"] < float(p["atr_max"])
    out["rsi_pass"] = out["rsi14"] < float(p["rsi_max"])
    out["volcorr_pass"] = out["vol_close_corr20"] >= float(p["vol_close_corr_min"])
    out["high52_pass"] = out["high_52w_gap"] <= float(p["near_52w_high_gap_max"])
    out["listing_pass"] = out["listing_days"] >= float(p["min_listing_days"])
    out["macd_confirm"] = out["macd_golden"].fillna(False).astype(bool)
    for role, cols in ROLES.items():
        out[f"role_{role}_pass"] = out[cols].fillna(False).astype(bool).all(axis=1)
    core = list(CONDITIONS)
    out["core9_pass_count"] = out[core].fillna(False).astype(bool).sum(axis=1)
    out["core9_pass"] = out["core9_pass_count"].eq(9)
    out["core9_plus_macd_pass"] = out["core9_pass"] & out["macd_confirm"]
    return out


def prepare(module, label: str, params: dict, start: pd.Timestamp, *, max_gap_sessions: int = 0):
    if label == "post_contract":
        raw = module._load_data(max_gap_sessions=max_gap_sessions)
    else:
        raw = module._load_data()
    meta = {
        "raw_rows": int(len(raw)),
        "integrity": dict(raw.attrs.get("price_history_integrity", {})),
    }
    calendar = pd.Index(sorted(pd.to_datetime(raw["date"], errors="coerce").dropna().dt.normalize().unique()))
    factors, latest, _ = module._compute_factors(raw)
    del raw
    gc.collect()
    recent = factors[pd.to_datetime(factors["date"]) >= start].copy()
    del factors
    gc.collect()
    recent["date"] = pd.to_datetime(recent["date"]).dt.normalize()
    recent = recent.sort_values(["code", "date"]).reset_index(drop=True)
    if label == "pre_contract":
        g = recent.groupby("code", sort=False)
        session_map = pd.Series(np.arange(len(calendar)), index=calendar)
        date_map = pd.Series(calendar.to_numpy(), index=np.arange(len(calendar)))
        recent["price_session_index"] = recent["date"].map(session_map)
        for h in (1, 2, 5):
            recent[f"fwd_ret_{h}d"] = g["close"].shift(-h) / recent["close"] - 1.0
            recent[f"fwd_date_{h}d"] = g["date"].shift(-h)
            expected = (recent["price_session_index"] + h).map(date_map)
            recent[f"session_mismatch_{h}d"] = recent[f"fwd_date_{h}d"].notna() & expected.notna() & recent[f"fwd_date_{h}d"].ne(expected)
    else:
        from utils.price_history_contract import add_trade_count_forward_returns

        recent = add_trade_count_forward_returns(recent, (1, 2, 5))
        for h in (1, 2, 5):
            # True when the forward fill crossed a soft-bridged gap (elapsed
            # calendar sessions > h) rather than landing exactly h sessions later.
            recent[f"session_mismatch_{h}d"] = recent[f"fwd_gap_sessions_{h}d"].notna() & recent[f"fwd_gap_sessions_{h}d"].gt(h)
    meta.update({"latest_date": str(pd.to_datetime(latest).date()), "calendar_sessions": int(len(calendar))})
    return add_passes(recent, params), meta


def common_dates(pre: pd.DataFrame, post: pd.DataFrame, min_universe: int):
    a = pre.groupby("date")["code"].nunique()
    b = post.groupby("date")["code"].nunique()
    return sorted(set(a[a >= min_universe].index).intersection(b[b >= min_universe].index))


def daily_alpha(x: pd.DataFrame, flag: str, target: str) -> dict:
    vals = []
    for _, g in x.groupby("date", sort=True):
        yes = g.loc[g[flag].fillna(False), target].dropna()
        no = g.loc[~g[flag].fillna(False), target].dropna()
        if len(yes) and len(no):
            vals.append(float(yes.mean() - no.mean()))
    if not vals:
        return {"daily_alpha_days": 0, "daily_alpha_mean_bps": None, "daily_alpha_median_bps": None, "daily_alpha_positive_ratio": None, "daily_alpha_ci95_low_bps": None, "daily_alpha_ci95_high_bps": None}
    arr = np.asarray(vals)
    mean = float(arr.mean())
    se = float(arr.std(ddof=1) / math.sqrt(len(arr))) if len(arr) > 1 else 0.0
    return {
        "daily_alpha_days": len(arr),
        "daily_alpha_mean_bps": mean * 10000,
        "daily_alpha_median_bps": float(np.median(arr)) * 10000,
        "daily_alpha_positive_ratio": float((arr > 0).mean()),
        "daily_alpha_ci95_low_bps": (mean - 1.96 * se) * 10000,
        "daily_alpha_ci95_high_bps": (mean + 1.96 * se) * 10000,
    }


def perf(df: pd.DataFrame, dimension: str, item: str, flag: str, h: int, regime: str, factor: str | None = None):
    target = f"fwd_ret_{h}d"
    mask = df[target].notna()
    if factor:
        mask &= df[factor].notna()
    if regime != "ALL":
        mask &= df["market_regime"].astype(str).eq(regime)
    x = df.loc[mask, ["date", target, flag]].copy()
    yes_mask = x[flag].fillna(False).astype(bool)
    yes = x.loc[yes_mask, target]
    no = x.loc[~yes_mask, target]
    row = {
        "dimension": dimension,
        "item": item,
        "horizon": h,
        "regime": regime,
        "n": len(x),
        "pass_n": len(yes),
        "fail_n": len(no),
        "pass_rate": float(yes_mask.mean()) if len(x) else None,
        "pass_mean_bps": float(yes.mean() * 10000) if len(yes) else None,
        "fail_mean_bps": float(no.mean() * 10000) if len(no) else None,
        "pass_minus_fail_bps": float((yes.mean() - no.mean()) * 10000) if len(yes) and len(no) else None,
        "pass_winrate": float((yes > 0).mean()) if len(yes) else None,
        "fail_winrate": float((no > 0).mean()) if len(no) else None,
    }
    row.update(daily_alpha(x, flag, target))
    return row


def coverage(pre: pd.DataFrame, post: pd.DataFrame):
    rows = []
    for h in (1, 2, 5):
        target = f"fwd_ret_{h}d"
        for label, df in (("pre_contract", pre), ("post_contract", post)):
            v = pd.to_numeric(df[target], errors="coerce").dropna()
            rows.append({
                "dataset": label,
                "horizon": h,
                "rows": len(df),
                "target_rows": len(v),
                "target_coverage": float(len(v) / len(df)) if len(df) else 0.0,
                "abs_return_gt_30pct_rows": int(v.abs().gt(0.30).sum()),
                "abs_return_gt_100pct_rows": int(v.abs().gt(1.00).sum()),
                "max_abs_return": float(v.abs().max()) if len(v) else None,
                "shifted_target_session_mismatch_rows": int(df.loc[df[target].notna(), f"session_mismatch_{h}d"].sum()),
            })
    return rows


def intersections(df: pd.DataFrame, label: str):
    rows = []
    for h in (1, 2, 5):
        target = f"fwd_ret_{h}d"
        valid = df[target].notna()
        universe = df.loc[valid, target]
        for name, flag in (("core9", "core9_pass"), ("core9_plus_macd", "core9_plus_macd_pass")):
            selected = df.loc[valid & df[flag], target]
            rows.append({
                "dataset": label,
                "horizon": h,
                "intersection": name,
                "universe_n": len(universe),
                "selected_n": len(selected),
                "selected_dates": int(df.loc[valid & df[flag], "date"].nunique()),
                "avg_selected_per_date": float(len(selected) / max(1, df.loc[valid, "date"].nunique())),
                "selected_mean_bps": float(selected.mean() * 10000) if len(selected) else None,
                "universe_mean_bps": float(universe.mean() * 10000) if len(universe) else None,
                "alpha_bps": float((selected.mean() - universe.mean()) * 10000) if len(selected) else None,
                "selected_winrate": float((selected > 0).mean()) if len(selected) else None,
            })
    return rows


def classify(signal: pd.DataFrame):
    rows = []
    overall = signal[signal["regime"].eq("ALL")]
    segmented = signal[signal["regime"].isin(["BULL", "BEAR"])]
    for condition, factor in CONDITIONS.items():
        x = overall[overall["item"].eq(condition)].sort_values("horizon")
        adequate = bool(len(x) == 3 and x["pass_n"].ge(200).all() and x["fail_n"].ge(200).all() and x["daily_alpha_days"].ge(20).all())
        alpha_map = {int(r.horizon): float(r.daily_alpha_mean_bps) for r in x.itertuples() if pd.notna(r.daily_alpha_mean_bps)}
        positive = sum(v > 0 for v in alpha_map.values())
        negative = sum(v < 0 for v in alpha_map.values())
        supported_positive = int(((x["daily_alpha_ci95_low_bps"] > 0) & x["daily_alpha_ci95_low_bps"].notna()).sum())
        supported_negative = int(((x["daily_alpha_ci95_high_bps"] < 0) & x["daily_alpha_ci95_high_bps"].notna()).sum())
        regime_conflicts = 0
        sx = segmented[segmented["item"].eq(condition)]
        for h in (1, 2, 5):
            hx = sx[sx["horizon"].eq(h)].set_index("regime")["daily_alpha_mean_bps"]
            if "BULL" in hx.index and "BEAR" in hx.index and pd.notna(hx["BULL"]) and pd.notna(hx["BEAR"]):
                regime_conflicts += int(float(hx["BULL"]) * float(hx["BEAR"]) < 0)
        if not adequate:
            status = "NOT_DECISION_READY"
            recommendation = "keep_policy_no_change_insufficient_contrast"
        elif supported_negative >= 2 and supported_positive == 0:
            status = "SUPPORTED_NEGATIVE_REVIEW"
            recommendation = "review_condition_role_no_auto_remove"
        elif supported_positive >= 2 and supported_negative == 0:
            status = "SUPPORTED_POSITIVE_OBSERVE"
            recommendation = "required_candidate_observe_only"
        elif supported_positive == 1 and supported_negative == 0:
            status = "PARTIAL_POSITIVE_OBSERVE"
            recommendation = "selective_observe_only"
        elif supported_negative == 1 and supported_positive == 0:
            status = "PARTIAL_NEGATIVE_REVIEW"
            recommendation = "review_with_more_samples"
        else:
            status = "DIRECTION_ONLY_NOT_DECISION_READY"
            recommendation = "keep_policy_no_change_more_samples"
        rows.append({
            "condition": condition,
            "factor": factor,
            "status": status,
            "recommendation": recommendation,
            "adequate_contrast": adequate,
            "positive_horizons": positive,
            "negative_horizons": negative,
            "supported_positive_horizons": supported_positive,
            "supported_negative_horizons": supported_negative,
            "regime_sign_conflicts": regime_conflicts,
            "h1_daily_alpha_bps": alpha_map.get(1),
            "h2_daily_alpha_bps": alpha_map.get(2),
            "h5_daily_alpha_bps": alpha_map.get(5),
            "policy_effect": False,
        })
    return rows

def leave_one_out(post: pd.DataFrame):
    rows = []
    core = list(CONDITIONS)
    for h in (1, 2, 5):
        x = post[post[f"fwd_ret_{h}d"].notna()]
        full = x[core].fillna(False).astype(bool).all(axis=1)
        for condition in core:
            other = x[[c for c in core if c != condition]].fillna(False).astype(bool).all(axis=1)
            rows.append({"horizon": h, "condition": condition, "core9_n": int(full.sum()), "other8_n": int(other.sum()), "incremental_blocked_rows": int((other & ~x[condition]).sum())})
    return rows


def pass_counts(post: pd.DataFrame):
    rows = []
    for h in (1, 2, 5):
        target = f"fwd_ret_{h}d"
        for count, g in post[post[target].notna()].groupby("core9_pass_count"):
            rows.append({"horizon": h, "pass_count": int(count), "n": len(g), "mean_bps": float(g[target].mean() * 10000), "winrate": float((g[target] > 0).mean())})
    return rows


def write_csv(path: Path, rows) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no rows)"
    clean = df.copy().where(pd.notna(df), "")
    columns = [str(c) for c in clean.columns]
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for row in clean.astype(str).itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(v).replace("|", "/" ) for v in row) + " |")
    return "\n".join(lines)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument(
        "--max-gap-sessions",
        type=int,
        default=2,
        help=(
            "Tolerate up to N consecutive missing sessions (short trading halts) "
            "without breaking a price-history segment in the post-contract dataset. "
            "Default 2 is based on the observed KRX gap-length distribution, where "
            "1-2 session gaps dominate and gaps of 3-4 sessions are rare. This only "
            "affects this analysis script's own reload of the current generator's "
            "_load_data(); live candidate generation always uses max_gap_sessions=0."
        ),
    )
    args = ap.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    current = load_module(CURRENT_GENERATOR, "nine_review_current")
    previous = load_module(PRE_GENERATOR, "nine_review_previous")
    stable = json.loads((RISK / "stable_params_v41_1.json").read_text(encoding="utf-8-sig"))
    params = current._normalize_params(stable)
    start = pd.Timestamp("2026-07-09") - pd.Timedelta(days=args.lookback_days)

    print("[NINE_REVIEW] prepare pre-contract")
    pre, pre_meta = prepare(previous, "pre_contract", params, start)
    print("[NINE_REVIEW] prepare post-contract")
    post, post_meta = prepare(current, "post_contract", params, start, max_gap_sessions=args.max_gap_sessions)
    latest = min(pre["date"].max(), post["date"].max())
    start = latest - pd.Timedelta(days=args.lookback_days)
    pre = pre[pre["date"].between(start, latest)].copy()
    post = post[post["date"].between(start, latest)].copy()
    dates = common_dates(pre, post, args.min_universe)
    pre = pre[pre["date"].isin(dates)].copy()
    post = post[post["date"].isin(dates)].copy()
    regimes = ["ALL"] + sorted(set(post["market_regime"].dropna().astype(str)).intersection({"BULL", "BEAR"}))

    signal_rows = [perf(post, "signal", condition, condition, h, regime, factor) for condition, factor in CONDITIONS.items() for h in (1, 2, 5) for regime in regimes]
    role_rows = [perf(post, "analysis_role_group", role, f"role_{role}_pass", h, regime) for role in ROLES for h in (1, 2, 5) for regime in regimes]
    signal_df = pd.DataFrame(signal_rows)
    classes = classify(signal_df)
    cover = coverage(pre, post)
    inter = intersections(pre, "pre_contract") + intersections(post, "post_contract")
    loo = leave_one_out(post)
    counts = pass_counts(post)
    for h in (1, 2, 5):
        expected = next(r["selected_n"] for r in inter if r["dataset"] == "post_contract" and r["horizon"] == h and r["intersection"] == "core9")
        pass_count_n = next(r["n"] for r in counts if r["horizon"] == h and r["pass_count"] == 9)
        loo_counts = {r["core9_n"] for r in loo if r["horizon"] == h}
        if pass_count_n != expected or loo_counts != {expected}:
            raise AssertionError(f"core9 reconciliation failed h={h}: intersection={expected} pass_count={pass_count_n} loo={loo_counts}")

    compare = []
    for h in (1, 2, 5):
        target = f"fwd_ret_{h}d"
        for condition, factor in CONDITIONS.items():
            row = {"horizon": h, "condition": condition}
            for label, df in (("pre", pre), ("post", post)):
                x = df[df[target].notna() & df[factor].notna()]
                row[f"{label}_evaluable_n"] = len(x)
                row[f"{label}_pass_n"] = int(x[condition].sum())
                row[f"{label}_pass_rate"] = float(x[condition].mean()) if len(x) else None
            row["pass_n_delta"] = row["post_pass_n"] - row["pre_pass_n"]
            compare.append(row)

    daily = []
    for label, df in (("pre_contract", pre), ("post_contract", post)):
        for h in (1, 2, 5):
            for date, g in df[df[f"fwd_ret_{h}d"].notna()].groupby("date"):
                daily.append({"dataset": label, "horizon": h, "date": str(date.date()), "universe_n": len(g), "core9_n": int(g["core9_pass"].sum()), "core9_plus_macd_n": int(g["core9_plus_macd_pass"].sum())})

    contract = post_meta.get("integrity", {})
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "scope": "read_only_nine_condition_integrity_review",
        "policy_effect": False,
        "order_path_effect": False,
        "core_conditions": list(CONDITIONS),
        "macd_treatment": "separate_additional_confirmation",
        "analysis_role_groups": ROLES,
        "role_group_caveat": "analytical taxonomy only; not a live strategy label",
        "window": {"start_date": str(dates[0].date()), "end_date": str(dates[-1].date()), "dates": len(dates), "min_universe": args.min_universe},
        "pre_contract": pre_meta,
        "post_contract": post_meta,
        "contract_counts": contract,
        "calculation_checks": {"core9_intersection_pass_count_leave_one_out_reconciled": True},
        "coverage": cover,
        "intersections": inter,
        "classifications": classes,
        "caveats": [
            "Condition results are observational and correlated; they do not establish causal lift.",
            "The recent window may not provide balanced regime coverage.",
            "Role groups are analytical taxonomies, not live strategy labels.",
            "The >30% flag is descriptive; only >100% adjacent-session breaks are contract boundaries.",
            "No threshold or live policy is changed by this review.",
        ],
    }

    base = "nine_condition_integrity_review"
    outputs = {
        "signal_performance": signal_rows,
        "role_performance": role_rows,
        "coverage": cover,
        "intersections": inter,
        "classification": classes,
        "condition_compare": compare,
        "candidate_daily": daily,
        "leave_one_out": loo,
        "pass_count": counts,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    (out / f"{base}_{stamp}.json").write_text(text, encoding="utf-8")
    (out / f"{base}_latest.json").write_text(text, encoding="utf-8")
    for name, rows in outputs.items():
        dated = out / f"{base}_{name}_{stamp}.csv"
        write_csv(dated, rows)
        (out / f"{base}_{name}_latest.csv").write_bytes(dated.read_bytes())

    md = [
        "# Nine-Condition Price-History Integrity Review",
        "",
        "## Technical summary",
        "",
        f"- Window: {payload['window']['start_date']} to {payload['window']['end_date']} ({payload['window']['dates']} common dates).",
        f"- Post-contract rows: {post_meta['raw_rows']:,}; invalid blocked: {int(contract.get('invalid_rows', 0)):,}; session gaps (hard breaks): {int(contract.get('session_gap_breaks', 0)):,}; soft-gap bridges (<= {int(contract.get('max_gap_sessions', 0))} sessions, not broken): {int(contract.get('soft_gap_bridges', 0)):,}; extreme breaks: {int(contract.get('extreme_return_breaks', 0)):,}.",
        "- Core nine and nine-plus-MACD are separate. No policy or order-path change was made.",
        "- Analysis-only: soft-gap bridging and trade-count forward returns apply only to this script's reload of the current generator (`max_gap_sessions` passed explicitly); live candidate generation always uses `max_gap_sessions=0` (unchanged strict behavior).",
        "",
        "## Evidence-status classification",
        "",
        markdown_table(pd.DataFrame(classes)),
        "",
        "## Pre/post intersections",
        "",
        markdown_table(pd.DataFrame(inter)),
        "",
        "## Coverage and abnormal-return flags",
        "",
        markdown_table(pd.DataFrame(cover)),
        "",
        "## Limitations",
        "",
        *[f"- {x}" for x in payload["caveats"]],
    ]
    md_text = "\n".join(md) + "\n"
    (out / f"{base}_{stamp}.md").write_text(md_text, encoding="utf-8")
    (out / f"{base}_latest.md").write_text(md_text, encoding="utf-8")
    print("[NINE_REVIEW] " + json.dumps({"status": payload["status"], "window": payload["window"], "classifications": classes, "output": str(out / f"{base}_latest.json")}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
