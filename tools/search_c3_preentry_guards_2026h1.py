from __future__ import annotations

import csv
import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

REVIVED_ROWS = LOG_DIR / "c3_revived_2026h1_trade_analysis_rows_latest.csv"

LATEST_JSON = LOG_DIR / "c3_preentry_guard_search_2026h1_latest.json"
LATEST_GUARDS_CSV = LOG_DIR / "c3_preentry_guard_search_2026h1_guards_latest.csv"
LATEST_FEATURES_CSV = LOG_DIR / "c3_preentry_guard_search_2026h1_features_latest.csv"
LATEST_MD = LOG_DIR / "c3_preentry_guard_search_2026h1_latest.md"

STATUS = "READ_ONLY_C3_PREENTRY_GUARD_SEARCH_2026H1_NOT_OPERATIONAL"


PRE_ENTRY_FEATURES = [
    "score",
    "entry_gap_pct",
    "signal_ret1_pct",
    "signal_rs",
    "signal_v_accel",
    "signal_stretch",
    "signal_atr_pct",
    "signal_high_52w_gap",
    "signal_value",
]

POST_ENTRY_DIAGNOSTICS = [
    "entry_day_open_to_close_ret",
    "mfe_1d",
    "mae_1d",
    "followthrough_1d",
]


def _profit_factor(ret: Iterable[float]) -> float | str:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return round(gains / losses, 6)
    if gains > 0:
        return "inf"
    return "NA"


def _summarize_mask(df: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    part = df[mask.fillna(False)].copy()
    ret = pd.to_numeric(part["ret"], errors="coerce").fillna(0.0)
    if part.empty:
        return {
            "n": 0,
            "win_n": 0,
            "loss_n": 0,
            "stop_n": 0,
            "large_win_n": 0,
            "ret_sum": 0.0,
            "profit_factor": "NA",
            "codes": "",
        }
    return {
        "n": int(len(part)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "stop_n": int(part["exit_reason"].astype(str).str.upper().eq("STOP").sum()),
        "large_win_n": int(part["row_quality"].astype(str).eq("large_single_win").sum()),
        "ret_sum": round(float(ret.sum()), 6),
        "profit_factor": _profit_factor(ret.tolist()),
        "codes": ",".join(part["code"].astype(str).tolist()),
    }


def _candidate_thresholds(series: pd.Series) -> list[float]:
    vals = sorted(set(float(x) for x in pd.to_numeric(series, errors="coerce").dropna().tolist()))
    if len(vals) <= 1:
        return vals
    mids = [(a + b) / 2.0 for a, b in zip(vals[:-1], vals[1:])]
    return vals + mids


def _guard_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    baseline = _summarize_mask(df, pd.Series(True, index=df.index))

    single_masks: list[tuple[str, pd.Series, dict[str, Any]]] = []
    for feature in PRE_ENTRY_FEATURES:
        series = pd.to_numeric(df[feature], errors="coerce")
        for threshold in _candidate_thresholds(series):
            for op in [">=", "<="]:
                mask = series >= threshold if op == ">=" else series <= threshold
                stats = _summarize_mask(df, mask)
                if stats["n"] == 0:
                    continue
                guard = {
                    "guard_type": "single_pre_entry",
                    "guard": f"{feature} {op} {threshold:.10g}",
                    "features": feature,
                    "thresholds": f"{threshold:.10g}",
                    "ops": op,
                    "decision_time": "pre_entry_or_entry_time",
                    **stats,
                    "baseline_n": baseline["n"],
                    "baseline_ret_sum": baseline["ret_sum"],
                    "comment": "",
                }
                guard["comment"] = _classify_guard(guard)
                rows.append(guard)
                if 1 <= stats["n"] <= 4:
                    single_masks.append((guard["guard"], mask, guard))

    for (guard_a, mask_a, _), (guard_b, mask_b, _) in itertools.combinations(single_masks, 2):
        feature_a = guard_a.split()[0]
        feature_b = guard_b.split()[0]
        if feature_a == feature_b:
            continue
        mask = mask_a & mask_b
        stats = _summarize_mask(df, mask)
        if stats["n"] == 0:
            continue
        guard = {
            "guard_type": "pair_pre_entry",
            "guard": f"{guard_a} AND {guard_b}",
            "features": f"{feature_a}|{feature_b}",
            "thresholds": "",
            "ops": "AND",
            "decision_time": "pre_entry_or_entry_time",
            **stats,
            "baseline_n": baseline["n"],
            "baseline_ret_sum": baseline["ret_sum"],
            "comment": "",
        }
        guard["comment"] = _classify_guard(guard)
        rows.append(guard)

    rows.sort(
        key=lambda r: (
            str(r["comment"]) != "candidate_but_too_small",
            -int(r["large_win_n"]),
            int(r["stop_n"]),
            -float(r["ret_sum"]),
            int(r["n"]),
        )
    )
    return rows


def _classify_guard(row: dict[str, Any]) -> str:
    n = int(row["n"])
    large_win_n = int(row["large_win_n"])
    stop_n = int(row["stop_n"])
    win_n = int(row["win_n"])
    ret_sum = float(row["ret_sum"])
    if n <= 2 and large_win_n >= 1 and stop_n == 0 and ret_sum > 0:
        return "candidate_but_too_small"
    if n <= 4 and large_win_n >= 1 and stop_n <= 1 and ret_sum > 0:
        return "weak_candidate_small_sample"
    if stop_n >= max(1, n // 2):
        return "reject_stop_heavy"
    if ret_sum <= 0:
        return "reject_non_positive"
    loss_n = int(row["loss_n"])
    if win_n <= loss_n:
        return "weak_low_win_count"
    return "weak_candidate"


def _feature_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    groups = {
        "all_revived": df,
        "large_single_win": df[df["row_quality"].eq("large_single_win")],
        "stop_loss": df[df["row_quality"].eq("stop_loss")],
        "q25_subset": df[df["membership_group"].eq("q25_subset")],
        "q25_losses": df[df["membership_group"].eq("q25_subset") & (df["ret"] < 0)],
    }
    for group, part in groups.items():
        for feature in PRE_ENTRY_FEATURES + POST_ENTRY_DIAGNOSTICS:
            vals = pd.to_numeric(part.get(feature, pd.Series(dtype=float)), errors="coerce").dropna()
            rows.append(
                {
                    "feature_group": "pre_entry" if feature in PRE_ENTRY_FEATURES else "post_entry_diagnostic",
                    "group": group,
                    "feature": feature,
                    "n": int(len(vals)),
                    "min": round(float(vals.min()), 10) if len(vals) else "",
                    "median": round(float(vals.median()), 10) if len(vals) else "",
                    "max": round(float(vals.max()), 10) if len(vals) else "",
                    "mean": round(float(vals.mean()), 10) if len(vals) else "",
                }
            )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 Pre-entry Guard Search 2026H1",
        "",
        f"- status: `{payload['status']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Top Guard Candidates",
        "",
        "| guard_type | guard | n | win | loss | stop | ret_sum | comment | codes |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in payload["top_guard_rows"][:20]:
        lines.append(
            "| {guard_type} | {guard} | {n} | {win_n} | {loss_n} | {stop_n} | {ret_sum} | {comment} | {codes} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Field Timing Boundary",
            "",
            "- Pre-entry / entry-time candidate fields: `score`, `entry_gap_pct`, `signal_ret1_pct`, `signal_rs`, `signal_v_accel`, `signal_stretch`, `signal_atr_pct`, `signal_high_52w_gap`, `signal_value`.",
            "- Post-entry diagnostic fields only: `entry_day_open_to_close_ret`, `mfe_1d`, `mae_1d`, `followthrough_1d`.",
            "",
            "## Boundary",
            "",
            "- Read-only guard search only.",
            "- No candidate generation, official backtest source, HPO, paper/live order, gate, stable parameter, or policy file was changed.",
            "- Small-sample guards are not approved operating rules.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not REVIVED_ROWS.exists():
        raise FileNotFoundError(REVIVED_ROWS)
    df = pd.read_csv(REVIVED_ROWS, dtype=str, encoding="utf-8-sig")
    for feature in PRE_ENTRY_FEATURES + POST_ENTRY_DIAGNOSTICS + ["ret"]:
        if feature in df.columns:
            df[feature] = pd.to_numeric(df[feature], errors="coerce")
    df["exit_reason"] = df["exit_reason"].astype(str)
    df["row_quality"] = df["row_quality"].astype(str)
    df["membership_group"] = df["membership_group"].astype(str)

    guard_rows = _guard_rows(df)
    feature_rows = _feature_rows(df)
    candidate_rows = [r for r in guard_rows if r["comment"] == "candidate_but_too_small"]
    weak_rows = [r for r in guard_rows if str(r["comment"]).startswith("weak_candidate")]

    if candidate_rows:
        conclusion = (
            "Only tiny pre-entry guards can isolate the large wins; no guard has enough 2026H1 support for operating use."
        )
    elif weak_rows:
        conclusion = "Some weak pre-entry guards exist, but none are strong enough for operating use."
    else:
        conclusion = "No useful pre-entry guard was found in the revived 2026H1 sample."

    payload = {
        "status": STATUS,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "revived_rows": str(REVIVED_ROWS),
        },
        "input_rows": int(len(df)),
        "pre_entry_features": PRE_ENTRY_FEATURES,
        "post_entry_diagnostics": POST_ENTRY_DIAGNOSTICS,
        "guard_row_count": int(len(guard_rows)),
        "candidate_but_too_small_count": int(len(candidate_rows)),
        "weak_candidate_count": int(len(weak_rows)),
        "top_guard_rows": guard_rows[:50],
        "feature_summary_rows": feature_rows,
        "conclusion": conclusion,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required": [
            "Do not approve mkt_vol20 relaxation from this 10-row revived sample.",
            "If pursuing high-volatility entries, build a separate strategy family and validate it beyond C3.",
            "If staying inside C3, keep mkt_vol20 as a volatility exclusion until a larger pre-entry guard is found.",
        ],
    }

    guard_fields = [
        "guard_type",
        "guard",
        "features",
        "thresholds",
        "ops",
        "decision_time",
        "n",
        "win_n",
        "loss_n",
        "stop_n",
        "large_win_n",
        "ret_sum",
        "profit_factor",
        "codes",
        "baseline_n",
        "baseline_ret_sum",
        "comment",
    ]
    feature_fields = ["feature_group", "group", "feature", "n", "min", "median", "max", "mean"]
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(LATEST_GUARDS_CSV, guard_rows, guard_fields)
    _write_csv(LATEST_FEATURES_CSV, feature_rows, feature_fields)
    _write_md(LATEST_MD, payload)
    print(json.dumps({"status": STATUS, "json": str(LATEST_JSON), "guards_csv": str(LATEST_GUARDS_CSV), "features_csv": str(LATEST_FEATURES_CSV), "md": str(LATEST_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
