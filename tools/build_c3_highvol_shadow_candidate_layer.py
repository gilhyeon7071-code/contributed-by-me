from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
HIGHVOL_TRADES_CSV = LOG_DIR / "c3_highvol_exit_variant_compare_trades_latest.csv"
C3_BASELINE_SUMMARY_CSV = LOG_DIR / "c3_mkt_vol_variant_compare_summary_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_shadow_candidate_layer_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_shadow_candidate_layer_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_shadow_candidate_layer_summary_latest.csv"
OUT_COMPARE = LOG_DIR / "c3_highvol_shadow_candidate_layer_compare_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_shadow_candidate_layer_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"{path} is not a JSON object")
    return data


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def as_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, "", "NA", "nan", "None"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_date(value: str) -> str:
    return (value or "")[:10]


def period_label(date_value: str) -> str:
    d = as_date(date_value)
    if not d:
        return "UNKNOWN"
    if "2024-01-01" <= d < "2024-07-01":
        return "2024H1"
    if "2024-07-01" <= d < "2025-01-01":
        return "2024H2"
    if "2025-01-01" <= d < "2025-07-01":
        return "2025H1"
    if "2025-07-01" <= d < "2026-01-01":
        return "2025H2"
    if "2026-01-01" <= d < "2026-07-01":
        return "2026H1"
    return "OTHER"


def metric_row(rows: Iterable[dict[str, Any]], profile: str, segment: str, note: str) -> dict[str, Any]:
    data = list(rows)
    rets = [as_float(r.get("ret"), 0.0) or 0.0 for r in data]
    wins = [x for x in rets if x > 0]
    losses = [x for x in rets if x <= 0]
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    pf: float | str
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = "inf"
    else:
        pf = 0.0
    entry_dates = sorted({as_date(str(r.get("entry_date_norm") or r.get("entry_date") or "")) for r in data if as_date(str(r.get("entry_date_norm") or r.get("entry_date") or ""))})
    return {
        "profile": profile,
        "segment": segment,
        "n": len(data),
        "win_n": len(wins),
        "loss_n": len(losses),
        "stop_n": sum(1 for r in data if str(r.get("exit_reason", "")).upper() == "STOP"),
        "win_rate": (len(wins) / len(data)) if data else "NA",
        "ret_sum": sum(rets),
        "ret_mean": (sum(rets) / len(data)) if data else "NA",
        "profit_factor": pf,
        "first_entry_date": entry_dates[0] if entry_dates else "",
        "last_entry_date": entry_dates[-1] if entry_dates else "",
        "unique_codes": len({str(r.get("code", "")) for r in data if str(r.get("code", ""))}),
        "note": note,
    }


def top_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rets = [as_float(r.get("ret"), 0.0) or 0.0 for r in rows]
    positives = sorted([x for x in rets if x > 0], reverse=True)
    gross_profit = sum(positives)
    top5 = sum(positives[:5])
    return {
        "gross_profit": gross_profit,
        "top1_ret": sum(positives[:1]),
        "top3_ret": sum(positives[:3]),
        "top5_ret": top5,
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else "NA",
        "ret_without_top5": sum(rets) - top5,
    }


def selected_baselines(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    keep = {
        ("hard_current_mkt_vol20", "all_contract_window"),
        ("hard_current_mkt_vol20", "target_2026H1"),
        ("remove_mkt_vol20_only", "all_contract_window"),
        ("remove_mkt_vol20_only", "target_2026H1"),
    }
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (row.get("variant", ""), row.get("segment", ""))
        if key not in keep:
            continue
        out.append({
            "comparison_type": "existing_c3_reference",
            "profile": row.get("variant", ""),
            "segment": row.get("segment", ""),
            "n": row.get("n", ""),
            "win_n": row.get("win_n", ""),
            "loss_n": row.get("loss_n", ""),
            "ret_sum": row.get("ret_sum", ""),
            "profit_factor": row.get("profit_factor", ""),
            "first_entry_date": row.get("first_entry_date", ""),
            "last_entry_date": row.get("last_entry_date", ""),
            "note": row.get("note", ""),
        })
    return out


def main() -> None:
    for path in [SPEC_JSON, HIGHVOL_TRADES_CSV, C3_BASELINE_SUMMARY_CSV]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    spec = load_json(SPEC_JSON)
    if spec.get("operational_decision") != "NOT_APPROVED" or spec.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("frozen spec must remain NOT_APPROVED / NOT_APPLIED")

    candidate = spec["candidate_spec"]
    gap_lo = float(candidate["entry_gap_pct"]["min_inclusive"])
    gap_hi = float(candidate["entry_gap_pct"]["max_exclusive"])
    vacc_lo = float(candidate["signal_v_accel"]["min_inclusive"])
    atr_lo = float(candidate["signal_atr_pct"]["min_inclusive"])
    atr_hi = float(candidate["signal_atr_pct"]["max_exclusive"])
    regimes = set(candidate["market_regime"])
    exit_variant = str(candidate["exit_variant"])

    profiles = [
        {
            "profile": "shadow_vaccel_lt_1_75",
            "v_accel_hi": float(candidate["signal_v_accel"]["best_same_sample_max_exclusive"]),
            "note": "same-sample best upper bound; research shadow only",
        },
        {
            "profile": "shadow_vaccel_lt_2_00",
            "v_accel_hi": float(candidate["signal_v_accel"]["walkforward_2025H2_selected_max_exclusive"]),
            "note": "walk-forward 2025H2 selected upper bound; research shadow only",
        },
    ]

    trade_rows = read_csv(HIGHVOL_TRADES_CSV)
    shadow_rows: list[dict[str, Any]] = []
    for row in trade_rows:
        if row.get("exit_variant") != exit_variant:
            continue
        if row.get("market_regime") not in regimes:
            continue
        gap = as_float(row.get("entry_gap_pct"))
        vacc = as_float(row.get("signal_v_accel"))
        atr = as_float(row.get("signal_atr_pct"))
        if gap is None or vacc is None or atr is None:
            continue
        if not (gap_lo <= gap < gap_hi and vacc_lo <= vacc and atr_lo <= atr < atr_hi):
            continue
        for profile in profiles:
            if vacc < profile["v_accel_hi"]:
                out = dict(row)
                out["shadow_profile"] = profile["profile"]
                out["shadow_note"] = profile["note"]
                out["shadow_candidate_id"] = spec["candidate_id"]
                out["entry_period"] = period_label(str(row.get("entry_date_norm") or row.get("entry_date") or ""))
                shadow_rows.append(out)

    row_key = ["shadow_profile", "signal_date", "entry_date", "exit_date", "code"]
    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in shadow_rows:
        key = tuple(str(row.get(k, "")) for k in row_key)
        if key in seen:
            raise ValueError(f"duplicate shadow row key: {key}")
        seen.add(key)
        deduped.append(row)
    shadow_rows = deduped

    summary_rows: list[dict[str, Any]] = []
    for profile in profiles:
        name = profile["profile"]
        rows = [r for r in shadow_rows if r["shadow_profile"] == name]
        summary_rows.append(metric_row(rows, name, "all", profile["note"]))
        summary_rows.append(metric_row([r for r in rows if str(r["entry_period"]) < "2026H1"], name, "pre_2026", profile["note"]))
        for segment in ["2024H1", "2024H2", "2025H1", "2025H2", "2026H1"]:
            summary_rows.append(metric_row([r for r in rows if r["entry_period"] == segment], name, segment, profile["note"]))
        for regime in ["BULL", "SIDEWAYS", "TRANSITION"]:
            summary_rows.append(metric_row([r for r in rows if r.get("market_regime") == regime], name, f"regime_{regime}", profile["note"]))

    concentration = {
        profile["profile"]: top_concentration([r for r in shadow_rows if r["shadow_profile"] == profile["profile"]])
        for profile in profiles
    }

    baseline_rows = selected_baselines(read_csv(C3_BASELINE_SUMMARY_CSV))
    compare_rows = list(baseline_rows)
    for row in summary_rows:
        if row["segment"] in {"all", "2026H1"}:
            compare_rows.append({
                "comparison_type": "highvol_shadow_research_only",
                "profile": row["profile"],
                "segment": row["segment"],
                "n": row["n"],
                "win_n": row["win_n"],
                "loss_n": row["loss_n"],
                "ret_sum": row["ret_sum"],
                "profit_factor": row["profit_factor"],
                "first_entry_date": row["first_entry_date"],
                "last_entry_date": row["last_entry_date"],
                "note": row["note"],
            })

    row_fields = list(shadow_rows[0].keys()) if shadow_rows else [
        "shadow_profile", "shadow_note", "shadow_candidate_id", "entry_period"
    ]
    summary_fields = [
        "profile", "segment", "n", "win_n", "loss_n", "stop_n", "win_rate",
        "ret_sum", "ret_mean", "profit_factor", "first_entry_date",
        "last_entry_date", "unique_codes", "note",
    ]
    compare_fields = [
        "comparison_type", "profile", "segment", "n", "win_n", "loss_n",
        "ret_sum", "profit_factor", "first_entry_date", "last_entry_date", "note",
    ]

    write_csv(OUT_ROWS, shadow_rows, row_fields)
    write_csv(OUT_SUMMARY, summary_rows, summary_fields)
    write_csv(OUT_COMPARE, compare_rows, compare_fields)

    spec_summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_c3_highvol_shadow_candidate_layer",
        "candidate_id": spec["candidate_id"],
        "source_spec": str(SPEC_JSON),
        "source_trades": str(HIGHVOL_TRADES_CSV),
        "source_baseline_summary": str(C3_BASELINE_SUMMARY_CSV),
        "outputs": {
            "rows": str(OUT_ROWS),
            "summary": str(OUT_SUMMARY),
            "compare": str(OUT_COMPARE),
            "markdown": str(OUT_MD),
        },
        "profiles": profiles,
        "input_rows": len(trade_rows),
        "shadow_rows": len(shadow_rows),
        "summary": summary_rows,
        "concentration": concentration,
        "classification": {
            "operational_decision": "NOT_APPROVED",
            "research_classification": "C3_HIGHVOL_SHADOW_LAYER_BUILT_READ_ONLY",
            "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
            "full_logic_application": "NOT_APPLIED",
            "reason": [
                "shadow_layer_only",
                "existing_c3_unchanged",
                "future_evidence_still_required",
            ],
        },
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "validation": [
            "frozen_spec_not_approved_not_applied: PASS",
            "shadow_row_duplicate_check: PASS",
            "summary_recomputed_from_rows: PASS",
            "baseline_reference_loaded: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }
    OUT_JSON.write_text(json.dumps(spec_summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Shadow Candidate Layer",
        "",
        f"- candidate_id: {spec_summary['candidate_id']}",
        f"- generated_at: {spec_summary['generated_at']}",
        "- status: READ_ONLY_SHADOW_LAYER",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Summary",
        "",
    ]
    for row in summary_rows:
        if row["segment"] in {"all", "2025H2", "2026H1"}:
            md.append(
                f"- {row['profile']} / {row['segment']}: "
                f"n={row['n']}, ret_sum={float(row['ret_sum']):.6f}, PF={row['profit_factor']}"
            )
    md.extend([
        "",
        "## Concentration",
        "",
    ])
    for profile, values in concentration.items():
        share = values["top5_share_gross_profit"]
        share_text = f"{float(share) * 100:.2f}%" if isinstance(share, (float, int)) else str(share)
        md.append(f"- {profile}: top5_share_gross_profit={share_text}, ret_without_top5={float(values['ret_without_top5']):.6f}")
    md.extend([
        "",
        "## Boundary",
        "",
        "- This is not an operational candidate-generation change.",
        "- This is not a stable/HPO/paper/broker approval.",
        "- Existing C3 remains unchanged.",
        "- Use only as a separate research shadow comparison layer.",
        "",
        "## Outputs",
        "",
        f"- rows: {OUT_ROWS}",
        f"- summary: {OUT_SUMMARY}",
        f"- compare: {OUT_COMPARE}",
        f"- json: {OUT_JSON}",
        "",
    ])
    OUT_MD.write_text("\n".join(md), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "out_json": str(OUT_JSON),
        "out_rows": str(OUT_ROWS),
        "out_summary": str(OUT_SUMMARY),
        "out_compare": str(OUT_COMPARE),
        "shadow_rows": len(shadow_rows),
        "classification": spec_summary["classification"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
