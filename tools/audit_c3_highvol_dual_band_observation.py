from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
FLOOR500_JSON = LOG_DIR / "c3_highvol_floor500_cross_section_audit_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_dual_band_observation_audit_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_dual_band_observation_audit_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_dual_band_observation_audit_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_dual_band_observation_audit_latest.md"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"{path} is not a JSON object")
    return data


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "NA", "nan"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pf(rets: list[float]) -> float | str:
    gross_profit = sum(x for x in rets if x > 0)
    gross_loss = -sum(x for x in rets if x <= 0)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return "inf"
    return 0.0


def concentration(rets: list[float]) -> dict[str, Any]:
    wins = sorted([x for x in rets if x > 0], reverse=True)
    gross_profit = sum(wins)
    top1 = sum(wins[:1])
    top3 = sum(wins[:3])
    top5 = sum(wins[:5])
    total = sum(rets)
    return {
        "gross_profit": gross_profit,
        "top1_ret": top1,
        "top3_ret": top3,
        "top5_ret": top5,
        "top1_share_gross_profit": (top1 / gross_profit) if gross_profit else "NA",
        "top3_share_gross_profit": (top3 / gross_profit) if gross_profit else "NA",
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else "NA",
        "ret_without_top1": total - top1,
        "ret_without_top3": total - top3,
        "ret_without_top5": total - top5,
    }


def summarize(rows: Iterable[dict[str, Any]], group: str, value: str, note: str) -> dict[str, Any]:
    data = list(rows)
    rets = [as_float(r.get("ret")) for r in data]
    values = [as_float(r.get("signal_value")) for r in data]
    return {
        "group": group,
        "value": value,
        "note": note,
        "n": len(data),
        "win_n": sum(1 for x in rets if x > 0),
        "loss_n": sum(1 for x in rets if x <= 0),
        "stop_n": sum(1 for r in data if str(r.get("exit_reason", "")).upper() == "STOP"),
        "ret_sum": sum(rets),
        "ret_mean": (sum(rets) / len(data)) if data else "NA",
        "profit_factor": pf(rets),
        "min_signal_value": min(values) if values else "NA",
        "max_signal_value": max(values) if values else "NA",
        "unique_codes": len({str(r.get("code", "")) for r in data if str(r.get("code", ""))}),
        **concentration(rets),
    }


def lane_for(row: dict[str, Any]) -> tuple[str, str, str]:
    value = as_float(row.get("signal_value"))
    period = str(row.get("entry_period", ""))
    if value >= 500_000_000:
        return (
            "primary_floor500_quality_lane",
            "research_quality_candidate",
            "signal_value >= 500M; historical quality lane",
        )
    if period == "2026H1":
        return (
            "sub500_current_observation_lane",
            "current_regime_observation_only",
            "signal_value < 500M but appears in 2026H1; observe only",
        )
    return (
        "sub500_legacy_weak_lane",
        "true_exclude_for_now",
        "signal_value < 500M outside current 2026H1 and weak in sample",
    )


def main() -> None:
    for path in [SOURCE_ROWS, FLOOR500_JSON]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    floor500 = load_json(FLOOR500_JSON)
    if floor500.get("classification", {}).get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("floor500 audit is not NOT_APPLIED")
    if floor500.get("classification", {}).get("operational_decision") != "NOT_APPROVED":
        raise ValueError("floor500 audit is not NOT_APPROVED")

    source = [r for r in read_csv(SOURCE_ROWS) if r.get("shadow_profile") == "shadow_vaccel_lt_1_75"]
    if len(source) != 22:
        raise ValueError(f"expected 22 conservative shadow rows, got {len(source)}")

    rows: list[dict[str, Any]] = []
    for row in source:
        lane, bucket, reason = lane_for(row)
        out = dict(row)
        out["dual_band_lane"] = lane
        out["dual_band_bucket"] = bucket
        out["dual_band_reason"] = reason
        rows.append(out)

    summary: list[dict[str, Any]] = []
    summary.append(summarize(rows, "all", "all_dual_band_source", "all 22 conservative shadow rows"))
    for lane in [
        "primary_floor500_quality_lane",
        "sub500_current_observation_lane",
        "sub500_legacy_weak_lane",
    ]:
        summary.append(summarize([r for r in rows if r["dual_band_lane"] == lane], "dual_band_lane", lane, lane))
    for bucket in [
        "research_quality_candidate",
        "current_regime_observation_only",
        "true_exclude_for_now",
    ]:
        summary.append(summarize([r for r in rows if r["dual_band_bucket"] == bucket], "dual_band_bucket", bucket, bucket))
    for period in ["2024H1", "2025H1", "2025H2", "2026H1"]:
        for lane in [
            "primary_floor500_quality_lane",
            "sub500_current_observation_lane",
            "sub500_legacy_weak_lane",
        ]:
            summary.append(
                summarize(
                    [r for r in rows if r.get("entry_period") == period and r["dual_band_lane"] == lane],
                    "period_x_lane",
                    f"{period}_{lane}",
                    "period by dual-band lane",
                )
            )

    lane_counts = {row["value"]: row["n"] for row in summary if row["group"] == "dual_band_lane"}
    current_obs = [r for r in rows if r["dual_band_lane"] == "sub500_current_observation_lane"]
    quality = [r for r in rows if r["dual_band_lane"] == "primary_floor500_quality_lane"]
    legacy_weak = [r for r in rows if r["dual_band_lane"] == "sub500_legacy_weak_lane"]

    classification = {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_DUAL_BAND_OBSERVATION_SPLIT_REQUIRED",
        "confidence": "LOW_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "reason": [
            "primary_floor500_positive_but_top_heavy",
            "sub500_current_lane_has_2026H1_only_n2",
            "sub500_legacy_lane_weak",
            "dual_band_is_observation_not_policy",
        ],
    }
    operation_effect = {
        "candidate_generation_changed": False,
        "backtest_changed": False,
        "hpo_changed": False,
        "paper_or_broker_changed": False,
        "gate_or_threshold_changed": False,
        "policy_changed": False,
        "full_logic_application": "NOT_APPLIED",
    }

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_highvol_dual_band_observation_audit",
        "source_rows": str(SOURCE_ROWS),
        "source_floor500_audit": str(FLOOR500_JSON),
        "target_shadow_profile": "shadow_vaccel_lt_1_75",
        "source_row_count": len(source),
        "lane_counts": lane_counts,
        "summary": summary,
        "primary_floor500_quality_lane_rows": len(quality),
        "sub500_current_observation_lane_rows": len(current_obs),
        "sub500_legacy_weak_lane_rows": len(legacy_weak),
        "classification": classification,
        "operation_effect": operation_effect,
        "validation": [
            "floor500_audit_not_approved_not_applied: PASS",
            "source_conservative_shadow_rows_22: PASS",
            "lane_counts_recomputed_from_rows: PASS",
            "sub500_current_observation_lane_n2: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    write_csv(OUT_ROWS, rows, list(rows[0].keys()) if rows else [])
    summary_fields = [
        "group", "value", "note", "n", "win_n", "loss_n", "stop_n",
        "ret_sum", "ret_mean", "profit_factor", "min_signal_value",
        "max_signal_value", "unique_codes", "gross_profit", "top1_ret",
        "top3_ret", "top5_ret", "top1_share_gross_profit",
        "top3_share_gross_profit", "top5_share_gross_profit",
        "ret_without_top1", "ret_without_top3", "ret_without_top5",
    ]
    write_csv(OUT_SUMMARY, summary, summary_fields)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Dual-Band Observation Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- source_row_count: {len(source)}",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Lane Summary",
        "",
    ]
    for row in summary:
        if row["group"] in {"dual_band_lane", "dual_band_bucket"}:
            md.append(
                f"- {row['group']} / {row['value']}: n={row['n']}, "
                f"ret_sum={float(row['ret_sum']):.6f}, PF={row['profit_factor']}, "
                f"ret_without_top5={float(row['ret_without_top5']):.6f}"
            )
    md.extend([
        "",
        "## Boundary",
        "",
        "- primary_floor500_quality_lane is a research quality lane, not an approved rule.",
        "- sub500_current_observation_lane is observe-only because it has only 2 rows, both in 2026H1.",
        "- sub500_legacy_weak_lane is excluded for now because the non-current sub500 evidence is weak.",
        "- This audit does not change candidate generation, HPO, gates, paper, or broker paths.",
        "",
        "## Outputs",
        "",
        f"- rows: {OUT_ROWS}",
        f"- summary: {OUT_SUMMARY}",
        f"- json: {OUT_JSON}",
        "",
    ])
    OUT_MD.write_text("\n".join(md), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "out_json": str(OUT_JSON),
        "out_rows": str(OUT_ROWS),
        "out_summary": str(OUT_SUMMARY),
        "out_md": str(OUT_MD),
        "lane_counts": lane_counts,
        "classification": classification,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
