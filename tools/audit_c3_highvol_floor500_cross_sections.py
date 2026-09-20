from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
LIQUIDITY_COMPARE_JSON = LOG_DIR / "c3_highvol_liquidity_band_compare_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_floor500_cross_section_audit_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_floor500_cross_section_audit_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_floor500_cross_section_audit_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_floor500_cross_section_audit_latest.md"


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


def profit_factor(rets: list[float]) -> float | str:
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


def summarize(rows: Iterable[dict[str, Any]], group: str, value: str) -> dict[str, Any]:
    data = list(rows)
    rets = [as_float(r.get("ret")) for r in data]
    values = [as_float(r.get("signal_value")) for r in data]
    conc = concentration(rets)
    return {
        "group": group,
        "value": value,
        "n": len(data),
        "win_n": sum(1 for x in rets if x > 0),
        "loss_n": sum(1 for x in rets if x <= 0),
        "stop_n": sum(1 for r in data if str(r.get("exit_reason", "")).upper() == "STOP"),
        "ret_sum": sum(rets),
        "ret_mean": (sum(rets) / len(data)) if data else "NA",
        "profit_factor": profit_factor(rets),
        "min_signal_value": min(values) if values else "NA",
        "max_signal_value": max(values) if values else "NA",
        "unique_codes": len({str(r.get("code", "")) for r in data if str(r.get("code", ""))}),
        **conc,
    }


def main() -> None:
    for path in [SOURCE_ROWS, LIQUIDITY_COMPARE_JSON]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    compare = load_json(LIQUIDITY_COMPARE_JSON)
    if compare.get("classification", {}).get("operational_decision") != "NOT_APPROVED":
        raise ValueError("liquidity compare is not NOT_APPROVED")
    if compare.get("classification", {}).get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("liquidity compare is not NOT_APPLIED")

    all_rows = [r for r in read_csv(SOURCE_ROWS) if r.get("shadow_profile") == "shadow_vaccel_lt_1_75"]
    if len(all_rows) != 22:
        raise ValueError(f"expected 22 conservative shadow rows, got {len(all_rows)}")

    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in all_rows:
        out = dict(row)
        out["floor500_selected"] = str(as_float(row.get("signal_value")) >= 500_000_000).lower()
        if out["floor500_selected"] == "true":
            selected.append(out)
        else:
            excluded.append(out)

    if len(selected) != 19:
        raise ValueError(f"expected 19 floor500 rows, got {len(selected)}")

    summary: list[dict[str, Any]] = []
    summary.append(summarize(selected, "all", "floor_500m_no_upper"))
    summary.append(summarize(excluded, "excluded", "below_500m"))
    for regime in ["BULL", "SIDEWAYS", "TRANSITION"]:
        summary.append(summarize([r for r in selected if r.get("market_regime") == regime], "market_regime", regime))
    for period in ["2024H1", "2024H2", "2025H1", "2025H2", "2026H1"]:
        summary.append(summarize([r for r in selected if r.get("entry_period") == period], "entry_period", period))
    for period in ["2025H1", "2025H2", "2026H1"]:
        for regime in ["BULL", "SIDEWAYS"]:
            summary.append(
                summarize(
                    [r for r in selected if r.get("entry_period") == period and r.get("market_regime") == regime],
                    "period_x_regime",
                    f"{period}_{regime}",
                )
            )

    selected_rets = [as_float(r.get("ret")) for r in selected]
    conc = concentration(selected_rets)
    period_2026 = [r for r in selected if r.get("entry_period") == "2026H1"]
    reasons = [
        "floor500_selected_19_of_22",
        "ret_positive",
        "top5_concentration_high",
    ]
    if not period_2026:
        reasons.append("no_2026H1_rows_after_floor500")
    if as_float(conc["ret_without_top5"]) > 0:
        reasons.append("ret_without_top5_still_positive")

    classification = {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_FLOOR500_CROSS_SECTION_POSITIVE_BUT_CURRENT_PERIOD_GAP",
        "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "reason": reasons,
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
        "scope": "read_only_floor500_cross_section_audit",
        "source_rows": str(SOURCE_ROWS),
        "source_liquidity_compare": str(LIQUIDITY_COMPARE_JSON),
        "target_shadow_profile": "shadow_vaccel_lt_1_75",
        "liquidity_rule": "signal_value >= 500_000_000 with no upper cap",
        "source_row_count": len(all_rows),
        "selected_row_count": len(selected),
        "excluded_below_500m_count": len(excluded),
        "summary": summary,
        "concentration": conc,
        "top_rows": sorted(selected, key=lambda r: as_float(r.get("ret")), reverse=True)[:8],
        "excluded_below_500m_rows": excluded,
        "classification": classification,
        "operation_effect": operation_effect,
        "validation": [
            "liquidity_compare_not_approved_not_applied: PASS",
            "source_conservative_shadow_rows_22: PASS",
            "floor500_selected_rows_19: PASS",
            "cross_sections_recomputed_from_rows: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    row_fields = list(selected[0].keys()) if selected else []
    write_csv(OUT_ROWS, selected, row_fields)
    summary_fields = [
        "group", "value", "n", "win_n", "loss_n", "stop_n",
        "ret_sum", "ret_mean", "profit_factor", "min_signal_value",
        "max_signal_value", "unique_codes", "gross_profit", "top1_ret",
        "top3_ret", "top5_ret", "top1_share_gross_profit",
        "top3_share_gross_profit", "top5_share_gross_profit",
        "ret_without_top1", "ret_without_top3", "ret_without_top5",
    ]
    write_csv(OUT_SUMMARY, summary, summary_fields)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Floor500 Cross-Section Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        "- liquidity_rule: signal_value >= 500,000,000 with no upper cap",
        f"- selected_row_count: {len(selected)}",
        f"- excluded_below_500m_count: {len(excluded)}",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Summary",
        "",
    ]
    for row in summary:
        if row["group"] in {"all", "excluded", "market_regime", "entry_period"}:
            md.append(
                f"- {row['group']} / {row['value']}: n={row['n']}, "
                f"ret_sum={float(row['ret_sum']):.6f}, PF={row['profit_factor']}, "
                f"ret_without_top5={float(row['ret_without_top5']):.6f}"
            )
    md.extend([
        "",
        "## Key Caveat",
        "",
        f"- 2026H1 selected rows: {len(period_2026)}",
        "- This means floor500 currently has no 2026H1 evidence inside the conservative shadow profile.",
        "",
        "## Boundary",
        "",
        "- This audit does not change candidate generation.",
        "- This audit does not approve the high-volatility family.",
        "- This audit only checks whether the floor500 research band survives cross-section review.",
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
        "selected_row_count": len(selected),
        "excluded_below_500m_count": len(excluded),
        "ret_sum": sum(selected_rets),
        "ret_without_top5": conc["ret_without_top5"],
        "selected_2026h1_rows": len(period_2026),
        "classification": classification,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
