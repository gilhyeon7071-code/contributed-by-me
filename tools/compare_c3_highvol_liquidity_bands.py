from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

AUDIT_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
OFFICIAL_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_liquidity_band_compare_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_liquidity_band_compare_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_liquidity_band_compare_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_liquidity_band_compare_latest.md"


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


def official_value_cap(contract: dict[str, Any]) -> float:
    payload = contract.get("selection_contract", contract)
    for item in payload.get("numeric_filters", []):
        if item.get("field") == "signal_value" and item.get("op") == "<=":
            return float(item["value"])
    raise ValueError("official signal_value <= cap not found")


def profit_factor(rets: list[float]) -> float | str:
    gross_profit = sum(x for x in rets if x > 0)
    gross_loss = -sum(x for x in rets if x <= 0)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return "inf"
    return 0.0


def concentration(rets: list[float]) -> dict[str, Any]:
    positives = sorted([x for x in rets if x > 0], reverse=True)
    gross_profit = sum(positives)
    top3 = sum(positives[:3])
    top5 = sum(positives[:5])
    total = sum(rets)
    return {
        "gross_profit": gross_profit,
        "top3_ret": top3,
        "top5_ret": top5,
        "top3_share_gross_profit": (top3 / gross_profit) if gross_profit else "NA",
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else "NA",
        "ret_without_top3": total - top3,
        "ret_without_top5": total - top5,
    }


def summarize(rows: Iterable[dict[str, Any]], band_id: str, band_note: str) -> dict[str, Any]:
    data = list(rows)
    rets = [as_float(r.get("ret")) for r in data]
    values = [as_float(r.get("signal_value")) for r in data]
    conc = concentration(rets)
    return {
        "band_id": band_id,
        "band_note": band_note,
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
    for path in [AUDIT_ROWS, OFFICIAL_CONTRACT]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    cap = official_value_cap(load_json(OFFICIAL_CONTRACT))
    rows = read_csv(AUDIT_ROWS)
    target = [r for r in rows if r.get("shadow_profile") == "shadow_vaccel_lt_1_75"]
    if len(target) != 22:
        raise ValueError(f"expected 22 conservative shadow rows, got {len(target)}")

    def val(row: dict[str, Any]) -> float:
        return as_float(row.get("signal_value"))

    band_specs: list[tuple[str, str, Callable[[dict[str, Any]], bool]]] = [
        ("all_conservative_shadow", "all 22 conservative shadow rows", lambda r: True),
        ("current_cap_le_2b", "current official C3 value upper cap", lambda r: val(r) <= cap),
        ("value_gt_2b_all", "all rows above current official C3 value cap", lambda r: val(r) > cap),
        ("value_gt_2b_to_20b", "above current cap but below 20B", lambda r: cap < val(r) < 20_000_000_000),
        ("value_gt_3b_to_20b", "mid-liquidity band excluding just-above-cap rows", lambda r: 3_000_000_000 <= val(r) < 20_000_000_000),
        ("value_20b_to_100b", "large liquidity band", lambda r: 20_000_000_000 <= val(r) < 100_000_000_000),
        ("value_100b_plus", "mega-cap liquidity band", lambda r: val(r) >= 100_000_000_000),
        ("floor_500m_no_upper", "liquidity floor 500M with no upper cap", lambda r: val(r) >= 500_000_000),
        ("floor_1b_no_upper", "liquidity floor 1B with no upper cap", lambda r: val(r) >= 1_000_000_000),
        ("floor_2b_no_upper", "liquidity floor 2B with no upper cap", lambda r: val(r) >= cap),
        ("floor_3b_no_upper", "liquidity floor 3B with no upper cap", lambda r: val(r) >= 3_000_000_000),
        ("value_500m_to_20b", "practical band from 500M to below 20B", lambda r: 500_000_000 <= val(r) < 20_000_000_000),
    ]

    expanded_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for band_id, note, predicate in band_specs:
        selected = []
        for row in target:
            if predicate(row):
                out = dict(row)
                out["liquidity_band_id"] = band_id
                out["liquidity_band_note"] = note
                selected.append(out)
                expanded_rows.append(out)
        summary_rows.append(summarize(selected, band_id, note))

    # Research ranking intentionally penalizes tiny n and top-heavy profiles.
    ranked = []
    for row in summary_rows:
        n = int(row["n"])
        ret_sum = as_float(row["ret_sum"])
        pf = row["profit_factor"]
        pf_num = 99.0 if pf == "inf" else as_float(pf)
        top5_share = row["top5_share_gross_profit"]
        top5_num = 1.0 if top5_share == "NA" else as_float(top5_share)
        ret_wo_top5 = as_float(row["ret_without_top5"])
        score = ret_sum + min(pf_num, 5.0) * 0.03 + min(n, 20) * 0.005 - top5_num * 0.15
        if ret_wo_top5 <= 0:
            score -= 0.20
        if n < 5:
            score -= 0.30
        ranked.append({**row, "research_rank_score": score})
    ranked.sort(key=lambda r: as_float(r["research_rank_score"]), reverse=True)

    # Keep this as research guidance only, not policy.
    preferred = [
        row for row in ranked
        if int(row["n"]) >= 6 and as_float(row["ret_sum"]) > 0 and as_float(row["ret_without_top5"]) > 0
    ][:3]

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_highvol_liquidity_band_compare",
        "source_rows": str(AUDIT_ROWS),
        "official_contract": str(OFFICIAL_CONTRACT),
        "target_shadow_profile": "shadow_vaccel_lt_1_75",
        "official_signal_value_cap": cap,
        "target_rows": len(target),
        "summary": summary_rows,
        "ranked_research_only": ranked,
        "preferred_research_only": preferred,
        "classification": {
            "operational_decision": "NOT_APPROVED",
            "research_classification": "C3_HIGHVOL_LIQUIDITY_BAND_COMPARE_RESEARCH_ONLY",
            "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
            "full_logic_application": "NOT_APPLIED",
            "reason": [
                "small_sample",
                "same_shadow_family_only",
                "liquidity_band_not_policy",
                "top_winner_concentration_still_material",
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
            "official_signal_value_cap_extracted: PASS",
            "target_shadow_profile_row_count_22: PASS",
            "band_summaries_recomputed_from_rows: PASS",
            "research_ranking_is_non_operational: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    write_csv(OUT_ROWS, expanded_rows, list(expanded_rows[0].keys()) if expanded_rows else [])
    summary_fields = [
        "band_id", "band_note", "n", "win_n", "loss_n", "stop_n",
        "ret_sum", "ret_mean", "profit_factor", "min_signal_value",
        "max_signal_value", "unique_codes", "gross_profit", "top3_ret",
        "top5_ret", "top3_share_gross_profit", "top5_share_gross_profit",
        "ret_without_top3", "ret_without_top5",
    ]
    write_csv(OUT_SUMMARY, summary_rows, summary_fields)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Liquidity Band Compare",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- target_rows: {len(target)}",
        f"- official_signal_value_cap: {cap:.0f}",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Band Summary",
        "",
    ]
    for row in summary_rows:
        md.append(
            f"- {row['band_id']}: n={row['n']}, ret_sum={float(row['ret_sum']):.6f}, "
            f"PF={row['profit_factor']}, ret_without_top5={float(row['ret_without_top5']):.6f}, "
            f"top5_share={row['top5_share_gross_profit']}"
        )
    md.extend(["", "## Preferred Research-Only Candidates", ""])
    for row in preferred:
        md.append(
            f"- {row['band_id']}: n={row['n']}, ret_sum={float(row['ret_sum']):.6f}, "
            f"PF={row['profit_factor']}, score={float(row['research_rank_score']):.6f}"
        )
    md.extend([
        "",
        "## Boundary",
        "",
        "- This comparison does not change current C3 value-cap policy.",
        "- This comparison does not approve high-volatility candidate generation.",
        "- The result is a research-only liquidity-band comparison inside the conservative shadow profile.",
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
        "target_rows": len(target),
        "preferred_research_only": [
            {
                "band_id": row["band_id"],
                "n": row["n"],
                "ret_sum": row["ret_sum"],
                "profit_factor": row["profit_factor"],
                "ret_without_top5": row["ret_without_top5"],
                "research_rank_score": row["research_rank_score"],
            }
            for row in preferred
        ],
        "classification": payload["classification"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
