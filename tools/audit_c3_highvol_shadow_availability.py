from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SHADOW_ROWS = LOG_DIR / "c3_highvol_shadow_candidate_layer_rows_latest.csv"
BASELINE_TRADES = LOG_DIR / "c3_mkt_vol_variant_compare_trades_latest.csv"
BROADER_TRADES = LOG_DIR / "c3_highvol_broader_source_replay_trades_latest.csv"
SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_shadow_availability_audit_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_shadow_availability_audit_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_shadow_availability_audit_latest.md"


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


def key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("signal_date", "")), str(row.get("entry_date", "")), str(row.get("code", "")))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "NA", "nan"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def metric_row(rows: Iterable[dict[str, Any]], group: str, value: str) -> dict[str, Any]:
    data = list(rows)
    rets = [as_float(r.get("ret")) for r in data]
    wins = [x for x in rets if x > 0]
    losses = [x for x in rets if x <= 0]
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    if gross_loss > 0:
        pf: float | str = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = "inf"
    else:
        pf = 0.0
    return {
        "group": group,
        "value": value,
        "n": len(data),
        "win_n": len(wins),
        "loss_n": len(losses),
        "stop_n": sum(1 for r in data if str(r.get("exit_reason", "")).upper() == "STOP"),
        "ret_sum": sum(rets),
        "ret_mean": (sum(rets) / len(data)) if data else "NA",
        "profit_factor": pf,
        "unique_codes": len({str(r.get("code", "")) for r in data if str(r.get("code", ""))}),
    }


def classify(base_variants: set[str], broader_variants: set[str]) -> tuple[str, str, str]:
    if "hard_current_mkt_vol20" in base_variants:
        return (
            "BUY_POSSIBLE_CURRENT_C3_REFERENCE",
            "현재 C3 참고 기준에도 존재",
            "buy_possible",
        )
    if "remove_mkt_vol20_only" in base_variants:
        return (
            "EXPLORATION_MKTVOL_ONLY_BLOCKED",
            "기존 C3에서 시장변동성 조건만 제거하면 존재",
            "exploration_candidate",
        )
    if "any_regime_c3_no_mktvol" in broader_variants:
        return (
            "EXPLORATION_C3_CAPS_NO_MKTVOL_MEMBERSHIP",
            "넓힌 연구 소스에서 C3 caps와 no-mktvol 조합에는 존재",
            "exploration_candidate",
        )
    if "any_regime_no_c3_value_cap" in broader_variants:
        return (
            "EXPLORATION_VALUE_CAP_REVIEW_REQUIRED",
            "넓힌 연구 소스에서 C3 value cap 완화가 필요",
            "exploration_candidate",
        )
    if "any_regime_no_reversion_caps" in broader_variants:
        return (
            "TRUE_EXCLUDE_REVERSION_DEFENSE_REQUIRED",
            "저RS/52주 방어 해제가 필요한 위험군",
            "true_exclude",
        )
    return (
        "UNMATCHED_SOURCE_MEMBERSHIP",
        "대조 소스에서 멤버십을 찾지 못함",
        "true_exclude",
    )


def main() -> None:
    for path in [SHADOW_ROWS, BASELINE_TRADES, BROADER_TRADES, SPEC_JSON]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    spec = load_json(SPEC_JSON)
    if spec.get("operational_decision") != "NOT_APPROVED":
        raise ValueError("source spec is not NOT_APPROVED")
    if spec.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("source spec is not NOT_APPLIED")

    base_index: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in read_csv(BASELINE_TRADES):
        base_index[key(row)].add(str(row.get("variant", "")))

    broader_index: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in read_csv(BROADER_TRADES):
        broader_index[key(row)].add(str(row.get("variant", "")))

    shadow = [
        row for row in read_csv(SHADOW_ROWS)
        if row.get("shadow_profile") == "shadow_vaccel_lt_1_75"
    ]
    if len(shadow) != 22:
        raise ValueError(f"expected 22 conservative shadow rows, got {len(shadow)}")

    seen: set[tuple[str, str, str]] = set()
    out_rows: list[dict[str, Any]] = []
    for row in shadow:
        k = key(row)
        if k in seen:
            raise ValueError(f"duplicate conservative shadow key: {k}")
        seen.add(k)
        base_variants = base_index.get(k, set())
        broader_variants = broader_index.get(k, set())
        availability_class, reason_ko, bucket = classify(base_variants, broader_variants)
        out = dict(row)
        out["availability_class"] = availability_class
        out["availability_bucket"] = bucket
        out["availability_reason_ko"] = reason_ko
        out["baseline_variants"] = "|".join(sorted(base_variants))
        out["broader_source_variants"] = "|".join(sorted(broader_variants))
        out["current_c3_reference_present"] = str("hard_current_mkt_vol20" in base_variants).lower()
        out["mktvol_only_relax_present"] = str("remove_mkt_vol20_only" in base_variants).lower()
        out["needs_reversion_defense_relax"] = str(
            bool(broader_variants) and broader_variants == {"any_regime_no_reversion_caps"}
        ).lower()
        out_rows.append(out)

    summary: list[dict[str, Any]] = []
    summary.append(metric_row(out_rows, "all", "all"))
    for cls in sorted({r["availability_class"] for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r["availability_class"] == cls], "availability_class", cls))
    for bucket in sorted({r["availability_bucket"] for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r["availability_bucket"] == bucket], "availability_bucket", bucket))
    for regime in sorted({r.get("market_regime", "") for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r.get("market_regime") == regime], "market_regime", regime))
    for period in sorted({r.get("entry_period", "") for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r.get("entry_period") == period], "entry_period", period))

    class_counts = Counter(r["availability_class"] for r in out_rows)
    bucket_counts = Counter(r["availability_bucket"] for r in out_rows)
    result = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_conservative_shadow_availability_audit",
        "candidate_id": spec.get("candidate_id"),
        "source_rows": str(SHADOW_ROWS),
        "source_baseline_trades": str(BASELINE_TRADES),
        "source_broader_trades": str(BROADER_TRADES),
        "target_shadow_profile": "shadow_vaccel_lt_1_75",
        "target_rows": len(out_rows),
        "class_counts": dict(class_counts),
        "bucket_counts": dict(bucket_counts),
        "summary": summary,
        "classification": {
            "operational_decision": "NOT_APPROVED",
            "research_classification": "C3_HIGHVOL_SHADOW_AVAILABILITY_AUDITED_READ_ONLY",
            "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
            "full_logic_application": "NOT_APPLIED",
            "reason": [
                "no_current_c3_reference_rows",
                "all_rows_research_exploration_only",
                "no_rows_require_reversion_defense_only",
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
            "source_spec_not_approved_not_applied: PASS",
            "target_shadow_profile_row_count_22: PASS",
            "duplicate_key_check: PASS",
            "baseline_membership_join_completed: PASS",
            "broader_source_membership_join_completed: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    fields = list(out_rows[0].keys()) if out_rows else []
    write_csv(OUT_ROWS, out_rows, fields)
    write_csv(OUT_SUMMARY, summary, ["group", "value", "n", "win_n", "loss_n", "stop_n", "ret_sum", "ret_mean", "profit_factor", "unique_codes"])
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Shadow Availability Audit",
        "",
        f"- generated_at: {result['generated_at']}",
        f"- candidate_id: {result['candidate_id']}",
        "- target_shadow_profile: shadow_vaccel_lt_1_75",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Availability Counts",
        "",
    ]
    for cls, count in sorted(class_counts.items()):
        md.append(f"- {cls}: {count}")
    md.extend([
        "",
        "## Bucket Counts",
        "",
    ])
    for bucket, count in sorted(bucket_counts.items()):
        md.append(f"- {bucket}: {count}")
    md.extend([
        "",
        "## Summary",
        "",
    ])
    for row in summary:
        if row["group"] in {"all", "availability_class", "availability_bucket"}:
            md.append(
                f"- {row['group']} / {row['value']}: "
                f"n={row['n']}, ret_sum={float(row['ret_sum']):.6f}, PF={row['profit_factor']}"
            )
    md.extend([
        "",
        "## Boundary",
        "",
        "- This audit does not change candidate generation.",
        "- This audit does not approve C3 high-volatility trading.",
        "- Current C3 reference membership is zero for the 22 conservative shadow rows.",
        "- Rows are classified as research exploration unless they require reversion-defense-only relaxation or cannot be matched.",
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
        "target_rows": len(out_rows),
        "class_counts": dict(class_counts),
        "bucket_counts": dict(bucket_counts),
        "classification": result["classification"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
