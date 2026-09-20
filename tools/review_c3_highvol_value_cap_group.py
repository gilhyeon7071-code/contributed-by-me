from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

AUDIT_ROWS = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
OFFICIAL_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_value_cap_group_review_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_value_cap_group_review_rows_latest.csv"
OUT_SUMMARY = LOG_DIR / "c3_highvol_value_cap_group_review_summary_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_value_cap_group_review_latest.md"


TARGET_CLASS = "EXPLORATION_VALUE_CAP_REVIEW_REQUIRED"


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


def value_bucket(value: float, cap: float) -> str:
    if value <= cap:
        return "at_or_below_current_cap"
    if value < 3_000_000_000:
        return "just_above_2b_to_3b"
    if value < 20_000_000_000:
        return "mid_3b_to_20b"
    if value < 100_000_000_000:
        return "large_20b_to_100b"
    return "mega_100b_plus"


def ret_bucket(ret: float) -> str:
    if ret <= -0.06:
        return "stop_like_loss"
    if ret < 0:
        return "small_loss"
    if ret < 0.05:
        return "small_win_0_to_5pct"
    if ret < 0.15:
        return "mid_win_5_to_15pct"
    return "large_win_15pct_plus"


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
    values = [as_float(r.get("signal_value")) for r in data]
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
        "min_signal_value": min(values) if values else "NA",
        "max_signal_value": max(values) if values else "NA",
        "unique_codes": len({str(r.get("code", "")) for r in data if str(r.get("code", ""))}),
    }


def top_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rets = sorted([as_float(r.get("ret")) for r in rows if as_float(r.get("ret")) > 0], reverse=True)
    all_rets = [as_float(r.get("ret")) for r in rows]
    gross_profit = sum(rets)
    top3 = sum(rets[:3])
    top5 = sum(rets[:5])
    return {
        "gross_profit": gross_profit,
        "top1_ret": sum(rets[:1]),
        "top3_ret": top3,
        "top5_ret": top5,
        "top3_share_gross_profit": (top3 / gross_profit) if gross_profit else "NA",
        "top5_share_gross_profit": (top5 / gross_profit) if gross_profit else "NA",
        "ret_without_top3": sum(all_rets) - top3,
        "ret_without_top5": sum(all_rets) - top5,
    }


def current_signal_value_cap(contract: dict[str, Any]) -> float:
    payload = contract.get("selection_contract", contract)
    for item in payload.get("numeric_filters", []):
        if item.get("field") == "signal_value" and item.get("op") == "<=":
            return float(item["value"])
    raise ValueError("signal_value <= cap not found in official contract")


def main() -> None:
    for path in [AUDIT_ROWS, OFFICIAL_CONTRACT]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    cap = current_signal_value_cap(load_json(OFFICIAL_CONTRACT))
    source_rows = [row for row in read_csv(AUDIT_ROWS) if row.get("availability_class") == TARGET_CLASS]
    if len(source_rows) != 14:
        raise ValueError(f"expected 14 value-cap review rows, got {len(source_rows)}")

    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        value = as_float(row.get("signal_value"))
        ret = as_float(row.get("ret"))
        out = dict(row)
        out["official_signal_value_cap"] = cap
        out["value_over_cap"] = value - cap
        out["value_over_cap_ratio"] = value / cap if cap else "NA"
        out["value_bucket"] = value_bucket(value, cap)
        out["ret_bucket"] = ret_bucket(ret)
        out_rows.append(out)

    if any(as_float(row.get("signal_value")) <= cap for row in out_rows):
        raise ValueError("value-cap review row at or below current cap")

    summary: list[dict[str, Any]] = [metric_row(out_rows, "all", "all")]
    for bucket in ["just_above_2b_to_3b", "mid_3b_to_20b", "large_20b_to_100b", "mega_100b_plus"]:
        summary.append(metric_row([r for r in out_rows if r["value_bucket"] == bucket], "value_bucket", bucket))
    for regime in sorted({r.get("market_regime", "") for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r.get("market_regime") == regime], "market_regime", regime))
    for period in sorted({r.get("entry_period", "") for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r.get("entry_period") == period], "entry_period", period))
    for bucket in sorted({r.get("ret_bucket", "") for r in out_rows}):
        summary.append(metric_row([r for r in out_rows if r.get("ret_bucket") == bucket], "ret_bucket", bucket))

    concentration = top_concentration(out_rows)
    bucket_counts = Counter(r["value_bucket"] for r in out_rows)
    ret_bucket_counts = Counter(r["ret_bucket"] for r in out_rows)

    classification_reason = [
        "all_rows_above_current_2b_value_cap",
        "positive_as_research_group",
        "contains_megacap_and_mid_value_rows",
        "small_sample_not_operational",
    ]
    if concentration["ret_without_top5"] <= 0:
        classification_reason.append("top5_dependent")
    else:
        classification_reason.append("ret_without_top5_positive")

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "read_only_highvol_value_cap_group_review",
        "source_rows": str(AUDIT_ROWS),
        "official_contract": str(OFFICIAL_CONTRACT),
        "target_availability_class": TARGET_CLASS,
        "official_signal_value_cap": cap,
        "target_rows": len(out_rows),
        "value_bucket_counts": dict(bucket_counts),
        "ret_bucket_counts": dict(ret_bucket_counts),
        "summary": summary,
        "concentration": concentration,
        "classification": {
            "operational_decision": "NOT_APPROVED",
            "research_classification": "C3_HIGHVOL_VALUE_CAP_REVIEW_GROUP_POSITIVE_BUT_SMALL",
            "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
            "full_logic_application": "NOT_APPLIED",
            "reason": classification_reason,
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
            "target_value_cap_rows_14: PASS",
            "all_rows_above_official_cap: PASS",
            "summary_recomputed_from_rows: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    write_csv(OUT_ROWS, out_rows, list(out_rows[0].keys()))
    write_csv(
        OUT_SUMMARY,
        summary,
        [
            "group", "value", "n", "win_n", "loss_n", "stop_n",
            "ret_sum", "ret_mean", "profit_factor", "min_signal_value",
            "max_signal_value", "unique_codes",
        ],
    )
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    md = [
        "# C3 High-Volatility Value Cap Group Review",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- official_signal_value_cap: {cap:.0f}",
        f"- target_rows: {len(out_rows)}",
        "- operational_decision: NOT_APPROVED",
        "- full_logic_application: NOT_APPLIED",
        "",
        "## Value Buckets",
        "",
    ]
    for row in summary:
        if row["group"] in {"all", "value_bucket"}:
            md.append(
                f"- {row['group']} / {row['value']}: n={row['n']}, "
                f"ret_sum={float(row['ret_sum']):.6f}, PF={row['profit_factor']}, "
                f"value_range={row['min_signal_value']}~{row['max_signal_value']}"
            )
    md.extend([
        "",
        "## Concentration",
        "",
        f"- top3_share_gross_profit: {float(concentration['top3_share_gross_profit']) * 100:.2f}%",
        f"- top5_share_gross_profit: {float(concentration['top5_share_gross_profit']) * 100:.2f}%",
        f"- ret_without_top3: {float(concentration['ret_without_top3']):.6f}",
        f"- ret_without_top5: {float(concentration['ret_without_top5']):.6f}",
        "",
        "## Boundary",
        "",
        "- This review does not approve value-cap relaxation.",
        "- This review does not change candidate generation, gates, HPO, paper, or broker paths.",
        "- The result asks whether high-volatility strategy should have a separate value/liquidity rule, not whether current C3 should be broadly relaxed.",
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
        "official_signal_value_cap": cap,
        "value_bucket_counts": dict(bucket_counts),
        "classification": payload["classification"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
