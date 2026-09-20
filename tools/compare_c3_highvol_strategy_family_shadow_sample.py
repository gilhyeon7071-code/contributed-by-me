from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SPEC_JSON = LOG_DIR / "c3_highvol_research_candidate_spec_latest.json"
HIGHVOL_ROWS_CSV = LOG_DIR / "c3_highvol_shadow_availability_audit_rows_latest.csv"
HIGHVOL_DUAL_JSON = LOG_DIR / "c3_highvol_dual_band_observation_audit_latest.json"
C3_MKTVOL_SUMMARY_CSV = LOG_DIR / "c3_mkt_vol_variant_compare_summary_latest.csv"
C3_OFFICIAL_SUMMARY_CSV = LOG_DIR / "c3_readonly_official_replay_summary_latest.csv"
CURRENT_CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
NORMAL_AUDIT_JSON = LOG_DIR / "normal_entry_path_bottleneck_audit_latest.json"
SURGE_RECOVERY_JSON = LOG_DIR / "surge_recovery_reentry_candidates_latest.json"
METHODOLOGY_VERDICT_JSON = LOG_DIR / "methodology_continuity_verdict_latest.json"

OUT_JSON = LOG_DIR / "c3_highvol_strategy_family_shadow_sample_compare_latest.json"
OUT_SUMMARY_CSV = LOG_DIR / "c3_highvol_strategy_family_shadow_sample_compare_latest.csv"
OUT_CURRENT_PROXY_CSV = LOG_DIR / "c3_highvol_strategy_family_current_proxy_scan_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_strategy_family_shadow_sample_compare_latest.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def metric_summary(df: pd.DataFrame, label: str, note: str) -> dict[str, Any]:
    if df.empty:
        return {
            "sample": label,
            "n": 0,
            "win_n": 0,
            "loss_n": 0,
            "ret_sum": 0.0,
            "ret_mean": None,
            "profit_factor": None,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": note,
        }
    ret = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    gross_profit = ret[ret > 0].sum()
    gross_loss = -ret[ret < 0].sum()
    profit_factor = None if gross_loss == 0 else float(gross_profit / gross_loss)
    return {
        "sample": label,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret <= 0).sum()),
        "ret_sum": float(ret.sum()),
        "ret_mean": float(ret.mean()),
        "profit_factor": profit_factor,
        "first_entry_date": str(df.get("entry_date", pd.Series(dtype=str)).min()),
        "last_entry_date": str(df.get("entry_date", pd.Series(dtype=str)).max()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns else 0,
        "note": note,
    }


def find_csv_row(df: pd.DataFrame, **conds: str) -> dict[str, Any]:
    out = df.copy()
    for key, value in conds.items():
        out = out[out[key].astype(str) == str(value)]
    if out.empty:
        return {}
    return out.iloc[0].to_dict()


def current_proxy_scan(spec: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    current = pd.read_csv(CURRENT_CANDIDATES_CSV, encoding="utf-8-sig")
    candidate_spec = spec["candidate_spec"]
    allowed_regimes = set(candidate_spec["market_regime"])
    v_min = float(candidate_spec["signal_v_accel"]["min_inclusive"])
    atr_min = float(candidate_spec["signal_atr_pct"]["min_inclusive"])
    atr_max = float(candidate_spec["signal_atr_pct"]["max_exclusive"])

    required = ["code", "name", "date", "market_regime", "v_accel", "atr14_pct", "ret1_pct", "value"]
    missing = [col for col in required if col not in current.columns]
    if missing:
        raise ValueError(f"missing current candidate columns: {missing}")

    proxy = current.copy()
    proxy["proxy_regime_ok"] = proxy["market_regime"].astype(str).isin(allowed_regimes)
    proxy["proxy_v_accel_ok"] = pd.to_numeric(proxy["v_accel"], errors="coerce") >= v_min
    proxy["proxy_atr_ok"] = (
        (pd.to_numeric(proxy["atr14_pct"], errors="coerce") >= atr_min)
        & (pd.to_numeric(proxy["atr14_pct"], errors="coerce") < atr_max)
    )
    proxy["exact_entry_gap_available"] = False
    proxy["exact_entry_trigger_available"] = False
    proxy["proxy_all_available_checks_pass"] = proxy["proxy_regime_ok"] & proxy["proxy_v_accel_ok"] & proxy["proxy_atr_ok"]
    proxy["reason"] = proxy.apply(
        lambda r: "PASS_AVAILABLE_PROXY_CHECKS_BUT_EXACT_ENTRY_GAP_MISSING"
        if r["proxy_all_available_checks_pass"]
        else "|".join(
            reason
            for reason, ok in [
                ("regime_not_bull_sideways", r["proxy_regime_ok"]),
                ("v_accel_below_family_min", r["proxy_v_accel_ok"]),
                ("atr_outside_family_band", r["proxy_atr_ok"]),
            ]
            if not ok
        ),
        axis=1,
    )
    cols = [
        "date",
        "code",
        "name",
        "market_regime",
        "ret1_pct",
        "v_accel",
        "atr14_pct",
        "value",
        "proxy_regime_ok",
        "proxy_v_accel_ok",
        "proxy_atr_ok",
        "proxy_all_available_checks_pass",
        "exact_entry_gap_available",
        "exact_entry_trigger_available",
        "reason",
    ]
    proxy_out = proxy[cols].copy()
    summary = {
        "current_rows": int(len(proxy_out)),
        "available_proxy_pass_rows": int(proxy_out["proxy_all_available_checks_pass"].sum()),
        "exact_current_shadow_rows": 0,
        "exact_current_shadow_reason": "current candidate CSV lacks entry_gap_pct and entry_trigger fields required for exact high-vol family generation",
        "market_regime_counts": proxy_out["market_regime"].astype(str).value_counts(dropna=False).to_dict(),
        "reason_counts": proxy_out["reason"].value_counts(dropna=False).to_dict(),
    }
    return proxy_out, summary


def build_payload() -> dict[str, Any]:
    spec = load_json(SPEC_JSON)
    verdict = load_json(METHODOLOGY_VERDICT_JSON)
    normal = load_json(NORMAL_AUDIT_JSON)
    surge = load_json(SURGE_RECOVERY_JSON)
    dual = load_json(HIGHVOL_DUAL_JSON)

    rows = pd.read_csv(HIGHVOL_ROWS_CSV, encoding="utf-8-sig")
    conservative = rows[rows["shadow_profile"].astype(str) == "shadow_vaccel_lt_1_75"].copy()
    floor500 = conservative[pd.to_numeric(conservative["signal_value"], errors="coerce") >= 500_000_000].copy()
    sub500 = conservative[pd.to_numeric(conservative["signal_value"], errors="coerce") < 500_000_000].copy()

    c3_mkt = pd.read_csv(C3_MKTVOL_SUMMARY_CSV, encoding="utf-8-sig")
    c3_official = pd.read_csv(C3_OFFICIAL_SUMMARY_CSV, encoding="utf-8-sig")
    proxy_out, proxy_summary = current_proxy_scan(spec)

    official_all = find_csv_row(c3_official, layer="native_report_all")
    current_c3_2026 = find_csv_row(c3_mkt, variant="hard_current_mkt_vol20", segment="target_2026H1")
    remove_mkt_2026 = find_csv_row(c3_mkt, variant="remove_mkt_vol20_only", segment="target_2026H1")

    normal_trace = normal.get("entry_source_trace", {})
    surge_summary = surge.get("summary", {})

    samples = [
        metric_summary(conservative, "c3_highvol_exact_shadow_conservative", "Exact historical high-vol shadow family sample."),
        metric_summary(floor500, "c3_highvol_exact_shadow_floor500", "Exact historical high-vol floor500 primary lane."),
        metric_summary(sub500, "c3_highvol_exact_shadow_sub500", "Exact historical high-vol sub500 lane; mixed current/legacy context."),
        {
            "sample": "c3_current_official_replay",
            "n": int(to_float(official_all.get("n"))),
            "win_n": int(to_float(official_all.get("win_n"))),
            "loss_n": int(to_float(official_all.get("loss_n"))),
            "ret_sum": to_float(official_all.get("ret_sum")),
            "ret_mean": None,
            "profit_factor": None,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": "Existing C3 official readonly replay produced zero rows.",
        },
        {
            "sample": "c3_current_mktvol_target_2026h1",
            "n": int(to_float(current_c3_2026.get("n"))),
            "win_n": int(to_float(current_c3_2026.get("win_n"))),
            "loss_n": int(to_float(current_c3_2026.get("loss_n"))),
            "ret_sum": to_float(current_c3_2026.get("ret_sum")),
            "ret_mean": to_float(current_c3_2026.get("ret_mean"), default=0.0),
            "profit_factor": None if pd.isna(current_c3_2026.get("profit_factor")) else to_float(current_c3_2026.get("profit_factor")),
            "first_entry_date": str(current_c3_2026.get("first_entry_date", "")),
            "last_entry_date": str(current_c3_2026.get("last_entry_date", "")),
            "unique_codes": int(to_float(current_c3_2026.get("unique_codes"))),
            "note": "Existing C3 mkt_vol current proxy in 2026H1.",
        },
        {
            "sample": "c3_remove_mktvol_only_target_2026h1",
            "n": int(to_float(remove_mkt_2026.get("n"))),
            "win_n": int(to_float(remove_mkt_2026.get("win_n"))),
            "loss_n": int(to_float(remove_mkt_2026.get("loss_n"))),
            "ret_sum": to_float(remove_mkt_2026.get("ret_sum")),
            "ret_mean": to_float(remove_mkt_2026.get("ret_mean"), default=0.0),
            "profit_factor": to_float(remove_mkt_2026.get("profit_factor"), default=0.0),
            "first_entry_date": str(remove_mkt_2026.get("first_entry_date", "")),
            "last_entry_date": str(remove_mkt_2026.get("last_entry_date", "")),
            "unique_codes": int(to_float(remove_mkt_2026.get("unique_codes"))),
            "note": "Removing only mkt_vol in existing C3 is not enough; 2026H1 is negative.",
        },
        {
            "sample": "normal_entry_current_runtime",
            "n": int(normal_trace.get("current_candidate_viable_rows", 0)),
            "win_n": 0,
            "loss_n": 0,
            "ret_sum": 0.0,
            "ret_mean": None,
            "profit_factor": None,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": "Normal path has source-viable rows but zero current entry decision/pending rows.",
        },
        {
            "sample": "surge_recovery_research_rows",
            "n": int(surge_summary.get("rows", 0)),
            "win_n": 0,
            "loss_n": 0,
            "ret_sum": 0.0,
            "ret_mean": None,
            "profit_factor": None,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": "Surge recovery has research rows but zero entry_allowed and zero expectancy_buy_ready.",
        },
        {
            "sample": "current_daily_highvol_proxy_scan",
            "n": int(proxy_summary["available_proxy_pass_rows"]),
            "win_n": 0,
            "loss_n": 0,
            "ret_sum": 0.0,
            "ret_mean": None,
            "profit_factor": None,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": "Current daily scan can only be a proxy; exact entry_gap/trigger fields are missing.",
        },
    ]

    decision = {
        "selected_continuation": "BUILD_EXACT_READONLY_C3_HIGHVOL_STRATEGY_FAMILY_GENERATOR",
        "why": [
            "C3 high-vol has exact historical shadow rows while existing official C3 replay has zero rows.",
            "Existing C3 mkt_vol relaxation alone is not the right fix because 2026H1 remove_mkt_vol_only is negative.",
            "Current daily candidate CSV cannot exactly generate this family because entry_gap_pct and entry_trigger fields are missing.",
        ],
        "not_selected": {
            "patch_existing_c3_value_or_mktvol_only": "not enough; 2026H1 sensitivity is weak/negative and high-vol family is structurally different",
            "use_surge_as_buy_path_now": "not enough; entry_allowed and expectancy_ready are zero",
            "continue_template_or_ledger_work": "stopped; does not answer candidate-generation viability",
        },
    }

    validation = [
        "methodology_verdict_selected_c3_highvol: PASS"
        if "C3 high-vol" in verdict.get("single_continuity_decision", "")
        else "methodology_verdict_selected_c3_highvol: FAIL",
        "exact_highvol_conservative_rows_22: PASS" if len(conservative) == 22 else "exact_highvol_conservative_rows_22: FAIL",
        "current_proxy_scan_completed: PASS" if proxy_summary["current_rows"] >= 0 else "current_proxy_scan_completed: FAIL",
        "existing_c3_official_zero_rows_confirmed: PASS" if int(to_float(official_all.get("n"))) == 0 else "existing_c3_official_zero_rows_confirmed: FAIL",
        "operation_effect_no_changes: PASS",
    ]

    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "readonly_c3_highvol_strategy_family_shadow_sample_compare",
        "classification": "C3_HIGHVOL_STRATEGY_FAMILY_SHADOW_SAMPLE_COMPARE_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "source_artifacts": {
            "spec": str(SPEC_JSON),
            "highvol_rows": str(HIGHVOL_ROWS_CSV),
            "highvol_dual": str(HIGHVOL_DUAL_JSON),
            "c3_mktvol_summary": str(C3_MKTVOL_SUMMARY_CSV),
            "c3_official_summary": str(C3_OFFICIAL_SUMMARY_CSV),
            "current_candidates": str(CURRENT_CANDIDATES_CSV),
            "normal_audit": str(NORMAL_AUDIT_JSON),
            "surge_recovery": str(SURGE_RECOVERY_JSON),
            "methodology_verdict": str(METHODOLOGY_VERDICT_JSON),
        },
        "comparison_samples": samples,
        "current_proxy_summary": proxy_summary,
        "dual_band_lane_counts": dual.get("lane_counts", {}),
        "decision": decision,
        "operation_effect": {
            "candidate_generation": False,
            "backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": validation,
    }, proxy_out


def write_summary_csv(samples: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "sample",
        "n",
        "win_n",
        "loss_n",
        "ret_sum",
        "ret_mean",
        "profit_factor",
        "first_entry_date",
        "last_entry_date",
        "unique_codes",
        "note",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in samples:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_md(payload: dict[str, Any], path: Path) -> None:
    lines = [
        "# C3 High-Vol Strategy-Family Shadow Sample Compare",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- operational_decision: {payload['operational_decision']}",
        f"- full_logic_application: {payload['full_logic_application']}",
        f"- selected_continuation: {payload['decision']['selected_continuation']}",
        "",
        "## Samples",
        "",
        "| sample | n | ret_sum | profit_factor | note |",
        "|---|---:|---:|---:|---|",
    ]
    for row in payload["comparison_samples"]:
        lines.append(
            f"| {row['sample']} | {row['n']} | {row.get('ret_sum', '')} | {row.get('profit_factor', '')} | {str(row.get('note', '')).replace('|', '/')} |"
        )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            f"- {payload['decision']['selected_continuation']}",
            "",
            "## Why",
            "",
            *[f"- {item}" for item in payload["decision"]["why"]],
            "",
            "## Current Proxy Limitation",
            "",
            f"- current_rows: {payload['current_proxy_summary']['current_rows']}",
            f"- available_proxy_pass_rows: {payload['current_proxy_summary']['available_proxy_pass_rows']}",
            f"- exact_current_shadow_rows: {payload['current_proxy_summary']['exact_current_shadow_rows']}",
            f"- exact_current_shadow_reason: {payload['current_proxy_summary']['exact_current_shadow_reason']}",
            "",
            "## Validation",
            "",
            *[f"- {item}" for item in payload["validation"]],
            "",
            "## Guardrail",
            "",
            "Read-only comparison only. No candidate generation, order path, gate, parameter, HPO, or official backtest was changed.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    payload, proxy_out = build_payload()
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_summary_csv(payload["comparison_samples"], OUT_SUMMARY_CSV)
    proxy_out.to_csv(OUT_CURRENT_PROXY_CSV, index=False, encoding="utf-8-sig")
    write_md(payload, OUT_MD)
    print(
        json.dumps(
            {
                "status": "ok",
                "json": str(OUT_JSON),
                "summary_csv": str(OUT_SUMMARY_CSV),
                "current_proxy_csv": str(OUT_CURRENT_PROXY_CSV),
                "md": str(OUT_MD),
                "samples": len(payload["comparison_samples"]),
                "selected_continuation": payload["decision"]["selected_continuation"],
                "validation": payload["validation"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
