from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

EXACT_JSON = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_latest.json"
EXACT_SUMMARY = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_summary_latest.csv"

OUT_JSON = LOG_DIR / "c3_normal_strategy_design_checklist_latest.json"
OUT_CSV = LOG_DIR / "c3_normal_strategy_design_checklist_latest.csv"
OUT_MD = LOG_DIR / "c3_normal_strategy_design_checklist_latest.md"

STATUS = "READ_ONLY_C3_NORMAL_STRATEGY_DESIGN_CHECKLIST_NOT_OPERATIONAL"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summary_row(summary: pd.DataFrame, tier: str, repair_variant: str) -> dict[str, Any]:
    mask = (
        summary["sample_mode"].astype(str).eq("dedup_trade_key")
        & summary["source_variant"].astype(str).eq("__ALL_SOURCE_VARIANTS__")
        & summary["tier"].astype(str).eq(tier)
        & summary["repair_variant"].astype(str).eq(repair_variant)
    )
    rows = summary.loc[mask].to_dict(orient="records")
    return rows[0] if rows else {}


def _float(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key))
    except Exception:
        return 0.0


def _delta(summary: pd.DataFrame, tier: str, variant: str) -> float:
    base = _summary_row(summary, tier, "baseline")
    var = _summary_row(summary, tier, variant)
    return round(_float(var, "ret_sum") - _float(base, "ret_sum"), 6)


def main() -> int:
    exact = _read_json(EXACT_JSON)
    summary = pd.read_csv(EXACT_SUMMARY)

    repair_variant = "profit_then_stop_repair_exact_entry_5pct_50pct"
    broad_tp_variant = "partial_tp_5pct_50pct_all_exact_entry"

    tiers = ["exact_c3_after_recheck", "c3_like_score080_follow1_v080", "all_cross_rows"]
    evidence = []
    for tier in tiers:
        base = _summary_row(summary, tier, "baseline")
        repair = _summary_row(summary, tier, repair_variant)
        broad = _summary_row(summary, tier, broad_tp_variant)
        evidence.append(
            {
                "tier": tier,
                "n": int(base.get("n", 0) or 0),
                "baseline_ret_sum": base.get("ret_sum", ""),
                "baseline_pf": base.get("profit_factor", ""),
                "repair_ret_sum": repair.get("ret_sum", ""),
                "repair_pf": repair.get("profit_factor", ""),
                "repair_delta_ret_sum": _delta(summary, tier, repair_variant),
                "repair_hit_n": int(repair.get("repair_hit_n", 0) or 0),
                "broad_tp_ret_sum": broad.get("ret_sum", ""),
                "broad_tp_pf": broad.get("profit_factor", ""),
                "broad_tp_delta_ret_sum": _delta(summary, tier, broad_tp_variant),
                "broad_tp_hit_n": int(broad.get("tp_hit_n", 0) or 0),
            }
        )

    checklist = [
        {
            "area": "strategy_family",
            "decision": "ADOPT_AS_NORMAL_RESEARCH_STRATEGY_FAMILY",
            "implementation_route": "normal_daily_adapter",
            "evidence": "C3/exact-entry tiers keep positive baseline and improve with targeted exit repair.",
            "apply_now": False,
            "reason": "Research evidence only; not wired to official candidate/order path.",
        },
        {
            "area": "surge_boundary",
            "decision": "KEEP_SEPARATE_FROM_REALTIME_SURGE",
            "implementation_route": "do_not_use_surge_inject_or_lob_orderflow",
            "evidence": "C3 rows come from daily report entry-trigger research artifacts, not realtime surge snapshots or LOB/orderflow.",
            "apply_now": False,
            "reason": "급등 로직은 기회 포착/실시간 구조이고 C3는 일봉 전환-눌림-재가속 구조.",
        },
        {
            "area": "candidate_selection",
            "decision": "NEEDS_RESEARCH_ADAPTER_BEFORE_OPERATIONAL_USE",
            "implementation_route": "add_c3_candidate_adapter_or_contract_layer",
            "evidence": "generate_candidates keeps score/v_accel/market_regime columns, while C3 requires followthrough/entry-trigger family contract.",
            "apply_now": False,
            "reason": "기존 후보 생성기에 억지로 조건을 섞으면 일반/급등/연구 기준이 다시 혼합됨.",
        },
        {
            "area": "entry_model",
            "decision": "REUSE_REPORT_ENTRY_TRIGGER_FOR_RESEARCH_REPLAY",
            "implementation_route": "breakout_prior_high_entry_trigger",
            "evidence": "report_backtest emits entry_px, entry_trigger_px, entry_trigger_reason; 269/269 rows matched exact-entry replay.",
            "apply_now": False,
            "reason": "공식 운영 진입으로 쓰려면 후보 생성 시점과 주문 가능 시점 계약을 별도로 맞춰야 함.",
        },
        {
            "area": "exit_model",
            "decision": "PROMOTE_TO_NEXT_OFFICIAL_REPLAY_TEST",
            "implementation_route": "targeted_profit_then_stop_repair_only",
            "evidence": "Exact-entry repair improved ret_sum in exact C3, C3-like, and all-cross headline tiers.",
            "apply_now": False,
            "reason": "방향성은 유지됐지만 공식 백테스트 재실행과 비용/체결 순서 검증 전 운영 적용 금지.",
        },
        {
            "area": "broad_partial_take_profit",
            "decision": "REJECT_AS_GENERAL_RULE",
            "implementation_route": "do_not_apply_to_all_c3_rows",
            "evidence": "Exact-entry broad 5% partial TP reduced ret_sum in all headline tiers.",
            "apply_now": False,
            "reason": "승자 수익을 너무 일찍 잘라 전체 수익합을 낮춤.",
        },
        {
            "area": "exclusion_rules",
            "decision": "DEFER",
            "implementation_route": "do_not_add_new_hard_exclusion_from_11_row_c3_only",
            "evidence": "Earlier exclusion-like variants improved by removing very few rows and risk overfit.",
            "apply_now": False,
            "reason": "사용자 목적은 매수 가능 구조 찾기이며, 작은 표본 차단 규칙은 다시 후보 없음 구조를 만들 수 있음.",
        },
        {
            "area": "capital_and_route",
            "decision": "REQUIRES_SEPARATE_BUDGET_BUCKET_DESIGN",
            "implementation_route": "normal_c3_research_bucket_before_paper",
            "evidence": "paper_engine has separate normal candidate and surge paths; C3 should not consume surge-specific slots.",
            "apply_now": False,
            "reason": "기존 일반/급등 자금·슬롯 구조와 섞으면 어느 방식이 개선됐는지 검증 불가.",
        },
        {
            "area": "next_validation",
            "decision": "RUN_SCOPED_OFFICIAL_REPLAY_BEFORE_CODE_INTEGRATION",
            "implementation_route": "official_backtest_replay_with_c3_contract_and_targeted_exit_repair",
            "evidence": "Read-only exact-entry replay is positive but not an official backtest rerun.",
            "apply_now": False,
            "reason": "운영 적용 전 공식 후보-진입-청산-비용 경로에서 같은 방향인지 확인 필요.",
        },
    ]

    out_df = pd.DataFrame(checklist)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": STATUS,
        "source_files": {
            "exact_json": str(EXACT_JSON),
            "exact_summary": str(EXACT_SUMMARY),
        },
        "source_status": exact.get("status"),
        "source_rows": exact.get("source_rows"),
        "matched_trade_rows": exact.get("matched_trade_rows"),
        "missing_trade_rows": exact.get("missing_trade_rows"),
        "path_available_rows": exact.get("path_available_rows"),
        "path_missing_rows": exact.get("path_missing_rows"),
        "evidence_summary": evidence,
        "checklist": checklist,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "markdown": str(OUT_MD),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 normal strategy design checklist",
        "",
        f"- status: `{STATUS}`",
        f"- source status: `{exact.get('status')}`",
        f"- source rows: {exact.get('source_rows')}",
        f"- matched trade rows: {exact.get('matched_trade_rows')}",
        f"- full logic application: `NOT_APPLIED`",
        "",
        "## Evidence summary",
        "",
    ]
    for row in evidence:
        lines.append(
            "- "
            f"{row['tier']}: n={row['n']}, "
            f"baseline={row['baseline_ret_sum']} PF={row['baseline_pf']}, "
            f"targeted_repair={row['repair_ret_sum']} delta={row['repair_delta_ret_sum']} PF={row['repair_pf']}, "
            f"broad_tp={row['broad_tp_ret_sum']} delta={row['broad_tp_delta_ret_sum']}"
        )
    lines.extend(["", "## Checklist", ""])
    for item in checklist:
        lines.append(
            "- "
            f"{item['area']}: {item['decision']} / route={item['implementation_route']} / "
            f"apply_now={str(item['apply_now']).lower()} / reason={item['reason']}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is a design artifact only.",
            "- It does not change candidates, backtests, HPO, paper trading, broker routing, gates, policy, or capital allocation.",
            "- C3 is classified here as a normal daily strategy family, not realtime surge.",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "status": STATUS,
                "checklist_rows": len(checklist),
                "evidence_rows": len(evidence),
                "json": str(OUT_JSON),
                "csv": str(OUT_CSV),
                "markdown": str(OUT_MD),
                "full_logic_application": "NOT_APPLIED",
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
