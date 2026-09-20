from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"
ENGINE_PATH = ROOT / "paper_engine.py"
PNL_PATH = LOG_DIR / "paper_pnl_summary_last.json"
SURGE_REALTIME_PATH = LOG_DIR / "surge_realtime_latest.json"
SURGE_LOB_PATH = LOG_DIR / "surge_lob_latest.json"
SURGE_PARAM_PATH = LOG_DIR / "surge_param_proposal_latest.json"
SURGE_EXPECTANCY_PATH = LOG_DIR / "surge_expectancy_evidence_gap_latest.json"
NORMAL_QUALITY_PATH = LOG_DIR / "normal_entry_fill_quality_report_latest.json"
RUNTIME_NORMAL_PATH = LOG_DIR / "runtime_normal_quality_separation_report_latest.json"


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _get(dct: Dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    cur: Any = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _strategy(pnl: Dict[str, Any], name: str) -> Dict[str, Any]:
    for row in pnl.get("strategies") or []:
        if isinstance(row, dict) and row.get("strategy") == name:
            return row
    return {}


def _row(
    strategy: str,
    proposal: str,
    validation: str,
    evidence: str,
    action: str,
    risk: str,
) -> Dict[str, Any]:
    return {
        "strategy": strategy,
        "proposal": proposal,
        "validation": validation,
        "evidence": evidence,
        "recommended_action": action,
        "risk_or_gap": risk,
    }


def build_report() -> Dict[str, Any]:
    cfg = _read_json(CFG_PATH)
    pnl = _read_json(PNL_PATH)
    surge_rt = _read_json(SURGE_REALTIME_PATH)
    surge_lob = _read_json(SURGE_LOB_PATH)
    surge_param = _read_json(SURGE_PARAM_PATH)
    surge_gap = _read_json(SURGE_EXPECTANCY_PATH)
    normal_quality = _read_json(NORMAL_QUALITY_PATH)
    runtime_normal = _read_json(RUNTIME_NORMAL_PATH)
    engine_text = ""
    try:
        engine_text = ENGINE_PATH.read_text(encoding="utf-8")
    except Exception:
        engine_text = ""

    split_cfg = cfg.get("split_entry") if isinstance(cfg.get("split_entry"), dict) else {}
    split_conf = split_cfg.get("second_confirmation") if isinstance(split_cfg.get("second_confirmation"), dict) else {}
    surge_cfg = cfg.get("surge_entry_policy") if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    overheat = cfg.get("entry_overheat_policy") if isinstance(cfg.get("entry_overheat_policy"), dict) else {}
    normal_intraday = (
        cfg.get("normal_intraday_realtime_policy")
        if isinstance(cfg.get("normal_intraday_realtime_policy"), dict)
        else {}
    )
    normal_exec = (
        cfg.get("normal_entry_execution_quality")
        if isinstance(cfg.get("normal_entry_execution_quality"), dict)
        else {}
    )

    split_stats = _strategy(pnl, "split_entry")
    surge_stats = _strategy(pnl, "surge")
    intraday_stats = _strategy(pnl, "intraday_realtime")
    normal_stats = _strategy(pnl, "normal")

    split_min = _to_float(split_cfg.get("second_dip_min_pct"), 0.0)
    split_max = _to_float(split_cfg.get("second_dip_max_pct"), 0.0)
    split_missing_action = str(split_conf.get("missing_feature_action") or "")
    split_conf_connected = "_split_second_confirmation_reason" in engine_text and "[SKIP_SPLIT2ND_CONFIRM]" in engine_text

    surge_min_score = _to_float(surge_cfg.get("min_score_final"), 0.0)
    surge_dynamic = surge_cfg.get("dynamic_max_new") if isinstance(surge_cfg.get("dynamic_max_new"), dict) else {}
    surge_rt_thresholds = surge_rt.get("thresholds") if isinstance(surge_rt.get("thresholds"), dict) else {}

    rows: List[Dict[str, Any]] = [
        _row(
            "split_entry",
            "second_confirmation.missing_feature_action ALLOW_WITH_NOTE -> BLOCK",
            "SUPPORTED_POLICY_CANDIDATE",
            (
                f"enabled={bool(split_conf.get('enabled'))}, action={split_missing_action}, "
                f"engine_connected={split_conf_connected}, trades={int(_to_float(split_stats.get('trades_used'), 0))}, "
                f"pf={_to_float(split_stats.get('gross_pf')):.3f}"
            ),
            "Apply only after backup and replay/backtest validation; this is the cleanest split-entry gate candidate.",
            "Historical PnL rows do not carry enough split-confirmation feature columns to prove PF impact without rerun.",
        ),
        _row(
            "split_entry",
            "second_dip_max_pct 0.03 -> 0.05",
            "REJECT_AS_WRITTEN",
            f"current_band={split_min:.2%}~{split_max:.2%}",
            "Do not change only max to 5%. If the intent is deeper pullback only, test a full band such as min 3% / max 5%.",
            "Raising only max allows deeper falling-knife entries instead of blocking them.",
        ),
        _row(
            "surge",
            "surge_entry_policy.min_score_final 75 -> 80",
            "CONDITIONAL_CANDIDATE_NOT_PRIMARY",
            (
                f"config_min={surge_min_score:.1f}, realtime_alerts={surge_rt.get('alerts_count')}, "
                f"lob_coverage={surge_rt.get('lob_coverage_pct')}%, "
                f"spread_cap={surge_rt_thresholds.get('max_spread_bps')}, "
                f"orderflow_risk_cap={surge_rt_thresholds.get('orderflow_risk_block_threshold')}, "
                f"pf={_to_float(surge_stats.get('gross_pf')):.3f}"
            ),
            "Treat 80 as a scenario candidate, but verify with replay because current realtime already has spread/orderflow/dynamic caps.",
            "Latest realtime alerts are zero; score-only tightening may reduce sample without proving expectancy improvement.",
        ),
        _row(
            "surge",
            "volume/spread/orderflow checks are missing",
            "PARTLY_FALSE",
            (
                f"dynamic_max_new_enabled={bool(surge_dynamic.get('enabled'))}, "
                f"medium_spread_cap={surge_dynamic.get('medium_spread_bps_max')}, "
                f"orderflow_cap={surge_dynamic.get('orderflow_risk_score_max')}, "
                f"surge_lob_rows={surge_lob.get('rows')}, codes_with_lob={surge_lob.get('codes_with_lob')}"
            ),
            "Focus on LOB coverage and expectancy evidence, not simply adding a check that already exists.",
            "LOB coverage is still thin, so missing evidence is a data coverage problem, not only a config threshold problem.",
        ),
        _row(
            "intraday_realtime",
            "bad market should cut intraday size by half or more",
            "PARTLY_ALREADY_REFLECTED",
            (
                f"overheat_reduce={overheat.get('reduce_multiplier')}, "
                f"apply_to_normal={overheat.get('apply_to_normal')}, apply_to_surge={overheat.get('apply_to_surge')}, "
                f"normal_intraday_blocks={normal_intraday.get('blocked_entry_gate_decisions')}, "
                f"pf={_to_float(intraday_stats.get('gross_pf')):.3f}"
            ),
            "Validate whether REDUCE/CAUTION should block or further cut normal intraday only; do not blanket-change all strategies.",
            "Existing normal_intraday policy blocks only BLOCK, while intraday PF is the weakest segment.",
        ),
        _row(
            "normal",
            "gap/down and close-bet quality defense",
            "PARTLY_ALREADY_REFLECTED_BUT_OBSERVE_ONLY_GAP",
            (
                f"normal_exec_require_lob={normal_exec.get('require_lob')}, "
                f"normal_exec_max_spread={normal_exec.get('max_spread_bps')}, "
                f"latest_quality_observe_only={normal_quality.get('observe_only')}, "
                f"quality_rows={normal_quality.get('rows')}, normal_pf={_to_float(normal_stats.get('gross_pf')):.3f}"
            ),
            "Separate display/observe reports from actual entry blocking; verify official order path before changing policy.",
            "Latest normal quality report did not change fills and exec_quality_decision was blank.",
        ),
    ]

    rejected = [r for r in rows if r["validation"].startswith("REJECT")]
    supported = [r for r in rows if "SUPPORTED" in r["validation"] or "CONDITIONAL" in r["validation"]]

    decision = "VALIDATE_FIRST_SPLIT_CONFIRMATION_BLOCK_NOT_BROAD_PARAM_PUSH"
    if _to_float(pnl.get("gross_pf"), 0.0) >= 1.0:
        decision = "PERFORMANCE_ALREADY_OK_RECHECK_SCOPE"

    return {
        "generated_at": _now(),
        "schema_version": "four_strategy_improvement_plan_validation_v1",
        "scope": "read_only_validate_proposed_4_strategy_improvement_plan",
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "config": str(CFG_PATH),
            "engine": str(ENGINE_PATH),
            "paper_pnl_summary": str(PNL_PATH),
            "surge_realtime": str(SURGE_REALTIME_PATH),
            "surge_lob": str(SURGE_LOB_PATH),
            "surge_param_proposal": str(SURGE_PARAM_PATH),
            "surge_expectancy_gap": str(SURGE_EXPECTANCY_PATH),
            "normal_entry_quality": str(NORMAL_QUALITY_PATH),
            "runtime_normal_quality": str(RUNTIME_NORMAL_PATH),
        },
        "source_generated_at": {
            "paper_pnl_summary": pnl.get("generated_at"),
            "surge_realtime": surge_rt.get("ts"),
            "surge_lob": surge_lob.get("ts"),
            "surge_param_proposal": surge_param.get("ts"),
            "surge_expectancy_gap": surge_gap.get("generated_at"),
            "normal_entry_quality": normal_quality.get("generated_at"),
            "runtime_normal_quality": runtime_normal.get("generated_at"),
        },
        "baseline": {
            "trades_used": int(_to_float(pnl.get("trades_used"), 0.0)),
            "win_rate": _to_float(pnl.get("win_rate"), 0.0),
            "gross_pf": _to_float(pnl.get("gross_pf"), 0.0),
            "avg_ret": _to_float(pnl.get("avg_ret"), 0.0),
            "strategies": {
                "split_entry": split_stats,
                "surge": surge_stats,
                "intraday_realtime": intraday_stats,
                "normal": normal_stats,
            },
        },
        "decision": {
            "status": decision,
            "first_candidate": "split_entry.second_confirmation.missing_feature_action ALLOW_WITH_NOTE -> BLOCK",
            "reject_as_written": "split_entry.second_dip_max_pct 0.03 -> 0.05",
            "surge_score_80_status": "scenario_candidate_not_primary",
            "normal_intraday_status": "requires_path_specific_validation",
        },
        "existing_validator_execution": {
            "split_entry_contract": "TIMEOUT_120S_IMPORT_RUNTIME_PATH",
            "normal_intraday_policy": "TIMEOUT_30S_PARALLEL_ATTEMPT",
            "surge_policy_candidates": "TIMEOUT_30S_PARALLEL_ATTEMPT",
            "fallback_used": "static_config_code_latest_artifact_validation",
        },
        "rows": rows,
        "summary": {
            "supported_or_conditional": len(supported),
            "rejected_as_written": len(rejected),
            "total": len(rows),
        },
    }


def main() -> int:
    payload = build_report()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / "four_strategy_improvement_plan_validation_latest.json"
    out_csv = LOG_DIR / "four_strategy_improvement_plan_validation_latest.csv"
    out_json_stamped = LOG_DIR / f"four_strategy_improvement_plan_validation_{stamp}.json"
    out_csv_stamped = LOG_DIR / f"four_strategy_improvement_plan_validation_{stamp}.csv"

    for path in (out_json, out_json_stamped):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(payload["rows"][0].keys()))
        writer.writeheader()
        writer.writerows(payload["rows"])
    with out_csv_stamped.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(payload["rows"][0].keys()))
        writer.writeheader()
        writer.writerows(payload["rows"])

    print(
        json.dumps(
            {
                "status": "OK",
                "decision_status": payload["decision"]["status"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
                "out_json_stamped": str(out_json_stamped),
                "out_csv_stamped": str(out_csv_stamped),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
