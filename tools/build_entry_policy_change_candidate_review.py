from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
GROUP_REPORT_JSON = LOG_DIR / "entry_policy_group_report_latest.json"
GROUP_SUMMARY_CSV = LOG_DIR / "entry_policy_group_report_summary_latest.csv"
FALLBACK_WHATIF_JSON = LOG_DIR / "entry_fallback_stage_whatif_latest.json"
RISK_ORCH_JSON = LOG_DIR / "risk_orchestration_latest.json"
PRODUCTION_RISK_JSON = LOG_DIR / "production_risk_playbook_latest.json"

OUT_JSON = LOG_DIR / "entry_policy_change_candidate_review_latest.json"
OUT_CSV = LOG_DIR / "entry_policy_change_candidate_review_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _return_stats(summary_rows: List[Dict[str, str]], group: str) -> Dict[str, Any]:
    for row in summary_rows:
        if row.get("kind") == "return_stats" and row.get("entry_policy_group") == group:
            return row
    return {}


def _f(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def _decision(status: str, candidate: str, current: str, reason: str, next_step: str) -> Dict[str, Any]:
    return {
        "candidate": candidate,
        "decision": status,
        "current_state": current,
        "reason": reason,
        "next_step": next_step,
        "trading_effect": "false",
        "policy_effect": "false",
        "policy_change_applied": "false",
    }


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    group_report = _read_json(GROUP_REPORT_JSON)
    summary_rows = _read_csv(GROUP_SUMMARY_CSV)
    fallback = _read_json(FALLBACK_WHATIF_JSON)
    risk_orch = _read_json(RISK_ORCH_JSON)
    production_risk = _read_json(PRODUCTION_RISK_JSON)

    market_ops = cfg.get("market_ops_policy", {})
    fallback_policy = market_ops.get("entry_fallback_policy", {})
    risk_block = risk_orch.get("risk_orchestration", {})

    normal = _return_stats(summary_rows, "normal_entry")
    intraday = _return_stats(summary_rows, "intraday_realtime")
    surge = _return_stats(summary_rows, "surge_immediate")
    validation = _return_stats(summary_rows, "validation_reduce")

    normal_n = int(float(normal.get("n") or 0))
    normal_avg = _f(normal.get("avg"))
    normal_win = _f(normal.get("win_rate"))
    intraday_avg = _f(intraday.get("avg"))
    surge_avg = _f(surge.get("avg"))
    validation_avg = _f(validation.get("avg"))

    candidates: List[Dict[str, Any]] = []
    candidates.append(
        _decision(
            "HOLD",
            "normal_entry_policy_value_change",
            f"normal_trades={normal_n}, avg={normal_avg}, win_rate={normal_win}",
            "Clean normal sample is small and not positive enough to justify threshold relaxation.",
            "Keep collecting normalized normal_entry rows before changing normal entry thresholds.",
        )
    )
    candidates.append(
        _decision(
            "REJECT_NOW",
            "entry_fallback_stage_expansion",
            f"max_stage={fallback_policy.get('max_stage')}, stage1_allowed={fallback.get('current_policy_stage1_allowed')}, stage2_allowed={fallback.get('current_policy_stage2_allowed')}",
            "Current what-if produced zero current-policy stage1/stage2 opportunities and mixed historical rows are not valid proof.",
            "Keep stage expansion as shadow-only until fresh normal_entry what-if rows exist.",
        )
    )
    candidates.append(
        _decision(
            "REJECT_NOW",
            "intraday_realtime_promotion",
            f"avg={intraday_avg}, rows={intraday.get('n')}",
            "Intraday realtime closed-trade group is negative and must not justify normal policy.",
            "Keep intraday_realtime separate; require independent improvement evidence.",
        )
    )
    candidates.append(
        _decision(
            "REJECT_NOW",
            "surge_immediate_promotion",
            f"avg={surge_avg}, rows={surge.get('n')}",
            "Surge immediate closed-trade group is negative and high-risk.",
            "Keep surge_immediate separate and shadow/hold by default unless later evidence improves.",
        )
    )
    candidates.append(
        _decision(
            "HOLD",
            "validation_reduce_as_policy_proof",
            f"avg={validation_avg}, rows={validation.get('n')}",
            "Validation-reduce rows are diagnostic samples, not clean normal-entry policy proof.",
            "Analyze validation_reduce only as validation-mode behavior.",
        )
    )
    candidates.append(
        _decision(
            "APPROVE_READ_ONLY_NEXT",
            "runtime_normalized_policy_group_fields",
            "report exists, runtime source fields not yet normalized",
            "The current blocker is evidence hygiene, not an entry relaxation value.",
            "Add read-only normalized fields to runtime/report artifacts before any policy value change.",
        )
    )
    candidates.append(
        _decision(
            "HOLD",
            "risk_gate_relaxation_for_samples",
            f"dd_stop_triggered={risk_block.get('dd_stop_triggered')}, production_action={production_risk.get('action')}, production_blocked={production_risk.get('blocked')}",
            "Risk/DD/production gates are defensive controls and current evidence does not justify relaxing them for samples.",
            "Do not relax DDM, risk orchestration, production risk, P1 gate, or kill switch to force samples.",
        )
    )

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "entry_policy_change_candidate_review_v1",
        "source_files": {
            "config": str(CONFIG_PATH),
            "entry_policy_group_report": str(GROUP_REPORT_JSON),
            "entry_policy_group_summary": str(GROUP_SUMMARY_CSV),
            "entry_fallback_stage_whatif": str(FALLBACK_WHATIF_JSON),
            "risk_orchestration": str(RISK_ORCH_JSON),
            "production_risk_playbook": str(PRODUCTION_RISK_JSON),
        },
        "group_counts": group_report.get("entry_policy_group_counts", {}),
        "candidate_decisions": candidates,
        "final_decision": "NO_LIVE_POLICY_VALUE_CHANGE",
        "approved_next_step": "READ_ONLY_RUNTIME_NORMALIZED_POLICY_GROUP_FIELDS",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "candidate",
        "decision",
        "current_state",
        "reason",
        "next_step",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, candidates, fields)
    print(f"[FINAL] entry policy change candidate review -> {OUT_JSON} candidates={len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
