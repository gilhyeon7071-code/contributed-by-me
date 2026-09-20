from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

RELABEL_JSON = LOG_DIR / "normal_intraday_policy_relabel_latest.json"
WHATIF_JSON = LOG_DIR / "backtest_acceptance_intraday_shadow_whatif_latest.json"
VALIDATION_JSON = LOG_DIR / "normal_intraday_policy_validation_latest.json"

OUT_JSON = LOG_DIR / "normal_intraday_policy_shadow_proof_latest.json"
OUT_CSV = LOG_DIR / "normal_intraday_policy_shadow_proof_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "rank",
        "rule",
        "blocked_entries",
        "blocked_losses",
        "blocked_winners",
        "loss_avoided_abs",
        "opportunity_cost_positive_ret",
        "actual_net_sum",
        "simulated_allowed_net_sum",
        "net_improvement_vs_actual",
        "improvement_ratio_of_actual_loss",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    tmp.replace(path)


def _num(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _best_passing_whatif(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    passing = [row for row in rows if bool(row.get("would_pass"))]
    if not passing:
        return {}
    return sorted(passing, key=lambda r: _num(r.get("ci95_low"), -999.0), reverse=True)[0]


def build_payload() -> Dict[str, Any]:
    relabel = _read_json(RELABEL_JSON)
    whatif = _read_json(WHATIF_JSON)
    validation = _read_json(VALIDATION_JSON)

    summary = relabel.get("summary") if isinstance(relabel.get("summary"), list) else []
    ranked_rows: List[Dict[str, Any]] = []
    actual_net = _num((relabel.get("actual") or {}).get("net_sum"))
    actual_loss_abs = abs(actual_net) if math.isfinite(actual_net) and actual_net < 0 else math.nan
    for idx, row in enumerate(summary, start=1):
        out = dict(row)
        out["rank"] = idx
        improvement = _num(row.get("net_improvement_vs_actual"))
        out["improvement_ratio_of_actual_loss"] = (
            improvement / actual_loss_abs
            if math.isfinite(improvement) and math.isfinite(actual_loss_abs) and actual_loss_abs > 0
            else math.nan
        )
        ranked_rows.append(out)

    best_rule = ranked_rows[0] if ranked_rows else {}
    whatif_rows = whatif.get("scenarios") if isinstance(whatif.get("scenarios"), list) else []
    best_passing_whatif = _best_passing_whatif(whatif_rows)
    validation_status = str(validation.get("status") or "")
    validation_summary = validation.get("summary") if isinstance(validation.get("summary"), dict) else {}

    best_ratio = _num(best_rule.get("improvement_ratio_of_actual_loss"))
    min_passing_loss_reduction = 0.50 if any(
        row.get("scenario") == "reduce_intraday_split_loss_days_50pct" and bool(row.get("would_pass"))
        for row in whatif_rows
    ) else math.nan

    proof_status = "PASS" if (
        validation_status == "PASS"
        and bool(best_rule)
        and math.isfinite(best_ratio)
        and math.isfinite(min_passing_loss_reduction)
        and best_ratio >= min_passing_loss_reduction
    ) else "INSUFFICIENT"

    return {
        "generated_at": _now_ts(),
        "status": proof_status,
        "scope": "read_only_normal_intraday_policy_shadow_proof",
        "source_files": {
            "relabel": str(RELABEL_JSON),
            "acceptance_whatif": str(WHATIF_JSON),
            "policy_validation": str(VALIDATION_JSON),
        },
        "baseline": {
            "actual_net_sum": actual_net,
            "actual_loss_abs": actual_loss_abs,
            "acceptance_baseline": whatif.get("baseline", {}),
        },
        "best_rule": best_rule,
        "acceptance_whatif_reference": {
            "min_passing_loss_reduction_ratio": min_passing_loss_reduction,
            "best_passing_scenario": best_passing_whatif,
        },
        "policy_contract_validation": {
            "status": validation_status,
            "summary": validation_summary,
        },
        "decision": (
            "SHADOW_PROOF_CANDIDATE"
            if proof_status == "PASS"
            else "NEEDS_MORE_EVIDENCE"
        ),
        "implemented_changes": {
            "policy_change_applied": False,
            "threshold_relaxation_applied": False,
            "entry_approval_changed": False,
            "order_fill_ledger_changed": False,
        },
        "interpretation": {
            "candidate": str(best_rule.get("rule") or ""),
            "reason": (
                "Best candidate's realized-loss improvement ratio is at or above the read-only what-if reduction level that made CI95 low positive."
                if proof_status == "PASS"
                else "Existing evidence is not enough to justify policy implementation."
            ),
            "limitation": "This combines shadow reports; it does not replay the full order engine and does not approve live transition.",
        },
        "ranked_rules": ranked_rows,
    }


def main() -> int:
    payload = build_payload()
    rows = payload.get("ranked_rules") if isinstance(payload.get("ranked_rules"), list) else []
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload.get("status"), "decision": payload.get("decision"), "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
