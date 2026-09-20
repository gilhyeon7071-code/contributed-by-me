from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FILLS_CSV = PAPER_DIR / "fills.csv"
STATE_JSON = PAPER_DIR / "paper_state.json"
ENTRY_TRACE_JSON = LOG_DIR / "entry_layer_input_trace_audit_latest.json"
ENTRY_LAYER_VERIFY_JSON = LOG_DIR / "entry_decision_layer_runtime_verify_latest.json"

OUT_JSON = LOG_DIR / "final_candidate_elimination_audit_latest.json"
OUT_CSV = LOG_DIR / "final_candidate_elimination_audit_latest.csv"


FINALIST_CODES = ["001740", "382800"]


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


def _ymd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _truthy(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def _float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def _latest_buy_d(fills: List[Dict[str, str]]) -> str:
    ymds = [
        _ymd(row.get("datetime"))
        for row in fills
        if str(row.get("side") or "").strip().upper() == "BUY"
    ]
    ymds = [x for x in ymds if x]
    return max(ymds) if ymds else ""


def _open_codes() -> set[str]:
    state = _read_json(STATE_JSON)
    rows = state.get("open_positions", []) if isinstance(state.get("open_positions"), list) else []
    return {
        str(row.get("code") or "").zfill(6)
        for row in rows
        if isinstance(row, dict) and str(row.get("code") or "").strip()
    }


def _finalist_rows() -> List[Dict[str, Any]]:
    candidates = {str(row.get("code") or "").zfill(6): row for row in _read_csv(CANDIDATES_FINAL)}
    fills = _read_csv(FILLS_CSV)
    d_ref = _latest_buy_d(fills)
    open_codes = _open_codes()
    buy_rows_by_code: Dict[str, List[Dict[str, str]]] = {}
    for row in fills:
        code = str(row.get("code") or "").zfill(6)
        if str(row.get("side") or "").strip().upper() == "BUY" and _ymd(row.get("datetime")) == d_ref:
            buy_rows_by_code.setdefault(code, []).append(row)

    rows: List[Dict[str, Any]] = []
    for code in FINALIST_CODES:
        cand = candidates.get(code, {})
        buy_rows = buy_rows_by_code.get(code, [])
        score = _float(cand.get("final_score"))
        sector_strength = _float(cand.get("sector_strength"))
        union_ok = (
            str(cand.get("candidate_origin") or "").strip().upper() == "SECTOR_PREFILTER_UNION"
            and str(cand.get("sector_action") or "").strip().upper() == "BUY"
            and _truthy(cand.get("sector_entry_allowed"))
            and (sector_strength is not None and sector_strength >= 0.63)
        )
        reasons: List[str] = []
        if not cand:
            reasons.append("candidate_missing")
        if score is None or score <= 0:
            reasons.append("final_score_not_positive")
        if not _truthy(cand.get("execution_pool")):
            reasons.append("raw_execution_pool_false")
        if union_ok:
            reasons.append("union_conditional_entry_pool")
        if code in open_codes:
            reasons.append("already_open")
        if buy_rows:
            reasons.append("already_bought_on_D")
        rows.append(
            {
                "d_ref": d_ref,
                "code": code,
                "name": cand.get("name", ""),
                "candidate_date": cand.get("date", ""),
                "final_score": cand.get("final_score", ""),
                "candidate_origin": cand.get("candidate_origin", ""),
                "raw_execution_pool": cand.get("execution_pool", ""),
                "sector_action": cand.get("sector_action", ""),
                "sector_entry_allowed": cand.get("sector_entry_allowed", ""),
                "sector_strength": cand.get("sector_strength", ""),
                "union_conditional_entry_pool": str(bool(union_ok)).lower(),
                "buy_on_d_count": len(buy_rows),
                "buy_on_d_order_ids": ";".join(str(row.get("order_id") or "") for row in buy_rows),
                "open_position": str(code in open_codes).lower(),
                "elimination_reason": ";".join(reasons),
                "final_candidate_removed_by_same_day_buy_filter": str(bool(buy_rows)).lower(),
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )
    return rows


def main() -> int:
    rows = _finalist_rows()
    trace = _read_json(ENTRY_TRACE_JSON)
    verify = _read_json(ENTRY_LAYER_VERIFY_JSON)
    reason_counts: Counter[str] = Counter()
    for row in rows:
        for reason in str(row.get("elimination_reason") or "").split(";"):
            if reason:
                reason_counts[reason] += 1

    all_removed_by_same_day = bool(rows) and all(
        str(row.get("final_candidate_removed_by_same_day_buy_filter")) == "true"
        for row in rows
    )
    out = {
        "generated_at": _now_ts(),
        "status": "PASS" if all_removed_by_same_day else "WARN",
        "schema_version": "final_candidate_elimination_audit_v1",
        "source_files": {
            "candidates_final": str(CANDIDATES_FINAL),
            "fills": str(FILLS_CSV),
            "paper_state": str(STATE_JSON),
            "entry_trace": str(ENTRY_TRACE_JSON),
            "entry_layer_verify": str(ENTRY_LAYER_VERIFY_JSON),
        },
        "finalist_codes": FINALIST_CODES,
        "finalist_rows": len(rows),
        "all_finalists_removed_by_same_day_buy_filter": all_removed_by_same_day,
        "reason_counts": dict(sorted(reason_counts.items())),
        "entry_trace_primary": trace.get("primary_trace"),
        "same_cycle_comparable": trace.get("same_cycle_comparable"),
        "entry_layer_verify_status": verify.get("status"),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "interpretation": "The final normal-entry candidates were not missing due to a new required path; they were already bought on D and were removed by the existing same-day duplicate buy guard.",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "d_ref",
        "code",
        "name",
        "candidate_date",
        "final_score",
        "candidate_origin",
        "raw_execution_pool",
        "sector_action",
        "sector_entry_allowed",
        "sector_strength",
        "union_conditional_entry_pool",
        "buy_on_d_count",
        "buy_on_d_order_ids",
        "open_position",
        "elimination_reason",
        "final_candidate_removed_by_same_day_buy_filter",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    print(f"[FINAL] final candidate elimination audit -> {OUT_JSON} status={out['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
