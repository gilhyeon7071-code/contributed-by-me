from __future__ import annotations

import csv
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ENTRY_LAYER_JSON = LOG_DIR / "entry_decision_layers_runtime_latest.json"
ENTRY_LAYER_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
P1_JSON = LOG_DIR / "p1_entry_gate_status_latest.json"
PENDING_JSON = LOG_DIR / "pending_entry_status_latest.json"
FILLS_CSV = PAPER_DIR / "fills.csv"
STATE_JSON = PAPER_DIR / "paper_state.json"

OUT_JSON = LOG_DIR / "entry_layer_input_trace_audit_latest.json"
OUT_CSV = LOG_DIR / "entry_layer_input_trace_audit_latest.csv"


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
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    tmp_path.replace(path)


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def _mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat()


def _ymd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def _current_bought_codes() -> tuple[str, set[str]]:
    fills = _read_csv(FILLS_CSV)
    buys = [r for r in fills if str(r.get("side") or "").strip().upper() == "BUY"]
    d_ref = max((_ymd(r.get("datetime")) for r in buys), default="")
    codes = {
        str(r.get("code") or "").zfill(6)
        for r in buys
        if _ymd(r.get("datetime")) == d_ref and str(r.get("code") or "").strip()
    }
    return d_ref, codes


def _open_codes() -> set[str]:
    state = _read_json(STATE_JSON)
    rows = state.get("open_positions", []) if isinstance(state.get("open_positions"), list) else []
    return {
        str(r.get("code") or "").zfill(6)
        for r in rows
        if isinstance(r, dict) and str(r.get("code") or "").strip()
    }


def _candidate_reasons(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    d_ref, bought = _current_bought_codes()
    open_codes = _open_codes()
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        code = str(row.get("code") or "").zfill(6)
        reasons: List[str] = []
        fs = _float(row.get("final_score"))
        if fs is None:
            reasons.append("final_score_invalid")
        elif fs <= 0:
            reasons.append("final_score_non_positive")
        if not _truthy(row.get("execution_pool")):
            reasons.append("execution_pool_false")
        if not _truthy(row.get("sector_entry_allowed")):
            reasons.append("sector_entry_not_allowed")
        if code in open_codes:
            reasons.append("already_open")
        if code in bought:
            reasons.append("already_bought_on_D")
        if not str(row.get("date") or "").strip():
            reasons.append("candidate_date_missing")
        out.append(
            {
                "row_id": idx,
                "d_ref": d_ref,
                "code": code,
                "name": row.get("name", ""),
                "candidate_date": row.get("date", ""),
                "final_score": row.get("final_score", ""),
                "execution_pool": row.get("execution_pool", ""),
                "sector_entry_allowed": row.get("sector_entry_allowed", ""),
                "open_or_bought_state": ";".join(x for x in ("already_open" if code in open_codes else "", "already_bought_on_D" if code in bought else "") if x),
                "trace_reasons": ";".join(reasons),
                "normal_entry_source_viable_now": "true" if not reasons else "false",
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )
    return out


def main() -> int:
    candidates = _read_csv(CANDIDATES_FINAL)
    entry_layer = _read_json(ENTRY_LAYER_JSON)
    p1 = _read_json(P1_JSON)
    pending = _read_json(PENDING_JSON)
    trace_rows = _candidate_reasons(candidates)
    reason_counts: Counter[str] = Counter()
    for row in trace_rows:
        for reason in str(row.get("trace_reasons") or "").split(";"):
            if reason:
                reason_counts[reason] += 1

    mtimes = {
        "candidates_final": _mtime(CANDIDATES_FINAL),
        "entry_decision_layers_json": _mtime(ENTRY_LAYER_JSON),
        "entry_decision_layers_csv": _mtime(ENTRY_LAYER_CSV),
        "p1_entry_gate_status": _mtime(P1_JSON),
        "pending_entry_status": _mtime(PENDING_JSON),
    }
    candidate_newer_than_entry_layer = (
        CANDIDATES_FINAL.exists()
        and ENTRY_LAYER_JSON.exists()
        and CANDIDATES_FINAL.stat().st_mtime > ENTRY_LAYER_JSON.stat().st_mtime
    )
    same_cycle_comparable = not candidate_newer_than_entry_layer

    if candidate_newer_than_entry_layer:
        primary_trace = "FRESHNESS_MISMATCH_CANDIDATES_NEWER_THAN_ENTRY_LAYER"
    elif int(entry_layer.get("candidate_rows") or 0) == 0:
        primary_trace = "ENTRY_LAYER_INPUT_EMPTY"
    else:
        primary_trace = "ENTRY_LAYER_HAS_INPUT_ROWS"

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "entry_layer_input_trace_audit_v1",
        "source_files": {
            "candidates_final": str(CANDIDATES_FINAL),
            "entry_decision_layers_json": str(ENTRY_LAYER_JSON),
            "entry_decision_layers_csv": str(ENTRY_LAYER_CSV),
            "p1_entry_gate_status": str(P1_JSON),
            "pending_entry_status": str(PENDING_JSON),
        },
        "mtimes": mtimes,
        "same_cycle_comparable": same_cycle_comparable,
        "candidate_newer_than_entry_layer": candidate_newer_than_entry_layer,
        "candidate_rows_now": len(candidates),
        "entry_layer_candidate_rows": int(entry_layer.get("candidate_rows") or 0),
        "entry_layer_decision_rows": int(entry_layer.get("decision_rows") or 0),
        "p1_entry_candidates_before": int(p1.get("entry_candidates_before") or 0),
        "p1_entry_candidates_after": int(p1.get("entry_candidates_after") or 0),
        "pending_entry_ready": int(pending.get("entry_ready") or 0),
        "pending_candidates_after_caps": int(pending.get("candidates_after_caps") or 0),
        "current_candidate_trace_reason_counts": dict(sorted(reason_counts.items())),
        "current_candidate_viable_rows": sum(1 for row in trace_rows if row.get("normal_entry_source_viable_now") == "true"),
        "primary_trace": primary_trace,
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "next_step": "Refresh or trace the official entry-layer generation path in the same cycle before claiming candidate-to-entry-layer loss.",
        "official_generation_path": "paper_engine.py -> paper_engine/entry.py::_write_entry_runtime_snapshots_and_reports",
        "official_refresh_executed": False,
        "official_refresh_not_run_reason": "paper_engine runtime may update paper state/orders/fills/logs; this audit stays read-only.",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "row_id",
        "d_ref",
        "code",
        "name",
        "candidate_date",
        "final_score",
        "execution_pool",
        "sector_entry_allowed",
        "open_or_bought_state",
        "trace_reasons",
        "normal_entry_source_viable_now",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(OUT_JSON, json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, trace_rows, fields)
    print(f"[FINAL] entry layer input trace audit -> {OUT_JSON} primary={primary_trace}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
