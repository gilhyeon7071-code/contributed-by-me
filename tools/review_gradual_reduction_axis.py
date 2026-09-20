"""Review which single axis is suitable for gradual reduction.

Read-only. This does not change gates, thresholds, candidates, orders, fills,
ledger, or policy. It only summarizes second-chance ready rows and recommends
the next review axis before any approved policy change.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SECOND_CHANCE_JSON = LOG_DIR / "second_chance_shadow_entry_latest.json"
DESIGN_JSON = LOG_DIR / "second_chance_sample_collection_design_latest.json"
OUT_JSON = LOG_DIR / "gradual_reduction_axis_review_latest.json"
OUT_CSV = LOG_DIR / "gradual_reduction_axis_review_latest.csv"

MIN_UNIQUE_READY_CODES_FOR_POLICY_AXIS = 5


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _reason_tokens(value: Any) -> List[str]:
    tokens: List[str] = []
    for raw in re.split(r"[|,]", str(value or "")):
        token = raw.strip().split(":", 1)[0]
        if token:
            tokens.append(token)
    return tokens


def _axis_for_token(token: str) -> str:
    if token == "sector_strength_unknown":
        return "DATA_COMPLETENESS_SECTOR"
    if token in {"surge_policy_excluded", "REF_STALE", "NO_REFERENCE"}:
        return "DATA_FRESHNESS_REFERENCE"
    if token == "p1_gate_closed":
        return "P1_GLOBAL_RISK_GATE"
    if token == "MARKOUT_NEGATIVE_BLOCK":
        return "ORDERFLOW_MARKOUT_RISK"
    if token in {"ENTRY_CHANGE_BLOCK", "ENTRY_ATR_CAP", "SCORE_RVOL_OVERHEAT_BLOCK", "RVOL_OVERHEAT_BLOCK"}:
        return "SURGE_OVERHEAT_RISK"
    if token == "NO_LOB_BLOCK":
        return "LOB_AVAILABILITY"
    if token.startswith("KRX_"):
        return "KRX_RISK"
    return "OTHER"


def _axis_rows(ready_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    axis_counter: Counter[str] = Counter()
    axis_codes: Dict[str, set[str]] = defaultdict(set)
    axis_examples: Dict[str, List[str]] = defaultdict(list)
    for row in ready_rows:
        code = _code(row.get("code"))
        seen_axes = set()
        for token in _reason_tokens(row.get("exclude_reasons")):
            axis = _axis_for_token(token)
            seen_axes.add(axis)
            if len(axis_examples[axis]) < 5:
                axis_examples[axis].append(f"{code}:{token}")
        for axis in seen_axes:
            axis_counter[axis] += 1
            if code:
                axis_codes[axis].add(code)

    rows: List[Dict[str, Any]] = []
    for axis, count in axis_counter.most_common():
        policy_candidate = axis in {"ORDERFLOW_MARKOUT_RISK", "SURGE_OVERHEAT_RISK", "LOB_AVAILABILITY"}
        if axis.startswith("DATA_"):
            action = "FIX_OR_VALIDATE_DATA_BEFORE_POLICY"
        elif axis == "P1_GLOBAL_RISK_GATE":
            action = "DO_NOT_RELAX_FIRST"
        elif axis == "ORDERFLOW_MARKOUT_RISK":
            action = "SHADOW_CONTROL_REVIEW_FIRST"
        else:
            action = "KEEP_SHADOW_REVIEW"
        rows.append(
            {
                "axis": axis,
                "ready_row_count": count,
                "unique_code_count": len(axis_codes[axis]),
                "policy_candidate": policy_candidate,
                "recommended_action": action,
                "examples": ";".join(axis_examples[axis]),
            }
        )
    return rows


def _dedup_ready_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if not code:
            continue
        old = best.get(code)
        if old is None or _f(row.get("return_pct_since_first_seen")) > _f(old.get("return_pct_since_first_seen")):
            best[code] = row
    return sorted(best.values(), key=lambda r: _f(r.get("return_pct_since_first_seen")), reverse=True)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "axis",
        "ready_row_count",
        "unique_code_count",
        "policy_candidate",
        "recommended_action",
        "examples",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    second = _read_json(SECOND_CHANCE_JSON)
    design = _read_json(DESIGN_JSON)
    rows = second.get("rows") if isinstance(second.get("rows"), list) else []
    ready_rows = [r for r in rows if str(r.get("second_chance_verdict") or "") == "SECOND_CHANCE_REVIEW_READY"]
    dedup_ready = _dedup_ready_rows(ready_rows)
    axis_rows = _axis_rows(ready_rows)
    unique_ready_codes = sorted({_code(r.get("code")) for r in ready_rows if _code(r.get("code"))})

    if len(unique_ready_codes) < MIN_UNIQUE_READY_CODES_FOR_POLICY_AXIS:
        recommendation = "DO_NOT_APPLY_POLICY_REDUCTION_YET"
        first_axis = "DATA_COMPLETENESS_AND_DUPLICATE_REVIEW"
        reason = "ready rows are concentrated in too few unique codes"
    else:
        data_axes = [r for r in axis_rows if str(r.get("axis") or "").startswith("DATA_")]
        if data_axes:
            recommendation = "REVIEW_DATA_AXES_BEFORE_POLICY_REDUCTION"
            first_axis = str(data_axes[0]["axis"])
            reason = "data completeness or freshness blockers dominate ready rows"
        else:
            recommendation = "SHADOW_REVIEW_ORDERFLOW_MARKOUT_AXIS_FIRST"
            first_axis = "ORDERFLOW_MARKOUT_RISK"
            reason = "policy-like blocker can be isolated for one-axis shadow/control review"

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "gradual_reduction_axis_review_v1",
        "generated_from": {
            "second_chance_shadow_entry": str(SECOND_CHANCE_JSON),
            "second_chance_sample_collection_design": str(DESIGN_JSON),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "summary": {
            "source_rows": len(rows),
            "ready_rows": len(ready_rows),
            "unique_ready_codes": len(unique_ready_codes),
            "unique_ready_code_list": unique_ready_codes,
            "dedup_ready_rows": len(dedup_ready),
            "min_unique_ready_codes_for_policy_axis": MIN_UNIQUE_READY_CODES_FOR_POLICY_AXIS,
            "design_recommendation": design.get("recommendation"),
            "design_readiness_status": (design.get("readiness") or {}).get("status") if isinstance(design.get("readiness"), dict) else "",
        },
        "recommendation": {
            "action": recommendation,
            "first_axis": first_axis,
            "reason": reason,
            "approval_required_for_policy_change": True,
            "next_step_type": "read_only_review" if recommendation != "DO_NOT_APPLY_POLICY_REDUCTION_YET" else "data_quality_review",
        },
        "axis_rows": axis_rows,
        "dedup_ready_rows": dedup_ready,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, axis_rows)
    print(
        json.dumps(
            {
                "status": "OK",
                "ready_rows": len(ready_rows),
                "unique_ready_codes": len(unique_ready_codes),
                "recommendation": recommendation,
                "first_axis": first_axis,
                "out_json": str(OUT_JSON),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
