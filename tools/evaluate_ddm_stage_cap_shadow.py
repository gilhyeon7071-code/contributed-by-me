"""Evaluate whether ddm_stage_cap blocked risky shadow-promotion candidates.

Read-only evaluator. It summarizes shadow promotion history and classifies
whether rows blocked by ddm_stage_cap also carried surge/overheat risk markers.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HISTORY_CSV = LOG_DIR / "shadow_promotion_report_history.csv"
LATEST_JSON = LOG_DIR / "shadow_promotion_report_latest.json"
OUT_JSON = LOG_DIR / "ddm_stage_cap_shadow_eval_latest.json"
OUT_CSV = LOG_DIR / "ddm_stage_cap_shadow_eval_latest.csv"

ENTRY_CHANGE_LIMIT = 16.0
ATR_CAP_TOKEN = "ENTRY_ATR_CAP"
NO_LOB_TOKEN = "NO_LOB_BLOCK"
RVOL_OVERHEAT_TOKEN = "SCORE_RVOL_OVERHEAT_BLOCK"
KRX_TOKEN = "KRX_"


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if text == "":
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _has(row: Dict[str, Any], text: str) -> bool:
    blob = "|".join(str(row.get(k) or "") for k in ("shadow_blocked_reasons", "shadow_observation"))
    return text in blob


def _classify(row: Dict[str, Any]) -> Dict[str, Any]:
    change_pct = _f(row.get("current_change_pct"), 0.0)
    reasons = str(row.get("shadow_blocked_reasons") or row.get("shadow_observation") or "")
    ddm_blocked = "ddm_stage_cap" in reasons or "p1_gate_closed" in reasons
    entry_change_block = "ENTRY_CHANGE_BLOCK" in reasons or abs(change_pct) > ENTRY_CHANGE_LIMIT
    atr_cap = ATR_CAP_TOKEN in reasons
    no_lob = NO_LOB_TOKEN in reasons
    rvol_overheat = RVOL_OVERHEAT_TOKEN in reasons
    krx_flag = KRX_TOKEN in reasons
    sector_unknown = "sector_strength_unknown" in reasons
    shadow_pass = _truthy(row.get("shadow_pass"))

    risk_markers = [
        name
        for name, flag in (
            ("entry_change_block", entry_change_block),
            ("atr_cap", atr_cap),
            ("no_lob", no_lob),
            ("rvol_overheat", rvol_overheat),
            ("krx_flag", krx_flag),
            ("sector_unknown", sector_unknown),
        )
        if flag
    ]

    if shadow_pass:
        assessment = "PASS_ROW_NOT_DDM_BLOCKED"
    elif ddm_blocked and len(risk_markers) >= 2:
        assessment = "DDM_BLOCK_ALIGNED_WITH_RISK"
    elif ddm_blocked and risk_markers:
        assessment = "DDM_BLOCK_PARTIAL_RISK"
    elif ddm_blocked:
        assessment = "DDM_BLOCK_NEEDS_REVIEW"
    else:
        assessment = "NOT_DDM_BLOCKED"

    return {
        "generated_at": str(row.get("generated_at") or ""),
        "code": str(row.get("code") or ""),
        "name": str(row.get("name") or ""),
        "candidate_source": str(row.get("candidate_source") or ""),
        "review_bucket": str(row.get("review_bucket") or ""),
        "return_pct_since_first_seen": _f(row.get("return_pct_since_first_seen"), 0.0),
        "current_change_pct": change_pct,
        "shadow_pass": shadow_pass,
        "ddm_blocked": ddm_blocked,
        "risk_marker_count": len(risk_markers),
        "risk_markers": ",".join(risk_markers),
        "assessment": assessment,
        "blocked_reasons": reasons,
    }


def _count(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        out[value] = out.get(value, 0) + 1
    return out


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "generated_at",
        "code",
        "name",
        "candidate_source",
        "review_bucket",
        "return_pct_since_first_seen",
        "current_change_pct",
        "shadow_pass",
        "ddm_blocked",
        "risk_marker_count",
        "risk_markers",
        "assessment",
        "blocked_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    history_rows = _read_csv(HISTORY_CSV)
    latest_doc = _read_json(LATEST_JSON)
    eval_rows = [_classify(row) for row in history_rows]
    eval_rows.sort(
        key=lambda r: (
            _f(r.get("return_pct_since_first_seen")),
            _f(r.get("current_change_pct")),
        ),
        reverse=True,
    )
    ddm_rows = [r for r in eval_rows if bool(r.get("ddm_blocked"))]
    aligned = [r for r in eval_rows if r.get("assessment") == "DDM_BLOCK_ALIGNED_WITH_RISK"]
    needs_review = [r for r in eval_rows if r.get("assessment") == "DDM_BLOCK_NEEDS_REVIEW"]

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "ddm_stage_cap_shadow_eval_v1",
        "generated_from": {
            "history_csv": str(HISTORY_CSV),
            "latest_json": str(LATEST_JSON),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "summary": {
            "history_rows": len(history_rows),
            "ddm_blocked_rows": len(ddm_rows),
            "aligned_with_risk_rows": len(aligned),
            "ddm_needs_review_rows": len(needs_review),
            "assessment_counts": _count(eval_rows, "assessment"),
            "latest_shadow_summary": latest_doc.get("summary") if isinstance(latest_doc, dict) else {},
        },
        "interpretation": {
            "current_sample_assessment": (
                "ddm_stage_cap aligned with concurrent surge/overheat risk"
                if ddm_rows and len(ddm_rows) == len(aligned)
                else "ddm_stage_cap requires more shadow samples"
            ),
            "sample_size_warning": "single-day/small-sample" if len(history_rows) < 20 else "",
        },
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": eval_rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, eval_rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
