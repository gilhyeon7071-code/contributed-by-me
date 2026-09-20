"""Evaluate second-chance entry readiness for blocked shadow candidates.

Read-only. This does not approve entries, alter gates, write candidate inputs,
or change paper-engine policy. It only evaluates whether overheat-blocked rows
have cooled down enough to deserve later review.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

DDM_EVAL_JSON = LOG_DIR / "ddm_stage_cap_shadow_eval_latest.json"
SHADOW_PROMOTION_JSON = LOG_DIR / "shadow_promotion_report_latest.json"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
LOB_JSON = LOG_DIR / "surge_lob_latest.json"
OUT_JSON = LOG_DIR / "second_chance_shadow_entry_latest.json"
OUT_CSV = LOG_DIR / "second_chance_shadow_entry_latest.csv"

ENTRY_CHANGE_LIMIT = 0.16
ATR_ENTRY_LIMIT = 0.20
MIN_DRAWDOWN_FOR_COOLDOWN = -0.03
MAX_RVOL_FOR_COOLDOWN = 3.0


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


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _surge_by_code(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _lob_codes(doc: Dict[str, Any], key: str) -> set[str]:
    values = doc.get(key)
    if not isinstance(values, list):
        return set()
    return {_code(v) for v in values if _code(v)}


def _evaluate(row: Dict[str, Any], surge: Dict[str, Any], lob_doc: Dict[str, Any]) -> Dict[str, Any]:
    code = _code(row.get("code"))
    change_pct = _f(surge.get("change_pct"), _f(row.get("current_change_pct"), 0.0))
    atr14_pct = _f(surge.get("atr14_pct"), 0.0)
    drawdown = _f(surge.get("intraday_high_drawdown_pct"), 0.0)
    rvol20 = _f(surge.get("rvol20"), 0.0)
    trading_value = _f(surge.get("trading_value"), _f(row.get("current_trading_value"), 0.0))
    exclude_reasons = str(surge.get("exclude_reasons") or row.get("blocked_reasons") or "")

    codes_with_lob = _lob_codes(lob_doc, "codes_with_lob")
    codes_no_lob = _lob_codes(lob_doc, "codes_no_lob")
    lob_available = _truthy(surge.get("lob_available")) or code in codes_with_lob
    if code in codes_no_lob:
        lob_available = False

    checks = {
        "entry_change_cooled": change_pct <= ENTRY_CHANGE_LIMIT,
        "atr_cooled": atr14_pct <= ATR_ENTRY_LIMIT,
        "pullback_observed": drawdown <= MIN_DRAWDOWN_FOR_COOLDOWN,
        "rvol_not_overheat": rvol20 <= MAX_RVOL_FOR_COOLDOWN,
        "lob_available": lob_available,
    }
    passed = [name for name, ok in checks.items() if ok]
    failed = [name for name, ok in checks.items() if not ok]

    if all(checks.values()):
        verdict = "SECOND_CHANCE_REVIEW_READY"
    elif checks["pullback_observed"] and checks["rvol_not_overheat"] and checks["atr_cooled"]:
        verdict = "SECOND_CHANCE_WAIT_LOB_OR_CHANGE"
    else:
        verdict = "SECOND_CHANCE_BLOCKED_OVERHEAT"

    return {
        "code": code,
        "name": str(row.get("name") or surge.get("name") or ""),
        "candidate_source": str(row.get("candidate_source") or ""),
        "review_bucket": str(row.get("review_bucket") or ""),
        "ddm_assessment": str(row.get("assessment") or ""),
        "return_pct_since_first_seen": _f(row.get("return_pct_since_first_seen"), 0.0),
        "change_pct": change_pct,
        "atr14_pct": atr14_pct,
        "intraday_high_drawdown_pct": drawdown,
        "rvol20": rvol20,
        "trading_value": trading_value,
        "exclude_reasons": exclude_reasons,
        "second_chance_verdict": verdict,
        "passed_checks": ",".join(passed),
        "failed_checks": ",".join(failed),
        "second_chance_trading_allowed": False,
        "trading_effect": False,
        "policy_effect": False,
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
        "code",
        "name",
        "candidate_source",
        "review_bucket",
        "ddm_assessment",
        "return_pct_since_first_seen",
        "change_pct",
        "atr14_pct",
        "intraday_high_drawdown_pct",
        "rvol20",
        "trading_value",
        "second_chance_verdict",
        "passed_checks",
        "failed_checks",
        "second_chance_trading_allowed",
        "trading_effect",
        "policy_effect",
        "exclude_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    ddm_doc = _read_json(DDM_EVAL_JSON)
    ddm_rows = ddm_doc.get("rows") if isinstance(ddm_doc.get("rows"), list) else []
    surge_rows = _read_csv(SURGE_CSV)
    surge_map = _surge_by_code(surge_rows)
    lob_doc = _read_json(LOB_JSON)
    shadow_doc = _read_json(SHADOW_PROMOTION_JSON)

    eval_rows: List[Dict[str, Any]] = []
    for row in ddm_rows:
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        if not code:
            continue
        eval_rows.append(_evaluate(row, surge_map.get(code, {}), lob_doc))

    eval_rows.sort(
        key=lambda r: (
            1 if r.get("second_chance_verdict") == "SECOND_CHANCE_REVIEW_READY" else 0,
            _f(r.get("return_pct_since_first_seen")),
            _f(r.get("trading_value")),
        ),
        reverse=True,
    )
    payload = {
        "generated_at": _now_kst(),
        "schema_version": "second_chance_shadow_entry_v1",
        "generated_from": {
            "ddm_eval": str(DDM_EVAL_JSON),
            "shadow_promotion": str(SHADOW_PROMOTION_JSON),
            "surge_realtime": str(SURGE_CSV),
            "surge_lob": str(LOB_JSON),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "thresholds": {
            "entry_change_limit": ENTRY_CHANGE_LIMIT,
            "atr_entry_limit": ATR_ENTRY_LIMIT,
            "min_drawdown_for_cooldown": MIN_DRAWDOWN_FOR_COOLDOWN,
            "max_rvol_for_cooldown": MAX_RVOL_FOR_COOLDOWN,
        },
        "summary": {
            "source_rows": len(ddm_rows),
            "eval_rows": len(eval_rows),
            "verdict_counts": _count(eval_rows, "second_chance_verdict"),
            "review_ready_rows": int(sum(1 for r in eval_rows if r.get("second_chance_verdict") == "SECOND_CHANCE_REVIEW_READY")),
            "latest_shadow_summary": shadow_doc.get("summary") if isinstance(shadow_doc, dict) else {},
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
