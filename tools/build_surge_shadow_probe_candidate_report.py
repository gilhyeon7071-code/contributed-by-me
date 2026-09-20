"""Build shadow-only probe candidates for surge no-lob recheck rows.

This report designs a narrow small-size/probe candidate rule without enabling
entry, orders, or policy changes. It is for expected-value review only.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

NO_LOB_REVIEW_CSV = LOG_DIR / "no_lob_recheck_review_report_latest.csv"
PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
PARAMS_JSON = ROOT / "paper" / "surge_params.json"

OUT_JSON = LOG_DIR / "surge_shadow_probe_candidate_report_latest.json"
OUT_CSV = LOG_DIR / "surge_shadow_probe_candidate_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))

# Shadow-only design thresholds. These are not production policy values.
MIN_FOLLOWTHROUGH_PCT = 0.5
MAX_SPREAD_BPS = 25.0
MAX_RVOL20 = 2.5
MIN_TRADING_VALUE = 100_000_000_000.0
MIN_SURGE_SCORE_FINAL = 80.0
ALLOWED_TYPES = {"LIMIT_UP_NEAR", "PRICE_RANGE_BREAKOUT", "PRICE_VOL_BREAKOUT"}


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _plan_trading_flags() -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for row in _read_csv(PLAN_CSV):
        code = _code(row.get("code"))
        if code:
            out[code] = bool(out.get(code, False) or _truthy(row.get("trading_allowed")))
    return out


def _evaluate(row: Dict[str, str], plan_trading: Dict[str, bool]) -> Dict[str, Any]:
    code = _code(row.get("code"))
    ret = _f(row.get("return_pct_since_first_seen"))
    spread = _f(row.get("spread_bps"))
    rvol20 = _f(row.get("rvol20"))
    trading_value = _f(row.get("trading_value"))
    score = _f(row.get("surge_score_final"))
    detected_type = str(row.get("detected_surge_type") or "")
    blockers = str(row.get("remaining_hard_ex_no_lob") or "").strip()
    review_class = str(row.get("review_class") or "")
    next_action = str(row.get("next_action") or "")

    checks = {
        "review_class_candidate": review_class == "NARROW_POLICY_REVIEW_CANDIDATE",
        "next_action_promote": next_action == "PROMOTE_TO_RECHECK",
        "positive_followthrough": ret >= MIN_FOLLOWTHROUGH_PCT,
        "spread_ok": spread <= MAX_SPREAD_BPS,
        "rvol_ok": rvol20 <= MAX_RVOL20,
        "liquidity_ok": trading_value >= MIN_TRADING_VALUE,
        "score_ok": score >= MIN_SURGE_SCORE_FINAL,
        "surge_type_ok": detected_type in ALLOWED_TYPES,
        "no_remaining_hard": blockers == "",
        "source_not_trading": not bool(plan_trading.get(code, False)),
    }
    failed = [key for key, ok in checks.items() if not ok]
    shadow_probe_candidate = not failed
    if shadow_probe_candidate:
        shadow_class = "SHADOW_PROBE_CANDIDATE"
        action_hint = "SHADOW_ONLY_TRACK_NO_ENTRY"
    elif checks["review_class_candidate"]:
        shadow_class = "NEAR_MISS_SHADOW_PROBE"
        action_hint = "KEEP_SHADOW_REVIEW"
    else:
        shadow_class = "NOT_SHADOW_PROBE"
        action_hint = "OBSERVE_ONLY"

    return {
        "code": code,
        "name": row.get("name", ""),
        "next_action": next_action,
        "source_review_class": review_class,
        "shadow_class": shadow_class,
        "shadow_probe_candidate": shadow_probe_candidate,
        "action_hint": action_hint,
        "failed_shadow_checks": "|".join(failed),
        "return_pct_since_first_seen": round(ret, 6),
        "min_followthrough_pct": MIN_FOLLOWTHROUGH_PCT,
        "detected_surge_type": detected_type,
        "surge_score_final": round(score, 6),
        "min_surge_score_final": MIN_SURGE_SCORE_FINAL,
        "spread_bps": round(spread, 6),
        "max_spread_bps": MAX_SPREAD_BPS,
        "rvol20": round(rvol20, 6),
        "max_rvol20": MAX_RVOL20,
        "trading_value": round(trading_value, 2),
        "min_trading_value": MIN_TRADING_VALUE,
        "remaining_hard_ex_no_lob": blockers,
        "source_trading_allowed": bool(plan_trading.get(code, False)),
        "entry_approval_changed": False,
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    class_counts = Counter(str(row.get("shadow_class") or "") for row in rows)
    failed = Counter()
    for row in rows:
        for token in str(row.get("failed_shadow_checks") or "").split("|"):
            token = token.strip()
            if token:
                failed[token] += 1
    return {
        "rows": len(rows),
        "shadow_class_counts": dict(sorted(class_counts.items())),
        "failed_shadow_check_counts": dict(sorted(failed.items())),
        "shadow_probe_candidate_codes": [
            row.get("code") for row in rows if row.get("shadow_probe_candidate")
        ],
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["code"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    params = _read_json(PARAMS_JSON)
    _ = params  # Loaded for source-contract visibility; thresholds above are shadow-only.
    plan_trading = _plan_trading_flags()
    rows = [_evaluate(row, plan_trading) for row in _read_csv(NO_LOB_REVIEW_CSV)]
    payload = {
        "generated_at": _now(),
        "scope": "read_only_surge_shadow_probe_candidate",
        "policy_note": "Shadow-only design. No live entry, order route, policy value, or gate behavior is changed.",
        "shadow_thresholds": {
            "min_followthrough_pct": MIN_FOLLOWTHROUGH_PCT,
            "max_spread_bps": MAX_SPREAD_BPS,
            "max_rvol20": MAX_RVOL20,
            "min_trading_value": MIN_TRADING_VALUE,
            "min_surge_score_final": MIN_SURGE_SCORE_FINAL,
            "allowed_types": sorted(ALLOWED_TYPES),
        },
        "source_files": {
            "no_lob_review_csv": str(NO_LOB_REVIEW_CSV),
            "plan_csv": str(PLAN_CSV),
            "params_json": str(PARAMS_JSON),
        },
        "summary": _summary(rows),
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
