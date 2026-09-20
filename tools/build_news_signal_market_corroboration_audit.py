from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
IN_DIAG = LOGS / "news_signal_shadow_diagnostic_latest.csv"
IN_CANDIDATES = LOGS / "candidates_latest_data.with_final_score.csv"
IN_LOB = LOGS / "surge_lob_latest.csv"
IN_FINAL_STATUS = LOGS / "final_score_merge_status_latest.json"
OUT_CSV = LOGS / "news_signal_market_corroboration_audit_latest.csv"
OUT_JSON = LOGS / "news_signal_market_corroboration_audit_latest.json"
OUT_QUEUE_CSV = LOGS / "news_signal_lob_refresh_queue_latest.csv"
OUT_QUEUE_JSON = LOGS / "news_signal_lob_refresh_queue_latest.json"

VERSION = "news_signal_market_corroboration_audit_v1"
QUEUE_VERSION = "news_signal_lob_refresh_queue_v1"


def _now_kst() -> str:
    return datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _s(row: Dict[str, str], key: str) -> str:
    return str(row.get(key, "") or "").strip()


def _f(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        raw = _s(row, key)
        if raw == "":
            return default
        return float(raw)
    except Exception:
        return default


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "ok"}


def _latest_by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _audit_row(
    diag: Dict[str, str],
    candidate: Dict[str, str],
    lob: Dict[str, str],
) -> Dict[str, Any]:
    code = _code(diag.get("code"))
    stage = _s(diag, "news_signal_stage")
    publisher_gate_state = _s(diag, "publisher_gate_state").upper()
    publisher_trust = _s(diag, "publisher_trust")
    publisher_crypto_auth_status = _s(diag, "publisher_crypto_auth_status")
    publisher_verified = publisher_gate_state == "PUBLISHER_VERIFIED"
    publisher_corroboration_allowed = publisher_gate_state in {
        "PUBLISHER_VERIFIED",
        "PUBLISHER_AUTHENTICATED_PROVISIONAL",
    }
    candidate_lob_status = _s(candidate, "execution_lob_status") or "MISSING"
    candidate_orderflow_tag = _s(candidate, "execution_orderflow_tag") or "MISSING"
    candidate_market_score = _f(diag, "news_market_corroboration_score", 0.0)
    market_gap = _f(diag, "market_gap_to_confirm", 0.0)

    lob_in_snapshot = bool(lob)
    lob_available = _bool_text(lob.get("lob_available") if lob else "")
    lob_status = _s(lob, "lob_status") if lob else "MISSING"
    hoga_status = _s(lob, "hoga_fetch_status") if lob else "MISSING"
    spread_bps = _f(lob, "spread_bps", -1.0) if lob else -1.0
    orderflow_tag = _s(lob, "orderflow_tag") if lob else "MISSING"
    orderflow_risk = _f(lob, "orderflow_risk_score", 0.0) if lob else 0.0
    ofi_norm = _f(lob, "ofi_norm", 0.0) if lob else 0.0
    ask1 = _f(lob, "ask1", 0.0) if lob else 0.0
    bid1 = _f(lob, "bid1", 0.0) if lob else 0.0
    lob_ts = _s(lob, "ts") if lob else ""

    if stage == "CONFIRMED_SHADOW" and not publisher_verified:
        cause = "PUBLISHER_NOT_VERIFIED"
    elif stage in {"PRE_SIGNAL", "PUBLISHER_UNVERIFIED_SHADOW"} and not publisher_corroboration_allowed:
        cause = "PUBLISHER_NOT_VERIFIED"
    elif not lob_in_snapshot:
        cause = "CODE_NOT_IN_LOB_SNAPSHOT"
    elif not lob_available:
        cause = "LOB_ROW_UNAVAILABLE"
    elif spread_bps > 80.0:
        cause = "SPREAD_TOO_WIDE"
    elif orderflow_tag.upper() in {"PAUSE", "CAUTION"}:
        cause = "ORDERFLOW_NOT_OK"
    elif candidate_market_score < 0.50:
        cause = "MARKET_SCORE_BELOW_CONFIRM"
    else:
        cause = "MARKET_CORROBORATION_AVAILABLE"

    if cause == "PUBLISHER_NOT_VERIFIED":
        next_action = "WAIT_FOR_PUBLISHER_VERIFICATION"
    elif stage == "PRE_SIGNAL":
        if cause == "MARKET_CORROBORATION_AVAILABLE":
            next_action = "REVIEW_SHADOW_CONFIRM_RULE"
        else:
            next_action = "WAIT_FOR_LOB_OR_MARKET_REFRESH"
    elif stage == "NO_TEXT_SIGNAL":
        next_action = "WAIT_FOR_TEXT_SOURCE"
    else:
        next_action = "KEEP_OBSERVING"

    return {
        "code": code,
        "name": _s(diag, "name"),
        "news_signal_stage": stage,
        "news_text_strength": _s(diag, "news_text_strength"),
        "news_market_corroboration_score": round(candidate_market_score, 6),
        "market_gap_to_confirm": round(market_gap, 6),
        "corroboration_gap_cause": cause,
        "corroboration_next_action": next_action,
        "publisher_gate_state": publisher_gate_state,
        "publisher_trust": publisher_trust,
        "publisher_crypto_auth_status": publisher_crypto_auth_status,
        "candidate_lob_status": candidate_lob_status,
        "candidate_orderflow_tag": candidate_orderflow_tag,
        "lob_in_snapshot": str(lob_in_snapshot).lower(),
        "lob_available": str(lob_available).lower(),
        "lob_status": lob_status,
        "hoga_fetch_status": hoga_status,
        "lob_ts": lob_ts,
        "ask1": ask1,
        "bid1": bid1,
        "spread_bps": spread_bps,
        "orderflow_tag": orderflow_tag,
        "orderflow_risk_score": round(orderflow_risk, 6),
        "ofi_norm": round(ofi_norm, 6),
        "shadow_only": "true",
        "trading_effect": "false",
    }


def _queue_rows(audits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    refresh_causes = {"CODE_NOT_IN_LOB_SNAPSHOT", "LOB_ROW_UNAVAILABLE"}
    rows: List[Dict[str, Any]] = []
    for row in audits:
        if row.get("news_signal_stage") != "PRE_SIGNAL":
            continue
        cause = str(row.get("corroboration_gap_cause") or "")
        if cause not in refresh_causes:
            continue
        rows.append(
            {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "news_signal_stage": row.get("news_signal_stage", ""),
                "news_text_strength": row.get("news_text_strength", ""),
                "news_market_corroboration_score": row.get("news_market_corroboration_score", ""),
                "market_gap_to_confirm": row.get("market_gap_to_confirm", ""),
                "corroboration_gap_cause": cause,
                "publisher_gate_state": row.get("publisher_gate_state", ""),
                "publisher_trust": row.get("publisher_trust", ""),
                "publisher_crypto_auth_status": row.get("publisher_crypto_auth_status", ""),
                "requested_scope": "shadow_news_signal_lob_refresh",
                "refresh_reason": "pre_signal_missing_or_unavailable_lob",
                "source_audit": str(OUT_CSV),
                "shadow_only": "true",
                "trading_effect": "false",
                "policy_effect": "false",
            }
        )
    return rows


def main() -> int:
    diag_rows = _read_csv(IN_DIAG)
    candidate_by_code = _latest_by_code(_read_csv(IN_CANDIDATES))
    lob_by_code = _latest_by_code(_read_csv(IN_LOB))

    audits: List[Dict[str, Any]] = []
    for row in diag_rows:
        code = _code(row.get("code"))
        audits.append(_audit_row(row, candidate_by_code.get(code, {}), lob_by_code.get(code, {})))

    fields = [
        "code",
        "name",
        "news_signal_stage",
        "news_text_strength",
        "news_market_corroboration_score",
        "market_gap_to_confirm",
        "corroboration_gap_cause",
        "corroboration_next_action",
        "publisher_gate_state",
        "publisher_trust",
        "publisher_crypto_auth_status",
        "candidate_lob_status",
        "candidate_orderflow_tag",
        "lob_in_snapshot",
        "lob_available",
        "lob_status",
        "hoga_fetch_status",
        "lob_ts",
        "ask1",
        "bid1",
        "spread_bps",
        "orderflow_tag",
        "orderflow_risk_score",
        "ofi_norm",
        "shadow_only",
        "trading_effect",
    ]
    LOGS.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(audits)

    queue_rows = _queue_rows(audits)
    queue_fields = [
        "code",
        "name",
        "news_signal_stage",
        "news_text_strength",
        "news_market_corroboration_score",
        "market_gap_to_confirm",
        "corroboration_gap_cause",
        "publisher_gate_state",
        "publisher_trust",
        "publisher_crypto_auth_status",
        "requested_scope",
        "refresh_reason",
        "source_audit",
        "shadow_only",
        "trading_effect",
        "policy_effect",
    ]
    with OUT_QUEUE_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=queue_fields)
        writer.writeheader()
        writer.writerows(queue_rows)

    cause_counts = Counter(str(row["corroboration_gap_cause"]) for row in audits)
    action_counts = Counter(str(row["corroboration_next_action"]) for row in audits)
    pre_rows = [r for r in audits if r["news_signal_stage"] == "PRE_SIGNAL"]
    pre_cause_counts = Counter(str(row["corroboration_gap_cause"]) for row in pre_rows)
    queue_cause_counts = Counter(str(row["corroboration_gap_cause"]) for row in queue_rows)

    final_status = _read_json(IN_FINAL_STATUS)
    status = {
        "generated_at": _now_kst(),
        "version": VERSION,
        "status": "PASS",
        "quality": "PASS" if audits else "WARN",
        "reason": "ok" if audits else "diagnostic_input_empty",
        "input_diagnostic_csv": str(IN_DIAG),
        "input_candidates_csv": str(IN_CANDIDATES),
        "input_lob_csv": str(IN_LOB),
        "output_csv": str(OUT_CSV),
        "lob_refresh_queue_csv": str(OUT_QUEUE_CSV),
        "lob_refresh_queue_json": str(OUT_QUEUE_JSON),
        "rows": len(audits),
        "pre_signal_rows": len(pre_rows),
        "cause_counts": dict(cause_counts),
        "pre_signal_cause_counts": dict(pre_cause_counts),
        "next_action_counts": dict(action_counts),
        "lob_refresh_queue_rows": len(queue_rows),
        "lob_refresh_queue_cause_counts": dict(queue_cause_counts),
        "candidate_lob_matched_rows_from_final_status": (final_status.get("execution_lob") or {}).get("matched_rows"),
        "candidate_lob_adjusted_rows_from_final_status": (final_status.get("execution_lob") or {}).get("adjusted_rows"),
        "lob_snapshot_rows": len(lob_by_code),
        "shadow_only": True,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "consumed_by_order_path": False,
    }
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    queue_status = {
        "generated_at": status["generated_at"],
        "version": QUEUE_VERSION,
        "status": "PASS",
        "quality": "PASS" if queue_rows else "WARN",
        "reason": "ok" if queue_rows else "no_pre_signal_lob_refresh_needed",
        "source_audit_json": str(OUT_JSON),
        "source_audit_csv": str(OUT_CSV),
        "output_csv": str(OUT_QUEUE_CSV),
        "rows": len(queue_rows),
        "cause_counts": dict(queue_cause_counts),
        "shadow_only": True,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "consumed_by_order_path": False,
    }
    OUT_QUEUE_JSON.write_text(json.dumps(queue_status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": status["status"],
                "rows": status["rows"],
                "pre_signal_cause_counts": status["pre_signal_cause_counts"],
                "cause_counts": status["cause_counts"],
                "lob_refresh_queue_rows": status["lob_refresh_queue_rows"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
