"""Build a read-only tracker for strict surge attack candidates.

This script records attack-candidate observations only. It does not change
candidate generation, gates, order routes, fills, ledger, or stats.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"

OUT_JSON = LOG_DIR / "surge_attack_candidate_tracker_latest.json"
OUT_CSV = LOG_DIR / "surge_attack_candidate_tracker_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_attack_candidate_tracker_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_attack_candidate_tracker_history.jsonl"

ALLOWED_TYPES = {"PRICE_VOL_BREAKOUT", "PRICE_RANGE_BREAKOUT"}


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return [{str(k): str(v or "") for k, v in row.items()} for row in csv.DictReader(f)]
        except UnicodeDecodeError:
            continue
        except Exception:
            return []
    return []


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else [
        "observed_at",
        "date",
        "code",
        "attack_candidate",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _append_history(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    HISTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    write_header = not HISTORY_CSV.exists() or HISTORY_CSV.stat().st_size == 0
    with HISTORY_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    with HISTORY_JSONL.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _is_attack_candidate(row: dict[str, str]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    detected_type = _clean_text(row.get("detected_surge_type") or row.get("surge_type")).upper()
    rvol20 = _f(row.get("rvol20"))
    trading_value = _f(row.get("trading_value"))

    if not _truthy(row.get("detected_surge_flag")):
        reasons.append("not_detected")
    if detected_type not in ALLOWED_TYPES:
        reasons.append("type_not_allowed")
    if not _truthy(row.get("entry_allowed")):
        reasons.append("entry_not_allowed")
    if _clean_text(row.get("exclude_reasons")):
        reasons.append("exclude_reasons_present")
    if _clean_text(row.get("paper_probe_block_reasons")):
        reasons.append("paper_probe_block_present")
    if _clean_text(row.get("lob_status")).upper() != "OK":
        reasons.append("lob_not_ok")
    if _clean_text(row.get("orderflow_tag")).upper() != "OK":
        reasons.append("orderflow_not_ok")
    if not (2.0 <= rvol20 <= 5.0):
        reasons.append("rvol20_outside_2_to_5")
    if trading_value < 1_000_000_000.0:
        reasons.append("trading_value_below_1b")

    return not reasons, reasons


def _candidate_row(row: dict[str, str], observed_at: str) -> dict[str, Any]:
    code = _code(row.get("code"))
    date = _clean_text(row.get("date"))
    detected_type = _clean_text(row.get("detected_surge_type") or row.get("surge_type")).upper()
    is_candidate, failed = _is_attack_candidate(row)
    return {
        "observed_at": observed_at,
        "date": date,
        "code": code,
        "attack_candidate_version": "v1_strict",
        "attack_candidate": bool(is_candidate),
        "attack_candidate_status": "ATTACK_CANDIDATE_READ_ONLY" if is_candidate else "NOT_ATTACK_CANDIDATE",
        "failed_attack_checks": "|".join(failed),
        "detected_surge_type": detected_type,
        "entry_allowed": _truthy(row.get("entry_allowed")),
        "entry_decision": row.get("entry_decision", ""),
        "entry_reason": row.get("entry_reason", ""),
        "exclude_reasons": row.get("exclude_reasons", ""),
        "paper_probe_block_reasons": row.get("paper_probe_block_reasons", ""),
        "change_pct": _f(row.get("change_pct")),
        "rvol20": _f(row.get("rvol20")),
        "trading_value": _f(row.get("trading_value")),
        "surge_score_final": _f(row.get("surge_score_final")),
        "lob_status": row.get("lob_status", ""),
        "orderflow_tag": row.get("orderflow_tag", ""),
        "spread_bps": _f(row.get("spread_bps")),
        "orderflow_risk_score": _f(row.get("orderflow_risk_score")),
        "current_price": _f(row.get("current_price")),
        "max_future_ret_pct": "",
        "min_future_ret_pct": "",
        "followthrough_status": "PENDING_MARKOUT",
        "trading_effect": False,
        "policy_effect": False,
        "order_path_effect": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "research_only": True,
        "must_not_dispatch": True,
    }


def build() -> dict[str, Any]:
    observed_at = _now()
    source_rows = _read_csv(SURGE_REALTIME_CSV)
    detected_rows = [
        row for row in source_rows
        if _truthy(row.get("detected_surge_flag")) or _truthy(row.get("is_realtime_surge"))
    ]
    out_rows = [_candidate_row(row, observed_at) for row in detected_rows]
    attack_rows = [row for row in out_rows if row["attack_candidate"]]
    failed_reasons = Counter()
    for row in out_rows:
        for reason in str(row.get("failed_attack_checks") or "").split("|"):
            if reason:
                failed_reasons[reason] += 1

    payload = {
        "generated_at": observed_at,
        "status": "OK",
        "scope": "read_only_surge_attack_candidate_tracker",
        "policy_note": "Observation only. No entry approval, gate, order route, fill, ledger, or stat behavior is changed.",
        "source_files": {
            "surge_realtime": str(SURGE_REALTIME_CSV),
        },
        "summary": {
            "source_rows": len(source_rows),
            "detected_rows": len(detected_rows),
            "attack_candidate_rows": len(attack_rows),
            "attack_candidate_codes": [row["code"] for row in attack_rows],
            "failed_check_counts": dict(sorted(failed_reasons.items())),
            "trading_effect": False,
            "policy_effect": False,
            "order_path_effect": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": out_rows,
        "artifacts": {
            "csv": str(OUT_CSV),
            "history_csv": str(HISTORY_CSV),
            "history_jsonl": str(HISTORY_JSONL),
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, out_rows)
    _append_history(out_rows)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
        "history_csv": str(HISTORY_CSV),
        "summary": payload["summary"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
