"""Verify publisher historical TP/FP evidence is fresh before news scoring."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
STATUS_PATH = ROOT / "2_Logs" / "publisher_trust_historical_tp_fp_latest.json"
KST = timezone(timedelta(hours=9))


def _parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def _max_age_hours() -> float:
    raw = os.environ.get("PUBLISHER_TRUST_HIST_MAX_AGE_HOURS", "24")
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 24.0


def _status(reason: str, **extra: object) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "status": "PASS" if reason == "ok" else "FAIL",
        "reason": reason,
        "path": str(STATUS_PATH),
    }
    out.update(extra)
    return out


def verify() -> Dict[str, Any]:
    if not STATUS_PATH.exists():
        return _status("missing_status")
    try:
        data = json.loads(STATUS_PATH.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return _status("invalid_json", error=str(exc))

    generated_at = _parse_dt(data.get("generated_at"))
    if generated_at is None:
        return _status("missing_generated_at")

    now = datetime.now(KST)
    age_hours = (now - generated_at).total_seconds() / 3600.0
    max_age = _max_age_hours()
    if age_hours > max_age:
        return _status(
            "stale_status",
            generated_at=generated_at.isoformat(timespec="seconds"),
            age_hours=round(age_hours, 3),
            max_age_hours=max_age,
        )
    if data.get("status") != "PASS":
        return _status("status_not_pass", publisher_status=data.get("status"))
    if int(data.get("evaluated_rows") or 0) <= 0:
        return _status("no_evaluated_rows", evaluated_rows=data.get("evaluated_rows"))
    if not isinstance(data.get("by_source"), dict) or not data.get("by_source"):
        return _status("missing_by_source")

    return _status(
        "ok",
        generated_at=generated_at.isoformat(timespec="seconds"),
        age_hours=round(age_hours, 3),
        max_age_hours=max_age,
        evaluated_rows=int(data.get("evaluated_rows") or 0),
        by_source_count=len(data.get("by_source") or {}),
        trading_effect=bool(data.get("trading_effect")),
        policy_effect=bool(data.get("policy_effect")),
        auto_emit_enabled=bool(data.get("auto_emit_enabled")),
    )


def main() -> int:
    status = verify()
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0 if status.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
