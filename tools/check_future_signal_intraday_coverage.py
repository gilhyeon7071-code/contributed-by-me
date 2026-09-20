from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_INTRADAY_STATUS = LOGS / "intraday_prices_status_latest.json"
DEFAULT_VALIDATION = LOGS / "future_signal_validation_latest.json"
DEFAULT_OUTPUT = LOGS / "future_signal_intraday_coverage_check_latest.json"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _as_list(v: Any) -> List[str]:
    if isinstance(v, list):
        return sorted({str(x).strip().zfill(6) for x in v if str(x).strip()})
    return []


def _date8_from_text(v: Any) -> str:
    m = re.search(r"(\d{8})", str(v or ""))
    return m.group(1) if m else ""


def build_check(*, intraday_status_path: Path, validation_path: Path, output_path: Path) -> Dict[str, Any]:
    intraday = _load_json(intraday_status_path)
    validation = _load_json(validation_path)

    future_requested = int(intraday.get("future_signal_preview_codes_requested") or 0)
    future_covered = int(intraday.get("future_signal_preview_codes_covered") or 0)
    future_missing = _as_list(intraday.get("future_signal_preview_codes_missing"))
    requested_codes = _as_list(intraday.get("future_signal_preview_codes"))
    collected_codes = _as_list(intraday.get("codes_requested_list"))

    metrics = validation.get("metrics") if isinstance(validation.get("metrics"), dict) else {}
    intraday_coverage = metrics.get("intraday_coverage") if isinstance(metrics.get("intraday_coverage"), dict) else {}
    realized_rows = int(validation.get("realized_rows") or metrics.get("realized_rows") or 0)
    missing_price_rows = int(metrics.get("missing_price_rows") or 0)
    retrospective_missing_rows = int(metrics.get("intraday_retrospective_history_missing_rows") or 0)
    waiting_rows = int(metrics.get("waiting_rows") or 0)
    validation_state = str(validation.get("validation_state") or "")
    validation_d = str(validation.get("D") or "")
    intraday_history_d = _date8_from_text(intraday.get("history_csv"))
    intraday_status_d = _date8_from_text(intraday.get("ts"))
    intraday_d = intraday_history_d or intraday_status_d

    checks = {
        "intraday_status_exists": bool(intraday),
        "validation_exists": bool(validation),
        "intraday_validation_D_match": bool(intraday_d and validation_d and intraday_d == validation_d),
        "future_signal_status_fields_exist": future_requested > 0 or bool(requested_codes),
        "future_signal_codes_fully_requested": future_requested > 0 and future_requested == future_covered and not future_missing,
        "intraday_validation_has_realized_rows": realized_rows > 0,
        "intraday_missing_price_rows_zero": missing_price_rows == 0,
    }

    if not checks["intraday_status_exists"] or not checks["validation_exists"]:
        status = "FAIL"
        reason = "INPUT_MISSING"
    elif not checks["future_signal_status_fields_exist"]:
        status = "WAITING"
        reason = "FUTURE_SIGNAL_COVERAGE_FIELDS_NOT_YET_EMITTED"
    elif not checks["intraday_validation_D_match"] and intraday_coverage.get("mode") == "RETROSPECTIVE_HISTORY_ONLY":
        status = "WARN"
        reason = "RETROSPECTIVE_HISTORY_ONLY"
    elif not checks["intraday_validation_D_match"]:
        status = "WARN"
        reason = "INTRADAY_STATUS_VALIDATION_D_MISMATCH"
    elif not checks["future_signal_codes_fully_requested"]:
        status = "WARN"
        reason = "FUTURE_SIGNAL_CODES_NOT_FULLY_REQUESTED"
    elif not checks["intraday_missing_price_rows_zero"]:
        status = "WARN"
        reason = "INTRADAY_PRICE_ROWS_STILL_MISSING"
    else:
        status = "PASS"
        reason = "ok"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "reason": reason,
        "intraday_status_path": str(intraday_status_path),
        "validation_path": str(validation_path),
        "intraday_status_ts": str(intraday.get("ts") or ""),
        "intraday_history_csv": str(intraday.get("history_csv") or ""),
        "intraday_D": intraday_d,
        "validation_D": validation_d,
        "validation_state": validation_state,
        "future_signal_preview_codes_requested": future_requested,
        "future_signal_preview_codes_covered": future_covered,
        "future_signal_preview_codes_missing": future_missing,
        "future_signal_preview_codes": requested_codes,
        "codes_requested_list": collected_codes,
        "realized_rows": realized_rows,
        "missing_price_rows": missing_price_rows,
        "intraday_retrospective_history_missing_rows": retrospective_missing_rows,
        "intraday_coverage": intraday_coverage,
        "waiting_rows": waiting_rows,
        "checks": checks,
        "policy": {
            "read_only": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
        },
        "outputs": {"latest_json": str(output_path)},
    }
    _write_json(output_path, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Check future-signal intraday collection and validation coverage.")
    ap.add_argument("--intraday-status", default=str(DEFAULT_INTRADAY_STATUS))
    ap.add_argument("--validation", default=str(DEFAULT_VALIDATION))
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()
    payload = build_check(
        intraday_status_path=Path(args.intraday_status),
        validation_path=Path(args.validation),
        output_path=Path(args.output),
    )
    print(
        f"[FUTURE_INTRADAY_COVERAGE] status={payload.get('status')} reason={payload.get('reason')} "
        f"requested={payload.get('future_signal_preview_codes_requested')} "
        f"covered={payload.get('future_signal_preview_codes_covered')} "
        f"missing_price_rows={payload.get('missing_price_rows')}"
    )
    return 0 if payload.get("status") in {"PASS", "WARN", "WAITING"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
