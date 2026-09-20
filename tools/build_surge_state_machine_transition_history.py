from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

DEFAULT_SHADOW_JSON = LOGS / "surge_state_machine_shadow_latest.json"
DEFAULT_HISTORY_CSV = LOGS / "surge_state_machine_transition_history.csv"
DEFAULT_LATEST_JSON = LOGS / "surge_state_machine_transition_history_latest.json"

FIELDS = [
    "observed_ts",
    "ymd",
    "code",
    "name",
    "prev_state",
    "state",
    "transition",
    "state_reason",
    "next_required_condition",
    "precursor_score",
    "precursor_stage",
    "intraday_score",
    "intraday_stage",
    "surge_score_final",
    "change_pct",
    "rvol20",
    "spread_bps",
    "ask_depth_levels",
    "readiness_status",
    "stage_status",
    "source_layers",
    "paper_order_route",
    "broker_order_route",
    "dispatch_enabled",
    "trading_allowed",
    "research_only",
    "must_not_dispatch",
]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            return list(csv.DictReader(fp))
    except Exception:
        return []


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def _parse_ts(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def _market_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    return time(9, 0) <= now.time() <= time(15, 45)


def _last_by_code(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        if code and code != "000000":
            out[code] = row
    return out


def _bool_text(value: Any) -> str:
    return "true" if bool(value) else "false"


def _transition_row(row: dict[str, Any], prev: dict[str, Any] | None, observed_ts: datetime) -> dict[str, Any]:
    state = str(row.get("state") or "")
    prev_state = str((prev or {}).get("state") or "")
    code = str(row.get("code") or "").zfill(6)
    transition = "INITIAL" if not prev_state else f"{prev_state}->{state}"
    out = {
        "observed_ts": observed_ts.isoformat(timespec="seconds"),
        "ymd": observed_ts.strftime("%Y%m%d"),
        "code": code,
        "name": row.get("name", ""),
        "prev_state": prev_state,
        "state": state,
        "transition": transition,
        "state_reason": row.get("state_reason", ""),
        "next_required_condition": row.get("next_required_condition", ""),
        "precursor_score": row.get("precursor_score", ""),
        "precursor_stage": row.get("precursor_stage", ""),
        "intraday_score": row.get("intraday_score", ""),
        "intraday_stage": row.get("intraday_stage", ""),
        "surge_score_final": row.get("surge_score_final", ""),
        "change_pct": row.get("change_pct", ""),
        "rvol20": row.get("rvol20", ""),
        "spread_bps": row.get("spread_bps", ""),
        "ask_depth_levels": row.get("ask_depth_levels", ""),
        "readiness_status": row.get("readiness_status", ""),
        "stage_status": row.get("stage_status", ""),
        "source_layers": row.get("source_layers", ""),
        "paper_order_route": _bool_text(row.get("paper_order_route")),
        "broker_order_route": _bool_text(row.get("broker_order_route")),
        "dispatch_enabled": _bool_text(row.get("dispatch_enabled")),
        "trading_allowed": _bool_text(row.get("trading_allowed")),
        "research_only": _bool_text(row.get("research_only")),
        "must_not_dispatch": _bool_text(row.get("must_not_dispatch")),
    }
    return out


def _state_signature(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("state") or ""),
        str(row.get("readiness_status") or ""),
        str(row.get("stage_status") or ""),
    )


def build_transition_history(
    shadow_json: Path,
    history_csv: Path,
    latest_json: Path,
    *,
    allow_offhours: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    shadow = _read_json(shadow_json)
    rows = [row for row in shadow.get("rows", []) if str(row.get("code") or "").strip()]
    observed_ts = _parse_ts(shadow.get("ts"))
    in_session = _market_session(observed_ts)
    existing = _read_csv(history_csv)
    last = _last_by_code(existing)

    if not in_session and not allow_offhours:
        payload = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "status": "SKIPPED_OFFHOURS",
            "scope": "surge_state_machine_transition_history",
            "shadow_rows": len(rows),
            "history_rows_before": len(existing),
            "appended_rows": 0,
            "dry_run": bool(dry_run),
            "market_session": False,
            "history_csv": str(history_csv),
            "latest_json": str(latest_json),
            "risk_contract": _risk_contract(),
            "note": "state transitions are accumulated only during market session unless --allow-offhours is supplied",
        }
        if not dry_run:
            latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return payload

    appended: list[dict[str, Any]] = []
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        prev = last.get(code)
        if prev and _state_signature(prev) == _state_signature(row):
            continue
        appended.append(_transition_row(row, prev, observed_ts))

    combined = existing + appended
    state_counts = Counter(str(row.get("state") or "") for row in combined)
    transition_counts = Counter(str(row.get("transition") or "") for row in appended)
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "scope": "surge_state_machine_transition_history",
        "shadow_rows": len(rows),
        "history_rows_before": len(existing),
        "history_rows_after": len(combined),
        "appended_rows": len(appended),
        "dry_run": bool(dry_run),
        "market_session": bool(in_session),
        "allow_offhours": bool(allow_offhours),
        "state_counts_total": dict(state_counts),
        "transition_counts_appended": dict(transition_counts),
        "history_csv": str(history_csv),
        "latest_json": str(latest_json),
        "risk_contract": _risk_contract(),
        "rows_appended": appended[:50],
    }
    if not dry_run:
        _write_csv(history_csv, combined)
        latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _risk_contract() -> dict[str, bool]:
    return {
        "read_only_diagnostic": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "dispatch_enabled": False,
        "trading_allowed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Append read-only surge state-machine transitions.")
    parser.add_argument("--shadow-json", default=str(DEFAULT_SHADOW_JSON))
    parser.add_argument("--history-csv", default=str(DEFAULT_HISTORY_CSV))
    parser.add_argument("--latest-json", default=str(DEFAULT_LATEST_JSON))
    parser.add_argument("--allow-offhours", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = build_transition_history(
        Path(args.shadow_json),
        Path(args.history_csv),
        Path(args.latest_json),
        allow_offhours=bool(args.allow_offhours),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps({
        "status": payload.get("status"),
        "shadow_rows": payload.get("shadow_rows"),
        "appended_rows": payload.get("appended_rows"),
        "history_rows_after": payload.get("history_rows_after", payload.get("history_rows_before", 0)),
        "market_session": payload.get("market_session"),
        "dry_run": payload.get("dry_run"),
        "history_csv": payload.get("history_csv"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
