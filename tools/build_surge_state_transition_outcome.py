from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

DEFAULT_TRANSITION_CSV = LOGS / "surge_state_machine_transition_history.csv"
DEFAULT_OUT_JSON = LOGS / "surge_state_transition_outcome_latest.json"
DEFAULT_OUT_CSV = LOGS / "surge_state_transition_outcome_latest.csv"
TARGET_MINUTES = [1, 3, 5, 10]
MAX_LAG_SECONDS = 90

FIELDS = [
    "observed_ts",
    "ymd",
    "code",
    "name",
    "transition",
    "state",
    "base_price",
    "target_minutes",
    "target_ts",
    "observed_price_ts",
    "observed_price",
    "lag_seconds",
    "ret_pct",
    "outcome_status",
    "sample_quality",
    "precursor_score",
    "intraday_score",
    "surge_score_final",
    "change_pct",
    "rvol20",
    "spread_bps",
    "ask_depth_levels",
    "readiness_status",
    "stage_status",
    "paper_order_route",
    "broker_order_route",
    "dispatch_enabled",
    "trading_allowed",
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(fp)]
    except Exception:
        return []


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _price_history_path(ymd: str) -> Path:
    return LOGS / f"intraday_prices_history_{ymd}.csv"


def _price_rows_by_code(ymd: str) -> dict[str, list[tuple[datetime, float]]]:
    rows = _read_csv(_price_history_path(ymd))
    out: dict[str, list[tuple[datetime, float]]] = {}
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        ts = _parse_ts(row.get("ts"))
        price = _f(row.get("current_price"))
        if not code or code == "000000" or ts is None or price <= 0:
            continue
        out.setdefault(code, []).append((ts, price))
    for code in out:
        out[code].sort(key=lambda item: item[0])
    return out


def _first_at_or_after(rows: list[tuple[datetime, float]], target: datetime) -> tuple[datetime, float] | None:
    for ts, price in rows:
        if ts >= target:
            return ts, price
    return None


def _event_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        str(row.get("observed_ts") or ""),
        str(row.get("code") or "").zfill(6),
        str(row.get("transition") or ""),
        str(row.get("state") or ""),
    )


def _dedup_transitions(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        key = _event_key(row)
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _outcome_status(ret_pct: float | None) -> str:
    if ret_pct is None:
        return "MISSING_TIMEPOINT"
    if ret_pct > 0:
        return "POSITIVE_MARKOUT"
    if ret_pct < 0:
        return "NEGATIVE_MARKOUT"
    return "FLAT_MARKOUT"


def _outcome_rows(transitions: list[dict[str, str]]) -> list[dict[str, Any]]:
    price_cache: dict[str, dict[str, list[tuple[datetime, float]]]] = {}
    out: list[dict[str, Any]] = []
    for transition in transitions:
        ymd = str(transition.get("ymd") or "")[:8]
        code = str(transition.get("code") or "").zfill(6)
        event_ts = _parse_ts(transition.get("observed_ts"))
        if not ymd or not code or event_ts is None:
            continue
        if ymd not in price_cache:
            price_cache[ymd] = _price_rows_by_code(ymd)
        prices = price_cache[ymd].get(code, [])
        base_obs = _first_at_or_after(prices, event_ts)
        base_price = base_obs[1] if base_obs else 0.0
        for target_min in TARGET_MINUTES:
            target_ts = event_ts + timedelta(minutes=target_min)
            obs = _first_at_or_after(prices, target_ts)
            if base_price > 0 and obs:
                lag_seconds = max(0.0, (obs[0] - target_ts).total_seconds())
                ret_pct: float | None = (obs[1] / base_price - 1.0) * 100.0
                sample_quality = "ON_TIME_SAMPLE" if lag_seconds <= MAX_LAG_SECONDS else "LATE_SAMPLE"
            else:
                lag_seconds = None
                ret_pct = None
                sample_quality = "NO_SAMPLE"
            out.append({
                "observed_ts": transition.get("observed_ts", ""),
                "ymd": ymd,
                "code": code,
                "name": transition.get("name", ""),
                "transition": transition.get("transition", ""),
                "state": transition.get("state", ""),
                "base_price": round(base_price, 6) if base_price > 0 else "",
                "target_minutes": target_min,
                "target_ts": target_ts.isoformat(timespec="seconds"),
                "observed_price_ts": obs[0].isoformat(timespec="seconds") if obs else "",
                "observed_price": round(obs[1], 6) if obs else "",
                "lag_seconds": round(lag_seconds, 3) if lag_seconds is not None else "",
                "ret_pct": round(ret_pct, 6) if ret_pct is not None else "",
                "outcome_status": _outcome_status(ret_pct),
                "sample_quality": sample_quality,
                "precursor_score": transition.get("precursor_score", ""),
                "intraday_score": transition.get("intraday_score", ""),
                "surge_score_final": transition.get("surge_score_final", ""),
                "change_pct": transition.get("change_pct", ""),
                "rvol20": transition.get("rvol20", ""),
                "spread_bps": transition.get("spread_bps", ""),
                "ask_depth_levels": transition.get("ask_depth_levels", ""),
                "readiness_status": transition.get("readiness_status", ""),
                "stage_status": transition.get("stage_status", ""),
                "paper_order_route": "false",
                "broker_order_route": "false",
                "dispatch_enabled": "false",
                "trading_allowed": "false",
            })
    return out


def build_outcome(transition_csv: Path, out_json: Path, out_csv: Path, *, dry_run: bool = False) -> dict[str, Any]:
    raw_transitions = _read_csv(transition_csv)
    transitions = _dedup_transitions(raw_transitions)
    rows = _outcome_rows(transitions)
    status_counts = Counter(str(row.get("outcome_status") or "") for row in rows)
    quality_counts = Counter(str(row.get("sample_quality") or "") for row in rows)
    transitions_with_any_sample = sorted({row["code"] for row in rows if row.get("sample_quality") != "NO_SAMPLE"})
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if transitions else "NO_TRANSITION_HISTORY",
        "scope": "surge_state_transition_outcome",
        "target_minutes": TARGET_MINUTES,
        "transition_csv": str(transition_csv),
        "out_json": str(out_json),
        "out_csv": str(out_csv),
        "dry_run": bool(dry_run),
        "summary": {
            "raw_transition_rows": len(raw_transitions),
            "distinct_transition_rows": len(transitions),
            "outcome_rows": len(rows),
            "codes_with_any_sample": len(transitions_with_any_sample),
            "outcome_status_counts": dict(status_counts),
            "sample_quality_counts": dict(quality_counts),
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "policy_change": False,
        },
        "risk_contract": {
            "read_only_diagnostic": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
        },
        "rows": rows[:200],
    }
    if not dry_run:
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _write_csv(out_csv, rows)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only forward outcomes for surge state transitions.")
    parser.add_argument("--transition-csv", default=str(DEFAULT_TRANSITION_CSV))
    parser.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    payload = build_outcome(
        Path(args.transition_csv),
        Path(args.out_json),
        Path(args.out_csv),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps({
        "status": payload.get("status"),
        **payload.get("summary", {}),
        "out_json": payload.get("out_json"),
        "out_csv": payload.get("out_csv"),
        "dry_run": payload.get("dry_run"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
