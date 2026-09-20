"""Archive read-only markout evidence for no-trade/blocker decisions.

This tool preserves the current entry decision layer before later runs overwrite
``*_latest`` artifacts. It only writes diagnostic artifacts and history rows; it
does not change gates, policies, orders, fills, or ledger state.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

ENTRY_LAYER_JSON = LOG_DIR / "entry_decision_layers_runtime_latest.json"
ENTRY_LAYER_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
FILL_QUALITY_CSV = LOG_DIR / "normal_entry_fill_quality_report_latest.csv"
INTRADAY_LATEST_CSV = LOG_DIR / "intraday_prices_latest.csv"

OUT_JSON = LOG_DIR / "no_trade_blocker_markout_archive_latest.json"
OUT_CSV = LOG_DIR / "no_trade_blocker_markout_archive_latest.csv"
HISTORY_CSV = LOG_DIR / "no_trade_blocker_markout_archive_history.csv"
HISTORY_JSONL = LOG_DIR / "no_trade_blocker_markout_archive_history.jsonl"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


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
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _append_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _f(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _blocker_bucket(reason: str) -> str:
    reason_u = str(reason or "").upper()
    if "NORMAL_INTRADAY_MOMENTUM_BLOCK" in reason_u:
        return "NORMAL_MOMENTUM"
    if "NORMAL_SPREAD_BLOCK" in reason_u:
        return "NORMAL_SPREAD"
    if "NORMAL_LOB" in reason_u:
        return "NORMAL_LOB"
    if "NORMAL_MARKOUT" in reason_u:
        return "NORMAL_MARKOUT"
    if "CAP_SIGNALDATE_TOP" in reason_u:
        return "SIGNALDATE_CAP"
    if "GAP_RISK" in reason_u:
        return "GAP_RISK"
    if "BUY_EXECUTED" in reason_u:
        return "EXECUTED"
    if reason_u:
        return "OTHER_BLOCK"
    return "NO_REASON"


def _validation_read(ret_pct: float | None) -> str:
    if ret_pct is None:
        return "NO_PRICE_EVIDENCE"
    if ret_pct <= -1.0:
        return "BLOCK_HELPED_AVOID_LOSS"
    if ret_pct >= 1.0:
        return "BLOCK_MISSED_GAIN_CANDIDATE"
    return "BLOCK_NEUTRAL_SO_FAR"


def _markout_row(
    observed_at: str,
    d_ref: str,
    row: Dict[str, str],
    fill_quality: Dict[str, str],
    latest_price_row: Dict[str, str],
) -> Dict[str, Any]:
    code = _code(row.get("code"))
    reason = str(row.get("execution_reason") or "").strip()
    entry_price = (
        _f(fill_quality.get("simulated_fill_price"))
        or _f(fill_quality.get("entry_price_close_basis"))
        or _f(row.get("normal_executable_price"))
        or _f(row.get("normal_close_basis_entry_price"))
    )
    latest_price = _f(latest_price_row.get("current_price"))
    ret_pct = (latest_price / entry_price - 1.0) * 100.0 if entry_price and latest_price else None
    v_accel = _f(row.get("normal_intraday_rechecked_v_accel"))
    value_ratio = _f(row.get("normal_intraday_value_ratio"))
    blocker_bucket = _blocker_bucket(reason)
    positive_entry_ok = _truthy(row.get("positive_entry_ok"))
    current_price_ts = latest_price_row.get("ts") or latest_price_row.get("date") or ""
    return {
        "observed_at": observed_at,
        "d_ref": d_ref,
        "code": code,
        "name": row.get("name", ""),
        "rank_score": _f(row.get("rank_score")),
        "positive_entry_ok": positive_entry_ok,
        "execution_layer": row.get("execution_layer", ""),
        "execution_reason": reason,
        "blocker_bucket": blocker_bucket,
        "opportunity_state": row.get("opportunity_state", ""),
        "next_action": row.get("next_action", ""),
        "entry_gate_decision": row.get("entry_gate_decision", ""),
        "entry_gate_reason": row.get("entry_gate_reason", ""),
        "normal_intraday_rechecked_v_accel": v_accel,
        "normal_intraday_value_ratio": value_ratio,
        "entry_price_basis": entry_price,
        "latest_price": latest_price,
        "latest_price_ts": current_price_ts,
        "return_to_latest_pct": round(ret_pct, 6) if ret_pct is not None else None,
        "validation_read": _validation_read(ret_pct),
        "read_only_for_trading": True,
        "live_order_route_enabled": False,
        "entry_approval_changed": False,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    bucket_counts = Counter(str(row.get("blocker_bucket") or "") for row in rows)
    read_counts = Counter(str(row.get("validation_read") or "") for row in rows)
    priced = [row for row in rows if row.get("return_to_latest_pct") is not None]
    returns = [float(row["return_to_latest_pct"]) for row in priced]
    by_bucket: Dict[str, Dict[str, Any]] = {}
    for bucket in sorted(bucket_counts):
        subset = [row for row in rows if row.get("blocker_bucket") == bucket]
        vals = [float(row["return_to_latest_pct"]) for row in subset if row.get("return_to_latest_pct") is not None]
        by_bucket[bucket] = {
            "n": len(subset),
            "priced_n": len(vals),
            "avg_return_to_latest_pct": round(sum(vals) / len(vals), 6) if vals else None,
            "reads": dict(Counter(str(row.get("validation_read") or "") for row in subset)),
        }
    return {
        "rows": len(rows),
        "priced_rows": len(priced),
        "avg_return_to_latest_pct": round(sum(returns) / len(returns), 6) if returns else None,
        "blocker_bucket_counts": dict(bucket_counts),
        "validation_read_counts": dict(read_counts),
        "by_bucket": by_bucket,
        "live_order_route_enabled": False,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def main() -> int:
    observed_at = _now_ts()
    entry_layer_json = _read_json(ENTRY_LAYER_JSON)
    entry_rows = _read_csv(ENTRY_LAYER_CSV)
    fill_quality_by_code = _by_code(_read_csv(FILL_QUALITY_CSV))
    latest_by_code = _by_code(_read_csv(INTRADAY_LATEST_CSV))
    d_ref = str(entry_layer_json.get("d_ref") or entry_layer_json.get("as_of_ymd") or "")

    candidate_rows = [
        row
        for row in entry_rows
        if _code(row.get("code"))
        and (
            str(row.get("execution_reason") or "").strip()
            or str(row.get("execution_layer") or "").strip()
            or _truthy(row.get("positive_entry_ok"))
        )
    ]
    rows = [
        _markout_row(
            observed_at,
            d_ref,
            row,
            fill_quality_by_code.get(_code(row.get("code")), {}),
            latest_by_code.get(_code(row.get("code")), {}),
        )
        for row in candidate_rows
    ]
    fields = [
        "observed_at",
        "d_ref",
        "code",
        "name",
        "rank_score",
        "positive_entry_ok",
        "execution_layer",
        "execution_reason",
        "blocker_bucket",
        "opportunity_state",
        "next_action",
        "entry_gate_decision",
        "entry_gate_reason",
        "normal_intraday_rechecked_v_accel",
        "normal_intraday_value_ratio",
        "entry_price_basis",
        "latest_price",
        "latest_price_ts",
        "return_to_latest_pct",
        "validation_read",
        "read_only_for_trading",
        "live_order_route_enabled",
        "entry_approval_changed",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    payload = {
        "generated_at": observed_at,
        "status": "PASS",
        "scope": "read_only_no_trade_blocker_markout_archive",
        "policy_note": "Diagnostic archive only. No entry gate, order route, fill, ledger, threshold, or policy behavior is changed.",
        "source_files": {
            "entry_layer_json": str(ENTRY_LAYER_JSON),
            "entry_layer_csv": str(ENTRY_LAYER_CSV),
            "fill_quality_csv": str(FILL_QUALITY_CSV),
            "intraday_latest_csv": str(INTRADAY_LATEST_CSV),
        },
        "summary": _summary(rows),
        "rows": rows,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows, fields)
    _append_csv(HISTORY_CSV, rows, fields)
    _append_jsonl(HISTORY_JSONL, rows)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "out_json": str(OUT_JSON),
                "out_csv": str(OUT_CSV),
                "history_csv": str(HISTORY_CSV),
                "history_jsonl": str(HISTORY_JSONL),
                "summary": payload["summary"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
