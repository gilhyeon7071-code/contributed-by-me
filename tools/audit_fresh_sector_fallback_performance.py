"""Audit open/closed performance of fresh sector fallback buys.

Read-only. This keeps fallback-policy samples separate from normal-entry
baseline samples. It does not change orders, fills, ledger, gates, scores,
risk, or policy values.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

PATH_AUDIT_JSON = LOG_DIR / "fresh_sector_fallback_path_audit_latest.json"
FILLS_CSV = PAPER_DIR / "fills.csv"
TRADES_CSV = PAPER_DIR / "trades.csv"
STATE_JSON = PAPER_DIR / "paper_state.json"
INTRADAY_CSV = LOG_DIR / "intraday_prices_latest.csv"
INTRADAY_STATUS_JSON = LOG_DIR / "intraday_prices_status_latest.json"

OUT_JSON = LOG_DIR / "fresh_sector_fallback_performance_latest.json"
OUT_CSV = LOG_DIR / "fresh_sector_fallback_performance_latest.csv"


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


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _buy_fills_by_order(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        oid = str(row.get("order_id") or "").strip()
        if oid:
            out[oid] = row
    return out


def _trades_by_order(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        note = str(row.get("note") or "")
        source = ""
        for token in note.split(";"):
            if token.startswith("source_order_id="):
                source = token.split("=", 1)[1].strip()
                break
        if not source:
            source = str(row.get("trade_id") or "").strip()
        if source:
            out[source] = row
    return out


def _open_positions_by_order(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    positions = state.get("open_positions") if isinstance(state.get("open_positions"), list) else []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        oid = str(pos.get("entry_order_id") or pos.get("source_order_id") or "").strip()
        if oid:
            out[oid] = pos
    return out


def _intraday_by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "buy_order_id",
        "entry_ts",
        "entry_price",
        "qty",
        "state",
        "current_price",
        "unrealized_ret_pct",
        "closed_pnl_pct",
        "exit_reason",
        "sample_group",
        "decision_use",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    path_audit = _read_json(PATH_AUDIT_JSON)
    source_rows = path_audit.get("rows") if isinstance(path_audit.get("rows"), list) else []
    fallback_rows = [r for r in source_rows if str(r.get("verdict") or "") == "FALLBACK_ONLY_PATH"]

    buy_fills = _buy_fills_by_order(_read_csv(FILLS_CSV))
    trades = _trades_by_order(_read_csv(TRADES_CSV))
    open_positions = _open_positions_by_order(_read_json(STATE_JSON))
    prices = _intraday_by_code(_read_csv(INTRADAY_CSV))
    price_status = _read_json(INTRADAY_STATUS_JSON)

    rows: List[Dict[str, Any]] = []
    closed = 0
    open_n = 0
    ret_values: List[float] = []
    for src in fallback_rows:
        oid = str(src.get("buy_order_id") or "").strip()
        fill = buy_fills.get(oid, {})
        code = _code(src.get("code") or fill.get("code"))
        entry_price = _f(fill.get("price"))
        qty = _f(fill.get("qty"))
        trade = trades.get(oid, {})
        pos = open_positions.get(oid, {})
        px_row = prices.get(code, {})
        current_price = _f(px_row.get("current_price"), 0.0)
        closed_pnl = None
        unrealized = None
        if trade:
            closed += 1
            closed_pnl = _f(trade.get("pnl_pct"))
            state = "CLOSED"
            ret_values.append(closed_pnl)
        elif pos:
            open_n += 1
            state = "OPEN"
            if entry_price > 0 and current_price > 0:
                unrealized = (current_price / entry_price - 1.0) * 100.0
                ret_values.append(unrealized)
        else:
            state = "UNKNOWN"

        rows.append(
            {
                "code": code,
                "name": src.get("name", ""),
                "buy_order_id": oid,
                "entry_ts": fill.get("datetime") or pos.get("entry_ts", ""),
                "entry_price": entry_price,
                "qty": qty,
                "state": state,
                "current_price": current_price if current_price > 0 else "",
                "unrealized_ret_pct": round(unrealized, 6) if unrealized is not None else "",
                "closed_pnl_pct": round(closed_pnl, 6) if closed_pnl is not None else "",
                "exit_reason": trade.get("exit_reason", ""),
                "sample_group": "fresh_sector_allowed_fallback",
                "decision_use": "fallback_policy_review_only_not_normal_baseline",
            }
        )

    avg_ret = sum(ret_values) / len(ret_values) if ret_values else None
    if not rows:
        recommendation = "NO_FALLBACK_SAMPLE_TO_EVALUATE"
    elif closed == 0:
        recommendation = "DEFER_POLICY_DECISION_UNTIL_CLOSED_OR_MORE_SAMPLES"
    elif len(rows) < 5:
        recommendation = "KEEP_SEPARATE_AND_COLLECT_MORE_FALLBACK_SAMPLES"
    elif avg_ret is not None and avg_ret < 0:
        recommendation = "REVIEW_NARROW_OR_SHADOW_ONLY"
    else:
        recommendation = "KEEP_ENABLED_WITH_SEPARATE_MONITORING"

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "fresh_sector_fallback_performance_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "generated_from": {
            "path_audit": str(PATH_AUDIT_JSON),
            "fills": str(FILLS_CSV),
            "trades": str(TRADES_CSV),
            "state": str(STATE_JSON),
            "intraday_prices": str(INTRADAY_CSV),
            "intraday_price_status": str(INTRADAY_STATUS_JSON),
        },
        "summary": {
            "sample_group": "fresh_sector_allowed_fallback",
            "sample_count": len(rows),
            "open_count": open_n,
            "closed_count": closed,
            "average_current_or_closed_ret_pct": round(avg_ret, 6) if avg_ret is not None else None,
            "recommendation": recommendation,
            "baseline_policy": "exclude_from_normal_entry_baseline",
            "price_status": {
                "ts": price_status.get("ts"),
                "codes_ok": price_status.get("codes_ok"),
                "codes_failed": price_status.get("codes_failed"),
                "fallback_mode": price_status.get("fallback_mode"),
                "hoga_fallback_mode": price_status.get("hoga_fallback_mode"),
            },
        },
        "rows": rows,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
