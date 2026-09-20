from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OUT_JSON = LOG_DIR / "surge_high_rejection_gray_zone_markout_watch_latest.json"
OUT_CSV = LOG_DIR / "surge_high_rejection_gray_zone_markout_watch_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_high_rejection_gray_zone_markout_watch_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_high_rejection_gray_zone_markout_watch_history.jsonl"
KST = timezone(timedelta(hours=9))
HORIZONS_MIN = (3, 5, 10)
GRAY_MIN = -0.04
GRAY_MAX = -0.025
MAX_SPREAD_BPS = 20.0
HARD_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK", "ENTRY_ATR_CAP", "RVOL_OVERHEAT_BLOCK", "SCORE_RVOL_OVERHEAT_BLOCK",
    "SPREAD_BLOCK", "NO_LOB_BLOCK", "ORDERFLOW_RISK_BLOCK", "ORDERFLOW_PAUSE",
    "ORDER_IMBALANCE_EXTREME", "KYLE_LAMBDA_Z_BLOCK", "MARKOUT_NEGATIVE_BLOCK",
    "OFI_NORM_EXTREME", "KRX_ADMIN", "KRX_WARNING", "KRX_RISK", "KRX_CAUTION",
    "TRADING_VALUE_BLOCK", "TRADING_VALUE_FLOOR", "LOW_TRADING_VALUE", "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK", "FEATURE_BLOCK", "FEATURE_MISSING_BLOCK", "INSUFFICIENT_FEATURES",
    "JUNK_SCORE", "JUNK_GRADE",
}
FIELDS = [
    "observation_key", "ts", "date", "code", "detected_surge_type", "gray_drawdown_pct",
    "change_pct", "surge_score_final", "rvol20", "trading_value", "lob_available", "lob_status",
    "spread_bps", "ask_depth_levels", "other_hard_blockers", "review_class", "markout_status",
    "base_price", "last_price", "ret_3m_pct", "ret_5m_pct", "ret_10m_pct", "ret_to_last_pct",
    "positive_forward_points", "negative_forward_points", "policy_change", "entry_approval_changed",
    "paper_order_route", "broker_order_route", "trading_allowed", "research_only", "must_not_dispatch",
]


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in FIELDS})


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _b(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6)[-6:] if text.isdigit() else text


def _dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _reason_keys(*values: Any) -> List[str]:
    keys: List[str] = []
    for value in values:
        for part in str(value or "").split("|"):
            token = part.strip()
            if token:
                key = token.split(":", 1)[0].strip().upper()
                if key and key not in keys:
                    keys.append(key)
    return keys


def _history_path(date: str) -> Path:
    return LOG_DIR / f"intraday_prices_history_{date}.csv"


def _price_points(date: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in _read_csv(_history_path(date)):
        code = _code(row.get("code"))
        ts = _dt(row.get("ts"))
        price = _f(row.get("current_price"), 0.0)
        if code and ts is not None and price > 0:
            out.setdefault(code, []).append({"ts": ts, "price": price})
    for code in out:
        out[code].sort(key=lambda x: x["ts"])
    return out


def _markout(row: Dict[str, Any], points_by_code: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    ts = _dt(row.get("ts"))
    points = points_by_code.get(_code(row.get("code")), [])
    if ts is None or not points:
        return {"markout_status": "MISSING_PRICE_HISTORY"}
    after = [p for p in points if p["ts"] >= ts]
    if not after:
        return {"markout_status": "NO_PRICE_AFTER_SIGNAL"}
    base = after[0]["price"]
    last = after[-1]["price"]
    out: Dict[str, Any] = {"markout_status": "EVALUATED", "base_price": base, "last_price": last}
    pos = neg = 0
    for h in HORIZONS_MIN:
        target = ts + timedelta(minutes=h)
        candidates = [p for p in after if p["ts"] >= target]
        if candidates:
            ret = (candidates[0]["price"] / base - 1.0) * 100.0
            out[f"ret_{h}m_pct"] = round(ret, 6)
            if ret > 0:
                pos += 1
            elif ret < 0:
                neg += 1
        else:
            out[f"ret_{h}m_pct"] = ""
    ret_last = (last / base - 1.0) * 100.0
    out["ret_to_last_pct"] = round(ret_last, 6)
    if ret_last > 0:
        pos += 1
    elif ret_last < 0:
        neg += 1
    out["positive_forward_points"] = pos
    out["negative_forward_points"] = neg
    return out


def _append_history(rows: List[Dict[str, Any]]) -> None:
    existing = _read_csv(HISTORY_CSV)
    merged: Dict[str, Dict[str, Any]] = {str(r.get("observation_key")): dict(r) for r in existing if r.get("observation_key")}
    for row in rows:
        merged[str(row.get("observation_key"))] = row
    _write_csv(HISTORY_CSV, list(merged.values()))
    with HISTORY_JSONL.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build() -> Dict[str, Any]:
    surge_rows = _read_csv(SURGE_CSV)
    date = next((str(r.get("date") or "").strip()[:8] for r in surge_rows if str(r.get("date") or "").strip()), "")
    points_by_code = _price_points(date) if date else {}
    rows: List[Dict[str, Any]] = []
    for row in surge_rows:
        drawdown = _f(row.get("intraday_high_drawdown_pct"))
        if not (GRAY_MIN < drawdown <= GRAY_MAX):
            continue
        reasons = _reason_keys(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
        if "HIGH_REJECTION_ENTRY_BLOCK" not in reasons:
            continue
        other = sorted(k for k in reasons if k in HARD_BLOCKERS and k != "HIGH_REJECTION_ENTRY_BLOCK")
        lob_ok = _b(row.get("lob_available")) and str(row.get("lob_status") or "").upper() == "OK"
        spread = _f(row.get("spread_bps"), 999999.0)
        if other:
            review_class = "REJECT_OTHER_HARD_BLOCKER"
        elif not lob_ok:
            review_class = "WATCH_NEEDS_LOB_OK"
        elif spread > MAX_SPREAD_BPS:
            review_class = "WATCH_SPREAD_TOO_WIDE"
        else:
            review_class = "WATCH_ONLY_NEEDS_MORE_EVIDENCE"
        out: Dict[str, Any] = {
            "observation_key": f"{row.get('date','')}|{_code(row.get('code'))}|{row.get('ts','')}|HIGH_REJECTION_GRAY",
            "ts": row.get("ts", ""),
            "date": row.get("date", ""),
            "code": _code(row.get("code")),
            "detected_surge_type": row.get("detected_surge_type", "") or row.get("surge_type", ""),
            "gray_drawdown_pct": round(drawdown, 6),
            "change_pct": round(_f(row.get("change_pct")), 6),
            "surge_score_final": round(_f(row.get("surge_score_final"), _f(row.get("surge_score"))), 6),
            "rvol20": round(_f(row.get("rvol20")), 6),
            "trading_value": round(_f(row.get("trading_value")), 6),
            "lob_available": _b(row.get("lob_available")),
            "lob_status": row.get("lob_status", ""),
            "spread_bps": round(spread, 6),
            "ask_depth_levels": round(_f(row.get("ask_depth_levels")), 6),
            "other_hard_blockers": "|".join(other),
            "review_class": review_class,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        }
        out.update(_markout(out, points_by_code))
        rows.append(out)
    _write_csv(OUT_CSV, rows)
    _append_history(rows)
    history_rows = _read_csv(HISTORY_CSV)
    class_counts: Dict[str, int] = {}
    for row in rows:
        class_counts[str(row.get("review_class"))] = class_counts.get(str(row.get("review_class")), 0) + 1
    payload = {
        "ts": _now(),
        "status": "OK",
        "scope": "surge_high_rejection_gray_zone_markout_watch",
        "source_files": {"surge_csv": str(SURGE_CSV), "price_history_csv": str(_history_path(date))},
        "outputs": {"latest_csv": str(OUT_CSV), "history_csv": str(HISTORY_CSV), "history_jsonl": str(HISTORY_JSONL)},
        "summary": {
            "gray_zone_rows": len(rows),
            "watch_only_rows": class_counts.get("WATCH_ONLY_NEEDS_MORE_EVIDENCE", 0),
            "history_rows": len(history_rows),
            "evaluable_rows": sum(1 for r in rows if r.get("markout_status") == "EVALUATED"),
            "review_class_counts": [{"class": k, "count": v} for k, v in sorted(class_counts.items())],
            "policy_change": False,
            "entry_approval_changed": False,
            "decision": "WATCH_ONLY_NO_ENTRY_APPROVAL",
        },
        "risk_contract": {
            "policy_change": False, "entry_approval_changed": False, "entry_signal": False,
            "live_order_allowed": False, "paper_order_route": False, "broker_order_route": False,
            "trading_route": False, "research_only": True, "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({"status": payload["status"], "summary": payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
