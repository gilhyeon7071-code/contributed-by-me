from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OUT_JSON = LOG_DIR / "surge_entry_change_clean_continuation_watch_latest.json"
OUT_CSV = LOG_DIR / "surge_entry_change_clean_continuation_watch_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_entry_change_clean_continuation_watch_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_entry_change_clean_continuation_watch_history.jsonl"
KST = timezone(timedelta(hours=9))
HORIZONS_MIN = (3, 5, 10)
MAX_SPREAD_BPS = 20.0
MAX_RVOL20 = 5.0
HIGH_REJECTION_LIMIT = -0.025
OVERHEAT_SCORE_MIN = 90.0
OVERHEAT_RVOL20_MIN = 3.0
ENTRY_MIN = 0.16
ENTRY_MAX = 0.20
HARD_BLOCKERS = {
    "ENTRY_ATR_CAP", "HIGH_REJECTION_ENTRY_BLOCK", "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK", "SPREAD_BLOCK", "NO_LOB_BLOCK",
    "ORDERFLOW_RISK_BLOCK", "ORDERFLOW_PAUSE", "ORDER_IMBALANCE_EXTREME",
    "KYLE_LAMBDA_Z_BLOCK", "MARKOUT_NEGATIVE_BLOCK", "OFI_NORM_EXTREME",
    "KRX_ADMIN", "KRX_WARNING", "KRX_RISK", "KRX_CAUTION",
    "TRADING_VALUE_BLOCK", "TRADING_VALUE_FLOOR", "LOW_TRADING_VALUE",
    "NEWS_NEGATIVE", "NEWS_IMPLICATION_BLOCK", "FEATURE_BLOCK",
    "FEATURE_MISSING_BLOCK", "INSUFFICIENT_FEATURES", "JUNK_SCORE", "JUNK_GRADE",
}
FIELDS = [
    "observation_key", "ts", "date", "code", "detected_surge_type", "change_pct",
    "surge_score_final", "rvol20", "intraday_high_drawdown_pct", "lob_available",
    "lob_status", "spread_bps", "ask_depth_levels", "remaining_hard_blockers",
    "clean_16_20_candidate", "failed_conditions", "markout_status",
    "base_price", "last_price", "ret_3m_pct", "ret_5m_pct", "ret_10m_pct",
    "ret_to_last_pct", "positive_forward_points", "negative_forward_points",
    "policy_change", "entry_approval_changed", "paper_order_route", "broker_order_route",
    "trading_allowed", "research_only", "must_not_dispatch",
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
    rows = _read_csv(_history_path(date))
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        code = _code(row.get("code"))
        ts = _dt(row.get("ts"))
        price = _f(row.get("current_price"), 0.0)
        if not code or ts is None or price <= 0:
            continue
        out.setdefault(code, []).append({"ts": ts, "price": price})
    for code in out:
        out[code].sort(key=lambda x: x["ts"])
    return out


def _markout(row: Dict[str, Any], points_by_code: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    ts = _dt(row.get("ts"))
    code = _code(row.get("code"))
    points = points_by_code.get(code, [])
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


def _append_history(rows: List[Dict[str, Any]], date: str) -> None:
    existing = _read_csv(HISTORY_CSV)
    merged: Dict[str, Dict[str, Any]] = {}
    for r in existing:
        key = str(r.get("observation_key") or "")
        if not key:
            continue
        if str(r.get("date") or "") == date and key.endswith("|ENTRY_CHANGE_16_20"):
            continue
        merged[key] = dict(r)
    for row in rows:
        merged[str(row.get("observation_key"))] = row
    _write_csv(HISTORY_CSV, list(merged.values()))
    with HISTORY_JSONL.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build() -> Dict[str, Any]:
    surge_rows = _read_csv(SURGE_CSV)
    date = ""
    for row in surge_rows:
        if str(row.get("date") or "").strip():
            date = str(row.get("date")).strip()[:8]
            break
    points_by_code = _price_points(date) if date else {}
    rows: List[Dict[str, Any]] = []
    for row in surge_rows:
        change = _f(row.get("change_pct"))
        if not (ENTRY_MIN < change <= ENTRY_MAX):
            continue
        reasons = _reason_keys(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
        if not (_b(row.get("is_realtime_surge")) or "ENTRY_CHANGE_BLOCK" in reasons):
            continue
        score = _f(row.get("surge_score_final"), _f(row.get("surge_score")))
        rvol = _f(row.get("rvol20"))
        drawdown = _f(row.get("intraday_high_drawdown_pct"))
        spread = _f(row.get("spread_bps"), 999999.0)
        remaining = sorted(k for k in reasons if k in HARD_BLOCKERS and k != "ENTRY_CHANGE_BLOCK")
        failed: List[str] = []
        if remaining:
            failed.append("OTHER_HARD_BLOCKER:" + ",".join(remaining))
        if not _b(row.get("is_realtime_surge")):
            failed.append("NOT_REALTIME_SURGE")
        if not (_b(row.get("lob_available")) and str(row.get("lob_status") or "").upper() == "OK"):
            failed.append("LOB_NOT_OK")
        if spread > MAX_SPREAD_BPS:
            failed.append("SPREAD_GT_20BPS")
        if drawdown <= HIGH_REJECTION_LIMIT:
            failed.append("HIGH_REJECTION_DRAWDOWN")
        if rvol > MAX_RVOL20:
            failed.append("RVOL_GT_5")
        if score >= OVERHEAT_SCORE_MIN and rvol >= OVERHEAT_RVOL20_MIN:
            failed.append("SCORE_RVOL_OVERHEAT")
        out: Dict[str, Any] = {
            "observation_key": f"{row.get('date','')}|{_code(row.get('code'))}|{row.get('ts','')}|ENTRY_CHANGE_16_20",
            "ts": row.get("ts", ""),
            "date": row.get("date", ""),
            "code": _code(row.get("code")),
            "detected_surge_type": row.get("detected_surge_type", "") or row.get("surge_type", ""),
            "change_pct": round(change, 6),
            "surge_score_final": round(score, 6),
            "rvol20": round(rvol, 6),
            "intraday_high_drawdown_pct": round(drawdown, 6),
            "lob_available": _b(row.get("lob_available")),
            "lob_status": row.get("lob_status", ""),
            "spread_bps": round(spread, 6),
            "ask_depth_levels": round(_f(row.get("ask_depth_levels")), 6),
            "remaining_hard_blockers": "|".join(remaining),
            "clean_16_20_candidate": not failed,
            "failed_conditions": "|".join(failed),
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
    _append_history(rows, date)
    history_rows = _read_csv(HISTORY_CSV)
    payload = {
        "ts": _now(),
        "status": "OK",
        "scope": "surge_entry_change_clean_continuation_watch",
        "source_files": {"surge_csv": str(SURGE_CSV), "price_history_csv": str(_history_path(date))},
        "outputs": {"latest_csv": str(OUT_CSV), "history_csv": str(HISTORY_CSV), "history_jsonl": str(HISTORY_JSONL)},
        "summary": {
            "review_rows": len(rows),
            "clean_candidate_rows": sum(1 for r in rows if r.get("clean_16_20_candidate") is True),
            "history_rows": len(history_rows),
            "evaluable_rows": sum(1 for r in rows if r.get("markout_status") == "EVALUATED"),
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
