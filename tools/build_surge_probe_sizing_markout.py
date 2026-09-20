"""Build forward markouts for read-only surge probe sizing virtual ledger rows."""
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LEDGER_HISTORY = LOG_DIR / "surge_probe_sizing_virtual_ledger_history.csv"
OUT_JSON = LOG_DIR / "surge_probe_sizing_markout_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_sizing_markout_latest.csv"

TARGET_MINUTES = [1, 3, 5, 10, 15]
MAX_LAG_SECONDS = 90

FIELDS = [
    "ledger_id",
    "ledger_ts",
    "ymd",
    "code",
    "target_minutes",
    "target_ts",
    "base_price_ts",
    "base_price",
    "observed_price_ts",
    "observed_price",
    "lag_seconds",
    "ret_pct",
    "outcome_status",
    "sample_quality",
    "review_qty",
    "review_notional_krw",
    "estimated_loss_at_stop_krw",
    "probe_stop_loss_pct",
    "probe_timebox_min",
    "lob_quality_class",
    "spread_bps",
    "ask_depth_levels_live",
    "paper_order_route",
    "broker_order_route",
    "dispatch_enabled",
    "entry_approval_changed",
    "policy_change",
    "trading_allowed",
    "research_only",
    "must_not_dispatch",
]


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(fp)]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in FIELDS})


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
        text = str(value or "").replace(",", "").strip()
        if not text:
            return float(default)
        out = float(text)
        if out != out:
            return float(default)
        return out
    except Exception:
        return float(default)


def _b(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _history_path(ymd: str) -> Path:
    return LOG_DIR / f"intraday_prices_history_{ymd}.csv"


def _load_price_history(ymd: str) -> Dict[str, List[Tuple[datetime, float]]]:
    out: Dict[str, List[Tuple[datetime, float]]] = {}
    for row in _read_csv(_history_path(ymd)):
        code = str(row.get("code") or "").zfill(6)
        ts = _parse_ts(row.get("ts"))
        price = _f(row.get("current_price"))
        if not code or code == "000000" or ts is None or price <= 0:
            continue
        out.setdefault(code, []).append((ts, price))
    for code in out:
        out[code].sort(key=lambda item: item[0])
    return out


def _first_at_or_after(rows: List[Tuple[datetime, float]], target: datetime) -> Tuple[datetime, float] | None:
    for ts, price in rows:
        if ts >= target:
            return ts, price
    return None


def _dedupe_ledger(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set[str] = set()
    out: List[Dict[str, str]] = []
    for row in rows:
        ledger_id = str(row.get("ledger_id") or "")
        if not ledger_id or ledger_id in seen:
            continue
        seen.add(ledger_id)
        if str(row.get("ledger_status") or "") != "VIRTUAL_REVIEW_LOGGED":
            continue
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


def _build_rows(ledger_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    price_cache: Dict[str, Dict[str, List[Tuple[datetime, float]]]] = {}
    out: List[Dict[str, Any]] = []
    for row in ledger_rows:
        ledger_ts = _parse_ts(row.get("ledger_ts"))
        if ledger_ts is None:
            continue
        ymd = ledger_ts.strftime("%Y%m%d")
        code = str(row.get("code") or "").zfill(6)
        if ymd not in price_cache:
            price_cache[ymd] = _load_price_history(ymd)
        series = price_cache[ymd].get(code, [])
        base_obs = _first_at_or_after(series, ledger_ts)
        base_ts = base_obs[0] if base_obs else None
        base_price = base_obs[1] if base_obs else 0.0
        for target_min in TARGET_MINUTES:
            target_ts = ledger_ts + timedelta(minutes=target_min)
            obs = _first_at_or_after(series, target_ts)
            if base_price > 0 and obs:
                lag_seconds = max(0.0, (obs[0] - target_ts).total_seconds())
                ret_pct: float | None = (obs[1] / base_price - 1.0) * 100.0
                sample_quality = "ON_TIME_SAMPLE" if lag_seconds <= MAX_LAG_SECONDS else "LATE_SAMPLE"
            else:
                lag_seconds = None
                ret_pct = None
                sample_quality = "NO_SAMPLE"
            out.append({
                "ledger_id": row.get("ledger_id", ""),
                "ledger_ts": row.get("ledger_ts", ""),
                "ymd": ymd,
                "code": code,
                "target_minutes": target_min,
                "target_ts": target_ts.isoformat(timespec="seconds"),
                "base_price_ts": base_ts.isoformat(timespec="seconds") if base_ts else "",
                "base_price": round(base_price, 6) if base_price > 0 else "",
                "observed_price_ts": obs[0].isoformat(timespec="seconds") if obs else "",
                "observed_price": round(obs[1], 6) if obs else "",
                "lag_seconds": round(lag_seconds, 3) if lag_seconds is not None else "",
                "ret_pct": round(ret_pct, 6) if ret_pct is not None else "",
                "outcome_status": _outcome_status(ret_pct),
                "sample_quality": sample_quality,
                "review_qty": row.get("review_qty", ""),
                "review_notional_krw": row.get("review_notional_krw", ""),
                "estimated_loss_at_stop_krw": row.get("estimated_loss_at_stop_krw", ""),
                "probe_stop_loss_pct": row.get("probe_stop_loss_pct", ""),
                "probe_timebox_min": row.get("probe_timebox_min", ""),
                "lob_quality_class": row.get("lob_quality_class", ""),
                "spread_bps": row.get("spread_bps", ""),
                "ask_depth_levels_live": row.get("ask_depth_levels_live", ""),
                "paper_order_route": False,
                "broker_order_route": False,
                "dispatch_enabled": False,
                "entry_approval_changed": False,
                "policy_change": False,
                "trading_allowed": False,
                "research_only": True,
                "must_not_dispatch": True,
            })
    return out


def _summary(rows: List[Dict[str, Any]], ledger_count: int) -> Dict[str, Any]:
    by_quality = Counter(str(row.get("sample_quality") or "") for row in rows)
    by_outcome = Counter(str(row.get("outcome_status") or "") for row in rows)
    on_time = [row for row in rows if row.get("sample_quality") == "ON_TIME_SAMPLE"]
    returns = [_f(row.get("ret_pct")) for row in on_time if str(row.get("ret_pct") or "") != ""]
    return {
        "ledger_rows": ledger_count,
        "markout_rows": len(rows),
        "on_time_rows": len(on_time),
        "sample_quality_counts": dict(by_quality),
        "outcome_status_counts": dict(by_outcome),
        "avg_on_time_ret_pct": round(sum(returns) / len(returns), 6) if returns else 0.0,
        "paper_order_route": False,
        "broker_order_route": False,
        "dispatch_enabled": False,
        "entry_approval_changed": False,
        "policy_change": False,
        "trading_allowed": False,
    }


def main() -> int:
    ledger_rows = _dedupe_ledger(_read_csv(LEDGER_HISTORY))
    rows = _build_rows(ledger_rows)
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_probe_sizing_markout_read_only",
        "source_ledger_history_csv": str(LEDGER_HISTORY),
        "target_minutes": TARGET_MINUTES,
        "max_lag_seconds": MAX_LAG_SECONDS,
        "risk_contract": {
            "orders_exec_write": False,
            "fills_write": False,
            "rootb_ledger_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "summary": _summary(rows, ledger_count=len(ledger_rows)),
        "rows_preview": rows[:50],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({
        "status": "OK",
        "ledger_rows": len(ledger_rows),
        "markout_rows": len(rows),
        "on_time_rows": payload["summary"]["on_time_rows"],
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
