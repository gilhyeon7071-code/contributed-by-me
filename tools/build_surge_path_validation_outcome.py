from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INTENT_CSV = LOG_DIR / "surge_probe_intent_split_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OUT_JSON = LOG_DIR / "surge_path_validation_outcome_latest.json"
OUT_CSV = LOG_DIR / "surge_path_validation_outcome_latest.csv"
PATH_VALIDATION_CLASSES = {"PATH_VALIDATION_PROBE", "READ_ONLY_BLOCKED_PATH_VALIDATION"}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _history_path(ymd: str) -> Path:
    return LOG_DIR / f"intraday_prices_history_{ymd}.csv"


def _surge_history_paths(ymd: str) -> list[Path]:
    if not ymd:
        return []
    return sorted(LOG_DIR.glob(f"surge_realtime_{ymd}_*.csv"))


def _by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _signal_meta(ymd: str, code: str) -> dict[str, Any]:
    first_detected: dict[str, str] | None = None
    first_probe_eligible: dict[str, str] | None = None
    latest: dict[str, str] | None = None

    for path in _surge_history_paths(ymd):
        for row in _read_csv(path):
            if _code(row.get("code")) != code:
                continue
            if not _truthy(row.get("detected_surge_flag")):
                continue
            stamped = dict(row)
            stamped["_snapshot_path"] = str(path)
            latest = stamped
            if first_detected is None:
                first_detected = stamped
            if first_probe_eligible is None and (
                _truthy(row.get("entry_allowed")) or _truthy(row.get("no_lob_probe_allowed"))
            ):
                first_probe_eligible = stamped

    selected = first_probe_eligible or first_detected or latest or {}
    if selected is first_probe_eligible:
        source = "first_probe_eligible_ts"
    elif selected is first_detected:
        source = "first_detected_surge_ts"
    elif selected:
        source = "latest_historical_surge_ts"
    else:
        source = "latest_current_surge_ts"

    return {
        "selected": selected,
        "signal_ts_source": source,
        "first_detected_ts": (first_detected or {}).get("ts", ""),
        "first_probe_eligible_ts": (first_probe_eligible or {}).get("ts", ""),
        "latest_historical_ts": (latest or {}).get("ts", ""),
        "selected_snapshot_path": selected.get("_snapshot_path", "") if selected else "",
    }


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _history_by_code(path: Path) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for row in _read_csv(path):
        code = _code(row.get("code"))
        ts = _parse_ts(row.get("ts"))
        price = _float(row.get("current_price"))
        if not code or ts is None or price <= 0:
            continue
        out.setdefault(code, []).append(row)
    for rows in out.values():
        rows.sort(key=lambda row: str(row.get("ts") or ""))
    return out


def _price_at_or_after(rows: list[dict[str, str]], target_ts: datetime) -> tuple[float, str]:
    best_price = 0.0
    best_ts = ""
    for row in rows:
        ts = _parse_ts(row.get("ts"))
        if ts is None or ts < target_ts:
            continue
        best_price = _float(row.get("current_price"))
        best_ts = str(row.get("ts") or "")
        break
    return best_price, best_ts


def _price_at_or_after_points(points: list[tuple[datetime, float]], target_ts: datetime) -> tuple[float, str]:
    for ts, price in points:
        if ts >= target_ts:
            return price, ts.isoformat(timespec="seconds")
    return 0.0, ""


def _ret_pct(price: float, entry: float) -> Any:
    if price <= 0 or entry <= 0:
        return ""
    return round(price / entry - 1.0, 6)


def _evaluate_probe(
    intent: dict[str, str],
    surge: dict[str, str],
    hist_rows: list[dict[str, str]],
    signal: dict[str, Any],
) -> dict[str, Any]:
    signal_row = signal.get("selected") or {}
    signal_ts_text = signal_row.get("ts") or intent.get("ts") or surge.get("ts", "")
    signal_ts = _parse_ts(signal_ts_text)
    code = _code(intent.get("code"))
    entry_price = _float(signal_row.get("current_price"), _float(surge.get("current_price")))
    out: dict[str, Any] = {
        "code": code,
        "name": intent.get("name", ""),
        "signal_ts": signal_ts_text,
        "signal_ts_source": signal.get("signal_ts_source", ""),
        "first_detected_ts": signal.get("first_detected_ts", ""),
        "first_probe_eligible_ts": signal.get("first_probe_eligible_ts", ""),
        "latest_historical_ts": signal.get("latest_historical_ts", ""),
        "latest_intent_ts": intent.get("ts", ""),
        "selected_snapshot_path": signal.get("selected_snapshot_path", ""),
        "entry_reference_price": entry_price,
        "intent_class": intent.get("intent_class", ""),
        "intent_reason": intent.get("intent_reason", ""),
        "surge_type": intent.get("surge_type", ""),
        "change_pct": _float(intent.get("change_pct")),
        "remaining_to_30pct": _float(intent.get("remaining_to_30pct")),
        "lob_status": intent.get("lob_status", ""),
        "sanity_status": intent.get("sanity_status", ""),
        "history_points_after_signal": 0,
        "max_favorable_pct": "",
        "max_adverse_pct": "",
        "close_like_ret_pct": "",
        "outcome_class": "NOT_EVALUABLE",
        "outcome_reason": "missing_signal_or_history",
        "paper_order_route": False,
        "broker_order_route": False,
        "orders_modified": False,
        "fills_modified": False,
        "research_only": True,
    }
    if signal_ts is None or entry_price <= 0 or not hist_rows:
        return out

    after = []
    for row in hist_rows:
        ts = _parse_ts(row.get("ts"))
        price = _float(row.get("current_price"))
        if ts is not None and ts >= signal_ts and price > 0:
            after.append((ts, price))
    out["history_points_after_signal"] = len(after)
    if not after:
        out["outcome_reason"] = "no_history_after_signal"
        return out
    prices = [price for _, price in after]
    out["max_favorable_pct"] = round(max(prices) / entry_price - 1.0, 6)
    out["max_adverse_pct"] = round(min(prices) / entry_price - 1.0, 6)
    out["close_like_ret_pct"] = round(prices[-1] / entry_price - 1.0, 6)
    low_ts, low_price = min(after, key=lambda item: item[1])
    after_low = [(ts, price) for ts, price in after if ts >= low_ts]
    after_low_prices = [price for _, price in after_low]
    out["post_signal_low_ts"] = low_ts.isoformat(timespec="seconds")
    out["post_signal_low_price"] = round(low_price, 6)
    out["post_signal_low_ret_pct"] = _ret_pct(low_price, entry_price)
    out["rebound_from_post_low_pct"] = _ret_pct(after_low_prices[-1], low_price) if after_low_prices else ""
    out["max_rebound_from_post_low_pct"] = _ret_pct(max(after_low_prices), low_price) if after_low_prices else ""
    for minutes in (3, 5, 10):
        price, ts_text = _price_at_or_after_points(after_low, low_ts + timedelta(minutes=minutes))
        out[f"low_rebound_ret_{minutes}m_pct"] = _ret_pct(price, low_price)
        out[f"low_rebound_ret_{minutes}m_ts"] = ts_text
    out["v_rebound_path_status"] = "NOT_EVALUABLE"
    out["v_rebound_path_reason"] = "insufficient_post_low_history"
    rebound_5m = out.get("low_rebound_ret_5m_pct")
    rebound_10m = out.get("low_rebound_ret_10m_pct")
    max_rebound = out.get("max_rebound_from_post_low_pct")
    if rebound_5m != "" and float(rebound_5m) >= 0.003:
        out["v_rebound_path_status"] = "V_REBOUND_HELD_5M"
        out["v_rebound_path_reason"] = "post_low_rebound_positive_after_5m"
    elif rebound_10m != "" and float(rebound_10m) >= 0.003:
        out["v_rebound_path_status"] = "V_REBOUND_HELD_10M"
        out["v_rebound_path_reason"] = "post_low_rebound_positive_after_10m"
    elif max_rebound != "" and float(max_rebound) >= 0.005:
        out["v_rebound_path_status"] = "V_REBOUND_SPIKE_ONLY"
        out["v_rebound_path_reason"] = "post_low_rebound_seen_without_timepoint_hold"
    elif after_low:
        out["v_rebound_path_status"] = "NO_V_REBOUND"
        out["v_rebound_path_reason"] = "post_low_rebound_not_confirmed"
    for minutes in (3, 5, 10, 15):
        price, ts_text = _price_at_or_after(hist_rows, signal_ts + timedelta(minutes=minutes))
        out[f"ret_{minutes}m_pct"] = _ret_pct(price, entry_price)
        out[f"ret_{minutes}m_ts"] = ts_text

    max_fav = float(out["max_favorable_pct"])
    max_adv = float(out["max_adverse_pct"])
    close_ret = float(out["close_like_ret_pct"])
    if max_fav <= 0.001 and max_adv <= -0.01:
        out["outcome_class"] = "PATH_TEST_NEGATIVE"
        out["outcome_reason"] = "little_upside_with_adverse_move"
    elif close_ret > 0.003 or max_fav > 0.005:
        out["outcome_class"] = "PATH_TEST_POSITIVE"
        out["outcome_reason"] = "post_signal_continuation_observed"
    else:
        out["outcome_class"] = "PATH_TEST_NEUTRAL"
        out["outcome_reason"] = "no_clear_positive_expectancy"
    return out


def build() -> dict[str, Any]:
    intents = [row for row in _read_csv(INTENT_CSV) if row.get("intent_class") in PATH_VALIDATION_CLASSES]
    surge_by_code = _by_code(_read_csv(SURGE_CSV))
    ymds = sorted({str(row.get("date") or "") for row in intents if str(row.get("date") or "")})
    histories: dict[str, dict[str, list[dict[str, str]]]] = {
        ymd: _history_by_code(_history_path(ymd)) for ymd in ymds
    }
    rows: list[dict[str, Any]] = []
    for intent in intents:
        code = _code(intent.get("code"))
        ymd = str(intent.get("date") or "")
        rows.append(
            _evaluate_probe(
                intent,
                surge_by_code.get(code, {}),
                histories.get(ymd, {}).get(code, []),
                _signal_meta(ymd, code),
            )
        )
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("outcome_class") or "")
        counts[key] = counts.get(key, 0) + 1
    intent_counts: dict[str, int] = {}
    for row in intents:
        key = str(row.get("intent_class") or "")
        intent_counts[key] = intent_counts.get(key, 0) + 1
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_path_validation_outcome",
        "summary": {
            "path_validation_probe_rows": len(intents),
            "intent_class_counts": dict(sorted(intent_counts.items())),
            "direct_path_validation_probe_rows": int(intent_counts.get("PATH_VALIDATION_PROBE", 0)),
            "read_only_blocked_path_validation_rows": int(
                intent_counts.get("READ_ONLY_BLOCKED_PATH_VALIDATION", 0)
            ),
            "outcome_rows": len(rows),
            "outcome_counts": counts,
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "source_files": {
            "intent_split": str(INTENT_CSV),
            "surge": str(SURGE_CSV),
            "surge_history_template": str(LOG_DIR / "surge_realtime_YYYYMMDD_HHMMSS.csv"),
            "history_template": str(LOG_DIR / "intraday_prices_history_YYYYMMDD.csv"),
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "code",
        "name",
        "signal_ts",
        "signal_ts_source",
        "first_detected_ts",
        "first_probe_eligible_ts",
        "latest_historical_ts",
        "latest_intent_ts",
        "selected_snapshot_path",
        "entry_reference_price",
        "intent_class",
        "intent_reason",
        "surge_type",
        "change_pct",
        "remaining_to_30pct",
        "lob_status",
        "sanity_status",
        "history_points_after_signal",
        "ret_3m_pct",
        "ret_3m_ts",
        "ret_5m_pct",
        "ret_5m_ts",
        "ret_10m_pct",
        "ret_10m_ts",
        "ret_15m_pct",
        "ret_15m_ts",
        "post_signal_low_ts",
        "post_signal_low_price",
        "post_signal_low_ret_pct",
        "rebound_from_post_low_pct",
        "max_rebound_from_post_low_pct",
        "low_rebound_ret_3m_pct",
        "low_rebound_ret_3m_ts",
        "low_rebound_ret_5m_pct",
        "low_rebound_ret_5m_ts",
        "low_rebound_ret_10m_pct",
        "low_rebound_ret_10m_ts",
        "v_rebound_path_status",
        "v_rebound_path_reason",
        "max_favorable_pct",
        "max_adverse_pct",
        "close_like_ret_pct",
        "outcome_class",
        "outcome_reason",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    _write_csv(OUT_CSV, rows, fields)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
