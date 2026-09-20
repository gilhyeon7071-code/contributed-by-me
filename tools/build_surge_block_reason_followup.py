from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PRICE_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"

OUT_JSON = LOG_DIR / "surge_block_reason_followup_latest.json"
OUT_DETAIL_CSV = LOG_DIR / "surge_block_reason_followup_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "surge_block_reason_followup_summary_latest.csv"

HORIZONS = (1, 3, 5)


def _s(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _date8(value: Any) -> str:
    text = _s(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", text):
        text = text.split(".", 1)[0]
    digits = re.sub(r"\D", "", text)
    return digits[:8] if len(digits) >= 8 else ""


def _code(value: Any) -> str:
    text = _s(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", text):
        text = text.split(".", 1)[0]
    digits = re.sub(r"\D", "", text)
    return digits.zfill(6)[-6:] if digits else text.zfill(6)[-6:]


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _s(value).lower()
    return text in {"1", "true", "t", "yes", "y", "on"}


def _primary_reason(reason: Any) -> str:
    text = _s(reason) or "UNKNOWN"
    first = re.split(r"[|;,]", text, maxsplit=1)[0].strip()
    return re.sub(r"\(.*$", "", first).strip() or text


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _pending_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(LOG_DIR.glob("pending_entry_status*.json")):
        payload = _read_json(path)
        generated_at = _s(payload.get("generated_at"))
        for row in payload.get("entry_decision_rows") or []:
            if not isinstance(row, dict):
                continue
            if not _to_bool(row.get("is_surge")):
                continue
            signal = _s(row.get("signal")).upper()
            reason = _s(row.get("reason") or row.get("final_decision_reason"))
            primary = _primary_reason(reason or signal).upper()
            if signal in {"BUY", "BUY_READY"} or primary == "BUY_EXECUTED":
                continue
            code = _code(row.get("code"))
            signal_date = _date8(row.get("signal_date") or payload.get("d_ref"))
            if not code or not signal_date:
                continue
            events.append(
                {
                    "source_stage": "entry_decision",
                    "source_file": str(path),
                    "generated_at": generated_at,
                    "signal_date": signal_date,
                    "code": code,
                    "signal": signal,
                    "decision_reason": reason or signal or "UNKNOWN",
                    "primary_reason": _primary_reason(reason or signal),
                    "rank_score": row.get("rank_score"),
                    "surge_type": _s(row.get("surge_type_normalized") or row.get("surge_type")),
                }
            )
    return events


def _snapshot_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(LOG_DIR.glob("entry_signal_snapshot*.csv")):
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty or "code" not in df.columns:
            continue
        for _, row in df.iterrows():
            if not _to_bool(row.get("is_surge")):
                continue
            signal = _s(row.get("signal")).upper()
            reason = _s(row.get("final_decision_reason") or row.get("reason"))
            primary = _primary_reason(reason or signal).upper()
            if signal in {"BUY", "BUY_READY"} or primary == "BUY_EXECUTED":
                continue
            code = _code(row.get("code"))
            signal_date = _date8(row.get("signal_date") or row.get("d_ref"))
            if not code or not signal_date:
                continue
            events.append(
                {
                    "source_stage": "entry_snapshot",
                    "source_file": str(path),
                    "generated_at": _s(row.get("generated_at")),
                    "signal_date": signal_date,
                    "code": code,
                    "signal": signal,
                    "decision_reason": reason or signal or "UNKNOWN",
                    "primary_reason": _primary_reason(reason or signal),
                    "rank_score": row.get("rank_score"),
                    "surge_type": _s(row.get("surge_type_normalized") or row.get("surge_type")),
                }
            )
    return events


def _detector_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in [LOG_DIR / "surge_realtime_latest.csv"]:
        if not path.exists() or path.stat().st_size <= 5:
            continue
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except Exception:
            continue
        required = {"code", "date", "is_realtime_surge", "excluded_by_policy", "exclude_reasons"}
        if df.empty or not required.issubset(set(df.columns)):
            continue
        blocked = df[df["is_realtime_surge"].map(_to_bool) & df["excluded_by_policy"].map(_to_bool)]
        for _, row in blocked.iterrows():
            reason = _s(row.get("exclude_reasons"))
            code = _code(row.get("code"))
            signal_date = _date8(row.get("date"))
            if not code or not signal_date:
                continue
            events.append(
                {
                    "source_stage": "realtime_detector",
                    "source_file": str(path),
                    "generated_at": _s(row.get("ts")),
                    "signal_date": signal_date,
                    "code": code,
                    "signal": "BLOCKED",
                    "decision_reason": reason or "EXCLUDED_BY_POLICY",
                    "primary_reason": _primary_reason(reason or "EXCLUDED_BY_POLICY"),
                    "rank_score": row.get("surge_score_final") or row.get("surge_score"),
                    "surge_type": _s(row.get("surge_type")),
                }
            )
    return events


def _collect_events() -> pd.DataFrame:
    raw = _pending_events() + _snapshot_events() + _detector_events()
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df["_dedupe_key"] = (
        df["source_stage"].astype(str)
        + "|"
        + df["signal_date"].astype(str)
        + "|"
        + df["code"].astype(str)
        + "|"
        + df["primary_reason"].astype(str)
    )
    df = df.drop_duplicates("_dedupe_key", keep="last").drop(columns=["_dedupe_key"])
    return df.sort_values(["signal_date", "source_stage", "code", "primary_reason"]).reset_index(drop=True)


def _load_prices() -> tuple[pd.DataFrame, str]:
    if not PRICE_PATH.exists():
        return pd.DataFrame(), "missing_price_file"
    try:
        df = pd.read_parquet(PRICE_PATH)
    except Exception as exc:
        return pd.DataFrame(), f"price_read_failed:{type(exc).__name__}"
    required = {"date", "code", "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(), "missing_required_price_columns"
    out = df[list(required)].copy()
    out["date"] = out["date"].map(_date8)
    out["code"] = out["code"].map(_code)
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["date", "code", "close"]).sort_values(["code", "date"])
    return out, "ok"


def _markout_for_event(row: pd.Series, prices_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = _s(row.get("code"))
    signal_date = _date8(row.get("signal_date"))
    code_prices = prices_by_code.get(code)
    result: dict[str, Any] = {
        "base_close": None,
        "days_available": 0,
        "price_eval_status": "NO_PRICE_FOR_CODE",
    }
    for horizon in HORIZONS:
        result[f"ret_{horizon}d_close"] = None
    result["max_high_5d_ret"] = None
    result["min_low_5d_ret"] = None
    if code_prices is None or code_prices.empty:
        return result
    base_rows = code_prices[code_prices["date"] == signal_date]
    if base_rows.empty:
        result["price_eval_status"] = "NO_BASE_DATE_PRICE"
        return result
    base_close = float(base_rows.iloc[-1]["close"])
    if not math.isfinite(base_close) or base_close <= 0:
        result["price_eval_status"] = "INVALID_BASE_CLOSE"
        return result
    future = code_prices[code_prices["date"] > signal_date].head(max(HORIZONS))
    result["base_close"] = base_close
    result["days_available"] = int(len(future))
    if future.empty:
        result["price_eval_status"] = "PENDING_FUTURE_PRICE"
        return result
    for horizon in HORIZONS:
        if len(future) >= horizon:
            close_h = float(future.iloc[horizon - 1]["close"])
            result[f"ret_{horizon}d_close"] = close_h / base_close - 1.0
    first5 = future.head(5)
    if not first5.empty:
        result["max_high_5d_ret"] = float(first5["high"].max()) / base_close - 1.0
        result["min_low_5d_ret"] = float(first5["low"].min()) / base_close - 1.0
    result["price_eval_status"] = "EVALUATED" if len(future) >= 1 else "PENDING_FUTURE_PRICE"
    return result


def _with_markouts(events: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    if prices.empty:
        out = events.copy()
        out["price_eval_status"] = "PRICE_DATA_UNAVAILABLE"
        out["base_close"] = None
        out["days_available"] = 0
        for horizon in HORIZONS:
            out[f"ret_{horizon}d_close"] = None
        out["max_high_5d_ret"] = None
        out["min_low_5d_ret"] = None
        return out
    prices_by_code = {code: group.reset_index(drop=True) for code, group in prices.groupby("code", sort=False)}
    markouts = [_markout_for_event(row, prices_by_code) for _, row in events.iterrows()]
    return pd.concat([events.reset_index(drop=True), pd.DataFrame(markouts)], axis=1)


def _summary(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for (stage, reason), group in detail.groupby(["source_stage", "primary_reason"], dropna=False):
        row: dict[str, Any] = {
            "source_stage": stage,
            "primary_reason": reason,
            "events": int(len(group)),
            "codes": int(group["code"].nunique()),
        }
        for horizon in HORIZONS:
            col = f"ret_{horizon}d_close"
            values = pd.to_numeric(group[col], errors="coerce").dropna()
            row[f"evaluable_{horizon}d"] = int(len(values))
            row[f"mean_ret_{horizon}d"] = float(values.mean()) if len(values) else None
            row[f"median_ret_{horizon}d"] = float(values.median()) if len(values) else None
            row[f"positive_rate_{horizon}d"] = float((values > 0).mean()) if len(values) else None
        for col in ["max_high_5d_ret", "min_low_5d_ret"]:
            values = pd.to_numeric(group[col], errors="coerce").dropna()
            row[f"mean_{col}"] = float(values.mean()) if len(values) else None
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["source_stage", "events"], ascending=[True, False])


def _json_ready(value: Any) -> Any:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if pd.isna(value):
        return None
    return value


def main() -> int:
    generated_at = datetime.now().isoformat(timespec="seconds")
    events = _collect_events()
    prices, price_status = _load_prices()
    detail = _with_markouts(events, prices)
    summary = _summary(detail)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    detail.to_csv(OUT_DETAIL_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    evaluable_1d = int(pd.to_numeric(detail.get("ret_1d_close", pd.Series(dtype=float)), errors="coerce").notna().sum()) if not detail.empty else 0
    status = "OK" if len(detail) > 0 else "NO_BLOCKED_SURGE_EVENTS"
    evaluation_status = "EVALUABLE" if evaluable_1d > 0 else ("PENDING_OR_MISSING_FUTURE_PRICE" if len(detail) > 0 else "NO_EVENTS")
    payload = {
        "generated_at": generated_at,
        "status": status,
        "evaluation_status": evaluation_status,
        "price_status": price_status,
        "inputs": {
            "pending_entry_status_glob": str(LOG_DIR / "pending_entry_status*.json"),
            "entry_signal_snapshot_glob": str(LOG_DIR / "entry_signal_snapshot*.csv"),
            "surge_realtime_glob": str(LOG_DIR / "surge_realtime_*.csv"),
            "price_path": str(PRICE_PATH),
        },
        "outputs": {
            "detail_csv": str(OUT_DETAIL_CSV),
            "summary_csv": str(OUT_SUMMARY_CSV),
        },
        "counts": {
            "events": int(len(detail)),
            "summary_rows": int(len(summary)),
            "evaluable_1d": evaluable_1d,
            "evaluable_3d": int(pd.to_numeric(detail.get("ret_3d_close", pd.Series(dtype=float)), errors="coerce").notna().sum()) if not detail.empty else 0,
            "evaluable_5d": int(pd.to_numeric(detail.get("ret_5d_close", pd.Series(dtype=float)), errors="coerce").notna().sum()) if not detail.empty else 0,
        },
        "summary": [
            {key: _json_ready(value) for key, value in row.items()}
            for row in summary.to_dict(orient="records")
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": status, "evaluation_status": evaluation_status, "events": len(detail), "summary_rows": len(summary), "json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
