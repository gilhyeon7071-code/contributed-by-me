from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
ROOTB_LEDGER = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")

OUT_JSON = LOG_DIR / "surge_intraday_execution_validation_latest.json"
OUT_CSV = LOG_DIR / "surge_intraday_execution_validation_latest.csv"


def _norm_ymd(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))[:8]


def _norm_ts14(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))[:14]


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _extract_note_field(note: Any, key: str) -> str:
    try:
        m = re.search(rf"{re.escape(str(key))}=([^;]*)", str(note or ""))
        return str(m.group(1)).strip() if m else ""
    except Exception:
        return ""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def _load_intraday_history(ymd: str) -> pd.DataFrame:
    path = LOG_DIR / f"intraday_prices_history_{ymd}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if "code" not in df.columns:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str).str.zfill(6)
    if "date" in df.columns:
        df["date"] = df["date"].map(_norm_ymd)
        df = df[df["date"] == ymd]
    for col in ["current_price", "open", "high", "low", "volume", "trading_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["_ts14"] = df["ts"].map(_norm_ts14) if "ts" in df.columns else ""
    return df[pd.to_numeric(df.get("current_price", 0), errors="coerce").fillna(0.0) > 0].copy()


def _evaluate_intraday_reversal(row: Dict[str, Any], hist: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, Any]:
    code = str(row.get("code") or "").zfill(6)
    entry_ts = _norm_ts14(row.get("entry_ts") or row.get("datetime"))
    work = hist[hist["code"] == code].copy() if not hist.empty else pd.DataFrame()
    if entry_ts and "_ts14" in work.columns:
        work = work[(work["_ts14"] == "") | (work["_ts14"] >= entry_ts)].copy()
    min_points = max(2, int(cfg.get("min_points", 3) or 3))
    lookback = max(min_points, int(cfg.get("lookback_points", 5) or 5))
    if len(work) < min_points:
        return {"state": "INSUFFICIENT_POINTS", "points": int(len(work)), "signals": []}
    work = work.sort_values("_ts14").tail(lookback)
    prices = pd.to_numeric(work["current_price"], errors="coerce").dropna()
    if len(prices) < min_points:
        return {"state": "INSUFFICIENT_PRICE_POINTS", "points": int(len(prices)), "signals": []}

    entry_price = _to_float(row.get("entry_price") or row.get("price"), 0.0)
    max_close = _to_float(row.get("max_close"), entry_price)
    # The intraday snapshot high column is session-to-date and can include pre-entry highs.
    # Match paper_engine.py: reversal validation must use entry-or-later executable prices.
    high_cols = [float(entry_price or 0.0), float(prices.max())]
    if max_close <= max(high_cols):
        high_cols.append(float(max_close))
    high_since_entry = max(high_cols)
    current = float(prices.iloc[-1])
    drawdown = (current / high_since_entry - 1.0) if high_since_entry > 0 else 0.0
    signals: List[str] = []
    if drawdown <= -abs(float(cfg.get("high_rejection_min_drawdown_pct", 0.025) or 0.025)):
        signals.append("INTRADAY_HIGH_REJECTION")
    down_points = max(2, int(cfg.get("consecutive_down_points", 3) or 3))
    recent = prices.tail(down_points)
    if len(recent) >= down_points and bool((recent.diff().dropna() < 0).all()):
        signals.append("INTRADAY_LOWER_CLOSES")
    if "volume" in work.columns:
        vols = pd.to_numeric(work["volume"], errors="coerce").fillna(0.0)
        delta = vols.diff().clip(lower=0.0).fillna(vols)
        peak = float(delta.max() or 0.0)
        recent_avg = float(delta.tail(2).mean() or 0.0)
        if peak > 0 and (recent_avg / peak) <= float(cfg.get("volume_fade_ratio_max", 0.35) or 0.35):
            signals.append("INTRADAY_VOLUME_FADE")
    trigger_count = max(1, int(cfg.get("trigger_count", 2) or 2))
    require_high_rejection = bool(cfg.get("require_high_rejection_for_exit", False))
    missing_required_high_rejection = require_high_rejection and "INTRADAY_HIGH_REJECTION" not in signals
    triggered = bool(len(signals) >= trigger_count) and not missing_required_high_rejection
    return {
        "state": "TRIGGER" if triggered else "WATCH",
        "reason": (
            "missing_required_high_rejection"
            if missing_required_high_rejection and len(signals) >= trigger_count
            else ("triggered" if triggered else "not_triggered")
        ),
        "points": int(len(work)),
        "signals": signals,
        "trigger_count": int(trigger_count),
        "require_high_rejection_for_exit": bool(require_high_rejection),
        "current_price": current,
        "high_since_entry": high_since_entry,
        "drawdown_pct": drawdown,
        "latest_ts": str(work["_ts14"].iloc[-1]) if "_ts14" in work.columns and len(work) else "",
    }


def _load_config() -> Dict[str, Any]:
    path = PAPER_DIR / "paper_engine_config.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_broker_submit_map(ymd: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for path in sorted(PAPER_DIR.glob(f"orders_{ymd}_broker_submit_*.csv"), key=lambda p: p.stat().st_mtime):
        df = _read_csv(path)
        if df.empty:
            continue
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
        for _, row in df.iterrows():
            note = str(row.get("note", "") or "")
            keys = [
                str(row.get("entry_order_id", "") or "").strip(),
                _extract_note_field(note, "entry_order_id"),
                str(row.get("order_id", "") or "").strip(),
            ]
            rec = {
                "submit_path": str(path),
                "submit_price": _to_float(row.get("price"), 0.0),
                "order_type": str(row.get("order_type", "") or "").strip().lower(),
                "kis_api_call": _truthy(row.get("kis_api_call")),
                "apply": _truthy(row.get("apply")),
                "dispatch_status": str(row.get("dispatch_status", "") or ""),
                "ord_no": str(row.get("ord_no", "") or ""),
                "code": str(row.get("code", "") or "").zfill(6),
                "side": str(row.get("side", "") or "").strip().upper(),
            }
            for key in keys:
                if key:
                    out[key] = rec
    return out


def build_report(ymd: str) -> Dict[str, Any]:
    cfg = _load_config()
    intraday_cfg = (
        ((cfg.get("surge_exit_policy") or {}).get("intraday_reversal_exit") or {})
        if isinstance(cfg.get("surge_exit_policy"), dict)
        else {}
    )
    fills = _read_csv(PAPER_DIR / "fills.csv")
    state_path = PAPER_DIR / "paper_state.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    except Exception:
        state = {}
    hist = _load_intraday_history(ymd)
    ledger = _read_csv(ROOTB_LEDGER)
    broker_submit_map = _load_broker_submit_map(ymd)

    rows: List[Dict[str, Any]] = []
    if not fills.empty and "note" in fills.columns:
        fills = fills.copy()
        fills["date"] = fills["datetime"].map(_norm_ymd) if "datetime" in fills.columns else ""
        fills["code"] = fills["code"].astype(str).str.zfill(6) if "code" in fills.columns else ""
        fills["side"] = fills["side"].astype(str).str.upper() if "side" in fills.columns else ""
        note = fills["note"].astype(str)
        buys = fills[(fills["date"] == ymd) & (fills["side"] == "BUY") & note.str.contains("surge_immediate=1|surge_type=", regex=True, na=False)]
        for _, r in buys.iterrows():
            note_text = str(r.get("note", "") or "")
            pred_pct = _to_float(_extract_note_field(note_text, "surge_lob_slippage_pct"), math.nan)
            pred_src = _extract_note_field(note_text, "surge_lob_slippage_source")
            rec = {
                "kind": "BUY_SLIPPAGE",
                "datetime": r.get("datetime", ""),
                "code": str(r.get("code", "")).zfill(6),
                "qty": r.get("qty", ""),
                "price": r.get("price", ""),
                "predicted_lob_slippage_pct": pred_pct if math.isfinite(pred_pct) else "",
                "predicted_lob_slippage_bps": round(pred_pct * 10000.0, 4) if math.isfinite(pred_pct) else "",
                "predicted_lob_source": pred_src or "MISSING",
                "broker_submit_price": "",
                "broker_submit_order_type": "",
                "broker_submit_kis_api_call": "",
                "broker_submit_apply": "",
                "broker_submit_status": "",
                "broker_submit_path": "",
                "ledger_price": "",
                "ledger_slippage_actual_bps": "",
                "ledger_slippage_ref_source": "",
                "actual_market_slippage_bps": "",
                "predicted_vs_actual_error_bps": "",
                "actual_accuracy_state": "NO_BROKER_MARKET_FILL_BASELINE",
                "note": "paper_fill_has_lob_prediction_only",
            }
            broker_key = str(r.get("order_id", "") or "").strip() or _extract_note_field(note_text, "entry_order_id")
            broker_submit = broker_submit_map.get(broker_key, {})
            if broker_submit:
                rec["broker_submit_price"] = broker_submit.get("submit_price", "")
                rec["broker_submit_order_type"] = broker_submit.get("order_type", "")
                rec["broker_submit_kis_api_call"] = bool(broker_submit.get("kis_api_call", False))
                rec["broker_submit_apply"] = bool(broker_submit.get("apply", False))
                rec["broker_submit_status"] = broker_submit.get("dispatch_status", "")
                rec["broker_submit_path"] = broker_submit.get("submit_path", "")
            if not ledger.empty and "order_id" in ledger.columns and "order_id" in r.index:
                hit = ledger[ledger["order_id"].astype(str) == str(r.get("order_id", ""))]
                if not hit.empty:
                    lr = hit.iloc[-1]
                    rec["ledger_slippage_actual_bps"] = lr.get("slippage_actual_bps", "")
                    rec["ledger_slippage_ref_source"] = lr.get("slippage_ref_source", "")
                    rec["ledger_price"] = lr.get("price", lr.get("fill_price", ""))
                    submit_price = _to_float(rec.get("broker_submit_price"), 0.0)
                    fill_price = _to_float(rec.get("ledger_price"), _to_float(r.get("price"), 0.0))
                    if (
                        submit_price > 0
                        and fill_price > 0
                        and str(rec.get("broker_submit_order_type", "")).lower() == "market"
                        and bool(rec.get("broker_submit_kis_api_call", False))
                    ):
                        actual_bps = ((fill_price - submit_price) / submit_price) * 10000.0
                        rec["actual_market_slippage_bps"] = round(float(actual_bps), 4)
                        if math.isfinite(pred_pct):
                            rec["predicted_vs_actual_error_bps"] = round((pred_pct * 10000.0) - float(actual_bps), 4)
                        rec["actual_accuracy_state"] = "EVALUATED_BROKER_MARKET_SUBMIT"
                        rec["note"] = "broker_market_submit_price_vs_fill_price"
                    elif broker_submit:
                        rec["actual_accuracy_state"] = "NO_APPLIED_BROKER_MARKET_SUBMIT"
            rows.append(rec)

    for pos in list((state or {}).get("open_positions") or []):
        if not (str(pos.get("_surge_immediate", "")).lower() in {"1", "true", "yes"} or str(pos.get("surge_type", "") or "").strip()):
            continue
        if _norm_ymd(pos.get("entry_date")) != ymd:
            continue
        rev = _evaluate_intraday_reversal(pos, hist, intraday_cfg)
        rows.append({
            "kind": "OPEN_POSITION_REVERSAL",
            "datetime": pos.get("entry_ts", ""),
            "code": str(pos.get("code", "")).zfill(6),
            "qty": pos.get("qty", ""),
            "price": pos.get("entry_price", ""),
            "reversal_state": rev.get("state", ""),
            "reversal_signals": ",".join(rev.get("signals", []) or []),
            "reversal_points": rev.get("points", 0),
            "reversal_current_price": rev.get("current_price", ""),
            "reversal_high_since_entry": rev.get("high_since_entry", ""),
            "reversal_drawdown_pct": rev.get("drawdown_pct", ""),
            "reversal_latest_ts": rev.get("latest_ts", ""),
            "note": "intraday_history_based",
        })

    out_df = pd.DataFrame(rows)
    if not out_df.empty:
        out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    else:
        OUT_CSV.write_text("", encoding="utf-8-sig")

    slippage_rows = [r for r in rows if r.get("kind") == "BUY_SLIPPAGE"]
    reversal_rows = [r for r in rows if r.get("kind") == "OPEN_POSITION_REVERSAL"]
    evaluated_slip = [r for r in slippage_rows if r.get("actual_accuracy_state") == "EVALUATED_BROKER_MARKET_SUBMIT"]
    err_vals = pd.to_numeric(
        pd.Series([r.get("predicted_vs_actual_error_bps", "") for r in evaluated_slip]),
        errors="coerce",
    ).dropna()
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of": ymd,
        "status": "PASS",
        "rows": len(rows),
        "files": {
            "out_json": str(OUT_JSON),
            "out_csv": str(OUT_CSV),
            "fills": str(PAPER_DIR / "fills.csv"),
            "paper_state": str(state_path),
            "intraday_history": str(LOG_DIR / f"intraday_prices_history_{ymd}.csv"),
            "ledger": str(ROOTB_LEDGER),
        },
        "slippage_validation": {
            "surge_buy_rows": len(slippage_rows),
            "lob_prediction_rows": sum(1 for r in slippage_rows if str(r.get("predicted_lob_source", "")) != "MISSING"),
            "broker_submit_rows": len(broker_submit_map),
            "broker_market_accuracy_rows": len(evaluated_slip),
            "actual_accuracy_state": "EVALUATED" if evaluated_slip else "NO_BROKER_MARKET_FILL_BASELINE",
            "mean_abs_prediction_error_bps": float(err_vals.abs().mean()) if len(err_vals) else None,
            "max_abs_prediction_error_bps": float(err_vals.abs().max()) if len(err_vals) else None,
        },
        "intraday_reversal_validation": {
            "open_surge_positions": len(reversal_rows),
            "trigger_rows": sum(1 for r in reversal_rows if r.get("reversal_state") == "TRIGGER"),
            "watch_rows": sum(1 for r in reversal_rows if r.get("reversal_state") == "WATCH"),
            "insufficient_rows": sum(1 for r in reversal_rows if str(r.get("reversal_state", "")).startswith("INSUFFICIENT")),
            "config": intraday_cfg,
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="")
    args = parser.parse_args()
    ymd = _norm_ymd(args.date) or datetime.now().strftime("%Y%m%d")
    payload = build_report(ymd)
    print(json.dumps({
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "rows": payload.get("rows"),
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
