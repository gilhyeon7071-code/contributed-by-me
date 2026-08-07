"""I/O helpers for paper_engine.

Includes directory creation, CSV header/schema handling, and state persistence.
Split out from the legacy ``paper_engine.py`` as Phase 2 of the module refactor.
"""

from __future__ import annotations


__all__ = [
    'ensure_dirs',
    'read_header',
    'detect_schema',
    '_write_dashboard_compat_csv',
    'ensure_csv',
    '_migrate_legacy_trades_header_if_needed',
    '_default_state',
    '_normalize_state_payload',
    'load_state',
    '_json_safe',
    'save_state',
    '_latest_lob_row_for_code',
    '_load_intraday_history_for_ymd',
    '_intraday_price_window_since_entry',
    '_derive_d_from_fills_path',
    '_write_market_event_guard_status',
]

import csv
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from paper_engine.common import (
    _extract_ymd_from_ts_text,
    _norm_ts14,
    _norm_ymd_text,
    _path_from_env,
)
from utils.common import now_ymd
from state_containers import (
    PortfolioState,
    json_safe as _state_json_safe,
    read_json_text_with_fallback,
)

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"
ROOTB_DIR = BASE_DIR.parent / "vibe" / "buffett"


FILLS = _path_from_env("PAPER_FILLS_PATH", PAPER_DIR / "fills.csv")
TRADES = _path_from_env("PAPER_TRADES_PATH", PAPER_DIR / "trades.csv")
TRADES_CALC = _path_from_env("PAPER_TRADES_CALC_PATH", PAPER_DIR / "trades_calc.csv")
STATE_PATH = _path_from_env("PAPER_STATE_PATH", PAPER_DIR / "paper_state.json")
T2_SETTLEMENT_CASH_STATUS_PATH = _path_from_env(
    "PAPER_T2_SETTLEMENT_CASH_STATUS_PATH",
    LOG_DIR / "t2_settlement_cash_status_latest.json",
)

# Artifact paths used by guard/evaluation modules.
PRODUCTION_RISK_PLAYBOOK_PATH = _path_from_env(
    "PAPER_PRODUCTION_RISK_PLAYBOOK_PATH",
    LOG_DIR / "production_risk_playbook_latest.json",
)
FINAL_SCORE_MERGE_STATUS_PATH = _path_from_env(
    "PAPER_FINAL_SCORE_STATUS_PATH",
    LOG_DIR / "final_score_merge_status_latest.json",
)
CANDIDATES_META_PATH = _path_from_env(
    "PAPER_CANDIDATES_META_PATH",
    LOG_DIR / "candidates_latest_meta.json",
)
PENDING_ENTRY_STATUS_PATH = _path_from_env(
    "PAPER_PENDING_ENTRY_STATUS_PATH",
    LOG_DIR / "pending_entry_status_latest.json",
)
NEWS_COLLECT_STATUS_PATH = _path_from_env(
    "PAPER_NEWS_COLLECT_STATUS_PATH",
    LOG_DIR / "news_collect_status_latest.json",
)
PAPER_FILLS_LEDGER_PATH = _path_from_env(
    "PAPER_FILLS_LEDGER_PATH",
    ROOTB_DIR / "data" / "ledger" / "paper_fills_ledger.csv",
)
AFTER_CLOSE_SUMMARY_LAST_PATH = _path_from_env(
    "PAPER_AFTER_CLOSE_SUMMARY_LAST_PATH",
    LOG_DIR / "after_close_summary_last.json",
)
SURGE_REALTIME_STATUS_PATH = _path_from_env(
    "PAPER_SURGE_REALTIME_STATUS_PATH",
    LOG_DIR / "surge_realtime_latest.json",
)
SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH = _path_from_env(
    "PAPER_SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH",
    LOG_DIR / "surge_active_response_layer_latest.csv",
)
SURGE_LIVE_READINESS_AUDIT_CSV_PATH = _path_from_env(
    "PAPER_SURGE_LIVE_READINESS_AUDIT_CSV_PATH",
    LOG_DIR / "surge_live_readiness_audit_latest.csv",
)
SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH = _path_from_env(
    "PAPER_SURGE_REALTIME_SHADOW_RUNTIME_CSV_PATH",
    LOG_DIR / "surge_realtime_shadow_runtime_latest.csv",
)
SURGE_REALTIME_SHADOW_RUNTIME_JSON_PATH = _path_from_env(
    "PAPER_SURGE_REALTIME_SHADOW_RUNTIME_JSON_PATH",
    LOG_DIR / "surge_realtime_shadow_runtime_latest.json",
)
INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH = _path_from_env(
    "PAPER_INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH",
    LOG_DIR / "intraday_residual_overnight_guard_shadow_latest.csv",
)
INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH = _path_from_env(
    "PAPER_INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH",
    LOG_DIR / "intraday_residual_overnight_guard_shadow_latest.json",
)

# Live bridge and replay/recovery artifact paths.
ROOTB_REGEN_LIVE_FILLS = _path_from_env(
    "PAPER_LIVE_BRIDGE_FILLS_PATH", ROOTB_DIR / "data" / "live" / "live_fills.csv"
)
PENDING_SIGNALS_PATH = _path_from_env(
    "PAPER_PENDING_SIGNALS_PATH", LOG_DIR / "pending_entry_signals_latest.csv"
)
PENDING_STATUS_PATH = _path_from_env(
    "PAPER_PENDING_STATUS_PATH", LOG_DIR / "pending_entry_status_latest.json"
)
OPS_ALERT_LATEST_PATH = _path_from_env(
    "PAPER_OPS_ALERT_PATH", LOG_DIR / "market_ops_alert_latest.json"
)
ENTRY_SOURCE_ARCHIVE_HISTORY_PATH = _path_from_env(
    "PAPER_ENTRY_SOURCE_ARCHIVE_HISTORY_PATH",
    LOG_DIR / "entry_source_archive_history.csv",
)
ENTRY_SOURCE_ARCHIVE_FIELDS = [
    "archived_at", "run_label", "paper_session_id", "entry_source_kind",
    "candidate_snapshot_id", "code", "name", "signal_date", "entry_day",
    "entry_ts", "entry_order_id", "entry_intent_id", "entry_trace_id",
    "replay_chain_id", "qty", "entry_price", "horizon", "entry_timing",
    "fallback_stage", "is_surge_immediate", "surge_type", "surge_score_final",
    "surge_rvol20", "surge_spread_bps", "signal_id", "captured_at",
    "source_row_json",
]
REPLAY_ORDERS_PATH = _path_from_env(
    "PAPER_REPLAY_ORDERS_PATH",
    BASE_DIR.parent / "vibe" / "buffett" / "data" / "orders" / "replay_orders_latest.csv",
)
REPLAY_QUEUE_STATUS_PATH = _path_from_env(
    "PAPER_REPLAY_QUEUE_STATUS_PATH", LOG_DIR / "replay_queue_status_latest.json"
)
RECOVERY_STATUS_PATH = _path_from_env(
    "PAPER_RECOVERY_STATUS_PATH", LOG_DIR / "paper_recovery_status_latest.json"
)
REPLAY_QUARANTINE_CSV_PATH = _path_from_env(
    "PAPER_REPLAY_QUARANTINE_CSV_PATH", LOG_DIR / "replay_queue_quarantine_latest.csv"
)
REPLAY_QUARANTINE_JSON_PATH = _path_from_env(
    "PAPER_REPLAY_QUARANTINE_JSON_PATH", LOG_DIR / "replay_queue_quarantine_latest.json"
)
REPLAY_QUEUE_ARCHIVE_DIR = _path_from_env(
    "PAPER_REPLAY_QUEUE_ARCHIVE_DIR", LOG_DIR / "replay_queue_archive"
)
REPLAY_CONSISTENCY_PATH = _path_from_env(
    "PAPER_REPLAY_CONSISTENCY_PATH", LOG_DIR / "replay_queue_consistency_latest.json"
)

SELL_VALIDATION_REPORT_PATH = _path_from_env(
    "PAPER_SELL_VALIDATION_REPORT_PATH",
    LOG_DIR / "sell_validation_report_latest.json",
)
ORDER_VALIDATION_REPORT_PATH = _path_from_env(
    "PAPER_ORDER_VALIDATION_REPORT_PATH",
    LOG_DIR / "paper_order_validation_report_latest.json",
)

ROOTB_REPLAY_REGEN_SCRIPT = ROOTB_DIR / "make_orders_exec_from_fills.py"
ROOTB_REPLAY_SUMMARY_LATEST = ROOTB_DIR / "data" / "orders" / "replay_orders_latest.json"
ROOTB_REGEN_EVAL_DIRS = [
    ROOTB_DIR / "data" / "orders",
    ROOTB_DIR / "data" / "orders" / "_hold",
    ROOTB_DIR / "data" / "orders" / "_snap",
]

PENDING_SIGNALS_SCHEMA: List[str] = [
    "signal_date",
    "code",
    "name",
    "carry_reason",
    "carry_origin_reason",
    "captured_at",
    "carryover_count",
    "fallback_stage",
    "score",
    "final_score",
    "carryover_max_age_days",
    "entry_day_override",
    "entry_timing",
    "surge_type_entry_timing",
    "_surge_immediate",
    "surge_type",
    "surge_per_symbol_alloc_pct",
    "surge_total_alloc_pct",
    "surge_type_qty_multiplier",
    "surge_type_policy_note",
    "surge_type_first_ratio",
    "surge_type_stop_pct",
    "surge_type_tp_pct",
    "surge_type_max_hold_days",
    "surge_score",
    "surge_score_final",
    "surge_rvol20",
    "surge_spread_bps",
    "surge_orderflow_tag",
    "surge_orderflow_risk_score",
    "surge_lob_slippage_pct",
    "surge_lob_slippage_source",
    "split_entry_2nd",
    "split_remaining_qty",
    "split_first_entry_price",
    "split_first_order_id",
    "split_first_qty",
    "split_first_fill_id",
    "split_signal_id",
    "split_current_entry_price",
    "split_dip_pct",
    "split_dip_min_pct",
    "split_dip_max_pct",
    "split_confirmation_reason",
    "signal",
    "reason",
    "rank_score",
]

LEGACY_FILLS_HEADER = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
LEGACY_TRADES_HEADER = ["trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "pnl_krw", "exit_reason", "note", "is_surge"]
LEGACY_TRADES_HEADER_WITH_PARTIAL = LEGACY_TRADES_HEADER + ["_partial_exit"]
LEGACY_TRADES_HEADER_OLD = ["trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "pnl_krw", "exit_reason", "note"]

V411_FILLS_HEADER = ["ts","date","code","name","side","qty","price","fee","slippage","order_id","note"]
V411_TRADES_HEADER = ["trade_id","entry_ts","exit_ts","code","name","side","qty","entry_price","exit_price",
                      "gross_ret","net_ret","fee","slippage","stop_hit","take_profit_hit","trail_hit","note"]


def ensure_dirs() -> None:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def read_header(path: Path) -> Optional[List[str]]:
    if not path.exists():
        return None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                r = csv.reader(f)
                return next(r, None)
        except Exception:
            continue
    return None


def detect_schema() -> str:
    env_schema = str(os.getenv("PAPER_SCHEMA", "auto")).strip().lower()
    if env_schema in ("legacy", "v41.1", "v411"):
        return "legacy" if env_schema == "legacy" else "v41.1"

    hf = read_header(FILLS)
    ht = read_header(TRADES)
    if hf is None and ht is None:
        return "legacy"

    if hf == LEGACY_FILLS_HEADER and ht in (LEGACY_TRADES_HEADER, LEGACY_TRADES_HEADER_WITH_PARTIAL):
        return "legacy"
    if hf == V411_FILLS_HEADER and ht == V411_TRADES_HEADER:
        return "v41.1"

    raise SystemExit(
        "[FATAL] paper schema mismatch.\n"
        f"  fills_header={hf}\n"
        f"  trades_header={ht}\n"
        "Fix: make both legacy OR both v41.1 consistently."
    )


DASHBOARD_COMPAT_FILLS_PATH = PAPER_DIR / "fills_dashboard_compat.csv"
DASHBOARD_COMPAT_TRADES_PATH = PAPER_DIR / "trades_dashboard_compat.csv"


def _write_dashboard_compat_csv(schema: str) -> None:
    """Maintain legacy-format CSV copies for dashboard compatibility when v41.1 is active."""
    if schema == "legacy":
        return
    try:
        import pandas as pd
    except Exception:
        return
    try:
        df_fills = pd.read_csv(FILLS, encoding="utf-8-sig")
        if set(LEGACY_FILLS_HEADER).issubset(df_fills.columns):
            df_fills[LEGACY_FILLS_HEADER].to_csv(DASHBOARD_COMPAT_FILLS_PATH, index=False, encoding="utf-8-sig")
        else:
            mapped = pd.DataFrame({
                "datetime": df_fills.get("ts"),
                "code": df_fills.get("code"),
                "side": df_fills.get("side"),
                "qty": df_fills.get("qty"),
                "price": df_fills.get("price"),
                "order_id": df_fills.get("order_id"),
                "note": df_fills.get("note"),
            })
            mapped.to_csv(DASHBOARD_COMPAT_FILLS_PATH, index=False, encoding="utf-8-sig")
    except Exception as e:
        print(f"[DASHBOARD_COMPAT] fills conversion skipped: {e}")
    try:
        df_trades = pd.read_csv(TRADES, encoding="utf-8-sig")
        if set(LEGACY_TRADES_HEADER).issubset(df_trades.columns):
            df_trades[LEGACY_TRADES_HEADER].to_csv(DASHBOARD_COMPAT_TRADES_PATH, index=False, encoding="utf-8-sig")
        else:
            mapped = pd.DataFrame({
                "trade_id": df_trades.get("trade_id"),
                "code": df_trades.get("code"),
                "entry_date": df_trades.get("entry_ts"),
                "entry_price": df_trades.get("entry_price"),
                "exit_date": df_trades.get("exit_ts"),
                "exit_price": df_trades.get("exit_price"),
                "pnl_pct": df_trades.get("net_ret"),
                "pnl_krw": None,
                "exit_reason": "",
                "note": df_trades.get("note"),
                "is_surge": 0,
            })
            mapped.to_csv(DASHBOARD_COMPAT_TRADES_PATH, index=False, encoding="utf-8-sig")
    except Exception as e:
        print(f"[DASHBOARD_COMPAT] trades conversion skipped: {e}")


def ensure_csv(path: Path, header: List[str]) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(header)


def _migrate_legacy_trades_header_if_needed(path: Path) -> None:
    """
    Backward compatibility for old legacy trades header (without is_surge).
    This keeps existing shadow history while making schema detection consistent.
    """
    h = read_header(path)
    if h != LEGACY_TRADES_HEADER_OLD:
        return
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path, encoding="utf-8")
    if "is_surge" not in df.columns:
        df["is_surge"] = 0
    for c in LEGACY_TRADES_HEADER:
        if c not in df.columns:
            df[c] = "" if c != "is_surge" else 0
    df = df[LEGACY_TRADES_HEADER].copy()
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[SCHEMA_MIGRATE] legacy trades header upgraded: {path.name} (+is_surge)")


def _default_state() -> Dict[str, Any]:
    return PortfolioState.from_raw({"open_positions": [], "next_trade_seq": 1, "processed_signals": []}).to_dict()


def _normalize_state_payload(st: Any) -> Dict[str, Any]:
    return PortfolioState.from_raw(st).to_dict()


def load_state() -> Dict[str, Any]:
    """
    paper_state.json
      - open_positions: currently open paper positions.
      - next_trade_seq: next trade id sequence.
      - processed_signals: "CODE:YYYYMMDD" keys used for duplicate entry prevention.
    """
    if not STATE_PATH.exists():
        return _default_state()
    try:
        raw = read_json_text_with_fallback(STATE_PATH)
        parsed_via_nan_fix = False
        try:
            st = json.loads(raw)
        except Exception:
            # Legacy state may contain non-JSON NaN tokens; try one safe normalization pass.
            st = json.loads(re.sub(r"\bNaN\b", "null", raw))
            parsed_via_nan_fix = True
        normalized = _normalize_state_payload(st)
        if parsed_via_nan_fix:
            # Rewrite once in canonical UTF-8 JSON to prevent repeated decode/parse edge cases.
            save_state(normalized)
            print("[STATE] normalized NaN tokens and rewrote paper_state.json")
        return normalized
    except Exception as e:
        bad_path: Optional[Path] = None
        try:
            bak_dir = STATE_PATH.parent / "_bak"
            bak_dir.mkdir(parents=True, exist_ok=True)
            bad_path = bak_dir / f"{STATE_PATH.name}.corrupt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(STATE_PATH, bad_path)
            print(f"[STATE] load failed; corrupt backup saved: {bad_path} ({type(e).__name__})")
        except Exception:
            pass
        raise RuntimeError(f"paper state load failed; fail-closed instead of resetting state: {bad_path or STATE_PATH}") from e


def _json_safe(value: Any) -> Any:
    return _state_json_safe(value)


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    payload = json.dumps(_json_safe(_normalize_state_payload(state)), ensure_ascii=True, indent=2, allow_nan=False)
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        # atomic-ish on Windows: replace existing file after flushing tmp content.
        tmp.replace(STATE_PATH)
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise



def _latest_lob_row_for_code(code: str) -> Dict[str, Any]:
    path = LOG_DIR / "surge_lob_latest.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, dtype={"code": str})
    except Exception:
        return {}
    if df.empty or "code" not in df.columns:
        return {}
    df["code"] = df["code"].astype(str).str.zfill(6)
    row = df[df["code"] == str(code).zfill(6)]
    if row.empty:
        return {}
    return row.iloc[-1].to_dict()


def _load_intraday_history_for_ymd(ymd: str) -> pd.DataFrame:
    ymd8 = _norm_ymd_text(ymd)
    paths = []
    if len(ymd8) == 8:
        paths.append(LOG_DIR / f"intraday_prices_history_{ymd8}.csv")
    paths.append(LOG_DIR / "intraday_prices_latest.csv")

    frames: List[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, dtype=str)
        except Exception:
            continue
        if df.empty or "code" not in df.columns:
            continue
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        if "date" in df.columns:
            df["date"] = df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        else:
            df["date"] = ymd8
        df["code"] = df["code"].astype(str).str.zfill(6)
        if len(ymd8) == 8:
            df = df[df["date"] == ymd8]
        for col in ["current_price", "open", "high", "low", "volume", "trading_value"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "ts" in df.columns:
            df["_ts14"] = df["ts"].map(_norm_ts14)
        else:
            df["_ts14"] = ""
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out[pd.to_numeric(out.get("current_price", 0), errors="coerce").fillna(0.0) > 0].copy()
    if out.empty:
        return out
    out = out.sort_values(["code", "_ts14"]).drop_duplicates(["code", "_ts14"], keep="last")
    return out.reset_index(drop=True)


def _intraday_price_window_since_entry(code: str, entry_ts: Any, entry_date: str) -> Dict[str, Any]:
    ymd8 = _norm_ymd_text(entry_date)
    if len(ymd8) != 8:
        return {"points": 0}
    hist = _load_intraday_history_for_ymd(ymd8)
    if hist.empty or "current_price" not in hist.columns:
        return {"points": 0}
    code6 = str(code or "").zfill(6)
    work = hist[hist["code"] == code6].copy()
    if work.empty:
        return {"points": 0}
    entry_ts14 = _norm_ts14(entry_ts)
    if len(entry_ts14) >= 12 and "_ts14" in work.columns:
        work = work[(work["_ts14"] == "") | (work["_ts14"] >= entry_ts14)].copy()
    prices = pd.to_numeric(work["current_price"], errors="coerce").dropna()
    prices = prices[prices > 0]
    if prices.empty:
        return {"points": 0}
    latest_ts = ""
    if "_ts14" in work.columns and len(work) > 0:
        latest_ts = str(work["_ts14"].iloc[-1] or "")
    return {
        "points": int(len(prices)),
        "current_price": float(prices.iloc[-1]),
        "low_price": float(prices.min()),
        "high_price": float(prices.max()),
        "latest_ts": latest_ts,
    }


def _derive_d_from_fills_path(fills_path: Path) -> str:
    if not fills_path.exists():
        return now_ymd()
    try:
        fills = pd.read_csv(fills_path)
    except Exception:
        return now_ymd()
    if fills.empty:
        return now_ymd()

    dt_col = None
    for c in ["datetime", "ts", "date"]:
        if c in fills.columns:
            dt_col = c
            break
    if dt_col is None:
        return now_ymd()

    side_col = "side" if "side" in fills.columns else None
    if side_col is not None:
        side = fills[side_col].astype(str).str.strip().str.upper()
        buys = fills.loc[side == "BUY"].copy()
        if not buys.empty:
            ymd = buys[dt_col].map(_extract_ymd_from_ts_text)
            ymd = ymd[ymd.str.len() == 8]
            if len(ymd) > 0:
                return str(ymd.max())

    ymd_all = fills[dt_col].map(_extract_ymd_from_ts_text)
    ymd_all = ymd_all[ymd_all.str.len() == 8]
    if len(ymd_all) > 0:
        return str(ymd_all.max())
    return now_ymd()



ENTRY_SIGNAL_SNAPSHOT_PATH = _path_from_env("PAPER_ENTRY_SIGNAL_SNAPSHOT_PATH", LOG_DIR / "entry_signal_snapshot_latest.csv")

ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH = _path_from_env(
    "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_CSV_PATH",
    LOG_DIR / "entry_decision_layers_runtime_latest.csv",
)

ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH = _path_from_env(
    "PAPER_ENTRY_DECISION_LAYERS_RUNTIME_JSON_PATH",
    LOG_DIR / "entry_decision_layers_runtime_latest.json",
)

NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH = _path_from_env(
    "PAPER_NORMAL_ENTRY_FILL_QUALITY_REPORT_CSV_PATH",
    LOG_DIR / "normal_entry_fill_quality_report_latest.csv",
)

NORMAL_ENTRY_FILL_QUALITY_REPORT_JSON_PATH = _path_from_env(
    "PAPER_NORMAL_ENTRY_FILL_QUALITY_REPORT_JSON_PATH",
    LOG_DIR / "normal_entry_fill_quality_report_latest.json",
)

P1_GATE_STATUS_PATH = _path_from_env("PAPER_P1_GATE_STATUS_PATH", LOG_DIR / "p1_entry_gate_status_latest.json")

P1_GATE_STATUS_HISTORY_CSV_PATH = _path_from_env("PAPER_P1_GATE_STATUS_HISTORY_PATH", LOG_DIR / "p1_entry_gate_status_history.csv")

MARKET_EVENT_GUARD_STATUS_PATH = _path_from_env(
    "PAPER_MARKET_EVENT_GUARD_STATUS_PATH",
    LOG_DIR / "market_event_guard_status_latest.json",
)

def _write_market_event_guard_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    MARKET_EVENT_GUARD_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKET_EVENT_GUARD_STATUS_PATH.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
