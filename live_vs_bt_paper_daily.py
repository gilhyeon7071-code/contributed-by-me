# -*- coding: utf-8 -*-
"""Daily paper fill monitor + live-vs-backtest feedback loop.

Inputs:
- <BASE_DIR>/paper/fills.csv
- <BASE_DIR>/paper/prices/ohlcv_paper.parquet
- <BASE_DIR>/2_Logs/paper_pnl_summary_last.json
- <BASE_DIR>/12_Risk_Controlled/report_backtest_summary_v41_1.json

Outputs:
- <BASE_DIR>/2_Logs/live_vs_bt_paper_<YYYYMMDD>.csv
- <BASE_DIR>/2_Logs/live_vs_bt_paper_<YYYYMMDD>.json
- <BASE_DIR>/2_Logs/live_vs_bt_feedback_<YYYYMMDD>.json
- <BASE_DIR>/2_Logs/live_vs_bt_feedback_latest.json

Optional:
- --auto-optimize: trigger optimize_if_due_v41_1.py when divergence exceeds threshold.
"""
from __future__ import annotations

import argparse
import os
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


_RET_COL_CANDS = ["pnl_pct", "ret", "ret_pct", "pnl", "profit_pct"]

def _pick_col(cols, candidates):
    s = {str(c).lower(): c for c in cols}
    for cand in candidates:
        c = s.get(str(cand).lower())
        if c is not None:
            return c
    return None



def _normalize_ymd(x) -> Optional[str]:
    if x is None:
        return None
    try:
        if hasattr(x, "strftime"):
            return x.strftime("%Y%m%d")
    except Exception:
        pass
    try:
        s = str(x).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10].replace("-", "")
        if len(s) >= 8 and s[:8].isdigit():
            return s[:8]
    except Exception:
        return None
    return None


def _resolve_oper_start_ymd() -> str:
    raw = os.getenv("PAPER_OPER_START_YMD", "20260301")
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return s[:8] if len(s) >= 8 else ""

def _load_prices(parquet_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    date_col = _pick_col(df.columns, ["date", "ymd", "trade_date", "일자", "날짜"])
    if date_col is None:
        if df.index is not None:
            df = df.reset_index()
            date_col = _pick_col(df.columns, ["date", "index", df.columns[0]])
    if date_col is None:
        raise RuntimeError("price parquet: cannot find date column")

    df["__ymd"] = df[date_col].apply(_normalize_ymd)

    code_col = _pick_col(df.columns, ["code", "ticker", "종목코드", "티커"])
    if code_col is None:
        raise RuntimeError("price parquet: cannot find code column")
    df["__code"] = df[code_col].astype(str).str.strip().str.zfill(6)

    open_col = _pick_col(df.columns, ["open", "시가", "Open", "OPEN"])
    close_col = _pick_col(df.columns, ["close", "종가", "Close", "CLOSE"])
    if open_col is None and close_col is None:
        raise RuntimeError("price parquet: cannot find open/close columns")

    out = df[["__ymd", "__code"]].copy()
    out["open"] = pd.to_numeric(df[open_col], errors="coerce") if open_col is not None else pd.NA
    out["close"] = pd.to_numeric(df[close_col], errors="coerce") if close_col is not None else pd.NA

    out = out.dropna(subset=["__ymd", "__code"])
    out = out.drop_duplicates(subset=["__ymd", "__code"], keep="last")
    return out


def _load_fills(fills_path: Path) -> pd.DataFrame:
    df = pd.read_csv(fills_path, dtype=str, encoding="utf-8-sig")
    need = ["datetime", "code", "side", "qty", "price"]
    for c in need:
        if c not in df.columns:
            raise RuntimeError(f"fills.csv missing column: {c}")

    df["__ymd"] = df["datetime"].astype(str).str.slice(0, 8)
    df["__code"] = df["code"].astype(str).str.strip().str.zfill(6)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["fill_price"] = pd.to_numeric(df["price"], errors="coerce")
    df["side"] = df["side"].astype(str).str.upper().str.strip()

    if "note" not in df.columns:
        df["note"] = ""
    if "order_id" not in df.columns:
        df["order_id"] = ""
    return df


def _slip(fill: float, ref: float) -> Optional[float]:
    if pd.isna(fill) or pd.isna(ref) or ref == 0:
        return None
    return (fill / ref - 1.0) * 100.0


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Paper live-vs-bt monitor and feedback trigger")
    ap.add_argument("--date", default="", help="Run date YYYYMMDD (default: today)")
    ap.add_argument("--auto-optimize", action="store_true", help="Auto trigger optimize_if_due on divergence")
    ap.add_argument("--deviation-threshold", type=float, default=0.03, help="|live_avg_ret - bt_avg_ret| trigger")
    ap.add_argument("--ratio-threshold", type=float, default=0.50, help="|live/bt - 1| trigger, 0 to disable")
    ap.add_argument("--min-live-trades", type=int, default=20, help="Min live trades to allow trigger")
    ap.add_argument("--cooldown-days", type=int, default=3, help="Min days between auto triggers")

    # alignment: compare live and backtest over the same exit-date window
    ap.add_argument("--align-window-trades", type=int, default=30, help="Live trailing trades used for aligned comparison")
    ap.add_argument("--min-shared-trades", type=int, default=10, help="Min trades required in both live and bt aligned windows")
    ap.add_argument("--min-shared-trades-relaxed", type=int, default=5, help="Relaxed min shared trades when bt window is structurally small")
    ap.add_argument("--allow-summary-fallback", action="store_true", help="Allow trigger fallback to summary means if aligned window is unavailable")

    # backtest quality/freshness gates (must pass before auto-optimize)
    ap.add_argument("--max-backtest-age-days", type=float, default=7.0, help="Max age for backtest summary file")
    ap.add_argument("--min-oos-trades", type=int, default=20, help="Min OOS trades required for optimize trigger")
    ap.add_argument("--min-oos-pf", type=float, default=0.75, help="Min OOS PF required for optimize trigger")
    ap.add_argument("--min-stable-score", type=float, default=0.0, help="Min stable best_score required for optimize trigger")
    return ap.parse_args()


def _load_live_metrics(logs_dir: Path) -> Dict[str, Any]:
    p = logs_dir / "paper_pnl_summary_last.json"
    out: Dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "avg_ret": None,
        "trades_used": 0,
        "last_exit_date": None,
    }
    if not p.exists():
        return out
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        out["avg_ret"] = float(j.get("avg_ret")) if j.get("avg_ret") is not None else None
        out["trades_used"] = int(j.get("trades_used") or 0)
        eq = j.get("equity") or {}
        out["last_exit_date"] = str(eq.get("last_exit_date") or "") or None
    except Exception as e:
        out["error"] = f"{type(e).__name__}:{e}"
    return out


def _load_bt_metrics(base_dir: Path) -> Dict[str, Any]:
    p = base_dir / "12_Risk_Controlled" / "report_backtest_summary_v41_1.json"
    out: Dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "avg_ret": None,
        "split": None,
        "n": 0,
    }
    if not p.exists():
        return out
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        splits = j.get("splits") or {}
        for k in ["OOS", "VAL", "TRAIN"]:
            s = splits.get(k) or {}
            if s.get("mean") is not None:
                out["avg_ret"] = float(s.get("mean"))
                out["split"] = k
                out["n"] = int(s.get("n") or 0)
                break
    except Exception as e:
        out["error"] = f"{type(e).__name__}:{e}"
    return out


def _read_csv_flex(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _pick_ret_col(cols: List[str]) -> Optional[str]:
    low = {str(c).lower(): c for c in cols}
    for cand in _RET_COL_CANDS:
        c = low.get(cand.lower())
        if c is not None:
            return c
    return None


def _sample_trade_rows(df: pd.DataFrame, ret_col: Optional[str], max_rows: int = 5) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    keep = [c for c in ["trade_id", "code", "entry_date", "exit_date", "exit_reason", "note"] if c in df.columns]
    frame = df.copy()
    if ret_col and ret_col in frame.columns:
        frame["ret_sample"] = pd.to_numeric(frame[ret_col], errors="coerce")
        keep.append("ret_sample")
    if "pnl_krw" in frame.columns:
        keep.append("pnl_krw")
    rows: List[Dict[str, Any]] = []
    for _, row in frame.tail(max_rows)[keep].iterrows():
        item: Dict[str, Any] = {}
        for col in keep:
            val = row.get(col)
            if pd.isna(val):
                item[col] = None
            elif hasattr(val, "item"):
                try:
                    item[col] = val.item()
                except Exception:
                    item[col] = str(val)
            else:
                item[col] = val
        rows.append(item)
    return rows


def _month_counts(frame: pd.DataFrame, ymd_col: str = "exit_ymd") -> Dict[str, int]:
    if frame is None or frame.empty or ymd_col not in frame.columns:
        return {}
    ymd = frame[ymd_col].astype(str)
    month = ymd[ymd.str.len() >= 6].str[:6]
    counts = month.value_counts().sort_index()
    return {str(k): int(v) for k, v in counts.to_dict().items()}


def _diagnose_feedback_loop(
    *,
    live_n: int,
    bt_n: int,
    min_shared: int,
    expansion_steps: int,
    live_month_counts: Dict[str, int],
    bt_month_counts: Dict[str, int],
) -> Dict[str, Any]:
    live_n = int(live_n or 0)
    bt_n = int(bt_n or 0)
    min_shared = max(1, int(min_shared or 1))
    ratio = (float(bt_n) / float(live_n)) if live_n > 0 else None
    months = sorted(set(live_month_counts) | set(bt_month_counts))
    monthly_rows = [
        {
            "month": m,
            "live_n": int(live_month_counts.get(m, 0)),
            "bt_n": int(bt_month_counts.get(m, 0)),
        }
        for m in months
    ]
    live_active_months = sum(1 for v in live_month_counts.values() if int(v) > 0)
    bt_active_months = sum(1 for v in bt_month_counts.values() if int(v) > 0)
    state = "OK"
    reason = "aligned_sample_sufficient"
    if live_n >= min_shared and bt_n < min_shared:
        state = "DIVERGED" if (ratio is not None and ratio < 0.10 and expansion_steps > 0) else "BT_SAMPLE_INSUFFICIENT"
        reason = "bt_does_not_represent_live_window" if state == "DIVERGED" else "bt_too_few_trades_in_live_window"
    return {
        "state": state,
        "reason": reason,
        "bt_live_coverage_ratio": ratio,
        "live_active_months": int(live_active_months),
        "bt_active_months": int(bt_active_months),
        "monthly_counts": monthly_rows,
    }


def _read_json_flex(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _resolve_run_id(logs_dir: Path, run_ymd: str) -> str:
    env_run_id = str(os.getenv("RUN_ID", "")).strip()
    if env_run_id:
        return env_run_id

    summary = _read_json_flex(logs_dir / "paper_pnl_summary_last.json")
    summary_as_of = _normalize_ymd(summary.get("as_of") or summary.get("as_of_ymd"))
    summary_run_id = str(summary.get("run_id") or "").strip()
    if summary_run_id and summary_as_of == str(run_ymd):
        return summary_run_id

    return f"live_vs_bt_paper_{run_ymd}_{datetime.now().strftime('%H%M%S')}"


def _load_real_validation_reference(base_dir: Path) -> Dict[str, Any]:
    path = base_dir / "2_Logs" / "backtest_validation_latest.json"
    doc = _read_json_flex(path)
    out: Dict[str, Any] = {
        "path": str(path),
        "available": bool(doc),
        "engine": None,
        "strategy_source": None,
        "backtest_source": None,
        "oper_start_ymd": None,
        "operating_rows": None,
        "n_buy_trades": 0,
        "n_active_days": 0,
        "pnl_total_return": None,
        "profit_factor": None,
        "return_rows": 0,
        "trade_rows": 0,
        "returns": [],
        "trades": [],
        "passed": None,
        "represents_live_behavior": False,
    }
    if not doc:
        return out
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    base_metrics = artifacts.get("base_metrics") if isinstance(artifacts.get("base_metrics"), dict) else {}
    base_meta = artifacts.get("base_meta") if isinstance(artifacts.get("base_meta"), dict) else {}
    base_series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    integration = artifacts.get("integration") if isinstance(artifacts.get("integration"), dict) else {}
    op = artifacts.get("operating_window") if isinstance(artifacts.get("operating_window"), dict) else {}
    returns = base_series.get("returns") if isinstance(base_series.get("returns"), list) else []
    trades = base_series.get("trades") if isinstance(base_series.get("trades"), list) else []
    try:
        n_buy = int(float(base_metrics.get("n_buy_trades") or base_metrics.get("n_trade_proxy") or 0))
    except Exception:
        n_buy = 0
    try:
        n_active = int(float(base_metrics.get("n_active_days") or 0))
    except Exception:
        n_active = 0
    engine = str(base_meta.get("engine") or "")
    strategy_source = str(integration.get("strategy_source") or "")
    backtest_source = str(integration.get("backtest_source") or "")
    out.update(
        {
            "engine": engine or None,
            "strategy_source": strategy_source or None,
            "backtest_source": backtest_source or None,
            "oper_start_ymd": _normalize_ymd(op.get("oper_start_ymd")),
            "operating_rows": op.get("operating_rows"),
            "n_buy_trades": n_buy,
            "n_active_days": n_active,
            "pnl_total_return": base_metrics.get("pnl_total_return"),
            "profit_factor": base_metrics.get("profit_factor"),
            "return_rows": int(len(returns)),
            "trade_rows": int(len(trades)),
            "returns": returns,
            "trades": trades,
            "passed": bool(doc.get("passed")),
            "represents_live_behavior": (
                engine == "real_strategy_backtest"
                or "backtest_real_strategy_adapter.py" in strategy_source
                or "backtest_real_strategy_adapter.py" in backtest_source
            ),
        }
    )
    return out


def _live_daily_return_frame(live_window: pd.DataFrame, ret_col: Optional[str]) -> pd.DataFrame:
    if live_window is None or live_window.empty or not ret_col or ret_col not in live_window.columns:
        return pd.DataFrame(columns=["date", "live_ret", "live_trades"])
    frame = live_window.copy()
    frame["date"] = frame.get("__exit_ymd", frame.get("exit_date")).apply(_normalize_ymd)
    frame["ret"] = pd.to_numeric(frame[ret_col], errors="coerce")
    frame = frame.dropna(subset=["date", "ret"])
    frame = frame[frame["date"].astype(str).str.len() == 8].copy()
    if frame.empty:
        return pd.DataFrame(columns=["date", "live_ret", "live_trades"])
    grouped = frame.groupby("date")["ret"].agg(["mean", "count"]).reset_index()
    grouped.columns = ["date", "live_ret", "live_trades"]
    return grouped


def _bt_daily_return_frame(real_validation: Dict[str, Any]) -> pd.DataFrame:
    rows = real_validation.get("returns") if isinstance(real_validation, dict) else []
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame(columns=["date", "bt_ret"])
    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ymd = _normalize_ymd(row.get("date"))
        if len(ymd) != 8:
            continue
        try:
            ret = float(row.get("return"))
        except Exception:
            continue
        out_rows.append({"date": ymd, "bt_ret": ret})
    return pd.DataFrame(out_rows, columns=["date", "bt_ret"])


def _resolve_capital_total(base_dir: Path) -> float:
    cfg_path = base_dir / "paper" / "paper_engine_config.json"
    cfg = _read_json_flex(cfg_path)
    try:
        cap = float(cfg.get("capital_total") or 0.0)
    except Exception:
        cap = 0.0
    return cap if cap > 0 else 100000000.0


def _load_ddm_stage(base_dir: Path) -> Optional[int]:
    doc = _read_json_flex(base_dir / "2_Logs" / "paper_ddm_status_latest.json")
    if not doc:
        return None
    try:
        return int(doc.get("stage_idx"))
    except Exception:
        return None


def _live_portfolio_daily_return_frame(
    live_window: pd.DataFrame,
    *,
    capital_total: float,
) -> pd.DataFrame:
    if live_window is None or live_window.empty or "pnl_krw" not in live_window.columns:
        return pd.DataFrame(columns=["date", "live_ret", "live_trades", "live_pnl_krw"])
    cap = max(float(capital_total or 0.0), 1.0)
    frame = live_window.copy()
    frame["date"] = frame.get("__exit_ymd", frame.get("exit_date")).apply(_normalize_ymd)
    frame["pnl_krw_num"] = pd.to_numeric(frame["pnl_krw"], errors="coerce")
    frame = frame.dropna(subset=["date", "pnl_krw_num"])
    frame = frame[frame["date"].astype(str).str.len() == 8].copy()
    if frame.empty:
        return pd.DataFrame(columns=["date", "live_ret", "live_trades", "live_pnl_krw"])
    grouped = frame.groupby("date")["pnl_krw_num"].agg(["sum", "count"]).reset_index()
    grouped.columns = ["date", "live_pnl_krw", "live_trades"]
    grouped["live_ret"] = pd.to_numeric(grouped["live_pnl_krw"], errors="coerce") / cap
    return grouped[["date", "live_ret", "live_trades", "live_pnl_krw"]]


def _live_entry_unit_count(live_window: pd.DataFrame, fallback_count: int) -> int:
    if live_window is None or live_window.empty or "note" not in live_window.columns:
        return int(max(0, fallback_count))
    note = live_window["note"].fillna("").astype(str)
    key = pd.Series([""] * len(live_window), index=live_window.index, dtype=object)
    for field in ("entry_order_id", "entry_intent_id", "source_order_id"):
        extracted = note.str.extract(rf"{field}=([^;]*)", expand=False).fillna("").astype(str).str.strip()
        key = key.mask(key.astype(str).str.strip().eq("") & extracted.ne(""), extracted)
    nonblank = key.astype(str).str.strip().ne("")
    return int(key[nonblank].nunique() + (~nonblank).sum())


def _build_real_validation_comparable(
    live_window: pd.DataFrame,
    live_ret_col: Optional[str],
    real_validation: Dict[str, Any],
    base_dir: Path,
    w_start: str,
    w_end: str,
) -> Dict[str, Any]:
    capital_total = _resolve_capital_total(base_dir)
    live_daily = _live_portfolio_daily_return_frame(live_window, capital_total=capital_total)
    live_basis = "live_daily_realized_pnl_krw_over_capital_total"
    if live_daily.empty:
        live_daily = _live_daily_return_frame(live_window, live_ret_col)
        live_basis = "live_exit_date_avg_trade_return"
    mode = (
        "live_portfolio_daily_return_vs_real_validation_daily_return"
        if live_basis == "live_daily_realized_pnl_krw_over_capital_total"
        else "live_exit_date_avg_vs_real_validation_daily_return"
    )
    bt_daily = _bt_daily_return_frame(real_validation)
    out: Dict[str, Any] = {
        "available": False,
        "mode": mode,
        "live_basis": live_basis,
        "bt_basis": "real_validation_daily_portfolio_return",
        "capital_total": capital_total,
        "window_start": w_start,
        "window_end": w_end,
        "live_days": int(len(live_daily)),
        "bt_days": int(len(bt_daily)),
        "common_days": 0,
        "live_avg_ret": None,
        "bt_avg_ret": None,
        "diff": None,
        "abs_diff": None,
        "rows": [],
        "sample_rows": [],
    }
    if live_daily.empty or bt_daily.empty:
        return out
    live_daily = live_daily[live_daily["date"].astype(str).between(w_start, w_end)].copy()
    bt_daily = bt_daily[bt_daily["date"].astype(str).between(w_start, w_end)].copy()
    merged = live_daily.merge(bt_daily, on="date", how="inner").sort_values("date")
    out["live_days"] = int(len(live_daily))
    out["bt_days"] = int(len(bt_daily))
    out["common_days"] = int(len(merged))
    if merged.empty:
        return out
    live_avg = float(pd.to_numeric(merged["live_ret"], errors="coerce").mean())
    bt_avg = float(pd.to_numeric(merged["bt_ret"], errors="coerce").mean())
    diff = live_avg - bt_avg
    out.update(
        {
            "available": True,
            "live_avg_ret": live_avg,
            "bt_avg_ret": bt_avg,
            "diff": float(diff),
            "abs_diff": float(abs(diff)),
            "rows": [
                {
                    "date": str(r.get("date")),
                    "live_ret": float(r.get("live_ret")),
                    "bt_ret": float(r.get("bt_ret")),
                    "live_trades": int(r.get("live_trades")),
                    "live_pnl_krw": float(r.get("live_pnl_krw")) if "live_pnl_krw" in merged.columns else None,
                }
                for _, r in merged.tail(250).iterrows()
            ],
            "sample_rows": [
                {
                    "date": str(r.get("date")),
                    "live_ret": float(r.get("live_ret")),
                    "bt_ret": float(r.get("bt_ret")),
                    "live_trades": int(r.get("live_trades")),
                    "live_pnl_krw": float(r.get("live_pnl_krw")) if "live_pnl_krw" in merged.columns else None,
                }
                for _, r in merged.tail(10).iterrows()
            ],
        }
    )
    return out


def _load_aligned_metrics(base_dir: Path, align_window_trades: int, min_shared_trades: int) -> Dict[str, Any]:
    live_path = base_dir / "paper" / "trades.csv"
    bt_path = base_dir / "12_Risk_Controlled" / "report_backtest_trades_v41_1.csv"
    real_validation = _load_real_validation_reference(base_dir)
    out: Dict[str, Any] = {
        "mode": "aligned_exit_window",
        "ready": False,
        "reason": None,
        "live_path": str(live_path),
        "bt_path": str(bt_path),
        "bt_source": "legacy_v41_trade_file",
        "real_validation_reference": real_validation,
        "real_validation_comparable": {},
        "comparison_source_status": None,
        "window_start": None,
        "window_end": None,
        "live_n": 0,
        "bt_n": 0,
        "live_avg_ret": None,
        "bt_avg_ret": None,
        "ret_col_live": None,
        "ret_col_bt": None,
        "requested_window_trades": int(max(1, int(align_window_trades))),
        "effective_window_trades": None,
        "window_expanded": False,
        "expansion_steps": 0,
        "live_sample_rows": [],
        "bt_sample_rows": [],
        "feedback_loop_state": None,
        "feedback_loop_reason": None,
        "bt_live_coverage_ratio": None,
        "live_month_counts": {},
        "bt_month_counts": {},
        "monthly_counts": [],
    }

    if not live_path.exists():
        out["reason"] = "missing_live_trades"
        return out
    if not bt_path.exists():
        out["reason"] = "missing_bt_trades"
        return out

    try:
        live_df = _read_csv_flex(live_path)
        bt_df = _read_csv_flex(bt_path)
    except Exception as e:
        out["reason"] = f"csv_read_fail:{type(e).__name__}:{e}"
        return out

    live_ret_col = _pick_ret_col([str(c) for c in live_df.columns])
    bt_ret_col = _pick_ret_col([str(c) for c in bt_df.columns])
    out["ret_col_live"] = live_ret_col
    out["ret_col_bt"] = bt_ret_col

    if live_ret_col is None:
        out["reason"] = "live_ret_col_missing"
        return out
    if bt_ret_col is None:
        out["reason"] = "bt_ret_col_missing"
        return out
    if "exit_date" not in live_df.columns or "exit_date" not in bt_df.columns:
        out["reason"] = "exit_date_missing"
        return out

    live_df = live_df.copy()
    bt_df = bt_df.copy()
    live_df["__exit_ymd"] = live_df["exit_date"].apply(_normalize_ymd)
    bt_df["__exit_ymd"] = bt_df["exit_date"].apply(_normalize_ymd)

    live = live_df[["exit_date", live_ret_col]].copy()
    bt = bt_df[["exit_date", bt_ret_col]].copy()

    live["exit_ymd"] = live["exit_date"].apply(_normalize_ymd)
    bt["exit_ymd"] = bt["exit_date"].apply(_normalize_ymd)
    live["ret"] = pd.to_numeric(live[live_ret_col], errors="coerce")
    bt["ret"] = pd.to_numeric(bt[bt_ret_col], errors="coerce")

    live = live.dropna(subset=["exit_ymd", "ret"])
    bt = bt.dropna(subset=["exit_ymd", "ret"])
    live = live[live["exit_ymd"].astype(str).str.len() == 8].copy()
    bt = bt[bt["exit_ymd"].astype(str).str.len() == 8].copy()

    oper_start_ymd = _resolve_oper_start_ymd()
    out["oper_start_ymd"] = oper_start_ymd or None
    if oper_start_ymd:
        live = live[live["exit_ymd"].astype(str) >= oper_start_ymd].copy()
        bt = bt[bt["exit_ymd"].astype(str) >= oper_start_ymd].copy()
        live_df = live_df[live_df["__exit_ymd"].astype(str) >= oper_start_ymd].copy()
        bt_df = bt_df[bt_df["__exit_ymd"].astype(str) >= oper_start_ymd].copy()

    if live.empty:
        out["reason"] = "live_no_valid_closed_trades"
        return out

    live = live.sort_values("exit_ymd")
    k = max(1, int(align_window_trades))
    min_shared = max(1, int(min_shared_trades))
    live_tail = live.tail(k).copy()
    out["live_sample_rows"] = _sample_trade_rows(live_df.copy(), live_ret_col)

    if len(live_tail) < min_shared:
        out["reason"] = "live_too_few_trades"
        out["live_n"] = int(len(live_tail))
        out["effective_window_trades"] = int(len(live_tail))
        return out

    live = live.reset_index(drop=True)
    bt_exit = bt["exit_ymd"].astype(str)
    live_exit = live["exit_ymd"].astype(str)
    start_idx = max(0, len(live) - k)
    end_ymd = str(live.iloc[-1]["exit_ymd"])
    expansion_steps = 0

    while True:
        start_ymd = str(live.iloc[start_idx]["exit_ymd"])
        bt_w = bt[(bt_exit >= start_ymd) & (bt_exit <= end_ymd)].copy()
        if len(bt_w) >= min_shared or start_idx == 0:
            break
        start_idx -= 1
        expansion_steps += 1

    w_start = str(live.iloc[start_idx]["exit_ymd"])
    w_end = end_ymd

    live_w = live[(live_exit >= w_start) & (live_exit <= w_end)].copy()
    live_window = live_df[live_df["__exit_ymd"].astype(str).between(w_start, w_end)].copy()
    bt_window = bt_df[bt_df["__exit_ymd"].astype(str).between(w_start, w_end)].copy()
    live_month_counts = _month_counts(live_w)
    bt_month_counts = _month_counts(bt_w)
    feedback_loop = _diagnose_feedback_loop(
        live_n=int(len(live_w)),
        bt_n=int(len(bt_w)),
        min_shared=min_shared,
        expansion_steps=int(expansion_steps),
        live_month_counts=live_month_counts,
        bt_month_counts=bt_month_counts,
    )

    out["window_start"] = w_start
    out["window_end"] = w_end
    out["live_n"] = int(len(live_w))
    out["bt_n"] = int(len(bt_w))
    out["effective_window_trades"] = int(len(live_w))
    out["window_expanded"] = bool(expansion_steps > 0)
    out["expansion_steps"] = int(expansion_steps)
    out["live_sample_rows"] = _sample_trade_rows(live_window, live_ret_col)
    out["bt_sample_rows"] = _sample_trade_rows(bt_window, bt_ret_col)
    out["feedback_loop_state"] = feedback_loop.get("state")
    out["feedback_loop_reason"] = feedback_loop.get("reason")
    out["bt_live_coverage_ratio"] = feedback_loop.get("bt_live_coverage_ratio")
    out["live_month_counts"] = live_month_counts
    out["bt_month_counts"] = bt_month_counts
    out["monthly_counts"] = feedback_loop.get("monthly_counts", [])

    if len(bt_w) < min_shared:
        if bool(real_validation.get("represents_live_behavior")) and int(real_validation.get("n_buy_trades") or 0) >= min_shared:
            comparable = _build_real_validation_comparable(live_window, live_ret_col, real_validation, base_dir, w_start, w_end)
            live_entry_units = _live_entry_unit_count(live_window, int(len(live_w)))
            representative_ratio = (
                float(real_validation.get("n_buy_trades") or 0) / float(live_entry_units)
                if live_entry_units > 0
                else 0.0
            )
            out["reason"] = "bt_comparison_source_mismatch"
            out["comparison_source_status"] = "LEGACY_V41_NOT_REPRESENTATIVE_REAL_VALIDATION_AVAILABLE"
            comparable_ready = bool(
                comparable.get("available")
                and int(comparable.get("common_days") or 0) >= min_shared
                and int(real_validation.get("n_buy_trades") or 0) >= 30
                and representative_ratio >= 0.50
                and float(real_validation.get("profit_factor") or 0.0) >= 0.50
                and str(comparable.get("mode") or "") == "live_portfolio_daily_return_vs_real_validation_daily_return"
                and comparable.get("live_avg_ret") is not None
                and comparable.get("bt_avg_ret") is not None
            )
            if comparable_ready:
                out["ready"] = True
                out["reason"] = "real_validation_portfolio_series_aligned"
                out["comparison_source_status"] = "REAL_VALIDATION_PORTFOLIO_SERIES_ACCEPTED"
                out["live_avg_ret"] = float(comparable.get("live_avg_ret"))
                out["bt_avg_ret"] = float(comparable.get("bt_avg_ret"))
                out["bt_source"] = "real_validation_portfolio_series"
            elif bool(comparable.get("available")) and int(comparable.get("common_days") or 0) >= min_shared:
                out["reason"] = "bt_comparable_series_available_review_required"
                out["comparison_source_status"] = "REAL_VALIDATION_COMPARABLE_SERIES_AVAILABLE_REVIEW_REQUIRED"
            out["bt_representative_n"] = int(real_validation.get("n_buy_trades") or 0)
            out["real_validation_comparable"] = comparable
            out["feedback_loop_state"] = "SOURCE_MISMATCH"
            out["feedback_loop_reason"] = "legacy_v41_trade_file_not_representative_real_validation_available"
            if comparable_ready:
                out["feedback_loop_state"] = "ALIGNED_COMPARABLE"
                out["feedback_loop_reason"] = "real_validation_portfolio_series_accepted"
            elif bool(comparable.get("available")):
                out["feedback_loop_state"] = "COMPARABLE_SERIES_AVAILABLE"
                out["feedback_loop_reason"] = "real_validation_daily_series_available_review_required"
            out["live_entry_units"] = int(live_entry_units)
            out["bt_live_coverage_ratio_representative"] = representative_ratio if live_entry_units > 0 else None
            out["bt_live_coverage_ratio_representative_basis"] = "real_validation_n_buy_trades_over_live_entry_units"
            return out
        out["reason"] = "bt_too_few_trades_in_window"
        return out

    out["live_avg_ret"] = float(live_w["ret"].mean())
    out["bt_avg_ret"] = float(bt_w["ret"].mean())
    out["ready"] = True
    return out


def _mtime_days(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
        return float(age.total_seconds() / 86400.0)
    except Exception:
        return None


def _extract_summary_asof_ymd(summary: Dict[str, Any]) -> Optional[str]:
    if not isinstance(summary, dict):
        return None
    cand = _normalize_ymd(summary.get("as_of_ymd") or summary.get("as_of") or summary.get("input_asof_ymd"))
    if cand:
        return cand
    # generated_at(YYYY-MM-DD HH:MM:SS)만 있는 구버전 포맷 호환
    g = str(summary.get("generated_at") or "").strip()
    if len(g) >= 10 and g[4] == "-" and g[7] == "-":
        return _normalize_ymd(g[:10])
    return None


def _gate_backtest_freshness(base_dir: Path, max_backtest_age_days: float) -> Dict[str, Any]:
    p = base_dir / "12_Risk_Controlled" / "report_backtest_summary_v41_1.json"
    age = _mtime_days(p)
    out: Dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "max_age_days": float(max_backtest_age_days),
        "age_days": age,
        "ok": False,
        "reason": None,
        "as_of_ymd": None,
        "as_of_age_days": None,
    }
    if not p.exists():
        out["reason"] = "missing_backtest_summary"
        return out
    try:
        summary = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        out["reason"] = f"summary_parse_fail:{type(e).__name__}"
        return out
    asof_ymd = _extract_summary_asof_ymd(summary)
    out["as_of_ymd"] = asof_ymd
    if not asof_ymd:
        out["reason"] = "missing_backtest_asof_ymd"
        return out
    try:
        asof_dt = datetime.strptime(str(asof_ymd), "%Y%m%d").date()
        asof_age_days = float((datetime.now().date() - asof_dt).days)
    except Exception:
        out["reason"] = "invalid_backtest_asof_ymd"
        return out
    out["as_of_age_days"] = asof_age_days
    if asof_age_days > float(max_backtest_age_days):
        out["reason"] = f"stale_backtest_asof({asof_age_days:.2f}d>{float(max_backtest_age_days):.2f}d)"
        return out
    if age is None:
        out["reason"] = "age_check_failed"
        return out
    if age > float(max_backtest_age_days):
        out["reason"] = f"stale_backtest_summary({age:.2f}d>{float(max_backtest_age_days):.2f}d)"
        return out
    out["ok"] = True
    out["reason"] = "fresh"
    return out


def _gate_backtest_quality(base_dir: Path, min_oos_trades: int, min_oos_pf: float, min_stable_score: float) -> Dict[str, Any]:
    sum_path = base_dir / "12_Risk_Controlled" / "report_backtest_summary_v41_1.json"
    stable_path = base_dir / "12_Risk_Controlled" / "stable_params_v41_1.json"
    final_path = base_dir / "2_Logs" / "backtest_final_output_latest.json"
    validation_path = base_dir / "2_Logs" / "backtest_validation_latest.json"
    out: Dict[str, Any] = {
        "summary_path": str(sum_path),
        "stable_path": str(stable_path),
        "final_output_path": str(final_path),
        "validation_path": str(validation_path),
        "min_oos_trades": int(min_oos_trades),
        "min_oos_pf": float(min_oos_pf),
        "min_stable_score": float(min_stable_score),
        "oos_n": None,
        "oos_pf": None,
        "oos_source": "summary",
        "summary_asof_ymd": None,
        "stable_asof_ymd": None,
        "stable_oos_n": None,
        "stable_oos_pf": None,
        "source_switched_to_stable": False,
        "stable_blocks_quality": None,
        "stable_score": None,
        "stable_promoted": None,
        "real_validation_quality": {},
        "source_switched_to_real_validation": False,
        "final_output_overall_pass": None,
        "final_output_gate_decision": None,
        "final_output_gate_override": False,
        "final_output_blocks_quality": None,
        "ok": False,
        "reasons": [],
    }

    if not sum_path.exists():
        out["reasons"].append("missing_backtest_summary")
        return out
    if not stable_path.exists():
        out["reasons"].append("missing_stable_params")
        return out

    try:
        s = json.loads(sum_path.read_text(encoding="utf-8"))
        oos = (s.get("splits") or {}).get("OOS") or {}
        oos_n = int(oos.get("n") or 0)
        oos_pf = float(oos.get("pf") or 0.0)
        out["oos_n"] = oos_n
        out["oos_pf"] = oos_pf
        out["summary_asof_ymd"] = _extract_summary_asof_ymd(s)
    except Exception as e:
        out["reasons"].append(f"summary_parse_fail:{type(e).__name__}")
        return out

    try:
        st = json.loads(stable_path.read_text(encoding="utf-8-sig"))
        stable_score = float(st.get("best_score") or 0.0)
        out["stable_promoted"] = bool(st.get("promoted", False))
        out["stable_score"] = stable_score
        out["stable_asof_ymd"] = _normalize_ymd(st.get("as_of"))
        sw = [w for w in (st.get("windows") or []) if str((w or {}).get("split") or "").upper() == "OOS"]
        sw = [w for w in sw if int((w or {}).get("n_trades") or 0) > 0]
        if sw:
            sw_sorted = sorted(sw, key=lambda w: str((w or {}).get("end") or ""))
            latest = sw_sorted[-1] if sw_sorted else sw[0]
            out["stable_oos_n"] = int((latest or {}).get("n_trades") or 0)
            try:
                out["stable_oos_pf"] = float((latest or {}).get("pf"))
            except Exception:
                out["stable_oos_pf"] = None
    except Exception as e:
        out["reasons"].append(f"stable_parse_fail:{type(e).__name__}")
        return out

    sum_asof = str(out.get("summary_asof_ymd") or "")
    st_asof = str(out.get("stable_asof_ymd") or "")
    st_oos_n = int(out.get("stable_oos_n") or 0)
    st_oos_pf = out.get("stable_oos_pf")
    if sum_asof and st_asof and sum_asof < st_asof and st_oos_n > 0 and st_oos_pf is not None:
        out["oos_n"] = st_oos_n
        out["oos_pf"] = float(st_oos_pf)
        out["oos_source"] = "stable_windows"
        out["source_switched_to_stable"] = True

    stable_blocks_quality = (not sum_asof) or (bool(st_asof) and st_asof >= sum_asof)
    out["stable_blocks_quality"] = bool(stable_blocks_quality)

    real_validation = _load_real_validation_reference(base_dir)
    out["real_validation_quality"] = {
        "available": bool(real_validation.get("available")),
        "passed": real_validation.get("passed"),
        "represents_live_behavior": bool(real_validation.get("represents_live_behavior")),
        "n_buy_trades": real_validation.get("n_buy_trades"),
        "profit_factor": real_validation.get("profit_factor"),
        "engine": real_validation.get("engine"),
    }
    try:
        rv_n = int(real_validation.get("n_buy_trades") or 0)
        rv_pf = float(real_validation.get("profit_factor"))
    except Exception:
        rv_n = 0
        rv_pf = 0.0
    if (
        bool(real_validation.get("available"))
        and real_validation.get("passed") is True
        and bool(real_validation.get("represents_live_behavior"))
        and rv_n > 0
        and rv_pf > 0.0
    ):
        out["oos_n"] = int(rv_n)
        out["oos_pf"] = float(rv_pf)
        out["oos_source"] = "real_validation"
        out["source_switched_to_real_validation"] = True

    if int(out["oos_n"] or 0) < int(min_oos_trades):
        out["reasons"].append(f"oos_trades_low({out['oos_n']}<{int(min_oos_trades)})")
    if float(out["oos_pf"] or 0.0) < float(min_oos_pf):
        out["reasons"].append(f"oos_pf_low({float(out['oos_pf']):.4f}<{float(min_oos_pf):.4f})")
    if stable_blocks_quality:
        if float(out["stable_score"] or 0.0) < float(min_stable_score):
            out["reasons"].append(f"stable_score_low({float(out['stable_score']):.4f}<{float(min_stable_score):.4f})")
        if out.get("stable_promoted") is not True:
            out["reasons"].append("stable_not_promoted")
    elif st_asof:
        out["reasons_stable_reference"] = [
            "stable_older_than_summary_not_blocking",
        ]

    if final_path.exists():
        final_doc: Dict[str, Any] = {}
        for enc in ("utf-8-sig", "utf-8", "cp949"):
            try:
                obj = json.loads(final_path.read_text(encoding=enc))
                final_doc = obj if isinstance(obj, dict) else {}
                break
            except Exception:
                continue
        if final_doc:
            final_pass = bool(final_doc.get("overall_pass"))
            final_decision = str(final_doc.get("final_gate_decision") or "").upper()
            final_gate_ok = final_pass or final_decision == "GO"
            out["final_output_overall_pass"] = final_pass
            out["final_output_gate_decision"] = final_decision or None
            out["final_output_gate_override"] = bool((not final_pass) and final_decision == "GO")
            out["final_output_blocks_quality"] = not final_gate_ok
            if final_gate_ok:
                if out["reasons"]:
                    out["reasons_overridden_by_final_output"] = list(out["reasons"])
                    out["reasons"] = []
                out["quality_source_selected"] = "backtest_final_output"
            else:
                out["reasons"].append("final_output_not_pass")
    else:
        out["final_output_blocks_quality"] = True
        out["reasons"].append("missing_backtest_final_output")

    out["ok"] = len(out["reasons"]) == 0
    return out

def _days_between(ymd_a: str, ymd_b: str) -> Optional[int]:
    try:
        a = datetime.strptime(str(ymd_a), "%Y%m%d").date()
        b = datetime.strptime(str(ymd_b), "%Y%m%d").date()
        return abs((a - b).days)
    except Exception:
        return None


def _read_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_runnable_python(exe: str) -> bool:
    try:
        p = subprocess.run([exe, "-V"], capture_output=True, text=True)
        return int(p.returncode) == 0
    except Exception:
        return False


def _resolve_python(base_dir: Path) -> Optional[str]:
    cands = []
    if sys.executable:
        cands.append(sys.executable)
    cands.append(str(base_dir / ".venv" / "Scripts" / "python.exe"))
    env_py = str(os.getenv("PAPER_PYTHON_EXE", "")).strip()
    if env_py:
        cands.append(env_py)
    py = shutil.which("python")
    if py:
        cands.append(py)
    py3 = shutil.which("py")
    if py3:
        cands.append(py3)
    for c in cands:
        if c and _is_runnable_python(c):
            return c
    return None



def _rc_int(x: Any, default: int = 1) -> int:
    try:
        if x is None:
            return int(default)
        return int(x)
    except Exception:
        return int(default)


def _run_optimize(base_dir: Path, reason: str, args: argparse.Namespace) -> Dict[str, Any]:
    script = base_dir / "optimize_if_due_v41_1.py"
    py = _resolve_python(base_dir)
    out: Dict[str, Any] = {
        "script": str(script),
        "python": py,
        "invoked": False,
        "returncode": None,
        "error": None,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    if not script.exists():
        out["error"] = "missing optimize_if_due_v41_1.py"
        return out

    if not py:
        out["error"] = "missing_python_runtime_for_optimize"
        return out

    cmd = [
        py, str(script),
        "--force", "--reason", reason,
        "--max-backtest-age-days", str(float(args.max_backtest_age_days)),
        "--min-oos-trades", str(int(args.min_oos_trades)),
        "--min-oos-pf", str(float(args.min_oos_pf)),
        "--min-stable-score", str(float(args.min_stable_score)),
        "--allow-early-logic-check-optimize",
    ]
    try:
        p = subprocess.run(cmd, cwd=str(base_dir), capture_output=True, text=True)
        out["invoked"] = True
        out["returncode"] = int(p.returncode)
        out["stdout_tail"] = "\n".join((p.stdout or "").strip().splitlines()[-20:])
        out["stderr_tail"] = "\n".join((p.stderr or "").strip().splitlines()[-20:])
    except Exception as e:
        out["error"] = f"{type(e).__name__}:{e}"
    return out


def main() -> int:
    args = _parse_args()

    base_dir = Path(__file__).resolve().parent
    fills_path = base_dir / "paper" / "fills.csv"
    prices_path = base_dir / "paper" / "prices" / "ohlcv_paper.parquet"
    logs_dir = base_dir / "2_Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    if not fills_path.exists():
        print(f"[FATAL] missing: {fills_path}")
        return 2
    if not prices_path.exists():
        print(f"[FATAL] missing: {prices_path}")
        return 3

    fills = _load_fills(fills_path)
    prices = _load_prices(prices_path)
    live_summary = _load_live_metrics(logs_dir)

    if args.date and len(args.date) == 8 and args.date.isdigit():
        run_ymd = args.date
    else:
        last_exit_date = str(live_summary.get("last_exit_date") or "").strip()
        fills_max_ymd = ""
        try:
            if "__ymd" in fills.columns and len(fills):
                fills_max_ymd = str(fills["__ymd"].astype(str).max() or "").strip()
        except Exception:
            fills_max_ymd = ""
        run_ymd = last_exit_date or fills_max_ymd or datetime.now().strftime("%Y%m%d")
    run_id = _resolve_run_id(logs_dir, run_ymd)

    rows_total = int(len(fills))
    m = fills.merge(prices, on=["__ymd", "__code"], how="left", suffixes=("", "_px"))
    m["ref_open"] = m["open"]
    m["ref_close"] = m["close"]
    m["slip_vs_open_pct"] = [_slip(fp, ro) for fp, ro in zip(m["fill_price"], m["ref_open"])]
    m["slip_vs_close_pct"] = [_slip(fp, rc) for fp, rc in zip(m["fill_price"], m["ref_close"])]

    m = m[m["__ymd"].astype(str) == str(run_ymd)].copy()
    rows_as_of = int(len(m))

    out_csv = logs_dir / f"live_vs_bt_paper_{run_ymd}.csv"
    out_json = logs_dir / f"live_vs_bt_paper_{run_ymd}.json"
    fb_json = logs_dir / f"live_vs_bt_feedback_{run_ymd}.json"
    fb_latest_json = logs_dir / "live_vs_bt_feedback_latest.json"

    out_cols = [
        "datetime", "side", "code", "__code", "qty", "fill_price",
        "ref_open", "ref_close", "slip_vs_open_pct", "slip_vs_close_pct",
        "order_id", "note", "__ymd",
    ]
    for c in out_cols:
        if c not in m.columns:
            m[c] = pd.NA

    out_df = m[out_cols].rename(columns={"__code": "code6", "__ymd": "ymd"})
    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    def _summ(side: str) -> Dict[str, Any]:
        sub = out_df[out_df["side"].astype(str).str.upper() == side].copy()
        slip = pd.to_numeric(sub["slip_vs_open_pct"], errors="coerce")
        ok = slip.dropna()
        return {
            "fills": int(len(sub)),
            "missing_price_rows": int(sub["ref_open"].isna().sum() + sub["ref_close"].isna().sum()),
            "slip_vs_open_pct": {
                "mean": float(ok.mean()) if len(ok) else None,
                "median": float(ok.median()) if len(ok) else None,
                "min": float(ok.min()) if len(ok) else None,
                "max": float(ok.max()) if len(ok) else None,
            },
        }
    bt_summary = _load_bt_metrics(base_dir)
    aligned = _load_aligned_metrics(base_dir, int(args.align_window_trades), int(args.min_shared_trades))
    relaxed_shared = max(1, int(args.min_shared_trades_relaxed))
    if (not bool(aligned.get("ready"))) and str(aligned.get("reason") or "") == "bt_too_few_trades_in_window":
        bt_n = int(aligned.get("bt_n") or 0)
        live_n = int(aligned.get("live_n") or 0)
        if bt_n >= relaxed_shared and live_n >= relaxed_shared:
            aligned["ready"] = True
            aligned["reason"] = "bt_window_relaxed_min_shared"
            aligned["relaxed_min_shared_trades"] = int(relaxed_shared)
            if aligned.get("live_avg_ret") is None:
                try:
                    lvals = [
                        float(r.get("ret_sample"))
                        for r in (aligned.get("live_sample_rows") or [])
                        if r.get("ret_sample") is not None
                    ]
                    if lvals:
                        aligned["live_avg_ret"] = float(sum(lvals) / len(lvals))
                except Exception:
                    pass
            if aligned.get("bt_avg_ret") is None:
                try:
                    bvals = [
                        float(r.get("ret_sample"))
                        for r in (aligned.get("bt_sample_rows") or [])
                        if r.get("ret_sample") is not None
                    ]
                    if bvals:
                        aligned["bt_avg_ret"] = float(sum(bvals) / len(bvals))
                except Exception:
                    pass

    comparison_mode = "aligned_window" if bool(aligned.get("ready")) else ("summary_fallback" if bool(args.allow_summary_fallback) else "aligned_required")

    live = dict(live_summary)
    bt = dict(bt_summary)

    if bool(aligned.get("ready")):
        live_ret = aligned.get("live_avg_ret")
        bt_ret = aligned.get("bt_avg_ret")
        live["avg_ret_aligned"] = live_ret
        bt["avg_ret_aligned"] = bt_ret
        bt["split"] = "ALIGNED_WINDOW"
        bt["n"] = int(aligned.get("bt_n") or 0)
        live_trades_for_trigger = int(aligned.get("live_n") or 0)
    elif bool(args.allow_summary_fallback):
        live_ret = live.get("avg_ret")
        bt_ret = bt.get("avg_ret")
        live_trades_for_trigger = int(live.get("trades_used") or 0)
    else:
        live_ret = None
        bt_ret = None
        live_trades_for_trigger = int(aligned.get("live_n") or 0)

    freshness_gate = _gate_backtest_freshness(base_dir, float(args.max_backtest_age_days))
    quality_gate = _gate_backtest_quality(
        base_dir,
        int(args.min_oos_trades),
        float(args.min_oos_pf),
        float(args.min_stable_score),
    )

    gate_reasons: List[str] = []
    if (not bool(aligned.get("ready"))) and (not bool(args.allow_summary_fallback)):
        gate_reasons.append(str(aligned.get("reason") or "aligned_metrics_unavailable"))
    if not bool(freshness_gate.get("ok")):
        gate_reasons.append(str(freshness_gate.get("reason") or "backtest_freshness_failed"))
    if not bool(quality_gate.get("ok")):
        gate_reasons.extend([str(x) for x in (quality_gate.get("reasons") or ["backtest_quality_failed"])])
    gate_ok = len(gate_reasons) == 0

    diff = None
    abs_diff = None
    ratio = None
    ratio_gap = None
    if live_ret is not None and bt_ret is not None:
        diff = float(live_ret) - float(bt_ret)
        abs_diff = abs(diff)
        if abs(float(bt_ret)) > 1e-12:
            ratio = float(live_ret) / float(bt_ret)
            ratio_gap = abs(ratio - 1.0)

    enough_trades = int(live_trades_for_trigger) >= int(args.min_live_trades)
    trigger_abs = abs_diff is not None and abs_diff >= float(args.deviation_threshold)
    trigger_ratio = False
    if float(args.ratio_threshold) > 0 and ratio_gap is not None:
        trigger_ratio = ratio_gap >= float(args.ratio_threshold)

    comparable_aligned = str(aligned.get("reason") or "") == "real_validation_portfolio_series_aligned"
    ddm_stage_idx = _load_ddm_stage(base_dir)
    comparable_ddm_block = bool(comparable_aligned and ddm_stage_idx is not None and int(ddm_stage_idx) >= 2)
    comparable_trigger_abs = bool(comparable_aligned and trigger_abs)
    if comparable_aligned:
        trigger_ratio = False
        trigger_raw = bool(enough_trades and comparable_trigger_abs and not comparable_ddm_block)
    else:
        trigger_raw = bool(enough_trades and (trigger_abs or trigger_ratio))
    comparable_trigger_policy_block = bool(comparable_aligned and comparable_ddm_block)
    if comparable_trigger_policy_block:
        trigger_raw = False

    state_path = logs_dir / "live_vs_bt_auto_opt_state.json"
    state = _read_state(state_path)
    last_trigger_ymd = str(state.get("last_trigger_ymd") or "")
    since_last = _days_between(run_ymd, last_trigger_ymd) if last_trigger_ymd else None
    cooldown_block = since_last is not None and since_last < int(args.cooldown_days)

    should_trigger = bool(args.auto_optimize and trigger_raw and not cooldown_block and gate_ok)
    optimize_result: Dict[str, Any] = {
        "requested": bool(args.auto_optimize),
        "trigger_raw": bool(trigger_raw),
        "cooldown_block": bool(cooldown_block),
        "cooldown_days": int(args.cooldown_days),
        "last_trigger_ymd": last_trigger_ymd or None,
        "days_since_last_trigger": since_last,
        "comparison_mode": comparison_mode,
        "alignment": aligned,
        "freshness_gate": freshness_gate,
        "quality_gate": quality_gate,
        "gate_ok": bool(gate_ok),
        "gate_reasons": gate_reasons,
        "executed": False,
        "returncode": None,
    }

    if should_trigger:
        reason = (
            f"live_vs_bt_divergence ymd={run_ymd} mode={comparison_mode} "
            f"live={live_ret} bt={bt_ret} abs_diff={abs_diff}"
        )
        optimize_result = _run_optimize(base_dir, reason, args)
        optimize_result.update({
            "requested": True,
            "trigger_raw": True,
            "cooldown_block": False,
            "cooldown_days": int(args.cooldown_days),
            "last_trigger_ymd": last_trigger_ymd or None,
            "days_since_last_trigger": since_last,
            "comparison_mode": comparison_mode,
            "alignment": aligned,
            "freshness_gate": freshness_gate,
            "quality_gate": quality_gate,
            "gate_ok": bool(gate_ok),
            "gate_reasons": gate_reasons,
            "executed": bool(optimize_result.get("invoked")),
        })

        if _rc_int(optimize_result.get("returncode"), default=1) == 0:
            state["last_trigger_ymd"] = run_ymd
            state["last_reason"] = "divergence"
            state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _write_json(state_path, state)

    feedback = {
        "run_id": run_id,
        "as_of": run_ymd,
        "run_ymd": run_ymd,
        "thresholds": {
            "deviation_threshold": float(args.deviation_threshold),
            "ratio_threshold": float(args.ratio_threshold),
            "min_live_trades": int(args.min_live_trades),
            "cooldown_days": int(args.cooldown_days),
            "align_window_trades": int(args.align_window_trades),
            "min_shared_trades": int(args.min_shared_trades),
            "min_shared_trades_relaxed": int(args.min_shared_trades_relaxed),
            "max_backtest_age_days": float(args.max_backtest_age_days),
            "min_oos_trades": int(args.min_oos_trades),
            "min_oos_pf": float(args.min_oos_pf),
            "min_stable_score": float(args.min_stable_score),
            "allow_summary_fallback": bool(args.allow_summary_fallback),
            "oper_start_ymd": _resolve_oper_start_ymd() or None,
        },
        "live": live,
        "backtest": bt,
        "comparison": {
            "mode": comparison_mode,
            "alignment_ready": bool(aligned.get("ready")),
            "alignment_reason": aligned.get("reason"),
            "live_trades_for_trigger": int(live_trades_for_trigger),
        },
        "divergence": {
            "diff": diff,
            "abs_diff": abs_diff,
            "actual_over_bt": ratio,
            "ratio_gap": ratio_gap,
            "enough_trades": bool(enough_trades),
            "trigger_abs": bool(trigger_abs),
            "trigger_ratio": bool(trigger_ratio),
            "trigger_policy_block": bool(comparable_trigger_policy_block),
            "trigger_policy_reason": (
                f"ddm_stage_ge_2(stage_idx={ddm_stage_idx})"
                if comparable_trigger_policy_block
                else None
            ),
            "ddm_stage_idx": ddm_stage_idx,
            "comparable_trigger_abs_only": bool(comparable_aligned),
            "trigger_raw": bool(trigger_raw),
        },
        "optimize": optimize_result,
        "state_file": str(state_path),
    }

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": run_id,
        "as_of": run_ymd,
        "source": "paper_fills_vs_ohlc",
        "rows_total": rows_total,
        "rows_as_of": rows_as_of,
        "paths": {
            "fills": str(fills_path),
            "prices": str(prices_path),
            "out_csv": str(out_csv),
            "out_json": str(out_json),
            "feedback_json": str(fb_json),
        },
        "by_side": {
            "BUY": _summ("BUY"),
            "SELL": _summ("SELL"),
        },
        "feedback_loop": feedback,
        "notes": "Paper fills vs same-day OHLC(open/close) + live-vs-backtest divergence monitor.",
    }

    _write_json(out_json, summary)
    _write_json(fb_json, feedback)
    _write_json(fb_latest_json, feedback)

    print("[OK] wrote:", out_csv)
    print("[OK] wrote:", out_json)
    print("[OK] wrote:", fb_json)
    print(
        "[SUMMARY] BUY fills=", summary["by_side"]["BUY"]["fills"],
        "SELL fills=", summary["by_side"]["SELL"]["fills"],
        "trigger_raw=", feedback["divergence"]["trigger_raw"],
        "auto_opt=", bool(args.auto_optimize),
    )

    if should_trigger and _rc_int(optimize_result.get("returncode"), default=1) != 0:
        print("[ERROR] auto optimize failed")
        return _rc_int(optimize_result.get("returncode"), default=1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
