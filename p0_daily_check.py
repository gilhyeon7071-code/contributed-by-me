#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
p0_daily_check.py

P0 ?쇱씪 ?먭?(異붿젙 ?놁씠 '?뚯씪???덈뒗 ?ъ떎'留뚯쑝濡??먯젙)
- prices(paper/prices/ohlcv_paper.parquet) 理쒖떊???됱닔/肄붾뱶??
- candidates 硫뷀?(2_Logs/candidates_latest_meta.json) 理쒖떊???쒖옣援?㈃
- risk_off: 媛寃⑸뜲?댄꽣 理쒖떊?쇱씠 ?꾨낫硫뷀? 理쒖떊?쇰낫???ㅼ퀜吏硫?True
- paper open_positions_count: paper/trades.csv??exit_* 而щ읆??鍮꾩뼱?덈뒗 ???섎줈 怨꾩궛 (None 湲덉?)
- max_hold_days: paper/paper_engine_config.json?먯꽌 ?쎌쓬

異쒕젰:
- 2_Logs/p0_daily_check_YYYYMMDD_HHMMSS.json
- 肄섏넄 4以??붿빟
"""

from __future__ import annotations
import utils.common as ucommon

def _krx_clean_ncode(base_dir, date_max_yyyymmdd):
    """
    krx_daily_*_clean.parquet (?대떦 date_max)?먯꽌 code ?좊땲??媛쒖닔 ?곗텧.
    ?ㅽ뙣 ??None 諛섑솚(?곸쐞 try/except?먯꽌 krx_clean_ncode_fail濡??쒓린).
    """
    try:
        ymd = str(date_max_yyyymmdd or "").strip()
        if not ymd:
            return None

        base = Path(str(base_dir))
        cand = []

        d_manual = base / "_krx_manual"
        if d_manual.exists():
            cand += list(d_manual.glob(f"krx_daily_*_{ymd}_clean.parquet"))
            cand += list(d_manual.glob(f"krx_daily_{ymd}_{ymd}_clean.parquet"))

        if not cand:
            cand = list(base.rglob(f"krx_daily_*_{ymd}_clean.parquet"))

        if not cand:
            return None

        p = max(cand, key=lambda x: x.stat().st_mtime)

        try:
            df = pd.read_parquet(p, columns=["code"])
        except Exception:
            df = pd.read_parquet(p)

        if "code" not in df.columns:
            df2 = df.reset_index()
            if "code" in df2.columns:
                df = df2
            else:
                return None

        codes = df["code"].astype(str).map(norm_code)
        return int(codes.nunique(dropna=True))
    except Exception:
        return None

import json
import logging
import re
import datetime as dt
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import os  # PATCH: for trades mtime stamp
import pandas as pd

import time

# 怨듯넻 ?좏떥由ы떚 紐⑤뱢 import
from utils.common import (
    now_tag,
    prev_weekday,
    read_parquet_date_max,
    latest_file,
    read_json,
    parse_yyyymmdd,
    norm_code,
)

# ============================================================================
# 濡쒓퉭 ?ㅼ젙 (?먮윭 異붿쟻 媛쒖꽑)
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("p0_daily_check")
# Suppress third-party logging formatting tracebacks (e.g., pykrx util wrapper).
logging.raiseExceptions = False

def cleanup_old_logs(
    log_dir: Path,
    days: int = 30,
    enabled: bool = False,
    keep_last: int = 50,
    patterns: list[str] | None = None,
) -> dict:
    """2_Logs ?뺣━(?붿씠?몃━?ㅽ듃 湲곕컲).
    - 湲곕낯: enabled=False (??젣?섏? ?딄퀬 'would_delete'留??곗텧)
    - ?덉쟾?μ튂: ?⑦꽩蹂?理쒖떊 keep_last媛쒕뒗 蹂댁〈
    """
    now = time.time()
    cutoff = now - (int(days) * 86400)

    pats = patterns or [
        "p0_daily_check_*.json",
        "gate_daily_*.json",
        "after_close_summary_*.json",
        "paper_pending_report_*.json",
        "paper_pending_report_*.txt",
        "paper_pnl_summary_*.json",
        "p0_live_vs_bt_core_*.json",
        "p0_stop_report_*.json",
        "run_paper_daily_last.log",
    ]

    would_delete = []
    deleted = []
    errors = []

    for pat in pats:
        try:
            files = sorted(list(log_dir.glob(pat)), key=lambda p: p.stat().st_mtime)
            if not files:
                continue

            # 理쒖떊 keep_last媛?蹂댄샇
            protected = set(files[-int(keep_last):]) if keep_last and len(files) > keep_last else set()

            for f in files:
                try:
                    if f in protected:
                        continue
                    if f.stat().st_mtime < cutoff:
                        would_delete.append(str(f))
                except Exception as e:
                    errors.append(f"stat_fail:{f}:{type(e).__name__}")
        except Exception as e:
            errors.append(f"glob_fail:{pat}:{type(e).__name__}")

    if enabled:
        for fp in would_delete:
            try:
                Path(fp).unlink()
                deleted.append(fp)
            except Exception as e:
                errors.append(f"unlink_fail:{fp}:{type(e).__name__}")

    return {
        "enabled": bool(enabled),
        "days": int(days),
        "keep_last": int(keep_last),
        "patterns": pats,
        "would_delete_count": int(len(would_delete)),
        "deleted_count": int(len(deleted)),
        "would_delete_sample": would_delete[:20],
        "deleted_sample": deleted[:20],
        "errors": errors[:20],
    }
# now_tag, prev_weekday -> utils.common?쇰줈 ?대룞??


# _parquet_date_max_via_stats -> utils.common.read_parquet_date_max濡??대룞??


def _krx_clean_date_max(base_dir: Path, flags: list[str]) -> str | None:
    """krx_daily_*_clean.parquet?ㅼ쓽 date_max(YYYYMMDD) 以?理쒕뙎媛?"""
    files = list(base_dir.rglob("krx_daily_*_clean.parquet"))
    if not files:
        flags.append("krx_clean_not_found")
        return None
    mx = None
    for p in files:
        d = read_parquet_date_max(p, "date")  # ??utils.common ?ъ슜
        if d is None:
            flags.append(f"krx_clean_no_stats:{p}")
            continue
        m = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", str(d))
        if m:
            d = f"{m.group(1)}{m.group(2)}{m.group(3)}"
        mx = d if (mx is None or d > mx) else mx
    return mx


# latest_file -> utils.common.latest_file濡??대룞??

def _dup_trades_count(trades_csv: Path, flags: list[str]) -> int:
    """?숈씪 trade媛 以묐났?쇰줈 ?꾩쟻?섎뒗 寃쎌슦瑜??먯?(??而щ읆 湲곕컲)."""
    if not trades_csv.exists():
        return 0
    try:
        t = pd.read_csv(trades_csv)
        if len(t) == 0:
            return 0
        key_pref = ["code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "exit_reason"]
        key_cols = [c for c in key_pref if c in t.columns]
        if len(key_cols) < 3:
            flags.append("dup_trades_key_cols_insufficient")
            return 0
        dup = t.duplicated(subset=key_cols, keep="first")
        return int(dup.sum())
    except Exception as e:
        flags.append(f"dup_trades_parse_fail:{trades_csv}:{type(e).__name__}")
        return 0


# read_json -> utils.common.read_json濡??대룞??
# parse_yyyymmdd -> utils.common.parse_yyyymmdd濡??대룞??


def _read_prices_stats(prices_parquet: Path, flags: list[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {'path': str(prices_parquet), 'rows': 0, 'codes': 0, 'date_max': None}
    if not prices_parquet.exists():
        flags.append(f'prices_missing:{prices_parquet}')
        return out

    try:
        df_date = pd.read_parquet(prices_parquet, columns=['date'])
        out['rows'] = int(len(df_date))
        if len(df_date) > 0:
            out['date_max'] = parse_yyyymmdd(df_date['date'].max())
    except Exception as e:
        flags.append(f'prices_parquet_read_fail:{type(e).__name__}')
        return out

    # code 而щ읆???덉쑝硫?codes 怨꾩궛
    try:
        df = pd.read_parquet(prices_parquet)
        code_col = None
        for c in ['code', 'ticker', 'symbol']:
            if c in df.columns:
                code_col = c
                break
        out['codes'] = int(pd.Series(df[code_col]).astype(str).nunique()) if code_col else 0
    except Exception as e:
        flags.append(f'prices_codes_read_fail:{type(e).__name__}')
        out['codes'] = 0

    return out



def _gap_level_from_G(G: int) -> str:
    if G is None:
        return "UNKNOWN"
    if G <= 1:
        return "NORMAL"
    if G == 2:
        return "WARN"
    return "DANGER"  # G >= 3


def _build_self_adaptive_gap(prev_weekday_ymd: str, prices_date_max: str, krx_clean_date_max: str, cand_latest_date: str) -> dict:
    """
    Self-adaptive v1 (record-only):
      G<=1: NORMAL
      G==2: WARN
      G>=3: DANGER
    NOTE: v1? YYYYMMDD ?뺤닔 李? (?곸뾽??媛?? v1.1?먯꽌 援먯껜)
    """
    def _gap(prev, d):
        try:
            if not prev or not d:
                return None
            return int(str(prev)) - int(str(d))
        except Exception:
            return None

    g_prices = _gap(prev_weekday_ymd, prices_date_max)
    g_krx    = _gap(prev_weekday_ymd, krx_clean_date_max)
    g_cand   = _gap(prev_weekday_ymd, cand_latest_date)

    gs = [x for x in [g_prices, g_krx, g_cand] if isinstance(x, int)]
    G = max(gs) if gs else None
    level = _gap_level_from_G(G)

    plan = {
        "level": level,
        "checks": ["schema", "freshness"],
        "rationale": [],
        "policy": "gap_v1",
        "G": G,
        "gaps": {"prices": g_prices, "krx_clean": g_krx, "candidates_meta": g_cand},
    }
    if level == "WARN":
        plan["checks"] += ["universe_crosscheck", "meta_consistency", "krx_clean_consistency"]
        plan["rationale"].append("G==2 => WARN: strengthen universe validation")
    elif level == "DANGER":
        plan["checks"] += ["snapshot_only"]
        plan["rationale"].append("G>=3 => DANGER: would block (record-only in v1)")
    else:
        plan["rationale"].append("G<=1 => NORMAL")

    return {"gap_level": level, "gap_max": G, "gap_parts": plan["gaps"], "verify_plan": plan}



def _crash_local_proxy_metrics(as_of_ymd: str, window_days: int) -> dict:
    """Fallback crash metrics using market-wide proxy from krx_clean first, then paper prices."""
    out = {
        "ok": False,
        "status": None,
        "rows": 0,
        "used_rows": 0,
        "max_dd": None,
        "day_ret": None,
        "error": None,
        "source": "local_prices_proxy",
    }

    def _build_idx_from_parquet(px_path: Path, source_tag: str) -> Optional[pd.Series]:
        try:
            df = pd.read_parquet(px_path, columns=["date", "close"])
        except Exception:
            return None
        if df is None or len(df) == 0:
            return None

        d8 = df["date"].astype(str).str.replace("-", "", regex=False).str[:8]
        close = pd.to_numeric(df["close"], errors="coerce")
        mdf = pd.DataFrame({"date8": d8, "close": close}).dropna()
        mdf = mdf[mdf["close"] > 0]
        mdf = mdf[mdf["date8"] <= str(as_of_ymd)]
        if mdf.empty:
            return None

        idx = mdf.groupby("date8", as_index=True)["close"].mean().sort_index()
        if idx is None or len(idx) < 2:
            return None

        out["source"] = source_tag
        return idx

    try:
        base_dir = Path(__file__).resolve().parent

        # 1) Prefer market-wide krx_clean proxy (full universe) to avoid candidate-only bias.
        krx_candidates = []
        for p in base_dir.rglob("krx_daily_*_clean.parquet"):
            d = read_parquet_date_max(p, "date")
            d8 = parse_yyyymmdd(d) if d is not None else None
            if not d8:
                continue
            if d8 > str(as_of_ymd):
                continue
            krx_candidates.append((str(d8), float(p.stat().st_mtime), p))

        krx_candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        idx = None
        for _, __, kp in krx_candidates:
            idx = _build_idx_from_parquet(kp, "krx_clean_proxy")
            if idx is not None and not idx.empty:
                break

        # 2) Fallback to paper prices proxy only when krx_clean proxy is unavailable.
        if idx is None:
            px_path = base_dir / "paper" / "prices" / "ohlcv_paper.parquet"
            if not px_path.exists():
                out["status"] = "error_fallback_prices_missing"
                out["error"] = str(px_path)
                return out
            idx = _build_idx_from_parquet(px_path, "paper_prices_proxy")

        if idx is None or idx.empty:
            out["status"] = "error_fallback_index_empty"
            return out

        tail = idx.tail(max(2, int(window_days)))
        dd = (tail / tail.cummax() - 1.0).min()
        day_ret = tail.pct_change().iloc[-1]

        out["ok"] = True
        out["status"] = "ok"
        out["rows"] = int(len(idx))
        out["used_rows"] = int(len(tail))
        out["max_dd"] = float(dd)
        out["day_ret"] = float(day_ret)
        return out
    except Exception as e:
        out["status"] = f"error_fallback_calc:{type(e).__name__}"
        out["error"] = str(e)[:200]
        return out

def _default_index_code_for_market(index_market: str, index_name_contains: str) -> str:
    """Best-effort default KRX index code when config is missing or malformed."""
    mkt = str(index_market or "").strip().upper()
    if mkt == "KOSPI":
        return "1001"
    if mkt == "KOSDAQ":
        return "2001"
    if mkt == "KRX":
        return "1002"
    return ""

def _extract_close_series_from_index_ohlcv(df: pd.DataFrame) -> Optional[pd.Series]:
    """Robust close-series extractor for pykrx index OHLCV schema drift."""
    if df is None or len(df) == 0:
        return None

    cols = list(df.columns)
    for key in ("Close", "close", "CLSPRC_IDX"):
        if key in cols:
            s = pd.to_numeric(df[key], errors="coerce")
            if int(s.notna().sum()) >= 2:
                return s

    for c in cols:
        lc = str(c).strip().lower()
        if ("close" in lc) or ("clsprc" in lc):
            s = pd.to_numeric(df[c], errors="coerce")
            if int(s.notna().sum()) >= 2:
                return s

    best = None
    best_cnt = -1
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce")
        cnt = int(s.notna().sum())
        if cnt > best_cnt:
            best = s
            best_cnt = cnt
    if best is not None and best_cnt >= 2:
        return best
    return None

def _eval_crash_risk_off(as_of_ymd: str, cfg: dict) -> dict:
    """
    Index-based crash risk-off using pykrx (optional).
    - always returns dict with metrics.status present
    - must NOT reference external locals (e.g., krx_clean/meta/report)
    - fallback: if pykrx fetch/resolution fails, use local prices market proxy.
    """
    crash_cfg = (cfg or {}).get("crash_risk_off") or {}

    # [CRASH_RISK_OFF_RESOLVE_INDEX_CODE_V2]
    # If enabled=True and index_code missing, try pykrx resolution first,
    # then fall back to a stable default per market.
    _idx_list_err = ""
    try:
        _enabled_cfg = bool((crash_cfg or {}).get("enabled", False)) if isinstance(crash_cfg, dict) else False
        _idx_code = str((crash_cfg or {}).get("index_code") or "").strip() if isinstance(crash_cfg, dict) else ""
        if _enabled_cfg and (not _idx_code):
            _idx_market = str((crash_cfg or {}).get("index_market") or "").strip().upper()
            _name_contains = str((crash_cfg or {}).get("index_name_contains") or "").strip()
            if _idx_market and _name_contains:
                from pykrx import stock as _pykrx_stock
                try:
                    _ticks = _pykrx_stock.get_index_ticker_list(market=_idx_market)
                except Exception as _e:
                    _ticks = []
                    _idx_list_err = f"{type(_e).__name__}:{str(_e)}"
                for _t in _ticks:
                    try:
                        _nm = _pykrx_stock.get_index_ticker_name(_t)
                    except Exception:
                        continue
                    if _name_contains in str(_nm):
                        crash_cfg["index_code"] = str(_t)
                        break

            if not str((crash_cfg or {}).get("index_code") or "").strip():
                _fallback_idx = _default_index_code_for_market(_idx_market, _name_contains)
                if _fallback_idx:
                    crash_cfg["index_code"] = _fallback_idx
    except Exception:
        pass

    enabled = bool(crash_cfg.get("enabled", False))

    mode = str(crash_cfg.get("mode", "BLOCK")).upper().strip() or "BLOCK"
    try:
        window_days = int(crash_cfg.get("window_days", 60) or 60)
    except Exception:
        window_days = 60
    try:
        trig_dd = float(crash_cfg.get("trigger_max_dd_pct", 0.12) or 0.12)
    except Exception:
        trig_dd = 0.12
    try:
        trig_day = float(crash_cfg.get("trigger_day_ret_pct", 0.05) or 0.05)
    except Exception:
        trig_day = 0.05
    try:
        fb_trig_dd = float(crash_cfg.get("fallback_trigger_max_dd_pct", max(0.25, trig_dd)) or max(0.25, trig_dd))
    except Exception:
        fb_trig_dd = max(0.25, trig_dd)
    try:
        fb_trig_day = float(crash_cfg.get("fallback_trigger_day_ret_pct", max(0.15, trig_day)) or max(0.15, trig_day))
    except Exception:
        fb_trig_day = max(0.15, trig_day)

    index_code = str(crash_cfg.get("index_code", "") or "").strip()

    out = {
        "enabled": enabled,
        "triggered": False,
        "reasons": [],
        "limits": {
            "mode": mode,
            "window_days": window_days,
            "trigger_max_dd_pct": trig_dd,
            "trigger_day_ret_pct": trig_day,
            "fallback_trigger_max_dd_pct": fb_trig_dd,
            "fallback_trigger_day_ret_pct": fb_trig_day,
        },
        "metrics": {
            "status": None,
            "as_of_ymd": str(as_of_ymd or ""),
            "index_code": index_code,
        },
        "source": "pykrx",
    }

    def _apply_trigger_from_metrics(use_fallback_limits: bool = False) -> None:
        dd_lim = float(fb_trig_dd if use_fallback_limits else trig_dd)
        day_lim = float(fb_trig_day if use_fallback_limits else trig_day)
        dd = out["metrics"].get("max_dd")
        day_ret = out["metrics"].get("day_ret")
        if dd is not None and float(dd) <= -dd_lim:
            out["triggered"] = True
            out["reasons"].append(f"max_dd({float(dd):.6f} <= -{dd_lim:.6f})")
        if day_ret is not None and float(day_ret) <= -day_lim:
            out["triggered"] = True
            out["reasons"].append(f"day_ret({float(day_ret):.6f} <= -{day_lim:.6f})")
        if out["triggered"]:
            out["metrics"]["status"] = "triggered"

    def _apply_local_proxy_fallback(from_status: str) -> bool:
        fb = _crash_local_proxy_metrics(str(as_of_ymd or ""), int(window_days))
        out["metrics"]["fallback_from"] = from_status
        out["metrics"]["fallback_status"] = str(fb.get("status") or "")
        if not bool(fb.get("ok", False)):
            if fb.get("error"):
                out["metrics"]["fallback_error"] = str(fb.get("error"))[:200]
            return False

        out["source"] = str(fb.get("source") or "local_prices_proxy")
        out["metrics"]["rows"] = int(fb.get("rows") or 0)
        out["metrics"]["used_rows"] = int(fb.get("used_rows") or 0)
        out["metrics"]["max_dd"] = float(fb.get("max_dd"))
        out["metrics"]["day_ret"] = float(fb.get("day_ret"))
        _src = str(out.get("source") or "local_prices_proxy").strip().lower()
        out["metrics"]["status"] = f"ok_fallback_{_src}"
        _apply_trigger_from_metrics(use_fallback_limits=True)
        return True

    if not enabled:
        out["metrics"]["status"] = "disabled_by_config"
        return out

    if not index_code:
        out["reasons"].append("missing_index_code")
        _err = str(_idx_list_err or "").strip()
        if _err:
            out["metrics"]["status"] = "error_index_list_fetch_fail"
            out["metrics"]["error"] = _err
        else:
            out["metrics"]["status"] = "error_config_missing_index_code"
        if _apply_local_proxy_fallback(str(out["metrics"]["status"])):
            return out
        return out

    try:
        from pykrx import stock
    except Exception as e:
        out["reasons"].append("pykrx_import_fail")
        out["metrics"]["status"] = f"error_pykrx_import:{type(e).__name__}"
        out["metrics"]["error"] = str(e)[:200]
        if _apply_local_proxy_fallback(str(out["metrics"]["status"])):
            return out
        return out

    try:
        import datetime as dt
        import pandas as pd

        asof = dt.datetime.strptime(str(as_of_ymd), "%Y%m%d").date()
        # ??? ??? ??? ?? ??? ??
        start = (asof - dt.timedelta(days=max(120, int(window_days) * 3))).strftime("%Y%m%d")
        end = asof.strftime("%Y%m%d")

        _root_logger = logging.getLogger()
        _prev_level = _root_logger.level
        try:
            if _prev_level <= logging.INFO:
                _root_logger.setLevel(logging.WARNING)
            # pykrx in some environments fails when resolving ticker display name
            # (internal get_index_ticker_name key mismatch). Keep raw OHLCV retrieval only.
            try:
                df = stock.get_index_ohlcv_by_date(start, end, index_code, name_display=False)
            except TypeError:
                # Backward compatibility for older pykrx without name_display param.
                df = stock.get_index_ohlcv_by_date(start, end, index_code)
        finally:
            _root_logger.setLevel(_prev_level)
        if df is None or len(df) == 0:
            out["reasons"].append("fetch_empty")
            out["metrics"]["status"] = "error_fetch_empty"
            out["metrics"]["rows"] = 0
            if _apply_local_proxy_fallback("error_fetch_empty"):
                return out
            return out

        close = _extract_close_series_from_index_ohlcv(df)
        if close is None:
            out["reasons"].append("close_column_missing")
            out["metrics"]["status"] = "error_close_column_missing"
            out["metrics"]["rows"] = int(len(df))
            if _apply_local_proxy_fallback("error_close_column_missing"):
                return out
            return out

        tail = close.tail(window_days) if len(close) >= window_days else close
        dd = (tail / tail.cummax() - 1.0).min()  # negative
        day_ret = tail.pct_change().iloc[-1]

        out["metrics"]["rows"] = int(len(df))
        out["metrics"]["used_rows"] = int(len(tail))
        out["metrics"]["max_dd"] = float(dd)
        out["metrics"]["day_ret"] = float(day_ret)
        out["metrics"]["status"] = "ok"
        _apply_trigger_from_metrics()
        return out

    except Exception as e:
        out["reasons"].append("fetch_calc_fail")
        out["metrics"]["status"] = f"error_fetch_calc:{type(e).__name__}"
        out["metrics"]["error"] = str(e)[:200]
        if _apply_local_proxy_fallback(str(out["metrics"]["status"])):
            return out
        return out


def _compute_open_positions_from_trades(trades_csv: Path, flags: list[str]) -> int:
    """exit_* 而щ읆??紐⑤몢 鍮꾩뼱?덈뒗 ???섎? ?ㅽ뵂 ?ъ??섏쑝濡?怨꾩궛."""
    if not trades_csv.exists():
        return 0
    try:
        t = pd.read_csv(trades_csv)
        if len(t) == 0:
            return 0
        exit_cols = [c for c in t.columns if 'exit' in c.lower()]
        if not exit_cols:
            flags.append('trades_csv_no_exit_cols')
            return 0

        def _has_value(x: Any) -> bool:
            try:
                if pd.isna(x):
                    return False
            except Exception:
                pass
            s = str(x).strip()
            return (s != '') and (s.lower() != 'nan')

        exit_any = t[exit_cols].apply(lambda col: col.map(_has_value)).any(axis=1)
        return int((~exit_any).sum())

    except Exception as e:
        flags.append(f'trades_csv_parse_fail:{trades_csv}:{type(e).__name__}')
        return 0


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / '2_Logs'
    logs_dir.mkdir(parents=True, exist_ok=True)

    flags: list[str] = []

    # meta
    meta_path = logs_dir / 'candidates_latest_meta.json'
    meta = read_json(meta_path) if meta_path.exists() else None
    md: Optional[str] = None
    mr: Optional[str] = None
    if meta:
        try:
            md = parse_yyyymmdd(meta.get('latest_date'))
            mr = meta.get('market_regime')
        except Exception:
            flags.append(f'meta_parse_fail:{meta_path}')

    # prices
    prices_parquet = base_dir / 'paper' / 'prices' / 'ohlcv_paper.parquet'
    prices = _read_prices_stats(prices_parquet, flags)
    date_max: Optional[str] = prices.get('date_max')

    # config
    cfg_path = base_dir / 'paper' / 'paper_engine_config.json'
    cfg = read_json(cfg_path) if cfg_path.exists() else None
    # fix12: ensure crash_risk_off is always defined (avoid NameError on report write)
    crash_cfg0 = (cfg or {}).get("crash_risk_off") or {}
    if not isinstance(crash_cfg0, dict):
        crash_cfg0 = {}
    crash_risk_off = {
        "enabled": bool(crash_cfg0.get("enabled", False)),
        "triggered": False,
        "reasons": [],
        "limits": dict(crash_cfg0),
        "metrics": {},
        "source": str(cfg_path),
    }

    max_hold_days = cfg.get('max_hold_days') if isinstance(cfg, dict) else None

    # open positions
    trades_csv = base_dir / 'paper' / 'trades.csv'
    open_pos_trades = _compute_open_positions_from_trades(trades_csv, flags)
    open_pos_state = None
    try:
        sp = base_dir / 'paper' / 'paper_state.json'
        if sp.exists():
            txt = sp.read_text(encoding='utf-8')
            if txt.strip():
                sj = json.loads(txt)
                op = sj.get('open_positions', None)
                if isinstance(op, list):
                    open_pos_state = int(len(op))
    except Exception as e:
        flags.append(f'paper_state_parse_fail:{type(e).__name__}')
    open_positions_count = int(open_pos_state if (open_pos_state is not None) else open_pos_trades)
    open_positions = int(open_positions_count)

    dup_trades = _dup_trades_count(trades_csv, flags)
    if dup_trades:
        flags.append(f"dup_trades:{dup_trades}")

    krx_clean_date_max = _krx_clean_date_max(base_dir, flags)
    prev_weekday = ucommon.prev_weekday(dt.date.today())
    # krx_clean_ncode: krx_clean_date_max ?좎쭨???대떦?섎뒗 parquet???좊땲踰꾩뒪(code) ??理쒕?媛?

    krx_clean_ncode = _krx_clean_ncode(base_dir, krx_clean_date_max) if krx_clean_date_max else None
    try:
        if krx_clean_date_max:
            best_n = 0
            for _p in list(base_dir.rglob("krx_daily_*_clean.parquet")):
                _d = ucommon.read_parquet_date_max(_p, 'date')
                if _d is None:
                    continue
                _m = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", str(_d))
                if _m:
                    _d = f"{_m.group(1)}{_m.group(2)}{_m.group(3)}"
                if str(_d) != str(krx_clean_date_max):
                    continue
                _df = pd.read_parquet(_p, columns=["date","code"])
                _dd = pd.to_datetime(_df["date"], errors="coerce")
                if _dd.isna().all():
                    continue
                _mx = _dd.max()
                _n = int(_df.loc[_dd==_mx, "code"].astype(str).nunique())
                if _n > best_n:
                    best_n = _n
            krx_clean_ncode = best_n if best_n > 0 else krx_clean_ncode
    except Exception as e:
        flags.append(f"krx_clean_ncode_refine_fail:{type(e).__name__}")
        # keep previously computed krx_clean_ncode (do not overwrite to None)
    # risk_off: 理쒖냼 洹쒖튃留?(嫄곕옒/?먮떒??留됱븘???섎뒗 耳?댁뒪留?
    # NOTE: candidates meta??latest_date媛 吏곸쟾 ?됱씪蹂대떎 怨쇨굅?щ룄, gate瑜?留됱? ?딆쓬(濡쒓렇濡쒕쭔 ?뺤씤)
    risk_off = {"enabled": True, "reasons": ["INIT_FAIL_CLOSED"]}
    kill_switch = {"triggered": False, "reasons": [], "source": ""}  # default
    crash_risk_off = {"triggered": False, "reasons": []}  # default

    try:
        # 1) paper 媛寃??곗씠?곌? ?꾨낫(?좏샇) 湲곗??쇰낫???ㅼ퀜吏?寃쎌슦
        if md and date_max:
            md_yyyymmdd = re.sub(r"[^0-9]", "", str(md))  # '2025-12-24' -> '20251224'
            if md_yyyymmdd and (str(date_max) < md_yyyymmdd):
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"prices_date_max({date_max}) < meta_latest_date({md_yyyymmdd})")

        # 1.1) candidates meta媛 '吏곸쟾 ?됱씪'蹂대떎 怨쇨굅硫??ㅽ뀒??, ?좉퇋吏꾩엯??留됱븘????
        md_yyyymmdd2 = re.sub(r"[^0-9]", "", str(md)) if md else ""
        md_yyyymmdd2 = md_yyyymmdd2[:8]
        # [GATE_RELAX] cand_latest_date: allow lag-1 => SOFT flag, lag-2+ => risk_off
        prev_weekday_lag1 = None
        try:
            if prev_weekday:
                _d = dt.datetime.strptime(str(prev_weekday), "%Y%m%d").date() - dt.timedelta(days=1)
                while _d.weekday() >= 5:
                    _d = _d - dt.timedelta(days=1)
                prev_weekday_lag1 = _d.strftime("%Y%m%d")
        except Exception:
            flags.append("prev_weekday_lag1_calc_fail")
        if md_yyyymmdd2 and prev_weekday and prev_weekday_lag1:
            if md_yyyymmdd2 < prev_weekday_lag1:
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"cand_latest_date({md_yyyymmdd2}) < prev_weekday_lag1({prev_weekday_lag1})")
            elif md_yyyymmdd2 < prev_weekday:
                flags.append(f"SOFT_GATE cand_latest_date({md_yyyymmdd2}) < prev_weekday({prev_weekday})")
        # 1.2) krx clean ?좊땲踰꾩뒪媛 MIN_UNI(=2000) 誘몃쭔?대㈃(遺遺??곗씠??, ?좉퇋吏꾩엯??留됱븘????
        if (krx_clean_ncode is not None) and (int(krx_clean_ncode) < 2000):
            risk_off["enabled"] = True
            risk_off["reasons"].append(f"krx_clean_universe_degraded(ncode={int(krx_clean_ncode)} < MIN_UNI=2000)")

        # 1.5) Kill Switch (ROLLING DD / daily loss) - auto release (rolling window)
        kill_cfg = cfg.get("kill_switch", {}) if isinstance(cfg, dict) else {}
        try:
            ks_dd_lim = float(kill_cfg.get("max_drawdown_pct", 0.15) or 0.15)
        except Exception:
            ks_dd_lim = 0.15
        try:
            ks_day_lim = float(kill_cfg.get("max_daily_loss_pct", 0.08) or 0.08)
        except Exception:
            ks_day_lim = 0.08
        try:
            window_days = int(kill_cfg.get("window_days", 60) or 60)  # 理쒓렐 exit_date N媛??? 湲곗?
        except Exception:
            window_days = 60
        ks_mode = str(kill_cfg.get("mode", "BLOCK")).upper()

        kill_switch = {
            "enabled": False,
            "triggered": False,
            "reasons": [],
            "limits": {
                "max_drawdown_pct": abs(ks_dd_lim),
                "max_daily_loss_pct": abs(ks_day_lim),
                "mode": ks_mode,
                "window_days": int(window_days),
                "metric": "rolling_exit_dates",
            },
            "metrics": {},
            "source": "",
            # PATCH: record actual rolling input (if available)
            "input_trades_path": None,
            "input_ret_col": None,
            "input_trades_mtime": None,
        }

        # A?? paper_pnl_summary_last.json(=equity ?ы븿, ?ㅽ궎留??덉젙) ?곗꽑 ?ъ슜
        pnl_sum_path = None
        try:
            _last = logs_dir / "paper_pnl_summary_last.json"
            if _last.exists():
                try:
                    _j = json.load(open(_last, "r", encoding="utf-8"))
                    _eq = (_j.get("equity", None) if isinstance(_j, dict) else None)
                    if isinstance(_eq, dict) and ("max_drawdown_pct" in _eq) and ("last_day_ret" in _eq):
                        pnl_sum_path = _last
                except Exception:
                    pnl_sum_path = None
        except Exception:
            pnl_sum_path = None

        if pnl_sum_path is None:
            pnl_sum_path = None
        try:
            # (B) equity ?ы븿 summary 以?"理쒖떊"???좏깮 (?놁쑝硫?last.json fallback)
            best = None
            best_mtime = -1.0

            xs = sorted(logs_dir.glob("paper_pnl_summary_*.json"),
                        key=lambda x: x.stat().st_mtime, reverse=True)
            for fp in xs[:80]:
                try:
                    j = json.load(open(fp, "r", encoding="utf-8"))
                    eq0 = j.get("equity", None) if isinstance(j, dict) else None
                    if isinstance(eq0, dict) and all(k in eq0 for k in ("max_drawdown_pct","last_day_ret","last_exit_date")):
                        best = fp
                        best_mtime = fp.stat().st_mtime
                        break
                except Exception:
                    continue

            last_good = None
            last_path = (logs_dir / "paper_pnl_summary_last.json")
            if last_path.exists():
                try:
                    j = json.load(open(last_path, "r", encoding="utf-8"))
                    eq0 = j.get("equity", None) if isinstance(j, dict) else None
                    if isinstance(eq0, dict) and all(k in eq0 for k in ("max_drawdown_pct","last_day_ret","last_exit_date")):
                        last_good = last_path
                except Exception:
                    last_good = None

            if last_good is not None and (best is None or last_good.stat().st_mtime > best_mtime):
                pnl_sum_path = last_good
            else:
                pnl_sum_path = best

        except Exception:
            pnl_sum_path = None

        # 理쒖쥌 fallback (fail-closed): last.json -> 理쒖떊 summary_*.json
        if pnl_sum_path is None:
            _lp = logs_dir / "paper_pnl_summary_last.json"
            pnl_sum_path = _lp if _lp.exists() else latest_file(logs_dir, "paper_pnl_summary_*.json")
        kill_switch["source"] = str(pnl_sum_path) if pnl_sum_path else ""

        eq = {}
        if pnl_sum_path and pnl_sum_path.exists():
            try:
                pnl = json.load(open(pnl_sum_path, "r", encoding="utf-8"))
                eq = pnl.get("equity", {}) if isinstance(pnl, dict) else {}
            except Exception:
                eq = {}

        lifetime_max_dd = None
        lifetime_last_day_ret = None
        lifetime_last_exit_date = None
        try:
            if isinstance(eq, dict) and ("max_drawdown_pct" in eq):
                lifetime_max_dd = float(eq.get("max_drawdown_pct"))
            if isinstance(eq, dict) and ("last_day_ret" in eq):
                lifetime_last_day_ret = float(eq.get("last_day_ret"))
            if isinstance(eq, dict) and ("last_exit_date" in eq):
                lifetime_last_exit_date = str(eq.get("last_exit_date") or "")
        except Exception:
            pass

        roll = None
        try:
            import pandas as _pd
            import math as _math

            _df = _pd.read_csv(trades_csv, encoding="utf-8-sig")

            _ret_col = None
            for _c in ["pnl_pct", "ret_pct", "ret", "pnl", "profit_pct"]:
                if _c in _df.columns:
                    _ret_col = _c
                    break

            # PATCH: record actual inputs used for rolling calc attempt
            kill_switch["input_trades_path"] = str(trades_csv)
            kill_switch["input_ret_col"] = _ret_col
            try:
                if trades_csv.exists():
                    _mt = trades_csv.stat().st_mtime
                    kill_switch["input_trades_mtime"] = dt.datetime.fromtimestamp(_mt).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                kill_switch["input_trades_mtime"] = None

            if _ret_col and ("exit_date" in _df.columns):
                _df[_ret_col] = _df[_ret_col].apply(lambda x: float(str(x).strip()) if str(x).strip() else _math.nan)
                _df["exit_date"] = _df["exit_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
                _d = _df[(_df["exit_date"].str.len() == 8) & (~_df[_ret_col].isna())]
                if len(_d) > 0:
                    # rolling (SSOT for kill_switch decision): portfolio-style return per exit_date
                    # - MEAN: ?숈씪鍮꾩쨷 ?щ’ 媛??(USED/SSOT)
                    # - SUM : 李멸퀬
                    # - PROD: 李멸퀬(day-comp)
                    _g_all_sum  = _d.groupby("exit_date")[_ret_col].sum().sort_index()
                    _g_all_mean = _d.groupby("exit_date")[_ret_col].mean().sort_index()
                    # debug alt: PRODUCT comp per exit_date (same method as paper_pnl_report._equity_metrics/day_comp)
                    def _day_comp(_series):
                        vals = [v for v in _series.tolist() if isinstance(v, (int, float)) and (not _math.isnan(v))]
                        if not vals:
                            return 0.0
                        _eq = 1.0
                        for _r in vals:
                            _eq *= (1.0 + float(_r))
                        return _eq - 1.0
                    _g_all_prod = _d.groupby("exit_date")[_ret_col].apply(_day_comp).sort_index()

                    # apply same rolling window (by unique exit_date count)
                    if int(window_days) > 0 and len(_g_all_sum) > int(window_days):
                        _g_sum = _g_all_sum.iloc[-int(window_days):]
                        _g_mean = _g_all_mean.loc[_g_sum.index]
                        _g_prod = _g_all_prod.loc[_g_sum.index]
                    else:
                        _g_sum = _g_all_sum
                        _g_mean = _g_all_mean
                        _g_prod = _g_all_prod

                    # USED method (SSOT): MEAN
                    _eqs_used = (1.0 + _g_mean).cumprod()
                    _peak_used = _eqs_used.cummax()
                    _dd_used = _eqs_used / _peak_used - 1.0

                    # debug alt (SUM)
                    _eqs_sum = (1.0 + _g_sum).cumprod()
                    _peak_sum = _eqs_sum.cummax()
                    _dd_sum = _eqs_sum / _peak_sum - 1.0

                    roll = {
                        "mode": "rolling_exit_dates_used=MEAN",
                        "window_days": int(window_days),
                        "used_days": int(len(_g_mean)),
                        "last_exit_date": str(_g_mean.index[-1]),
                        "last_day_ret": float(_g_mean.iloc[-1]),
                        "end_equity": float(_eqs_used.iloc[-1]),
                        "peak_equity": float(_peak_used.iloc[-1]),
                        "dd_end_pct": float(_dd_used.iloc[-1]),
                        "max_drawdown_pct": float(_dd_used.min()),
                        "calc_method": "mean_by_exit_date_then_cumprod",
                    }

                    # debug alt (PRODUCT method)
                    try:
                        _eqs_prod = (1.0 + _g_prod).cumprod()
                        _peak_prod = _eqs_prod.cummax()
                        _dd_prod = _eqs_prod / _peak_prod - 1.0
                        roll["debug_alt_product"] = {
                            "last_exit_date": str(_g_prod.index[-1]) if len(_g_prod) else "",
                            "last_day_ret": float(_g_prod.iloc[-1]) if len(_g_prod) else 0.0,
                            "end_equity": float(_eqs_prod.iloc[-1]) if len(_eqs_prod) else 1.0,
                            "peak_equity": float(_peak_prod.iloc[-1]) if len(_peak_prod) else 1.0,
                            "dd_end_pct": float(_dd_prod.iloc[-1]) if len(_dd_prod) else 0.0,
                            "max_drawdown_pct": float(_dd_prod.min()) if len(_dd_prod) else 0.0,
                            "calc_method": "product_by_exit_date_then_cumprod",
                        }
                    except Exception as _e2:
                        roll["debug_alt_product"] = {"error": str(_e2), "calc_method": "product_by_exit_date_then_cumprod"}
        except Exception as _e:
            roll = {"error": str(_e)}

        max_dd = None
        last_day_ret = None
        last_exit_date = ""

        if isinstance(roll, dict) and ("max_drawdown_pct" in roll) and ("last_day_ret" in roll):
            max_dd = float(roll.get("max_drawdown_pct", 0.0) or 0.0)
            last_day_ret = float(roll.get("last_day_ret", 0.0) or 0.0)
            last_exit_date = str(roll.get("last_exit_date", "") or "")
            kill_switch["enabled"] = True
            kill_switch["metrics"] = {
                "mode": "rolling",
                "window_days": int(window_days),
                "used_days": int(roll.get("used_days", 0) or 0),
                "max_drawdown_pct": max_dd,
                "last_day_ret": last_day_ret,
                "last_exit_date": last_exit_date,
                "end_equity": roll.get("end_equity", None),
                "peak_equity": roll.get("peak_equity", None),
                "dd_end_pct": roll.get("dd_end_pct", None),
                "debug_lifetime_max_drawdown_pct": lifetime_max_dd,
                "debug_lifetime_last_day_ret": lifetime_last_day_ret,
                "debug_lifetime_last_exit_date": lifetime_last_exit_date,
                "debug_alt_product": (roll.get("debug_alt_product") if isinstance(roll, dict) else None),

                "debug_lifetime_calc_method": "product_by_exit_date_then_cumprod (paper_pnl_report._equity_metrics)",
                "debug_rolling_calc_method": str(roll.get("calc_method")) if isinstance(roll, dict) else None,
                "debug_diff_max_drawdown_pct": (float(lifetime_max_dd) - float(max_dd)) if (lifetime_max_dd is not None and max_dd is not None) else None,

            }
        elif isinstance(eq, dict) and ("max_drawdown_pct" in eq) and ("last_day_ret" in eq):
            # fail-safe fallback (legacy lifetime) if rolling calc is unavailable
            max_dd = float(eq.get("max_drawdown_pct", 0.0) or 0.0)
            last_day_ret = float(eq.get("last_day_ret", 0.0) or 0.0)
            last_exit_date = str(eq.get("last_exit_date", "") or "")
            kill_switch["enabled"] = True
            kill_switch["metrics"] = {
                "mode": "lifetime_fallback",
                "max_drawdown_pct": max_dd,
                "last_day_ret": last_day_ret,
                "last_exit_date": last_exit_date,
                "end_equity": eq.get("end_equity", None),
            }
        # --------------------------------------------------------------------
        # PATCH v2: kill_switch DD provenance + dd_curve artifact (always recompute from trades)
        # - does NOT change trading logic; adds auditable artifacts in 2_Logs
        # - FIX: recomputed/curve MUST align with SSOT calc_method=mean_by_exit_date_then_cumprod
        #        keep SUM as debug, but recomputed uses MEAN to make match_fmt4=True.
        # --------------------------------------------------------------------
        if kill_switch.get("enabled") and isinstance(kill_switch.get("metrics"), dict):
            try:
                import hashlib
                import datetime as _dt
                from pathlib import Path as _Path

                def _sha256_file(_p: _Path) -> str:
                    h = hashlib.sha256()
                    with open(_p, "rb") as f:
                        for chunk in iter(lambda: f.read(1 << 20), b""):
                            h.update(chunk)
                    return h.hexdigest()

                def _read_csv_min(_path: _Path, usecols: list[str]):
                    # encoding fallback
                    for _enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
                        try:
                            return pd.read_csv(_path, usecols=usecols, encoding=_enc), _enc
                        except Exception:
                            continue
                    # last resort
                    return pd.read_csv(_path, usecols=usecols), None

                def _read_header(_path: _Path):
                    for _enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
                        try:
                            df0 = pd.read_csv(_path, nrows=0, encoding=_enc)
                            return list(df0.columns), _enc
                        except Exception:
                            continue
                    df0 = pd.read_csv(_path, nrows=0)
                    return list(df0.columns), None

                tag = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")

                # determine trades path
                _trades_path = _Path(str(kill_switch.get("input_trades_path") or trades_csv))
                if not _trades_path.exists():
                    raise FileNotFoundError(f"trades_not_found:{_trades_path}")

                cols, _hdr_enc = _read_header(_trades_path)

                # infer exit_col
                _exit_col = "exit_date" if "exit_date" in cols else None
                if _exit_col is None:
                    for c in cols:
                        lc = str(c).lower()
                        if ("exit" in lc) and ("date" in lc):
                            _exit_col = c
                            break
                if _exit_col is None:
                    raise RuntimeError(f"exit_col_not_found in {cols[:50]}")

                # infer ret_col (priority: actual selected -> pnl_summary ret_col -> common heuristics)
                _ret_col = kill_switch.get("input_ret_col")
                if (not _ret_col) or (_ret_col not in cols):
                    # try pnl summary source (kill_switch["source"])
                    try:
                        _src = _Path(str(kill_switch.get("source") or ""))
                        if _src.exists():
                            _pj = json.load(open(_src, "r", encoding="utf-8"))
                            _rc = _pj.get("ret_col")
                            if isinstance(_rc, str) and _rc.strip() and (_rc.strip() in cols):
                                _ret_col = _rc.strip()
                    except Exception:
                        pass

                if (not _ret_col) or (_ret_col not in cols):
                    pref = ["pnl_pct", "ret", "ret_pct", "return", "returns", "pnl_ret", "day_ret", "day_ret_pct", "trade_ret"]
                    for c in pref:
                        if c in cols:
                            _ret_col = c
                            break

                if (not _ret_col) or (_ret_col not in cols):
                    # last heuristic: pick first column containing 'pnl' and ('pct' or 'ret')
                    for c in cols:
                        lc = str(c).lower()
                        if ("pnl" in lc) and (("pct" in lc) or ("ret" in lc) or ("return" in lc)):
                            _ret_col = c
                            break

                if (not _ret_col) or (_ret_col not in cols):
                    raise RuntimeError(f"ret_col_not_found (input_ret_col={kill_switch.get('input_ret_col')})")

                # read minimal data
                df, _enc_used = _read_csv_min(_trades_path, usecols=[_exit_col, _ret_col])

                ex = df[_exit_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
                rt = pd.to_numeric(df[_ret_col], errors="coerce")

                d = pd.DataFrame({"exit_date": ex, "ret": rt}).dropna()
                d = d[d["exit_date"].astype(str).str.len() == 8]
                if len(d) <= 0:
                    raise RuntimeError("no_valid_rows_for_dd_curve")

                g_sum_all  = d.groupby("exit_date")["ret"].sum().sort_index()
                g_mean_all = d.groupby("exit_date")["ret"].mean().sort_index()

                # apply same rolling window definition: last N unique exit_date rows (SSOT window anchored on MEAN)
                if int(window_days) > 0 and len(g_mean_all) > int(window_days):
                    g_mean = g_mean_all.iloc[-int(window_days):]
                    g_sum  = g_sum_all.loc[g_mean.index]
                else:
                    g_mean = g_mean_all
                    g_sum  = g_sum_all

                # SSOT recompute (MEAN)
                eq_mean   = (1.0 + g_mean).cumprod()
                peak_mean = eq_mean.cummax()
                dd_mean   = eq_mean / peak_mean - 1.0

                # debug (SUM)
                eq_sum   = (1.0 + g_sum).cumprod()
                peak_sum = eq_sum.cummax()
                dd_sum   = eq_sum / peak_sum - 1.0

                # recomputed values MUST follow SSOT (MEAN)
                max_dd_re = float(dd_mean.min()) if len(dd_mean) else 0.0
                dd_end_re = float(dd_mean.iloc[-1]) if len(dd_mean) else 0.0
                fmt4_re = float(f"{max_dd_re:.4f}")

                # debug(sum) recompute stats (kept for diagnosis)
                max_dd_sum_re = float(dd_sum.min()) if len(dd_sum) else 0.0
                fmt4_sum_re = float(f"{max_dd_sum_re:.4f}")

                # write dd_curve artifact ALWAYS (keep old *_sum columns + add *_mean columns)
                curve_p = logs_dir / f"kill_switch_dd_curve_{tag}.csv"
                pd.DataFrame({
                    "exit_date": g_mean.index.astype(str),

                    # debug(sum) - backward-compatible columns
                    "day_ret_sum": g_sum.values,
                    "equity_sum":  eq_sum.values,
                    "peak_sum":    peak_sum.values,
                    "dd_sum":      dd_sum.values,

                    # SSOT(mean) - new columns
                    "day_ret_mean": g_mean.values,
                    "equity_mean":  eq_mean.values,
                    "peak_mean":    peak_mean.values,
                    "dd_mean":      dd_mean.values,
                }).to_csv(curve_p, index=False, encoding="utf-8")

                curve_sha = _sha256_file(curve_p)

                # build provenance
                metrics_max_dd = float(kill_switch["metrics"].get("max_drawdown_pct", 0.0) or 0.0)
                metrics_fmt4 = float(f"{metrics_max_dd:.4f}")

                dd_source = {
                    "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
                    "generator": "p0_daily_check.py",
                    "input": {
                        "trades_path": str(_trades_path),
                        "trades_mtime": kill_switch.get("input_trades_mtime"),
                        "exit_col": str(_exit_col),
                        "ret_col": str(_ret_col),
                        "encoding_used": _enc_used,
                    },
                    "limits": {
                        "window_days": int(window_days),
                        "max_drawdown_pct": float(abs(ks_dd_lim)),
                        "max_daily_loss_pct": float(abs(ks_day_lim)),
                        "metric": "rolling_exit_dates",
                    },
                    "calc": {
                        "mode": str(kill_switch["metrics"].get("mode") or ""),
                        "calc_method": "mean_by_exit_date_then_cumprod",
                        "used_days": int(len(g_mean)),
                        "last_exit_date": str(g_mean.index[-1]) if len(g_mean) else "",
                        "max_dd_recomputed_raw": max_dd_re,
                        "max_dd_recomputed_fmt4": fmt4_re,
                        "dd_end_recomputed": dd_end_re,
                        "max_dd_metrics_raw": metrics_max_dd,
                        "max_dd_metrics_fmt4": metrics_fmt4,
                        "diff_metrics_minus_recomputed": float(metrics_max_dd - max_dd_re),
                        "match_fmt4": bool(metrics_fmt4 == fmt4_re),
                        "max_dd_recomputed_sum_raw": max_dd_sum_re,
                        "max_dd_recomputed_sum_fmt4": fmt4_sum_re,
                        "pnl_summary_source": str(kill_switch.get("source") or ""),
                    },
                    "artifact": {
                        "dd_curve_csv": {
                            "path": str(curve_p),
                            "rows": int(len(g_mean)),
                            "sha256": curve_sha,
                        }
                    },
                }

                src_p = logs_dir / f"kill_switch_dd_source_{tag}.json"
                with open(src_p, "w", encoding="utf-8") as f:
                    json.dump(dd_source, f, ensure_ascii=False, indent=2)
                dd_source["artifact"]["dd_source_json"] = {"path": str(src_p)}

                # attach to in-memory output
                kill_switch["metrics"]["dd_source"] = dd_source

            except Exception as _e:
                kill_switch["metrics"]["dd_source_error"] = f"{type(_e).__name__}: {_e}"


        if kill_switch.get("enabled"):
            if max_dd is None: max_dd = 0.0
            if last_day_ret is None: last_day_ret = 0.0
            if max_dd <= -abs(ks_dd_lim):
                kill_switch["triggered"] = True
                kill_switch["reasons"].append(f"MAX_DD({max_dd:.4f}<= -{abs(ks_dd_lim):.4f})")
            if last_day_ret <= -abs(ks_day_lim):
                kill_switch["triggered"] = True
                kill_switch["reasons"].append(f"DAILY_LOSS({last_day_ret:.4f} <= -{abs(ks_day_lim):.4f})")

        # 1.5b) Link kill_switch -> risk_off (fail-closed)
        # cleanup: INIT_FAIL_CLOSED is only for 'no-signal yet'. if any concrete reason exists, drop INIT.
        if "INIT_FAIL_CLOSED" in (risk_off.get("reasons") or []) and any(
            r for r in (risk_off.get("reasons") or []) if r and r != "INIT_FAIL_CLOSED"
        ):
            risk_off["reasons"] = [r for r in (risk_off.get("reasons") or []) if r != "INIT_FAIL_CLOSED"]

        if isinstance(kill_switch, dict) and kill_switch.get("triggered"):
            risk_off["enabled"] = True
            if "kill_switch" not in risk_off["reasons"]:
                risk_off["reasons"].append("kill_switch")
            for _r in (kill_switch.get("reasons") or []):
                _tag = "kill_switch:" + str(_r)
                if _tag not in risk_off["reasons"]:
                    risk_off["reasons"].append(_tag)

        # crash risk-off (index-based)

        crash_risk_off = _eval_crash_risk_off(str(date_max), cfg) if date_max else _eval_crash_risk_off(dt.datetime.now().strftime("%Y%m%d"), cfg)
        # normalize crash_risk_off (post-eval)
        # - metrics.status must be a non-empty string for logging/diagnostics
        if not isinstance(crash_risk_off, dict):
            crash_risk_off = {
                "enabled": bool((((cfg or {}).get("crash_risk_off") or {}).get("enabled", False)) if isinstance(cfg, dict) else False),
                "triggered": False,
                "reasons": ["crash_eval_missing"],
                "limits": ((((cfg or {}).get("crash_risk_off") or {}).get("limits") or {}) if isinstance(cfg, dict) else {}),
                "metrics": {"status": "missing_from_eval", "as_of_ymd": str(date_max) if date_max else None},
                "source": "pykrx",
            }
        else:
            m2 = crash_risk_off.get("metrics")
            if not isinstance(m2, dict):
                crash_risk_off["metrics"] = {}
                m2 = crash_risk_off["metrics"]
            if (m2.get("status") is None) or (str(m2.get("status")).strip() == ""):
                if crash_risk_off.get("enabled") is False:
                    m2["status"] = "disabled_by_config"
                else:
                    m2["status"] = "success_or_not_implemented"
        if crash_risk_off.get("triggered"):
            risk_off["enabled"] = True
            risk_off["reasons"].append("crash_risk_off")


# 2) KRX clean parquet媛 '吏곸쟾 ?됱씪' 湲곗??쇰줈 ?ㅻ옒??寃쎌슦(?좏샇媛 怨쇨굅 ?곗씠??湲곕컲)
        #    - ?붿슂???ㅼ쟾??吏곸쟾 湲덉슂?쇨퉴吏 ?덉뼱???뺤긽
        # [GATE_RELAX] krx_clean_date_max: allow lag-1 => SOFT flag, lag-2+ => risk_off
        prev_weekday_lag1 = None
        try:
            if prev_weekday:
                _d = dt.datetime.strptime(str(prev_weekday), "%Y%m%d").date() - dt.timedelta(days=1)
                while _d.weekday() >= 5:
                    _d = _d - dt.timedelta(days=1)
                prev_weekday_lag1 = _d.strftime("%Y%m%d")
        except Exception as e:
            flags.append("prev_weekday_lag1_calc_fail")
        if krx_clean_date_max and prev_weekday and prev_weekday_lag1:
            if krx_clean_date_max < prev_weekday_lag1:
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"krx_clean_date_max({krx_clean_date_max}) < prev_weekday_lag1({prev_weekday_lag1})")
            elif krx_clean_date_max < prev_weekday:
                flags.append(f"SOFT_GATE krx_clean_date_max({krx_clean_date_max}) < prev_weekday({prev_weekday})")

        # Fail-Closed release: 李⑤떒 ?ъ쑀媛 INIT留??⑥븘?덉쑝硫??덉슜?쇰줈 ?댁젣
        if (risk_off.get("reasons") == ["INIT_FAIL_CLOSED"]) or (not risk_off.get("reasons")):
            risk_off["enabled"] = False
            risk_off["reasons"] = []
    except Exception as e:
        # Fail-Closed: ?됯? ?덉쇅 ???좉퇋 吏꾩엯 李⑤떒 ?좎?
        risk_off['enabled'] = True
        rr = risk_off.get('reasons') or []
        rr = [x for x in rr if x and x != 'INIT_FAIL_CLOSED']
        rr.append(f"RISK_OFF_EVAL_FAIL({type(e).__name__})")
        risk_off['reasons'] = rr
        import traceback as _tb; _last=_tb.extract_tb(e.__traceback__)[-1] if getattr(e,"__traceback__",None) else None; flags.append(f"risk_off_eval_fail:{type(e).__name__}:{e} @ {_last.filename}:{_last.lineno}" if _last else f"risk_off_eval_fail:{type(e).__name__}:{e}")

        # optional log cleanup (default disabled)
    # normalize crash_risk_off (so metrics.status is always present)
    if not isinstance(crash_risk_off, dict):
        crash_risk_off = {
            "enabled": bool((((cfg or {}).get("crash_risk_off") or {}).get("enabled", False)) if isinstance(cfg, dict) else False),
            "triggered": False,
            "reasons": ["crash_eval_missing"],
            "limits": ((((cfg or {}).get("crash_risk_off") or {}).get("limits") or {}) if isinstance(cfg, dict) else {}),
            "metrics": {"status": "missing_from_eval", "as_of_ymd": str(date_max) if date_max else None},
            "source": "pykrx",
        }
    elif ("metrics" not in crash_risk_off) or (not isinstance(crash_risk_off.get("metrics"), dict)):
        crash_risk_off["metrics"] = {"status": "missing_metrics", "as_of_ymd": str(date_max) if date_max else None}
    _lc = ((cfg or {}).get("log_cleanup") or {}) if isinstance(cfg, dict) else {}
    log_cleanup = cleanup_old_logs(
        logs_dir,
        days=int(_lc.get("days", 30) or 30),
        enabled=bool(_lc.get("enabled", False)),
        keep_last=int(_lc.get("keep_last", 50) or 50),
        patterns=_lc.get("patterns", None),
    )

    # self-adaptive data_state / verify_plan (no report self-reference)
    try:
        _sa_gap = _build_self_adaptive_gap(
            str(prev_weekday or ''),
            str((prices.get('date_max') if isinstance(prices, dict) else '') or ''),
            str(krx_clean_date_max or ''),
            str(md or ''),
        )
        data_state = _sa_gap if isinstance(_sa_gap, dict) else None
        verify_plan = (data_state.get('verify_plan') if isinstance(data_state, dict) else None)
    except Exception as e:
        data_state = {'gap_level':'UNKNOWN','gap_parts':{},'gap_max':None,'error':f"{type(e).__name__}:{str(e)[:120]}"}
        verify_plan = {'level':'UNKNOWN','G':None,'checks':[],'error':f"{type(e).__name__}:{str(e)[:120]}"}
        try:
            flags.append(f"self_adaptive_eval_fail:{type(e).__name__}:{str(e)[:120]}")
        except Exception:
            pass

    report: Dict[str, Any] = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'as_of_ymd': (str(prices.get('date_max')) if prices.get('date_max') else None),
        'base_dir': str(base_dir),
        'prices': prices,
        'meta': {'latest_date': md, 'market_regime': mr, 'path': str(meta_path)},
        'krx_clean': {'date_max': krx_clean_date_max, 'prev_weekday': prev_weekday, 'ncode': krx_clean_ncode},
        'risk_off': risk_off,
        'kill_switch': kill_switch,
        'crash_risk_off': crash_risk_off,
        'data_state': data_state,
        'verify_plan': verify_plan,
        'paper': {'open_positions_count': int(open_positions_count), 'dup_trades_count': int(dup_trades or 0), 'trades_csv': str(trades_csv)},
        'config': {'max_hold_days': max_hold_days, 'path': str(cfg_path), 'crash_risk_off': (cfg.get('crash_risk_off') if isinstance(cfg, dict) else None)},
        'log_cleanup': log_cleanup,
        'flags': flags,
    }

    # final cleanup: drop INIT_FAIL_CLOSED if any concrete reason exists

    try:

        rr = (risk_off.get('reasons') or []) if isinstance(risk_off, dict) else []

        if 'INIT_FAIL_CLOSED' in rr and any(r for r in rr if r and r != 'INIT_FAIL_CLOSED'):

            risk_off['reasons'] = [r for r in rr if r != 'INIT_FAIL_CLOSED']

    except Exception:

        pass


    out_path = logs_dir / f"p0_daily_check_{now_tag()}.json"
    report["open_positions"] = int(open_positions)
    report["open_positions_state"] = (int(open_pos_state) if (open_pos_state is not None) else None)
    report["open_positions_trades_debug"] = int(open_pos_trades)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f"[P0_CHECK] wrote: {out_path}")
    print(f"[P0_CHECK] prices_date_max={report['prices'].get('date_max')} rows={report['prices'].get('rows')} codes={report['prices'].get('codes')}")
    print(f"[P0_CHECK] market_regime={mr} risk_off={report['risk_off']['enabled']} reasons={report['risk_off']['reasons']}")
    print(f"[P0_CHECK] open_positions={report['paper'].get('open_positions_count')} max_hold_days={report['config'].get('max_hold_days')}")
    print(f"[P0_CHECK] krx_clean_date_max={report.get('krx_clean',{}).get('date_max')} prev_weekday={report.get('krx_clean',{}).get('prev_weekday')} dup_trades={report.get('paper',{}).get('dup_trades_count')}")
    if report['flags']:
        print(f"[P0_CHECK] flags={report['flags']}")


if __name__ == '__main__':
    main()





