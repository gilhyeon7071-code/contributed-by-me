#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
p0_daily_check.py

P0 daily check.
- Validate freshness and code coverage of paper/prices/ohlcv_paper.parquet.
- Validate latest candidate metadata and market context.
- risk_off: enabled when price data is stale against required operating dates.
- paper open_positions_count: derived from trades/fills state without using None as a count.
- max_hold_days: read from paper/paper_engine_config.json.

Outputs:
- 2_Logs/p0_daily_check_YYYYMMDD_HHMMSS.json
- Console summary.
"""

from __future__ import annotations
import utils.common as ucommon

def _krx_clean_ncode(base_dir, date_max_yyyymmdd):
    """
    Count unique codes in the krx_daily_*_clean.parquet file for date_max.
    Return None on failure so the caller can record krx_clean_ncode_fail.
    """
    try:
        ymd = str(date_max_yyyymmdd or "").strip()
        if not ymd:
            return None

        base = Path(str(base_dir))
        cand = []

        cand += _krx_clean_files(base, f"krx_daily_*_{ymd}_clean.parquet")
        cand += _krx_clean_files(base, f"krx_daily_{ymd}_{ymd}_clean.parquet")

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


def _krx_clean_files(base_dir, pattern: str = "krx_daily_*_clean.parquet"):
    base = Path(str(base_dir))
    out = []
    seen = set()
    for d in [base / "_krx_manual", base / "krx_daily_archive", base]:
        try:
            if not d.exists() or not d.is_dir():
                continue
            for p in d.glob(pattern):
                key = str(p.resolve())
                if key in seen:
                    continue
                seen.add(key)
                out.append(p)
        except Exception:
            continue
    return out

import json
import logging
import re
import subprocess
import sys
import datetime as dt
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import os  # PATCH: for trades mtime stamp
import pandas as pd

from holiday_manager import HolidayManager


def _load_krx_soft_skip_meta(logs_dir: Path) -> Dict[str, Dict[str, Any]]:
    try:
        fp = logs_dir / "krx_price_integrity_status_latest.json"
        if not fp.exists():
            return {}
        obj = json.loads(fp.read_text(encoding="utf-8"))
        rows = obj.get("soft_skipped_days") if isinstance(obj, dict) else None
        if not isinstance(rows, list):
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            ymd = _norm_ymd_oper(row.get("date"))
            if len(ymd) != 8:
                continue
            out[ymd] = row
        return out
    except Exception:
        return {}


def _norm_ymd_oper(x: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(x or ""))
    return s[:8] if len(s) >= 8 else ""


def _resolve_oper_start_ymd() -> str:
    raw = os.getenv("PAPER_OPER_START_YMD", "20260301")
    ymd = _norm_ymd_oper(raw)
    return ymd if len(ymd) == 8 else ""
def _resolve_hard_lag_days(default: int = 2) -> int:
    raw = str(os.getenv("P0_DATA_HARD_LAG_DAYS", str(default)) or str(default)).strip()
    try:
        v = int(raw)
    except Exception:
        v = int(default)
    return max(1, min(v, 5))


def _is_daily_loss_active(last_exit_date: Any, current_ymd: Any, last_day_ret: Any = None) -> bool:
    last_exit = str(last_exit_date or "").strip()
    current = str(current_ymd or "").strip()
    if current and last_day_ret is not None:
        try:
            if not pd.isna(last_day_ret):
                return True
        except Exception:
            pass
    return bool(last_exit and current and (last_exit == current))


def _prev_weekday_lag(ymd: str, lag_days: int) -> str | None:
    try:
        cal = HolidayManager()
        cur = str(ymd)
        for _ in range(int(lag_days)):
            cur = cal.previous_trading_day(cur)
            if not cur:
                return None
        return cur
    except Exception:
        return None

import time

# 怨듯넻 ?좏떥由ы떚 紐⑤뱢 import
from utils.common import (
    now_tag,
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


def _p0_checkpoint(logs_dir: Path, step: str, extra: Dict[str, Any] | None = None) -> None:
    """Write the latest P0 progress marker so timeout failures keep a last-known step."""
    try:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "step": str(step),
            "extra": extra or {},
        }
        latest_json = logs_dir / "p0_daily_check_progress_latest.json"
        latest_txt = logs_dir / "p0_daily_check_progress_latest.txt"
        latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        with latest_txt.open("a", encoding="utf-8") as f:
            f.write(f"{payload['generated_at']} {payload['step']} {json.dumps(payload['extra'], ensure_ascii=False)}\n")
    except Exception:
        pass


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

    if not enabled:
        return {
            "enabled": False,
            "days": int(days),
            "keep_last": int(keep_last),
            "patterns": pats,
            "would_delete_count": 0,
            "deleted_count": 0,
            "would_delete_sample": [],
            "deleted_sample": [],
            "errors": ["scan_skipped_cleanup_disabled"],
        }

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
    files = _krx_clean_files(base_dir)
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


# latest_file helper is imported from utils.common.

def _dup_trades_count(trades_csv: Path, flags: list[str]) -> int:
    """Count duplicate trade rows using the available trade key columns."""
    if not trades_csv.exists():
        return 0
    try:
        t = pd.read_csv(trades_csv)
        if len(t) == 0:
            return 0
        # Partial exits can share same code/date/price/reason. Include sell_qty from note when available.
        #
        # [2026-09-10] sell_qty 만으로는 부족했다. **14건이 전부 오탐이었다.**
        #   손절 경로에는 "50% 부분 손절 -> 재손절로 잔량 강제청산" 의 두 다리가 있다.
        #   두 다리의 수량이 같으면(예: 12주 -> 6주 + 6주) 이 키가 충돌한다.
        #   실측: 004310 20260619 은 T000596(ratio=50, partial=1, NORMAL) 과
        #         T000597(ratio=100, partial=0, FORCE, force:repeat_stop) 로 **다른 청산**이고
        #         pnl 도 -3859.88 씩 나눠 갖는다(합 -7720 = 12주 x 9010 의 -7.14%). 이중계상이 아니다.
        #   오탐이 STOP 18.2% / STOP_PREEMPTIVE 11.1% 에만 몰리고 나머지 24종은 0% 였던 것도
        #   재손절 강제청산 로직이 그 경로에만 있기 때문이다.
        #   partial_exit 을 키에 넣으면 14 -> 0. note 까지 완전 동일한 행은 원래 0건이다
        #   (= 진짜 이중기록은 존재하지 않았다).
        if "note" in t.columns:
            try:
                note_s = t["note"].astype(str)
                t["_sell_qty"] = note_s.str.extract(r"sell_qty=([^;]*)", expand=False).fillna("").astype(str).str.strip()
                t["_sell_ratio"] = note_s.str.extract(r"sell_ratio_pct=([^;]*)", expand=False).fillna("").astype(str).str.strip()
                t["_partial"] = note_s.str.extract(r"partial_exit=([^;]*)", expand=False).fillna("").astype(str).str.strip()
            except Exception:
                t["_sell_qty"] = ""
                t["_sell_ratio"] = ""
                t["_partial"] = ""
        else:
            t["_sell_qty"] = ""
            t["_sell_ratio"] = ""
            t["_partial"] = ""
        key_pref = ["code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "exit_reason",
                    "_sell_qty", "_sell_ratio", "_partial"]
        key_cols = [c for c in key_pref if c in t.columns]
        if len(key_cols) < 3:
            flags.append("dup_trades_key_cols_insufficient")
            return 0
        dup = t.duplicated(subset=key_cols, keep="first")
        return int(dup.sum())
    except Exception as e:
        flags.append(f"dup_trades_parse_fail:{trades_csv}:{type(e).__name__}")
        return 0


def _to_float_or_none(v: Any) -> float | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "null"}:
            return None
        return float(s)
    except Exception:
        return None


def _resolve_broker_account_metrics(base_dir: Path, max_age_days: float = 1.5) -> Dict[str, Any]:
    """[2026-09-11] **브로커 실측**을 자본 기준 후보로 읽는다.

    아래 _pick_conservative_account_metrics 의 주석은 "어느 원장이 맞는지 밝혀지기 전까지"
    보수적인 쪽을 쓴다고 적혀 있다. **브로커가 그 판별자다** - 원장이 아니라 외부 사실이다.
    생산자: tools/broker_ledger_reconcile.py -> 2_Logs/broker_account_basis_latest.json

    신선하지 않으면 {} 를 돌려주고 기존(보수적 선택) 경로로 떨어진다.
    """
    out: Dict[str, Any] = {}
    try:
        f = base_dir / "2_Logs" / "broker_account_basis_latest.json"
        if not f.is_file():
            return {}
        import time as _time
        age_days = (_time.time() - f.stat().st_mtime) / 86400.0
        if age_days > float(max_age_days):
            return {}
        d = json.loads(f.read_text(encoding="utf-8-sig"))
        if str(d.get("status") or "") != "PASS":
            return {}
        if not isinstance(d.get("max_drawdown_pct"), (int, float)):
            return {}
        out = dict(d)
        out["account_basis_selected"] = "broker"
        out["broker_artifact_age_days"] = round(age_days, 4)
    except Exception:
        return {}
    return out


def _pick_conservative_account_metrics(primary: Dict[str, Any], trades: Dict[str, Any]) -> Dict[str, Any]:
    """[2026-09-04] 두 원장 경로 중 **더 깊은 낙폭**(보수적)을 자본 잠금의 근거로 고른다.

    자본 잠금은 안전장치다. 두 원장이 어긋날 때 느슨한 쪽을 고르면 장치가 조용히 둔해진다.
    어느 원장이 맞는지 밝혀지기 전까지는 막는 쪽을 고른다. 괴리는 통째로 기록해 드러낸다.
    """
    def _usable(m):
        return (
            isinstance(m, dict)
            and str(m.get("status") or "") == "PASS"
            and isinstance(m.get("max_drawdown_pct"), (int, float))
        )

    if not _usable(primary):
        return trades if _usable(trades) else (primary if isinstance(primary, dict) else {})
    if not _usable(trades):
        return primary

    p_dd = float(primary.get("max_drawdown_pct"))
    t_dd = float(trades.get("max_drawdown_pct"))
    chosen, other, chosen_tag = (
        (primary, trades, "primary") if p_dd <= t_dd else (trades, primary, "trades_csv")
    )
    out = dict(chosen)
    out["account_basis_selected"] = chosen_tag
    out["account_basis_divergence"] = {
        "primary_source": str(primary.get("source") or ""),
        "primary_basis": str(primary.get("basis") or ""),
        "primary_max_drawdown_pct": p_dd,
        "primary_equity_est": primary.get("equity_est"),
        "primary_realized_total_krw": primary.get("realized_total_krw"),
        "trades_source": str(trades.get("source") or ""),
        "trades_max_drawdown_pct": t_dd,
        "trades_equity_est": trades.get("equity_est"),
        "trades_realized_total_krw": trades.get("realized_total_krw"),
        "dd_gap_pct_points": round((p_dd - t_dd) * 100.0, 6),
        "chosen": chosen_tag,
        "rule": "deeper_drawdown_wins_until_ledger_reconciled",
    }
    try:
        _pe, _te = primary.get("equity_est"), trades.get("equity_est")
        if isinstance(_pe, (int, float)) and isinstance(_te, (int, float)):
            out["account_basis_divergence"]["equity_gap_krw"] = round(float(_pe) - float(_te), 2)
    except Exception:
        pass
    notes = list(out.get("notes") or [])
    if abs(p_dd - t_dd) > 1e-9:
        notes.append("account_ledger_divergence_conservative_pick:%s" % chosen_tag)
    out["notes"] = notes
    return out


def _resolve_account_kill_switch_metrics(
    base_dir: Path, cfg: dict, as_of_ymd: str, skip_dashboard: bool = False
) -> Dict[str, Any]:
    """Resolve account/equity-basis loss metrics for capital-lock decisions.

    Strategy return-curve MDD remains useful for diagnostics, but a hard capital
    lock needs an account basis so percent loss is measured against capital.

    [2026-09-04] skip_dashboard 추가. 호출부가 대시보드 경로와 trades.csv 경로를
      **둘 다** 구해 대조하고 보수적인 쪽을 쓰기 위한 것이다. 두 원장이 실현손익
      기준으로 1,084만원(5.7배) 어긋나 있는데, 그동안 as_of 불일치로 대시보드 경로가
      계속 건너뛰어져서 이 괴리가 드러난 적이 없었다. 상세는 PLANS (210).
    """
    out: Dict[str, Any] = {
        "status": "UNKNOWN",
        "basis": "capital_total_plus_realized_pnl",
        "as_of_ymd": str(as_of_ymd or ""),
        "capital_total": None,
        "equity_est": None,
        "realized_total_krw": None,
        "realized_today_krw": None,
        "max_drawdown_pct": None,
        "last_day_ret": None,
        "source": "",
        "notes": [],
    }
    try:
        capital_total = _to_float_or_none((cfg or {}).get("capital_total"))
        if not capital_total or capital_total <= 0:
            out["status"] = "NO_CAPITAL_TOTAL"
            return out

        dashboard_path = base_dir.parent / "vibe" / "buffett" / "runs" / "dashboard_state_latest.json"
        try:
            if (not skip_dashboard) and dashboard_path.exists():
                dash = json.loads(dashboard_path.read_text(encoding="utf-8-sig"))
                dash_asof = str((dash or {}).get("as_of_ymd") or "").strip()
                account = (dash or {}).get("account") if isinstance(dash, dict) else None
                if dash_asof == str(as_of_ymd or "").strip() and isinstance(account, dict):
                    dash_cap = _to_float_or_none(account.get("capital_total")) or capital_total
                    equity_est = _to_float_or_none(account.get("equity_est"))
                    realized_today = _to_float_or_none(account.get("realized_today_krw"))
                    realized_total = _to_float_or_none(account.get("realized_total_krw"))
                    unrealized_pnl = _to_float_or_none(account.get("unrealized_pnl_krw"))
                    if unrealized_pnl is None:
                        unrealized_pnl = _to_float_or_none(account.get("eval_pnl_krw"))
                    position_value = _to_float_or_none(account.get("position_value"))
                    position_cost = _to_float_or_none(account.get("position_cost"))
                    if dash_cap and dash_cap > 0 and equity_est is not None:
                        realized_equity = float(equity_est)
                        realized_daily_loss_pct = float((realized_today or 0.0) / dash_cap)
                        mtm_equity = realized_equity
                        basis = "dashboard_account_realized_equity"
                        notes = ["realized_equity_drawdown_used"]
                        if unrealized_pnl is not None and position_value is not None and position_value > 0:
                            mtm_equity = realized_equity + float(unrealized_pnl)
                            basis = "dashboard_account_mark_to_market_equity"
                            notes = ["mark_to_market_equity_includes_unrealized_pnl"]
                        prior_peak = None
                        mtm_daily_loss_pct = None
                        mtm_daily_loss_status = "NEEDS_ACCOUNT_EQUITY_HISTORY"
                        try:
                            hist_latest = base_dir / "2_Logs" / "account_equity_history_latest.json"
                            if hist_latest.exists():
                                hist_j = json.loads(hist_latest.read_text(encoding="utf-8-sig"))
                                prior_peak = _to_float_or_none(hist_j.get("history_peak_equity"))
                                mtm_daily_loss_pct = _to_float_or_none(hist_j.get("mark_to_market_daily_loss_pct"))
                                mtm_daily_loss_status = str(
                                    hist_j.get("mark_to_market_daily_loss_status")
                                    or "NEEDS_PREVIOUS_DAILY_EQUITY"
                                )
                        except Exception:
                            prior_peak = None
                        account_peak_equity = max(
                            float(dash_cap),
                            float(mtm_equity),
                            float(prior_peak) if prior_peak is not None else float(dash_cap),
                        )
                        current_dd = float(mtm_equity / account_peak_equity - 1.0)
                        out.update(
                            {
                                "status": "PASS",
                                "basis": basis,
                                "capital_total": float(dash_cap),
                                "equity_est": float(mtm_equity),
                                "realized_equity_est": realized_equity,
                                "mark_to_market_equity_est": float(mtm_equity),
                                "unrealized_pnl_krw": unrealized_pnl,
                                "position_value": position_value,
                                "position_cost": position_cost,
                                "realized_total_krw": realized_total,
                                "realized_today_krw": realized_today,
                                "max_drawdown_pct": current_dd,
                                "account_peak_equity": account_peak_equity,
                                "account_mdd_basis": "history_peak_with_capital_floor",
                                "last_day_ret": realized_daily_loss_pct,
                                "daily_loss_pct": realized_daily_loss_pct,
                                "daily_loss_basis": "realized_today_over_capital",
                                "mark_to_market_daily_loss_pct": mtm_daily_loss_pct,
                                "mark_to_market_daily_loss_status": mtm_daily_loss_status,
                                "source": str(dashboard_path),
                                "used_days": None,
                                "last_exit_date": str(as_of_ymd or ""),
                                "notes": notes,
                            }
                        )
                        return out
        except Exception as dash_e:
            out["notes"].append(f"dashboard_account_fallback:{type(dash_e).__name__}:{dash_e}")

        trades_path = base_dir / "paper" / "trades.csv"
        out["source"] = str(trades_path)
        if not trades_path.exists():
            out["status"] = "TRADES_MISSING"
            return out

        df = pd.read_csv(trades_path, encoding="utf-8-sig")
        if df.empty:
            out.update(
                {
                    "status": "PASS",
                    "capital_total": float(capital_total),
                    "equity_est": float(capital_total),
                    "realized_total_krw": 0.0,
                    "realized_today_krw": 0.0,
                    "max_drawdown_pct": 0.0,
                    "last_day_ret": 0.0,
                }
            )
            return out
        if "pnl_krw" not in df.columns:
            out["status"] = "PNL_KRW_MISSING"
            return out

        date_col = None
        for c in ("exit_date", "exit_ts", "datetime"):
            if c in df.columns:
                date_col = c
                break
        if date_col is None:
            out["status"] = "EXIT_DATE_MISSING"
            return out

        df["_exit_ymd"] = df[date_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
        df["_pnl_krw"] = pd.to_numeric(df["pnl_krw"], errors="coerce").fillna(0.0)
        df = df[df["_exit_ymd"].astype(str).str.len() == 8].copy()
        if df.empty:
            out["status"] = "NO_VALID_EXIT_ROWS"
            return out

        as_of = str(as_of_ymd or "").strip()
        if as_of:
            df = df[df["_exit_ymd"] <= as_of].copy()
        if df.empty:
            out.update(
                {
                    "status": "PASS",
                    "capital_total": float(capital_total),
                    "equity_est": float(capital_total),
                    "realized_total_krw": 0.0,
                    "realized_today_krw": 0.0,
                    "max_drawdown_pct": 0.0,
                    "last_day_ret": 0.0,
                }
            )
            return out

        daily = df.groupby("_exit_ymd", sort=True)["_pnl_krw"].sum()
        equity = float(capital_total) + daily.cumsum()
        peak = equity.cummax()
        dd = equity / peak - 1.0
        realized_total = float(daily.sum())
        realized_today = float(daily.loc[as_of]) if as_of and as_of in daily.index else 0.0
        out.update(
            {
                "status": "PASS",
                "capital_total": float(capital_total),
                "equity_est": float(capital_total) + realized_total,
                "realized_total_krw": realized_total,
                "realized_today_krw": realized_today,
                "max_drawdown_pct": float(dd.min()) if len(dd) else 0.0,
                "last_day_ret": float(realized_today / float(capital_total)),
                "used_days": int(len(daily)),
                "last_exit_date": str(daily.index[-1]) if len(daily) else "",
            }
        )
        return out
    except Exception as e:
        out["status"] = f"ERROR:{type(e).__name__}"
        out["error"] = str(e)
        return out


def _write_account_equity_history(logs_dir: Path, report: Dict[str, Any]) -> Dict[str, Any]:
    status: Dict[str, Any] = {
        "status": "NA",
        "reason": "",
        "history_csv": str(logs_dir / "account_equity_history.csv"),
        "latest_json": str(logs_dir / "account_equity_history_latest.json"),
    }
    try:
        metrics = ((report.get("kill_switch") or {}).get("metrics") or {})
        account = metrics.get("account_basis") if isinstance(metrics, dict) else None
        if not isinstance(account, dict) or account.get("status") != "PASS":
            status.update({"status": "SKIP", "reason": "account_basis_not_pass"})
            return status
        equity = _to_float_or_none(account.get("equity_est"))
        capital = _to_float_or_none(account.get("capital_total"))
        if equity is None or capital is None or capital <= 0:
            status.update({"status": "SKIP", "reason": "missing_equity_or_capital"})
            return status

        generated_at = str(report.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        as_of_ymd = str(account.get("as_of_ymd") or report.get("as_of_ymd") or "").strip()
        row = {
            "generated_at": generated_at,
            "as_of_ymd": as_of_ymd,
            "basis": account.get("basis"),
            "capital_total": float(capital),
            "equity_est": float(equity),
            "realized_equity_est": _to_float_or_none(account.get("realized_equity_est")),
            "mark_to_market_equity_est": _to_float_or_none(account.get("mark_to_market_equity_est")),
            "realized_total_krw": _to_float_or_none(account.get("realized_total_krw")),
            "realized_today_krw": _to_float_or_none(account.get("realized_today_krw")),
            "unrealized_pnl_krw": _to_float_or_none(account.get("unrealized_pnl_krw")),
            "position_value": _to_float_or_none(account.get("position_value")),
            "position_cost": _to_float_or_none(account.get("position_cost")),
            "source": account.get("source"),
        }

        history_csv = logs_dir / "account_equity_history.csv"
        if history_csv.exists():
            hist = pd.read_csv(history_csv, dtype={"as_of_ymd": str})
        else:
            hist = pd.DataFrame()
        hist = pd.concat([hist, pd.DataFrame([row])], ignore_index=True)
        hist = hist.drop_duplicates(subset=["generated_at", "as_of_ymd"], keep="last")
        hist = hist.sort_values(["as_of_ymd", "generated_at"], kind="stable")
        hist.to_csv(history_csv, index=False, encoding="utf-8-sig")

        daily = hist.copy()
        daily["equity_est"] = pd.to_numeric(daily["equity_est"], errors="coerce")
        daily = daily.dropna(subset=["as_of_ymd", "equity_est"])
        daily = daily[daily["as_of_ymd"].astype(str).str.fullmatch(r"\d{8}")]
        daily = daily.sort_values(["as_of_ymd", "generated_at"], kind="stable")
        daily = daily.groupby("as_of_ymd", as_index=False).tail(1)
        daily = daily.sort_values("as_of_ymd", kind="stable")
        daily_ret = None
        mdd = None
        peak_equity = None
        previous_equity = None
        if len(daily):
            eq = daily["equity_est"].astype(float)
            capital_series = pd.to_numeric(daily["capital_total"], errors="coerce").dropna()
            capital_floor = float(capital_series.iloc[0]) if len(capital_series) else 0.0
            peak = pd.concat([pd.Series([capital_floor]), eq], ignore_index=True).cummax().iloc[1:]
            dd = eq.reset_index(drop=True) / peak.reset_index(drop=True) - 1.0
            mdd = float(dd.min())
            peak_equity = float(peak.max())
            if len(daily) >= 2:
                previous_equity = float(eq.iloc[-2])
                if previous_equity > 0:
                    daily_ret = float(eq.iloc[-1] / previous_equity - 1.0)

        latest = {
            "generated_at": generated_at,
            "status": "PASS",
            "history_csv": str(history_csv),
            "rows": int(len(hist)),
            "daily_rows": int(len(daily)),
            "latest": row,
            "account_mdd_from_history_peak": mdd,
            "history_peak_equity": peak_equity,
            "previous_daily_equity": previous_equity,
            "mark_to_market_daily_loss_pct": daily_ret,
            "mark_to_market_daily_loss_status": "PASS" if daily_ret is not None else "NEEDS_PREVIOUS_DAILY_EQUITY",
        }
        latest_json = logs_dir / "account_equity_history_latest.json"
        latest_json.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")
        status.update(
            {
                "status": "PASS",
                "reason": "written",
                "rows": int(len(hist)),
                "daily_rows": int(len(daily)),
                "account_mdd_from_history_peak": mdd,
                "mark_to_market_daily_loss_pct": daily_ret,
                "mark_to_market_daily_loss_status": latest["mark_to_market_daily_loss_status"],
            }
        )
        return status
    except Exception as e:
        status.update({"status": "FAIL", "reason": f"{type(e).__name__}:{e}"})
        return status


# read_json helper is imported from utils.common.
# parse_yyyymmdd helper is imported from utils.common.


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
    NOTE: v1 only compares YYYYMMDD integers; calendar-aware gaps can be added in v1.1.
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
    min_proxy_rows = max(20, min(60, int(window_days or 60)))
    skipped_sources = []

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

        # [2026-08-31 O5-8] 0) **진짜 지수(구성일치 혼합)를 가장 먼저 쓴다.**
        #   아래 krx_clean 프록시의 "지수" 는 파케이 파일 안 종목들의 평균 종가다.
        #   단일 파일에서 60일을 요구하는데 최근 자료는 1일치 파일이라 최신 파일을 전부 건너뛰고
        #   2026-04-03 에서 끝나는 옛 아카이브로 **성공**했다 -> 4월 시장으로 오늘을 판정했다.
        #   파일을 합쳐 고치려니 유니버스가 달라 수준이 튀어 가짜 폭락(-98%)이 났다.
        #   .agent/PLANS.md (163)(164)(165)(166)
        #   -> 2_Logs/index_daily_history.csv (VIBE_Index_Daily_Fetch 매일 16:05 갱신)의
        #      KOSPI/KOSDAQ 을 체결 구성(46.4/53.6)으로 가중한 혼합 지수를 쓴다.
        #      일별 수익률을 섞는다(레벨을 섞지 않는다). utils/crash_index_blend.py
        #   실패하면 None 을 얻어 기존 경로가 그대로 동작한다(FAIL-CLOSED).
        #   **문턱은 바꾸지 않았다.** 프록시 경로의 실효 한계 fallback_trigger_max_dd_pct=0.35 는
        #   이 혼합 지수에서 연 5일(2.2%, 약 p98) 발동으로 이미 타당하다.
        try:
            from utils.crash_index_blend import build_blend_series, max_dd_and_day_ret
            _bs, _bi = build_blend_series(str(as_of_ymd or ""))
            if _bs and len(_bs) >= min_proxy_rows:
                _bm = max_dd_and_day_ret(_bs, int(window_days or 60))
                if _bm:
                    out["source"] = "index_blend"
                    out["rows"] = int(len(_bs))
                    out["used_rows"] = int(_bm["used_rows"])
                    out["max_dd"] = float(_bm["max_dd"])
                    out["day_ret"] = float(_bm["day_ret"])
                    # [2026-09-07] 실시간 낙폭 판정용. max_dd 는 참고로 남긴다.
                    for _k in ("cur_dd", "window_high", "window_low", "rebound_from_low"):
                        if _k in _bm:
                            out[_k] = float(_bm[_k])
                    out["blend_date_max"] = str(_bm["date_max"])
                    out["blend_weights"] = _bi.get("weights")
                    out["blend_label_mismatch"] = _bi.get("label_mismatch")
                    out["status"] = "ok"
                    out["ok"] = True
                    return out
            else:
                skipped_sources.append({"path": "index_blend", "rows": int(len(_bs)),
                                        "reason": str(_bi.get("status") or "insufficient_rows")})
        except Exception as _be:  # noqa: BLE001
            skipped_sources.append({"path": "index_blend", "rows": 0,
                                    "reason": "error:%s" % type(_be).__name__})

        # 1) Prefer market-wide krx_clean proxy (full universe) to avoid candidate-only bias.
        krx_candidates = []
        for p in _krx_clean_files(base_dir):
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
            if idx is not None and len(idx) >= min_proxy_rows:
                break
            if idx is not None:
                skipped_sources.append({"path": str(kp), "rows": int(len(idx)), "reason": "insufficient_proxy_rows"})
            idx = None

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
            if skipped_sources:
                out["skipped_sources"] = skipped_sources[:10]
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
        if skipped_sources:
            out["skipped_sources"] = skipped_sources[:10]
            out["min_proxy_rows"] = int(min_proxy_rows)
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


def _fetch_pykrx_index_metrics_subprocess(
    start_ymd: str,
    end_ymd: str,
    index_code: str,
    window_days: int,
    timeout_sec: int,
) -> Dict[str, Any]:
    code = r"""
import json
import sys

import pandas as pd
from pykrx import stock

start, end, index_code, window_days = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
try:
    try:
        df = stock.get_index_ohlcv_by_date(start, end, index_code, name_display=False)
    except TypeError:
        df = stock.get_index_ohlcv_by_date(start, end, index_code)
    if df is None or len(df) == 0:
        print(json.dumps({"ok": False, "status": "error_fetch_empty", "rows": 0}))
        raise SystemExit(0)
    close = None
    for key in ("Close", "close", "CLSPRC_IDX"):
        if key in df.columns:
            s = pd.to_numeric(df[key], errors="coerce")
            if int(s.notna().sum()) >= 2:
                close = s
                break
    if close is None:
        for c in df.columns:
            lc = str(c).strip().lower()
            if ("close" in lc) or ("clsprc" in lc):
                s = pd.to_numeric(df[c], errors="coerce")
                if int(s.notna().sum()) >= 2:
                    close = s
                    break
    if close is None:
        best = None
        best_cnt = -1
        for c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            cnt = int(s.notna().sum())
            if cnt > best_cnt:
                best = s
                best_cnt = cnt
        if best is not None and best_cnt >= 2:
            close = best
    if close is None:
        print(json.dumps({"ok": False, "status": "error_close_column_missing", "rows": int(len(df))}))
        raise SystemExit(0)
    tail = close.tail(window_days) if len(close) >= window_days else close
    dd = (tail / tail.cummax() - 1.0).min()
    day_ret = tail.pct_change().iloc[-1]
    print(json.dumps({
        "ok": True,
        "status": "ok",
        "rows": int(len(df)),
        "used_rows": int(len(tail)),
        "max_dd": float(dd),
        "day_ret": float(day_ret),
    }))
except Exception as e:
    print(json.dumps({"ok": False, "status": "error_fetch_calc:" + type(e).__name__, "error": str(e)[:200]}))
"""
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code, str(start_ymd), str(end_ymd), str(index_code), str(int(window_days))],
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_sec)),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "status": "error_fetch_timeout", "error": f"timeout_sec={int(timeout_sec)}"}
    except Exception as e:
        return {"ok": False, "status": f"error_fetch_subprocess:{type(e).__name__}", "error": str(e)[:200]}

    raw = (proc.stdout or "").strip().splitlines()
    if proc.returncode != 0 and not raw:
        return {
            "ok": False,
            "status": f"error_fetch_subprocess_rc:{proc.returncode}",
            "error": (proc.stderr or "")[:200],
        }
    if not raw:
        return {"ok": False, "status": "error_fetch_subprocess_empty", "error": (proc.stderr or "")[:200]}
    try:
        obj = json.loads(raw[-1])
        if isinstance(obj, dict):
            if proc.returncode != 0 and obj.get("ok") is not True:
                obj.setdefault("returncode", proc.returncode)
            return obj
    except Exception as e:
        return {"ok": False, "status": f"error_fetch_subprocess_json:{type(e).__name__}", "error": raw[-1][:200]}
    return {"ok": False, "status": "error_fetch_subprocess_invalid"}


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
            resolve_with_pykrx = str(os.getenv("P0_CRASH_RESOLVE_INDEX_WITH_PYKRX", "0") or "0").strip() == "1"
            if resolve_with_pykrx and _idx_market and _name_contains:
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

    # [2026-09-07] 실시간 낙폭(cur_dd) 히스테리시스 문턱. 설정에 없으면 유도값을 쓴다.
    try:
        crash_trigger_dd_pct = float(crash_cfg.get("cur_dd_trigger_pct", 0.25) or 0.25)
    except Exception:
        crash_trigger_dd_pct = 0.25
    try:
        crash_release_dd_pct = float(crash_cfg.get("cur_dd_release_pct", 0.10) or 0.10)
    except Exception:
        crash_release_dd_pct = 0.10
    if not (0.0 < crash_release_dd_pct < crash_trigger_dd_pct):
        # 해제선이 발동선보다 크거나 같으면 히스테리시스가 성립하지 않는다. 유도값으로 되돌린다.
        crash_trigger_dd_pct, crash_release_dd_pct = 0.25, 0.10

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

    def _crash_state_path():
        return Path(__file__).resolve().parent / "2_Logs" / "crash_guard_state.json"

    def _read_crash_state() -> bool:
        """직전 발동 상태. 없거나 못 읽으면 None(=히스테리시스 미적용)."""
        try:
            d = json.loads(_crash_state_path().read_text(encoding="utf-8-sig"))
            v = d.get("triggered")
            return bool(v) if isinstance(v, bool) else None
        except Exception:
            return None

    def _now_stamp() -> str:
        # 이 함수 스코프에 dt 지역 바인딩이 있어 모듈의 `import datetime as dt` 를 가린다.
        # (NameError: cannot access free variable). 여기서 직접 임포트한다.
        import datetime as _d
        return _d.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write_crash_state(fired: bool, cur: float, on: float, off: float) -> None:
        try:
            _crash_state_path().write_text(json.dumps({
                "triggered": bool(fired), "cur_dd": float(cur),
                "trigger_pct": float(on), "release_pct": float(off),
                "as_of_ymd": str(as_of_ymd or ""),
                "updated_at": _now_stamp(),
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _apply_trigger_from_metrics(use_fallback_limits: bool = False) -> None:
        dd_lim = float(fb_trig_dd if use_fallback_limits else trig_dd)
        day_lim = float(fb_trig_day if use_fallback_limits else trig_day)
        dd = out["metrics"].get("max_dd")
        day_ret = out["metrics"].get("day_ret")

        # [2026-09-07] 실시간 낙폭 + 히스테리시스.
        #   max_dd(창 고점~저점)는 회복을 반영하지 않아 해제가 달력으로 결정됐다.
        #   cur_dd(창 고점~현재가)로 바꾸고, 잦은 온/오프를 막으려 발동/해제선을 나눈다.
        #   문턱 유도: 11.6년 2,838일 실측에서 현행 발동빈도(연 5.9일)와 같은 쌍.
        #   0.25/0.10 = 연 5.9일, 발동 2회, 휩쏘 0회. 상세 PLANS 2026-09-07.
        #   cur_dd 가 없으면(구 프록시 경로) 아래 기존 max_dd 판정이 그대로 돈다.
        cur_dd = out["metrics"].get("cur_dd")
        if cur_dd is not None:
            on = float(crash_trigger_dd_pct)
            off = float(crash_release_dd_pct)
            cur = abs(float(cur_dd))
            prev = _read_crash_state()
            if prev is True:
                fired = cur > off          # 해제선 아래로 내려와야 풀린다
            else:
                fired = cur >= on          # 상태 미상이면 발동선 단독(보수적)
            out["metrics"]["cur_dd_trigger_pct"] = on
            out["metrics"]["cur_dd_release_pct"] = off
            out["metrics"]["prev_state"] = prev
            out["metrics"]["hysteresis"] = bool(prev is True)
            # [2026-09-08] 어느 문턱이 실제로 판정했는지 산출물에 적는다.
            #   limits 에는 trigger_max_dd_pct(0.12) / fallback_trigger_max_dd_pct(0.35) 가
            #   그대로 실려 있는데 이 경로에서는 **하나도 쓰이지 않는다**(0.25/0.10 이 판정한다).
            #   읽는 쪽이 limits 만 보고 "0.12 에서 발동한다"고 오독할 수 있고,
            #   이 저장소에서 설정값과 실제 발동값이 다르다는 발견이 이미 두 번 있었다
            #   (낙폭 하드블록 설정 45% vs 실제 36%). 판정은 그대로 두고 **무엇이 유효했는지**만 적는다.
            out["limits"]["effective_path"] = "cur_dd_hysteresis"
            out["limits"]["effective_trigger_pct"] = on
            out["limits"]["effective_release_pct"] = off
            if fired:
                out["triggered"] = True
                out["reasons"].append(
                    f"cur_dd({cur:.6f} >= {on:.6f})" if prev is not True
                    else f"cur_dd({cur:.6f} > release {off:.6f}, 발동 유지)")
            _write_crash_state(fired, cur, on, off)
            if day_ret is not None and float(day_ret) <= -day_lim:
                out["triggered"] = True
                out["reasons"].append(f"day_ret({float(day_ret):.6f} <= -{day_lim:.6f})")
            if out["triggered"]:
                out["metrics"]["status"] = "triggered"
            return

        # 구 프록시 경로. cur_dd 를 못 구했을 때만 여기까지 온다.
        # trigger_max_dd_pct / fallback_trigger_max_dd_pct 는 **이 경로 전용**이다.
        out["limits"]["effective_path"] = "max_dd_proxy_fallback"
        out["limits"]["effective_trigger_pct"] = float(dd_lim)
        if dd is not None and float(dd) <= -dd_lim:
            out["triggered"] = True
            out["reasons"].append(f"max_dd({float(dd):.6f} <= -{dd_lim:.6f})")
        if day_ret is not None and float(day_ret) <= -day_lim:
            out["triggered"] = True
            out["reasons"].append(f"day_ret({float(day_ret):.6f} <= -{day_lim:.6f})")
        if out["triggered"]:
            out["metrics"]["status"] = "triggered"

    def _apply_local_proxy_fallback(from_status: str, fb: Optional[dict] = None,
                                    role: str = "fallback") -> bool:
        # [2026-09-08] fb 를 밖에서 넘길 수 있게 했다. 혼합지수를 **주 소스**로 올리면서
        #   같은 계산을 두 번 하지 않기 위해서다. role 은 산출물 표기용이고 판정에는 안 쓴다.
        if fb is None:
            fb = _crash_local_proxy_metrics(str(as_of_ymd or ""), int(window_days))
        out["metrics"]["fallback_from"] = from_status
        out["metrics"]["fallback_status"] = str(fb.get("status") or "")
        out["metrics"]["verdict_role"] = str(role)
        if not bool(fb.get("ok", False)):
            if fb.get("error"):
                out["metrics"]["fallback_error"] = str(fb.get("error"))[:200]
            return False

        out["source"] = str(fb.get("source") or "local_prices_proxy")
        out["metrics"]["rows"] = int(fb.get("rows") or 0)
        out["metrics"]["used_rows"] = int(fb.get("used_rows") or 0)
        out["metrics"]["max_dd"] = float(fb.get("max_dd"))
        out["metrics"]["day_ret"] = float(fb.get("day_ret"))
        for _k in ("cur_dd", "window_high", "window_low", "rebound_from_low"):
            if fb.get(_k) is not None:
                out["metrics"][_k] = float(fb.get(_k))
        _src = str(out.get("source") or "local_prices_proxy").strip().lower()
        # [2026-09-08] 라벨이 역할을 따라간다. 혼합지수가 주 소스가 된 뒤에도
        #   "ok_fallback_index_blend" 로 찍히면 방금 없앤 오독을 그대로 다시 만든다.
        out["metrics"]["status"] = ("ok_primary_%s" % _src) if role == "primary" else ("ok_fallback_%s" % _src)
        _apply_trigger_from_metrics(use_fallback_limits=True)

        # [2026-09-04] 이 가드가 실제로 **어느 날짜의 데이터**를 봤는지 남긴다.
        #   as_of 를 고쳐도, 지수 수집(VIBE_Index_Daily_Fetch 16:05)이 멈추면
        #   가드는 낡은 창을 조용히 평가하면서 최신인 척한다 - 이 저장소가 반복해 당한 형태다.
        #   그래서 (1) 실제 데이터 최신일과 지연일수를 기록하고
        #        (2) 지연이 문턱을 넘으면 **fail-closed**(막는 쪽)로 간다. 열지 않는다.
        _data_max = str(fb.get("blend_date_max") or "").strip()
        if _data_max:
            out["metrics"]["data_date_max"] = _data_max
            try:
                import datetime as _dt
                _a = _dt.datetime.strptime(str(as_of_ymd), "%Y%m%d").date()
                _d = _dt.datetime.strptime(_data_max, "%Y%m%d").date()
                _lag = (_a - _d).days
                out["metrics"]["data_lag_days"] = int(_lag)
                _lag_lim = int((crash_cfg or {}).get("max_data_lag_days", 7) or 7)
                out["limits"]["max_data_lag_days"] = _lag_lim
                if _lag > _lag_lim:
                    out["triggered"] = True
                    out["reasons"].append(
                        "stale_index_data(lag=%dd > %dd, data_date_max=%s)" % (_lag, _lag_lim, _data_max)
                    )
                    out["metrics"]["status"] = "triggered_stale_data"
            except Exception as _le:  # noqa: BLE001
                # 계산 실패는 판정을 열지 않는다. 관측 값만 비운다.
                out["metrics"]["data_lag_error"] = "%s: %s" % (type(_le).__name__, str(_le)[:120])
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
        import datetime as dt

        asof = dt.datetime.strptime(str(as_of_ymd), "%Y%m%d").date()
        # ??? ??? ??? ?? ??? ??
        start = (asof - dt.timedelta(days=max(120, int(window_days) * 3))).strftime("%Y%m%d")
        end = asof.strftime("%Y%m%d")
        try:
            pykrx_timeout_sec = int(crash_cfg.get("pykrx_timeout_sec", os.getenv("P0_CRASH_PYKRX_TIMEOUT_SEC", "20")) or 20)
        except Exception:
            pykrx_timeout_sec = 20
        pykrx_timeout_sec = max(1, min(int(pykrx_timeout_sec), 60))

        # [2026-09-08 사용자 승인] **구성일치 혼합지수를 주 소스로 올린다. pykrx 는 폴백으로 내린다.**
        #   왜. (1) 이미 그것이 판정하고 있었다 - 09-07 이후 전부 ok_fallback_index_blend.
        #       (2) 2_Logs/index_daily_history.csv 는 매일 16:05 수집하는 진짜 지수이고
        #           신선도 fail-closed 가 이미 이 경로에 붙어 있다(2026-09-04).
        #       (3) 스타일 일치 - 실제 체결 구성(KOSPI 46.4 / KOSDAQ 53.6)으로 섞는다.
        #           pykrx 경로는 단일 코드이고 config 의 index_code=1001 은 실제로 KOSDAQ 인데
        #           index_market 은 KOSPI 라 라벨이 어긋나 있다 (PLANS (166) 실측).
        #       (4) **전략 패널과 독립이다.** 킬스위치가 전략과 같은 데이터에 물리면 둘이 같이 죽는다.
        #       (5) pykrx 는 외부 라이브러리 파싱 의존이고 지금 깨져 있다
        #           (보관 p0 38개 전부 error_fetch_empty / KeyError '종가' 재현).
        #   주의. 여기서 받아들이는 것은 **index_blend 하나뿐**이다. 같은 함수가 실패 시
        #   krx_clean/paper 프록시로도 내려가는데, 그것들은 "4월 시장으로 오늘을 판정" 사고를
        #   낸 경로다(PLANS (163)~(166)). 그 경로는 pykrx 다음 순서로 남긴다.
        _blend = _crash_local_proxy_metrics(str(as_of_ymd or ""), int(window_days))
        if bool(_blend.get("ok", False)) and str(_blend.get("source") or "") == "index_blend":
            out["metrics"]["primary_source"] = "index_blend"
            out["metrics"]["primary_source_ok"] = True
            out["metrics"]["primary_source_status"] = str(_blend.get("status") or "ok")
            if _apply_local_proxy_fallback("primary_index_blend", fb=_blend, role="primary"):
                out["metrics"]["verdict_source"] = "primary"
                return out
            # 신선도 fail-closed 등으로 되돌아온 경우다. 아래 pykrx 로 내려간다.
            out["metrics"]["primary_source_ok"] = False

        fetched = _fetch_pykrx_index_metrics_subprocess(start, end, index_code, int(window_days), pykrx_timeout_sec)
        status = str(fetched.get("status") or "error_fetch_unknown")
        if not bool(fetched.get("ok", False)):
            out["reasons"].append(status)
            out["metrics"]["status"] = status
            out["metrics"]["pykrx_timeout_sec"] = int(pykrx_timeout_sec)
            if fetched.get("rows") is not None:
                out["metrics"]["rows"] = int(fetched.get("rows") or 0)
            if fetched.get("error"):
                out["metrics"]["error"] = str(fetched.get("error"))[:200]
            # [2026-09-08] 주 소스(pykrx)가 죽었다는 사실을 **판정과 분리해서** 남긴다.
            #   실측: 보관된 p0 산출물 38개(2026-08-10 이후) 전부에 error_fetch_empty 가 있다.
            #   재현: get_index_ohlcv_by_date(...,'1001'/'2001') -> KeyError '종가' (라이브러리 파싱 파손).
            #   즉 매매 정지 스위치가 30일 넘게 한 번도 주 소스로 판정한 적이 없다.
            #   안 드러난 이유는 폴백이 조용히 성공했고 실패 사유가 reasons 안에서
            #   발동 사유와 섞였기 때문이다. 읽는 쪽이 구분할 수 있게 전용 필드로 올린다.
            #   판정은 바꾸지 않는다 - 무엇이 판정했는지만 적는다.
            #   [2026-09-08 소스 재배치 이후] 여기까지 왔다는 것은 혼합지수가 이미 실패했다는 뜻이다.
            #   pykrx 는 이제 1차 폴백이고, 그것도 실패했으므로 아래에서 나머지 프록시로 내려간다.
            out["metrics"]["fallback1_source"] = "pykrx"
            out["metrics"]["fallback1_ok"] = False
            out["metrics"]["fallback1_status"] = status
            if _apply_local_proxy_fallback(status, role="fallback2_local_proxy"):
                out["metrics"]["verdict_source"] = "fallback2_local_proxy"
                return out
            # [2026-09-08 사용자 승인] **세 소스가 다 실패하면 fail-closed 로 간다.**
            #   그 전까지는 triggered=False 로 조용히 통과했다.
            #   **판정을 못 한 것과 "폭락 아님" 은 다르다.** 시장 상태를 모르는 채로 사는 것이
            #   이 가드가 막으려던 바로 그 상황이다.
            #   새 정책이 아니다 - 같은 함수의 신선도 분기가 이미 같은 선택을 해 뒀고
            #   ("지연이 문턱을 넘으면 fail-closed(막는 쪽)로 간다. 열지 않는다", 2026-09-04)
            #   빠진 자리에 그 선례를 채우는 것이다.
            #   mode 는 설정값(REDUCE 등)을 그대로 따른다. 여기서 강도를 새로 정하지 않는다.
            out["metrics"]["verdict_source"] = "none"
            out["metrics"]["status"] = "no_source_verdict"
            out["triggered"] = True
            out["reasons"].append("no_source_verdict(fail_closed)")
            return out

        out["metrics"]["rows"] = int(fetched.get("rows") or 0)
        out["metrics"]["used_rows"] = int(fetched.get("used_rows") or 0)
        out["metrics"]["max_dd"] = float(fetched.get("max_dd"))
        out["metrics"]["day_ret"] = float(fetched.get("day_ret"))
        out["metrics"]["status"] = "ok"
        # [2026-09-08] 소스 재배치 이후 이 경로는 1차 폴백이다. 성공했을 때도 누가 판정했는지 남긴다
        #   (남기지 않으면 verdict_source 가 None 으로 비어 다시 "누가 판정했나"를 못 읽는다).
        out["metrics"]["fallback1_source"] = "pykrx"
        out["metrics"]["fallback1_ok"] = True
        out["metrics"]["verdict_source"] = "fallback1_pykrx"
        out["metrics"]["pykrx_timeout_sec"] = int(pykrx_timeout_sec)
        _apply_trigger_from_metrics()
        return out

    except Exception as e:
        out["reasons"].append("fetch_calc_fail")
        out["metrics"]["status"] = f"error_fetch_calc:{type(e).__name__}"
        out["metrics"]["error"] = str(e)[:200]
        if _apply_local_proxy_fallback(str(out["metrics"]["status"]), role="fallback2_local_proxy"):
            out["metrics"]["verdict_source"] = "fallback2_local_proxy"
            return out
        # [2026-09-08 사용자 승인] 위 정상 경로와 같은 이유로 여기도 fail-closed 다.
        #   예외로 빠졌는데 폴백까지 실패하면 판정한 소스가 하나도 없다.
        #   같은 결함을 두 자리에 남기지 않는다 ([[feedback_same_defect_is_copied_in_several_files]]).
        out["metrics"]["verdict_source"] = "none"
        out["triggered"] = True
        out["reasons"].append("no_source_verdict(fail_closed)")
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


def _latest_buy_ymd_from_fills(fills_csv: Path, flags: list[str]) -> Optional[str]:
    if not fills_csv.exists():
        flags.append(f"fills_csv_missing:{fills_csv}")
        return None
    try:
        df = pd.read_csv(fills_csv, dtype=str, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(fills_csv, dtype=str, encoding="utf-8")
        except Exception as e:
            flags.append(f"fills_csv_parse_fail:{type(e).__name__}")
            return None
    if df.empty or "datetime" not in df.columns:
        flags.append("fills_csv_no_datetime")
        return None
    side = df.get("side")
    if side is not None:
        buy = df[side.astype(str).str.upper().eq("BUY")].copy()
    else:
        buy = df.iloc[0:0].copy()
    src = buy if not buy.empty else df
    ymd = (
        src["datetime"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str.slice(0, 8)
    )
    ymd = ymd[ymd.str.fullmatch(r"\d{8}", na=False)]
    if ymd.empty:
        flags.append("fills_csv_no_valid_ymd")
        return None
    return str(ymd.max())


def _intraday_prices_stats(logs_dir: Path, flags: list[str]) -> dict:
    path = logs_dir / "intraday_prices_latest.csv"
    out = {"path": str(path), "exists": path.exists(), "date_max": None, "codes": 0, "rows": 0}
    if not path.exists():
        return out
    try:
        df = pd.read_csv(path, dtype=str)
        out["rows"] = int(len(df))
        if "date" in df.columns:
            ymd = (
                df["date"]
                .astype(str)
                .str.replace(r"[^0-9]", "", regex=True)
                .str.slice(0, 8)
            )
            ymd = ymd[ymd.str.fullmatch(r"\d{8}", na=False)]
            if not ymd.empty:
                out["date_max"] = str(ymd.max())
        if "code" in df.columns:
            out["codes"] = int(df["code"].astype(str).map(norm_code).nunique(dropna=True))
    except Exception as e:
        flags.append(f"intraday_prices_read_fail:{type(e).__name__}")
    return out


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / '2_Logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    _p0_checkpoint(logs_dir, "main_start")

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
    _p0_checkpoint(logs_dir, "meta_loaded", {"latest_date": md, "market_regime": mr})

    # prices
    prices_parquet = base_dir / 'paper' / 'prices' / 'ohlcv_paper.parquet'
    prices = _read_prices_stats(prices_parquet, flags)
    date_max: Optional[str] = prices.get('date_max')
    intraday_prices = _intraday_prices_stats(logs_dir, flags)
    intraday_date_max = _norm_ymd_oper(intraday_prices.get("date_max"))
    fills_csv = base_dir / 'paper' / 'fills.csv'
    d_rule_ymd = _latest_buy_ymd_from_fills(fills_csv, flags)
    _p0_checkpoint(
        logs_dir,
        "prices_and_fills_loaded",
        {"prices_date_max": prices.get("date_max"), "d_rule_ymd": d_rule_ymd},
    )

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
    kill_trades_csv = (base_dir / 'paper' / 'trades_calc.csv') if (base_dir / 'paper' / 'trades_calc.csv').exists() else trades_csv
    open_pos_trades = _compute_open_positions_from_trades(trades_csv, flags)
    open_pos_state = None
    open_positions_list: list[dict] = []
    horizon_counts: dict[str, int] = {}
    horizon_hold_stats: dict[str, Optional[int]] = {"min": None, "max": None}
    try:
        sp = base_dir / 'paper' / 'paper_state.json'
        if sp.exists():
            txt = sp.read_text(encoding='utf-8')
            if txt.strip():
                sj = json.loads(txt)
                op = sj.get('open_positions', None)
                if isinstance(op, list):
                    open_pos_state = int(len(op))
                    open_positions_list = [x for x in op if isinstance(x, dict)]
                    hold_vals: list[int] = []
                    for _pos in open_positions_list:
                        _hz = str(_pos.get("horizon_label") or "").strip().upper() or "UNKNOWN"
                        horizon_counts[_hz] = int(horizon_counts.get(_hz, 0)) + 1
                        _hold = _pos.get("horizon_max_hold_days")
                        try:
                            _hold_i = int(_hold)
                        except Exception:
                            _hold_i = 0
                        if _hold_i > 0:
                            hold_vals.append(_hold_i)
                    if hold_vals:
                        horizon_hold_stats = {"min": int(min(hold_vals)), "max": int(max(hold_vals))}
    except Exception as e:
        flags.append(f'paper_state_parse_fail:{type(e).__name__}')
    open_positions_count = int(open_pos_state if (open_pos_state is not None) else open_pos_trades)
    open_positions = int(open_positions_count)

    dup_trades = _dup_trades_count(trades_csv, flags)
    if dup_trades:
        flags.append(f"dup_trades:{dup_trades}")
    _p0_checkpoint(
        logs_dir,
        "paper_state_loaded",
        {"open_positions": open_positions_count, "dup_trades": int(dup_trades or 0)},
    )

    krx_clean_date_max = _krx_clean_date_max(base_dir, flags)
    prev_weekday = HolidayManager().previous_trading_day(dt.date.today().strftime("%Y%m%d"))
    # krx_clean_ncode: krx_clean_date_max ?좎쭨???대떦?섎뒗 parquet???좊땲踰꾩뒪(code) ??理쒕?媛?

    krx_clean_ncode = _krx_clean_ncode(base_dir, krx_clean_date_max) if krx_clean_date_max else None
    _p0_checkpoint(
        logs_dir,
        "krx_clean_basic_loaded",
        {"krx_clean_date_max": krx_clean_date_max, "krx_clean_ncode": krx_clean_ncode},
    )
    krx_soft_skip_map = _load_krx_soft_skip_meta(logs_dir)
    krx_effective_ncode = krx_clean_ncode
    krx_effective_date_max = krx_clean_date_max
    krx_effective_reason = None
    try:
        if krx_clean_date_max:
            best_n = 0
            for _p in _krx_clean_files(base_dir):
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
    krx_effective_ncode = krx_clean_ncode
    krx_effective_date_max = krx_clean_date_max
    _p0_checkpoint(
        logs_dir,
        "krx_clean_refine_done",
        {"krx_clean_date_max": krx_clean_date_max, "krx_clean_ncode": krx_clean_ncode},
    )
    try:
        if isinstance(krx_soft_skip_map, dict) and krx_soft_skip_map:
            soft_latest = max(krx_soft_skip_map.keys())
            soft_row = krx_soft_skip_map.get(soft_latest) or {}
            soft_reason = str(soft_row.get("reason") or "")
            soft_effective_ncode = soft_row.get("effective_ncode")
            if (
                soft_latest
                and soft_reason
                in {
                    "skip_write_due_tolerated_zero_ohlc_deficit",
                    "skip_write_due_tolerated_zero_ohlc_and_reference_gap",
                }
                and soft_effective_ncode is not None
            ):
                krx_effective_date_max = soft_latest
                krx_effective_ncode = int(soft_effective_ncode)
                krx_effective_reason = soft_reason
    except Exception as e:
        flags.append(f"krx_soft_skip_meta_fail:{type(e).__name__}")
    # risk_off should block only minimum critical stale-data cases.
    # NOTE: candidate metadata lag is logged separately unless it crosses the hard gate.
    risk_off = {"enabled": True, "reasons": ["INIT_FAIL_CLOSED"]}
    kill_switch = {"triggered": False, "reasons": [], "source": ""}  # default
    crash_risk_off = {"triggered": False, "reasons": []}  # default

    try:
        _p0_checkpoint(logs_dir, "risk_eval_start")
        # 1) Block when paper price data is stale against the required operating date.
        if md and date_max:
            md_yyyymmdd = re.sub(r"[^0-9]", "", str(md))  # '2025-12-24' -> '20251224'
            if md_yyyymmdd and (str(date_max) < md_yyyymmdd):
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"prices_date_max({date_max}) < meta_latest_date({md_yyyymmdd})")
        if d_rule_ymd:
            if not date_max:
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"prices_date_max_missing < D({d_rule_ymd})")
            elif str(date_max) < str(d_rule_ymd):
                if intraday_date_max and intraday_date_max >= str(d_rule_ymd):
                    flags.append(
                        f"SOFT_GATE prices_date_max({date_max}) < D({d_rule_ymd}) "
                        f"covered_by_intraday_prices_date_max({intraday_date_max})"
                    )
                else:
                    risk_off["enabled"] = True
                    risk_off["reasons"].append(f"prices_date_max({date_max}) < D({d_rule_ymd})")

        # 1.1) Candidate metadata lag beyond the hard threshold blocks new entries.
        md_yyyymmdd2 = re.sub(r"[^0-9]", "", str(md)) if md else ""
        md_yyyymmdd2 = md_yyyymmdd2[:8]
        # [GATE_RELAX] cand_latest_date: allow lag-N(기본2) => SOFT flag, lag-(N+1)+ => risk_off
        lag_days = _resolve_hard_lag_days(2)
        prev_weekday_lagN = _prev_weekday_lag(prev_weekday, lag_days) if prev_weekday else None
        if prev_weekday and (not prev_weekday_lagN):
            flags.append("prev_weekday_lagN_calc_fail")
        if md_yyyymmdd2 and prev_weekday and prev_weekday_lagN:
            if md_yyyymmdd2 < prev_weekday_lagN:
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"cand_latest_date({md_yyyymmdd2}) < prev_weekday_lag{lag_days}({prev_weekday_lagN})")
            elif md_yyyymmdd2 < prev_weekday:
                flags.append(f"SOFT_GATE cand_latest_date({md_yyyymmdd2}) < prev_weekday({prev_weekday})")
        # 1.2) krx clean universe guard: use shared KRX_MIN_UNI threshold.
        min_uni_raw = str(os.getenv("KRX_MIN_UNI", "1800") or "1800").strip()
        try:
            min_uni = max(1, int(float(min_uni_raw)))
        except Exception:
            min_uni = 1800
        krx_guard_ncode = krx_effective_ncode if (krx_effective_ncode is not None) else krx_clean_ncode
        if (krx_guard_ncode is not None) and (int(krx_guard_ncode) < min_uni):
            risk_off["enabled"] = True
            risk_off["reasons"].append(f"krx_clean_universe_degraded(ncode={int(krx_guard_ncode)} < MIN_UNI={min_uni})")

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
        _p0_checkpoint(
            logs_dir,
            "kill_switch_config_loaded",
            {"window_days": int(window_days), "mode": ks_mode},
        )

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

            last_good = None
            last_path = (logs_dir / "paper_pnl_summary_last.json")
            if last_path.exists():
                try:
                    _p0_checkpoint(logs_dir, "pnl_summary_last_read_start", {"path": str(last_path)})
                    j = json.load(open(last_path, "r", encoding="utf-8"))
                    eq0 = j.get("equity", None) if isinstance(j, dict) else None
                    if isinstance(eq0, dict) and all(k in eq0 for k in ("max_drawdown_pct","last_day_ret","last_exit_date")):
                        last_good = last_path
                        best_mtime = last_good.stat().st_mtime
                        _p0_checkpoint(logs_dir, "pnl_summary_last_valid", {"path": str(last_good)})
                except Exception:
                    last_good = None

            if last_good is None:
                _p0_checkpoint(logs_dir, "pnl_summary_glob_scan_start", {"pattern": "paper_pnl_summary_*.json", "limit": 80})
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
                _p0_checkpoint(logs_dir, "pnl_summary_glob_scan_done", {"path": str(best) if best else ""})

            if last_good is not None:
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
        _p0_checkpoint(logs_dir, "pnl_summary_selected", {"path": str(pnl_sum_path) if pnl_sum_path else ""})

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

            _p0_checkpoint(logs_dir, "rolling_kill_switch_read_start", {"path": str(kill_trades_csv)})
            _df = _pd.read_csv(kill_trades_csv, encoding="utf-8-sig")
            _p0_checkpoint(logs_dir, "rolling_kill_switch_read_done", {"rows": int(len(_df))})
            _blocked_filter_meta = {"enabled": False, "reason": "not_applied"}
            try:
                from paper_pnl_report import _ensure_exit_date as _pnl_ensure_exit_date
                from paper_pnl_report import _filter_blocked_entry_trades as _pnl_filter_blocked_entry_trades

                _p0_checkpoint(logs_dir, "rolling_kill_switch_filter_start", {"rows": int(len(_df))})
                _df = _pnl_ensure_exit_date(_df)
                _df, _blocked_filter_meta = _pnl_filter_blocked_entry_trades(_df, base_dir / "paper")
                kill_switch["blocked_entry_filter"] = _blocked_filter_meta
                _p0_checkpoint(logs_dir, "rolling_kill_switch_filter_done", {"rows": int(len(_df))})
            except Exception as _filter_e:
                _blocked_filter_meta = {
                    "enabled": False,
                    "reason": f"filter_error:{type(_filter_e).__name__}:{_filter_e}",
                }
                kill_switch["blocked_entry_filter"] = _blocked_filter_meta

            _ret_col = None
            if ("exit_date" not in _df.columns) and ("exit_ts" in _df.columns):
                _df["exit_date"] = _df["exit_ts"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)

            for _c in ["pnl_pct", "net_ret", "ret_pct", "ret", "pnl", "profit_pct"]:
                if _c in _df.columns:
                    _ret_col = _c
                    break

            # PATCH: record actual inputs used for rolling calc attempt
            kill_switch["input_trades_path"] = str(kill_trades_csv)
            kill_switch["input_ret_col"] = _ret_col
            try:
                if kill_trades_csv.exists():
                    _mt = kill_trades_csv.stat().st_mtime
                    kill_switch["input_trades_mtime"] = dt.datetime.fromtimestamp(_mt).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                kill_switch["input_trades_mtime"] = None

            if _ret_col and ("exit_date" in _df.columns):
                _df[_ret_col] = _df[_ret_col].apply(lambda x: float(str(x).strip()) if str(x).strip() else _math.nan)
                _df["exit_date"] = _df["exit_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
                _d = _df[(_df["exit_date"].str.len() == 8) & (~_df[_ret_col].isna())]
                _oper_start = _resolve_oper_start_ymd()
                if _oper_start:
                    _d = _d[_d["exit_date"] >= _oper_start]
                if len(_d) > 0:
                    _p0_checkpoint(logs_dir, "rolling_kill_switch_calc_start", {"rows": int(len(_d)), "ret_col": str(_ret_col)})
                    # Align kill-switch day return aggregation with paper_pnl_report
                    # so split exits do not get overweighted by row count alone.
                    if "qty" in _d.columns:
                        _d["qty"] = _pd.to_numeric(_d["qty"], errors="coerce")
                    if "entry_price" in _d.columns:
                        _d["entry_price"] = _pd.to_numeric(_d["entry_price"], errors="coerce")

                    def _weighted_day_ret(_grp):
                        _vals = _pd.to_numeric(_grp[_ret_col], errors="coerce").fillna(0.0)
                        if {"qty", "entry_price"}.issubset(_grp.columns):
                            _qty = _pd.to_numeric(_grp["qty"], errors="coerce").fillna(0.0)
                            _ep = _pd.to_numeric(_grp["entry_price"], errors="coerce").fillna(0.0)
                            _w = (_qty * _ep).astype(float)
                            _wsum = float(_w.sum())
                            if _wsum > 0.0:
                                return float((_vals * _w).sum() / _wsum)
                        return float(_vals.mean()) if len(_vals) else 0.0

                    # rolling (SSOT for kill_switch decision): portfolio-style return per exit_date
                    # - weighted mean: USED/SSOT
                    # - sum/product: debug references only
                    _grouped = _d.groupby("exit_date", sort=True)
                    _g_all_sum = _grouped[_ret_col].sum().sort_index()
                    _g_all_mean = _pd.Series(
                        {str(_k): _weighted_day_ret(_grp) for _k, _grp in _grouped},
                        dtype="float64",
                    ).sort_index()
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

                    # USED method (SSOT): MEAN by exit_date then cumprod
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
                        "calc_method": "weighted_mean_by_exit_date_then_cumprod",
                    }
                    _p0_checkpoint(logs_dir, "rolling_kill_switch_calc_done", {"used_days": int(len(_g_mean))})

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
                _p0_checkpoint(logs_dir, "dd_source_provenance_start", {"source": str(kill_switch.get("source") or "")})

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
                if _exit_col is None and "exit_ts" in cols:
                    _exit_col = "exit_ts"
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

                _usecols = [_exit_col, _ret_col]
                if "qty" in cols:
                    _usecols.append("qty")
                if "entry_price" in cols:
                    _usecols.append("entry_price")

                # Read full rows so the blocked-entry filter can use note/order lineage,
                # then project back to the minimal columns needed for the DD curve.
                df, _enc_used = _read_csv_min(_trades_path, usecols=cols)
                _blocked_filter_meta2 = {"enabled": False, "reason": "not_applied"}
                try:
                    from paper_pnl_report import _ensure_exit_date as _pnl_ensure_exit_date
                    from paper_pnl_report import _filter_blocked_entry_trades as _pnl_filter_blocked_entry_trades

                    df = _pnl_ensure_exit_date(df)
                    df, _blocked_filter_meta2 = _pnl_filter_blocked_entry_trades(df, base_dir / "paper")
                except Exception as _filter_e:
                    _blocked_filter_meta2 = {
                        "enabled": False,
                        "reason": f"filter_error:{type(_filter_e).__name__}:{_filter_e}",
                    }
                    _p0_checkpoint(
                        logs_dir,
                        "rolling_kill_switch_calc_done",
                        {
                            "rows": int(len(_df)),
                            "used_days": int(roll.get("used_days") or 0),
                            "last_exit_date": str(roll.get("last_exit_date") or ""),
                        },
                    )
                kill_switch["metrics"]["dd_blocked_entry_filter"] = _blocked_filter_meta2

                ex = df[_exit_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
                rt = pd.to_numeric(df[_ret_col], errors="coerce")
                d = pd.DataFrame({"exit_date": ex, "ret": rt})
                if "qty" in df.columns:
                    d["qty"] = pd.to_numeric(df["qty"], errors="coerce")
                if "entry_price" in df.columns:
                    d["entry_price"] = pd.to_numeric(df["entry_price"], errors="coerce")
                d = d.dropna(subset=["exit_date", "ret"])
                d = d[d["exit_date"].astype(str).str.len() == 8]
                _oper_start = _resolve_oper_start_ymd()
                if _oper_start:
                    d = d[d["exit_date"] >= _oper_start]
                if len(d) <= 0:
                    raise RuntimeError("no_valid_rows_for_dd_curve")

                def _weighted_dd_ret(_grp):
                    _vals = pd.to_numeric(_grp["ret"], errors="coerce").fillna(0.0)
                    if {"qty", "entry_price"}.issubset(_grp.columns):
                        _qty = pd.to_numeric(_grp["qty"], errors="coerce").fillna(0.0)
                        _ep = pd.to_numeric(_grp["entry_price"], errors="coerce").fillna(0.0)
                        _w = (_qty * _ep).astype(float)
                        _wsum = float(_w.sum())
                        if _wsum > 0.0:
                            return float((_vals * _w).sum() / _wsum)
                    return float(_vals.mean()) if len(_vals) else 0.0

                g_sum_all = d.groupby("exit_date")["ret"].sum().sort_index()
                g_mean_all = pd.Series(
                    {str(_k): _weighted_dd_ret(_grp) for _k, _grp in d.groupby("exit_date", sort=True)},
                    dtype="float64",
                ).sort_index()

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
                        "oper_start_ymd": _oper_start,
                        "blocked_entry_filter": _blocked_filter_meta2,
                    },
                    "limits": {
                        "window_days": int(window_days),
                        "max_drawdown_pct": float(abs(ks_dd_lim)),
                        "max_daily_loss_pct": float(abs(ks_day_lim)),
                        "metric": "rolling_exit_dates",
                    },
                    "calc": {
                        "mode": str(kill_switch["metrics"].get("mode") or ""),
                        "calc_method": "weighted_mean_by_exit_date_then_cumprod",
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
                _p0_checkpoint(
                    logs_dir,
                    "dd_source_provenance_done",
                    {"curve_rows": int(len(g_mean)), "source_path": str(src_p)},
                )

            except Exception as _e:
                kill_switch["metrics"]["dd_source_error"] = f"{type(_e).__name__}: {_e}"
                _p0_checkpoint(logs_dir, "dd_source_provenance_error", {"error": str(kill_switch["metrics"].get("dd_source_error") or "")})


        if kill_switch.get("enabled"):
            if max_dd is None: max_dd = 0.0
            if last_day_ret is None: last_day_ret = 0.0
            last_exit_date_norm = str(last_exit_date or "").strip()
            # [2026-09-04] 두 가지를 같이 고쳤다. 상세 PLANS (210).
            #
            # (1) 기준일에서 d_rule_ymd(마지막 BUY 체결일)를 뒤로 뺐다.
            #     계좌 낙폭은 "지금까지"를 재야 하는데 "마지막으로 산 날까지"를 재고 있었다.
            #     trades.csv 경로가 `_exit_ymd <= as_of` 로 자르므로 그 뒤 청산이 통째로 빠진다.
            #     실측 2026-09-04: 20260825 청산 1건(-769원)이 빠져 있었다.
            #
            # (2) 그런데 (1)만 고치면 조용히 **원장이 갈아끼워진다**.
            #     대시보드 경로는 `dash_asof == as_of_ymd` 일 때만 열리는데,
            #     as_of 가 20260824 라 계속 닫혀 있었고 그 덕에 아무도 두 원장을 대조한 적이 없다.
            #     실측: 실현손익 A(paper/trades.csv) -13,181,597 vs B(대시보드/paper_fills_ledger)
            #           -2,334,944. 차이 1,084만원(5.7배). 비용 모델 차이가 아니다 -
            #           **19일은 부호까지 반대**이고 덮는 날짜도 다르다(A 103일 / B 102일 / 합집합 121일).
            #     낙폭으로는 -13.24% vs -2.33%. (1)만 고치면 자본 잠금이 5.7배 둔해진다.
            #     어느 원장이 맞는지 모르는 채로 안전장치를 푸는 셈이라 그렇게 하지 않는다.
            #
            #     -> 두 경로를 **둘 다** 구해 기록하고, **보수적인 쪽(더 깊은 낙폭)** 을 쓴다.
            #        원장 정합성이 밝혀지기 전까지의 조치다. 괴리는 account_basis_divergence 로 남긴다.
            current_ymd_norm = str(date_max or d_rule_ymd or "").strip()
            _acct_primary = _resolve_account_kill_switch_metrics(base_dir, cfg, current_ymd_norm)
            _acct_trades = _resolve_account_kill_switch_metrics(
                base_dir, cfg, current_ymd_norm, skip_dashboard=True
            )
            # [2026-09-11] 브로커 실측이 신선하면 **그것을 권위로 삼는다.**
            #   위 주석의 "어느 원장이 맞는지 밝혀지기 전까지" 가 끝나는 지점이다.
            #   브로커는 원장이 아니라 외부 사실이므로 보수적 선택의 대상이 아니다.
            #   **방향 주의**: 이 전환은 낙폭을 얕게 만든다(실측 -13.24% -> -0.75%).
            #   두 내부 경로는 account_basis_divergence 에 남겨 대조 가능하게 둔다.
            _acct_broker = _resolve_broker_account_metrics(base_dir)
            if _acct_broker:
                account_metrics = _acct_broker
                account_metrics["account_basis_divergence"] = {
                    "rule": "broker_is_external_truth",
                    "primary_max_drawdown_pct": _acct_primary.get("max_drawdown_pct"),
                    "primary_equity_est": _acct_primary.get("equity_est"),
                    "trades_max_drawdown_pct": _acct_trades.get("max_drawdown_pct"),
                    "trades_equity_est": _acct_trades.get("equity_est"),
                    "broker_equity_est": _acct_broker.get("equity_est"),
                    "chosen": "broker",
                }
            else:
                account_metrics = _pick_conservative_account_metrics(_acct_primary, _acct_trades)
            kill_switch["metrics"]["strategy_basis"] = {
                "max_drawdown_pct": float(max_dd),
                "last_day_ret": float(last_day_ret),
                "last_exit_date": last_exit_date_norm,
                "source": str(kill_switch.get("source") or ""),
                "role": "advisory_reference",
            }
            kill_switch["metrics"]["account_basis"] = account_metrics
            hard_max_dd = max_dd
            hard_last_day_ret = last_day_ret
            hard_last_exit_date = last_exit_date_norm
            hard_basis = "strategy_return_curve_fallback"
            hard_daily_loss_basis = "strategy_last_day_return"
            if isinstance(account_metrics, dict) and account_metrics.get("status") == "PASS":
                hard_max_dd = float(account_metrics.get("max_drawdown_pct") or 0.0)
                hard_last_day_ret = float(
                    account_metrics.get("daily_loss_pct")
                    if account_metrics.get("daily_loss_pct") is not None
                    else account_metrics.get("last_day_ret")
                    or 0.0
                )
                hard_last_exit_date = str(account_metrics.get("last_exit_date") or current_ymd_norm)
                hard_basis = str(account_metrics.get("basis") or "account_equity")
                hard_daily_loss_basis = str(account_metrics.get("daily_loss_basis") or "account_last_day_return")
            kill_switch["metrics"]["hard_trigger_basis"] = hard_basis
            kill_switch["metrics"]["hard_trigger_metrics"] = {
                "max_drawdown_pct": float(hard_max_dd),
                "last_day_ret": float(hard_last_day_ret),
                "daily_loss_pct": float(hard_last_day_ret),
                "daily_loss_basis": hard_daily_loss_basis,
                "last_exit_date": hard_last_exit_date,
            }
            daily_loss_active = _is_daily_loss_active(last_exit_date_norm, current_ymd_norm, last_day_ret)
            hard_daily_loss_active = _is_daily_loss_active(hard_last_exit_date, current_ymd_norm, hard_last_day_ret)
            kill_switch["metrics"]["daily_loss_active"] = daily_loss_active
            kill_switch["metrics"]["daily_loss_active_reason"] = (
                "fresh_last_day_ret_available"
                if daily_loss_active
                else f"stale_last_exit_date({last_exit_date_norm} != {current_ymd_norm})"
            )
            kill_switch["metrics"]["hard_daily_loss_active"] = hard_daily_loss_active
            kill_switch["metrics"]["hard_daily_loss_active_reason"] = (
                "fresh_last_day_ret_available"
                if hard_daily_loss_active
                else f"stale_last_exit_date({hard_last_exit_date} != {current_ymd_norm})"
            )
            # [2026-08-21] 판정에 실제로 쓴 값을 한도 옆에 남긴다.
            #
            # metrics.max_drawdown_pct 는 전략 수익률 곡선 기준이고(2026-08-21 기준 -0.5463),
            # limits.max_drawdown_pct 는 0.36 이다. 둘이 나란히 보이면 한도 위반인데
            # triggered=False 인 것처럼 읽힌다. 실제 판정은 hard_max_dd 로 하는데
            # account_basis 가 PASS 면 그것이 계좌 자산 기준값(당시 약 -0.139)으로 교체된다.
            # 판정값이 어디에도 한도 옆에 없어서 하루를 오해했다.
            # 기록만 추가한다. 판정 로직은 그대로다. 상세: .agent/PLANS.md 2026-08-21 (20)
            kill_switch["metrics"]["judged_max_drawdown_pct"] = float(hard_max_dd)
            kill_switch["metrics"]["judged_last_day_ret"] = float(hard_last_day_ret)
            kill_switch["metrics"]["judged_basis"] = str(hard_basis)
            kill_switch["metrics"]["judged_daily_loss_basis"] = str(hard_daily_loss_basis)
            kill_switch["metrics"]["judged_vs_limit"] = {
                "max_drawdown_limit": float(abs(ks_dd_lim)),
                "max_drawdown_breach": bool(hard_max_dd <= -abs(ks_dd_lim)),
                "daily_loss_limit": float(abs(ks_day_lim)),
                "daily_loss_breach": bool(hard_daily_loss_active and (hard_last_day_ret <= -abs(ks_day_lim))),
            }
            if hard_max_dd <= -abs(ks_dd_lim):
                kill_switch["triggered"] = True
                kill_switch["reasons"].append(f"MAX_DD({hard_max_dd:.4f}<= -{abs(ks_dd_lim):.4f};basis={hard_basis})")
            if hard_daily_loss_active and (hard_last_day_ret <= -abs(ks_day_lim)):
                kill_switch["triggered"] = True
                kill_switch["reasons"].append(f"DAILY_LOSS({hard_last_day_ret:.4f} <= -{abs(ks_day_lim):.4f};basis={hard_daily_loss_basis})")

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

        # [2026-09-04] as_of 에서 d_rule_ymd 를 뺐다. 사용자 승인.
        #   d_rule_ymd = paper/fills.csv 의 **마지막 BUY 체결일**이다(1304행).
        #   1453~1465 의 "가격이 D 를 커버하는가" 검사에는 맞는 값이지만,
        #   폭락 가드는 "지금 시장이 어떤가"를 물어야 하는데 그 변수를 그대로 써서
        #   **"마지막으로 샀을 때 시장이 어땠나"** 를 묻고 있었다.
        #
        #   순환: 매수 차단 -> 새 BUY 체결 없음 -> d_rule_ymd 전진 안 함 -> 60일 창 전진 안 함
        #        -> 창 안 고점(06-17)이 안 빠짐 -> 낙폭 그대로 -> 매수 차단
        #   실측 2026-09-04: 같은 보고서의 날짜 기준점 다섯 중 이것만 뒤처져 있었다.
        #        as_of_ymd / prices.date_max / intraday.date_max / meta.latest_date = 20260903
        #        crash metrics.as_of_ymd = 20260824  (11일)
        #   PLANS (208). 근거 기록은 RootA/RootB 어디에도 0건이었다 - 설계가 아니라 변수 재사용.
        #
        #   판정 영향: 오늘은 없다. -37.26%(0824) -> -37.00%(0903) 둘 다 -35% 문턱 아래로 발동.
        #             바뀌는 것은 창이 매일 전진해 **자동 해제가 실제로 오게** 된다는 것뿐이다.
        crash_asof = str(date_max or dt.datetime.now().strftime("%Y%m%d"))
        _p0_checkpoint(logs_dir, "crash_risk_off_eval_start", {"as_of": crash_asof})
        crash_risk_off = _eval_crash_risk_off(crash_asof, cfg)
        _p0_checkpoint(
            logs_dir,
            "crash_risk_off_eval_done",
            {"status": str(((crash_risk_off or {}).get("metrics") or {}).get("status") or "") if isinstance(crash_risk_off, dict) else ""},
        )
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


        # 2) Block when KRX clean parquet is stale beyond the hard threshold.
        #    - Recent-weekend/holiday lag is tolerated by the lag-N rule below.
        # [GATE_RELAX] krx_clean_date_max: allow lag-N(기본2) => SOFT flag, lag-(N+1)+ => risk_off
        lag_days = _resolve_hard_lag_days(2)
        prev_weekday_lagN = _prev_weekday_lag(prev_weekday, lag_days) if prev_weekday else None
        if prev_weekday and (not prev_weekday_lagN):
            flags.append("prev_weekday_lagN_calc_fail")
        if krx_clean_date_max and prev_weekday and prev_weekday_lagN:
            if krx_clean_date_max < prev_weekday_lagN:
                risk_off["enabled"] = True
                risk_off["reasons"].append(f"krx_clean_date_max({krx_clean_date_max}) < prev_weekday_lag{lag_days}({prev_weekday_lagN})")
            elif krx_clean_date_max < prev_weekday:
                flags.append(f"SOFT_GATE krx_clean_date_max({krx_clean_date_max}) < prev_weekday({prev_weekday})")

        # Fail-Closed release: clear INIT-only block when no real blocking reason remains.
        if (risk_off.get("reasons") == ["INIT_FAIL_CLOSED"]) or (not risk_off.get("reasons")):
            risk_off["enabled"] = False
            risk_off["reasons"] = []
        _p0_checkpoint(logs_dir, "risk_eval_done", {"risk_off": bool(risk_off.get("enabled")), "reasons": risk_off.get("reasons") or []})
    except Exception as e:
        # Fail-Closed: unexpected evaluation errors keep new entries blocked.
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
    _p0_checkpoint(logs_dir, "log_cleanup_done", {"would_delete": int(len(log_cleanup.get("would_delete") or []))})

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

    _report_asof_candidates = [
        d_rule_ymd,
        _norm_ymd_oper(md),
        prices.get('date_max') if isinstance(prices, dict) else None,
        krx_clean_date_max,
        intraday_date_max,
    ]
    report_as_of_ymd = max(
        [
            str(x)
            for x in _report_asof_candidates
            if re.fullmatch(r"\d{8}", str(x or ""))
        ],
        default=None,
    )

    report: Dict[str, Any] = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'as_of_ymd': report_as_of_ymd,
        'base_dir': str(base_dir),
        'prices': prices,
        'intraday_prices': intraday_prices,
        'meta': {'latest_date': md, 'market_regime': mr, 'path': str(meta_path)},
        'krx_clean': {
            'date_max': krx_clean_date_max,
            'prev_weekday': prev_weekday,
            'ncode': krx_clean_ncode,
            'effective_date_max': krx_effective_date_max,
            'effective_ncode': krx_effective_ncode,
            'effective_reason': krx_effective_reason,
        },
        'risk_off': risk_off,
        'kill_switch': kill_switch,
        'crash_risk_off': crash_risk_off,
        'data_state': data_state,
        'verify_plan': verify_plan,
        'paper': {
            'open_positions_count': int(open_positions_count),
            'dup_trades_count': int(dup_trades or 0),
            'trades_csv': str(trades_csv),
            'fills_csv': str(fills_csv),
            'd_rule_ymd': d_rule_ymd,
            'horizon_counts': horizon_counts,
            'horizon_hold_days': horizon_hold_stats,
        },
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
    report["account_equity_history"] = _write_account_equity_history(logs_dir, report)
    _p0_checkpoint(logs_dir, "report_write_start", {"out_path": str(out_path)})
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    _p0_checkpoint(logs_dir, "report_write_done", {"out_path": str(out_path)})

    print(f"[P0_CHECK] wrote: {out_path}")
    print(f"[P0_CHECK] prices_date_max={report['prices'].get('date_max')} rows={report['prices'].get('rows')} codes={report['prices'].get('codes')}")
    print(f"[P0_CHECK] market_regime={mr} risk_off={report['risk_off']['enabled']} reasons={report['risk_off']['reasons']}")
    print(
        f"[P0_CHECK] open_positions={report['paper'].get('open_positions_count')} "
        f"max_hold_days(base)={report['config'].get('max_hold_days')} "
        f"horizon_counts={report['paper'].get('horizon_counts')}"
    )
    print(f"[P0_CHECK] krx_clean_date_max={report.get('krx_clean',{}).get('date_max')} prev_weekday={report.get('krx_clean',{}).get('prev_weekday')} dup_trades={report.get('paper',{}).get('dup_trades_count')}")
    if report['flags']:
        print(f"[P0_CHECK] flags={report['flags']}")


if __name__ == '__main__':
    main()

