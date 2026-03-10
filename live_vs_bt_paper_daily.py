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
    ap.add_argument("--allow-summary-fallback", action="store_true", help="Allow trigger fallback to summary means if aligned window is unavailable")

    # backtest quality/freshness gates (must pass before auto-optimize)
    ap.add_argument("--max-backtest-age-days", type=float, default=7.0, help="Max age for backtest summary file")
    ap.add_argument("--min-oos-trades", type=int, default=20, help="Min OOS trades required for optimize trigger")
    ap.add_argument("--min-oos-pf", type=float, default=0.80, help="Min OOS PF required for optimize trigger")
    ap.add_argument("--min-stable-score", type=float, default=-1e9, help="Min stable best_score required for optimize trigger")
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


def _load_aligned_metrics(base_dir: Path, align_window_trades: int, min_shared_trades: int) -> Dict[str, Any]:
    live_path = base_dir / "paper" / "trades.csv"
    bt_path = base_dir / "12_Risk_Controlled" / "report_backtest_trades_v41_1.csv"
    out: Dict[str, Any] = {
        "mode": "aligned_exit_window",
        "ready": False,
        "reason": None,
        "live_path": str(live_path),
        "bt_path": str(bt_path),
        "window_start": None,
        "window_end": None,
        "live_n": 0,
        "bt_n": 0,
        "live_avg_ret": None,
        "bt_avg_ret": None,
        "ret_col_live": None,
        "ret_col_bt": None,
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

    if live.empty:
        out["reason"] = "live_no_valid_closed_trades"
        return out

    live = live.sort_values("exit_ymd")
    k = max(1, int(align_window_trades))
    live_tail = live.tail(k).copy()

    if len(live_tail) < max(1, int(min_shared_trades)):
        out["reason"] = "live_too_few_trades"
        out["live_n"] = int(len(live_tail))
        return out

    w_start = str(live_tail["exit_ymd"].min())
    w_end = str(live_tail["exit_ymd"].max())

    bt_w = bt[(bt["exit_ymd"].astype(str) >= w_start) & (bt["exit_ymd"].astype(str) < w_end)].copy()

    out["window_start"] = w_start
    out["window_end"] = w_end
    out["live_n"] = int(len(live_tail))
    out["bt_n"] = int(len(bt_w))

    if len(bt_w) < max(1, int(min_shared_trades)):
        out["reason"] = "bt_too_few_trades_in_window"
        return out

    out["live_avg_ret"] = float(live_tail["ret"].mean())
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
    }
    if not p.exists():
        out["reason"] = "missing_backtest_summary"
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
    out: Dict[str, Any] = {
        "summary_path": str(sum_path),
        "stable_path": str(stable_path),
        "min_oos_trades": int(min_oos_trades),
        "min_oos_pf": float(min_oos_pf),
        "min_stable_score": float(min_stable_score),
        "oos_n": None,
        "oos_pf": None,
        "stable_score": None,
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
    except Exception as e:
        out["reasons"].append(f"summary_parse_fail:{type(e).__name__}")
        return out

    try:
        st = json.loads(stable_path.read_text(encoding="utf-8"))
        stable_score = float(st.get("best_score") or 0.0)
        out["stable_score"] = stable_score
    except Exception as e:
        out["reasons"].append(f"stable_parse_fail:{type(e).__name__}")
        return out

    if int(out["oos_n"] or 0) < int(min_oos_trades):
        out["reasons"].append(f"oos_trades_low({out['oos_n']}<{int(min_oos_trades)})")
    if float(out["oos_pf"] or 0.0) < float(min_oos_pf):
        out["reasons"].append(f"oos_pf_low({float(out['oos_pf']):.4f}<{float(min_oos_pf):.4f})")
    if float(out["stable_score"] or 0.0) < float(min_stable_score):
        out["reasons"].append(f"stable_score_low({float(out['stable_score']):.4f}<{float(min_stable_score):.4f})")

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
    cands.append(str(Path(r"E:\\vibe\\buffett\\.venv\\Scripts\\python.exe")))
    cands.append(str(Path(r"C:\\Users\\jjtop\\AppData\\Local\\Programs\\Python\\Python312\\python.exe")))
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

    run_ymd = datetime.now().strftime("%Y%m%d")
    if args.date and len(args.date) == 8 and args.date.isdigit():
        run_ymd = args.date

    fills = _load_fills(fills_path)
    prices = _load_prices(prices_path)

    m = fills.merge(prices, on=["__ymd", "__code"], how="left", suffixes=("", "_px"))
    m["ref_open"] = m["open"]
    m["ref_close"] = m["close"]
    m["slip_vs_open_pct"] = [_slip(fp, ro) for fp, ro in zip(m["fill_price"], m["ref_open"])]
    m["slip_vs_close_pct"] = [_slip(fp, rc) for fp, rc in zip(m["fill_price"], m["ref_close"])]

    m = m[m["__ymd"].astype(str) == str(run_ymd)].copy()

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
    live_summary = _load_live_metrics(logs_dir)
    bt_summary = _load_bt_metrics(base_dir)
    aligned = _load_aligned_metrics(base_dir, int(args.align_window_trades), int(args.min_shared_trades))

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

    trigger_raw = bool(enough_trades and (trigger_abs or trigger_ratio))

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
        "run_ymd": run_ymd,
        "thresholds": {
            "deviation_threshold": float(args.deviation_threshold),
            "ratio_threshold": float(args.ratio_threshold),
            "min_live_trades": int(args.min_live_trades),
            "cooldown_days": int(args.cooldown_days),
            "align_window_trades": int(args.align_window_trades),
            "min_shared_trades": int(args.min_shared_trades),
            "max_backtest_age_days": float(args.max_backtest_age_days),
            "min_oos_trades": int(args.min_oos_trades),
            "min_oos_pf": float(args.min_oos_pf),
            "min_stable_score": float(args.min_stable_score),
            "allow_summary_fallback": bool(args.allow_summary_fallback),
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
            "trigger_raw": bool(trigger_raw),
        },
        "optimize": optimize_result,
        "state_file": str(state_path),
    }

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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











