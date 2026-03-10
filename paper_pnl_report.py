# -*- coding: utf-8 -*-
"""
paper_pnl_report.py

- Reads paper/trades.csv
- Writes 2_Logs/paper_pnl_summary_YYYYMMDD_HHMMSS.json
- Adds equity / drawdown / latest-day return metrics for Kill Switch usage.
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd

def try_read_json(p: "Path"):
    try:
        if p is None:
            return None
        pp = Path(p)
        if not pp.exists():
            return None
        import json
        return json.loads(pp.read_text(encoding="utf-8"))
    except Exception:
        return None

def file_meta(p: "Path") -> dict:
    try:
        if p is None:
            return {"path": None, "exists": False}
        pp = Path(p)
        if not pp.exists():
            return {"path": str(pp), "exists": False}
        st = pp.stat()
        return {
            "path": str(pp),
            "exists": True,
            "size": int(st.st_size),
            "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
        }
    except Exception as e:
        return {"path": str(p) if p is not None else None, "exists": False, "error": str(e)}



def _to_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        s = str(x).strip()
        if s == "":
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


def _normalize_ymd(s: str) -> str:
    # Accept "YYYYMMDD" or "YYYY-MM-DD" etc -> "YYYYMMDD"
    if s is None:
        return ""
    t = "".join(ch for ch in str(s) if ch.isdigit())
    if len(t) >= 8:
        return t[:8]
    return t


def _load_trades(trades_path: Path) -> pd.DataFrame:
    if not trades_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(trades_path, encoding="utf-8-sig")
    except Exception:
        # fallback for legacy encodings
        return pd.read_csv(trades_path, encoding="cp949", errors="replace")


def _pick_ret_col(df: pd.DataFrame) -> str | None:
    # Prefer pnl_pct; fallback to known alternatives
    for c in ["pnl_pct", "ret_pct", "ret", "pnl", "profit_pct"]:
        if c in df.columns:
            return c
    return None


def _equity_metrics(df: pd.DataFrame, ret_col: str) -> dict:
    """
    Build day-level equity curve from CLOSED trades (exit_date present).
    daily_ret = prod(1+ret) - 1 for each exit_date
    """
    if df.empty:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
        }

    exit_col = "exit_date" if "exit_date" in df.columns else None
    if exit_col is None:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
        }

    dfx = df.copy()
    dfx[ret_col] = dfx[ret_col].apply(_to_float)
    dfx[exit_col] = dfx[exit_col].apply(_normalize_ymd)

    dfx = dfx[dfx[exit_col].astype(str).str.len() == 8]
    dfx = dfx[~dfx[ret_col].isna()]

    if dfx.empty:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
        }

    # day-level compound return
    def day_comp(series: pd.Series) -> float:
        vals = [v for v in series.tolist() if isinstance(v, (int, float)) and not math.isnan(v)]
        if not vals:
            return 0.0
        eq = 1.0
        for r in vals:
            eq *= (1.0 + float(r))
        return eq - 1.0

    g = dfx.groupby(exit_col, sort=True)[ret_col].apply(day_comp)
    g = g.sort_index()

    eq = (1.0 + g).cumprod()
    peak = eq.cummax()
    dd = (eq / peak) - 1.0

    last_exit = str(g.index[-1])
    last_day_ret = float(g.iloc[-1])
    end_equity = float(eq.iloc[-1])
    peak_equity = float(peak.iloc[-1])
    dd_end = float(dd.iloc[-1])
    max_dd = float(dd.min())

    last_5 = []
    tail = g.tail(5)
    for k, v in tail.items():
        last_5.append({"date": str(k), "ret": float(v)})

    return {
        "last_exit_date": last_exit,
        "last_day_ret": last_day_ret,
        "end_equity": end_equity,
        "peak_equity": peak_equity,
        "dd_end_pct": dd_end,
        "max_drawdown_pct": max_dd,
        "last_5_days": last_5,
    }


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    trades_path = base_dir / "paper" / "trades.csv"
    logs_dir = base_dir / "2_Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    df = _load_trades(trades_path)
    # --- META: config/lock + (optional) risk context ---
    cfg_path = CFG_PATH if 'CFG_PATH' in globals() else (Path(__file__).resolve().parent / "paper" / "paper_engine_config.json")
    lock_path = LOCK_PATH if 'LOCK_PATH' in globals() else (Path(__file__).resolve().parent / "paper" / "paper_engine_config.lock.json")
    gate_last_path = GATE_LAST if 'GATE_LAST' in globals() else (Path(__file__).resolve().parent / "2_Logs" / "gate_daily_last.json")
    after_close_last_path = AFTER_LAST if 'AFTER_LAST' in globals() else (Path(__file__).resolve().parent / "2_Logs" / "after_close_summary_last.json")

    cfg_file = file_meta(cfg_path)
    lock_file = file_meta(lock_path)

    risk_ctx = {}
    g = try_read_json(gate_last_path)
    if isinstance(g, dict):
        # gate 구조는 다양한데, risk_off/reasons만 있으면 싣는다(없으면 None)
        risk_ctx["gate_daily_last"] = {
            "path": str(gate_last_path),
            "generated_at": g.get("generated_at") or g.get("ts") or g.get("as_of") or None,
            "risk_off": g.get("risk_off") if "risk_off" in g else (g.get("p0", {}) or {}).get("risk_off"),
            "reasons": g.get("reasons") if "reasons" in g else (g.get("p0", {}) or {}).get("reasons"),
        }
    a = try_read_json(after_close_last_path)
    if isinstance(a, dict):
        risk_ctx["after_close_summary_last"] = {
            "path": str(after_close_last_path),
            "generated_at": a.get("generated_at") or None,
            # SSOT: after_close_summary_last.json -> p0.risk_off_reasons, gate.snapshot.reasons
            "risk_off": (a.get("p0") or {}).get("risk_off"),
            "risk_off_reasons": (
                ((a.get("p0") or {}).get("risk_off_reasons"))
                or (((((a.get("gate") or {}).get("snapshot") or {}).get("risk_off") or {})).get("reasons"))
                or (((a.get("gate") or {}).get("snapshot") or {}).get("reasons"))
                or None
            ),
            "reasons": (
                ((a.get("p0") or {}).get("risk_off_reasons"))
                or (((a.get("gate") or {}).get("snapshot") or {}).get("reasons"))
                or (((((a.get("gate") or {}).get("snapshot") or {}).get("risk_off") or {})).get("reasons"))
                or None
            ),
            "note": ((a.get("gate") or {}).get("note") if isinstance(a.get("gate"), dict) else None),
        }
    ret_col = _pick_ret_col(df)

    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = logs_dir / f"paper_pnl_summary_{now_tag}.json"
    last_json = logs_dir / "paper_pnl_summary_last.json"

    if df.empty or ret_col is None:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "trades_used": 0,
            "ret_col": ret_col,
            "avg_ret": 0.0,
            "gross_pf": None,
            "note": "no trades or missing return column",
            "equity": _equity_metrics(pd.DataFrame(), ret_col or "pnl_pct"),
            "meta": {"config_file": cfg_file, "lock_file": lock_file, "risk_context": risk_ctx},
        }
        # PATCH: force payload['after_close'] from payload.meta.risk_context.after_close_summary_last
        try:
            _rc = (payload.get('meta', {}) or {}).get('risk_context', {})
            _ac = (_rc or {}).get('after_close_summary_last', {})
            if isinstance(_ac, dict):
                _ro = _ac.get('risk_off')
                _rs = _ac.get('risk_off_reasons') or _ac.get('reasons')
                if isinstance(_ro, bool) and isinstance(_rs, list):
                    payload['after_close'] = {'risk_off': bool(_ro), 'reasons': [str(x) for x in _rs]}
        except Exception as _e:
            try:
                payload.setdefault('meta', {})
                payload['meta'].setdefault('warnings', [])
                payload['meta']['warnings'].append(f'after_close_promote_fail:{type(_e).__name__}:{_e}')
            except Exception:
                pass
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        # PATCH: force payload['after_close'] from payload.meta.risk_context.after_close_summary_last
        try:
            _rc = (payload.get('meta', {}) or {}).get('risk_context', {})
            _ac = (_rc or {}).get('after_close_summary_last', {})
            if isinstance(_ac, dict):
                _ro = _ac.get('risk_off')
                _rs = _ac.get('risk_off_reasons') or _ac.get('reasons')
                if isinstance(_ro, bool) and isinstance(_rs, list):
                    payload['after_close'] = {'risk_off': bool(_ro), 'reasons': [str(x) for x in _rs]}
        except Exception as _e:
            try:
                payload.setdefault('meta', {})
                payload['meta'].setdefault('warnings', [])
                payload['meta']['warnings'].append(f'after_close_promote_fail:{type(_e).__name__}:{_e}')
            except Exception:
                pass
        last_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(str(out_json))
        return 0

    # numeric returns
    rets = [v for v in df[ret_col].apply(_to_float).tolist() if isinstance(v, (int, float)) and not math.isnan(v)]
    trades_used = int(len(rets))

    avg_ret = float(sum(rets) / trades_used) if trades_used > 0 else 0.0
    gpos = float(sum([r for r in rets if r > 0]))
    gneg_abs = float(sum([-r for r in rets if r < 0]))
    gross_pf = (gpos / gneg_abs) if gneg_abs > 0 else None

    eqm = _equity_metrics(df, ret_col)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trades_used": trades_used,
        "ret_col": ret_col,
        "avg_ret": avg_ret,
        "gross_pf": gross_pf,
        "equity": eqm,
        "meta": {"config_file": cfg_file, "lock_file": lock_file, "risk_context": risk_ctx},
    }

    # PATCH: force payload['after_close'] from payload.meta.risk_context.after_close_summary_last
    try:
        _rc = (payload.get('meta', {}) or {}).get('risk_context', {})
        _ac = (_rc or {}).get('after_close_summary_last', {})
        if isinstance(_ac, dict):
            _ro = _ac.get('risk_off')
            _rs = _ac.get('risk_off_reasons') or _ac.get('reasons')
            if isinstance(_ro, bool) and isinstance(_rs, list):
                payload['after_close'] = {'risk_off': bool(_ro), 'reasons': [str(x) for x in _rs]}
    except Exception as _e:
        try:
            payload.setdefault('meta', {})
            payload['meta'].setdefault('warnings', [])
            payload['meta']['warnings'].append(f'after_close_promote_fail:{type(_e).__name__}:{_e}')
        except Exception:
            pass
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # keep a stable pointer for other scripts
    # PROMOTE_AFTER_CLOSE_BEFORE_WRITE_V7
    # PATCH: force payload['after_close'] from payload.meta.risk_context.after_close_summary_last
    try:
        _rc = (payload.get('meta', {}) or {}).get('risk_context', {})
        _ac = (_rc or {}).get('after_close_summary_last', {})
        if isinstance(_ac, dict):
            _ro = _ac.get('risk_off')
            _rs = _ac.get('risk_off_reasons') or _ac.get('reasons')
            if isinstance(_ro, bool) and isinstance(_rs, list):
                payload['after_close'] = {'risk_off': bool(_ro), 'reasons': [str(x) for x in _rs]}
    except Exception as _e:
        try:
            payload.setdefault('meta', {})
            payload['meta'].setdefault('warnings', [])
            payload['meta']['warnings'].append(f'after_close_promote_fail:{type(_e).__name__}:{_e}')
        except Exception:
            pass
    last_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(out_json))

    # PROMOTE_AFTER_CLOSE_TOPLEVEL_SSOT
    # PATCH: promote after_close to top-level for SSOT reason path (meta.risk_context -> after_close)
    try:
        _ac = (out.get('meta', {}) or {}).get('risk_context', {})
        _ac = (_ac or {}).get('after_close_summary_last', {})
        if isinstance(_ac, dict) and ('risk_off' in _ac) and ('reasons' in _ac):
            payload['after_close'] = {'risk_off': bool(_ac.get('risk_off')), 'reasons': list(_ac.get('reasons') or [])}
    except Exception as _e:
        try:
            payload.setdefault('meta', {})
            payload['meta'].setdefault('warnings', [])
            payload['meta']['warnings'].append(f'after_close_promote_fail:{type(_e).__name__}:{_e}')
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
