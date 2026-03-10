#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
redteam_check_v1.py (read-only)
Exit code: 0=PASS, 2=FAIL
Writes: E:\1_Data\2_Logs\redteam_check_YYYYMMDD_HHMMSS.json
Checks:
  - after_close reported_dd <-> dd_source/dd_curve consistency
  - trades.csv schema/scale sanity
  - prices_date_max vs candidates_latest_date lag sanity
  - *_last.json staleness vs newest timestamped files
"""
from __future__ import annotations

import csv, json, math, re, datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"
PAPER = ROOT / "paper"

WARN_STALE_SEC = 86400    # 1 day
HARD_STALE_SEC = 259200   # 3 days
SOFT_LAG_DAYS = -1        # prices behind candidates by 1 day => WARN
HARD_LAG_DAYS = -2        # prices behind candidates by >=2 days => HARD

def now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).isoformat(timespec="seconds")

def stat(p: Optional[Path]) -> Dict[str, Any]:
    if p is None: return {"path": None, "exists": False}
    if not p.exists(): return {"path": str(p), "exists": False}
    st = p.stat()
    return {"path": str(p), "exists": True, "size": int(st.st_size), "mtime": iso(st.st_mtime)}

def read_text(p: Path) -> str:
    for enc in ("utf-8","utf-8-sig","cp949","latin-1"):
        try: return p.read_text(encoding=enc)
        except UnicodeDecodeError: pass
    return p.read_text(errors="replace")

def read_json(p: Path) -> Any:
    return json.loads(read_text(p))

def parse_yyyymmdd(x: Any) -> Optional[str]:
    if x is None: return None
    s = re.sub(r"[^0-9]", "", str(x).strip())
    return s[:8] if len(s) >= 8 else None

def latest_file(dirp: Path, pattern: str) -> Optional[Path]:
    xs = list(dirp.glob(pattern))
    if not xs: return None
    xs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return xs[0]

def recompute_min_dd_sum(dd_curve_csv: Path) -> Tuple[Optional[float], Optional[str]]:
    try:
        vals: List[float] = []
        with dd_curve_csv.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            if "dd_sum" not in (rdr.fieldnames or []):
                return None, "missing_col:dd_sum"
            for row in rdr:
                try: vals.append(float(row["dd_sum"]))
                except Exception: pass
        if not vals: return None, "no_dd_values"
        return min(vals), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def read_prices_date_max(parq: Path) -> Tuple[Optional[str], Optional[str]]:
    try:
        import pandas as pd  # type: ignore
        df = pd.read_parquet(parq, columns=["date"])
        if len(df) == 0: return None, "empty_parquet"
        return parse_yyyymmdd(df["date"].max()), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def main() -> int:
    tag = now_tag()
    out_p = LOGS / f"redteam_check_{tag}.json"
    rep: Dict[str, Any] = {"generated_at": dt.datetime.now().isoformat(timespec="seconds"), "hard_fail": [], "warn": [], "info": {}, "paths": {}}
    def HARD(code: str, msg: str, **kw): rep["hard_fail"].append({"code": code, "msg": msg, **kw})
    def WARN(code: str, msg: str, **kw): rep["warn"].append({"code": code, "msg": msg, **kw})

    # ---------- key paths ----------
    after_p  = LOGS / "after_close_summary_last.json"
    pnl_last = LOGS / "paper_pnl_summary_last.json"
    trades_p = PAPER / "trades.csv"
    prices_p = PAPER / "prices" / "ohlcv_paper.parquet"
    cand_p   = LOGS / "candidates_latest_meta.json"

    rep["paths"]["after_close_summary_last"] = stat(after_p)
    rep["paths"]["paper_pnl_summary_last"]   = stat(pnl_last)
    rep["paths"]["paper_trades_csv"]         = stat(trades_p)
    rep["paths"]["prices_parquet"]           = stat(prices_p)
    rep["paths"]["candidates_meta"]          = stat(cand_p if cand_p.exists() else latest_file(LOGS, "candidates*meta*.json"))

    if not after_p.exists(): HARD("missing_after_close_last", "after_close_summary_last.json not found", path=str(after_p))
    if not trades_p.exists(): HARD("missing_trades_csv", "paper/trades.csv not found", path=str(trades_p))

    # ---------- prices vs candidates lag ----------
    cand_latest = None
    try:
        cand_path = Path(rep["paths"]["candidates_meta"]["path"]) if rep["paths"]["candidates_meta"]["path"] else None
        if cand_path and cand_path.exists():
            cand = read_json(cand_path)
            if isinstance(cand, dict): cand_latest = parse_yyyymmdd(cand.get("latest_date"))
    except Exception as e:
        WARN("cand_meta_read_fail", "failed reading candidates meta", err=str(e))

    prices_max = None
    if prices_p.exists():
        prices_max, err = read_prices_date_max(prices_p)
        if err: WARN("prices_read_fail", "failed reading prices date_max", err=err)

    rep["info"]["cand_latest_date"] = cand_latest
    rep["info"]["prices_date_max"] = prices_max

    try:
        if prices_max and cand_latest:
            d1 = dt.datetime.strptime(prices_max, "%Y%m%d").date()
            d2 = dt.datetime.strptime(cand_latest, "%Y%m%d").date()
            lag = (d1 - d2).days
            rep["info"]["prices_minus_candidates_days"] = lag
            if lag <= HARD_LAG_DAYS:
                HARD("timeline_prices_behind_candidates", "prices_date_max behind candidates_latest_date by >=2 days", prices_date_max=prices_max, candidates_latest_date=cand_latest, lag_days=lag)
            elif lag == SOFT_LAG_DAYS:
                WARN("timeline_prices_behind_candidates_soft", "prices_date_max behind candidates_latest_date by 1 day (SOFT)", prices_date_max=prices_max, candidates_latest_date=cand_latest, lag_days=lag)
    except Exception as e:
        WARN("timeline_calc_fail", "failed timeline lag calc", err=str(e))

    # ---------- after_close reported_dd ----------
    reasons: List[str] = []
    reported_fmt4 = None
    if after_p.exists():
        try:
            after = read_json(after_p)
            p0 = after.get("p0", {}) if isinstance(after, dict) else {}
            rs = p0.get("risk_off_reasons") if isinstance(p0, dict) else None
            if rs is None and isinstance(after, dict): rs = after.get("risk_off_reasons")
            reasons = [str(x) for x in rs] if isinstance(rs, list) else []
            rep["info"]["after_close_reasons"] = reasons
            m = None
            for r in reasons:
                m = re.search(r"MAX_DD\\(([-+]?\\d*\\.?\\d+)\\s*<=\\s*([-+]?\\d*\\.?\\d+)\\)", r)
                if m: break
            if m:
                reported_dd = float(m.group(1))
                reported_fmt4 = float(f"{reported_dd:.4f}")
                rep["info"]["reported_dd_fmt4"] = reported_fmt4
        except Exception as e:
            HARD("after_close_read_fail", "failed reading after_close_summary_last.json", err=str(e), path=str(after_p))

    # ---------- dd chain (match by fmt4) ----------
    if reported_fmt4 is not None:
        dd_source_p = None
        dd_curve_p = None
        xs = sorted(LOGS.glob("kill_switch_dd_source_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for sp in xs[:300]:
            try:
                j = read_json(sp)
                calc = j.get("calc", {}) if isinstance(j.get("calc"), dict) else {}
                art = j.get("artifact", {}) if isinstance(j.get("artifact"), dict) else {}
                f = calc.get("max_dd_recomputed_fmt4")
                ddc = art.get("dd_curve_csv") if isinstance(art.get("dd_curve_csv"), dict) else None
                if f is None or ddc is None or (not ddc.get("path")): continue
                if float(f) != float(reported_fmt4): continue
                dd_source_p = sp
                dd_curve_p = Path(str(ddc["path"]))
                break
            except Exception:
                continue
        rep["paths"]["dd_source_json"] = stat(dd_source_p)
        rep["paths"]["dd_curve_csv"] = stat(dd_curve_p)
        if dd_source_p is None or (not dd_source_p.exists()): HARD("missing_dd_source", "reported_dd exists but matching dd_source not found", reported_fmt4=reported_fmt4)
        if dd_curve_p is None or (not dd_curve_p.exists()): HARD("missing_dd_curve", "reported_dd exists but dd_curve_csv missing", reported_fmt4=reported_fmt4)
        if dd_curve_p and dd_curve_p.exists():
            mn, err = recompute_min_dd_sum(dd_curve_p)
            if err: HARD("dd_curve_recompute_fail", "failed recomputing min(dd_sum)", err=err, path=str(dd_curve_p))
            else:
                fmt4 = float(f"{mn:.4f}")
                rep["info"]["dd_curve_min_dd_sum_fmt4"] = fmt4
                if fmt4 != float(reported_fmt4): HARD("dd_mismatch", "dd_curve fmt4 != reported_dd fmt4", reported_fmt4=reported_fmt4, dd_curve_fmt4=fmt4, path=str(dd_curve_p))

    # ---------- trades sanity ----------
    if trades_p.exists():
        try:
            import pandas as pd  # type: ignore
            df = pd.read_csv(trades_p)
            cols = set([str(c).strip() for c in df.columns])
            need = {"exit_date","pnl_pct"}
            if not need.issubset(cols):
                HARD("trades_missing_cols", "trades.csv missing required columns", need=list(need), cols=list(cols))
            else:
                ex = df["exit_date"].astype(str).str.replace(r"[^0-9]","", regex=True).str.slice(0,8)
                bad = int((ex.str.len() != 8).sum())
                rep["info"]["trades_rows"] = int(len(df))
                rep["info"]["trades_bad_exit_date_rows"] = bad
                if bad > 0: WARN("trades_bad_exit_date", "some exit_date invalid(len!=8)", bad_rows=bad)

                pnl = pd.to_numeric(df["pnl_pct"], errors="coerce")
                nan_rows = int(pnl.isna().sum())
                rep["info"]["trades_pnl_nan_rows"] = nan_rows
                if nan_rows > 0: WARN("trades_pnl_non_numeric", "some pnl_pct non-numeric/blank", nan_rows=nan_rows)

                pnl2 = pnl.dropna()
                if len(pnl2) > 0:
                    abs_max = float(pnl2.abs().max())
                    minv = float(pnl2.min())
                    rep["info"]["trades_pnl_abs_max"] = abs_max
                    rep["info"]["trades_pnl_min"] = minv
                    if abs_max > 2.0: HARD("pnl_scale_extreme", "pnl_pct abs > 2.0 indicates scale/data error", abs_max=abs_max)
                    elif abs_max > 1.0: WARN("pnl_scale_suspect", "pnl_pct abs > 1.0 may indicate scale issue", abs_max=abs_max)
                    if minv < -1.0: HARD("pnl_below_minus1", "pnl_pct < -1.0 implies < -100% return", min=minv)
                    elif minv <= -0.9999: WARN("pnl_near_minus1", "pnl_pct near -1.0 (equity->0) verify definition", min=minv)
        except Exception as e:
            WARN("trades_read_fail", "failed reading trades.csv for sanity", err=str(e))

    # ---------- last pointer staleness ----------
    def check_last(last_name: str, pattern: str):
        lastp = LOGS / last_name
        newest = latest_file(LOGS, pattern)
        rep["paths"][f"last:{last_name}"] = stat(lastp)
        rep["paths"][f"newest:{pattern}"] = stat(newest)
        if lastp.exists() and newest and newest.exists():
            stale = newest.stat().st_mtime - lastp.stat().st_mtime
            rep["info"][f"stale_sec:{last_name}"] = stale
            if stale > HARD_STALE_SEC:
                HARD("last_pointer_stale", f"{last_name} older than newest({pattern}) by >2h", last=stat(lastp), newest=stat(newest), stale_sec=stale)
            elif stale > WARN_STALE_SEC:
                WARN("last_pointer_stale_soft", f"{last_name} older than newest({pattern}) by >10m", last=stat(lastp), newest=stat(newest), stale_sec=stale)

    check_last("paper_pnl_summary_last.json", "paper_pnl_summary_*.json")
    check_last("after_close_summary_last.json", "after_close_summary_*.json")
    check_last("stats_pack_p0_last.json", "stats_pack_p0_*.json")

    out_p.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[REDTEAM] wrote:", out_p)
    print("[REDTEAM] HARD_FAIL:", len(rep["hard_fail"]), "WARN:", len(rep["warn"]))
    if rep["hard_fail"]:
        for x in rep["hard_fail"][:10]: print("[HARD]", x.get("code"), "-", x.get("msg"))
        return 2
    for x in rep["warn"][:10]: print("[WARN]", x.get("code"), "-", x.get("msg"))
    print("[PASS] no hard fails")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
