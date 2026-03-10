#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
redteam_check_v0.py (read-only)
Exit code: 0=PASS, 2=FAIL
Writes: E:\1_Data\2_Logs\redteam_check_YYYYMMDD_HHMMSS.json
"""
from __future__ import annotations

import csv, json, re, datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"
PAPER = ROOT / "paper"

def now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).isoformat(timespec="seconds")

def stat(p: Optional[Path]) -> Dict[str, Any]:
    if p is None: return {"path": None, "exists": False}
    if not p.exists(): return {"path": str(p), "exists": False}
    st = p.stat(); return {"path": str(p), "exists": True, "size": int(st.st_size), "mtime": iso(st.st_mtime)}

def read_text(p: Path) -> str:
    for enc in ("utf-8","utf-8-sig","cp949","latin-1"):
        try: return p.read_text(encoding=enc)
        except UnicodeDecodeError: pass
    return p.read_text(errors="replace")

def read_json(p: Path) -> Any:
    return json.loads(read_text(p))

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

def main() -> int:
    tag = now_tag()
    out_p = LOGS / f"redteam_check_{tag}.json"
    rep: Dict[str, Any] = {"generated_at": dt.datetime.now().isoformat(timespec="seconds"), "hard_fail": [], "warn": [], "info": {}, "paths": {}}
    def HARD(code: str, msg: str, **kw): rep["hard_fail"].append({"code": code, "msg": msg, **kw})
    def WARN(code: str, msg: str, **kw): rep["warn"].append({"code": code, "msg": msg, **kw})

    after_p = LOGS / "after_close_summary_last.json"
    trades_p = PAPER / "trades.csv"
    rep["paths"]["after_close_summary_last"] = stat(after_p)
    rep["paths"]["paper_trades_csv"] = stat(trades_p)
    if not after_p.exists(): HARD("missing_after_close_last", "after_close_summary_last.json not found", path=str(after_p))
    if not trades_p.exists(): HARD("missing_trades_csv", "paper/trades.csv not found", path=str(trades_p))

    reported_fmt4 = None
    if after_p.exists():
        try:
            after = read_json(after_p)
            p0 = after.get("p0", {}) if isinstance(after, dict) else {}
            rs = p0.get("risk_off_reasons") if isinstance(p0, dict) else None
            if rs is None and isinstance(after, dict): rs = after.get("risk_off_reasons")
            reasons = [str(x) for x in rs] if isinstance(rs, list) else []
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
