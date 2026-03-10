from __future__ import annotations

import re, json
from pathlib import Path
from datetime import datetime, date, timedelta
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict, List, Tuple

try:
    import pandas as pd
except Exception:
    pd = None


ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = ROOT / "2_Logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

CAND_META = LOGS_DIR / "candidates_latest_meta.json"
CAND_CSV  = LOGS_DIR / "candidates_latest_data.csv"

PRICES_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"

KRX_DIR = ROOT / "_krx_manual"  # pick latest krx_daily_*_clean.parquet here


def prev_weekday_lag1(d: date) -> date:
    x = d - timedelta(days=1)
    while x.weekday() >= 5:
        x -= timedelta(days=1)
    return x

def norm8(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    m = re.match(r"^(\d{4})[-/.]?(\d{2})[-/.]?(\d{2})$", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    m = re.search(r"(\d{8})", s)
    return m.group(1) if m else None

def file_mtime(p: Path) -> Optional[str]:
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def lag_days(expected_ymd: str, got_ymd: str) -> Optional[int]:
    try:
        e = datetime.strptime(expected_ymd, "%Y%m%d").date()
        g = datetime.strptime(got_ymd, "%Y%m%d").date()
        return (e - g).days
    except Exception:
        return None


# Allow exactly same-day source (+1 day ahead vs expected) for intraday pre-close runs.
ALLOWED_NEGATIVE_LAG_DAYS = {-1}


def is_fresh_lag_ok(lag: Optional[int]) -> bool:
    if lag is None:
        return False
    if lag == 0:
        return True
    return lag in ALLOWED_NEGATIVE_LAG_DAYS


def lag_fail_note(source: str, lag: int) -> str:
    if lag > 0:
        return f"{source} behind by {lag} day(s)"
    return f"{source} ahead by {-lag} day(s) (future beyond allowed window)"

def max_date_from_meta_json(p: Path) -> Optional[str]:
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

    # search common keys / any string containing yyyymmdd
    keys = [
        "latest_date","max_date","date_max","cand_latest_date",
        "as_of","as_of_ymd","ymd","D","d","date"
    ]
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            v = norm8(obj.get(k))
            if v:
                return v

    # deep scan
    def walk(x: Any):
        if isinstance(x, dict):
            for v in x.values(): yield from walk(v)
        elif isinstance(x, list):
            for v in x: yield from walk(v)
        else:
            yield x

    best = None
    for v in walk(obj):
        if isinstance(v, (str,int)):
            nv = norm8(str(v))
            if nv and (best is None or nv > best):
                best = nv
    return best

def max_date_from_candidates_csv(p: Path) -> Optional[str]:
    # prefer filename if it contains yyyymmdd (it doesn't now), else parse columns
    if pd is None:
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        return None
    if df.empty:
        return None

    cols = [c.lower() for c in df.columns]
    cand_cols = ["date","dt","ymd","d","trade_date","tradedate","as_of","yyyymmdd"]
    for cand in cand_cols:
        if cand in cols:
            col = df.columns[cols.index(cand)]
            s = df[col].dropna()
            if s.empty:
                continue
            v = s.max()
            if hasattr(v, "strftime"):
                return v.strftime("%Y%m%d")
            nv = norm8(str(v))
            if nv:
                return nv

    # fallback: scan any object column for 8-digit
    best = None
    for c in df.columns:
        try:
            s = df[c].dropna().astype(str)
        except Exception:
            continue
        for v in s.head(5000):  # cap
            nv = norm8(v)
            if nv and (best is None or nv > best):
                best = nv
    return best

def max_date_from_prices_parquet(p: Path) -> Optional[str]:
    if pd is None:
        return norm8(p.name)
    try:
        df = pd.read_parquet(p, engine="pyarrow")
    except Exception:
        return norm8(p.name)
    if df is None or df.empty:
        return norm8(p.name)

    cols = [c.lower() for c in df.columns]
    cand_cols = ["date","dt","ymd","d","trade_date","tradedate","as_of","yyyymmdd","timestamp"]
    for cand in cand_cols:
        if cand in cols:
            col = df.columns[cols.index(cand)]
            s = df[col].dropna()
            if s.empty:
                continue
            v = s.max()
            if hasattr(v, "strftime"):
                return v.strftime("%Y%m%d")
            nv = norm8(str(v))
            if nv:
                return nv
    # try index
    try:
        v = df.index.max()
        if hasattr(v, "strftime"):
            return v.strftime("%Y%m%d")
        nv = norm8(str(v))
        if nv:
            return nv
    except Exception:
        pass
    return norm8(p.name)

def pick_latest_krx_clean(dirp: Path) -> Optional[Path]:
    if not dirp.exists():
        return None
    files = [p for p in dirp.rglob("krx_daily_*_clean.parquet") if "_bad" not in str(p).lower()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]

def max_date_from_krx_filename(p: Path) -> Optional[str]:
    # krx_daily_YYYYMMDD_YYYYMMDD_clean.parquet -> second date
    m = re.search(r"krx_daily_(\d{8})_(\d{8})_clean\.parquet$", p.name)
    if m:
        return m.group(2)
    # fallback scan
    dates = re.findall(r"(\d{8})", p.name)
    return dates[-1] if dates else None


@dataclass
class SourceResult:
    max_date: Optional[str] = None
    expected_date: Optional[str] = None
    lag_days: Optional[int] = None
    path: Optional[str] = None
    file_mtime: Optional[str] = None
    status: str = "ERROR"  # PASS | HARD_FAIL | WARN | ERROR
    note: Optional[str] = None


def print_summary_10(out: Dict[str, Any], evidence_path: Path) -> None:
    """Print exactly 10 lines for ops logs."""
    exp = out.get("expected_date")
    verdict = out.get("verdict")
    cand = out.get("cand") or {}
    krx = out.get("krx_clean") or {}
    prices = out.get("prices") or {}
    paths = out.get("paths") or {}

    lines = [
        f"[SUMMARY] verdict={verdict}",
        f"[SUMMARY] expected_date={exp}",
        f"[SUMMARY] cand status={cand.get('status')} max_date={cand.get('max_date')} lag={cand.get('lag_days')}",
        f"[SUMMARY] krx_clean status={krx.get('status')} max_date={krx.get('max_date')} lag={krx.get('lag_days')}",
        f"[SUMMARY] prices status={prices.get('status')} max_date={prices.get('max_date')} lag={prices.get('lag_days')}",
        f"[SUMMARY] cand_meta={paths.get('cand_meta')}",
        f"[SUMMARY] cand_csv={paths.get('cand_csv')}",
        f"[SUMMARY] krx_dir={paths.get('krx_dir')}",
        f"[SUMMARY] prices_parquet={paths.get('prices_parquet')}",
        f"[SUMMARY] evidence={str(evidence_path)}",
    ]
    for ln in lines:
        print(ln)

def main():
    expected = prev_weekday_lag1(date.today()).strftime("%Y%m%d")

    # ---- cand ----
    cand = SourceResult(expected_date=expected)
    cand.path = str(CAND_META if CAND_META.exists() else CAND_CSV)
    cand.file_mtime = file_mtime(CAND_META if CAND_META.exists() else CAND_CSV) if (CAND_META.exists() or CAND_CSV.exists()) else None

    cand_max = None
    if CAND_META.exists():
        cand_max = max_date_from_meta_json(CAND_META)
    if cand_max is None and CAND_CSV.exists():
        cand_max = max_date_from_candidates_csv(CAND_CSV)

    cand.max_date = cand_max
    if cand.max_date is None:
        cand.status = "HARD_FAIL"
        cand.note = f"candidates meta/csv exists? meta={CAND_META.exists()} csv={CAND_CSV.exists()} (could not infer max_date)"
    else:
        cand.lag_days = lag_days(expected, cand.max_date)
        if cand.lag_days is None:
            cand.status = "HARD_FAIL"
            cand.note = f"could not compute lag expected={expected} max_date={cand.max_date}"
        elif is_fresh_lag_ok(cand.lag_days):
            cand.status = "PASS"
        else:
            cand.status = "HARD_FAIL"
            cand.note = lag_fail_note("cand", int(cand.lag_days))

    # ---- prices ----
    prices = SourceResult(expected_date=expected)
    prices.path = str(PRICES_PARQUET)
    prices.file_mtime = file_mtime(PRICES_PARQUET) if PRICES_PARQUET.exists() else None
    if not PRICES_PARQUET.exists():
        prices.status = "HARD_FAIL"
        prices.note = f"missing prices parquet: {PRICES_PARQUET}"
    else:
        prices.max_date = max_date_from_prices_parquet(PRICES_PARQUET)
        prices.lag_days = lag_days(expected, prices.max_date) if prices.max_date else None
        if prices.max_date is None or prices.lag_days is None:
            prices.status = "HARD_FAIL"
            prices.note = f"could not infer/lag for prices: max_date={prices.max_date}"
        elif is_fresh_lag_ok(prices.lag_days):
            prices.status = "PASS"
        else:
            prices.status = "HARD_FAIL"
            prices.note = lag_fail_note("prices", int(prices.lag_days))

    # ---- krx_clean ----
    krx = SourceResult(expected_date=expected)
    krx_parq = pick_latest_krx_clean(KRX_DIR)
    if not krx_parq:
        krx.status = "HARD_FAIL"
        krx.note = f"no krx_daily_*_clean.parquet under {KRX_DIR}"
    else:
        krx.path = str(krx_parq)
        krx.file_mtime = file_mtime(krx_parq)
        krx.max_date = max_date_from_krx_filename(krx_parq)
        krx.lag_days = lag_days(expected, krx.max_date) if krx.max_date else None
        if krx.max_date is None or krx.lag_days is None:
            krx.status = "HARD_FAIL"
            krx.note = f"could not infer/lag for krx_clean: max_date={krx.max_date}"
        elif is_fresh_lag_ok(krx.lag_days):
            krx.status = "PASS"
        else:
            krx.status = "HARD_FAIL"
            krx.note = lag_fail_note("krx_clean", int(krx.lag_days))

    verdict = "PASS"
    reasons = []
    for nm, rr in [("cand", cand), ("krx_clean", krx), ("prices", prices)]:
        if rr.status != "PASS":
            verdict = "HARD_FAIL"
            reasons.append(f"{nm}: {rr.status} (max_date={rr.max_date} expected={expected} lag={rr.lag_days}) {rr.note or ''}".strip())

    out = {
        "run_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "expected_date": expected,
        "verdict": verdict,
        "reasons": reasons,
        "cand": asdict(cand),
        "krx_clean": asdict(krx),
        "prices": asdict(prices),
        "paths": {
            "cand_meta": str(CAND_META),
            "cand_csv": str(CAND_CSV),
            "prices_parquet": str(PRICES_PARQUET),
            "krx_dir": str(KRX_DIR),
        }
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = LOGS_DIR / f"freshness_source_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    (LOGS_DIR / "freshness_source_last.json").write_text(
        json.dumps({"last": str(out_path), "ts": ts}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # ops summary (fixed 10 lines)
    print_summary_10(out, out_path)


    print(f"[FRESHNESS] wrote: {out_path}")
    print(f"[FRESHNESS] verdict={verdict} reasons={len(reasons)}")
    if verdict != "PASS":
        for r in reasons[:10]:
            print(f"[FRESHNESS] HARD_FAIL: {r}")

    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())



