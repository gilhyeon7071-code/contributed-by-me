import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOTA = Path(r"E:\1_Data")
LOGS = ROOTA / "2_Logs"
CACHE = ROOTA / "_cache"

CAND = (LOGS / "candidates_latest_data.filtered.csv") if (LOGS / "candidates_latest_data.filtered.csv").exists() else (LOGS / "candidates_latest_data.csv")
SSOT_SECTOR = CACHE / "sector_ssot.csv"
MAP = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"

OUT = LOGS / "candidates_latest_data.with_sector_score.csv"
HIST = LOGS / "sector_score_history.csv"

SNAP_COLS = ["date8", "code", "krx_sector", "sector_code", "sector_action", "sector_score", "sector_strength"]
ALLOWED_SECTOR_CODES = {"005", "008", "009", "011", "012", "013", "015", "016", "017", "018", "019", "020", "022", "024", "025", "026"}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _extract_date8(cand: pd.DataFrame) -> str:
    if "date" in cand.columns:
        raw = cand["date"].dropna()
        if len(raw) > 0:
            try:
                return str(raw.iloc[0]).replace("-", "").replace("/", "")[:8]
            except Exception:
                pass
    return datetime.now().strftime("%Y%m%d")


def _resolve_window(cand: pd.DataFrame) -> tuple[str, str, str]:
    date8 = _extract_date8(cand)
    try:
        asof = datetime.strptime(date8, "%Y%m%d")
    except Exception:
        asof = datetime.now()
        date8 = asof.strftime("%Y%m%d")

    end_date = asof.strftime("%Y-%m-%d")
    start_date = (asof - timedelta(days=550)).strftime("%Y-%m-%d")
    return start_date, end_date, date8


def _save_snapshot(df: pd.DataFrame, date8: str) -> None:
    df["date8"] = date8
    snap = df[[c for c in SNAP_COLS if c in df.columns]].copy()

    snap_path = LOGS / f"sector_score_snapshot_{date8}.csv"
    snap.to_csv(snap_path, index=False, encoding="utf-8-sig")
    print(f"SNAP {snap_path} rows={len(snap)}")

    if HIST.exists():
        try:
            hist = pd.read_csv(HIST, dtype={"code": str, "date8": str})
            hist = hist[hist["date8"] != date8]
            hist = pd.concat([hist, snap], ignore_index=True)
        except Exception:
            hist = snap.copy()
    else:
        hist = snap.copy()

    hist.to_csv(HIST, index=False, encoding="utf-8-sig")
    print(f"HIST {HIST} rows={len(hist)} (+{len(snap)} for {date8})")


def _build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--krx-key", default=os.getenv("KRX_API_KEY", ""))
    ap.add_argument("--force-mock", action="store_true", help="Force mock mode even when KRX key exists")
    return ap.parse_args()


def main() -> int:
    args = _build_args()

    if not CAND.exists():
        raise SystemExit(f"FATAL missing {CAND}")
    if not SSOT_SECTOR.exists():
        raise SystemExit(f"FATAL missing {SSOT_SECTOR}")
    if not MAP.exists():
        raise SystemExit(f"FATAL missing {MAP}")

    cand = pd.read_csv(CAND, dtype={"code": str})
    cand["code"] = cand["code"].astype(str).str.zfill(6)

    ssot = pd.read_csv(SSOT_SECTOR, dtype={"code": str, "krx_sector": str})
    ssot["code"] = ssot["code"].astype(str).str.zfill(6)
    ssot["krx_sector"] = ssot["krx_sector"].astype(str).str.strip()

    mp = pd.read_csv(MAP, dtype={"krx_sector": str, "sector_code": str})
    mp["krx_sector"] = mp["krx_sector"].astype(str).str.strip()
    mp["sector_code"] = mp["sector_code"].astype(str).str.strip()

    df = cand.merge(ssot[["code", "krx_sector"]], on="code", how="left")
    df["krx_sector"] = df["krx_sector"].astype(str).str.strip()
    df = df.merge(mp[["krx_sector", "sector_code"]], on="krx_sector", how="left")

    sc = df["sector_code"].astype(str).str.strip()
    df["sector_code"] = sc
    sector_codes = sorted([x for x in sc.unique().tolist() if x and x.lower() != "nan" and x in ALLOWED_SECTOR_CODES])

    df["sector_score"] = 0.0
    df["sector_action"] = ""
    df["sector_strength"] = 0.0

    start_date, end_date, date8 = _resolve_window(cand)

    if not sector_codes:
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        print("WROTE", OUT, "note=no_sector_codes")
        _save_snapshot(df, date8)
        return 0

    sys.path.insert(0, r"E:\1_Data\_dev\kospi_sector")
    from data.krx_api import KRXClient
    from data.oecd_cli import OECDCLIClient
    from strategy.sector_signals import SectorSignalEngine

    krx_key = str(args.krx_key or "").strip()
    force_mock = bool(args.force_mock or _env_flag("SECTOR_SCORE_FORCE_MOCK", False))
    use_mock = force_mock or (not krx_key)

    krx = KRXClient(auth_key=(None if use_mock else krx_key), mock=use_mock)
    oecd = OECDCLIClient(mock=force_mock)

    print(f"[SECTOR] mode={'MOCK' if use_mock else 'REAL'} krx_key={'SET' if krx_key else 'EMPTY'} start={start_date} end={end_date}")

    sector_data = {}
    for code in sector_codes:
        try:
            s = krx.get_sector_index(code, start_date, end_date)
            if s is None or len(s) == 0 or ("close" not in s.columns):
                continue
            sector_data[code] = s[["close"]].rename(columns={"close": code})
        except Exception as e:
            print(f"[SECTOR][WARN] code={code} fetch failed: {type(e).__name__}:{e}")

    if not sector_data:
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        print("WROTE", OUT, "note=no_sector_data")
        _save_snapshot(df, date8)
        return 0

    prices = pd.concat([sector_data[c] for c in sorted(sector_data.keys())], axis=1).dropna(how="all")
    if prices is None or len(prices) == 0:
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        print("WROTE", OUT, "note=empty_prices_matrix")
        _save_snapshot(df, date8)
        return 0

    cli_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
    cli_df = oecd.get_cli(start=cli_start)
    cli_gate = cli_df.set_index("date")["risk_on"]
    cli_gate.index = pd.to_datetime(cli_gate.index)

    eng = SectorSignalEngine(prices=prices, cli_gate=cli_gate)
    latest_date = prices.index[-1]
    signals = eng.scan_signals(latest_date)

    sig_map = {k: (v.action, float(v.strength)) for k, v in signals.items()}

    for i, row in df.iterrows():
        code = str(row["sector_code"]).strip()
        if code in sig_map:
            act, strength = sig_map[code]
            df.at[i, "sector_action"] = act
            df.at[i, "sector_strength"] = strength
            df.at[i, "sector_score"] = strength if act == "BUY" else 0.0

    df.to_csv(OUT, index=False, encoding="utf-8-sig")
    print("WROTE", OUT, "sectors", sorted(sector_data.keys()), "asof", str(latest_date)[:10])

    _save_snapshot(df, date8)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())