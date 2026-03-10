from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

ROOTA = Path(r"E:\1_Data")
LOGS = ROOTA / "2_Logs"
LOGS.mkdir(parents=True, exist_ok=True)


def _now_ymd() -> str:
    return date.today().strftime("%Y%m%d")


def _write_json(p: Path, obj: dict) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _clean_json_obj(x: Any) -> Any:
    # Convert numpy scalar to builtin scalar first.
    if hasattr(x, "item"):
        try:
            x = x.item()
        except Exception:
            pass
    if isinstance(x, dict):
        return {k: _clean_json_obj(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_clean_json_obj(v) for v in x]
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    return x


def _as_ymd(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        if hasattr(x, "strftime"):
            return x.strftime("%Y%m%d")
    except Exception:
        pass
    s = str(x).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10].replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return None


def _load_cli_signal() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "cli_mom": None,
        "risk_on_cli": None,
        "quality": "LOW",
        "source": "none",
    }
    try:
        import sys

        sys.path.insert(0, r"E:\1_Data\_dev\kospi_sector")
        from data.oecd_cli import OECDCLIClient  # type: ignore

        try:
            oecd = OECDCLIClient(mock=False)
            cli_df = oecd.get_cli(start=(datetime.now().strftime("%Y") + "-01"))
            q = "HIGH"
            src = "oecd_cli_real"
        except Exception:
            oecd = OECDCLIClient(mock=True)
            cli_df = oecd.get_cli(start="2023-01")
            q = "MED"
            src = "oecd_cli_mock"

        if not cli_df.empty:
            out["cli_mom"] = float(cli_df["cli_mom"].iloc[-1])
            out["risk_on_cli"] = bool(cli_df["risk_on"].iloc[-1])
            out["quality"] = q
            out["source"] = src
    except Exception as e:
        print(f"[WARN] oecd_cli unavailable: {type(e).__name__}: {e}")
    return out


def _load_kospi_from_pykrx(as_of_ymd: str, lookback_days: int = 260) -> Optional[pd.DataFrame]:
    try:
        from pykrx import stock  # type: ignore

        end_dt = datetime.strptime(as_of_ymd, "%Y%m%d").date()
        start_dt = end_dt - timedelta(days=max(lookback_days * 2, 400))

        # 1001 is KOSPI in pykrx index code table.
        df = stock.get_index_ohlcv_by_date(start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"), "1001")
        if df is None or df.empty:
            return None
        x = df.reset_index().copy()
        date_col = x.columns[0]
        x["date"] = pd.to_datetime(x[date_col], errors="coerce")
        close_col = next((c for c in ["종가", "close", "Close"] if c in x.columns), None)
        if close_col is None:
            return None
        x["close"] = pd.to_numeric(x[close_col], errors="coerce")
        x = x[["date", "close"]].dropna().sort_values("date")
        return x.tail(lookback_days + 20)
    except Exception as e:
        print(f"[WARN] pykrx KOSPI fetch failed: {type(e).__name__}: {e}")
        return None


def _load_local_proxy_prices() -> Optional[pd.DataFrame]:
    p = ROOTA / "paper" / "prices" / "ohlcv_paper.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
        date_col = next((c for c in ["date", "ymd", "trade_date"] if c in df.columns), None)
        close_col = next((c for c in ["close", "종가", "Close"] if c in df.columns), None)
        if date_col is None or close_col is None:
            return None

        x = df.copy()
        x["ymd"] = x[date_col].map(_as_ymd)
        x["date"] = pd.to_datetime(x["ymd"], format="%Y%m%d", errors="coerce")
        x["close"] = pd.to_numeric(x[close_col], errors="coerce")
        x = x.dropna(subset=["date", "close"]).copy()

        # Equal-weight cross-sectional close as a local market proxy.
        g = (
            x.groupby("date", as_index=False)
            .agg(close=("close", "mean"), ncode=("close", "size"))
            .sort_values("date")
        )
        # Guard: too-few codes means this proxy is not representative.
        g = g[g["ncode"] >= 500].copy()
        if g.empty:
            return None
        return g[["date", "close"]].tail(320)
    except Exception as e:
        print(f"[WARN] local proxy load failed: {type(e).__name__}: {e}")
        return None



def _load_local_proxy_from_krx_clean(lookback_days: int = 320) -> Optional[pd.DataFrame]:
    krx_dir = ROOTA / "_krx_manual"
    if not krx_dir.exists():
        return None
    files = sorted(krx_dir.glob("krx_daily_*_clean.parquet"))
    if not files:
        return None
    files = files[-40:]
    parts = []
    for p in files:
        try:
            df = pd.read_parquet(p)
        except Exception:
            continue
        date_col = next((c for c in ["date", "ymd", "trade_date"] if c in df.columns), None)
        code_col = next((c for c in ["code", "ticker"] if c in df.columns), None)
        close_col = next((c for c in ["close", "종가", "Close"] if c in df.columns), None)
        if date_col is None or code_col is None or close_col is None:
            continue

        x = df[[date_col, code_col, close_col]].copy()
        x["ymd"] = x[date_col].map(_as_ymd)
        x["date"] = pd.to_datetime(x["ymd"], format="%Y%m%d", errors="coerce")
        x["code"] = x[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
        x["close"] = pd.to_numeric(x[close_col], errors="coerce")
        x = x.dropna(subset=["date", "code", "close"]).copy()
        x = x[x["close"] > 0]
        if not x.empty:
            parts.append(x[["date", "code", "close"]])

    if not parts:
        return None

    z = pd.concat(parts, ignore_index=True)
    z = z.drop_duplicates(["date", "code"], keep="last")
    z = z.sort_values(["code", "date"]).copy()
    z["ret1"] = z.groupby("code")["close"].pct_change(1)
    z = z.dropna(subset=["ret1"]).copy()
    # Guard against bad ticks/outliers in fallback proxy.
    z = z[(z["ret1"] > -0.30) & (z["ret1"] < 0.30)]

    g = (
        z.groupby("date", as_index=False)
        .agg(ret1=("ret1", "median"), ncode=("code", "nunique"))
        .sort_values("date")
    )
    g = g[g["ncode"] >= 500].copy()
    if g.empty:
        return None

    g["ret1"] = pd.to_numeric(g["ret1"], errors="coerce").fillna(0.0).clip(-0.15, 0.15)
    g["close"] = (1.0 + g["ret1"]).cumprod() * 100.0
    return g[["date", "close"]].tail(lookback_days + 20)


def _read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _load_rate_series_local(lookback_rows: int = 600) -> Optional[pd.DataFrame]:
    candidates = [
        LOGS / "rate_series_latest.csv",
        LOGS / "rate_series.csv",
        ROOTA / "_cache" / "rate_series_latest.csv",
        ROOTA / "_cache" / "rate_series.csv",
    ]
    src = next((p for p in candidates if p.exists()), None)
    if src is None:
        return None

    try:
        df = _read_csv_any(src)
    except Exception as e:
        print(f"[WARN] rate series read failed: {type(e).__name__}: {e}")
        return None

    if df is None or df.empty:
        return None

    date_col = next((c for c in ["date", "ymd", "trade_date", "dt"] if c in df.columns), None)
    rate_col = next((c for c in ["rate", "base_rate", "policy_rate", "korea_base_rate", "yield_3y", "3y", "금리", "기준금리"] if c in df.columns), None)
    if date_col is None or rate_col is None:
        return None

    x = df[[date_col, rate_col]].copy()
    x["ymd"] = x[date_col].map(_as_ymd)
    x["date"] = pd.to_datetime(x["ymd"], format="%Y%m%d", errors="coerce")
    x["rate"] = pd.to_numeric(x[rate_col], errors="coerce")
    x = x.dropna(subset=["date", "rate"]).sort_values("date")
    if x.empty:
        return None

    # Keep rate unit in percentage points (e.g., 3.50), not decimal.
    x["rate"] = x["rate"].clip(-5.0, 30.0)
    x["source"] = src.name
    return x[["date", "rate", "source"]].tail(lookback_rows)


def _compute_rate_context(rate_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if rate_df is None or rate_df.empty:
        return {
            "available": False,
            "source": "none",
            "hawkish_score": 0.0,
            "metrics": {},
        }

    x = rate_df.sort_values("date").copy()
    s = pd.to_numeric(x["rate"], errors="coerce").dropna()
    if len(s) < 2:
        return {
            "available": False,
            "source": str(x["source"].iloc[-1]) if "source" in x.columns and len(x) else "unknown",
            "hawkish_score": 0.0,
            "metrics": {},
        }

    last = float(s.iloc[-1])
    prev20 = float(s.iloc[-20]) if len(s) > 20 else float(s.iloc[0])
    prev60 = float(s.iloc[-60]) if len(s) > 60 else float(s.iloc[0])
    chg20 = last - prev20
    chg60 = last - prev60

    # Positive score means hawkish (rate-up pressure).
    hawkish = _clamp(max(chg20 / 0.50, chg60 / 1.00), -1.0, 1.0)

    return {
        "available": True,
        "source": str(x["source"].iloc[-1]) if "source" in x.columns else "rate_series",
        "hawkish_score": float(hawkish),
        "metrics": {
            "rate_level": last,
            "rate_chg_20": float(chg20),
            "rate_chg_60": float(chg60),
        },
    }
def _compute_macro_from_prices(px: pd.DataFrame, rate_ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if px is None or px.empty:
        return {
            "regime": "UNKNOWN",
            "rate_signal": 0.0,
            "crash_prob": 0.5,
            "risk_on_mkt": False,
            "metrics": {},
        }

    s = px.sort_values("date")["close"].astype(float)
    df = pd.DataFrame({"close": s.values}, index=px.sort_values("date")["date"])
    df["ret1"] = df["close"].pct_change(1)
    df["ret20"] = df["close"].pct_change(20)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["rv20"] = df["ret1"].rolling(20).std()
    df["peak120"] = df["close"].rolling(120).max()
    df["dd120"] = df["close"] / (df["peak120"] + 1e-9) - 1.0

    last = df.iloc[-1]
    ret1 = float(last.get("ret1") or 0.0)
    ret20 = float(last.get("ret20") or 0.0)
    rv20 = float(last.get("rv20") or 0.0)
    dd120 = float(last.get("dd120") or 0.0)
    ma20 = float(last.get("ma20") or last.get("close") or 0.0)
    ma60 = float(last.get("ma60") or last.get("close") or 0.0)
    close = float(last.get("close") or 0.0)

    vol_score = _clamp((rv20 - 0.012) / 0.03, 0.0, 1.0)
    dd_score = _clamp(abs(min(dd120, 0.0)) / 0.22, 0.0, 1.0)
    drop_score = _clamp(abs(min(ret1, 0.0)) / 0.05, 0.0, 1.0)
    crash_prob = float(_clamp(max(vol_score, dd_score, drop_score), 0.0, 1.0))

    rate_hawkish = 0.0
    rate_source = "none"
    rate_metrics: Dict[str, Any] = {}
    if isinstance(rate_ctx, dict):
        rate_hawkish = float(_clamp(rate_ctx.get("hawkish_score", 0.0), -1.0, 1.0))
        rate_source = str(rate_ctx.get("source") or "none")
        rate_metrics = dict(rate_ctx.get("metrics") or {})

    if dd120 <= -0.18 or ret1 <= -0.035:
        regime = "CRASH"
    elif (close < ma60 and ret20 < -0.03) or (rate_hawkish >= 0.70 and close < ma20):
        regime = "RATE_HIKE_FEAR"
    elif rv20 >= 0.028:
        regime = "VOLATILE"
    elif close > ma60 and ma20 >= ma60 and ret20 > 0:
        regime = "NORMAL"
    elif close > ma60 and ret20 > -0.01:
        regime = "RECOVERY"
    else:
        regime = "NORMAL"

    # Keep field name 'rate_signal' for compatibility.
    # Positive rate_hawkish means tightening pressure, so subtract from risk-on momentum signal.
    momentum_sig = (ret20 / 0.08) - (rv20 / 0.05)
    rate_signal = float(_clamp(momentum_sig - 0.80 * rate_hawkish, -1.0, 1.0))
    risk_on_mkt = bool((crash_prob < 0.55) and (regime not in {"CRASH", "RATE_HIKE_FEAR"}) and (rate_hawkish < 0.80))

    metrics = {
        "ret1": ret1,
        "ret20": ret20,
        "rv20": rv20,
        "dd120": dd120,
        "close": close,
        "ma20": ma20,
        "ma60": ma60,
        "rate_hawkish": float(rate_hawkish),
        "rate_source": rate_source,
    }
    for k, v in rate_metrics.items():
        metrics[str(k)] = v

    return {
        "regime": regime,
        "rate_signal": rate_signal,
        "crash_prob": crash_prob,
        "risk_on_mkt": risk_on_mkt,
        "metrics": metrics,
    }


def main() -> int:
    today = _now_ymd()

    try:
        from holiday_manager import HolidayManager  # type: ignore

        hm = HolidayManager()
        if not hm.explain(today).is_open:
            print(f"[SKIP] market closed: {today}")
            return 0
    except Exception as e:
        print(f"[WARN] holiday_manager unavailable -> continue: {type(e).__name__}: {e}")

    cli = _load_cli_signal()

    px = _load_kospi_from_pykrx(today, lookback_days=260)
    macro_source = "pykrx_kospi"
    q_macro = "HIGH"
    if px is None or px.empty:
        px = _load_local_proxy_from_krx_clean(lookback_days=260)
        macro_source = "krx_clean_proxy"
        q_macro = "MED" if (px is not None and not px.empty) else "LOW"
    if px is None or px.empty:
        px = _load_local_proxy_prices()
        macro_source = "local_proxy_prices"
        q_macro = "LOW" if (px is not None and not px.empty) else "LOW"

    rate_df = _load_rate_series_local(lookback_rows=600)
    rate_ctx = _compute_rate_context(rate_df)
    macro = _compute_macro_from_prices(px if px is not None else pd.DataFrame(), rate_ctx=rate_ctx)

    risk_on_cli = cli.get("risk_on_cli")
    risk_on_mkt = bool(macro.get("risk_on_mkt", False))
    risk_on = bool(risk_on_mkt and risk_on_cli) if (risk_on_cli is not None) else bool(risk_on_mkt)

    quality = "LOW"
    if cli.get("quality") == "HIGH" and q_macro == "HIGH":
        quality = "HIGH"
    elif (cli.get("quality") in {"HIGH", "MED"}) and (q_macro in {"HIGH", "MED"}):
        quality = "MED"

    out = {
        "as_of_ymd": today,
        "regime": macro.get("regime", "UNKNOWN"),
        "cli_mom": cli.get("cli_mom"),
        "rate_signal": float(macro.get("rate_signal", 0.0) or 0.0),
        "crash_prob": float(macro.get("crash_prob", 0.0) or 0.0),
        "risk_on": bool(risk_on),
        "quality": quality,
        "sources": {
            "cli": cli.get("source"),
            "macro": macro_source,
            "rate": rate_ctx.get("source"),
        },
        "rate_context": rate_ctx,
        "market_metrics": macro.get("metrics", {}),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out = _clean_json_obj(out)

    p_last = LOGS / "macro_signal_latest.json"
    p_day = LOGS / f"macro_signal_{today}.json"
    _write_json(p_day, out)
    _write_json(p_last, out)
    print("WROTE", p_last)
    print("[MACRO] source=", macro_source, "regime=", out["regime"], "risk_on=", out["risk_on"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




