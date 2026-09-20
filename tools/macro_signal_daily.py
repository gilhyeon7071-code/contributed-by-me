from __future__ import annotations

import logging
import json
import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

ROOTA = Path(__file__).resolve().parents[1]
LOGS = ROOTA / "2_Logs"
LOGS.mkdir(parents=True, exist_ok=True)




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _now_ymd() -> str:
    return date.today().strftime("%Y%m%d")


def _write_json(p: Path, obj: dict) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _clamp(v: float, lo: float, hi: float) -> float:
    try:
        fv = float(v)
    except Exception:
        return lo
    if math.isnan(fv) or math.isinf(fv):
        return lo
    return max(lo, min(hi, fv))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        fv = float(v)
    except Exception:
        return default
    if math.isnan(fv) or math.isinf(fv):
        return default
    return fv


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
        "mode": "none",
        "fallback_used": False,
        "error": None,
    }
    try:
        import sys

        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_dev" / "kospi_sector"))
        from data.oecd_cli import OECDCLIClient  # type: ignore

        real_err = None
        try:
            oecd = OECDCLIClient(mock=False)
            # [2026-08-21] 실제 소스를 사실대로 기록한다.
            #
            # OECDCLIClient._fetch_cli 는 404 를 **내부에서 삼키고** _mock_cli() 를 반환한다.
            # 예외가 안 나므로 아래 except 로 가지 않고, 호출부가 넘긴 mock=False 만 보고
            # "oecd_cli_real / mode=real / fallback_used=False" 로 기록해 왔다.
            # 실제로 들어온 값은 시드 고정 사인파(_mock_cli: default_rng(42) + sin)다.
            # 클라이언트는 폴백 시 "... use mock fallback ..." 을 로깅하므로 그것을 잡는다.
            # 클라이언트 파일(_dev/)은 건드리지 않는다. 상세: .agent/PLANS.md 2026-08-21 (20)
            import logging as _logging

            _oecd_log = _logging.getLogger(OECDCLIClient.__module__)
            _captured: list = []

            class _OecdFallbackProbe(_logging.Handler):
                def emit(self, record):  # noqa: D102
                    try:
                        _captured.append(str(record.getMessage()))
                    except Exception:
                        pass

            _probe = _OecdFallbackProbe()
            _prev_level = _oecd_log.level
            _oecd_log.addHandler(_probe)
            # 404 폴백은 logger.info 로 남는다. 기본 레벨(WARNING)이면 레코드가 생성되기 전에
            # 걸러져 핸들러에 도달하지 않는다. 잡으려면 레벨을 내려야 한다.
            _oecd_log.setLevel(_logging.INFO)
            try:
                cli_df = oecd.get_cli(start=(datetime.now().strftime("%Y") + "-01"))
            finally:
                _oecd_log.removeHandler(_probe)
                _oecd_log.setLevel(_prev_level)

            _fell_back = any("mock fallback" in m for m in _captured)
            if _fell_back:
                q = "MED"
                src = "oecd_cli_mock_silent_fallback"
                mode = "mock"
                real_err = next((m for m in _captured if "mock fallback" in m), "silent_mock_fallback")
            else:
                q = "HIGH"
                src = "oecd_cli_real"
                mode = "real"
        except Exception as e:
            real_err = f"{type(e).__name__}: {e}"
            oecd = OECDCLIClient(mock=True)
            cli_df = oecd.get_cli(start="2023-01")
            q = "MED"
            src = "oecd_cli_mock"
            mode = "mock"

        if not cli_df.empty:
            out["cli_mom"] = float(cli_df["cli_mom"].iloc[-1])
            out["risk_on_cli"] = bool(cli_df["risk_on"].iloc[-1])
            out["quality"] = q
            out["source"] = src
            out["mode"] = mode
            if mode == "mock":
                out["fallback_used"] = True
                out["error"] = real_err
    except Exception as e:
        _log_print(f"[WARN] oecd_cli unavailable: {type(e).__name__}: {e}")
        out["error"] = f"{type(e).__name__}: {e}"
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
    except KeyError:
        _log_print("[WARN] pykrx KOSPI fetch unavailable -> fallback")
        return None
    except Exception as e:
        _log_print(f"[WARN] pykrx KOSPI fetch failed: {type(e).__name__}: {e}")
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
        _log_print(f"[WARN] local proxy load failed: {type(e).__name__}: {e}")
        return None



def _load_local_proxy_from_krx_clean(lookback_days: int = 320) -> Optional[pd.DataFrame]:
    def _build_proxy_from_files(paths: list[Path]) -> Optional[pd.DataFrame]:
        parts = []
        for p in paths:
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
        z = z[(z["ret1"] > -0.30) & (z["ret1"] < 0.30)]

        g = (
            z.groupby("date", as_index=False)
            .agg(ret1_mean=("ret1", "mean"), ret1_median=("ret1", "median"), ncode=("code", "nunique"))
            .sort_values("date")
        )
        g = g[g["ncode"] >= 500].copy()
        if g.empty:
            return None

        gap = (pd.to_numeric(g["ret1_mean"], errors="coerce") - pd.to_numeric(g["ret1_median"], errors="coerce")).abs()
        g["ret1"] = g["ret1_mean"].where(gap <= 0.05, g["ret1_median"])
        g["ret1"] = pd.to_numeric(g["ret1"], errors="coerce").fillna(0.0).clip(-0.15, 0.15)
        g["close"] = (1.0 + g["ret1"]).cumprod() * 100.0
        return g[["date", "close"]].tail(lookback_days + 20)

    krx_dir = ROOTA / "_krx_manual"
    files_manual = sorted(krx_dir.glob("krx_daily_*_clean.parquet")) if krx_dir.exists() else []
    out = _build_proxy_from_files(files_manual[-120:]) if files_manual else None
    if out is not None and len(out) >= min(120, lookback_days):
        return out

    # If manual source is too short, expand with archive source for rolling-window stability.
    arch_dir = ROOTA / "krx_daily_archive"
    files_arch = sorted(arch_dir.glob("krx_daily_*_clean.parquet")) if arch_dir.exists() else []
    if not files_arch:
        return out
    out2 = _build_proxy_from_files(files_arch[-12:])
    if out2 is None:
        return out
    return out2


def _load_external_equity_proxy(series_id: str = "SP500", lookback_rows: int = 320) -> Optional[pd.DataFrame]:
    p = LOGS / "rate_series_external_all_latest.csv"
    if not p.exists():
        return None
    try:
        df = _read_csv_any(p)
    except Exception as e:
        _log_print(f"[WARN] external equity proxy read failed: {type(e).__name__}: {e}")
        return None
    if df is None or df.empty or "series_id" not in df.columns:
        return None
    x = df[df["series_id"].astype(str).str.upper() == str(series_id).upper()].copy()
    if x.empty:
        return None
    x["date"] = pd.to_datetime(x["date"].map(_as_ymd), format="%Y%m%d", errors="coerce")
    x["close"] = pd.to_numeric(x["value"], errors="coerce")
    x = x.dropna(subset=["date", "close"]).sort_values("date")
    if len(x) < 80:
        return None
    return x[["date", "close"]].tail(lookback_rows)


def _read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _load_rate_series_local(lookback_rows: int = 600) -> Optional[pd.DataFrame]:
    candidates = [
        LOGS / "rate_series_external_latest.csv",
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
        _log_print(f"[WARN] rate series read failed: {type(e).__name__}: {e}")
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
    # Prefer short-term direction when 20d and 60d conflict.
    hawk20 = _clamp(chg20 / 0.50, -1.0, 1.0)
    hawk60 = _clamp(chg60 / 1.00, -1.0, 1.0)
    if (hawk20 * hawk60) < 0:
        hawkish = _clamp(0.75 * hawk20 + 0.25 * hawk60, -1.0, 1.0)
    else:
        hawkish = _clamp(0.60 * hawk20 + 0.40 * hawk60, -1.0, 1.0)

    return {
        "available": True,
        "source": str(x["source"].iloc[-1]) if "source" in x.columns else "rate_series",
        "hawkish_score": float(hawkish),
        "metrics": {
            "rate_level": last,
            "rate_chg_20": float(chg20),
            "rate_chg_60": float(chg60),
            "hawkish_20": float(hawk20),
            "hawkish_60": float(hawk60),
        },
    }
def _infer_hmm_regime(px: pd.DataFrame) -> Dict[str, Any]:
    if px is None or px.empty or "close" not in px.columns:
        return {"available": False, "regime": "UNKNOWN", "confidence": 0.0, "reason": "missing_prices"}

    x = px.sort_values("date").copy()
    close = pd.to_numeric(x["close"], errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna()
    limited_history = len(close) < 80
    if len(close) < 10:
        return {"available": False, "regime": "UNKNOWN", "confidence": 0.0, "reason": "insufficient_history", "rows": int(len(close))}

    ret1 = close.pct_change(1).fillna(0.0)
    ret20 = close.pct_change(20).fillna(0.0)
    rv20 = ret1.rolling(20, min_periods=10).std().fillna(ret1.expanding().std()).fillna(0.0)
    peak120 = close.rolling(120, min_periods=20).max()
    dd120 = (close / (peak120 + 1e-9) - 1.0).fillna(0.0)
    obs = pd.DataFrame({"ret20": ret20, "rv20": rv20, "dd120": dd120}).tail(180)

    states = ["BULL", "SIDEWAYS", "BEAR", "CRASH"]
    profile = {
        "BULL": {"ret20": 0.05, "rv20": 0.012, "dd120": -0.02},
        "SIDEWAYS": {"ret20": 0.00, "rv20": 0.018, "dd120": -0.06},
        "BEAR": {"ret20": -0.05, "rv20": 0.025, "dd120": -0.14},
        "CRASH": {"ret20": -0.12, "rv20": 0.040, "dd120": -0.24},
    }
    scale = {"ret20": 0.08, "rv20": 0.02, "dd120": 0.12}
    stay_bonus = 0.35
    jump_penalty = 0.30

    scores: Dict[str, float] = {s: 0.0 for s in states}
    paths: Dict[str, list[str]] = {s: [s] for s in states}
    for _, row in obs.iterrows():
        emit: Dict[str, float] = {}
        for s in states:
            p = profile[s]
            dist = 0.0
            for k, denom in scale.items():
                v = float(row.get(k, 0.0) or 0.0)
                dist += ((v - float(p[k])) / float(denom)) ** 2
            emit[s] = -dist

        next_scores: Dict[str, float] = {}
        next_paths: Dict[str, list[str]] = {}
        for s in states:
            best_prev = states[0]
            best_score = -10**18
            for prev in states:
                trans = stay_bonus if prev == s else -jump_penalty * abs(states.index(prev) - states.index(s))
                cand = scores[prev] + trans + emit[s]
                if cand > best_score:
                    best_prev = prev
                    best_score = cand
            next_scores[s] = best_score
            next_paths[s] = paths[best_prev] + [s]
        scores, paths = next_scores, next_paths

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else best_score
    confidence = max(0.0, min(1.0, (best_score - second_score) / 8.0))
    if limited_history:
        confidence = min(confidence, 0.25)
    return {
        "available": True,
        "regime": best,
        "confidence": round(float(confidence), 6),
        "limited_history": bool(limited_history),
        "reason": "limited_history" if limited_history else "ok",
        "rows": int(len(obs)),
        "last_metrics": {k: round(float(obs.iloc[-1][k]), 6) for k in obs.columns},
        "path_tail": paths[best][-5:],
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
    has_ma60 = not pd.isna(last.get("ma60"))
    has_dd120 = not pd.isna(last.get("dd120"))
    close = _safe_float(last.get("close"), 0.0)
    ret1 = _safe_float(last.get("ret1"), 0.0)
    prev_ret1 = _safe_float(df["ret1"].iloc[-2], 0.0) if len(df) >= 2 else 0.0
    ret20 = _safe_float(last.get("ret20"), 0.0)
    rv20 = _safe_float(last.get("rv20"), 0.0)
    dd120 = _safe_float(last.get("dd120"), 0.0)
    ma20 = _safe_float(last.get("ma20"), close)
    ma60 = _safe_float(last.get("ma60"), close)

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

    # CRASH: 120일 낙폭 -18% 초과, 또는 연속 하락(-3.5%+전일-1%) AND 이미 -6% 이상 DD
    if dd120 <= -0.18 or (ret1 <= -0.035 and prev_ret1 <= -0.01 and dd120 <= -0.06):
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

    hmm_ctx = _infer_hmm_regime(px)
    hmm_regime = str(hmm_ctx.get("regime") or "UNKNOWN").upper()
    hmm_confidence = float(hmm_ctx.get("confidence", 0.0) or 0.0)
    risk_rank = {"BULL": 0, "NORMAL": 0, "RECOVERY": 0, "SIDEWAYS": 1, "VOLATILE": 2, "RATE_HIKE_FEAR": 2, "BEAR": 3, "CRASH": 4}
    if hmm_confidence >= 0.55 and risk_rank.get(hmm_regime, -1) > risk_rank.get(regime, -1):
        regime = "CRASH" if hmm_regime == "CRASH" else "BEAR"

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
        "samples": int(len(df)),
        "has_ma60": bool(has_ma60),
        "has_dd120": bool(has_dd120),
        "hmm_regime": hmm_regime,
        "hmm_confidence": hmm_confidence,
    }
    for k, v in rate_metrics.items():
        metrics[str(k)] = v

    return {
        "regime": regime,
        "rate_signal": rate_signal,
        "crash_prob": crash_prob,
        "risk_on_mkt": risk_on_mkt,
        "metrics": metrics,
        "hmm_context": hmm_ctx,
    }




def _load_external_macro_features() -> Dict[str, Any]:
    p = LOGS / "macro_feature_external_latest.json"
    if not p.exists():
        return {
            "available": False,
            "source": "none",
            "global_signal": None,
            "exposure_multiplier": None,
            "fx_context": {},
            "freshness_summary": {},
            "indicator_source_mapping": [],
            "series": {},
        }
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        _log_print(f"[WARN] external macro feature read failed: {type(e).__name__}: {e}")
        return {
            "available": False,
            "source": p.name,
            "global_signal": None,
            "exposure_multiplier": None,
            "fx_context": {},
            "freshness_summary": {},
            "indicator_source_mapping": [],
            "series": {},
        }

    sig = obj.get("signals", {}) if isinstance(obj, dict) else {}
    g = sig.get("global", {}) if isinstance(sig, dict) else {}
    fs = obj.get("indicator_freshness_summary", {}) if isinstance(obj, dict) else {}
    mapping = obj.get("indicator_source_mapping", []) if isinstance(obj, dict) else []
    if not isinstance(mapping, list):
        mapping = []
    return {
        "available": True,
        "source": p.name,
        "global_signal": g.get("signal") if isinstance(g, dict) else None,
        "exposure_multiplier": g.get("exposure_multiplier") if isinstance(g, dict) else None,
        "fx_context": obj.get("fx_context") if isinstance(obj.get("fx_context"), dict) else {},
        "freshness_summary": fs if isinstance(fs, dict) else {},
        "indicator_source_mapping": mapping,
        "series": obj.get("series") if isinstance(obj.get("series"), dict) else {},
        "signals": sig if isinstance(sig, dict) else {},
    }


def _load_supply_context() -> Dict[str, Any]:
    p = LOGS / "candidates_latest_data.csv"
    out: Dict[str, Any] = {
        "available": False,
        "source": str(p.name),
        "score": 0.0,
        "n": 0,
        "fields": [],
    }
    if not p.exists():
        return out
    try:
        df = _read_csv_any(p)
    except Exception:
        return out
    if df is None or df.empty:
        return out

    candidates = [
        ("foreign_net_20d", ["foreign_net_20d", "foreign_net"]),
        ("institution_net_20d", ["institution_net_20d", "institution_net"]),
    ]
    scores: list[float] = []
    fields: list[str] = []
    sample_n = 0
    for _, aliases in candidates:
        col = next((c for c in aliases if c in df.columns), None)
        if not col:
            continue
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(s) == 0:
            continue
        pos = float((s > 0).mean())
        neg = float((s < 0).mean())
        scores.append(float(_clamp(pos - neg, -1.0, 1.0)))
        fields.append(str(col))
        sample_n = max(sample_n, int(len(s)))

    if not scores:
        return out

    out["available"] = True
    out["score"] = float(_clamp(sum(scores) / float(len(scores)), -1.0, 1.0))
    out["n"] = int(sample_n)
    out["fields"] = fields
    return out


def _extract_oil_context(ext_macro: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "available": False,
        "wti_chg20_pct": None,
        "brent_chg20_pct": None,
        "oil_shock_score": 0.0,
        "oil_surge_score": 0.0,    # 급등 방향 전용 (인플레·원가 압박)
        "oil_collapse_score": 0.0, # 급락 방향 전용 (수요 붕괴)
        "oil_direction": "STABLE", # SURGE | COLLAPSE | STABLE
    }
    series = ext_macro.get("series") if isinstance(ext_macro, dict) else {}
    if not isinstance(series, dict) or not series:
        return out

    def _pct_from_series(node: Dict[str, Any]) -> Optional[float]:
        latest = _safe_float(node.get("latest_value"), float("nan"))
        chg20 = _safe_float(node.get("chg_20"), float("nan"))
        base = latest - chg20
        if (not math.isfinite(latest)) or (not math.isfinite(chg20)) or abs(base) < 1e-9:
            return None
        return float(chg20 / base)

    wti = series.get("DCOILWTICO") if isinstance(series.get("DCOILWTICO"), dict) else {}
    brent = series.get("DCOILBRENTEU") if isinstance(series.get("DCOILBRENTEU"), dict) else {}
    wti_pct = _pct_from_series(wti)
    brent_pct = _pct_from_series(brent)
    vals = [abs(x) for x in [wti_pct, brent_pct] if x is not None]
    if not vals:
        return out

    out["available"] = True
    out["wti_chg20_pct"] = wti_pct
    out["brent_chg20_pct"] = brent_pct
    out["oil_shock_score"] = float(_clamp(max(vals) / 0.25, 0.0, 1.0))

    # 방향별 분리: 급등(양수) / 급락(음수)
    surge_vals    = [x for x in [wti_pct, brent_pct] if x is not None and x > 0]
    collapse_vals = [abs(x) for x in [wti_pct, brent_pct] if x is not None and x < 0]
    out["oil_surge_score"]    = float(_clamp(max(surge_vals)    / 0.25, 0.0, 1.0)) if surge_vals    else 0.0
    out["oil_collapse_score"] = float(_clamp(max(collapse_vals) / 0.25, 0.0, 1.0)) if collapse_vals else 0.0

    # 방향 판정: 0.25 이상(6.25% 변동)을 방향성 있다고 간주
    if out["oil_surge_score"] >= 0.25:
        out["oil_direction"] = "SURGE"
    elif out["oil_collapse_score"] >= 0.25:
        out["oil_direction"] = "COLLAPSE"
    else:
        out["oil_direction"] = "STABLE"

    return out


def main() -> int:
    today = _now_ymd()

    try:
        if str(ROOTA) not in sys.path:
            sys.path.insert(0, str(ROOTA))
        from holiday_manager import HolidayManager  # type: ignore

        hm = HolidayManager()
        if not hm.explain(today).is_open:
            _log_print(f"[SKIP] market closed: {today}")
            return 0
    except Exception as e:
        _log_print(f"[WARN] holiday_manager unavailable -> continue: {type(e).__name__}: {e}")

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
    if px is None or px.empty or len(px) < 80:
        ext_px = _load_external_equity_proxy("SP500")
        if ext_px is not None and not ext_px.empty:
            px = ext_px
            macro_source = "external_sp500_direct"
            q_macro = "MED"
    if px is None or px.empty or len(px) < 80:
        ext_px = _load_external_equity_proxy("NASDAQCOM")
        if ext_px is not None and not ext_px.empty:
            px = ext_px
            macro_source = "external_nasdaq_direct"
            q_macro = "MED"

    rate_df = _load_rate_series_local(lookback_rows=600)
    rate_ctx = _compute_rate_context(rate_df)
    macro = _compute_macro_from_prices(px if px is not None else pd.DataFrame(), rate_ctx=rate_ctx)
    ext_macro = _load_external_macro_features()
    supply_ctx = _load_supply_context()
    oil_ctx = _extract_oil_context(ext_macro)

    risk_on_cli = cli.get("risk_on_cli")
    risk_on_mkt = bool(macro.get("risk_on_mkt", False))
    risk_on = bool(risk_on_mkt and risk_on_cli) if (risk_on_cli is not None) else bool(risk_on_mkt)

    ext_sig = str(ext_macro.get("global_signal") or "").upper()
    if ext_sig == "RISK_OFF":
        risk_on = False
    ext_signals = ext_macro.get("signals") if isinstance(ext_macro.get("signals"), dict) else {}
    ext_us = ext_signals.get("us") if isinstance(ext_signals.get("us"), dict) else {}
    ext_us_raw = ext_us.get("raw") if isinstance(ext_us.get("raw"), dict) else {}
    ext_sp500_declining = bool(ext_us_raw.get("sp500_declining", False))
    ext_nasdaq_declining = bool(ext_us_raw.get("nasdaq_declining", False))
    if ext_sp500_declining or ext_nasdaq_declining:
        risk_on = False

    # Freshness guard: critical daily failures close immediately; broad stale/unknown degradation also closes.
    freshness = ext_macro.get("freshness_summary") if isinstance(ext_macro.get("freshness_summary"), dict) else {}
    fresh_ok = int(freshness.get("ok") or 0)
    fresh_stale = int(freshness.get("stale") or 0)
    fresh_unknown = int(freshness.get("unknown") or 0)
    fresh_total = int(freshness.get("total") or 0)
    indicator_mapping = ext_macro.get("indicator_source_mapping") if isinstance(ext_macro.get("indicator_source_mapping"), list) else []
    critical_ids = {"VIXCLS", "BAMLH0A0HYM2"}
    critical_rows = [r for r in indicator_mapping if str((r or {}).get("series_id") or "") in critical_ids]
    critical_total = int(len(critical_rows))
    critical_bad = int(sum(1 for r in critical_rows if str((r or {}).get("freshness") or "").upper() in {"STALE", "UNKNOWN"}))
    critical_details = [
        {
            "series_id": str((r or {}).get("series_id") or ""),
            "freshness": str((r or {}).get("freshness") or ""),
            "rows": int((r or {}).get("rows") or 0),
            "latest_date": (r or {}).get("latest_date"),
            "age_days": (r or {}).get("age_days"),
        }
        for r in critical_rows
    ]
    freshness_bad_total = int(fresh_stale + fresh_unknown)
    freshness_ratio_threshold = max(1, int(round(fresh_total * 0.6))) if fresh_total > 0 else 0
    freshness_ratio_guard = bool(fresh_total > 0 and freshness_bad_total >= freshness_ratio_threshold)
    if critical_bad > 0:
        freshness_guard = True
        freshness_guard_mode = "critical_daily"
    elif freshness_ratio_guard:
        freshness_guard = True
        freshness_guard_mode = "stale_unknown_ratio"
    else:
        freshness_guard = False
        freshness_guard_mode = "fresh"
    if freshness_guard:
        risk_on = False

    supply_pressure_block = False
    supply_score = float(supply_ctx.get("score", 0.0) or 0.0) if bool(supply_ctx.get("available")) else None
    macro_regime_now = str(macro.get("regime") or "UNKNOWN").upper()
    if (
        supply_score is not None
        and int(supply_ctx.get("n") or 0) >= 3
        and supply_score <= -0.50
        and macro_regime_now in {"CRASH", "RATE_HIKE_FEAR", "VOLATILE"}
    ):
        risk_on = False
        supply_pressure_block = True

    quality = "LOW"
    if cli.get("quality") == "HIGH" and q_macro == "HIGH":
        quality = "HIGH"
    elif (cli.get("quality") in {"HIGH", "MED"}) and (q_macro in {"HIGH", "MED"}):
        quality = "MED"
    if freshness_guard:
        # Do not show high confidence when indicator freshness is widely degraded.
        quality = "LOW"
    market_metrics = macro.get("metrics", {}) if isinstance(macro.get("metrics"), dict) else {}
    market_metrics["oil_shock_score"]    = float(oil_ctx.get("oil_shock_score",    0.0) or 0.0)
    market_metrics["oil_surge_score"]    = float(oil_ctx.get("oil_surge_score",    0.0) or 0.0)
    market_metrics["oil_collapse_score"] = float(oil_ctx.get("oil_collapse_score", 0.0) or 0.0)
    market_metrics["oil_direction"]      = str(oil_ctx.get("oil_direction", "STABLE") or "STABLE")
    market_metrics["wti_chg20_pct"]      = oil_ctx.get("wti_chg20_pct")
    market_metrics["brent_chg20_pct"]    = oil_ctx.get("brent_chg20_pct")
    market_metrics["supply_score"] = supply_score
    market_metrics["supply_n"] = int(supply_ctx.get("n") or 0)
    if not bool(market_metrics.get("has_ma60", False)) or not bool(market_metrics.get("has_dd120", False)):
        quality = "LOW"

    reasons = []
    reasons.append(f"macro_source={macro_source}")
    reasons.append(f"regime={macro.get('regime', 'UNKNOWN')}")
    reasons.append(f"risk_on_mkt={risk_on_mkt}")
    if risk_on_cli is not None:
        reasons.append(f"risk_on_cli={bool(risk_on_cli)}")
    if ext_sig:
        reasons.append(f"external_macro_signal={ext_sig}")
    if ext_sp500_declining:
        reasons.append("sp500_direct_declining")
    if ext_nasdaq_declining:
        reasons.append("nasdaq_direct_declining")
    if freshness_guard:
        reasons.append("freshness_guard_active")
    if supply_pressure_block:
        reasons.append("supply_pressure_negative")
    if bool(oil_ctx.get("available")) and float(oil_ctx.get("oil_shock_score", 0.0) or 0.0) >= 0.70:
        reasons.append("oil_shock_high")
    if not bool(market_metrics.get("has_ma60", False)) or not bool(market_metrics.get("has_dd120", False)):
        reasons.append("market_history_insufficient")

    out = {
        "as_of_ymd": today,
        "regime": macro.get("regime", "UNKNOWN"),
        "cli_mom": cli.get("cli_mom"),
        "rate_signal": float(macro.get("rate_signal", 0.0) or 0.0),
        "crash_prob": float(macro.get("crash_prob", 0.0) or 0.0),
        "risk_on": bool(risk_on),
        "quality": quality,
        # Backward-compatible single source/reasons for downstream diagnostics.
        "source": macro_source,
        "reasons": reasons,
        "sources": {
            "cli": cli.get("source"),
            "macro": macro_source,
            "rate": rate_ctx.get("source"),
            "external_macro": ext_macro.get("source"),
        },
        "rate_context": rate_ctx,
        "cli_context": {
            "mode": cli.get("mode"),
            "fallback_used": bool(cli.get("fallback_used", False)),
            "error": cli.get("error"),
        },
        "supply_context": supply_ctx,
        "fx_context": ext_macro.get("fx_context") if isinstance(ext_macro.get("fx_context"), dict) else {},
        "oil_context": oil_ctx,
        "hmm_context": macro.get("hmm_context", {}),
        "market_metrics": market_metrics,
        "external_macro": ext_macro,
        "freshness_guard": {
            "active": freshness_guard,
            "mode": freshness_guard_mode,
            "critical_total": critical_total,
            "critical_bad": critical_bad,
            "critical_details": critical_details,
            "ok": fresh_ok,
            "stale": fresh_stale,
            "unknown": fresh_unknown,
            "total": fresh_total,
            "bad_total": freshness_bad_total,
            "ratio_threshold": freshness_ratio_threshold,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    out = _clean_json_obj(out)

    p_last = LOGS / "macro_signal_latest.json"
    p_day = LOGS / f"macro_signal_{today}.json"
    _write_json(p_day, out)
    _write_json(p_last, out)
    _log_print("WROTE", p_last)
    _log_print("[MACRO] source=", macro_source, "regime=", out["regime"], "risk_on=", out["risk_on"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


