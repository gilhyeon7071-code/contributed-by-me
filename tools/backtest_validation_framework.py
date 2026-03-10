
from __future__ import annotations

import argparse
import importlib
import importlib.util
import itertools
import json
import math
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


@dataclass
class CostModel:
    commission_bps: float = 2.0
    slippage_bps: float = 3.0
    spread_bps: float = 2.0

    def roundtrip_bps(self) -> float:
        return 2.0 * self.commission_bps + 2.0 * self.slippage_bps + self.spread_bps


@dataclass
class BacktestResult:
    returns: pd.Series
    equity: pd.Series
    trades: pd.DataFrame
    metrics: Dict[str, float] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    name: str
    passed: bool
    summary: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineReport:
    passed: bool
    gate_results: List[ValidationResult]
    artifacts: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "gate_results": [asdict(x) for x in self.gate_results],
            "artifacts": self.artifacts,
        }


def make_equity_curve(returns: pd.Series, initial_capital: float = 1.0) -> pd.Series:
    return initial_capital * (1.0 + returns.fillna(0.0)).cumprod()


def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return np.nan
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def sharpe_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    r = returns.dropna()
    if len(r) < 2:
        return np.nan
    vol = float(r.std(ddof=1))
    if vol == 0.0 or not math.isfinite(vol):
        return np.nan
    return float(r.mean() / vol * np.sqrt(periods_per_year))


def basic_metrics(returns: pd.Series, equity: pd.Series) -> Dict[str, float]:
    rr = returns.dropna()
    return {
        "annual_return": annualized_return(returns),
        "sharpe": sharpe_ratio(returns),
        "max_drawdown": max_drawdown(equity),
        "win_rate": float((rr > 0).mean()) if len(rr) else np.nan,
        "n_obs": int(len(rr)),
    }


def annualized_return(returns: pd.Series, periods_per_year: int = 252) -> float:
    rr = returns.dropna()
    if len(rr) == 0:
        return np.nan
    total = float((1.0 + rr).prod())
    years = float(len(rr)) / float(periods_per_year)
    if years <= 0:
        return np.nan
    return total ** (1.0 / years) - 1.0


def _safe_pct_change(s: pd.Series) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce").pct_change()
    return out.replace([np.inf, -np.inf], np.nan)


def _safe_float(v: Any, default: float = np.nan) -> float:
    try:
        out = float(v)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _parse_json_arg(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    text = raw.strip()
    if text.startswith("{"):
        obj = json.loads(text)
    else:
        obj = json.loads(Path(text).read_text(encoding="utf-8-sig"))
    if not isinstance(obj, dict):
        raise ValueError("JSON arg must be object")
    return obj




def _parse_windows_arg(raw: str) -> Optional[List[Tuple[str, str, str]]]:
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("["):
        obj = json.loads(text)
    else:
        obj = json.loads(Path(text).read_text(encoding="utf-8-sig"))

    out: List[Tuple[str, str, str]] = []
    if not isinstance(obj, list):
        raise ValueError("scenario windows json must be list")

    for row in obj:
        if isinstance(row, dict):
            name = str(row.get("name", "scenario"))
            start = str(row.get("start", ""))
            end = str(row.get("end", ""))
        elif isinstance(row, (list, tuple)) and len(row) >= 3:
            name = str(row[0])
            start = str(row[1])
            end = str(row[2])
        else:
            continue
        if start and end:
            out.append((name, start, end))
    return out or None
_ALIAS_MAP: Dict[str, Sequence[str]] = {
    "date": ["date", "datetime", "timestamp", "dt", "trade_date", "trd_date"],
    "open": ["open", "o", "Open"],
    "high": ["high", "h", "High"],
    "low": ["low", "l", "Low"],
    "close": ["close", "c", "Close", "adj_close", "price"],
    "volume": ["volume", "vol", "Volume"],
}


def _normalize_column_mapping(df: pd.DataFrame, column_map: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, str]]:
    out = df.copy()
    used: Dict[str, str] = {}
    mapping = column_map or {}

    direct = {}
    for canonical, src in mapping.items():
        if isinstance(src, str) and src in out.columns and canonical != src:
            direct[src] = canonical
            used[canonical] = src
        elif canonical in out.columns:
            used[canonical] = canonical

    if direct:
        out = out.rename(columns=direct)

    for canonical, aliases in _ALIAS_MAP.items():
        if canonical in out.columns:
            used.setdefault(canonical, canonical)
            continue
        found = next((a for a in aliases if a in out.columns), None)
        if found is not None:
            out = out.rename(columns={found: canonical})
            used[canonical] = found

    return out, used


def load_market_csv(path: Path, date_col: str, column_map: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    raw = pd.read_csv(path)
    df, used = _normalize_column_mapping(raw, column_map)

    idx_col = date_col
    if idx_col not in df.columns and "date" in df.columns:
        idx_col = "date"
    if idx_col not in df.columns:
        raise ValueError(f"date column not found: {date_col}")

    df[idx_col] = pd.to_datetime(df[idx_col], errors="coerce")
    df = df.dropna(subset=[idx_col]).set_index(idx_col).sort_index()

    if "close" not in df.columns:
        raise ValueError("market csv missing close after column mapping")

    meta = {
        "date_col_used": idx_col,
        "used_column_map": used,
        "source_rows": int(len(raw)),
        "output_rows": int(len(df)),
    }
    return df, meta



def load_inflation_csv(path: Path) -> pd.Series:
    raw = pd.read_csv(path)
    cols = {str(c).lower(): str(c) for c in raw.columns}

    if "date" in cols:
        dcol = cols["date"]
        dt_idx = pd.to_datetime(raw[dcol], errors="coerce")
        year = pd.Series(dt_idx).dt.year
    elif "year" in cols:
        year = pd.to_numeric(raw[cols["year"]], errors="coerce")
    else:
        raise ValueError("inflation csv must include date or year column")

    if "inflation_rate" in cols:
        rate = pd.to_numeric(raw[cols["inflation_rate"]], errors="coerce")
    elif "cpi" in cols:
        cpi = pd.to_numeric(raw[cols["cpi"]], errors="coerce")
        rate = cpi.pct_change()
    elif "cpi_index" in cols:
        cpi = pd.to_numeric(raw[cols["cpi_index"]], errors="coerce")
        rate = cpi.pct_change()
    elif "value" in cols:
        val = pd.to_numeric(raw[cols["value"]], errors="coerce")
        if val.abs().median(skipna=True) > 3.0:
            rate = val / 100.0
        else:
            rate = val
    else:
        raise ValueError("inflation csv must include one of inflation_rate/cpi/cpi_index/value")

    y = pd.DataFrame({"year": year, "infl": rate}).dropna()
    y["year"] = y["year"].astype(int)
    y = y.groupby("year", as_index=True)["infl"].last().sort_index()
    return y.replace([np.inf, -np.inf], np.nan).dropna()


def fetch_worldbank_inflation_series(country_code: str = "KR") -> Tuple[pd.Series, Dict[str, Any]]:
    indicator = "FP.CPI.TOTL.ZG"
    c = (country_code or "KR").upper()
    url = (
        "https://api.worldbank.org/v2/country/"
        + urllib.parse.quote(c)
        + "/indicator/"
        + indicator
        + "?format=json&per_page=200"
    )
    with urllib.request.urlopen(url, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise RuntimeError("unexpected worldbank response")

    rows = payload[1]
    out: Dict[int, float] = {}
    for r in rows:
        y = r.get("date")
        v = r.get("value")
        yy = _safe_float(y, np.nan)
        vv = _safe_float(v, np.nan)
        if pd.notna(yy) and pd.notna(vv):
            out[int(yy)] = float(vv) / 100.0

    if not out:
        raise RuntimeError("worldbank inflation empty")

    s = pd.Series(out).sort_index()
    meta = {
        "source": "worldbank",
        "country_code": c,
        "indicator": indicator,
        "rows": int(len(s)),
        "year_min": int(s.index.min()),
        "year_max": int(s.index.max()),
    }
    return s, meta
def ensure_signal_schema(signal_df: pd.DataFrame, market_index: Optional[pd.DatetimeIndex] = None) -> pd.DataFrame:
    if isinstance(signal_df, pd.Series):
        signal_df = pd.DataFrame({"signal": signal_df})
    if not isinstance(signal_df, pd.DataFrame):
        raise TypeError("strategy output must be DataFrame/Series")

    out = signal_df.copy()
    if "signal" not in out.columns:
        if "position" in out.columns:
            out["signal"] = pd.to_numeric(out["position"], errors="coerce").shift(-1)
        elif {"entry", "exit"}.issubset(out.columns):
            pos = np.zeros(len(out), dtype=float)
            cur = 0.0
            for i, (_, row) in enumerate(out.iterrows()):
                if bool(row.get("exit", False)):
                    cur = 0.0
                elif bool(row.get("entry", False)):
                    cur = 1.0
                pos[i] = cur
            out["position"] = pd.Series(pos, index=out.index)
            out["signal"] = out["position"].shift(-1)
        else:
            numeric = [c for c in out.columns if pd.api.types.is_numeric_dtype(out[c])]
            if len(numeric) == 1:
                out["signal"] = out[numeric[0]]
            else:
                raise ValueError("signal column missing and no adapter matched")

    out["signal"] = pd.to_numeric(out["signal"], errors="coerce").fillna(0.0)
    if "position" in out.columns:
        out["position"] = pd.to_numeric(out["position"], errors="coerce").fillna(0.0)

    if market_index is not None:
        out = out.reindex(market_index)
        out["signal"] = out["signal"].fillna(0.0)
        if "position" in out.columns:
            out["position"] = out["position"].fillna(0.0)

    return out


def _load_callable(spec: str, fallback: Callable[..., Any]) -> Tuple[Callable[..., Any], str]:
    spec = (spec or "").strip()
    if not spec:
        return fallback, "builtin"
    if ":" not in spec:
        raise ValueError("spec format: module:function or path.py:function")

    source, fn_name = spec.rsplit(":", 1)
    source = source.strip()
    fn_name = fn_name.strip()

    if source.lower().endswith(".py"):
        p = Path(source)
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.exists():
            raise FileNotFoundError(f"callable file not found: {p}")
        mod_name = f"btval_ext_{p.stem}"
        spec_obj = importlib.util.spec_from_file_location(mod_name, str(p))
        if spec_obj is None or spec_obj.loader is None:
            raise ImportError(f"failed to load module from {p}")
        mod = importlib.util.module_from_spec(spec_obj)
        spec_obj.loader.exec_module(mod)
    else:
        mod = importlib.import_module(source)

    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise AttributeError(f"callable not found: {spec}")
    return fn, spec


def _call_strategy_fn(fn: Callable[..., Any], market_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    attempts = [
        lambda: fn(market_df, params),
        lambda: fn(market_df=market_df, params=params),
        lambda: fn(df=market_df, params=params),
    ]
    last_err = None
    for run in attempts:
        try:
            return ensure_signal_schema(run(), market_df.index)
        except TypeError as e:
            last_err = e
    if last_err is not None:
        raise last_err
    raise RuntimeError("strategy call failed")


def _call_backtest_fn(fn: Callable[..., Any], market_df: pd.DataFrame, signal_df: pd.DataFrame, params: Dict[str, Any], cost_model: CostModel) -> BacktestResult:
    attempts = [
        lambda: fn(market_df, signal_df, params, cost_model),
        lambda: fn(market_df=market_df, signal_df=signal_df, params=params, cost_model=cost_model),
    ]
    last_err = None
    for run in attempts:
        try:
            raw = run()
            if isinstance(raw, BacktestResult):
                return raw
            if isinstance(raw, dict):
                returns = pd.Series(raw.get("returns"), dtype=float)
                equity = pd.Series(raw.get("equity"), dtype=float)
                if len(equity) == 0 and len(returns):
                    equity = make_equity_curve(returns)
                trades = raw.get("trades", pd.DataFrame())
                if not isinstance(trades, pd.DataFrame):
                    trades = pd.DataFrame(trades)
                return BacktestResult(returns, equity, trades, raw.get("metrics", basic_metrics(returns, equity)), raw.get("meta", {}))
            raise TypeError("custom backtest must return BacktestResult or dict")
        except TypeError as e:
            last_err = e
    if last_err is not None:
        raise last_err
    raise RuntimeError("backtest call failed")


def reference_backtest(market_df: pd.DataFrame, signal_df: pd.DataFrame, params: Dict[str, Any], cost_model: CostModel) -> BacktestResult:
    if "close" not in market_df.columns:
        raise ValueError("market_df must contain close")
    sig = ensure_signal_schema(signal_df, market_df.index)

    out = pd.DataFrame(index=market_df.index)
    out["ret_cc"] = _safe_pct_change(market_df["close"]).fillna(0.0)
    out["signal"] = sig["signal"].reindex(out.index).fillna(0.0)
    if "position" in sig.columns:
        out["position"] = sig["position"].reindex(out.index).fillna(0.0)
    else:
        out["position"] = out["signal"].shift(1).fillna(0.0)

    out["turnover"] = out["position"].diff().abs().fillna(out["position"].abs())

    cost_rate = (cost_model.commission_bps + cost_model.slippage_bps + cost_model.spread_bps / 2.0) / 10000.0
    out["gross_return"] = out["position"] * out["ret_cc"]
    out["cost"] = out["turnover"] * cost_rate
    out["net_return"] = out["gross_return"] - out["cost"]

    equity = make_equity_curve(out["net_return"])
    trades = pd.DataFrame(
        {
            "timestamp": out.index[out["turnover"] > 0],
            "return": out.loc[out["turnover"] > 0, "net_return"].values,
            "cost": out.loc[out["turnover"] > 0, "cost"].values,
        }
    )

    return BacktestResult(out["net_return"], equity, trades, basic_metrics(out["net_return"], equity), {"engine": "reference_backtest"})


class BiasAudit:
    def run(self, market_df: pd.DataFrame, signal_df: pd.DataFrame, threshold_corr: float = 0.2) -> List[ValidationResult]:
        sig = ensure_signal_schema(signal_df, market_df.index)
        future_ret = _safe_pct_change(market_df["close"]).shift(-1)
        corr = sig["signal"].corr(future_ret)
        lookahead_ok = not (pd.notna(corr) and abs(float(corr)) > threshold_corr)

        if "position" in sig.columns:
            expected = sig["signal"].shift(1).fillna(0.0)
            mismatch = float((sig["position"].fillna(0.0) != expected).mean())
            lag_ok = mismatch <= 0.10
        else:
            mismatch = np.nan
            lag_ok = False

        return [
            ValidationResult("look_ahead_proxy", lookahead_ok, "signal-future corr audit", {"corr": corr, "threshold": threshold_corr}),
            ValidationResult("position_lag", lag_ok, "position follows lagged signal", {"mismatch_ratio": mismatch}),
        ]



class DataQualityValidator:
    def run(self, market_df: pd.DataFrame) -> List[ValidationResult]:
        out: List[ValidationResult] = []
        close = pd.to_numeric(market_df.get("close"), errors="coerce")

        nonpos = int((close <= 0).sum()) if close is not None else 0
        close_min = float(close.min()) if close is not None and len(close.dropna()) else np.nan
        out.append(
            ValidationResult(
                "data_close_integrity",
                nonpos == 0,
                "close value integrity",
                {
                    "nonpositive_close_count": nonpos,
                    "close_min": close_min,
                },
            )
        )

        ret = close.pct_change() if close is not None else pd.Series(dtype=float)
        rv = ret.to_numpy(dtype=float)
        nonfinite = int((~np.isfinite(rv)).sum()) if len(rv) else 0
        excess_nonfinite = max(nonfinite - 1, 0)
        out.append(
            ValidationResult(
                "data_return_finite",
                excess_nonfinite == 0,
                "return finite check",
                {
                    "nonfinite_return_count": nonfinite,
                    "excess_nonfinite_count": excess_nonfinite,
                },
            )
        )

        monotonic = bool(market_df.index.is_monotonic_increasing)
        dup_count = int(market_df.index.duplicated().sum())
        out.append(
            ValidationResult(
                "data_time_index_integrity",
                monotonic and dup_count == 0,
                "time index integrity",
                {
                    "index_monotonic": monotonic,
                    "duplicate_index_count": dup_count,
                },
            )
        )
        return out
class WalkForwardValidator:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn

    def run(self, market_df: pd.DataFrame, base_params: Dict[str, Any], param_grid: Sequence[Dict[str, Any]], cost_model: CostModel, train_size: int, test_size: int) -> Tuple[ValidationResult, pd.DataFrame]:
        if len(market_df) < train_size + test_size:
            return ValidationResult("walk_forward", False, "insufficient data", {"len": len(market_df)}), pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        start = 0
        fold = 0
        while start + train_size + test_size <= len(market_df):
            train_df = market_df.iloc[start : start + train_size]
            test_df = market_df.iloc[start + train_size : start + train_size + test_size]

            best_score = -np.inf
            best_params = None
            best_is_bt = None
            for p in param_grid:
                params = {**base_params, **p}
                bt_is = self.backtest_fn(train_df, self.strategy_fn(train_df, params), params, cost_model)
                sc = _safe_float(bt_is.metrics.get("sharpe", np.nan), np.nan)
                if pd.isna(sc):
                    sc = _safe_float(annualized_return(bt_is.returns), np.nan)
                if pd.notna(sc) and sc > best_score:
                    best_score = sc
                    best_params = params
                    best_is_bt = bt_is

            if best_params is None or best_is_bt is None:
                rows.append({"fold": fold, "is_sharpe": np.nan, "oos_sharpe": np.nan, "wfe": -999.0, "wfe_valid": False, "reason": "no_best_params"})
            else:
                joined_df = pd.concat([train_df, test_df])
                joined_sig = ensure_signal_schema(self.strategy_fn(joined_df, best_params), joined_df.index)
                oos_sig = joined_sig.reindex(test_df.index).fillna(0.0)
                bt_oos = self.backtest_fn(test_df, oos_sig, best_params, cost_model)
                is_s = _safe_float(best_is_bt.metrics.get("sharpe", np.nan), np.nan)
                if pd.isna(is_s):
                    is_s = _safe_float(annualized_return(best_is_bt.returns), np.nan)
                oos_s = _safe_float(bt_oos.metrics.get("sharpe", np.nan), np.nan)
                if pd.isna(oos_s):
                    oos_s = _safe_float(annualized_return(bt_oos.returns), np.nan)

                min_abs_is = 0.25
                if pd.isna(is_s) or pd.isna(oos_s) or abs(is_s) < min_abs_is:
                    wfe = -999.0
                    wfe_valid = False
                    reason = "invalid_is_or_oos_or_small_is"
                else:
                    wfe = (oos_s / abs(is_s)) * 100.0
                    wfe_valid = True
                    reason = "ok"

                rows.append(
                    {
                        "fold": fold,
                        "is_sharpe": is_s,
                        "oos_sharpe": oos_s,
                        "wfe": float(wfe),
                        "wfe_valid": bool(wfe_valid),
                        "reason": reason,
                        "best_params": best_params,
                    }
                )

            fold += 1
            start += test_size

        wf = pd.DataFrame(rows)
        if len(wf) == 0:
            med = -999.0
            valid_folds = 0
        else:
            valid = wf[wf["wfe_valid"] == True] if "wfe_valid" in wf.columns else pd.DataFrame()
            valid_folds = int(len(valid))
            med = float(valid["wfe"].median()) if valid_folds > 0 else -999.0

        gate = ValidationResult(
            "walk_forward",
            bool(valid_folds >= 1 and med >= 50.0),
            "median WFE >= 50",
            {"median_wfe": med, "n_folds": int(len(wf)), "valid_folds": valid_folds, "min_abs_is_sharpe": 0.25},
        )
        return gate, wf
class MonteCarloValidator:
    def run(self, bt: BacktestResult, max_allowed_mc95_mdd: float = -0.30, n_sim: int = 2500) -> Tuple[ValidationResult, Dict[str, Any]]:
        r = bt.returns.dropna().to_numpy(dtype=float)
        if len(r) == 0:
            return ValidationResult("monte_carlo", False, "no returns", {}), {}

        mdds: List[float] = []
        finals: List[float] = []
        for _ in range(n_sim):
            sampled = np.random.choice(r, size=len(r), replace=True)
            eq = np.cumprod(1.0 + sampled)
            peak = np.maximum.accumulate(eq)
            dd = eq / peak - 1.0
            mdds.append(float(dd.min()))
            finals.append(float(eq[-1] - 1.0))

        mc95 = float(np.percentile(mdds, 95))
        art = {
            "mdd_pct": {str(k): float(v) for k, v in zip([50, 90, 95, 99], np.percentile(mdds, [50, 90, 95, 99]))},
            "final_pct": {str(k): float(v) for k, v in zip([50, 90, 95, 99], np.percentile(finals, [50, 90, 95, 99]))},
        }
        gate = ValidationResult("monte_carlo", bool(mc95 >= max_allowed_mc95_mdd), "MC 95pct MDD check", {"mc95_mdd": mc95, "limit": max_allowed_mc95_mdd})
        return gate, art


class CPCVValidator:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn

    def run(self, market_df: pd.DataFrame, base_params: Dict[str, Any], param_grid: Sequence[Dict[str, Any]], cost_model: CostModel, n_groups: int = 8, k_test: int = 2, purge_bars: int = 2, max_splits: int = 40) -> Tuple[ValidationResult, pd.DataFrame]:
        n = len(market_df)
        if n < 200:
            return ValidationResult("cpcv_pbo", False, "insufficient data", {"len": n}), pd.DataFrame()

        n_groups = max(3, min(int(n_groups), n))
        idx = np.arange(n)
        buckets = np.array_split(idx, n_groups)

        combos = list(itertools.combinations(range(n_groups), max(1, min(k_test, n_groups - 1))))
        if len(combos) > max_splits:
            rng = np.random.default_rng(42)
            picks = rng.choice(len(combos), size=max_splits, replace=False)
            combos = [combos[i] for i in picks]

        rows: List[Dict[str, Any]] = []
        for i, test_groups in enumerate(combos):
            test_mask = np.zeros(n, dtype=bool)
            for g in test_groups:
                test_mask[buckets[g]] = True
            train_mask = ~test_mask

            if purge_bars > 0:
                tix = np.where(test_mask)[0]
                for t in tix:
                    lo = max(0, t - purge_bars)
                    hi = min(n, t + purge_bars + 1)
                    train_mask[lo:hi] = False

            if train_mask.sum() < 80 or test_mask.sum() < 20:
                continue

            train_df = market_df.iloc[np.where(train_mask)[0]]
            test_df = market_df.iloc[np.where(test_mask)[0]]

            best_score = -np.inf
            best_params = None
            for p in param_grid:
                params = {**base_params, **p}
                bt_is = self.backtest_fn(train_df, self.strategy_fn(train_df, params), params, cost_model)
                sc = _safe_float(bt_is.metrics.get("sharpe", np.nan), np.nan)
                if pd.isna(sc):
                    sc = _safe_float(annualized_return(bt_is.returns), np.nan)
                if pd.notna(sc) and sc > best_score:
                    best_score = sc
                    best_params = params

            if best_params is None:
                continue

            bt_oos = self.backtest_fn(test_df, self.strategy_fn(test_df, best_params), best_params, cost_model)
            oos_sharpe = _safe_float(bt_oos.metrics.get("sharpe", np.nan), np.nan)
            rows.append({"split": i, "test_groups": list(test_groups), "is_best_sharpe": float(best_score), "oos_sharpe": oos_sharpe})

        df = pd.DataFrame(rows)
        if len(df) == 0:
            return ValidationResult("cpcv_pbo", False, "no valid split", {"n_splits": 0}), df

        pbo_approx = float((df["oos_sharpe"] <= 0).mean())
        med_oos = float(df["oos_sharpe"].median())
        passed = bool(pbo_approx <= 0.50 and med_oos >= 0.0)

        gate = ValidationResult("cpcv_pbo", passed, "PBO approx <= 0.50 and median OOS Sharpe >= 0", {"pbo_approx": pbo_approx, "median_oos_sharpe": med_oos, "n_splits": int(len(df))})
        return gate, df



class StrategyParameterValidator:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn

    def run(
        self,
        market_df: pd.DataFrame,
        params: Dict[str, Any],
        param_grid: Sequence[Dict[str, Any]],
        cost_model: CostModel,
    ) -> Tuple[ValidationResult, Dict[str, Any]]:
        domain_ok = True
        boundary_hits = 0
        checked_keys = 0

        for k, v in params.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                continue
            vals = [x.get(k) for x in param_grid if isinstance(x.get(k), (int, float)) and not isinstance(x.get(k), bool)]
            if len(vals) == 0:
                continue
            checked_keys += 1
            vmin = float(min(vals))
            vmax = float(max(vals))
            vv = float(v)
            if vv < vmin or vv > vmax:
                domain_ok = False
            if vv == vmin or vv == vmax:
                boundary_hits += 1

        boundary_ratio = (boundary_hits / checked_keys) if checked_keys > 0 else 0.0

        rows: List[Dict[str, Any]] = []
        for p in param_grid:
            bt = self.backtest_fn(market_df, self.strategy_fn(market_df, p), p, cost_model)
            rows.append({"params": p, "sharpe": _safe_float(bt.metrics.get("sharpe", np.nan), np.nan)})
        eval_df = pd.DataFrame(rows)

        best_sharpe = float(eval_df["sharpe"].max()) if len(eval_df) else np.nan
        q75 = float(eval_df["sharpe"].quantile(0.75)) if len(eval_df) else np.nan
        median_top = float(eval_df.loc[eval_df["sharpe"] >= q75, "sharpe"].median()) if len(eval_df) else np.nan
        robust_ratio = (median_top / best_sharpe) if (pd.notna(best_sharpe) and best_sharpe != 0 and pd.notna(median_top)) else np.nan

        if any((p == params) for p in param_grid):
            base_rows = eval_df[eval_df["params"].apply(lambda x: x == params)]
            base_sharpe = float(base_rows["sharpe"].iloc[0]) if len(base_rows) else np.nan
        else:
            bt_base = self.backtest_fn(market_df, self.strategy_fn(market_df, params), params, cost_model)
            base_sharpe = _safe_float(bt_base.metrics.get("sharpe", np.nan), np.nan)

        passed = bool(
            domain_ok
            and boundary_ratio <= 0.50
            and pd.notna(robust_ratio)
            and robust_ratio >= 0.60
        )

        gate = ValidationResult(
            "strategy_parameter_validation",
            passed,
            "parameter domain/robustness validation",
            {
                "domain_ok": domain_ok,
                "checked_params": int(checked_keys),
                "boundary_ratio": float(boundary_ratio),
                "base_sharpe": base_sharpe,
                "best_sharpe": best_sharpe,
                "robust_ratio": robust_ratio,
                "n_grid": int(len(eval_df)),
            },
        )
        return gate, {"grid_eval": eval_df.to_dict(orient="records")}


class MarketRegimeValidator:
    def run(self, market_df: pd.DataFrame, bt: BacktestResult) -> Tuple[ValidationResult, Dict[str, Any]]:
        if "close" not in market_df.columns:
            return ValidationResult(
                "market_regime_response",
                False,
                "close missing for regime analysis",
                {"valid_regimes": 0},
            ), {}

        close = pd.to_numeric(market_df["close"], errors="coerce")
        mkt_ret = close.pct_change()
        sma50 = close.rolling(50).mean()
        trend = np.where(close >= sma50, "BULL", "BEAR")

        vol20 = mkt_ret.rolling(20).std()
        vol_med = float(vol20.median()) if len(vol20.dropna()) else np.nan
        vol_tag = np.where(vol20 >= vol_med, "HIGHVOL", "LOWVOL")

        regime = pd.Series(trend, index=market_df.index).astype(str) + "_" + pd.Series(vol_tag, index=market_df.index).astype(str)
        aligned = pd.DataFrame({"ret": bt.returns, "regime": regime}).dropna()

        rows: List[Dict[str, Any]] = []
        for reg, part in aligned.groupby("regime"):
            if len(part) == 0:
                continue
            eq = make_equity_curve(part["ret"])
            m = basic_metrics(part["ret"], eq)
            rows.append(
                {
                    "regime": reg,
                    "n_obs": int(m.get("n_obs", 0)),
                    "sharpe": m.get("sharpe", np.nan),
                    "max_drawdown": m.get("max_drawdown", np.nan),
                    "win_rate": m.get("win_rate", np.nan),
                }
            )

        perf = pd.DataFrame(rows)
        if len(perf) == 0:
            return ValidationResult(
                "market_regime_response",
                False,
                "no regime observations",
                {"valid_regimes": 0},
            ), {"regime_performance": []}

        valid = perf[perf["n_obs"] >= 20].copy()
        valid_count = int(len(valid))
        worst_mdd = float(valid["max_drawdown"].min()) if valid_count > 0 else np.nan
        median_regime_sharpe = float(valid["sharpe"].median()) if valid_count > 0 else np.nan
        catastrophic = int((valid["max_drawdown"] <= -0.60).sum()) if valid_count > 0 else 0

        passed = bool(
            valid_count >= 2
            and catastrophic == 0
            and pd.notna(median_regime_sharpe)
            and median_regime_sharpe >= -0.50
        )

        gate = ValidationResult(
            "market_regime_response",
            passed,
            "multi-regime performance validation",
            {
                "valid_regimes": valid_count,
                "worst_mdd": worst_mdd,
                "median_regime_sharpe": median_regime_sharpe,
                "catastrophic_regimes": catastrophic,
            },
        )
        return gate, {"regime_performance": perf.to_dict(orient="records")}


class InflationValidator:
    def run(self, bt: BacktestResult, inflation_by_year: Optional[pd.Series]) -> ValidationResult:
        if inflation_by_year is None or len(inflation_by_year.dropna()) == 0:
            return ValidationResult(
                "inflation_real_return",
                False,
                "real return vs inflation",
                {"years_covered": 0, "median_real_return": np.nan, "worst_real_return": np.nan, "reason": "inflation data missing"},
            )

        infl = pd.to_numeric(inflation_by_year, errors="coerce").dropna().copy()
        if len(infl) == 0:
            return ValidationResult(
                "inflation_real_return",
                False,
                "real return vs inflation",
                {"years_covered": 0, "median_real_return": np.nan, "worst_real_return": np.nan, "reason": "inflation data invalid"},
            )

        if infl.abs().median() > 1.0:
            infl = infl / 100.0

        yearly_nominal = bt.returns.dropna().groupby(bt.returns.dropna().index.year).apply(lambda x: (1.0 + x).prod() - 1.0)
        common_years = sorted(set(yearly_nominal.index).intersection(set(infl.index.astype(int))))
        if len(common_years) < 2:
            return ValidationResult(
                "inflation_real_return",
                False,
                "real return vs inflation",
                {"years_covered": len(common_years), "median_real_return": np.nan, "worst_real_return": np.nan, "reason": "insufficient overlapping years"},
            )

        nom = yearly_nominal.loc[common_years].astype(float)
        ii = infl.loc[common_years].astype(float)
        real = ((1.0 + nom) / (1.0 + ii)) - 1.0

        median_real = float(real.median()) if len(real) else np.nan
        worst_real = float(real.min()) if len(real) else np.nan
        passed = bool(pd.notna(median_real) and median_real >= 0.0 and pd.notna(worst_real) and worst_real > -0.30)

        return ValidationResult(
            "inflation_real_return",
            passed,
            "real return exceeds inflation (multi-year)",
            {
                "years_covered": int(len(common_years)),
                "median_real_return": median_real,
                "worst_real_return": worst_real,
            },
        )


class TemporalConsistencyValidator:
    def run(self, bt: BacktestResult) -> ValidationResult:
        rr = bt.returns.dropna()
        yearly = rr.groupby(rr.index.year).apply(lambda x: (1.0 + x).prod() - 1.0)
        if len(yearly) < 3:
            return ValidationResult(
                "temporal_consistency",
                False,
                "cross-period performance consistency",
                {"n_years": int(len(yearly)), "positive_year_ratio": np.nan, "worst_year_return": np.nan},
            )

        pos_ratio = float((yearly > 0).mean())
        worst_y = float(yearly.min())
        med_y = float(yearly.median())
        passed = bool(pos_ratio >= 0.40 and worst_y > -0.50)

        return ValidationResult(
            "temporal_consistency",
            passed,
            "yearly consistency check",
            {
                "n_years": int(len(yearly)),
                "positive_year_ratio": pos_ratio,
                "worst_year_return": worst_y,
                "median_year_return": med_y,
            },
        )


class PsychologicalToleranceValidator:
    def run(self, bt: BacktestResult, max_tolerable_mdd: float = 0.30) -> ValidationResult:
        mdd = _safe_float(bt.metrics.get("max_drawdown", np.nan), np.nan)
        limit = -abs(float(max_tolerable_mdd))
        passed = bool(pd.notna(mdd) and mdd >= limit)
        return ValidationResult(
            "psychological_tolerance",
            passed,
            "mdd within psychological tolerance",
            {"max_drawdown": mdd, "mdd_limit": limit},
        )


class OutlierConcentrationValidator:
    def run(self, bt: BacktestResult) -> ValidationResult:
        trades = bt.trades.copy() if isinstance(bt.trades, pd.DataFrame) else pd.DataFrame()
        if len(trades) == 0:
            return ValidationResult(
                "outlier_concentration",
                False,
                "outlier concentration check",
                {"top_contrib_ratio": 1.0, "sample_n": 0, "reason": "no_trades"},
            )

        key_col = None
        for c in ["symbol", "code", "ticker", "pdno"]:
            if c in trades.columns:
                key_col = c
                break

        value_col = None
        for c in ["pnl", "return", "ret", "pnl_net"]:
            if c in trades.columns:
                value_col = c
                break

        if value_col is None:
            return ValidationResult(
                "outlier_concentration",
                False,
                "outlier concentration check",
                {"top_contrib_ratio": 1.0, "sample_n": int(len(trades)), "reason": "trade return/pnl missing"},
            )

        v = pd.to_numeric(trades[value_col], errors="coerce").dropna()
        if len(v) == 0:
            return ValidationResult(
                "outlier_concentration",
                False,
                "outlier concentration check",
                {"top_contrib_ratio": 1.0, "sample_n": 0, "reason": "no_valid_trade_values"},
            )

        if key_col is not None:
            grp = pd.DataFrame({"k": trades.loc[v.index, key_col].astype(str), "v": v}).groupby("k")["v"].sum().abs()
            denom = float(grp.sum())
            top_ratio = float(grp.max() / denom) if denom > 0 else 1.0
        else:
            abs_v = v.abs().sort_values(ascending=False)
            denom = float(abs_v.sum())
            top_ratio = float(abs_v.iloc[0] / denom) if denom > 0 else 1.0

        passed = bool(len(v) >= 20 and pd.notna(top_ratio) and top_ratio <= 0.80)
        return ValidationResult(
            "outlier_concentration",
            passed,
            "single outlier dominance check",
            {"top_contrib_ratio": float(top_ratio), "sample_n": int(len(v))},
        )

class HistoricalScenarioValidator:
    DEFAULT_WINDOWS: List[Tuple[str, str, str]] = [
        ("dotcom_burst", "2000-03-01", "2002-10-31"),
        ("gfc_2008", "2007-10-01", "2009-03-31"),
        ("boxpi_2011_2016", "2011-01-01", "2016-12-31"),
        ("covid_crash", "2020-02-01", "2020-04-30"),
        ("rate_hike_2022", "2022-01-01", "2022-10-31"),
        ("bull_2017", "2017-01-01", "2017-12-31"),
    ]

    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn

    def run(
        self,
        market_df: pd.DataFrame,
        params: Dict[str, Any],
        cost_model: CostModel,
        windows: Optional[Sequence[Tuple[str, str, str]]] = None,
    ) -> Tuple[ValidationResult, Dict[str, Any]]:
        use_windows = list(windows) if windows is not None else list(self.DEFAULT_WINDOWS)
        rows: List[Dict[str, Any]] = []

        for label, start, end in use_windows:
            part = market_df.loc[start:end]
            if len(part) < 60:
                rows.append({"scenario": label, "start": start, "end": end, "covered": False, "n_obs": int(len(part))})
                continue
            bt = self.backtest_fn(part, self.strategy_fn(part, params), params, cost_model)
            sh = _safe_float(bt.metrics.get("sharpe", np.nan), np.nan)
            ar = annualized_return(bt.returns)
            if pd.notna(sh):
                score = float(sh)
            elif pd.notna(ar):
                score = float(ar * 10.0)
            else:
                score = -999.0

            rows.append(
                {
                    "scenario": label,
                    "start": start,
                    "end": end,
                    "covered": True,
                    "n_obs": int(len(part)),
                    "sharpe": sh,
                    "max_drawdown": _safe_float(bt.metrics.get("max_drawdown", np.nan), np.nan),
                    "annual_return": ar,
                    "score": score,
                }
            )

        df = pd.DataFrame(rows)
        covered = df[df.get("covered", False) == True] if len(df) else pd.DataFrame()
        covered_n = int(len(covered))
        if covered_n == 0:
            gate = ValidationResult(
                "historical_scenario_response",
                False,
                "historical scenario response",
                {"covered_scenarios": 0, "worst_mdd": -1.0, "median_sharpe": -999.0, "median_score": -999.0},
            )
            return gate, {"scenarios": rows}

        worst_mdd = float(covered["max_drawdown"].min()) if "max_drawdown" in covered.columns else -1.0
        if not math.isfinite(worst_mdd):
            worst_mdd = -1.0

        sh_series = pd.to_numeric(covered["sharpe"], errors="coerce") if "sharpe" in covered.columns else pd.Series(dtype=float)
        median_sh = float(sh_series.dropna().median()) if len(sh_series.dropna()) else -999.0

        score_series = pd.to_numeric(covered["score"], errors="coerce") if "score" in covered.columns else pd.Series(dtype=float)
        median_score = float(score_series.dropna().median()) if len(score_series.dropna()) else -999.0

        catastrophic = int((covered["max_drawdown"] <= -0.65).sum()) if "max_drawdown" in covered.columns else 0

        passed = bool(covered_n >= 2 and catastrophic == 0 and median_score >= -1.0)
        gate = ValidationResult(
            "historical_scenario_response",
            passed,
            "historical scenario response",
            {
                "covered_scenarios": covered_n,
                "worst_mdd": worst_mdd,
                "median_sharpe": median_sh,
                "median_score": median_score,
                "catastrophic_scenarios": catastrophic,
            },
        )
        return gate, {"scenarios": rows}

def make_param_grid(grid_spec: Dict[str, Sequence[Any]]) -> List[Dict[str, Any]]:
    keys = list(grid_spec.keys())
    vals = [grid_spec[k] for k in keys]
    out: List[Dict[str, Any]] = []
    for combo in itertools.product(*vals):
        row = dict(zip(keys, combo))
        if "fast" in row and "slow" in row and row["fast"] >= row["slow"]:
            continue
        out.append(row)
    return out


def report_to_dataframe(report: PipelineReport) -> pd.DataFrame:
    rows = []
    for g in report.gate_results:
        row = {"name": g.name, "passed": g.passed, "summary": g.summary}
        row.update(g.details)
        rows.append(row)
    return pd.DataFrame(rows)


def sma_cross_strategy(market_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    fast = int(params.get("fast", 20))
    slow = int(params.get("slow", 100))
    allow_short = bool(params.get("allow_short", False))
    position_scale = float(params.get("position_scale", 1.0))
    position_scale = max(0.1, min(position_scale, 1.0))

    close = market_df["close"]
    ma_f = close.rolling(fast).mean()
    ma_s = close.rolling(slow).mean()
    sig = np.where(ma_f > ma_s, 1.0, 0.0)
    if allow_short:
        sig = np.where(ma_f < ma_s, -1.0, sig)

    out = pd.DataFrame(index=market_df.index)
    out["signal"] = (pd.Series(sig, index=market_df.index).fillna(0.0) * position_scale).clip(-1.0, 1.0)
    out["position"] = out["signal"].shift(1).fillna(0.0)
    return out


def make_demo_market(periods: int = 252 * 8) -> pd.DataFrame:
    np.random.seed(42)
    idx = pd.date_range("2015-01-01", periods=periods, freq="B")
    rets = np.random.normal(0.0003, 0.012, len(idx))
    close = 100 * np.cumprod(1 + rets)
    high = close * (1 + np.random.uniform(0, 0.01, len(idx)))
    low = close * (1 - np.random.uniform(0, 0.01, len(idx)))
    open_ = close * (1 + np.random.uniform(-0.003, 0.003, len(idx)))
    return pd.DataFrame({"open": open_, "high": high, "low": low, "close": close}, index=idx)


def calibrate_cost_from_fills(fills_df: pd.DataFrame, base_cost: CostModel, fills_map: Optional[Dict[str, Any]] = None) -> Tuple[CostModel, Dict[str, Any]]:
    mapping = fills_map or {}
    use = {
        "entry_price": str(mapping.get("entry_price", "entry_price")),
        "exit_price": str(mapping.get("exit_price", "exit_price")),
        "qty": str(mapping.get("qty", "qty")),
        "pnl_net": str(mapping.get("pnl_net", "pnl_krw_net")),
    }

    df = fills_df.copy()
    rename = {src: k for k, src in use.items() if src in df.columns and src != k}
    if rename:
        df = df.rename(columns=rename)

    req = ["entry_price", "exit_price", "qty", "pnl_net"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        return base_cost, {"calibrated": False, "reason": f"missing columns: {missing}", "fills_map_used": use}

    for c in req:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=req)
    df = df[df["qty"].abs() > 0]
    if len(df) < 20:
        return base_cost, {"calibrated": False, "reason": "insufficient rows (<20)", "rows": int(len(df)), "fills_map_used": use}

    notional = (df["entry_price"].abs() * df["qty"].abs()).replace(0, np.nan)
    gross_ret = ((df["exit_price"] - df["entry_price"]) * df["qty"]) / notional
    net_ret = df["pnl_net"] / notional
    cost_ret = (gross_ret - net_ret).abs().replace([np.inf, -np.inf], np.nan).dropna()

    if len(cost_ret) < 20:
        return base_cost, {"calibrated": False, "reason": "insufficient valid samples (<20)", "rows_valid": int(len(cost_ret)), "fills_map_used": use}

    rt_est = float(cost_ret.median() * 10000.0)
    if not math.isfinite(rt_est) or rt_est <= 0:
        return base_cost, {"calibrated": False, "reason": "invalid estimated roundtrip bps", "roundtrip_bps_est": rt_est, "fills_map_used": use}

    base_rt = max(base_cost.roundtrip_bps(), 1e-9)
    new_cost = CostModel(
        commission_bps=float(max(0.01, rt_est * (base_cost.commission_bps / base_rt))),
        slippage_bps=float(max(0.01, rt_est * (base_cost.slippage_bps / base_rt))),
        spread_bps=float(max(0.01, rt_est * (base_cost.spread_bps / base_rt))),
    )

    info = {
        "calibrated": True,
        "rows_valid": int(len(cost_ret)),
        "roundtrip_bps_est_median": rt_est,
        "base_cost": asdict(base_cost),
        "calibrated_cost": asdict(new_cost),
        "fills_map_used": use,
    }
    return new_cost, info


class ValidationPipeline:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn
        self.dataq = DataQualityValidator()
        self.bias = BiasAudit()
        self.wf = WalkForwardValidator(strategy_fn, backtest_fn)
        self.mc = MonteCarloValidator()
        self.cpcv = CPCVValidator(strategy_fn, backtest_fn)
        self.paramv = StrategyParameterValidator(strategy_fn, backtest_fn)
        self.regimev = MarketRegimeValidator()
        self.inflationv = InflationValidator()
        self.temporalv = TemporalConsistencyValidator()
        self.psychv = PsychologicalToleranceValidator()
        self.outlierv = OutlierConcentrationValidator()
        self.scenariov = HistoricalScenarioValidator(strategy_fn, backtest_fn)

    def run(
        self,
        market_df: pd.DataFrame,
        params: Dict[str, Any],
        param_grid: Sequence[Dict[str, Any]],
        cost_model: CostModel,
        inflation_by_year: Optional[pd.Series] = None,
        scenario_market_df: Optional[pd.DataFrame] = None,
        scenario_windows: Optional[Sequence[Tuple[str, str, str]]] = None,
        max_tolerable_mdd: float = 0.30,
        enable_cpcv: bool = False,
        cpcv_groups: int = 8,
        cpcv_k_test: int = 2,
        cpcv_purge_bars: int = 2,
        cpcv_max_splits: int = 40,
        wf_train_size: int = 252,
        wf_test_size: int = 42,
    ) -> PipelineReport:
        sig = self.strategy_fn(market_df, params)
        bt = self.backtest_fn(market_df, sig, params, cost_model)

        gates: List[ValidationResult] = []
        artifacts: Dict[str, Any] = {
            "base_metrics": bt.metrics,
            "base_meta": bt.meta,
        }

        gates.extend(self.dataq.run(market_df))
        gates.extend(self.bias.run(market_df, sig))

        param_gate, param_art = self.paramv.run(market_df, params, param_grid, cost_model)
        gates.append(param_gate)
        artifacts["strategy_parameter_validation"] = param_art

        wf_gate, wf_df = self.wf.run(market_df, {}, param_grid, cost_model, train_size=wf_train_size, test_size=wf_test_size)
        gates.append(wf_gate)
        artifacts["walk_forward"] = wf_df.to_dict(orient="records")

        mc_gate, mc_art = self.mc.run(bt)
        gates.append(mc_gate)
        artifacts["monte_carlo"] = mc_art

        regime_gate, regime_art = self.regimev.run(market_df, bt)
        gates.append(regime_gate)
        artifacts["market_regime_response"] = regime_art

        scen_market = scenario_market_df if scenario_market_df is not None and len(scenario_market_df) else market_df
        scen_gate, scen_art = self.scenariov.run(scen_market, params, cost_model, windows=scenario_windows)
        gates.append(scen_gate)
        artifacts["historical_scenario_response"] = scen_art

        infl_gate = self.inflationv.run(bt, inflation_by_year)
        gates.append(infl_gate)

        temporal_gate = self.temporalv.run(bt)
        gates.append(temporal_gate)

        psych_gate = self.psychv.run(bt, max_tolerable_mdd=max_tolerable_mdd)
        gates.append(psych_gate)

        outlier_gate = self.outlierv.run(bt)
        gates.append(outlier_gate)

        if enable_cpcv:
            cpcv_gate, cpcv_df = self.cpcv.run(
                market_df,
                {},
                param_grid,
                cost_model,
                n_groups=cpcv_groups,
                k_test=cpcv_k_test,
                purge_bars=cpcv_purge_bars,
                max_splits=cpcv_max_splits,
            )
            gates.append(cpcv_gate)
            artifacts["cpcv"] = cpcv_df.to_dict(orient="records")

        return PipelineReport(passed=all(g.passed for g in gates), gate_results=gates, artifacts=artifacts)

def main() -> int:
    ap = argparse.ArgumentParser(description="Executable backtest validation runner")
    ap.add_argument("--market-csv", default="")
    ap.add_argument("--date-col", default="date")
    ap.add_argument("--column-map-json", default="")
    ap.add_argument("--demo", action="store_true")

    ap.add_argument("--strategy-spec", default="")
    ap.add_argument("--backtest-spec", default="")

    ap.add_argument("--params-json", default="")
    ap.add_argument("--grid-spec-json", default="")

    ap.add_argument("--cost-model-json", default="")
    ap.add_argument("--fills-csv", default="")
    ap.add_argument("--fills-map-json", default="")

    ap.add_argument("--enable-cpcv", action="store_true")
    ap.add_argument("--cpcv-groups", type=int, default=8)
    ap.add_argument("--cpcv-k-test", type=int, default=2)
    ap.add_argument("--cpcv-purge-bars", type=int, default=2)
    ap.add_argument("--cpcv-max-splits", type=int, default=40)

    ap.add_argument("--wf-train-size", type=int, default=504)
    ap.add_argument("--wf-test-size", type=int, default=63)

    ap.add_argument("--inflation-csv", default="")
    ap.add_argument("--auto-fetch-inflation", action="store_true")
    ap.add_argument("--inflation-country-code", default="KR")
    ap.add_argument("--scenario-market-csv", default="")
    ap.add_argument("--scenario-windows-json", default="")
    ap.add_argument("--max-tolerable-mdd", type=float, default=0.30)

    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    log_dir = root / "2_Logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    column_map = _parse_json_arg(args.column_map_json)
    params = _parse_json_arg(args.params_json) or {"fast": 10, "slow": 100, "allow_short": True, "position_scale": 0.7}
    grid_spec = _parse_json_arg(args.grid_spec_json) or {"fast": [8, 10, 12], "slow": [90, 100, 110], "allow_short": [True], "position_scale": [0.6, 0.7, 0.8]}

    cost_model = CostModel()
    cm_json = _parse_json_arg(args.cost_model_json)
    if cm_json:
        cost_model = CostModel(
            commission_bps=_safe_float(cm_json.get("commission_bps", cost_model.commission_bps), cost_model.commission_bps),
            slippage_bps=_safe_float(cm_json.get("slippage_bps", cost_model.slippage_bps), cost_model.slippage_bps),
            spread_bps=_safe_float(cm_json.get("spread_bps", cost_model.spread_bps), cost_model.spread_bps),
        )

    if args.demo or not args.market_csv:
        market = make_demo_market()
        market_meta = {"source": "demo"}
    else:
        market, market_meta = load_market_csv(Path(args.market_csv), args.date_col, column_map=column_map)

    scenario_market = market
    scenario_market_meta: Dict[str, Any] = {"source": "same_as_market"}
    if args.scenario_market_csv:
        smp = Path(args.scenario_market_csv)
        if smp.exists():
            scenario_market, scenario_market_meta = load_market_csv(smp, args.date_col, column_map=column_map)
            scenario_market_meta["source"] = str(smp)
        else:
            scenario_market_meta = {"source": "missing", "path": str(smp)}

    inflation_by_year: Optional[pd.Series] = None
    inflation_meta: Dict[str, Any] = {"source": "none"}
    if args.inflation_csv:
        ip = Path(args.inflation_csv)
        if ip.exists():
            inflation_by_year = load_inflation_csv(ip)
            inflation_meta = {
                "source": str(ip),
                "rows": int(len(inflation_by_year)),
                "year_min": int(inflation_by_year.index.min()) if len(inflation_by_year) else None,
                "year_max": int(inflation_by_year.index.max()) if len(inflation_by_year) else None,
            }
        else:
            inflation_meta = {"source": "missing", "path": str(ip)}
    elif args.auto_fetch_inflation:
        try:
            inflation_by_year, inflation_meta = fetch_worldbank_inflation_series(args.inflation_country_code)
        except Exception as ex:
            inflation_meta = {"source": "worldbank", "error": str(ex)}

    scenario_windows = _parse_windows_arg(args.scenario_windows_json)

    strategy_user, strategy_src = _load_callable(args.strategy_spec, sma_cross_strategy)
    backtest_user, backtest_src = _load_callable(args.backtest_spec, reference_backtest)

    def strategy_fn(df: pd.DataFrame, p: Dict[str, Any]) -> pd.DataFrame:
        return _call_strategy_fn(strategy_user, df, p)

    def backtest_fn(df: pd.DataFrame, sig: pd.DataFrame, p: Dict[str, Any], cm: CostModel) -> BacktestResult:
        return _call_backtest_fn(backtest_user, df, sig, p, cm)

    fills_map = _parse_json_arg(args.fills_map_json)
    calibration_info: Dict[str, Any] = {"calibrated": False}
    if args.fills_csv:
        fp = Path(args.fills_csv)
        if fp.exists():
            fills_df = pd.read_csv(fp)
            cost_model, calibration_info = calibrate_cost_from_fills(fills_df, cost_model, fills_map)
            calibration_info["fills_csv"] = str(fp)
        else:
            calibration_info = {"calibrated": False, "reason": f"fills csv not found: {fp}"}

    param_grid = make_param_grid(grid_spec)

    pipe = ValidationPipeline(strategy_fn=strategy_fn, backtest_fn=backtest_fn)
    report = pipe.run(
        market_df=market,
        params=params,
        param_grid=param_grid,
        cost_model=cost_model,
        inflation_by_year=inflation_by_year,
        scenario_market_df=scenario_market,
        scenario_windows=scenario_windows,
        max_tolerable_mdd=max(0.01, float(args.max_tolerable_mdd)),
        enable_cpcv=bool(args.enable_cpcv),
        cpcv_groups=max(3, args.cpcv_groups),
        cpcv_k_test=max(1, args.cpcv_k_test),
        cpcv_purge_bars=max(0, args.cpcv_purge_bars),
        cpcv_max_splits=max(1, args.cpcv_max_splits),
        wf_train_size=max(20, args.wf_train_size),
        wf_test_size=max(5, args.wf_test_size),
    )

    report.artifacts["integration"] = {
        "strategy_source": strategy_src,
        "backtest_source": backtest_src,
        "params": params,
        "grid_spec": grid_spec,
        "n_param_grid": int(len(param_grid)),
        "market_meta": market_meta,
        "column_map_input": column_map,
        "cost_model": asdict(cost_model),
        "cost_calibration": calibration_info,
        "fills_map_input": fills_map,
        "inflation_meta": inflation_meta,
        "scenario_market_meta": scenario_market_meta,
        "scenario_windows": scenario_windows,
        "max_tolerable_mdd": max(0.01, float(args.max_tolerable_mdd)),
    }

    gate_df = report_to_dataframe(report)

    out_json = Path(args.out_json) if args.out_json else (log_dir / "backtest_validation_latest.json")
    out_csv = Path(args.out_csv) if args.out_csv else (log_dir / "backtest_validation_gates_latest.csv")

    out_json.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    gate_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    pass_n = int(gate_df["passed"].fillna(False).astype(bool).sum()) if len(gate_df) else 0
    fail_n = int(len(gate_df) - pass_n)
    print(f"[BTVAL] passed={report.passed} pass_n={pass_n} fail_n={fail_n}")
    print(f"[BTVAL] strategy={strategy_src} backtest={backtest_src}")
    print(f"[BTVAL] cpcv={'on' if args.enable_cpcv else 'off'} n_param_grid={len(param_grid)}")
    print(f"[BTVAL] json={out_json}")
    print(f"[BTVAL] csv={out_csv}")

    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())





























