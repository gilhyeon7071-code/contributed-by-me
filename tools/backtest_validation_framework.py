
from __future__ import annotations

import argparse
import importlib
import importlib.util
import itertools
import json
import math
import os
import statistics
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import logging


@dataclass
class CostModel:
    """왕복 거래비용. 단위는 bps(0.01%).

    [2026-09-09] **매도 거래세 자리가 없었어요.** 구성이 수수료·슬리피지·스프레드뿐이라
      한국 시장의 매도 제세금 0.2% 가 들어갈 곳이 없었고, 그래서 이 프레임워크는
      실측보다 훨씬 싸게 계산하고 있었어요.
      ```
      기존 프로파일 backtest 2/3/2   왕복 0.120%
      배치가 넘기던 5/5/5            왕복 0.250%
      실측(브로커 정산 + 라이브 설정) 왕복 **0.400%**
                                     fee 0 x2 + slip 0.1% x2 + sell_tax 0.2%
      ```
      셋 다 실측보다 쌌어요. 싼 비용으로 재면 전략이 실제보다 좋아 보여요.
      `sell_tax_bps` 를 신설하고 프로파일을 실측에 맞춰요. 상세: PLANS (291).
    """
    commission_bps: float = 0.0
    slippage_bps: float = 10.0
    spread_bps: float = 0.0
    impact_bps: float = 0.0
    adverse_bps: float = 0.0
    sell_tax_bps: float = 20.0   # 매도 1회분. 왕복에 한 번만 더해요

    def roundtrip_bps(self) -> float:
        return (
            2.0 * self.commission_bps
            + 2.0 * self.slippage_bps
            + self.spread_bps
            + self.impact_bps
            + self.adverse_bps
            + self.sell_tax_bps
        )


# [2026-09-09] 실측 기준으로 다시 잡았어요.
#   brokerage 실측: 매도대금 88,730 / 수수료 0 / 제세금 175 = 0.19723% -> sell_tax 20bps
#   라이브 설정   : fee_pct 0.0 / slippage_pct 0.001 / sell_tax_pct 0.002
#   backtest 프로파일이 real 보다 싸면 백테스트가 항상 유리해 보여요. 셋을 같은 바닥에 둡니다.
_COST_PROFILES: Dict[str, CostModel] = {
    # 라이브 설정과 동일한 왕복 0.400%
    "backtest": CostModel(commission_bps=0.0, slippage_bps=10.0, spread_bps=0.0,
                          impact_bps=0.0, adverse_bps=0.0, sell_tax_bps=20.0),
    "paper": CostModel(commission_bps=0.0, slippage_bps=10.0, spread_bps=0.0,
                       impact_bps=0.5, adverse_bps=0.5, sell_tax_bps=20.0),
    # 실계좌는 체결 충격·역선택을 조금 더 봅니다
    "real": CostModel(commission_bps=0.0, slippage_bps=10.0, spread_bps=0.0,
                      impact_bps=1.0, adverse_bps=1.0, sell_tax_bps=20.0),
}


def _resolve_cost_model(cost_profile: str, overrides: Dict[str, Any]) -> Tuple[CostModel, Dict[str, Any]]:
    key = str(cost_profile or "backtest").strip().lower()
    if key not in _COST_PROFILES:
        key = "backtest"
    base = _COST_PROFILES[key]
    cm = CostModel(
        commission_bps=_safe_float(overrides.get("commission_bps", base.commission_bps), base.commission_bps),
        slippage_bps=_safe_float(overrides.get("slippage_bps", base.slippage_bps), base.slippage_bps),
        spread_bps=_safe_float(overrides.get("spread_bps", base.spread_bps), base.spread_bps),
        impact_bps=_safe_float(overrides.get("impact_bps", base.impact_bps), base.impact_bps),
        adverse_bps=_safe_float(overrides.get("adverse_bps", base.adverse_bps), base.adverse_bps),
    )
    meta = {"cost_profile": key, "cost_profile_base": asdict(base)}
    return cm, meta


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
        # [2026-09-09] **판정불가를 통과와 분리해서 셉니다. 게이트 판정은 바꾸지 않아요.**
        #   표본이 없어 미룬 게이트가 passed=True 로 나가요 (framework:2098
        #   `passed = bool(deferred or ...)`). 그 자체는 방어할 만해요 - 초기 운영 구간에서
        #   표본 부족으로 전부 막으면 아무것도 못 하니까요.
        #   문제는 **그 사실이 집계에서 사라진다**는 거예요. 위에서 "몇 개 통과" 를 세면
        #   판정불가가 통과에 섞여요.
        #   실측 2026-09-09: 18개 중 passed=False 는 3개인데
        #     signal_quality_ic_ir  (ess 129 < min_ess 200)  deferred -> passed=True
        #     inflation_real_return (겹치는 연도 0)           skipped  -> passed=True
        #   실제로는 **13 통과 / 3 실패 / 2 판정불가** 예요.
        #   검증 콘솔 화면은 본문에 "판정불가" 라고 정직하게 쓰는데 집계만 뭉개고 있었어요.
        def _is_deferred(x: "ValidationResult") -> bool:
            d = getattr(x, "details", None)
            if not isinstance(d, dict):
                return False
            return bool(d.get("deferred") or d.get("skipped"))

        rows = [asdict(x) for x in self.gate_results]
        deferred_names = [x.name for x in self.gate_results if _is_deferred(x)]
        failed_names = [x.name for x in self.gate_results if not x.passed]
        decided_pass = [x.name for x in self.gate_results
                        if x.passed and not _is_deferred(x)]
        return {
            "passed": self.passed,
            "gate_counts": {
                "total": len(rows),
                "passed_decided": len(decided_pass),
                "failed": len(failed_names),
                "undetermined": len(deferred_names),
                "note": ("undetermined 는 표본 부족 등으로 판정을 미룬 게이트예요. "
                         "passed=True 로 나가지만 통과가 아니에요."),
            },
            "undetermined_gates": deferred_names,
            "failed_gates": failed_names,
            "gate_results": rows,
            "artifacts": self.artifacts,
        }




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
def make_equity_curve(returns: pd.Series, initial_capital: float = 1.0) -> pd.Series:
    return initial_capital * (1.0 + returns.fillna(0.0)).cumprod()


def _series_records(series: pd.Series, value_name: str, max_rows: int = 500) -> List[Dict[str, Any]]:
    if not isinstance(series, pd.Series) or series.empty:
        return []
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return []
    idx = pd.to_datetime(s.index, errors="coerce")
    frame = pd.DataFrame({"date": idx.strftime("%Y%m%d"), value_name: s.to_numpy(dtype=float)})
    frame = frame.dropna(subset=["date"]).tail(max(1, int(max_rows)))
    return [
        {"date": str(row["date"]), value_name: float(row[value_name])}
        for _, row in frame.iterrows()
    ]


def _trade_records(trades: pd.DataFrame, max_rows: int = 500) -> List[Dict[str, Any]]:
    if not isinstance(trades, pd.DataFrame) or trades.empty:
        return []
    frame = trades.copy().tail(max(1, int(max_rows)))
    out: List[Dict[str, Any]] = []
    for _, row in frame.iterrows():
        item: Dict[str, Any] = {}
        for col in ("timestamp", "date", "return", "ret", "ledger_return", "turnover", "code", "symbol"):
            if col not in frame.columns:
                continue
            val = row.get(col)
            if pd.isna(val):
                item[col] = None
            elif col in {"timestamp", "date"}:
                item[col] = str(pd.to_datetime(val, errors="coerce").strftime("%Y%m%d") if not pd.isna(pd.to_datetime(val, errors="coerce")) else val)
            elif col in {"return", "ret", "ledger_return", "turnover"}:
                item[col] = float(val)
            else:
                item[col] = str(val)
        out.append(item)
    return out


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


def _annual_turnover_from_series(turnover: pd.Series, periods_per_year: int = 252) -> float:
    t = pd.to_numeric(turnover, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(t) == 0:
        return np.nan
    return float(t.mean() * float(periods_per_year))


def _objective_score(sharpe: float, annual_turnover: float, lambda_turnover: float) -> float:
    s = _safe_float(sharpe, np.nan)
    t = _safe_float(annual_turnover, np.nan)
    if pd.isna(s):
        return np.nan
    if pd.isna(t):
        t = 0.0
    penalty = float(lambda_turnover) * float(t)
    return float(s - penalty)


def _effective_sample_size(returns: pd.Series, max_lag: int = 10) -> float:
    r = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    n = len(r)
    if n <= 1:
        return float(n)
    den = 1.0
    for lag in range(1, min(max_lag, n - 1) + 1):
        ac = r.autocorr(lag=lag)
        if pd.isna(ac):
            continue
        den += 2.0 * float(ac)
    if den <= 0:
        return float(n)
    return float(max(1.0, min(float(n), float(n) / den)))


def _mbb_mean_effective_sample_size(
    returns: pd.Series,
    *,
    block_size: int = 21,
    n_boot: int = 500,
    seed: int = 42,
) -> Tuple[float, float]:
    rr = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
    n = len(rr)
    if n < 2:
        return float(n), np.nan

    sigma = float(np.std(rr, ddof=1))
    if not math.isfinite(sigma) or sigma <= 0.0:
        return float(n), sigma

    bs = max(1, min(int(block_size), n))
    rng = np.random.default_rng(int(seed))
    means: List[float] = []
    for _ in range(max(100, int(n_boot))):
        chunks: List[np.ndarray] = []
        while sum(len(c) for c in chunks) < n:
            s = int(rng.integers(0, n))
            e = s + bs
            if e <= n:
                blk = rr[s:e]
            else:
                blk = np.concatenate([rr[s:n], rr[0 : e - n]])
            chunks.append(blk)
        sample = np.concatenate(chunks)[:n]
        means.append(float(np.mean(sample)))

    mean_std = float(np.std(np.asarray(means, dtype=float), ddof=1))
    if not math.isfinite(mean_std) or mean_std <= 0.0:
        return float(n), sigma
    n_eff = (sigma / mean_std) ** 2
    return float(max(1.0, min(float(n), n_eff))), sigma


def _rolling_ic_series(signal: pd.Series, future_ret: pd.Series, window: int = 63) -> pd.Series:
    sig = pd.to_numeric(signal, errors="coerce")
    fr = pd.to_numeric(future_ret, errors="coerce")
    joined = pd.DataFrame({"s": sig, "r": fr}).dropna()
    if len(joined) < max(5, window):
        return pd.Series(dtype=float)
    vals: List[float] = []
    idxs: List[Any] = []
    for i in range(window, len(joined) + 1):
        part = joined.iloc[i - window : i]
        c = part["s"].corr(part["r"])
        vals.append(float(c) if pd.notna(c) else np.nan)
        idxs.append(joined.index[i - 1])
    return pd.Series(vals, index=idxs, dtype=float).dropna()


def _block_bootstrap_total_return_ci(
    returns: pd.Series,
    *,
    alpha: float = 0.05,
    n_boot: int = 1000,
    block_size: int = 21,
) -> Tuple[float, float, float]:
    rr = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
    n = len(rr)
    if n == 0:
        return np.nan, np.nan, np.nan
    if n == 1:
        total = float((1.0 + rr[0]) - 1.0)
        return total, total, total
    bs = max(2, min(int(block_size), n))
    rng = np.random.default_rng(42)
    totals: List[float] = []
    for _ in range(max(100, int(n_boot))):
        chunks: List[np.ndarray] = []
        while sum(len(c) for c in chunks) < n:
            s = int(rng.integers(0, n))
            e = s + bs
            if e <= n:
                blk = rr[s:e]
            else:
                pad = e - n
                blk = np.concatenate([rr[s:n], rr[0:pad]])
            chunks.append(blk)
        sample = np.concatenate(chunks)[:n]
        totals.append(float(np.prod(1.0 + sample) - 1.0))
    lo = float(np.percentile(totals, 100.0 * (alpha / 2.0)))
    med = float(np.percentile(totals, 50.0))
    hi = float(np.percentile(totals, 100.0 * (1.0 - alpha / 2.0)))
    return lo, med, hi


def _compute_sharpe_like(returns: pd.Series) -> Optional[float]:
    clean = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 2:
        return None
    values = [float(x) for x in clean.tolist()]
    stdev = statistics.stdev(values)
    if stdev <= 0:
        return None
    return statistics.mean(values) / stdev


def _compute_skew_kurtosis(returns: pd.Series) -> Tuple[float, float]:
    clean = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    values = [float(x) for x in clean.tolist()]
    n = len(values)
    if n < 3:
        return 0.0, 3.0

    mu = statistics.mean(values)
    m2 = sum((x - mu) ** 2 for x in values) / n
    if m2 <= 0:
        return 0.0, 3.0
    s = math.sqrt(m2)
    m3 = sum((x - mu) ** 3 for x in values) / n
    m4 = sum((x - mu) ** 4 for x in values) / n
    skew = m3 / (s ** 3)
    kurt = m4 / (s ** 4)
    if not math.isfinite(skew):
        skew = 0.0
    if not math.isfinite(kurt):
        kurt = 3.0
    return skew, kurt


def _first_trade_timestamp(bt: "BacktestResult") -> Optional[pd.Timestamp]:
    trades = getattr(bt, "trades", None)
    if not isinstance(trades, pd.DataFrame) or trades.empty:
        return None

    candidates: List[pd.Timestamp] = []
    idx = pd.to_datetime(trades.index, errors="coerce")
    if len(idx):
        idx = idx[~pd.isna(idx)]
        if len(idx):
            candidates.append(pd.Timestamp(idx.min()))

    for col in ("timestamp", "date", "datetime"):
        if col not in trades.columns:
            continue
        vals = pd.to_datetime(trades[col], errors="coerce")
        vals = vals[~pd.isna(vals)]
        if len(vals):
            candidates.append(pd.Timestamp(vals.min()))

    return min(candidates) if candidates else None


def _oper_start_timestamp() -> pd.Timestamp:
    raw = "".join(ch for ch in str(os.getenv("PAPER_OPER_START_YMD", "20260301") or "") if ch.isdigit())
    ymd = raw[:8] if len(raw) >= 8 else "20260301"
    ts = pd.to_datetime(ymd, format="%Y%m%d", errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp("2026-03-01")
    return pd.Timestamp(ts)


def _active_returns(bt: "BacktestResult") -> Tuple[pd.Series, pd.Timestamp]:
    returns_nonnull = bt.returns.dropna().copy()
    returns_index = pd.to_datetime(returns_nonnull.index, errors="coerce")
    returns_nonnull = returns_nonnull.loc[~pd.isna(returns_index)]
    returns_index = returns_index[~pd.isna(returns_index)]
    returns_nonnull.index = pd.DatetimeIndex(returns_index)

    oper_start = _oper_start_timestamp()
    active = returns_nonnull.loc[returns_nonnull.index >= oper_start]
    return active, oper_start


def _operating_market_df(market_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    oper_start = _oper_start_timestamp()
    idx = pd.to_datetime(market_df.index, errors="coerce")
    keep = ~pd.isna(idx)
    filtered = market_df.loc[keep].copy()
    idx = idx[keep]
    filtered.index = pd.DatetimeIndex(idx)
    filtered = filtered.loc[filtered.index >= oper_start].copy()
    filtered.attrs["oper_start_ymd"] = oper_start.strftime("%Y%m%d")
    filtered.attrs["operating_window_applied"] = True
    return filtered, {
        "oper_start_ymd": oper_start.strftime("%Y%m%d"),
        "raw_rows": int(len(market_df)),
        "operating_rows": int(len(filtered)),
    }


def _is_operating_window_df(market_df: pd.DataFrame) -> bool:
    return bool(getattr(market_df, "attrs", {}).get("operating_window_applied"))


def _compute_dsr_proxy(
    sharpe_like: Optional[float],
    n_obs: int,
    n_trials: int,
    skew: float,
    kurt: float,
) -> Optional[float]:
    if sharpe_like is None or n_obs < 2:
        return None

    n = int(max(2, n_obs))
    trials = int(max(1, n_trials))
    nd = statistics.NormalDist()

    if trials <= 1:
        sr_star = 0.0
    else:
        p1 = min(0.999999, max(1e-6, 1.0 - 1.0 / trials))
        p2 = min(0.999999, max(1e-6, 1.0 - 1.0 / (trials * math.e)))
        z1 = nd.inv_cdf(p1)
        z2 = nd.inv_cdf(p2)
        euler_gamma = 0.5772156649015329
        expected_max_z = (1.0 - euler_gamma) * z1 + euler_gamma * z2
        sr_star = expected_max_z / math.sqrt(max(1, n - 1))

    denom = 1.0 - float(skew) * float(sharpe_like) + ((float(kurt) - 1.0) / 4.0) * (float(sharpe_like) ** 2)
    denom = math.sqrt(max(1e-8, denom))
    z = (float(sharpe_like) - sr_star) * math.sqrt(max(1, n - 1)) / denom
    return float(nd.cdf(z))


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
                raw_returns = raw.get("returns")
                raw_equity = raw.get("equity")
                returns = raw_returns if isinstance(raw_returns, pd.Series) else pd.Series(raw_returns, dtype=float)
                equity = raw_equity if isinstance(raw_equity, pd.Series) else pd.Series(raw_equity, dtype=float)
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

    if "turnover" in sig.columns:
        out["turnover"] = pd.to_numeric(sig["turnover"], errors="coerce").reindex(out.index).fillna(0.0).clip(lower=0.0)
    else:
        out["turnover"] = out["position"].diff().abs().fillna(out["position"].abs())
    out["gross_return"] = out["position"] * out["ret_cc"]
    out["cost_commission"] = out["turnover"] * (float(cost_model.commission_bps) / 10000.0)
    out["cost_slippage"] = out["turnover"] * (float(cost_model.slippage_bps) / 10000.0)
    out["cost_spread"] = out["turnover"] * (float(cost_model.spread_bps) / 2.0 / 10000.0)
    out["cost_impact"] = out["turnover"] * (float(cost_model.impact_bps) / 10000.0)
    out["cost_adverse"] = out["turnover"] * (float(cost_model.adverse_bps) / 10000.0)
    out["cost"] = out[
        ["cost_commission", "cost_slippage", "cost_spread", "cost_impact", "cost_adverse"]
    ].sum(axis=1)
    out["net_return"] = out["gross_return"] - out["cost"]

    equity = make_equity_curve(out["net_return"])
    annual_turnover = _annual_turnover_from_series(out["turnover"])
    delay_proxy_days = 1.0
    adv_ratio = float((out["cost_adverse"].sum() / out["cost"].sum())) if float(out["cost"].sum()) > 0 else np.nan
    lo_ci, med_ci, hi_ci = _block_bootstrap_total_return_ci(out["net_return"], alpha=0.05, n_boot=1000, block_size=21)
    cost_breakdown = {
        "commission": float(out["cost_commission"].sum()),
        "slippage": float(out["cost_slippage"].sum()),
        "spread": float(out["cost_spread"].sum()),
        "impact": float(out["cost_impact"].sum()),
        "adverse_selection": float(out["cost_adverse"].sum()),
        "total": float(out["cost"].sum()),
    }
    trades = pd.DataFrame(
        {
            "timestamp": out.index[out["turnover"] > 0],
            "return": out.loc[out["turnover"] > 0, "net_return"].values,
            "cost": out.loc[out["turnover"] > 0, "cost"].values,
            "cost_commission": out.loc[out["turnover"] > 0, "cost_commission"].values,
            "cost_slippage": out.loc[out["turnover"] > 0, "cost_slippage"].values,
            "cost_spread": out.loc[out["turnover"] > 0, "cost_spread"].values,
            "cost_impact": out.loc[out["turnover"] > 0, "cost_impact"].values,
            "cost_adverse": out.loc[out["turnover"] > 0, "cost_adverse"].values,
        }
    )
    metrics = basic_metrics(out["net_return"], equity)
    metrics["annual_turnover"] = annual_turnover
    metrics["pnl_total_return"] = float(np.prod(1.0 + out["net_return"].fillna(0.0)) - 1.0)
    metrics["pnl_total_return_ci95_low"] = lo_ci
    metrics["pnl_total_return_ci95_med"] = med_ci
    metrics["pnl_total_return_ci95_high"] = hi_ci
    meta = {
        "engine": "reference_backtest",
        "execution": {
            "turnover_annualized": annual_turnover,
            "fill_delay_proxy_days": delay_proxy_days,
            "adverse_selection_ratio": adv_ratio,
            "cost_breakdown": cost_breakdown,
        },
    }
    return BacktestResult(out["net_return"], equity, trades, metrics, meta)


class BiasAudit:
    def run(
        self,
        market_df: pd.DataFrame,
        signal_df: pd.DataFrame,
        threshold_corr: float = 0.2,
        min_corr_obs: int = 63,
    ) -> List[ValidationResult]:
        sig = ensure_signal_schema(signal_df, market_df.index)
        future_ret = _safe_pct_change(market_df["close"]).shift(-1)
        corr_df = pd.DataFrame({"signal": sig["signal"], "future_ret": future_ret}).dropna()
        corr = corr_df["signal"].corr(corr_df["future_ret"]) if len(corr_df) else np.nan
        n_obs = int(len(corr_df))
        enough_obs = n_obs >= int(min_corr_obs)
        deferred = bool((not enough_obs) and _is_operating_window_df(market_df))
        lookahead_ok = bool(deferred or (enough_obs and not (pd.notna(corr) and abs(float(corr)) > threshold_corr)))
        lookahead_summary = (
            "signal-future corr audit"
            if enough_obs
            else ("deferred: insufficient operating observations for signal-future corr audit" if deferred else "insufficient data for signal-future corr audit")
        )

        if "position" in sig.columns:
            expected = sig["signal"].shift(1).fillna(0.0)
            mismatch = float((sig["position"].fillna(0.0) != expected).mean())
            lag_ok = mismatch <= 0.10
        else:
            mismatch = np.nan
            lag_ok = False

        return [
            ValidationResult(
                "look_ahead_proxy",
                lookahead_ok,
                lookahead_summary,
                {
                    "corr": corr,
                    "threshold": threshold_corr,
                    "n_obs": n_obs,
                    "min_n": int(min_corr_obs),
                    "deferred": bool(deferred),
                    "skipped": bool(deferred),
                    "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
                },
            ),
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

    def run(
        self,
        market_df: pd.DataFrame,
        base_params: Dict[str, Any],
        param_grid: Sequence[Dict[str, Any]],
        cost_model: CostModel,
        train_size: int,
        test_size: int,
        lambda_turnover: float = 0.0,
    ) -> Tuple[ValidationResult, pd.DataFrame]:
        if len(market_df) < train_size + test_size:
            deferred = _is_operating_window_df(market_df)
            return (
                ValidationResult(
                    "walk_forward",
                    bool(deferred),
                    "deferred: insufficient operating rows for walk-forward" if deferred else "insufficient data",
                    {
                        "len": int(len(market_df)),
                        "required_rows": int(train_size + test_size),
                        "deferred": bool(deferred),
                        "skipped": bool(deferred),
                        "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
                    },
                ),
                pd.DataFrame(),
            )

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
                turn = _safe_float(bt_is.metrics.get("annual_turnover", np.nan), np.nan)
                obj = _objective_score(sc, turn, lambda_turnover=lambda_turnover)
                if pd.notna(obj) and obj > best_score:
                    best_score = obj
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
                oos_turn = _safe_float(bt_oos.metrics.get("annual_turnover", np.nan), np.nan)
                oos_obj = _objective_score(oos_s, oos_turn, lambda_turnover=lambda_turnover)

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
                        "oos_annual_turnover": oos_turn,
                        "oos_objective": oos_obj,
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
            invalid_reason_counts: Dict[str, int] = {}
        else:
            valid = wf[wf["wfe_valid"] == True] if "wfe_valid" in wf.columns else pd.DataFrame()
            valid_folds = int(len(valid))
            med = float(valid["wfe"].median()) if valid_folds > 0 else -999.0
            invalid_reason_counts = {}
            if "reason" in wf.columns:
                invalid_reason_counts = {str(k): int(v) for k, v in wf["reason"].value_counts(dropna=False).items()}

        wf_summary = "median WFE >= 50" if valid_folds > 0 else "insufficient data: no valid walk-forward folds"

        gate = ValidationResult(
            "walk_forward",
            bool(valid_folds >= 1 and med >= 50.0),
            wf_summary,
            {
                "median_wfe": med,
                "n_folds": int(len(wf)),
                "valid_folds": valid_folds,
                "min_abs_is_sharpe": 0.25,
                "objective_lambda_turnover": float(lambda_turnover),
                "invalid_reason_counts": invalid_reason_counts,
            },
        )
        return gate, wf
class MonteCarloValidator:
    def run(
        self,
        bt: BacktestResult,
        max_allowed_mc95_mdd: float = -0.30,
        n_sim: int = 2500,
        alpha: float = 0.05,
        block_size: int = 1,
    ) -> Tuple[ValidationResult, Dict[str, Any]]:
        r = bt.returns.dropna().to_numpy(dtype=float)
        if len(r) == 0:
            return ValidationResult("monte_carlo", False, "no returns", {}), {}

        n = len(r)
        bs = max(1, min(int(block_size), n))
        alpha = min(0.40, max(0.001, float(alpha)))
        mdds: List[float] = []
        finals: List[float] = []
        rng = np.random.default_rng(42)
        for _ in range(n_sim):
            if bs <= 1:
                sampled = rng.choice(r, size=n, replace=True)
            else:
                chunks: List[np.ndarray] = []
                cur = 0
                while cur < n:
                    s = int(rng.integers(0, n))
                    e = s + bs
                    if e <= n:
                        blk = r[s:e]
                    else:
                        pad = e - n
                        blk = np.concatenate([r[s:n], r[0:pad]])
                    chunks.append(blk)
                    cur += len(blk)
                sampled = np.concatenate(chunks)[:n]
            eq = np.cumprod(1.0 + sampled)
            peak = np.maximum.accumulate(eq)
            dd = eq / peak - 1.0
            mdds.append(float(dd.min()))
            finals.append(float(eq[-1] - 1.0))

        mdd_pct = np.percentile(mdds, [1, 5, 10, 50, 90, 95, 99])
        final_pct = np.percentile(finals, [1, 5, 10, 50, 90, 95, 99])
        mc_tail = float(np.percentile(mdds, 100.0 * alpha))
        mc95_legacy = float(np.percentile(mdds, 95))
        art = {
            "mdd_pct": {str(k): float(v) for k, v in zip([1, 5, 10, 50, 90, 95, 99], mdd_pct)},
            "final_pct": {str(k): float(v) for k, v in zip([1, 5, 10, 50, 90, 95, 99], final_pct)},
            "mc_alpha": float(alpha),
            "mc_tail_mdd": float(mc_tail),
            "mc95_mdd_legacy": float(mc95_legacy),
            "block_size": int(bs),
            "n_sim": int(n_sim),
        }
        gate = ValidationResult(
            "monte_carlo",
            bool(mc_tail >= max_allowed_mc95_mdd),
            "MC tail MDD check",
            {
                "mc_alpha": float(alpha),
                "mc_tail_mdd": float(mc_tail),
                "limit": max_allowed_mc95_mdd,
                "mc95_mdd_legacy": float(mc95_legacy),
                "block_size": int(bs),
                "n_sim": int(n_sim),
            },
        )
        return gate, art


class CPCVValidator:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn

    def run(
        self,
        market_df: pd.DataFrame,
        base_params: Dict[str, Any],
        param_grid: Sequence[Dict[str, Any]],
        cost_model: CostModel,
        n_groups: int = 8,
        k_test: int = 2,
        purge_bars: int = 2,
        embargo_bars: int = 0,
        max_splits: int = 40,
        nested_inner_wf: bool = False,
        inner_train_size: int = 252,
        inner_test_size: int = 42,
        lambda_turnover: float = 0.0,
        min_ess: float = 200.0,
    ) -> Tuple[ValidationResult, pd.DataFrame]:
        n = len(market_df)
        if n < 200:
            deferred = _is_operating_window_df(market_df)
            return (
                ValidationResult(
                    "cpcv_pbo",
                    bool(deferred),
                    "deferred: insufficient operating rows for CPCV" if deferred else "insufficient data",
                    {
                        "len": int(n),
                        "min_n": 200,
                        "deferred": bool(deferred),
                        "skipped": bool(deferred),
                        "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
                    },
                ),
                pd.DataFrame(),
            )

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
            if embargo_bars > 0:
                tix = np.where(test_mask)[0]
                for t in tix:
                    lo = min(n, t + 1)
                    hi = min(n, t + 1 + embargo_bars)
                    if hi > lo:
                        train_mask[lo:hi] = False

            if train_mask.sum() < 80 or test_mask.sum() < 20:
                continue

            train_df = market_df.iloc[np.where(train_mask)[0]]
            test_df = market_df.iloc[np.where(test_mask)[0]]

            best_score = -np.inf
            best_params = None
            if nested_inner_wf:
                wf_gate, wf_df = WalkForwardValidator(self.strategy_fn, self.backtest_fn).run(
                    train_df,
                    base_params=base_params,
                    param_grid=param_grid,
                    cost_model=cost_model,
                    train_size=max(20, int(inner_train_size)),
                    test_size=max(5, int(inner_test_size)),
                    lambda_turnover=float(lambda_turnover),
                )
                _ = wf_gate
                if len(wf_df) > 0 and "best_params" in wf_df.columns:
                    cand = wf_df["best_params"].dropna().tolist()
                    if len(cand) > 0:
                        score_rows: List[Tuple[float, Dict[str, Any]]] = []
                        for cp in cand:
                            if not isinstance(cp, dict):
                                continue
                            merged = {**base_params, **cp}
                            bt_is = self.backtest_fn(train_df, self.strategy_fn(train_df, merged), merged, cost_model)
                            sc = _safe_float(bt_is.metrics.get("sharpe", np.nan), np.nan)
                            if pd.isna(sc):
                                sc = _safe_float(annualized_return(bt_is.returns), np.nan)
                            turn = _safe_float(bt_is.metrics.get("annual_turnover", np.nan), np.nan)
                            obj = _objective_score(sc, turn, lambda_turnover=float(lambda_turnover))
                            if pd.notna(obj):
                                score_rows.append((float(obj), merged))
                        if len(score_rows) > 0:
                            score_rows.sort(key=lambda x: x[0], reverse=True)
                            best_score, best_params = score_rows[0]
            if best_params is None:
                for p in param_grid:
                    params = {**base_params, **p}
                    bt_is = self.backtest_fn(train_df, self.strategy_fn(train_df, params), params, cost_model)
                    sc = _safe_float(bt_is.metrics.get("sharpe", np.nan), np.nan)
                    if pd.isna(sc):
                        sc = _safe_float(annualized_return(bt_is.returns), np.nan)
                    turn = _safe_float(bt_is.metrics.get("annual_turnover", np.nan), np.nan)
                    obj = _objective_score(sc, turn, lambda_turnover=float(lambda_turnover))
                    if pd.notna(obj) and obj > best_score:
                        best_score = obj
                        best_params = params

            if best_params is None:
                continue

            bt_oos = self.backtest_fn(test_df, self.strategy_fn(test_df, best_params), best_params, cost_model)
            oos_sharpe = _safe_float(bt_oos.metrics.get("sharpe", np.nan), np.nan)
            oos_turn = _safe_float(bt_oos.metrics.get("annual_turnover", np.nan), np.nan)
            oos_obj = _objective_score(oos_sharpe, oos_turn, lambda_turnover=float(lambda_turnover))
            oos_ess = _effective_sample_size(bt_oos.returns, max_lag=10)
            rows.append(
                {
                    "split": i,
                    "test_groups": list(test_groups),
                    "is_best_objective": float(best_score),
                    "oos_sharpe": oos_sharpe,
                    "oos_annual_turnover": oos_turn,
                    "oos_objective": oos_obj,
                    "oos_ess": float(oos_ess),
                    "nested_inner_wf": bool(nested_inner_wf),
                }
            )

        df = pd.DataFrame(rows)
        if len(df) == 0:
            return ValidationResult("cpcv_pbo", False, "no valid split", {"n_splits": 0}), df

        pbo_approx = float((df["oos_sharpe"] <= 0).mean())
        med_oos = float(df["oos_sharpe"].median())
        ess_med = float(df["oos_ess"].median()) if "oos_ess" in df.columns and len(df) else np.nan
        passed = bool(pbo_approx <= 0.50 and med_oos >= 0.0 and (pd.isna(ess_med) or ess_med >= float(min_ess)))

        gate = ValidationResult(
            "cpcv_pbo",
            passed,
            "PBO approx <= 0.50 and median OOS Sharpe >= 0",
            {
                "pbo_approx": pbo_approx,
                "median_oos_sharpe": med_oos,
                "median_oos_ess": ess_med,
                "min_ess": float(min_ess),
                "n_splits": int(len(df)),
                "purge_bars": int(purge_bars),
                "embargo_bars": int(embargo_bars),
                "nested_inner_wf": bool(nested_inner_wf),
                "objective_lambda_turnover": float(lambda_turnover),
            },
        )
        return gate, df


# [2026-08-29] DSR 의 n_trials 는 종전에 len(param_grid) = 4 였다.
#   그런데 이 프로젝트는 감사 전체에 걸쳐 수백 개 조합을 훑었다(HPO 160, 파라미터 표면 56,
#   94조합, 신호 재고 15축 ...). 4 를 넣으면 다중검정 보정이 사실상 없는 것과 같고,
#   그 상태의 "DSR PASS" 는 아무 보증도 아니다.
#   기록으로 방어 가능한 누적치를 원장에서 읽는다. 원장이 없으면 종전 동작(param_grid)로
#   떨어지되 details 에 출처를 남겨 과소 상태임이 보이게 한다.
_TRIAL_LEDGER_PATH = Path(__file__).resolve().parents[1] / "2_Logs" / "research_trial_ledger.json"


def load_research_trial_total(fallback: int) -> tuple:
    """(n_trials, source, breakdown) 을 돌려준다."""
    env = str(os.environ.get("BTVAL_N_TRIALS_TOTAL", "")).strip()
    if env.isdigit() and int(env) > 0:
        return int(env), "env:BTVAL_N_TRIALS_TOTAL", []
    try:
        obj = json.loads(_TRIAL_LEDGER_PATH.read_text(encoding="utf-8"))
        entries = obj.get("entries") or []
        total = sum(int(e.get("n") or 0) for e in entries if isinstance(e, dict))
        if total > 0:
            brk = [{"n": int(e.get("n") or 0), "source": str(e.get("source") or "")} for e in entries]
            return max(int(fallback), total), "research_trial_ledger.json", brk
    except Exception:
        pass
    return max(1, int(fallback)), "param_grid(UNDERSTATED)", []


class DeflatedSharpeValidator:
    def run(self, bt: BacktestResult, min_dsr: float = 0.10, n_trials: int = 1) -> ValidationResult:
        rr = pd.to_numeric(bt.returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(rr) < 2:
            return ValidationResult(
                "deflated_sharpe_ratio",
                False,
                "insufficient return observations for DSR",
                {
                    "deflated_sharpe_ratio": np.nan,
                    "min_dsr": float(min_dsr),
                    "n_obs": int(len(rr)),
                    "n_trials": int(max(1, n_trials)),
                    "reason": "insufficient_returns",
                },
            )

        sharpe_like = _compute_sharpe_like(rr)
        if sharpe_like is None:
            return ValidationResult(
                "deflated_sharpe_ratio",
                False,
                "invalid sharpe-like statistic for DSR",
                {
                    "deflated_sharpe_ratio": np.nan,
                    "min_dsr": float(min_dsr),
                    "n_obs": int(len(rr)),
                    "n_trials": int(max(1, n_trials)),
                    "reason": "invalid_sharpe_like",
                },
            )

        skew, kurt = _compute_skew_kurtosis(rr)
        dsr = _compute_dsr_proxy(
            sharpe_like=sharpe_like,
            n_obs=int(len(rr)),
            n_trials=int(max(1, n_trials)),
            skew=skew,
            kurt=kurt,
        )
        if dsr is None or not math.isfinite(dsr):
            return ValidationResult(
                "deflated_sharpe_ratio",
                False,
                "dsr proxy computation failed",
                {
                    "deflated_sharpe_ratio": np.nan,
                    "min_dsr": float(min_dsr),
                    "n_obs": int(len(rr)),
                    "n_trials": int(max(1, n_trials)),
                    "sharpe_like": sharpe_like,
                    "skew": skew,
                    "kurt": kurt,
                    "reason": "dsr_compute_failed",
                },
            )

        passed = bool(float(dsr) >= float(min_dsr))
        return ValidationResult(
            "deflated_sharpe_ratio",
            passed,
            "deflated sharpe ratio proxy gate",
            {
                "deflated_sharpe_ratio": float(dsr),
                "min_dsr": float(min_dsr),
                "n_obs": int(len(rr)),
                "n_trials": int(max(1, n_trials)),
                "sharpe_like": float(sharpe_like),
                "skew": float(skew),
                "kurt": float(kurt),
            },
        )



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
            deferred = _is_operating_window_df(market_df)
            return ValidationResult(
                "market_regime_response",
                bool(deferred),
                "deferred: insufficient operating observations for regime analysis" if deferred else "no regime observations",
                {
                    "valid_regimes": 0,
                    "deferred": bool(deferred),
                    "skipped": bool(deferred),
                    "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
                },
            ), {"regime_performance": []}

        valid = perf[perf["n_obs"] >= 20].copy()
        valid_count = int(len(valid))
        worst_mdd = float(valid["max_drawdown"].min()) if valid_count > 0 else np.nan
        median_regime_sharpe = float(valid["sharpe"].median()) if valid_count > 0 else np.nan
        catastrophic = int((valid["max_drawdown"] <= -0.60).sum()) if valid_count > 0 else 0

        deferred = bool(_is_operating_window_df(market_df) and valid_count < 2)
        passed = bool(
            deferred
            or (
            valid_count >= 2
            and catastrophic == 0
            and pd.notna(median_regime_sharpe)
            and median_regime_sharpe >= -0.50
            )
        )

        gate = ValidationResult(
            "market_regime_response",
            passed,
            "deferred: insufficient operating regimes for regime analysis" if deferred else "multi-regime performance validation",
            {
                "valid_regimes": valid_count,
                "worst_mdd": worst_mdd,
                "median_regime_sharpe": median_regime_sharpe,
                "catastrophic_regimes": catastrophic,
                "deferred": bool(deferred),
                "skipped": bool(deferred),
                "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
            },
        )
        return gate, {"regime_performance": perf.to_dict(orient="records")}


class InflationValidator:
    def run(
        self,
        bt: BacktestResult,
        inflation_by_year: Optional[pd.Series],
        require_inflation_data: bool = False,
    ) -> ValidationResult:
        if inflation_by_year is None or len(inflation_by_year.dropna()) == 0:
            return ValidationResult(
                "inflation_real_return",
                (not bool(require_inflation_data)),
                ("real return vs inflation (skipped: inflation data missing)" if not require_inflation_data else "real return vs inflation"),
                {
                    "years_covered": 0,
                    "median_real_return": np.nan,
                    "worst_real_return": np.nan,
                    "reason": "inflation data missing",
                    "skipped": (not bool(require_inflation_data)),
                    "require_inflation_data": bool(require_inflation_data),
                },
            )

        infl = pd.to_numeric(inflation_by_year, errors="coerce").dropna().copy()
        if len(infl) == 0:
            return ValidationResult(
                "inflation_real_return",
                (not bool(require_inflation_data)),
                ("real return vs inflation (skipped: inflation data invalid)" if not require_inflation_data else "real return vs inflation"),
                {
                    "years_covered": 0,
                    "median_real_return": np.nan,
                    "worst_real_return": np.nan,
                    "reason": "inflation data invalid",
                    "skipped": (not bool(require_inflation_data)),
                    "require_inflation_data": bool(require_inflation_data),
                },
            )

        if infl.abs().median() > 1.0:
            infl = infl / 100.0

        returns_nonnull, oper_start = _active_returns(bt)
        returns_index = pd.to_datetime(returns_nonnull.index, errors="coerce")
        yearly_nominal = returns_nonnull.groupby(returns_index.year).apply(lambda x: (1.0 + x).prod() - 1.0)
        common_years = sorted(set(yearly_nominal.index).intersection(set(infl.index.astype(int))))
        if len(common_years) < 2:
            skipped = not bool(require_inflation_data)
            return ValidationResult(
                "inflation_real_return",
                skipped,
                (
                    "real return vs inflation (skipped: insufficient overlapping years)"
                    if skipped
                    else "insufficient data: overlapping operating years"
                ),
                {
                    "years_covered": len(common_years),
                    "median_real_return": np.nan,
                    "worst_real_return": np.nan,
                    "reason": "insufficient overlapping operating years",
                    "deferred": skipped,
                    "skipped": skipped,
                    "require_inflation_data": bool(require_inflation_data),
                    "oper_start_ymd": oper_start.strftime("%Y%m%d"),
                },
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
                "skipped": False,
                "require_inflation_data": bool(require_inflation_data),
                "oper_start_ymd": oper_start.strftime("%Y%m%d"),
            },
        )


class TemporalConsistencyValidator:
    def run(self, bt: BacktestResult) -> ValidationResult:
        rr, oper_start = _active_returns(bt)
        yearly = rr.groupby(rr.index.year).apply(lambda x: (1.0 + x).prod() - 1.0)
        if len(yearly) < 3:
            return ValidationResult(
                "temporal_consistency",
                True,
                "deferred: insufficient operating years for temporal consistency",
                {
                    "n_years": int(len(yearly)),
                    "positive_year_ratio": np.nan,
                    "worst_year_return": np.nan,
                    "deferred": True,
                    "skipped": True,
                    "oper_start_ymd": oper_start.strftime("%Y%m%d"),
                },
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
                "oper_start_ymd": oper_start.strftime("%Y%m%d"),
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
                True,
                "deferred: insufficient operating observations for historical scenarios",
                {
                    "covered_scenarios": 0,
                    "worst_mdd": np.nan,
                    "median_sharpe": np.nan,
                    "median_score": np.nan,
                    "deferred": True,
                    "skipped": True,
                },
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


class StatisticalPowerMDEValidator:
    def run(
        self,
        bt: BacktestResult,
        *,
        economic_mde_sharpe: Optional[float],
        economic_mde_return: Optional[float],
        sharpe_scale: str = "annual",
        alpha: float = 0.01,
        power: float = 0.80,
        block_size: int = 21,
        n_boot: int = 500,
        seed: int = 42,
    ) -> ValidationResult:
        rr = pd.to_numeric(bt.returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        n_obs = int(len(rr))
        if n_obs < 2:
            return ValidationResult(
                "statistical_power_mde",
                False,
                "insufficient returns for MDE pre-registration",
                {"reason": "insufficient_returns", "n_obs": n_obs},
            )

        alpha = min(0.40, max(0.0001, float(alpha)))
        power = min(0.999, max(0.5001, float(power)))
        z_alpha_2 = float(statistics.NormalDist().inv_cdf(1.0 - alpha / 2.0))
        z_power = float(statistics.NormalDist().inv_cdf(power))
        z_sum = z_alpha_2 + z_power

        n_eff, sigma = _mbb_mean_effective_sample_size(
            rr,
            block_size=max(1, int(block_size)),
            n_boot=max(100, int(n_boot)),
            seed=int(seed),
        )
        if not math.isfinite(sigma) or sigma <= 0.0 or n_eff < 1.0:
            return ValidationResult(
                "statistical_power_mde",
                False,
                "invalid return volatility for MDE pre-registration",
                {"reason": "invalid_sigma", "n_obs": n_obs, "n_eff": n_eff, "sigma": sigma},
            )

        mde_return = float(z_sum * sigma / math.sqrt(n_eff))
        mde_sharpe_daily = float(mde_return / sigma)
        scale = str(sharpe_scale or "annual").strip().lower()
        if scale in {"annual", "annualized", "yearly"}:
            mde_sharpe = float(mde_sharpe_daily * math.sqrt(252.0))
            scale = "annual"
        else:
            mde_sharpe = mde_sharpe_daily
            scale = "daily"

        target_return = None if economic_mde_return is None else float(economic_mde_return)
        target_sharpe = None if economic_mde_sharpe is None else float(economic_mde_sharpe)
        if target_return is None and target_sharpe is None:
            return ValidationResult(
                "statistical_power_mde",
                False,
                "economic MDE target is not pre-registered",
                {
                    "reason": "missing_economic_mde",
                    "n_obs": n_obs,
                    "n_eff": n_eff,
                    "sigma": sigma,
                    "mde_return": mde_return,
                    "mde_sharpe": mde_sharpe,
                    "sharpe_scale": scale,
                },
            )

        return_ok = True if target_return is None else (math.isfinite(target_return) and mde_return <= abs(target_return))
        sharpe_ok = True if target_sharpe is None else (math.isfinite(target_sharpe) and mde_sharpe <= abs(target_sharpe))
        passed = bool(return_ok and sharpe_ok)
        summary = "pre-registered economic MDE is detectable" if passed else "sample power is below pre-registered economic MDE"
        return ValidationResult(
            "statistical_power_mde",
            passed,
            summary,
            {
                "alpha": alpha,
                "power": power,
                "z_alpha_2": z_alpha_2,
                "z_power": z_power,
                "n_obs": n_obs,
                "n_eff": n_eff,
                "sigma": sigma,
                "block_size": int(block_size),
                "bootstrap_runs": int(n_boot),
                "seed": int(seed),
                "mde_return": mde_return,
                "economic_mde_return": target_return,
                "mde_sharpe": mde_sharpe,
                "economic_mde_sharpe": target_sharpe,
                "sharpe_scale": scale,
                "return_target_detectable": bool(return_ok),
                "sharpe_target_detectable": bool(sharpe_ok),
            },
        )

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
    """데모/시험용 합성 시장.

    [2026-09-09] 시작일이 2015-01-01 고정이라 ValidationPipeline 의 운영구간 필터
      (index >= PAPER_OPER_START_YMD, 기본 20260301)에 **전부 잘려 0행**이 됐다.
      날짜 자체에는 의미가 없으므로 운영구간 시작에 맞춰 앞으로 당긴다.
      길이(periods)와 난수 시드는 그대로라 생성되는 가격 계열은 동일하다.
    """
    np.random.seed(42)
    idx = pd.date_range(_oper_start_timestamp(), periods=periods, freq="B")
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
    scale = float(rt_est / base_rt)
    new_cost = CostModel(
        commission_bps=float(max(0.0, base_cost.commission_bps * scale)),
        slippage_bps=float(max(0.0, base_cost.slippage_bps * scale)),
        spread_bps=float(max(0.0, base_cost.spread_bps * scale)),
        impact_bps=float(max(0.0, base_cost.impact_bps * scale)),
        adverse_bps=float(max(0.0, base_cost.adverse_bps * scale)),
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


class SignalQualityValidator:
    def run(self, market_df: pd.DataFrame, signal_df: pd.DataFrame, min_ess: float = 200.0) -> Tuple[ValidationResult, Dict[str, Any]]:
        sig = ensure_signal_schema(signal_df, market_df.index)
        if "close" not in market_df.columns:
            gate = ValidationResult(
                "signal_quality_ic_ir",
                False,
                "signal IC/IR stability",
                {"ic_mean": np.nan, "ic_std": np.nan, "ir": np.nan, "ess": 0.0, "min_ess": float(min_ess)},
            )
            return gate, {"ic_series": []}
        future_ret = _safe_pct_change(market_df["close"]).shift(-1)
        ic_series = _rolling_ic_series(sig["signal"], future_ret, window=63)
        ic_mean = float(ic_series.mean()) if len(ic_series) else np.nan
        ic_std = float(ic_series.std(ddof=1)) if len(ic_series) > 1 else np.nan
        ir = float(ic_mean / ic_std) if (pd.notna(ic_mean) and pd.notna(ic_std) and ic_std > 0) else np.nan
        ess = _effective_sample_size(future_ret.dropna(), max_lag=10)
        deferred = bool(_is_operating_window_df(market_df) and ess < float(min_ess))
        passed = bool(deferred or (pd.notna(ic_mean) and pd.notna(ir) and ir > 0 and ess >= float(min_ess)))
        gate = ValidationResult(
            "signal_quality_ic_ir",
            passed,
            "deferred: insufficient operating observations for signal IC/IR" if deferred else "signal IC/IR stability",
            {
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "ir": ir,
                "ess": float(ess),
                "min_ess": float(min_ess),
                "deferred": bool(deferred),
                "skipped": bool(deferred),
                "oper_start_ymd": market_df.attrs.get("oper_start_ymd", ""),
            },
        )
        art = {
            "ic_series": [{"ts": str(k), "ic": float(v)} for k, v in ic_series.items()],
            "ic_mean": ic_mean,
            "ic_std": ic_std,
            "ir": ir,
            "ess": float(ess),
        }
        return gate, art


class AcceptanceStressValidator:
    def run(
        self,
        bt: BacktestResult,
        market_df: pd.DataFrame,
        signal_df: pd.DataFrame,
        monthly_turnover_limit: float = 0.20,
        ci_alpha: float = 0.05,
    ) -> Tuple[ValidationResult, Dict[str, Any]]:
        rr = pd.to_numeric(bt.returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(rr) == 0:
            gate = ValidationResult(
                "acceptance_pnl_turnover",
                False,
                "pnl ci95 and turnover acceptance",
                {"pnl_ci95_low": np.nan, "turnover_monthly": np.nan, "turnover_limit_monthly": float(monthly_turnover_limit)},
            )
            return gate, {}
        pnl_low, pnl_med, pnl_high = _block_bootstrap_total_return_ci(rr, alpha=float(ci_alpha), n_boot=1200, block_size=21)
        sig = ensure_signal_schema(signal_df, market_df.index)
        pos = pd.to_numeric(sig.get("position", sig.get("signal", 0.0)), errors="coerce").reindex(market_df.index).fillna(0.0)
        if "turnover" in sig.columns:
            turn = pd.to_numeric(sig["turnover"], errors="coerce").reindex(market_df.index).fillna(0.0).clip(lower=0.0)
        else:
            turn = pos.diff().abs().fillna(pos.abs())
        turnover_monthly = float(turn.mean() * 21.0) if len(turn) else np.nan

        base = rr.to_numpy(dtype=float)
        vix = [0.8, 1.0, 1.2, 1.5]
        stress_rows: List[Dict[str, Any]] = []
        for vf in vix:
            shocked = base * float(vf)
            eq = np.cumprod(1.0 + shocked)
            peak = np.maximum.accumulate(eq)
            dd = eq / peak - 1.0
            sh = _compute_sharpe_like(pd.Series(shocked))
            stress_rows.append(
                {
                    "vol_mult": float(vf),
                    "sharpe_like": (float(sh) if sh is not None else np.nan),
                    "mdd": float(np.min(dd)) if len(dd) else np.nan,
                    "total_return": float(eq[-1] - 1.0) if len(eq) else np.nan,
                }
            )

        passed = bool(
            pd.notna(pnl_low)
            and float(pnl_low) > 0.0
            and pd.notna(turnover_monthly)
            and float(turnover_monthly) <= float(monthly_turnover_limit)
        )
        gate = ValidationResult(
            "acceptance_pnl_turnover",
            passed,
            "pnl ci95 and turnover acceptance",
            {
                "pnl_ci95_low": pnl_low,
                "pnl_ci95_med": pnl_med,
                "pnl_ci95_high": pnl_high,
                "turnover_monthly": turnover_monthly,
                "turnover_limit_monthly": float(monthly_turnover_limit),
            },
        )
        art = {
            "pnl_ci95_low": pnl_low,
            "pnl_ci95_med": pnl_med,
            "pnl_ci95_high": pnl_high,
            "turnover_monthly": turnover_monthly,
            "turnover_limit_monthly": float(monthly_turnover_limit),
            "stress_scenarios": stress_rows,
        }
        return gate, art


class OnlineConformalValidator:
    def run(
        self,
        market_df: pd.DataFrame,
        signal_df: pd.DataFrame,
        *,
        alpha: float = 0.10,
        calib_window: int = 252,
        gamma: float = 0.02,
        local_window: int = 63,
        coverage_tol: float = 0.05,
    ) -> Tuple[ValidationResult, Dict[str, Any]]:
        if "close" not in market_df.columns:
            gate = ValidationResult(
                "online_conformal_coverage",
                False,
                "adaptive conformal local coverage",
                {"reason": "close missing"},
            )
            return gate, {}

        sig = ensure_signal_schema(signal_df, market_df.index)
        y = _safe_pct_change(market_df["close"]).shift(-1)
        amp = y.abs().rolling(20, min_periods=5).mean()
        mu = pd.to_numeric(sig["signal"], errors="coerce").reindex(y.index).fillna(0.0) * amp.fillna(0.0)

        scores = (y - mu).abs()
        a_target = float(min(0.45, max(0.01, alpha)))
        a_t = a_target
        a_min = max(0.005, a_target / 5.0)
        a_max = min(0.49, a_target * 3.0)

        rows: List[Dict[str, Any]] = []
        miss_hist: List[int] = []
        idxs = list(scores.index)
        for i in range(len(idxs)):
            idx = idxs[i]
            if i < max(5, calib_window):
                continue
            hist = pd.to_numeric(scores.iloc[max(0, i - calib_window) : i], errors="coerce").dropna()
            yy = _safe_float(y.iloc[i], np.nan)
            mm = _safe_float(mu.iloc[i], np.nan)
            if len(hist) < 10 or pd.isna(yy) or pd.isna(mm):
                continue
            q = float(np.quantile(hist.to_numpy(dtype=float), 1.0 - float(a_t)))
            covered = bool(abs(yy - mm) <= q)
            miss = 0 if covered else 1
            miss_hist.append(int(miss))
            a_t = float(min(a_max, max(a_min, a_t + float(gamma) * (a_target - miss))))
            local_cov = np.nan
            if len(miss_hist) >= max(10, local_window):
                part = miss_hist[-int(local_window) :]
                local_cov = float(1.0 - (sum(part) / len(part)))
            rows.append(
                {
                    "ts": str(idx),
                    "alpha_t": float(a_t),
                    "q_t": float(q),
                    "covered": bool(covered),
                    "local_coverage": local_cov,
                    "target_coverage": float(1.0 - a_target),
                }
            )

        df = pd.DataFrame(rows)
        if len(df) == 0:
            gate = ValidationResult(
                "online_conformal_coverage",
                False,
                "adaptive conformal local coverage",
                {"reason": "insufficient conformal samples", "rows": 0},
            )
            return gate, {"rows": 0}

        cov_series = pd.to_numeric(df["covered"], errors="coerce").fillna(0.0)
        global_cov = float(cov_series.mean())
        local_series = pd.to_numeric(df["local_coverage"], errors="coerce").dropna()
        local_cov = float(local_series.iloc[-1]) if len(local_series) else np.nan
        target_cov = float(1.0 - a_target)
        local_gap = abs(local_cov - target_cov) if pd.notna(local_cov) else np.inf
        global_gap = abs(global_cov - target_cov)
        passed = bool(local_gap <= float(coverage_tol) and global_gap <= float(max(coverage_tol, 0.08)))

        gate = ValidationResult(
            "online_conformal_coverage",
            passed,
            "adaptive conformal local coverage",
            {
                "target_coverage": target_cov,
                "global_coverage": global_cov,
                "local_coverage_last": (float(local_cov) if pd.notna(local_cov) else np.nan),
                "local_gap": (float(local_gap) if math.isfinite(local_gap) else np.nan),
                "global_gap": float(global_gap),
                "coverage_tol": float(coverage_tol),
                "rows": int(len(df)),
            },
        )
        art = {
            "target_coverage": target_cov,
            "global_coverage": global_cov,
            "local_coverage_last": (float(local_cov) if pd.notna(local_cov) else np.nan),
            "coverage_tol": float(coverage_tol),
            "rows": int(len(df)),
            "series": df.tail(500).to_dict(orient="records"),
        }
        return gate, art


class ValidationPipeline:
    def __init__(self, strategy_fn: Callable[..., pd.DataFrame], backtest_fn: Callable[..., BacktestResult]):
        self.strategy_fn = strategy_fn
        self.backtest_fn = backtest_fn
        self.dataq = DataQualityValidator()
        self.bias = BiasAudit()
        self.wf = WalkForwardValidator(strategy_fn, backtest_fn)
        self.mc = MonteCarloValidator()
        self.cpcv = CPCVValidator(strategy_fn, backtest_fn)
        self.dsrv = DeflatedSharpeValidator()
        self.paramv = StrategyParameterValidator(strategy_fn, backtest_fn)
        self.regimev = MarketRegimeValidator()
        self.inflationv = InflationValidator()
        self.temporalv = TemporalConsistencyValidator()
        self.psychv = PsychologicalToleranceValidator()
        self.outlierv = OutlierConcentrationValidator()
        self.scenariov = HistoricalScenarioValidator(strategy_fn, backtest_fn)
        self.signalq = SignalQualityValidator()
        self.acceptv = AcceptanceStressValidator()
        self.conformalv = OnlineConformalValidator()
        self.powerv = StatisticalPowerMDEValidator()

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
        cpcv_embargo_bars: int = 0,
        cpcv_max_splits: int = 40,
        cpcv_nested_inner_wf: bool = False,
        cpcv_inner_wf_train_size: int = 252,
        cpcv_inner_wf_test_size: int = 42,
        wf_train_size: int = 252,
        wf_test_size: int = 42,
        objective_lambda_turnover: float = 0.0,
        monthly_turnover_limit: float = 0.20,
        pnl_ci_alpha: float = 0.05,
        min_ess: float = 200.0,
        enable_online_conformal: bool = False,
        conformal_alpha: float = 0.10,
        conformal_window: int = 252,
        conformal_gamma: float = 0.02,
        conformal_local_window: int = 63,
        conformal_coverage_tol: float = 0.05,
        min_dsr: float = 0.10,
        mc_n_sim: int = 2500,
        mc_alpha: float = 0.05,
        mc_block_size: int = 1,
        require_inflation_data: bool = False,
        enable_power_prereg: bool = False,
        economic_mde_sharpe: Optional[float] = None,
        economic_mde_return: Optional[float] = None,
        mde_sharpe_scale: str = "annual",
        mde_alpha: float = 0.01,
        mde_power: float = 0.80,
        mde_block_size: int = 21,
        mde_bootstrap_runs: int = 500,
        mde_seed: int = 42,
    ) -> PipelineReport:
        raw_market_df = market_df
        market_df, operating_meta = _operating_market_df(raw_market_df)
        # [2026-09-09] 운영구간 필터가 입력을 **통째로 잘라내면 조용히 넘어가지 않는다.**
        #   _operating_market_df 는 index >= PAPER_OPER_START_YMD(기본 20260301) 만 남긴다.
        #   그보다 오래된 시장을 넣으면 0행이 되고, 그 뒤 모든 게이트가
        #   "insufficient_returns" 로 실패한다 - 진짜 이유(구간 밖)는 어디에도 안 보인다.
        #   실측 2026-09-09: 프레임워크 자체 데모(make_demo_market, 2015~2017)가 이 상태였고
        #   test_statistical_power_mde_... 가 그 때문에 실패하고 있었다. CLI main() 도 같다.
        if int(operating_meta.get("operating_rows", 0)) == 0 and int(operating_meta.get("raw_rows", 0)) > 0:
            raise ValueError(
                "operating window removed every row: raw_rows=%d oper_start=%s. "
                "Input market ends before the operating window. "
                "Set PAPER_OPER_START_YMD or pass market data inside the window."
                % (int(operating_meta.get("raw_rows", 0)), operating_meta.get("oper_start_ymd"))
            )
        sig = self.strategy_fn(market_df, params)
        bt = self.backtest_fn(market_df, sig, params, cost_model)

        gates: List[ValidationResult] = []
        artifacts: Dict[str, Any] = {
            "operating_window": operating_meta,
            "base_metrics": bt.metrics,
            "base_meta": bt.meta,
            "base_series": {
                "returns": _series_records(bt.returns, "return"),
                "equity": _series_records(bt.equity, "equity"),
                "trades": _trade_records(bt.trades),
            },
        }

        gates.extend(self.dataq.run(market_df))
        gates.extend(self.bias.run(market_df, sig))
        signalq_gate, signalq_art = self.signalq.run(market_df, sig, min_ess=float(min_ess))
        gates.append(signalq_gate)
        artifacts["signal_quality"] = signalq_art

        param_gate, param_art = self.paramv.run(market_df, params, param_grid, cost_model)
        gates.append(param_gate)
        artifacts["strategy_parameter_validation"] = param_art

        wf_gate, wf_df = self.wf.run(
            market_df,
            {},
            param_grid,
            cost_model,
            train_size=wf_train_size,
            test_size=wf_test_size,
            lambda_turnover=float(objective_lambda_turnover),
        )
        gates.append(wf_gate)
        artifacts["walk_forward"] = wf_df.to_dict(orient="records")

        mc_gate, mc_art = self.mc.run(
            bt,
            n_sim=max(100, int(mc_n_sim)),
            alpha=min(0.40, max(0.001, float(mc_alpha))),
            block_size=max(1, int(mc_block_size)),
        )
        gates.append(mc_gate)
        artifacts["monte_carlo"] = mc_art

        regime_gate, regime_art = self.regimev.run(market_df, bt)
        gates.append(regime_gate)
        artifacts["market_regime_response"] = regime_art

        scen_market = scenario_market_df if scenario_market_df is not None and len(scenario_market_df) else market_df
        scen_gate, scen_art = self.scenariov.run(scen_market, params, cost_model, windows=scenario_windows)
        gates.append(scen_gate)
        artifacts["historical_scenario_response"] = scen_art

        infl_gate = self.inflationv.run(
            bt,
            inflation_by_year,
            require_inflation_data=bool(require_inflation_data),
        )
        gates.append(infl_gate)

        temporal_gate = self.temporalv.run(bt)
        gates.append(temporal_gate)

        psych_gate = self.psychv.run(bt, max_tolerable_mdd=max_tolerable_mdd)
        gates.append(psych_gate)

        outlier_gate = self.outlierv.run(bt)
        gates.append(outlier_gate)
        accept_gate, accept_art = self.acceptv.run(
            bt,
            market_df,
            sig,
            monthly_turnover_limit=float(monthly_turnover_limit),
            ci_alpha=float(pnl_ci_alpha),
        )
        gates.append(accept_gate)
        artifacts["acceptance"] = accept_art
        if enable_online_conformal:
            conformal_gate, conformal_art = self.conformalv.run(
                market_df,
                sig,
                alpha=float(conformal_alpha),
                calib_window=max(30, int(conformal_window)),
                gamma=float(conformal_gamma),
                local_window=max(20, int(conformal_local_window)),
                coverage_tol=float(conformal_coverage_tol),
            )
            gates.append(conformal_gate)
            artifacts["online_conformal"] = conformal_art

        if enable_cpcv:
            cpcv_gate, cpcv_df = self.cpcv.run(
                market_df,
                {},
                param_grid,
                cost_model,
                n_groups=cpcv_groups,
                k_test=cpcv_k_test,
                purge_bars=cpcv_purge_bars,
                embargo_bars=cpcv_embargo_bars,
                max_splits=cpcv_max_splits,
                nested_inner_wf=bool(cpcv_nested_inner_wf),
                inner_train_size=max(20, int(cpcv_inner_wf_train_size)),
                inner_test_size=max(5, int(cpcv_inner_wf_test_size)),
                lambda_turnover=float(objective_lambda_turnover),
                min_ess=float(min_ess),
            )
            gates.append(cpcv_gate)
            artifacts["cpcv"] = cpcv_df.to_dict(orient="records")

        _n_trials, _nt_src, _nt_brk = load_research_trial_total(max(1, len(param_grid)))
        dsr_gate = self.dsrv.run(bt, min_dsr=max(0.0, float(min_dsr)), n_trials=_n_trials)
        try:
            dsr_gate.details["n_trials_source"] = _nt_src
            dsr_gate.details["n_trials_param_grid"] = int(len(param_grid))
            if _nt_brk:
                dsr_gate.details["n_trials_breakdown"] = _nt_brk
        except Exception:
            pass
        gates.append(dsr_gate)
        artifacts["deflated_sharpe_ratio"] = dict(dsr_gate.details)

        power_gate = self.powerv.run(
            bt,
            economic_mde_sharpe=economic_mde_sharpe,
            economic_mde_return=economic_mde_return,
            sharpe_scale=mde_sharpe_scale,
            alpha=float(mde_alpha),
            power=float(mde_power),
            block_size=max(1, int(mde_block_size)),
            n_boot=max(100, int(mde_bootstrap_runs)),
            seed=int(mde_seed),
        )
        artifacts["statistical_power_mde"] = dict(power_gate.details)
        artifacts["statistical_power_mde"]["enabled_as_gate"] = bool(enable_power_prereg)
        if enable_power_prereg:
            gates.append(power_gate)

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
    ap.add_argument("--cost-profile", default="backtest", choices=["backtest", "paper", "real"])
    ap.add_argument("--fills-csv", default="")
    ap.add_argument("--fills-map-json", default="")

    ap.add_argument("--enable-cpcv", action="store_true")
    ap.add_argument("--cpcv-groups", type=int, default=8)
    ap.add_argument("--cpcv-k-test", type=int, default=2)
    ap.add_argument("--cpcv-purge-bars", type=int, default=2)
    ap.add_argument("--cpcv-embargo-bars", type=int, default=0)
    ap.add_argument("--cpcv-max-splits", type=int, default=40)
    ap.add_argument("--cpcv-nested-inner-wf", action="store_true")
    ap.add_argument("--cpcv-inner-wf-train-size", type=int, default=252)
    ap.add_argument("--cpcv-inner-wf-test-size", type=int, default=42)

    ap.add_argument("--wf-train-size", type=int, default=504)
    ap.add_argument("--wf-test-size", type=int, default=63)
    ap.add_argument("--objective-lambda-turnover", type=float, default=0.0)
    ap.add_argument("--monthly-turnover-limit", type=float, default=0.20)
    ap.add_argument("--pnl-ci-alpha", type=float, default=0.05)
    ap.add_argument("--min-ess", type=float, default=200.0)
    ap.add_argument("--enable-online-conformal", action="store_true")
    ap.add_argument("--conformal-alpha", type=float, default=0.10)
    ap.add_argument("--conformal-window", type=int, default=252)
    ap.add_argument("--conformal-gamma", type=float, default=0.02)
    ap.add_argument("--conformal-local-window", type=int, default=63)
    ap.add_argument("--conformal-coverage-tol", type=float, default=0.05)

    ap.add_argument("--inflation-csv", default="")
    ap.add_argument("--auto-fetch-inflation", action="store_true")
    ap.add_argument("--inflation-country-code", default="KR")
    ap.add_argument("--require-inflation-data", action="store_true")
    ap.add_argument("--scenario-market-csv", default="")
    ap.add_argument("--scenario-windows-json", default="")
    ap.add_argument("--max-tolerable-mdd", type=float, default=0.30)
    ap.add_argument("--min-dsr", type=float, default=0.10)
    ap.add_argument("--mc-n-sim", type=int, default=2500)
    ap.add_argument("--mc-alpha", type=float, default=0.05)
    ap.add_argument("--mc-block-size", type=int, default=1)
    ap.add_argument("--enable-power-prereg", action="store_true")
    ap.add_argument("--economic-mde-sharpe", type=float, default=None)
    ap.add_argument("--economic-mde-return", type=float, default=None)
    ap.add_argument("--mde-sharpe-scale", default="annual", choices=["annual", "daily"])
    ap.add_argument("--mde-alpha", type=float, default=0.01)
    ap.add_argument("--mde-power", type=float, default=0.80)
    ap.add_argument("--mde-block-size", type=int, default=21)
    ap.add_argument("--mde-bootstrap-runs", type=int, default=500)
    ap.add_argument("--mde-seed", type=int, default=42)

    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    log_dir = root / "2_Logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    column_map = _parse_json_arg(args.column_map_json)
    params = _parse_json_arg(args.params_json) or {"fast": 10, "slow": 100, "allow_short": True, "position_scale": 0.7}
    grid_spec = _parse_json_arg(args.grid_spec_json) or {"fast": [8, 10, 12], "slow": [90, 100, 110], "allow_short": [True], "position_scale": [0.6, 0.7, 0.8]}

    cost_model, cost_profile_meta = _resolve_cost_model(args.cost_profile, {})
    cm_json = _parse_json_arg(args.cost_model_json)
    if cm_json:
        cost_model, cost_profile_meta = _resolve_cost_model(args.cost_profile, cm_json)

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
        cpcv_embargo_bars=max(0, args.cpcv_embargo_bars),
        cpcv_max_splits=max(1, args.cpcv_max_splits),
        cpcv_nested_inner_wf=bool(args.cpcv_nested_inner_wf),
        cpcv_inner_wf_train_size=max(20, args.cpcv_inner_wf_train_size),
        cpcv_inner_wf_test_size=max(5, args.cpcv_inner_wf_test_size),
        wf_train_size=max(20, args.wf_train_size),
        wf_test_size=max(5, args.wf_test_size),
        objective_lambda_turnover=max(0.0, float(args.objective_lambda_turnover)),
        monthly_turnover_limit=max(0.0, float(args.monthly_turnover_limit)),
        pnl_ci_alpha=min(0.2, max(0.001, float(args.pnl_ci_alpha))),
        min_ess=max(20.0, float(args.min_ess)),
        enable_online_conformal=bool(args.enable_online_conformal),
        conformal_alpha=min(0.45, max(0.01, float(args.conformal_alpha))),
        conformal_window=max(30, int(args.conformal_window)),
        conformal_gamma=min(0.5, max(0.0, float(args.conformal_gamma))),
        conformal_local_window=max(20, int(args.conformal_local_window)),
        conformal_coverage_tol=min(0.30, max(0.01, float(args.conformal_coverage_tol))),
        min_dsr=max(0.0, float(args.min_dsr)),
        mc_n_sim=max(100, int(args.mc_n_sim)),
        mc_alpha=min(0.40, max(0.001, float(args.mc_alpha))),
        mc_block_size=max(1, int(args.mc_block_size)),
        require_inflation_data=bool(args.require_inflation_data),
        enable_power_prereg=bool(args.enable_power_prereg),
        economic_mde_sharpe=args.economic_mde_sharpe,
        economic_mde_return=args.economic_mde_return,
        mde_sharpe_scale=str(args.mde_sharpe_scale),
        mde_alpha=min(0.40, max(0.0001, float(args.mde_alpha))),
        mde_power=min(0.999, max(0.5001, float(args.mde_power))),
        mde_block_size=max(1, int(args.mde_block_size)),
        mde_bootstrap_runs=max(100, int(args.mde_bootstrap_runs)),
        mde_seed=int(args.mde_seed),
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
        "cost_profile": cost_profile_meta,
        "cost_calibration": calibration_info,
        "fills_map_input": fills_map,
        "inflation_meta": inflation_meta,
        "require_inflation_data": bool(args.require_inflation_data),
        "scenario_market_meta": scenario_market_meta,
        "scenario_windows": scenario_windows,
        "max_tolerable_mdd": max(0.01, float(args.max_tolerable_mdd)),
        "objective_lambda_turnover": max(0.0, float(args.objective_lambda_turnover)),
        "monthly_turnover_limit": max(0.0, float(args.monthly_turnover_limit)),
        "pnl_ci_alpha": min(0.2, max(0.001, float(args.pnl_ci_alpha))),
        "min_ess": max(20.0, float(args.min_ess)),
        "cpcv_embargo_bars": max(0, args.cpcv_embargo_bars),
        "cpcv_nested_inner_wf": bool(args.cpcv_nested_inner_wf),
        "cpcv_inner_wf_train_size": max(20, args.cpcv_inner_wf_train_size),
        "cpcv_inner_wf_test_size": max(5, args.cpcv_inner_wf_test_size),
        "enable_online_conformal": bool(args.enable_online_conformal),
        "conformal_alpha": min(0.45, max(0.01, float(args.conformal_alpha))),
        "conformal_window": max(30, int(args.conformal_window)),
        "conformal_gamma": min(0.5, max(0.0, float(args.conformal_gamma))),
        "conformal_local_window": max(20, int(args.conformal_local_window)),
        "conformal_coverage_tol": min(0.30, max(0.01, float(args.conformal_coverage_tol))),
        "min_dsr": max(0.0, float(args.min_dsr)),
        "mc_n_sim": max(100, int(args.mc_n_sim)),
        "mc_alpha": min(0.40, max(0.001, float(args.mc_alpha))),
        "mc_block_size": max(1, int(args.mc_block_size)),
        "enable_power_prereg": bool(args.enable_power_prereg),
        "economic_mde_sharpe": args.economic_mde_sharpe,
        "economic_mde_return": args.economic_mde_return,
        "mde_sharpe_scale": str(args.mde_sharpe_scale),
        "mde_alpha": min(0.40, max(0.0001, float(args.mde_alpha))),
        "mde_power": min(0.999, max(0.5001, float(args.mde_power))),
        "mde_block_size": max(1, int(args.mde_block_size)),
        "mde_bootstrap_runs": max(100, int(args.mde_bootstrap_runs)),
        "mde_seed": int(args.mde_seed),
    }

    gate_df = report_to_dataframe(report)

    out_json = Path(args.out_json) if args.out_json else (log_dir / "backtest_validation_latest.json")
    out_csv = Path(args.out_csv) if args.out_csv else (log_dir / "backtest_validation_gates_latest.csv")

    out_json.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    gate_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    pass_n = int(gate_df["passed"].fillna(False).astype(bool).sum()) if len(gate_df) else 0
    fail_n = int(len(gate_df) - pass_n)
    _log_print(f"[BTVAL] passed={report.passed} pass_n={pass_n} fail_n={fail_n}")
    _log_print(f"[BTVAL] strategy={strategy_src} backtest={backtest_src}")
    _log_print(f"[BTVAL] cpcv={'on' if args.enable_cpcv else 'off'} n_param_grid={len(param_grid)}")
    _log_print(f"[BTVAL] json={out_json}")
    _log_print(f"[BTVAL] csv={out_csv}")

    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())





























