"""Read-only Stage 3 execution grid for predeclared BREAKOUT_252D selections."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LIB_PATH = ROOT / "tools" / "build_new_method_strategy_library_validation.py"
PREFIX = LOG_DIR / "breakout_252d_selection_grid_execution"
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX.
#   **이 파일에는 SELL_TAX 항이 아예 없었다** - 오늘 산식에 추가했다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
FEE = 0.0
SLIPPAGE = 0.001
SELL_TAX = 0.002   # 브로커 실측 0.19723% (수수료는 0)
GRID = (("h1", 1), ("h1", 3), ("h2", 1), ("h2", 3))


def _load_library():
    spec = importlib.util.spec_from_file_location("strategy_library", LIB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load strategy library")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _net_return(gross: pd.Series) -> pd.Series:
    # [2026-09-10] 매도 거래세 항이 아예 없었다. 청산 쪽에 (1 - SELL_TAX) 를 곱한다.
    return ((1 + gross) * (1 - SLIPPAGE) * (1 - FEE) * (1 - SELL_TAX)) / ((1 + SLIPPAGE) * (1 + FEE)) - 1


def _summarize(frame: pd.DataFrame, horizon: str, n: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for period, group in frame.groupby("period", sort=True):
        rows.append(
            {
                "strategy": "BREAKOUT_252D",
                "horizon": horizon,
                "selection_variant": f"TOP_{n}",
                "period": period,
                "trades": int(len(group)),
                "avg_gross": float(group["gross_return"].mean()),
                "avg_net": float(group["net_return"].mean()),
                "win_rate": float(group["net_return"].gt(0).mean()),
                "avg_same_day_baseline_gross": float(group["same_day_baseline_gross"].mean()),
                "avg_same_day_baseline_net": float(group["same_day_baseline_net"].mean()),
                "avg_gross_excess_vs_same_day": float(group["gross_excess_vs_same_day"].mean()),
                "avg_net_excess_vs_same_day": float(group["net_excess_vs_same_day"].mean()),
            }
        )
    return rows


def main() -> int:
    lib = _load_library()
    base = lib._base()
    report = base._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors = base._strategy_features(factors)
    factors = base._atomic_signals(factors)
    factors = base._exact_returns(factors)
    factors = base._add_periods(factors)
    panel = factors[
        factors["date"].between(base.START, base.END)
        & factors["period"].notna()
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy().sort_values(["price_history_key", "price_session_index"], kind="mergesort")

    grouped = panel.groupby("price_history_key", sort=False)
    prior_high252 = grouped["close"].transform(lambda x: x.rolling(252, min_periods=60).max().shift(1))
    signal = panel["BREAKOUT_252D"].fillna(False)
    rank_score = -panel["close"].div(prior_high252 + 1e-9)
    rank = rank_score.loc[signal].groupby(panel.loc[signal, "date"], sort=False).rank(method="first")

    summary_rows: list[dict[str, object]] = []
    selected_frames: list[pd.DataFrame] = []
    count_rows: list[dict[str, object]] = []
    for horizon, n in GRID:
        return_column = f"path_return_{horizon}"
        valid_return = pd.to_numeric(panel[return_column], errors="coerce").notna()
        selected = signal & rank.reindex(panel.index).le(n).fillna(False) & valid_return
        chosen = panel.loc[selected].copy()
        baseline = panel.loc[valid_return].groupby("date", sort=False)[return_column].mean().rename("same_day_baseline_gross")
        chosen["gross_return"] = chosen[return_column]
        chosen["net_return"] = _net_return(chosen["gross_return"])
        chosen["same_day_baseline_gross"] = chosen["date"].map(baseline)
        chosen["same_day_baseline_net"] = _net_return(chosen["same_day_baseline_gross"])
        chosen["gross_excess_vs_same_day"] = chosen["gross_return"] - chosen["same_day_baseline_gross"]
        chosen["net_excess_vs_same_day"] = chosen["net_return"] - chosen["same_day_baseline_net"]
        chosen["horizon"] = horizon
        chosen["selection_variant"] = f"TOP_{n}"
        summary_rows.extend(_summarize(chosen, horizon, n))
        selected_frames.append(chosen)
        count_rows.append(
            {
                "horizon": horizon,
                "selection_variant": f"TOP_{n}",
                "selected_trades": int(len(chosen)),
                "unique_dates": int(chosen["date"].nunique()),
                "max_trades_per_date": int(chosen.groupby("date", sort=False).size().max()),
            }
        )

    summary = pd.DataFrame(summary_rows)
    trades = pd.concat(selected_frames, ignore_index=True)
    counts = pd.DataFrame(count_rows)
    summary.to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig")
    trades.to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig")
    counts.to_csv(f"{PREFIX}_counts_latest.csv", index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "strategy": "BREAKOUT_252D",
        "selection_grid": [{"horizon": horizon, "selection_variant": f"TOP_{n}"} for horizon, n in GRID],
        "selection_score": "close / prior_252_session_high descending",
        "entry": "same_close",
        "exit": "exact_global_trading_session_close_h1_or_h2",
        "cost": {"fee_pct": FEE, "slippage_pct": SLIPPAGE},
        "baseline": "same_date_full_universe_mean_with_equal_costs",
        "counts": count_rows,
        "price_history_integrity": integrity,
        "operational_change": False,
        "broker_order": False,
    }
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "variants": len(GRID), "selected_trades": int(len(trades))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
