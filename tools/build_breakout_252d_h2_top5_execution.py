"""Read-only Stage 3 execution validation for BREAKOUT_252D h2 TOP_5."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LIB_PATH = ROOT / "tools" / "build_new_method_strategy_library_validation.py"
PREFIX = LOG_DIR / "breakout_252d_h2_top5_execution"
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


def _summary(frame: pd.DataFrame, variant: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for period, group in frame.groupby("period", sort=True):
        rows.append(
            {
                "variant": variant,
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
    # Preserve Stage 2 eligibility: this flag was computed before the research-window filter.
    signal = panel["BREAKOUT_252D"].fillna(False)
    rank_score = -panel["close"].div(prior_high252 + 1e-9)
    rank = rank_score.loc[signal].groupby(panel.loc[signal, "date"], sort=False).rank(method="first")
    valid_return = pd.to_numeric(panel["path_return_h2"], errors="coerce").notna()
    selected = signal & rank.reindex(panel.index).le(5).fillna(False) & valid_return

    baseline = panel.loc[valid_return].groupby("date", sort=False)["path_return_h2"].mean().rename("same_day_baseline_gross")
    all_signals = panel.loc[signal & valid_return].copy()
    chosen = panel.loc[selected].copy()
    for frame in (all_signals, chosen):
        frame["gross_return"] = frame["path_return_h2"]
        frame["net_return"] = _net_return(frame["gross_return"])
        frame["same_day_baseline_gross"] = frame["date"].map(baseline)
        frame["same_day_baseline_net"] = _net_return(frame["same_day_baseline_gross"])
        frame["gross_excess_vs_same_day"] = frame["gross_return"] - frame["same_day_baseline_gross"]
        frame["net_excess_vs_same_day"] = frame["net_return"] - frame["same_day_baseline_net"]

    summary = pd.DataFrame(_summary(all_signals, "ALL_SIGNALS") + _summary(chosen, "TOP5_STRONGEST_BREAKOUT"))
    summary.to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig")
    chosen.to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "strategy": "BREAKOUT_252D",
        "horizon": "h2",
        "selection": "TOP_5_STRONGEST_BREAKOUT",
        "selection_score": "close / prior_252_session_high descending",
        "entry": "same_close",
        "exit": "exact_global_trading_session_close_h2",
        "cost": {"fee_pct": FEE, "slippage_pct": SLIPPAGE},
        "baseline": "same_date_full_universe_mean_h2_with_equal_costs",
        "periods_present": sorted(chosen["period"].dropna().unique().tolist()),
        "all_signals": int(len(all_signals)),
        "selected_trades": int(len(chosen)),
        "price_history_integrity": integrity,
        "operational_change": False,
        "broker_order": False,
    }
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "all_signals": len(all_signals), "selected_trades": len(chosen)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
