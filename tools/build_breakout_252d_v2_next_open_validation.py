"""Research-only V2 next-open execution validation for a fixed breakout grid."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LIB_PATH = ROOT / "tools" / "build_new_method_strategy_library_validation.py"
CONTRACT_PATH = LOG_DIR / "strategy_execution_contract_v2_next_open.json"
PREFIX = LOG_DIR / "breakout_252d_v2_next_open"
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
GRID = (("h1", 1), ("h1", 3), ("h1", 5), ("h2", 1), ("h2", 3), ("h2", 5))
LABEL_START = pd.Timestamp("2025-06-01")
SPLITS = {
    "TRAIN_202102_202312": (pd.Timestamp("2021-02-01"), pd.Timestamp("2023-12-29")),
    "VAL_202401_202412": (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-30")),
    "OOS_202501_202505": (pd.Timestamp("2025-01-02"), pd.Timestamp("2025-05-30")),
}


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


def _next_open_returns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    grouped = out.groupby("price_history_key", sort=False)
    entry_open = grouped["open"].shift(-1)
    entry_session = grouped["price_session_index"].shift(-1)
    for horizon, sessions in (("h1", 1), ("h2", 2)):
        exit_close = grouped["close"].shift(-sessions)
        exit_session = grouped["price_session_index"].shift(-sessions)
        exact_entry = entry_session.sub(out["price_session_index"]).eq(1)
        exact_exit = exit_session.sub(out["price_session_index"]).eq(sessions)
        valid = exact_entry & exact_exit & pd.to_numeric(entry_open, errors="coerce").gt(0)
        out[f"v2_return_{horizon}"] = exit_close.div(entry_open).sub(1.0).where(valid)
    return out


def _summary(frame: pd.DataFrame, horizon: str, n: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for split, group in frame.groupby("v2_split", sort=True):
        rows.append(
            {
                "strategy": "BREAKOUT_252D",
                "horizon": horizon,
                "selection_variant": f"TOP_{n}",
                "split": split,
                "trades": int(len(group)),
                "avg_gross": float(group["gross_return"].mean()),
                "avg_net": float(group["net_return"].mean()),
                "median_net": float(group["net_return"].median()),
                "win_rate": float(group["net_return"].gt(0).mean()),
                "avg_same_day_baseline_gross": float(group["same_day_baseline_gross"].mean()),
                "avg_same_day_baseline_net": float(group["same_day_baseline_net"].mean()),
                "avg_gross_excess_vs_same_day": float(group["gross_excess_vs_same_day"].mean()),
                "avg_net_excess_vs_same_day": float(group["net_excess_vs_same_day"].mean()),
            }
        )
    return rows


def _oos_diagnostic(frame: pd.DataFrame, horizon: str, n: int) -> dict[str, object]:
    oos = frame.loc[frame["v2_split"].eq("OOS_202501_202505")].sort_values("net_return", ascending=False)
    trimmed = oos.iloc[5:]
    return {
        "strategy": "BREAKOUT_252D",
        "horizon": horizon,
        "selection_variant": f"TOP_{n}",
        "trades": int(len(oos)),
        "avg_net": float(oos["net_return"].mean()) if len(oos) else None,
        "median_net": float(oos["net_return"].median()) if len(oos) else None,
        "win_rate": float(oos["net_return"].gt(0).mean()) if len(oos) else None,
        "top5_net_sum": float(oos.head(5)["net_return"].sum()) if len(oos) else None,
        "avg_net_excluding_top5": float(trimmed["net_return"].mean()) if len(trimmed) else None,
    }


def main() -> int:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    lib = _load_library()
    base = lib._base()
    report = base._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors = base._strategy_features(factors)
    factors = base._atomic_signals(factors)
    factors = factors.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    full_grouped = factors.groupby("price_history_key", sort=False)
    factors["prior_high252_full"] = full_grouped["close"].transform(
        lambda x: x.rolling(252, min_periods=60).max().shift(1)
    )
    factors = _next_open_returns(factors)
    factors["market_source"] = factors["market"].fillna("").astype(str).str.upper().str.strip()
    labeled = factors.loc[
        factors["date"].ge(LABEL_START) & factors["market_source"].isin(["KOSPI", "KOSDAQ"]), ["code", "market_source"]
    ].drop_duplicates()
    label_count = labeled.groupby("code", sort=False)["market_source"].nunique()
    label_map = labeled.loc[
        labeled["code"].isin(label_count[label_count.eq(1)].index)
    ].drop_duplicates("code").set_index("code")["market_source"]
    factors["market_effective"] = factors["market_source"]
    legacy_missing = factors["market_effective"].isin(["", "KRX"])
    factors.loc[legacy_missing, "market_effective"] = factors.loc[legacy_missing, "code"].map(label_map).fillna("")
    mapping_meta = {
        "reference_start": str(LABEL_START.date()),
        "uniquely_mapped_codes": int(len(label_map)),
        "ambiguous_codes_excluded": int(label_count.gt(1).sum()),
        "legacy_rows_mapped": int((legacy_missing & factors["market_effective"].isin(["KOSPI", "KOSDAQ"])).sum()),
    }
    factors["v2_split"] = pd.NA
    for label, (start, end) in SPLITS.items():
        factors.loc[factors["date"].between(start, end), "v2_split"] = label
    pre_panel = factors.loc[factors["v2_split"].notna()].copy()
    precoverage_rows: list[dict[str, object]] = []
    for split, group in pre_panel.groupby("v2_split", sort=True):
        market = group["market_effective"].isin(["KOSPI", "KOSDAQ"])
        close_ok = pd.to_numeric(group["close"], errors="coerce").gt(0)
        open_ok = pd.to_numeric(group["open"], errors="coerce").gt(0)
        value_ok = pd.to_numeric(group["value"], errors="coerce").gt(0)
        precoverage_rows.append({
            "split": split,
            "factor_rows": int(len(group)),
            "unique_dates": int(group["date"].nunique()),
            "unique_codes": int(group["code"].nunique()),
            "market_rows": int(market.sum()),
            "positive_close_rows": int(close_ok.sum()),
            "positive_open_rows": int(open_ok.sum()),
            "positive_value_rows": int(value_ok.sum()),
            "all_panel_filter_rows": int((market & close_ok & open_ok & value_ok).sum()),
        })
    panel = factors[
        factors["v2_split"].notna()
        & factors["market_effective"].isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["open"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy()
    signal = panel["BREAKOUT_252D"].fillna(False)
    rank_score = -panel["close"].div(panel["prior_high252_full"] + 1e-9)
    rank = rank_score.loc[signal].groupby(panel.loc[signal, "date"], sort=False).rank(method="first")
    coverage_rows: list[dict[str, object]] = []
    for split, group in panel.groupby("v2_split", sort=True):
        coverage_rows.append({
            "split": split,
            "panel_rows": int(len(group)),
            "unique_dates": int(group["date"].nunique()),
            "unique_codes": int(group["code"].nunique()),
            "prior_high252_available": int(group["prior_high252_full"].notna().sum()),
            "breakout_signals": int(signal.loc[group.index].sum()),
            "valid_next_open_h1": int(group["v2_return_h1"].notna().sum()),
            "valid_next_open_h2": int(group["v2_return_h2"].notna().sum()),
        })

    summary_rows: list[dict[str, object]] = []
    diagnostic_rows: list[dict[str, object]] = []
    selected_frames: list[pd.DataFrame] = []
    count_rows: list[dict[str, object]] = []
    for horizon, n in GRID:
        return_column = f"v2_return_{horizon}"
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
        summary_rows.extend(_summary(chosen, horizon, n))
        diagnostic_rows.append(_oos_diagnostic(chosen, horizon, n))
        selected_frames.append(chosen)
        count_rows.append(
            {
                "horizon": horizon,
                "selection_variant": f"TOP_{n}",
                "selected_trades": int(len(chosen)),
                "unique_dates": int(chosen["date"].nunique()),
                "max_trades_per_date": int(chosen.groupby("date", sort=False).size().max()) if len(chosen) else 0,
            }
        )

    summary = pd.DataFrame(summary_rows)
    trades = pd.concat(selected_frames, ignore_index=True)
    counts = pd.DataFrame(count_rows)
    diagnostic = pd.DataFrame(diagnostic_rows)
    coverage = pd.DataFrame(coverage_rows)
    precoverage = pd.DataFrame(precoverage_rows)
    summary.to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig")
    trades.to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig")
    counts.to_csv(f"{PREFIX}_counts_latest.csv", index=False, encoding="utf-8-sig")
    diagnostic.to_csv(f"{PREFIX}_oos_diagnostic_latest.csv", index=False, encoding="utf-8-sig")
    coverage.to_csv(f"{PREFIX}_coverage_latest.csv", index=False, encoding="utf-8-sig")
    precoverage.to_csv(f"{PREFIX}_prepanel_coverage_latest.csv", index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "contract_version": contract["contract_version"],
        "splits": {label: [str(start.date()), str(end.date())] for label, (start, end) in SPLITS.items()},
        "counts": count_rows,
        "coverage": coverage_rows,
        "prepanel_coverage": precoverage_rows,
        "market_mapping": mapping_meta,
        "price_history_integrity": integrity,
        "operational_change": False,
        "broker_order": False,
    }
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "variants": len(GRID), "selected_trades": int(len(trades))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
