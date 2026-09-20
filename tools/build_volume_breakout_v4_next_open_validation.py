"""Research-only V4 next-open validation for a fixed volume-breakout grid."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LIB_PATH = ROOT / "tools" / "build_new_method_strategy_library_validation.py"
CONTRACT_PATH = LOG_DIR / "strategy_execution_contract_v4_volume_breakout_next_open.json"
PREFIX = LOG_DIR / "volume_breakout_v4_next_open"
# [2026-09-10] 라이브 설정에 맞춰 교정 (BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2(수수료) + 0.001*2(슬리피지) + 0.002(거래세) = 0.00400.
#   **과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
#   **이 파일에는 SELL_TAX 항이 아예 없었다** - 오늘 산식에 추가했다.
FEE, SLIPPAGE = 0.0, 0.001
SELL_TAX = 0.002
GRID = tuple((horizon, n) for horizon in ("h1", "h2", "h5") for n in (1, 3, 5))
LABEL_START = pd.Timestamp("2025-06-01")
SPLITS = {"TRAIN_202102_202312": (pd.Timestamp("2021-02-01"), pd.Timestamp("2023-12-29")), "VAL_202401_202412": (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-30")), "OOS_202501_202505": (pd.Timestamp("2025-01-02"), pd.Timestamp("2025-05-30"))}


def _load_library():
    spec = importlib.util.spec_from_file_location("strategy_library", LIB_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load strategy library")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _net(gross: pd.Series) -> pd.Series:
    # [2026-09-10] 매도 거래세 항이 아예 없었다. 청산 쪽에 (1 - SELL_TAX) 를 곱한다.
    return ((1 + gross) * (1 - SLIPPAGE) * (1 - FEE) * (1 - SELL_TAX)) / ((1 + SLIPPAGE) * (1 + FEE)) - 1


def _returns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    grouped = out.groupby("price_history_key", sort=False)
    entry_open, entry_session = grouped["open"].shift(-1), grouped["price_session_index"].shift(-1)
    for horizon, sessions in (("h1", 1), ("h2", 2), ("h5", 5)):
        exit_close, exit_session = grouped["close"].shift(-sessions), grouped["price_session_index"].shift(-sessions)
        valid = entry_session.sub(out["price_session_index"]).eq(1) & exit_session.sub(out["price_session_index"]).eq(sessions) & pd.to_numeric(entry_open, errors="coerce").gt(0)
        out[f"v4_return_{horizon}"] = exit_close.div(entry_open).sub(1.0).where(valid)
    return out


def _period_rows(chosen: pd.DataFrame, horizon: str, n: int) -> list[dict[str, object]]:
    rows = []
    for split, group in chosen.groupby("v4_split", sort=True):
        rows.append({"strategy": "VOLUME_BREAKOUT", "horizon": horizon, "selection_variant": f"TOP_{n}", "split": split, "trades": int(len(group)), "avg_gross": float(group.gross_return.mean()), "avg_net": float(group.net_return.mean()), "median_net": float(group.net_return.median()), "win_rate": float(group.net_return.gt(0).mean()), "avg_same_day_baseline_gross": float(group.same_day_baseline_gross.mean()), "avg_same_day_baseline_net": float(group.same_day_baseline_net.mean()), "avg_gross_excess_vs_same_day": float(group.gross_excess_vs_same_day.mean()), "avg_net_excess_vs_same_day": float(group.net_excess_vs_same_day.mean())})
    return rows


def _oos_row(chosen: pd.DataFrame, horizon: str, n: int) -> dict[str, object]:
    oos = chosen.loc[chosen.v4_split.eq("OOS_202501_202505")].sort_values("net_return", ascending=False)
    trimmed = oos.iloc[5:]
    return {"strategy": "VOLUME_BREAKOUT", "horizon": horizon, "selection_variant": f"TOP_{n}", "trades": int(len(oos)), "avg_net": float(oos.net_return.mean()) if len(oos) else None, "median_net": float(oos.net_return.median()) if len(oos) else None, "win_rate": float(oos.net_return.gt(0).mean()) if len(oos) else None, "top5_net_sum": float(oos.head(5).net_return.sum()) if len(oos) else None, "avg_net_excluding_top5": float(trimmed.net_return.mean()) if len(trimmed) else None}


def main() -> int:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    lib, base = _load_library(), None
    base = lib._base()
    report = base._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors = base._strategy_features(factors)
    factors = base._atomic_signals(factors)
    factors = factors.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    factors["prior_high20_full"] = factors.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(20, min_periods=20).max().shift(1))
    factors = _returns(factors)
    factors["market_source"] = factors.market.fillna("").astype(str).str.upper().str.strip()
    labeled = factors.loc[factors.date.ge(LABEL_START) & factors.market_source.isin(["KOSPI", "KOSDAQ"]), ["code", "market_source"]].drop_duplicates()
    label_count = labeled.groupby("code", sort=False).market_source.nunique()
    label_map = labeled.loc[labeled.code.isin(label_count[label_count.eq(1)].index)].drop_duplicates("code").set_index("code").market_source
    factors["market_effective"] = factors.market_source
    legacy_missing = factors.market_effective.isin(["", "KRX"])
    factors.loc[legacy_missing, "market_effective"] = factors.loc[legacy_missing, "code"].map(label_map).fillna("")
    factors["v4_split"] = pd.NA
    for label, (start, end) in SPLITS.items():
        factors.loc[factors.date.between(start, end), "v4_split"] = label
    pre_panel = factors.loc[factors.v4_split.notna()].copy()
    precoverage = []
    for split, group in pre_panel.groupby("v4_split", sort=True):
        market, close_ok, open_ok, value_ok = group.market_effective.isin(["KOSPI", "KOSDAQ"]), pd.to_numeric(group.close, errors="coerce").gt(0), pd.to_numeric(group.open, errors="coerce").gt(0), pd.to_numeric(group.value, errors="coerce").gt(0)
        precoverage.append({"split": split, "factor_rows": int(len(group)), "unique_dates": int(group.date.nunique()), "unique_codes": int(group.code.nunique()), "market_rows": int(market.sum()), "positive_close_rows": int(close_ok.sum()), "positive_open_rows": int(open_ok.sum()), "positive_value_rows": int(value_ok.sum()), "all_panel_filter_rows": int((market & close_ok & open_ok & value_ok).sum())})
    panel = factors.loc[factors.v4_split.notna() & factors.market_effective.isin(["KOSPI", "KOSDAQ"]) & pd.to_numeric(factors.close, errors="coerce").gt(0) & pd.to_numeric(factors.open, errors="coerce").gt(0) & pd.to_numeric(factors.value, errors="coerce").gt(0)].copy()
    signal = panel.close.gt(panel.prior_high20_full) & panel.V_ACCEL_Q5.fillna(False)
    rank = pd.to_numeric(panel.loc[signal, "v_accel"], errors="coerce").groupby(panel.loc[signal, "date"], sort=False).rank(method="first", ascending=False)
    coverage = []
    for split, group in panel.groupby("v4_split", sort=True):
        coverage.append({"split": split, "panel_rows": int(len(group)), "unique_dates": int(group.date.nunique()), "unique_codes": int(group.code.nunique()), "volume_breakout_signals": int(signal.loc[group.index].sum()), "valid_next_open_h1": int(group.v4_return_h1.notna().sum()), "valid_next_open_h2": int(group.v4_return_h2.notna().sum()), "valid_next_open_h5": int(group.v4_return_h5.notna().sum())})
    summary_rows, diagnostic_rows, count_rows, selected_frames = [], [], [], []
    for horizon, n in GRID:
        ret_col = f"v4_return_{horizon}"
        valid = pd.to_numeric(panel[ret_col], errors="coerce").notna()
        chosen = panel.loc[signal & rank.reindex(panel.index).le(n).fillna(False) & valid].copy()
        baseline = panel.loc[valid].groupby("date", sort=False)[ret_col].mean()
        chosen["gross_return"] = chosen[ret_col]
        chosen["net_return"] = _net(chosen.gross_return)
        chosen["same_day_baseline_gross"] = chosen.date.map(baseline)
        chosen["same_day_baseline_net"] = _net(chosen.same_day_baseline_gross)
        chosen["gross_excess_vs_same_day"] = chosen.gross_return - chosen.same_day_baseline_gross
        chosen["net_excess_vs_same_day"] = chosen.net_return - chosen.same_day_baseline_net
        chosen["horizon"], chosen["selection_variant"] = horizon, f"TOP_{n}"
        summary_rows.extend(_period_rows(chosen, horizon, n)); diagnostic_rows.append(_oos_row(chosen, horizon, n)); selected_frames.append(chosen)
        count_rows.append({"horizon": horizon, "selection_variant": f"TOP_{n}", "selected_trades": int(len(chosen)), "unique_dates": int(chosen.date.nunique()), "max_trades_per_date": int(chosen.groupby("date", sort=False).size().max()) if len(chosen) else 0})
    pd.DataFrame(summary_rows).to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig")
    pd.concat(selected_frames, ignore_index=True).to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(count_rows).to_csv(f"{PREFIX}_counts_latest.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(diagnostic_rows).to_csv(f"{PREFIX}_oos_diagnostic_latest.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(coverage).to_csv(f"{PREFIX}_coverage_latest.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(precoverage).to_csv(f"{PREFIX}_prepanel_coverage_latest.csv", index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "contract_version": contract["contract_version"], "splits": {k: [str(v[0].date()), str(v[1].date())] for k, v in SPLITS.items()}, "counts": count_rows, "coverage": coverage, "prepanel_coverage": precoverage, "market_mapping": {"reference_start": str(LABEL_START.date()), "uniquely_mapped_codes": int(len(label_map)), "ambiguous_codes_excluded": int(label_count.gt(1).sum()), "legacy_rows_mapped": int((legacy_missing & factors.market_effective.isin(["KOSPI", "KOSDAQ"])).sum())}, "price_history_integrity": integrity, "operational_change": False, "broker_order": False}
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "variants": len(GRID), "selected_trades": int(sum(row["selected_trades"] for row in count_rows))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
