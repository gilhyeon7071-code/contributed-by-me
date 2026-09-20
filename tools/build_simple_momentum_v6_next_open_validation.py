"""Research-only V6 next-open validation for simple momentum."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
V5_PATH = ROOT / "tools" / "build_ma_cross_v5_next_open_validation.py"
CONTRACT_PATH = LOG_DIR / "strategy_execution_contract_v6_simple_momentum_next_open.json"
PREFIX = LOG_DIR / "simple_momentum_v6_next_open"
SPLITS = {"TRAIN_202102_202312": (pd.Timestamp("2021-02-01"), pd.Timestamp("2023-12-29")), "VAL_202401_202412": (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-30")), "OOS_202501_202505": (pd.Timestamp("2025-01-02"), pd.Timestamp("2025-05-30"))}


def _v5():
    spec = importlib.util.spec_from_file_location("v5_execution", V5_PATH)
    if spec is None or spec.loader is None: raise RuntimeError("cannot load V5 execution helpers")
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module


def _summary(chosen: pd.DataFrame, horizon: str) -> list[dict[str, object]]:
    return [{"strategy": "SIMPLE_MOMENTUM", "horizon": horizon, "selection_variant": "ALL_SIGNAL_ROWS", "split": split, "trades": int(len(group)), "avg_gross": float(group.gross_return.mean()), "avg_net": float(group.net_return.mean()), "median_net": float(group.net_return.median()), "win_rate": float(group.net_return.gt(0).mean()), "avg_same_day_baseline_gross": float(group.same_day_baseline_gross.mean()), "avg_same_day_baseline_net": float(group.same_day_baseline_net.mean()), "avg_gross_excess_vs_same_day": float(group.gross_excess_vs_same_day.mean()), "avg_net_excess_vs_same_day": float(group.net_excess_vs_same_day.mean())} for split, group in chosen.groupby("v6_split", sort=True)]


def main() -> int:
    contract, helper = json.loads(CONTRACT_PATH.read_text(encoding="utf-8")), _v5()
    lib = helper._library(); base = lib._base(); report = base._load_report_module(); raw = report.load_data(); integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw); factors["date"] = pd.to_datetime(factors.date, errors="coerce").dt.normalize(); factors = base._strategy_features(factors); factors = base._atomic_signals(factors); factors = helper._returns(factors)
    factors["market_source"] = factors.market.fillna("").astype(str).str.upper().str.strip(); labeled = factors.loc[factors.date.ge(helper.LABEL_START) & factors.market_source.isin(["KOSPI", "KOSDAQ"]), ["code", "market_source"]].drop_duplicates(); label_count = labeled.groupby("code", sort=False).market_source.nunique(); label_map = labeled.loc[labeled.code.isin(label_count[label_count.eq(1)].index)].drop_duplicates("code").set_index("code").market_source
    factors["market_effective"] = factors.market_source; legacy_missing = factors.market_effective.isin(["", "KRX"]); factors.loc[legacy_missing, "market_effective"] = factors.loc[legacy_missing, "code"].map(label_map).fillna(""); factors["v6_split"] = pd.NA
    for label, (start, end) in SPLITS.items(): factors.loc[factors.date.between(start, end), "v6_split"] = label
    panel = factors.loc[factors.v6_split.notna() & factors.market_effective.isin(["KOSPI", "KOSDAQ"]) & pd.to_numeric(factors.close, errors="coerce").gt(0) & pd.to_numeric(factors.open, errors="coerce").gt(0) & pd.to_numeric(factors.value, errors="coerce").gt(0)].copy(); signal = panel.RS_Q5.fillna(False)
    coverage = [{"split": split, "panel_rows": int(len(group)), "unique_dates": int(group.date.nunique()), "unique_codes": int(group.code.nunique()), "simple_momentum_signals": int(signal.loc[group.index].sum()), "valid_next_open_h1": int(group.v5_return_h1.notna().sum()), "valid_next_open_h2": int(group.v5_return_h2.notna().sum()), "valid_next_open_h5": int(group.v5_return_h5.notna().sum())} for split, group in panel.groupby("v6_split", sort=True)]
    summary_rows, diag_rows, count_rows, selected_frames = [], [], [], []
    for horizon, _ in helper.HORIZONS:
        col = f"v5_return_{horizon}"; valid = pd.to_numeric(panel[col], errors="coerce").notna(); chosen = panel.loc[signal & valid].copy(); baseline = panel.loc[valid].groupby("date", sort=False)[col].mean(); chosen["gross_return"] = chosen[col]; chosen["net_return"] = helper._net(chosen.gross_return); chosen["same_day_baseline_gross"] = chosen.date.map(baseline); chosen["same_day_baseline_net"] = helper._net(chosen.same_day_baseline_gross); chosen["gross_excess_vs_same_day"] = chosen.gross_return - chosen.same_day_baseline_gross; chosen["net_excess_vs_same_day"] = chosen.net_return - chosen.same_day_baseline_net; chosen["horizon"], chosen["selection_variant"] = horizon, "ALL_SIGNAL_ROWS"; summary_rows.extend(_summary(chosen, horizon)); oos = chosen.loc[chosen.v6_split.eq("OOS_202501_202505")].sort_values("net_return", ascending=False); trimmed = oos.iloc[5:]; diag_rows.append({"strategy": "SIMPLE_MOMENTUM", "horizon": horizon, "selection_variant": "ALL_SIGNAL_ROWS", "trades": int(len(oos)), "avg_net": float(oos.net_return.mean()) if len(oos) else None, "median_net": float(oos.net_return.median()) if len(oos) else None, "win_rate": float(oos.net_return.gt(0).mean()) if len(oos) else None, "top5_net_sum": float(oos.head(5).net_return.sum()) if len(oos) else None, "avg_net_excluding_top5": float(trimmed.net_return.mean()) if len(trimmed) else None}); selected_frames.append(chosen); count_rows.append({"horizon": horizon, "selection_variant": "ALL_SIGNAL_ROWS", "selected_trades": int(len(chosen)), "unique_dates": int(chosen.date.nunique()), "max_trades_per_date": int(chosen.groupby("date", sort=False).size().max()) if len(chosen) else 0})
    pd.DataFrame(summary_rows).to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig"); pd.concat(selected_frames, ignore_index=True).to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig"); pd.DataFrame(count_rows).to_csv(f"{PREFIX}_counts_latest.csv", index=False, encoding="utf-8-sig"); pd.DataFrame(diag_rows).to_csv(f"{PREFIX}_oos_diagnostic_latest.csv", index=False, encoding="utf-8-sig"); pd.DataFrame(coverage).to_csv(f"{PREFIX}_coverage_latest.csv", index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "contract_version": contract["contract_version"], "splits": {k: [str(v[0].date()), str(v[1].date())] for k, v in SPLITS.items()}, "counts": count_rows, "coverage": coverage, "market_mapping": {"reference_start": str(helper.LABEL_START.date()), "uniquely_mapped_codes": int(len(label_map)), "ambiguous_codes_excluded": int(label_count.gt(1).sum())}, "price_history_integrity": integrity, "operational_change": False, "broker_order": False}; Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"); print(json.dumps({"status": "OK", "variants": len(helper.HORIZONS), "selected_trades": int(sum(x["selected_trades"] for x in count_rows))}, ensure_ascii=False)); return 0


if __name__ == "__main__": raise SystemExit(main())
