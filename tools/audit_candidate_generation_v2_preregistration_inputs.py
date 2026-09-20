"""Read-only input inventory for Candidate-Generation Validation Guard V2."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
AXIS_PATH = ROOT / "tools" / "build_new_method_strategy_axis_matrix.py"
PREFIX = LOG_DIR / "candidate_generation_v2_preregistration_inventory"


def _axis_module():
    spec = importlib.util.spec_from_file_location("candidate_axis", AXIS_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load candidate-axis module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _coverage(frame: pd.DataFrame, name: str) -> dict[str, object]:
    series = frame[name]
    return {
        "axis_type": "STRATEGY" if name in {"MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D"} else "SIGNAL",
        "axis_name": name,
        "non_null_rows": int(series.notna().sum()),
        "true_rows": int(series.fillna(False).astype(bool).sum()),
        "unique_signal_dates": int(frame.loc[series.fillna(False).astype(bool), "date"].nunique()),
    }


def main() -> int:
    axis = _axis_module()
    report = axis._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["research_regime"] = factors["date"].map(report._assign_report_research_regime(factors))
    factors = axis._strategy_features(factors)
    factors = axis._atomic_signals(factors)
    factors = factors.sort_values(["date", "code"], kind="mergesort").copy()

    market_source = factors["market"].fillna("").astype(str).str.upper().str.strip()
    market_rows = (
        pd.DataFrame({"market_source": market_source})
        .value_counts(dropna=False)
        .rename("rows")
        .reset_index()
        .sort_values(["rows", "market_source"], ascending=[False, True])
    )
    regime_rows = (
        factors.groupby("research_regime", dropna=False)
        .agg(rows=("code", "size"), unique_dates=("date", "nunique"), unique_codes=("code", "nunique"))
        .reset_index()
        .sort_values("research_regime", na_position="last")
    )
    axis_names = ["MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D", "STRETCH_Q1", "RS_Q5", "RS_SLOPE_Q5", "V_ACCEL_Q5"]
    axis_rows = pd.DataFrame([_coverage(factors, name) for name in axis_names])
    preregistration = [
        {"field": "population_and_exclusions", "status": "UNRESOLVED", "reason": "market labels include legacy blanks/KRX and no V2 population rule is fixed"},
        {"field": "primary_horizon", "status": "UNRESOLVED", "reason": "must follow intended holding period, not prior results"},
        {"field": "daily_candidate_basket_and_capacity", "status": "UNRESOLVED", "reason": "candidate count and equal-weight construction require pre-registration"},
        {"field": "combination_list_and_baselines", "status": "UNRESOLVED", "reason": "must be frozen before exploration"},
        {"field": "CPCV_PBO_DSR_and_purge_embargo", "status": "UNRESOLVED", "reason": "requires primary horizon and return-series construction"},
        {"field": "minimum_unique_signal_dates", "status": "UNRESOLVED", "reason": "must be fixed per split and cell before confirmation"},
        {"field": "regime_truncate_replay", "status": "REQUIRED", "reason": "current rule appears trailing but has no replay regression artifact"},
    ]
    factor_columns = ["rs", "rs_slope", "stretch", "v_accel", "rsi14", "atr_pct", "value", "market_regime", "research_regime"]
    factor_rows = pd.DataFrame([
        {"field": field, "present": field in factors.columns, "non_null_rows": int(factors[field].notna().sum()) if field in factors.columns else 0}
        for field in factor_columns
    ])

    market_rows.to_csv(f"{PREFIX}_market_coverage_latest.csv", index=False, encoding="utf-8-sig")
    regime_rows.to_csv(f"{PREFIX}_regime_coverage_latest.csv", index=False, encoding="utf-8-sig")
    axis_rows.to_csv(f"{PREFIX}_axis_coverage_latest.csv", index=False, encoding="utf-8-sig")
    factor_rows.to_csv(f"{PREFIX}_factor_coverage_latest.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(preregistration).to_csv(f"{PREFIX}_unresolved_latest.csv", index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "round_type": "PREREGISTRATION_PREPARATION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "price_history_integrity": integrity,
        "raw_coverage": {
            "rows": int(len(raw)), "unique_dates": int(pd.to_datetime(raw["date"], errors="coerce").nunique()),
            "unique_codes": int(raw["code"].nunique()), "date_min": str(factors["date"].min().date()), "date_max": str(factors["date"].max().date()),
        },
        "axis_names": axis_names,
        "market_coverage": market_rows.to_dict(orient="records"),
        "regime_coverage": regime_rows.to_dict(orient="records"),
        "axis_coverage": axis_rows.to_dict(orient="records"),
        "factor_coverage": factor_rows.to_dict(orient="records"),
        "unresolved_preregistration": preregistration,
    }
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "round_type": payload["round_type"], "unresolved": len(preregistration)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
