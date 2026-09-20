"""Read-only validation of the feasible single-stock strategy library."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BASE_PATH = ROOT / "tools" / "build_new_method_strategy_axis_matrix.py"
OUT_DEFS = LOG_DIR / "new_method_strategy_library_definitions_latest.csv"
OUT_PERIOD = LOG_DIR / "new_method_strategy_library_period_latest.csv"
OUT_REPEAT = LOG_DIR / "new_method_strategy_library_repeat_latest.csv"
OUT_JSON = LOG_DIR / "new_method_strategy_library_latest.json"
OUT_MD = LOG_DIR / "new_method_strategy_library_latest.md"


def _base():
    spec = importlib.util.spec_from_file_location("strategy_axis_base", BASE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load strategy-axis base")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _safe(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def main() -> int:
    base = _base()
    report = base._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["research_regime"] = factors["date"].map(report._assign_report_research_regime(factors))
    factors = base._strategy_features(factors)
    factors = base._atomic_signals(factors)
    factors = base._exact_returns(factors)
    factors = base._add_periods(factors)
    panel = factors[
        factors["date"].between(base.START, base.END) & factors["period"].notna()
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0) & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy().sort_values(["price_history_key", "price_session_index"], kind="mergesort")
    g = panel.groupby("price_history_key", sort=False)
    prev_high20 = g["close"].transform(lambda x: x.rolling(20, min_periods=20).max().shift(1))
    prev_high60 = g["close"].transform(lambda x: x.rolling(60, min_periods=60).max().shift(1))
    low14 = g["low"].transform(lambda x: x.rolling(14, min_periods=14).min())
    high14 = g["high"].transform(lambda x: x.rolling(14, min_periods=14).max())
    panel["stoch_k14"] = 100 * (panel["close"] - low14) / (high14 - low14 + 1e-9)
    panel["stoch_d3"] = g["stoch_k14"].transform(lambda x: x.rolling(3, min_periods=3).mean())
    prev_k = g["stoch_k14"].shift(1)
    prev_d = g["stoch_d3"].shift(1)
    value_rank = panel["value"].groupby(panel["date"], sort=False).rank(method="first", pct=True)
    atr_rank = panel["atr_pct"].groupby(panel["date"], sort=False).rank(method="first", pct=True)
    rules = {
        "RSI_OVERSOLD": panel["rsi14"].le(30),
        "BOLLINGER_MR": panel["MR_BOLLINGER"],
        "Z_SCORE_MR": panel["strategy_z20"].le(-1.5),
        "STOCH_OVERSOLD_CROSS": panel["stoch_k14"].lt(20) & panel["stoch_k14"].gt(panel["stoch_d3"]) & prev_k.le(prev_d),
        "SIMPLE_MOMENTUM": panel["RS_Q5"],
        "MACD_GOLDEN": panel["macd_golden"].fillna(False),
        "MA_CROSS_UP": panel["MA_CROSS_UP"],
        "VOLUME_BREAKOUT": panel["close"].gt(prev_high20) & panel["V_ACCEL_Q5"],
        "SUPPORT_RESISTANCE_BREAK": panel["close"].gt(prev_high60),
        "BREAKOUT_252D": panel["BREAKOUT_252D"],
        "MOMENTUM_LOW_VOL": panel["RS_Q5"] & atr_rank.le(0.20),
        "MULTIFACTOR_PROXY": panel["RS_Q5"] & value_rank.ge(0.80) & atr_rank.le(0.20),
    }
    definitions = pd.DataFrame([
        ["BUY_HOLD", "full-universe equal-weight benchmark", "BENCHMARK_ONLY"],
        ["DCA", "periodic capital-allocation benchmark; no per-stock entry signal", "BENCHMARK_ONLY"],
        ["RSI_OVERSOLD", "RSI14 <= 30", "VALIDATED_HYPOTHESIS"],
        ["BOLLINGER_MR", "20-day z <= -1.5 AND RSI14 <= 30", "VALIDATED_HYPOTHESIS"],
        ["Z_SCORE_MR", "20-day z <= -1.5", "VALIDATED_HYPOTHESIS"],
        ["STOCH_OVERSOLD_CROSS", "%K14 < 20 and bullish %K/%D cross", "VALIDATED_HYPOTHESIS"],
        ["SIMPLE_MOMENTUM", "same-date RS top quintile", "VALIDATED_HYPOTHESIS"],
        ["MACD_GOLDEN", "MACD signal-line bullish cross", "VALIDATED_HYPOTHESIS"],
        ["MA_CROSS_UP", "MA20 crosses above MA60", "VALIDATED_HYPOTHESIS"],
        ["VOLUME_BREAKOUT", "20-day price high breakout AND volume acceleration top quintile", "VALIDATED_HYPOTHESIS"],
        ["SUPPORT_RESISTANCE_BREAK", "60-day resistance breakout proxy", "VALIDATED_HYPOTHESIS"],
        ["BREAKOUT_252D", "252-day breakout", "VALIDATED_HYPOTHESIS"],
        ["MOMENTUM_LOW_VOL", "RS top quintile AND ATR bottom quintile", "VALIDATED_HYPOTHESIS"],
        ["MULTIFACTOR_PROXY", "RS top quintile + value top quintile + ATR bottom quintile; quality unavailable", "PARTIAL_PROXY"],
        ["BREAKOUT_ATR_EXIT", "requires explicit ATR stop/take-profit exit simulation", "EXIT_MODEL_REQUIRED"],
        ["W_M_PATTERN", "requires formal swing-pattern detector", "FORMALIZATION_REQUIRED"],
        ["PAIR_TRADING", "requires pair/spread/cointegration panel", "SEPARATE_DATA_REQUIRED"],
        ["MACHINE_LEARNING", "requires target, features, embargoed walk-forward model protocol", "SEPARATE_MODEL_REQUIRED"],
    ], columns=["strategy_name", "research_rule", "validation_status"])
    frames=[]; full=pd.Series(True,index=panel.index)
    for horizon in base.HORIZONS:
        for name, mask in rules.items():
            assigned=panel.assign(strategy_name=name)
            frames.append(base._summarize_candidate(assigned,mask,full,[],["strategy_name"],"STRATEGY_ONLY",horizon))
            frames.append(base._summarize_candidate(assigned,mask,full,["research_regime"],["research_regime","strategy_name"],"REGIME_STRATEGY",horizon))
    period=pd.concat([x for x in frames if not x.empty],ignore_index=True,sort=False)
    ids=["scope","horizon","research_regime","strategy_name","baseline_type"]
    for c in ids:
        if c not in period: period[c]=pd.NA
    period=period[["period",*ids,"candidate_rows","unique_dates","unique_codes","avg_return","avg_excess_return","median_excess_return","avg_daily_equal_weight_excess","median_daily_equal_weight_excess","positive_excess_dates","baseline_closed_rows"]]
    if period.duplicated(["period",*ids]).any(): raise SystemExit("duplicate strategy-library period keys")
    repeats=[]
    for key,group in period.groupby(ids,dropna=False):
        tr=pd.to_numeric(group.loc[group.period.isin(["P1_202506_202509","P2_202510_202512","P3_202601_202603"]),"avg_daily_equal_weight_excess"],errors="coerce").dropna()
        p4=pd.to_numeric(group.loc[group.period.eq("P4_202604_202606"),"avg_daily_equal_weight_excess"],errors="coerce").dropna()
        tm=float(tr.mean()) if len(tr) else np.nan; hm=float(p4.mean()) if len(p4) else np.nan
        repeats.append({**dict(zip(ids,key)),"available_periods":int(group.period.nunique()),"train_periods":int(len(tr)),"train_positive_periods":int((tr>0).sum()),"train_mean_daily_excess":tm,"holdout_p4_daily_excess":hm,"evidence_state":"TIME_ORDERED_REPEAT_POSITIVE" if len(tr)>=2 and (tr>0).all() and hm>0 else "MIXED_OR_INSUFFICIENT"})
    repeat=pd.DataFrame(repeats)
    definitions.to_csv(OUT_DEFS,index=False,encoding="utf-8-sig"); period.to_csv(OUT_PERIOD,index=False,encoding="utf-8-sig"); repeat.to_csv(OUT_REPEAT,index=False,encoding="utf-8-sig")
    payload={"generated_at":datetime.now().isoformat(timespec="seconds"),"scope":"read_only_strategy_library_validation","window":{"start":str(base.START.date()),"end":str(base.END.date())},"price_history_contract":integrity,"row_counts":{"panel_rows":int(len(panel)),"period_rows":int(len(period)),"repeat_rows":int(len(repeat)),"time_ordered_positive":int((repeat.evidence_state=="TIME_ORDERED_REPEAT_POSITIVE").sum())},"definitions":_safe(definitions),"operational_change":False,"limitations":["No operating gates/orders/fills used.","Buy & Hold and DCA are benchmarks, not per-stock alpha signals.","Pair trading, formal patterns, ATR exit model, and ML require additional data or a separate protocol.","Multiple strategies are tested; positive rows are candidates for later implementation judgment, not final rules."]}
    OUT_JSON.write_text(json.dumps(payload,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
    pos=repeat[repeat.evidence_state.eq("TIME_ORDERED_REPEAT_POSITIVE")]
    lines=["# New Method Strategy Library Validation","",f"- window: {payload['window']['start']} to {payload['window']['end']}",f"- panel_rows: {payload['row_counts']['panel_rows']}",f"- time_ordered_positive: {len(pos)}","","## Time-ordered positive strategies"]
    for _,row in pos[pos.scope.eq("STRATEGY_ONLY")].sort_values("strategy_name").iterrows(): lines.append(f"- {row['strategy_name']} / {row['horizon']}: train={float(row['train_mean_daily_excess']):.6f}, P4={float(row['holdout_p4_daily_excess']):.6f}")
    lines.extend(["","## Caveats",*[f"- {x}" for x in payload['limitations']]])
    OUT_MD.write_text("\n".join(lines)+"\n",encoding="utf-8")
    print(json.dumps({"status":"OK","definitions":str(OUT_DEFS),"period":str(OUT_PERIOD),"repeat":str(OUT_REPEAT),"json":str(OUT_JSON),"md":str(OUT_MD)},ensure_ascii=False))

if __name__=="__main__": main()
