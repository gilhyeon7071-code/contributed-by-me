"""Research-only BULL/BEAR x actual v41_1 strategy x 511 signal subsets."""
from __future__ import annotations
import argparse, json, math, os, sys
from datetime import datetime
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("REPORT_ALLOW_UNAPPROVED_FALLBACK", "1")
import report_backtest_v41_1 as report

LOGS = ROOT / "2_Logs"
SIGNALS = tuple(report.CORE_SIGNAL_NAMES)
REGIMES = ("BULL", "BEAR")
STRATEGY = "V41_1_OPERATIONAL_NORMAL"
TRAIN_END = pd.Timestamp("2023-12-31")
VAL_START, VAL_END = pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")
OOS_START = pd.Timestamp("2026-01-01")

def names(cid: int) -> list[str]:
    return [s for bit, s in enumerate(SIGNALS) if cid & (1 << bit)]

def metrics(trades: pd.DataFrame) -> dict[str, Any]:
    keys = ("n","dates","mean_bps","median_bps","win_rate","profit_factor","daily_mean_bps","daily_ci95_low_bps","daily_p_one_sided","daily_mdd")
    if trades.empty:
        return dict.fromkeys(keys, None) | {"n": 0, "dates": 0}
    w = trades.copy()
    w["signal_date"] = pd.to_datetime(w["signal_date"], errors="coerce")
    w["ret"] = pd.to_numeric(w["ret"], errors="coerce")
    w = w.dropna(subset=["signal_date","ret"])
    r = w["ret"]
    daily = w.groupby("signal_date")["ret"].mean().sort_index()
    mean = float(daily.mean())
    if len(daily) > 1 and float(daily.std(ddof=1)) > 0:
        se = float(daily.std(ddof=1) / math.sqrt(len(daily)))
        p = 0.5 * math.erfc((mean / se) / math.sqrt(2.0))
        ci = mean - 1.96 * se
    else:
        p, ci = (0.5 if mean > 0 else 1.0), mean
    pos, neg = float(r[r > 0].sum()), float(-r[r < 0].sum())
    eq = (1.0 + daily).cumprod()
    return {
        "n": int(len(r)), "dates": int(len(daily)),
        "mean_bps": float(r.mean()*10000), "median_bps": float(r.median()*10000),
        "win_rate": float((r > 0).mean()),
        "profit_factor": float(pos/neg) if neg > 0 else (999.0 if pos > 0 else None),
        "daily_mean_bps": mean*10000, "daily_ci95_low_bps": ci*10000,
        "daily_p_one_sided": float(p),
        "daily_mdd": float((eq/eq.cummax()-1.0).min()) if len(eq) else None,
    }

def split_metrics(t: pd.DataFrame) -> dict[str, Any]:
    d = pd.to_datetime(t.get("signal_date", pd.Series(dtype=str)), errors="coerce")
    cuts = {
        "train": d <= TRAIN_END,
        "val": d.between(VAL_START, VAL_END),
        "val_2024": d.between("2024-01-01","2024-12-31"),
        "val_2025": d.between("2025-01-01","2025-12-31"),
    }
    out = {}
    for split, mask in cuts.items():
        for k,v in metrics(t.loc[mask].copy()).items():
            out[f"{split}_{k}"] = v
    return out

def bh(p: pd.Series) -> pd.Series:
    x = pd.to_numeric(p, errors="coerce").fillna(1.0).clip(0,1).to_numpy()
    order = np.argsort(x); ranked = x[order]
    q = np.minimum.accumulate((ranked*len(x)/np.arange(1,len(x)+1))[::-1])[::-1].clip(0,1)
    out = np.empty(len(x)); out[order] = q
    return pd.Series(out, index=p.index)

def score(r: pd.Series) -> float:
    daily = [r.get(f"{x}_daily_mean_bps") for x in ("train","val","val_2024","val_2025")]
    pf = [r.get("train_profit_factor"), r.get("val_profit_factor")]
    if any(pd.isna(x) for x in daily+pf): return -1e9
    return min(map(float,daily)) + .25*np.mean(daily) + 8*math.log(max(min(map(float,pf)),.01)) - 1.5*r["signal_count"]

def choose(g: pd.DataFrame):
    sample = g.train_n.ge(30)&g.train_dates.ge(20)&g.val_n.ge(20)&g.val_dates.ge(15)&g.val_2024_n.ge(5)&g.val_2025_n.ge(5)
    robust = sample&g.train_daily_ci95_low_bps.gt(0)&g.val_daily_ci95_low_bps.gt(0)&g.train_profit_factor.gt(1)&g.val_profit_factor.gt(1)&g.train_q_value.le(.1)&g.val_q_value.le(.1)
    pool, status = g.loc[robust], "TRAIN_VAL_MULTIPLE_TEST_PASS"
    if pool.empty: pool, status = g.loc[sample], "BEST_AVAILABLE_NOT_APPROVED"
    if pool.empty: pool, status = g, "INSUFFICIENT_SAMPLE"
    return pool.sort_values(["selection_score","signal_count","val_n"],ascending=[False,True,False]).iloc[0], status

def add_market_regime(data: pd.DataFrame) -> pd.DataFrame:
    overall = data.groupby("date")["close"].mean().sort_index()
    overall_bull = overall > overall.rolling(60).mean()
    idx = data[data["market"].astype(str).str.strip().ne("")].groupby(["market","date"],as_index=False)["close"].mean().sort_values(["market","date"])
    idx["ma60"] = idx.groupby("market")["close"].transform(lambda x: x.rolling(60).mean())
    idx["market_regime"] = np.where((idx["close"] > idx["ma60"]).fillna(False), "BULL", "BEAR")
    out = data.merge(idx[["market","date","market_regime"]], on=["market","date"], how="left", sort=False)
    fallback = np.where(out["date"].map(overall_bull).fillna(True), "BULL", "BEAR")
    out["market_regime"] = out["market_regime"].fillna(pd.Series(fallback, index=out.index))
    return out


def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--max-combo",type=int,default=511); args=ap.parse_args()
    max_combo=min(args.max_combo,511); LOGS.mkdir(exist_ok=True)
    print("[ACTUAL] load/compute")
    data=add_market_regime(report.compute_factors(report.load_data())); params=report.load_params()
    if not params.entry_enabled: raise RuntimeError(params.entry_block_reason)
    context=report.prepare_simulation_context(data)
    print(f"[ACTUAL] rows={len(data):,} dates={data.date.nunique():,} source={params.param_gate.get('param_source')}")
    rows=[]
    for regime in REGIMES:
        for cid in range(1,max_combo+1):
            sigs=names(cid)
            t=report.simulate_trades(data,params,{"required_signals":sigs,"regimes":[regime],"signal_date_start":"2021-01-01","signal_date_end":VAL_END},context)
            row={"regime":regime,"strategy":STRATEGY,"combo_id":cid,"signal_count":len(sigs),"signals":"|".join(sigs)}
            row.update(split_metrics(t)); rows.append(row)
            if cid%10==0 or cid==max_combo: print(f"[ACTUAL] {regime} {cid}/{max_combo}",flush=True)
    grid=pd.DataFrame(rows); winners=[]; oos_rows=[]
    for regime in REGIMES:
        idx=grid.regime.eq(regime)
        grid.loc[idx,"train_q_value"]=bh(grid.loc[idx,"train_daily_p_one_sided"])
        grid.loc[idx,"val_q_value"]=bh(grid.loc[idx,"val_daily_p_one_sided"])
        grid.loc[idx,"selection_score"]=grid.loc[idx].apply(score,axis=1)
        win,status=choose(grid.loc[idx]); cid=int(win.combo_id); sigs=names(cid)
        oos=report.simulate_trades(data,params,{"required_signals":sigs,"regimes":[regime],"signal_date_start":OOS_START},context)
        rec=win.to_dict()|{"selection_status":status}|{f"oos_{k}":v for k,v in metrics(oos).items()}
        rec["oos_verdict"]="OOS_PASS" if status=="TRAIN_VAL_MULTIPLE_TEST_PASS" and rec["oos_n"]>=10 and (rec["oos_profit_factor"] or 0)>1 and (rec["oos_daily_mean_bps"] or -1)>0 else "OOS_FAIL_OR_NOT_PROMOTABLE"
        winners.append(rec)
        if not oos.empty:
            oos.insert(0,"winner_combo_id",cid); oos.insert(0,"strategy",STRATEGY); oos.insert(0,"regime_test",regime); oos_rows.append(oos)
    paths={
      "grid":LOGS/"actual_strategy_signal_combination_grid_latest.csv",
      "summary":LOGS/"actual_strategy_signal_combination_summary_latest.csv",
      "count":LOGS/"actual_strategy_signal_count_summary_latest.csv",
      "oos":LOGS/"actual_strategy_signal_winner_oos_trades_latest.csv",
    }
    grid.to_csv(paths["grid"],index=False,encoding="utf-8-sig")
    summary=pd.DataFrame(winners); summary.to_csv(paths["summary"],index=False,encoding="utf-8-sig")
    count=grid.groupby(["regime","strategy","signal_count"]).agg(combinations=("combo_id","count"),best_selection_score=("selection_score","max"),median_val_daily_mean_bps=("val_daily_mean_bps","median"),best_val_profit_factor=("val_profit_factor","max"),median_val_n=("val_n","median")).reset_index()
    count.to_csv(paths["count"],index=False,encoding="utf-8-sig")
    (pd.concat(oos_rows,ignore_index=True) if oos_rows else pd.DataFrame()).to_csv(paths["oos"],index=False,encoding="utf-8-sig")
    payload={
      "generated_at":datetime.now().isoformat(timespec="seconds"),"status":"PASS",
      "scope":"research_only_actual_strategy_signal_combination_validation",
      "definitions":{"regimes":list(REGIMES),"strategies":[STRATEGY],"strategy_caveat":"Only v41_1 operational normal shares all nine signals.","signals":list(SIGNALS),"combinations_per_regime":max_combo,"selection":"TRAIN/VAL only; OOS only after winner fixed.","multiple_testing":"BH q-values by regime."},
      "params_gate":params.param_gate,
      "data":{"rows":len(data),"dates":data.date.nunique(),"date_min":str(data.date.min().date()),"date_max":str(data.date.max().date())},
      "winners":winners,"outputs":{k:str(v) for k,v in paths.items()},"policy_effect":False,"order_path_effect":False,
    }
    out=LOGS/"actual_strategy_signal_validation_latest.json"; out.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    print(summary[["regime","signals","selection_status","oos_verdict","train_n","val_n","oos_n"]].to_string(index=False)); print(f"[OK] {out}")
    return 0
if __name__=="__main__": raise SystemExit(main())
