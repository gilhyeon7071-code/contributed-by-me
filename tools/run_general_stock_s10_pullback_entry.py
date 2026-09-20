"""Read-only signal-close pullback entry diagnostic for structural S10 candidates."""
from __future__ import annotations
import hashlib, importlib.util, json
from datetime import datetime
from pathlib import Path
from typing import Any
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
LOG_DIR=ROOT/'2_Logs'
DIAG_PATH=ROOT/'tools'/'run_general_stock_s10_execution_diagnostic.py'
OUT_PREFIX='general_stock_s10_pullback_entry_20260720'

def sha256(path:Path)->str: return hashlib.sha256(path.read_bytes()).hexdigest()
def load_diag()->Any:
    spec=importlib.util.spec_from_file_location('s10_pullback_diag',DIAG_PATH)
    if spec is None or spec.loader is None: raise RuntimeError(f'cannot import {DIAG_PATH}')
    m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m
def records(df:pd.DataFrame)->list[dict[str,object]]: return df.astype(object).where(pd.notna(df),None).to_dict(orient='records')

def add_pullback_returns(u:pd.DataFrame,horizons:tuple[int,...],buy:float,sell:float)->pd.DataFrame:
    out=u.copy()
    lookup=out[['_entity_key','date','open','low']].rename(columns={'date':'entry_date','open':'next_open','low':'next_low'})
    out=out.merge(lookup,on=['_entity_key','entry_date'],how='left',validate='many_to_one')
    out['pullback_limit']=out['close']
    out['pullback_filled']=out['next_open'].le(out['pullback_limit']) | (out['next_low'].le(out['pullback_limit']) & out['next_open'].gt(out['pullback_limit']))
    out['pullback_entry']=out['pullback_limit']
    out.loc[out['next_open'].le(out['pullback_limit']),'pullback_entry']=out.loc[out['next_open'].le(out['pullback_limit']),'next_open']
    for h in horizons:
        c=f'pullback_net_h{h}'; exit_c=f'exit_close_h{h}'
        ok=out['pullback_filled']&out['pullback_entry'].gt(0)&out[exit_c].gt(0)
        out[c]=((out[exit_c]*(1-sell))/(out['pullback_entry']*(1+buy))-1).where(ok)
    return out

def summarize(valid:pd.DataFrame,col:str,partition:str)->dict[str,object]:
    cohort=valid.loc[valid['in_structural_cohort']&valid['strength_decile'].eq(10)].copy()
    filled=cohort.loc[cohort[col].notna()].copy()
    total_dates=int(cohort['date'].nunique())
    if filled.empty: return {'partition':partition,'horizon':col.replace('pullback_net_',''),'signal_dates':0,'candidate_rows':0,'fill_rate':0.0,'avg_constituents':None,'mean_daily_net_return_bps':None,'positive_daily_net_rate':None,'status':'DEFERRED_INSUFFICIENT_SAMPLE'}
    daily=filled.groupby('date',as_index=False).agg(net_return=(col,'mean'),constituent_count=(col,'size'))
    return {'partition':partition,'horizon':col.replace('pullback_net_',''),'signal_dates':int(len(daily)),'candidate_rows':int(len(filled)),'fill_rate':round(float(len(filled)/len(cohort)),6),'avg_constituents':round(float(daily['constituent_count'].mean()),4),'mean_daily_net_return_bps':round(float(daily['net_return'].mean()*10000),4),'positive_daily_net_rate':round(float(daily['net_return'].gt(0).mean()),6),'status':'DESCRIPTIVE' if len(daily)>=30 else 'DEFERRED_INSUFFICIENT_SAMPLE'}

def main()->int:
    d=load_diag(); strength=d.load_strength_runner(); base=strength.load_base(); report=base.load_report()
    print('[1/4] loading integrity-filtered price history')
    raw=report.load_data(); integrity=raw.attrs.get('price_history_integrity',{})
    print('[2/4] building S10 and next-session pullback fills')
    u=base.build_forward_panel(raw); u=base.attach_point_in_time_universe(u); u=base.add_observable_states(u); u=strength.add_strength_signal(u); u=add_pullback_returns(u,base.HORIZONS,d.BUY_COST,d.SELL_COST)
    print('[3/4] calculating daily equal-weight filled baskets')
    rows=[]
    for part,start,end in base.PARTITIONS:
        seg=u.loc[u['date'].between(pd.Timestamp(start),pd.Timestamp(end))]
        for h in base.HORIZONS: rows.append(summarize(seg,f'pullback_net_h{h}',part))
    summary=pd.DataFrame(rows).sort_values(['partition','horizon']).reset_index(drop=True)
    print('[4/4] writing research-only evidence')
    csvp=LOG_DIR/f'{OUT_PREFIX}_summary.csv'; jsp=LOG_DIR/f'{OUT_PREFIX}.json'; mdp=LOG_DIR/f'{OUT_PREFIX}.md'; summary.to_csv(csvp,index=False,encoding='utf-8-sig')
    payload={'generated_at':datetime.now().isoformat(timespec='seconds'),'status':'OK','scope':'read_only_s10_signal_close_pullback_entry','exploratory_only':True,'price_history_contract':integrity,'entry':'next actual session signal-close limit: next open if <= limit, otherwise limit if next low <= limit','costs':{'buy_cost':d.BUY_COST,'sell_cost':d.SELL_COST},'primary':'h60 daily equal-weight filled S10 net-return basket','input':{'universe_rows':int(len(u)),'runner_sha256':sha256(Path(__file__)),'diagnostic_sha256':sha256(DIAG_PATH)},'summary':records(summary),'operational_change':False,'promotion':'FORBIDDEN_EXPLORATORY_ONLY'}
    jsp.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    mdp.write_text('# S10 Pullback Entry Diagnostic — Exploratory\n\n```csv\n'+summary.loc[(summary.partition=='ALL_AVAILABLE')&(summary.horizon=='h60')].to_csv(index=False)+'```\n',encoding='utf-8')
    print(json.dumps({'status':'OK','summary':str(csvp),'json':str(jsp),'markdown':str(mdp)},ensure_ascii=False)); return 0
if __name__=='__main__': raise SystemExit(main())
