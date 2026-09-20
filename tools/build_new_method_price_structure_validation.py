"""Read-only historical validation for three price-structure strategy hypotheses."""
from __future__ import annotations
import importlib.util, json
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]; LOG=ROOT/'2_Logs'; BASE=ROOT/'tools'/'build_new_method_strategy_axis_matrix.py'
OUT_D=LOG/'new_method_price_structure_definitions_latest.csv'; OUT_P=LOG/'new_method_price_structure_period_latest.csv'; OUT_R=LOG/'new_method_price_structure_repeat_latest.csv'; OUT_J=LOG/'new_method_price_structure_latest.json'; OUT_M=LOG/'new_method_price_structure_latest.md'
def load_base():
 s=importlib.util.spec_from_file_location('price_structure_base',BASE); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
def records(x): return x.astype(object).where(pd.notna(x),None).to_dict(orient='records')
def main():
 b=load_base(); report=b._load_report_module(); raw=report.load_data(); integrity=raw.attrs.get('price_history_integrity',{})
 f=report.compute_factors(raw); f['date']=pd.to_datetime(f['date'],errors='coerce').dt.normalize(); f['research_regime']=f['date'].map(report._assign_report_research_regime(f)); f=b._exact_returns(f); f=b._add_periods(f)
 p=f[f.date.between(b.START,b.END)&f.period.notna()&f.market.astype(str).str.upper().isin(['KOSPI','KOSDAQ'])&(pd.to_numeric(f.close,errors='coerce')>0)&(pd.to_numeric(f.value,errors='coerce')>0)].copy().sort_values(['price_history_key','price_session_index'],kind='mergesort')
 g=p.groupby('price_history_key',sort=False); prev_close=g.close.shift(1); prev_high20=g.close.transform(lambda x:x.rolling(20,min_periods=20).max().shift(1));
 p['gap_pct']=p.open/(prev_close+1e-9)-1; p['ret20_residual']=p.ret_20-p.m_ret_20
 width=g.close.transform(lambda x:x.rolling(20,min_periods=20).std(ddof=0)/(x.rolling(20,min_periods=20).mean()+1e-9)); width_rank=width.groupby(p.date,sort=False).rank(method='first',pct=True); gap_rank=p.gap_pct.groupby(p.date,sort=False).rank(method='first',pct=True); residual_rank=p.ret20_residual.groupby(p.date,sort=False).rank(method='first',pct=True)
 rules={
  'SQUEEZE_VOLUME_BREAK':(width_rank<=.2)&(p.v_accel.groupby(p.date,sort=False).rank(method='first',pct=True)>=.8)&p.close.gt(prev_high20),
  'GAP_CONTINUATION':(gap_rank>=.8)&p.close.gt(p.open)&(p.v_accel.groupby(p.date,sort=False).rank(method='first',pct=True)>=.8),
  'GAP_REVERSAL':(gap_rank<=.2)&p.close.gt(p.open),
  'RESIDUAL_MOMENTUM':residual_rank>=.8,
 }
 defs=pd.DataFrame([[k,{'SQUEEZE_VOLUME_BREAK':'20-day volatility bottom quintile + volume acceleration top quintile + 20-day breakout','GAP_CONTINUATION':'top-quintile opening gap + positive close-to-open + volume acceleration top quintile','GAP_REVERSAL':'bottom-quintile opening gap + positive close-to-open','RESIDUAL_MOMENTUM':'20-day stock return minus market return, top quintile'}[k],'VALIDATED_HYPOTHESIS'] for k in rules],columns=['strategy_name','research_rule','validation_status'])
 full=pd.Series(True,index=p.index); frames=[]
 for h in b.HORIZONS:
  for name,mask in rules.items():
   q=p.assign(strategy_name=name); frames+=[b._summarize_candidate(q,mask,full,[],['strategy_name'],'STRATEGY_ONLY',h),b._summarize_candidate(q,mask,full,['research_regime'],['research_regime','strategy_name'],'REGIME_STRATEGY',h)]
 period=pd.concat([x for x in frames if not x.empty],ignore_index=True,sort=False); ids=['scope','horizon','research_regime','strategy_name','baseline_type']
 for c in ids:
  if c not in period: period[c]=pd.NA
 period=period[['period',*ids,'candidate_rows','unique_dates','unique_codes','avg_return','avg_excess_return','median_excess_return','avg_daily_equal_weight_excess','median_daily_equal_weight_excess','positive_excess_dates','baseline_closed_rows']]
 if period.duplicated(['period',*ids]).any(): raise SystemExit('duplicate keys')
 rr=[]
 for key,x in period.groupby(ids,dropna=False):
  tr=pd.to_numeric(x.loc[x.period.isin(['P1_202506_202509','P2_202510_202512','P3_202601_202603']),'avg_daily_equal_weight_excess'],errors='coerce').dropna(); ho=pd.to_numeric(x.loc[x.period.eq('P4_202604_202606'),'avg_daily_equal_weight_excess'],errors='coerce').dropna(); tm=float(tr.mean()) if len(tr) else np.nan; hm=float(ho.mean()) if len(ho) else np.nan
  rr.append({**dict(zip(ids,key)),'available_periods':int(x.period.nunique()),'train_periods':int(len(tr)),'train_positive_periods':int((tr>0).sum()),'train_mean_daily_excess':tm,'holdout_p4_daily_excess':hm,'evidence_state':'TIME_ORDERED_REPEAT_POSITIVE' if len(tr)>=2 and (tr>0).all() and hm>0 else 'MIXED_OR_INSUFFICIENT'})
 repeat=pd.DataFrame(rr); defs.to_csv(OUT_D,index=False,encoding='utf-8-sig');period.to_csv(OUT_P,index=False,encoding='utf-8-sig');repeat.to_csv(OUT_R,index=False,encoding='utf-8-sig')
 payload={'generated_at':datetime.now().isoformat(timespec='seconds'),'scope':'read_only_price_structure_strategy_validation','window':{'start':str(b.START.date()),'end':str(b.END.date())},'price_history_contract':integrity,'row_counts':{'panel_rows':int(len(p)),'period_rows':int(len(period)),'repeat_rows':int(len(repeat)),'positive_rows':int((repeat.evidence_state=='TIME_ORDERED_REPEAT_POSITIVE').sum())},'definitions':records(defs),'limitations':['Daily close signals only; no intraday execution model.','Event and sector data are intentionally excluded because their histories do not span this price window.'],'operational_change':False}; OUT_J.write_text(json.dumps(payload,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
 pos=repeat[(repeat.scope=='STRATEGY_ONLY')&(repeat.evidence_state=='TIME_ORDERED_REPEAT_POSITIVE')]; lines=['# Price Structure Strategy Validation','',f"- panel_rows: {len(p)}",'','## Time-ordered positive']+[f"- {r.strategy_name}/{r.horizon}: train={r.train_mean_daily_excess:.6f}, P4={r.holdout_p4_daily_excess:.6f}" for _,r in pos.iterrows()]+['','## Caveats',*['- '+z for z in payload['limitations']]]; OUT_M.write_text('\n'.join(lines)+'\n',encoding='utf-8'); print(json.dumps({'status':'OK','period':str(OUT_P),'repeat':str(OUT_R),'md':str(OUT_M)},ensure_ascii=False))
if __name__=='__main__': main()
