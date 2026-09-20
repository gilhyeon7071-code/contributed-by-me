"""Read-only status ledger combining historical and initial-OOS evidence."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];L=ROOT/'2_Logs';OUT=L/'new_method_research_strategy_status_latest.csv';J=L/'new_method_research_strategy_status_latest.json'
def main():
 h=pd.read_csv(L/'new_method_strategy_library_repeat_latest.csv',encoding='utf-8-sig');ps=pd.read_csv(L/'new_method_price_structure_repeat_latest.csv',encoding='utf-8-sig');o=pd.read_csv(L/'new_method_research_candidate_excess_summary_latest.csv',encoding='utf-8-sig'); rows=[]
 mapping={'BOLLINGER_MR':'BOLLINGER_MR','BREAKOUT_252D':'BREAKOUT_252D','RESIDUAL_MOMENTUM':'RESIDUAL_MOMENTUM','SQUEEZE_VOLUME_BREAK':'SQUEEZE_VOLUME_BREAK'}
 for s,hs in mapping.items():
  for horizon in ('h1','h2','h5'):
   source=ps if s in {'RESIDUAL_MOMENTUM','SQUEEZE_VOLUME_BREAK'} else h; hist=source[(source.strategy_name==hs)&(source.scope=='STRATEGY_ONLY')&(source.horizon==horizon)];oo=o[(o.strategy_name==s)&(o.horizon==horizon)];hist_ok=bool(len(hist) and hist.iloc[0].evidence_state=='TIME_ORDERED_REPEAT_POSITIVE'); ex=None if oo.empty else oo.iloc[0].avg_excess_return; n=0 if oo.empty else int(oo.iloc[0].closed_rows)
   state='OBSERVE_NO_OOS_SIGNAL' if n==0 and hist_ok else ('PRIORITY_TRACK' if hist_ok and ex is not None and ex>0 else ('HOLD_REVIEW' if hist_ok and ex is not None and ex<=0 else 'NOT_SUPPORTED'))
   rows.append({'strategy_name':s,'horizon':horizon,'historical_time_ordered_positive':hist_ok,'initial_oos_closed_rows':n,'initial_oos_avg_excess':ex,'research_state':state,'operational_use':False})
 d=pd.DataFrame(rows);d.to_csv(OUT,index=False,encoding='utf-8-sig');J.write_text(json.dumps({'generated_at':datetime.now().isoformat(timespec='seconds'),'scope':'research_only_strategy_status','operational_change':False,'rows':d.to_dict(orient='records')},ensure_ascii=False,indent=2),encoding='utf-8');print(d.to_csv(index=False))
if __name__=='__main__':main()
