"""Expanding-window rolling OOS validation of frozen execution-policy candidates."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]; LOG=ROOT/'2_Logs'
INP=LOG/'execution_policy_oos_selection_trades_latest.csv'; PREFIX=LOG/'execution_policy_rolling_wfo'
INITIAL_TRAIN_DAYS=57; OOS_DAYS=20

def main():
 d=pd.read_csv(INP,encoding='utf-8-sig',dtype={'code':str}); d['date']=pd.to_datetime(d.date,errors='coerce').dt.normalize()
 if d.duplicated(['selection_policy','date','code']).any(): raise RuntimeError('duplicate policy-date-code')
 days=sorted(d.date.dropna().unique())
 if len(days)<INITIAL_TRAIN_DAYS+OOS_DAYS: raise RuntimeError('insufficient complete daily coverage')
 rows=[]
 fold=0; start=INITIAL_TRAIN_DAYS
 while start+OOS_DAYS<=len(days):
  train_days=days[:start]; oos_days=days[start:start+OOS_DAYS]
  train=d[d.date.isin(train_days)].groupby('selection_policy').net_return.mean().rename('train_daily_net')
  eligible=train[train.gt(0)]
  chosen=str(eligible.sort_values(ascending=False).index[0]) if len(eligible) else ''
  for policy in sorted(d.selection_policy.unique()):
   out=d[(d.selection_policy==policy)&d.date.isin(oos_days)]
   rows.append({'fold':fold,'train_start':str(pd.Timestamp(train_days[0]).date()),'train_end':str(pd.Timestamp(train_days[-1]).date()),'train_days':len(train_days),'oos_start':str(pd.Timestamp(oos_days[0]).date()),'oos_end':str(pd.Timestamp(oos_days[-1]).date()),'oos_days':len(oos_days),'selection_policy':policy,'train_daily_net':float(train.loc[policy]),'selected_by_train_only':policy==chosen,'oos_trades':len(out),'oos_daily_net':float(out.net_return.mean()),'oos_positive':bool(out.net_return.mean()>0)})
  fold+=1; start+=OOS_DAYS
 out=pd.DataFrame(rows); decision=out[out.selected_by_train_only].copy(); agg=pd.DataFrame([{'folds':len(decision),'selected_oos_trades':int(decision.oos_trades.sum()),'positive_oos_folds':int(decision.oos_positive.sum()),'avg_selected_oos_daily_net':float(decision.oos_daily_net.mean()),'median_selected_oos_daily_net':float(decision.oos_daily_net.median()),'policy_selection_counts':decision.selection_policy.value_counts().to_dict(),'operational_approval':False}])
 out.to_csv(f'{PREFIX}_folds_latest.csv',index=False,encoding='utf-8-sig'); decision.to_csv(f'{PREFIX}_selected_oos_latest.csv',index=False,encoding='utf-8-sig'); agg.to_csv(f'{PREFIX}_summary_latest.csv',index=False,encoding='utf-8-sig')
 payload={'generated_at':datetime.now().isoformat(timespec='seconds'),'scope':'read_only_expanding_train_rolling_oos','input':str(INP),'initial_train_sessions':INITIAL_TRAIN_DAYS,'oos_sessions_per_fold':OOS_DAYS,'coverage':{'first':str(pd.Timestamp(days[0]).date()),'last':str(pd.Timestamp(days[-1]).date()),'unused_tail_sessions':len(days)-(start-OOS_DAYS+OOS_DAYS)},'summary':agg.iloc[0].to_dict(),'operational_change':False,'broker_order':False}
 Path(f'{PREFIX}_latest.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8'); Path(f'{PREFIX}_latest.md').write_text('# Execution Policy Rolling WFO\n\n'+agg.to_csv(index=False)+'\n\n'+decision.to_csv(index=False),encoding='utf-8')
 print(json.dumps({'status':'OK','folds':fold,'summary':agg.iloc[0].to_dict()},ensure_ascii=False))
if __name__=='__main__': main()
