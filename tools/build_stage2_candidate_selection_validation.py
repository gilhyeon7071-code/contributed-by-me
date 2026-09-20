import importlib.util,json
from pathlib import Path
import pandas as pd
R=Path(__file__).resolve().parents[1];L=R/'2_Logs';B=R/'tools'/'build_new_method_strategy_library_validation.py'
s=importlib.util.spec_from_file_location('lib',B);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);b=m._base();r=b._load_report_module();raw=r.load_data();f=r.compute_factors(raw);f['date']=pd.to_datetime(f.date).dt.normalize();f=b._strategy_features(f);f=b._atomic_signals(f);f=b._exact_returns(f);f=b._add_periods(f);p=f[(f.date.between(b.START,b.END))&f.period.notna()&f.market.astype(str).str.upper().isin(['KOSPI','KOSDAQ'])&f.close.gt(0)&f.value.gt(0)].copy().sort_values(['price_history_key','price_session_index']);g=p.groupby('price_history_key',sort=False);h20=g.close.transform(lambda x:x.rolling(20,min_periods=20).max().shift(1));h252=g.close.transform(lambda x:x.rolling(252,min_periods=60).max().shift(1));rules={'BOLLINGER_MR':(p.MR_BOLLINGER,p.strategy_z20),'BREAKOUT_252D':(p.BREAKOUT_252D,-p.close/(h252+1e-9)),'VOLUME_BREAKOUT':(p.close.gt(h20)&p.V_ACCEL_Q5,-p.v_accel)};full=pd.Series(True,index=p.index);out=[]
for name,(mask,score) in rules.items():
 for n in [0,1,3,5]:
  rank=score[mask].groupby(p.loc[mask,'date']).rank(method='first');use=mask.copy() if n==0 else (mask&rank.reindex(p.index).le(n).fillna(False));q=p.assign(strategy_name=name,selection_variant='ALL' if n==0 else f'TOP_{n}');
  for hz in (['h1','h2','h5'] if name=='BOLLINGER_MR' else (['h1','h2'] if name=='BREAKOUT_252D' else ['h1'])): out.append(b._summarize_candidate(q,use,full,[],['strategy_name','selection_variant'],'STAGE2_SELECTION',hz))
x=pd.concat([z for z in out if not z.empty],ignore_index=True);x.to_csv(L/'stage2_candidate_selection_validation_latest.csv',index=False,encoding='utf-8-sig');print(json.dumps({'rows':len(x)},ensure_ascii=False))
