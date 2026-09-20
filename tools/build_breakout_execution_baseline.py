import importlib.util,json
from datetime import datetime
from pathlib import Path
import pandas as pd
R=Path(__file__).resolve().parents[1];L=R/'2_Logs';B=R/'tools'/'build_new_method_price_structure_validation.py';P=L/'breakout_execution_baseline';F=.005;S=.001
def net(x): return ((1+x)*(1-S)*(1-F))/((1+S)*(1+F))-1
def per(d): return 'P2' if d<=pd.Timestamp('2025-12-31') else ('P3' if d<=pd.Timestamp('2026-03-31') else 'P4')
def main():
 s=importlib.util.spec_from_file_location('b',B);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);a=m.load_base();r=a._load_report_module();raw=r.load_data();f=a._exact_returns(r.compute_factors(raw));f['date']=pd.to_datetime(f.date).dt.normalize();f=f[(f.date>='2025-10-01')&(f.date<='2026-06-30')&f.market.astype(str).str.upper().isin(['KOSPI','KOSDAQ'])].copy();f=f[pd.to_numeric(f.path_return_h5,errors='coerce').notna()&f.close.gt(0)];g=f.groupby('price_history_key',sort=False).close;high=g.transform(lambda x:x.rolling(252,min_periods=60).max().shift(1));q=f[f.close.gt(high)].copy();q['ret']=q.path_return_h2;q['net']=net(q.ret);q['z']=-(f.close/(high+1e-9)).loc[q.index];rows=[]
 for name,x in {'ALL_SIGNALS':q,'TOP3_MOST_EXTREME':q.sort_values(['date','z']).groupby('date',group_keys=False).head(3)}.items():
  x=x.copy();x['period']=x.date.map(per)
  for k,y in x.groupby('period'): rows.append({'variant':name,'period':k,'trades':len(y),'avg_gross':y.ret.mean(),'avg_net':y.net.mean(),'win_rate':(y.net>0).mean()})
 o=pd.DataFrame(rows);o.to_csv(f'{P}_summary_latest.csv',index=False,encoding='utf-8-sig');q.to_csv(f'{P}_trades_latest.csv',index=False,encoding='utf-8-sig');Path(f'{P}_latest.json').write_text(json.dumps({'generated_at':datetime.now().isoformat(timespec='seconds'),'signals':len(q),'operational_change':False,'broker_order':False},ensure_ascii=False),encoding='utf-8');print(json.dumps({'status':'OK','signals':len(q)},ensure_ascii=False))
if __name__=='__main__':main()
