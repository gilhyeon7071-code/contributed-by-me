import importlib.util,json
from datetime import datetime
from pathlib import Path
import pandas as pd
R=Path(__file__).resolve().parents[1];L=R/'2_Logs';B=R/'tools'/'build_new_method_price_structure_validation.py';P=L/'bollinger_top5_execution';F=.005;S=.001
def net(x): return ((1+x)*(1-S)*(1-F))/((1+S)*(1+F))-1
def per(d): return 'P1' if d<=pd.Timestamp('2025-09-30') else ('P2' if d<=pd.Timestamp('2025-12-31') else ('P3' if d<=pd.Timestamp('2026-03-31') else 'P4'))
def main():
 s=importlib.util.spec_from_file_location('b',B);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);a=m.load_base();r=a._load_report_module();raw=r.load_data();f=a._exact_returns(r.compute_factors(raw));f['date']=pd.to_datetime(f.date).dt.normalize();f=f[(f.date>='2025-06-01')&(f.date<='2026-06-30')&f.market.astype(str).str.upper().isin(['KOSPI','KOSDAQ'])].copy();f=f[pd.to_numeric(f.path_return_h5,errors='coerce').notna()&f.close.gt(0)];g=f.groupby('price_history_key',sort=False).close;z=(f.close-g.transform(lambda x:x.rolling(20).mean()))/(g.transform(lambda x:x.rolling(20).std(ddof=0))+1e-9);q=f[z.le(-1.5)&pd.to_numeric(f.rsi14,errors='coerce').le(30)].copy();q['ret']=q.path_return_h5;q['net']=net(q.ret);q['z']=z.loc[q.index];benchmark=f.groupby('date',sort=False).path_return_h5.mean().rename('same_day_baseline_gross');q=q.join(benchmark,on='date');q['same_day_baseline_net']=net(q.same_day_baseline_gross);q['gross_excess_vs_same_day']=q.ret-q.same_day_baseline_gross;q['net_excess_vs_same_day']=q.net-q.same_day_baseline_net;chosen=q.sort_values(['date','z']).groupby('date',group_keys=False).head(5).copy();rows=[]
 for name,x in {'ALL_SIGNALS':q,'TOP5_MOST_EXTREME':chosen}.items():
  x=x.copy();x['period']=x.date.map(per)
  for k,y in x.groupby('period'): rows.append({'variant':name,'period':k,'trades':len(y),'avg_gross':y.ret.mean(),'avg_net':y.net.mean(),'win_rate':(y.net>0).mean(),'avg_same_day_baseline_gross':y.same_day_baseline_gross.mean(),'avg_same_day_baseline_net':y.same_day_baseline_net.mean(),'avg_gross_excess_vs_same_day':y.gross_excess_vs_same_day.mean(),'avg_net_excess_vs_same_day':y.net_excess_vs_same_day.mean()})
 o=pd.DataFrame(rows);o.to_csv(f'{P}_summary_latest.csv',index=False,encoding='utf-8-sig');chosen.to_csv(f'{P}_trades_latest.csv',index=False,encoding='utf-8-sig');Path(f'{P}_latest.json').write_text(json.dumps({'generated_at':datetime.now().isoformat(timespec='seconds'),'all_signals':len(q),'selected_trades':len(chosen),'operational_change':False,'broker_order':False},ensure_ascii=False),encoding='utf-8');print(json.dumps({'status':'OK','all_signals':len(q),'selected_trades':len(chosen)},ensure_ascii=False))
if __name__=='__main__':main()
