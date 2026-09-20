import argparse,json
from pathlib import Path
import pandas as pd
P=argparse.ArgumentParser();P.add_argument('--input',required=True);P.add_argument('--return-col',required=True);P.add_argument('--rank-col',required=True);P.add_argument('--output',required=True);P.add_argument('--ascending',action='store_true');a=P.parse_args()
d=pd.read_csv(a.input,encoding='utf-8-sig',dtype={'code':str});d['date']=pd.to_datetime(d['date']);d['ret']=pd.to_numeric(d[a.return_col],errors='coerce');d['close']=pd.to_numeric(d['close'],errors='coerce');d=d[d.ret.notna()&d.close.gt(0)].copy();days=sorted(d.date.unique());idx={x:i for i,x in enumerate(days)};cash=100_000_000;pend=[];op=[];ev=[];F=.005;S=.001
for day in days:
 i=idx[day];cash+=sum(x['amt'] for x in pend if x['r']<=i);pend=[x for x in pend if x['r']>i]
 for x in [z for z in op if z['e']<=i]:
  pro=x['p']*(1+x['ret'])*(1-S)*x['q'];pend.append({'r':i+2,'amt':pro*(1-F)});ev.append({'date':day,'side':'SELL','code':x['code'],'net_cash':pro*(1-F),'net_ret':pro*(1-F)/x['cost']-1});op.remove(x)
 q=d[d.date.eq(day)].sort_values([a.rank_col,'code'],ascending=[a.ascending,True]).head(3)
 for x in q.itertuples():
  px=x.close*(1+S);qty=int(5_000_000/px);cost=qty*px*(1+F)
  if qty and cost<=cash and len(op)<20: cash-=cost;op.append({'code':x.code,'q':qty,'p':x.close,'ret':x.ret,'cost':cost,'e':i+5});ev.append({'date':day,'side':'BUY','code':x.code,'net_cash':-cost})
e=pd.DataFrame(ev);o=Path(a.output);e.to_csv(o.with_suffix('.csv'),index=False,encoding='utf-8-sig');o.with_suffix('.json').write_text(json.dumps({'buy':int((e.side=='BUY').sum()),'sell':int((e.side=='SELL').sum()),'open':len(op),'cash':cash,'pending':sum(x['amt'] for x in pend),'operational_change':False},ensure_ascii=False),encoding='utf-8')