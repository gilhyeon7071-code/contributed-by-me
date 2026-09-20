import pandas as pd,json
from pathlib import Path
R=Path(__file__).resolve().parents[1];L=R/'2_Logs';I=L/'existing_paper_policy_reality_audit_selected_trades_latest.csv';O=L/'basic_paper_policy_portfolio';CAP=100_000_000;TARGET=5_000_000;F=.005;S=.001
d=pd.read_csv(I,encoding='utf-8-sig',dtype={'code':str});d['date']=pd.to_datetime(d.date);d=d[(d.policy_variant=='CLOSE_H5_TOP3_NO_REENTRY_WHILE_OPEN')&(d.date>='2025-10-01')&(d.date<='2026-06-30')].copy();days=sorted(d.date.unique());idx={x:i for i,x in enumerate(days)};cash=CAP;pending=[];openp=[];ev=[]
for day in days:
 i=idx[day];cash+=sum(x['amt'] for x in pending if x['release']<=i);pending=[x for x in pending if x['release']>i]
 for p in [x for x in openp if x['exit']<=i]:
  raw=p['price']*(1+p['ret']);pro=raw*(1-S)*p['qty'];fee=pro*F;pending.append({'release':i+2,'amt':pro-fee});ev.append({'date':day,'side':'SELL','code':p['code'],'qty':p['qty'],'price':raw,'net_cash':pro-fee,'net_ret':(pro-fee)/p['cost']-1});openp.remove(p)
 for x in d[d.date.eq(day)].itertuples():
  px=float(x.close)*(1+S);qty=int(TARGET/px);cost=qty*px*(1+F)
  if qty and cost<=cash and len(openp)<20:
   cash-=cost;openp.append({'code':x.code,'qty':qty,'price':float(x.close),'ret':float(x.gross_return),'cost':cost,'exit':i+5});ev.append({'date':day,'side':'BUY','code':x.code,'qty':qty,'price':float(x.close),'net_cash':-cost,'net_ret':None})
e=pd.DataFrame(ev);e.to_csv(f'{O}_ledger_latest.csv',index=False,encoding='utf-8-sig');s={'initial_capital':CAP,'equal_target_krw':TARGET,'max_positions':20,'max_new_per_day':3,'horizon':'h5','fee':F,'slippage':S,'t2':True,'buy':int((e.side=='BUY').sum()),'sell':int((e.side=='SELL').sum()),'open':len(openp),'settled_cash':cash,'pending_cash':sum(x['amt'] for x in pending),'operational_change':False};Path(f'{O}_latest.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False))
