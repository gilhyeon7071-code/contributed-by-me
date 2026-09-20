import pandas as pd,json
from pathlib import Path
R=Path(__file__).resolve().parents[1];L=R/'2_Logs';e=pd.read_csv(L/'basic_paper_policy_portfolio_ledger_latest.csv',encoding='utf-8-sig',dtype={'code':str});s=json.loads((L/'basic_paper_policy_portfolio_latest.json').read_text(encoding='utf-8'))
b=e[e.side.eq('BUY')];x=e[e.side.eq('SELL')];open_cost=float(-b.net_cash.sum()-x.net_cash.sum()+0) # replaced below from unmatched FIFO
q={};closed=[]
for r in e.itertuples():
 if r.side=='BUY': q.setdefault(r.code,[]).append(-float(r.net_cash))
 else:
  c=q[r.code].pop(0);closed.append((float(r.net_cash)-c,float(r.net_ret)))
remain=sum(sum(v) for v in q.values()); pnl=sum(z[0] for z in closed);out={'closed_trades':len(closed),'realized_pnl_krw':pnl,'realized_return_on_closed_cost':pnl/sum(-b.net_cash.iloc[:len(closed)]) if len(closed) else None,'closed_win_rate':sum(z[1]>0 for z in closed)/len(closed),'open_cost_basis_krw':remain,'settled_cash_krw':s['settled_cash'],'pending_t2_cash_krw':s['pending_cash'],'capital_accounting_check_krw':s['settled_cash']+s['pending_cash']+remain,'initial_capital_krw':s['initial_capital'],'operational_change':False};(L/'basic_paper_policy_portfolio_summary_latest.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(out,ensure_ascii=False))
