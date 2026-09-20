"""Read-only outcome audit for the independent research candidate layer."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];LOG=ROOT/'2_Logs';INP=LOG/'new_method_research_candidates_history.csv';OUT=LOG/'new_method_research_candidate_audit_summary_latest.csv';J=LOG/'new_method_research_candidate_audit_latest.json';M=LOG/'new_method_research_candidate_audit_latest.md'
def main():
 d=pd.read_csv(INP,encoding='utf-8-sig'); rows=[]
 for st,g in d.groupby('strategy_name'):
  for h in ('h1','h2','h5'):
   c=f'path_return_{h}';r=pd.to_numeric(g[c],errors='coerce').dropna();rows.append({'strategy_name':st,'horizon':h,'candidate_rows':len(g),'signal_dates':g.signal_date.nunique(),'closed_rows':len(r),'pending_rows':len(g)-len(r),'avg_return':r.mean() if len(r) else None,'median_return':r.median() if len(r) else None,'win_rate':(r>0).mean() if len(r) else None})
 s=pd.DataFrame(rows);s.to_csv(OUT,index=False,encoding='utf-8-sig');payload={'generated_at':datetime.now().isoformat(timespec='seconds'),'scope':'read_only_research_candidate_outcome_audit','input_rows':len(d),'summary_rows':len(s),'operational_change':False,'limitations':['Raw path returns only; same-date regime baseline and transaction-cost analysis are separate.']};J.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8');M.write_text('# Research Candidate Outcome Audit\n\n'+s.to_csv(index=False)+'\n',encoding='utf-8');print(json.dumps({'status':'OK','summary':str(OUT)},ensure_ascii=False))
if __name__=='__main__':main()
