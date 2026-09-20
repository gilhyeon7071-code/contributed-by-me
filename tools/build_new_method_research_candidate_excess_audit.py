"""Add same-date/regime/horizon excess returns to research candidates."""
from __future__ import annotations
import importlib.util,json
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];LOG=ROOT/'2_Logs';INP=LOG/'new_method_research_candidates_history.csv';OUT=LOG/'new_method_research_candidate_excess_summary_latest.csv';J=LOG/'new_method_research_candidate_excess_latest.json'
def main():
 spec=importlib.util.spec_from_file_location('rep',ROOT/'tools'/'build_new_method_representative_oos_candidates.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);report=m._load_report_module();raw=report.load_data();f=report.compute_factors(raw);f['date']=pd.to_datetime(f.date).dt.normalize();f['research_regime']=f.date.map(report._assign_report_research_regime(f));f=m._exact_returns(f);d=pd.read_csv(INP,encoding='utf-8-sig');d['date']=pd.to_datetime(d.signal_date).dt.normalize();rows=[]
 for h in ('h1','h2','h5'):
  c=f'path_return_{h}';base=f.groupby(['date','research_regime'])[c].mean().rename('base').reset_index();q=d.merge(base,on=['date','research_regime'],how='left');q['excess']=pd.to_numeric(q[c],errors='coerce')-q['base'];
  for st,g in q.groupby('strategy_name'):
   x=g.excess.dropna();rows.append({'strategy_name':st,'horizon':h,'closed_rows':len(x),'avg_excess_return':x.mean() if len(x) else None,'median_excess_return':x.median() if len(x) else None,'positive_excess_rows':int((x>0).sum())})
 s=pd.DataFrame(rows);s.to_csv(OUT,index=False,encoding='utf-8-sig');J.write_text(json.dumps({'scope':'read_only_research_candidate_excess_audit','rows':len(s),'operational_change':False},ensure_ascii=False,indent=2),encoding='utf-8');print(s.to_csv(index=False))
if __name__=='__main__':main()
