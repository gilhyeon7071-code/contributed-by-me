"""Paper-only ledger for the approved research-priority strategy; never writes orders/fills."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];L=ROOT/'2_Logs';INP=L/'new_method_research_candidates_history.csv';OUT=L/'new_method_strategy_paper_ledger_latest.csv';J=L/'new_method_strategy_paper_ledger_latest.json'
def main():
 d=pd.read_csv(INP,encoding='utf-8-sig');q=d[(d.strategy_name=='RESIDUAL_MOMENTUM')].copy();q['entry_date']=q.signal_date;q['entry_price']=pd.to_numeric(q.close,errors='coerce');q['horizon']='h5';q['path_return']=pd.to_numeric(q.path_return_h5,errors='coerce');q['exit_price']=q.entry_price*(1+q.path_return);q['paper_status']=q.path_return.notna().map({True:'CLOSED_SIMULATED',False:'OPEN_PENDING'});q['strategy_id']='RESIDUAL_MOMENTUM_H5_PAPER';q['operational_use']=False;q['broker_order']=False;q['position_size']='UNSIZED_RESEARCH';o=q[['strategy_id','entry_date','code','market','research_regime','entry_price','horizon','exit_price','path_return','paper_status','position_size','operational_use','broker_order']].sort_values(['entry_date','code']);o.to_csv(OUT,index=False,encoding='utf-8-sig');J.write_text(json.dumps({'generated_at':datetime.now().isoformat(timespec='seconds'),'scope':'paper_only_strategy_ledger','strategy':'RESIDUAL_MOMENTUM_H5','rows':len(o),'closed':int((o.paper_status=='CLOSED_SIMULATED').sum()),'open':int((o.paper_status=='OPEN_PENDING').sum()),'operational_change':False,'broker_order':False},ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps({'status':'OK','ledger':str(OUT)},ensure_ascii=False))
if __name__=='__main__':main()
