@echo off
cd /d E:\1_Data
set PYTHONIOENCODING=cp949
python -c "import json,pandas as pd; j=json.load(open(r'2_Logs\after_close_summary_last.json','r',encoding='utf-8')); buys=((j.get('executions') or {}).get('buys') or []); df=pd.read_csv(r'2_Logs\candidates_latest_data.csv',encoding='utf-8-sig'); df['code']=df['code'].astype(str).str.zfill(6); m=df.set_index('code')[['name','market','score']].to_dict('index'); print('BUY_COUNT='+str(len(buys))); [print('{}. {} {} [{}] score={} qty={} price={} dt={}'.format(i,c,m.get(c,{}).get('name',''),m.get(c,{}).get('market',''),m.get(c,{}).get('score',None),(b or {}).get('qty'),(b or {}).get('price'),(b or {}).get('datetime'))) for i,b in enumerate(buys,1) for c in [str((b or {}).get('code') or '').zfill(6)]]" > 2_Logs\buy_detail_last.txt
type 2_Logs\buy_detail_last.txt
