# 아카이브 품질 재검증 - 정제 없이 그대로 체인
import pandas as pd, numpy as np, glob
from pykrx import stock
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','close']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
d['close']=pd.to_numeric(d['close'],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date'])
print("%-12s %11s %11s %11s %9s"%("종목","아카이브","pykrx수정","차이(%p)","제한초과일"))
for code,nm in [('005930','삼성전자'),('000660','SK하이닉스'),('035420','NAVER'),
                ('051910','LG화학'),('207940','삼성바이오'),('068270','셀트리온')]:
    a=d[d['code']==code]
    if len(a)<100: print("%-12s 아카이브 부족"%nm); continue
    d0,d1=a['date'].iloc[0],a['date'].iloc[-1]
    arch=a['close'].iloc[-1]/a['close'].iloc[0]-1
    r1=a['close']/a['close'].shift(1)-1
    over=int((r1.abs()>0.305).sum())
    try:
        p=stock.get_market_ohlcv(d0,d1,code,adjusted=True)
        col=[c for c in p.columns if '종가' in c][0]
        true=p[col].iloc[-1]/p[col].iloc[0]-1
        print("%-12s %+10.1f%% %+10.1f%% %+10.1f%%p %8d"%(nm,arch*100,true*100,(arch-true)*100,over))
    except Exception as e:
        print("%-12s pykrx 실패 %s"%(nm,str(e)[:50]))
