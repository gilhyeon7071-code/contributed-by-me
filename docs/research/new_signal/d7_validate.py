# 아카이브 정제 수익률 vs pykrx 수정주가 - 방법 검증
import pandas as pd, numpy as np, glob
from pykrx import stock
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','close','value']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
d['close']=pd.to_numeric(d['close'],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date'])
for code,nm in [('005930','삼성전자'),('000660','SK하이닉스'),('035420','NAVER')]:
    a=d[d['code']==code].copy()
    if a.empty: print("%s 아카이브 없음"%nm); continue
    a['r1']=a['close']/a['close'].shift(1)-1.0
    a['gap']=(pd.to_datetime(a['date'])-pd.to_datetime(a['date'].shift(1))).dt.days
    cl=a['r1'].where((a['gap']<=4)&(a['r1'].abs()<=0.305),0.0)
    chain=(1+cl.fillna(0)).prod()-1
    d0,d1=a['date'].iloc[0],a['date'].iloc[-1]
    try:
        p=stock.get_market_ohlcv(d0,d1,code,adjusted=True)
        col=[c for c in p.columns if '종가' in c][0]
        true=p[col].iloc[-1]/p[col].iloc[0]-1
        print("%-10s %s~%s  아카이브체인 %+8.1f%%   pykrx수정 %+8.1f%%   차이 %+8.1f%%p"%(
            nm,d0,d1,chain*100,true*100,(chain-true)*100))
        print("           아카이브 원시종가 %s -> %s   (제외된 날 %d일)"%(
            a['close'].iloc[0],a['close'].iloc[-1],int(((a['gap']>4)|(a['r1'].abs()>0.305)).sum())))
    except Exception as e:
        print("%-10s pykrx 실패 %s"%(nm,str(e)[:70]))
