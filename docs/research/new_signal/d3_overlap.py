# 겹침 보정 실측 - 매일 진입 h보유 시 유효 표본
import pandas as pd, numpy as np, glob
from math import sqrt
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','close','value']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
for c in ['close','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date']).reset_index(drop=True)
g=d.groupby('code'); ret1=d['close']/g['close'].shift(1)-1.0
d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
print("%-5s %8s %9s %11s %11s %8s"%("h","신호일","sd(%p)","naive se","블록 se","팽창"), flush=True)
res={}
for H in [5,10,20,60]:
    c='f'
    d[c]=(g['close'].shift(-H)/d['close']-1.0)*100
    cap=50 if H<=10 else (100 if H<=20 else 200)
    s=d[(d['f'].abs()<=cap)].dropna(subset=['f','vol60'])
    s=s[s['value']>=1e9]
    dec=s.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
    s=s.assign(dec=dec)
    uni=s.groupby('date')['f'].mean(); b0=s[s['dec']==0].groupby('date')['f'].mean()
    ex=(b0-uni).dropna().sort_index()
    n=len(ex); sd=ex.std(); naive=sd/sqrt(n)
    blk=ex.values[::H]                      # 비겹침 블록
    bse=blk.std(ddof=1)/sqrt(len(blk))
    print("%-5d %8d %9.4f %11.5f %11.5f %8.2f배"%(H,n,sd,naive,bse,bse/naive), flush=True)
    res[H]=(ex.mean(),sd,len(blk),bse)
print(flush=True)
print("전진 관측 소요 - 비겹침 블록 기준, 검정력 80%, MES=0.358%p", flush=True)
print("%-5s %10s %12s %12s %12s"%("h","실측 효과","블록 sd","필요 블록","달력 기간"), flush=True)
for H,(m,sd,nb,bse) in res.items():
    blocksd=bse*sqrt(nb)
    for delta,lab in [(0.358,'MES'),(m,'실측효과')]:
        if delta<=0: continue
        need=(2.8*blocksd/delta)**2
        yrs=need*H/247
        print("%-5d %10s %12.4f %12.0f %10.1f년"%(H,lab,blocksd,need,yrs), flush=True)
