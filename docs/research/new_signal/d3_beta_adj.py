# 베타 조정이 검정력을 얼마나 회복시키는가
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
print("%-4s %6s %9s %9s %8s | %10s %10s"%("h","분위","raw sd","알파 sd","감소","알파(%p)","필요 블록"), flush=True)
print("-"*72, flush=True)
for H in [5,10,20,60]:
    cap=50 if H<=10 else (100 if H<=20 else 200)
    d['f']=(g['close'].shift(-H)/d['close']-1.0)*100
    s=d[d['f'].abs()<=cap].dropna(subset=['f','vol60'])
    s=s[s['value']>=1e9]
    s=s.assign(dec=s.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop')))
    uni=s.groupby('date')['f'].mean()
    for q,lab in [(0,'D0'),(9,'D9')]:
        b=s[s['dec']==q].groupby('date')['f'].mean()
        df=pd.DataFrame({'u':uni,'b':b}).dropna().sort_index()
        raw=(df['b']-df['u'])
        beta,alpha=np.polyfit(df['u'],df['b'],1)
        resid=df['b']-(alpha+beta*df['u'])
        nb=len(raw.values[::H])
        rsd=raw.values[::H].std(ddof=1); asd=resid.values[::H].std(ddof=1)
        need=(2.8*asd/abs(alpha))**2 if alpha!=0 else float('nan')
        print("%-4d %6s %9.4f %9.4f %7.1f%% | %+10.4f %10.0f (%.1f년)"%(
            H,lab,rsd,asd,100*(1-asd/rsd),alpha,need,need*H/247), flush=True)
