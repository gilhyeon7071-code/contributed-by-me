# NSV_002 등록값 재검증 - winsorization 유무에 따른 sd / alpha / MDE
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
H=10
d['f0']=(g['close'].shift(-H)/d['close']-1.0)*100
base=d.dropna(subset=['f0','vol60']); base=base[base['value']>=1e9]
print("%-16s %9s %10s %10s %12s %12s"%("정제","알파(%p)","알파sd","필요블록","MDE(n=27)","§4.1 판정"))
print("-"*76)
for cap,mode,lab in [(50,'drop','±50% drop (등록값)'),(100,'drop','±100% drop'),
                     (50,'clip','±50% clip'),(1e9,'none','정제 없음')]:
    s=base.copy()
    if mode=='drop': s=s[s['f0'].abs()<=cap]
    elif mode=='clip': s=s.assign(f0=s['f0'].clip(-cap,cap))
    s=s.assign(dec=s.groupby('date')[ 'vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop')))
    uni=s.groupby('date')['f0'].mean()
    b9=s[s['dec']==9].groupby('date')['f0'].mean()
    df=pd.DataFrame({'u':uni,'b':b9}).dropna().sort_index()
    beta,a=np.polyfit(df['u'],df['b'],1)
    resid=(df['b']-(a+beta*df['u'])).values[::H]
    asd=resid.std(ddof=1)
    need=(2.8*asd/abs(a))**2
    mde=2.8*asd/sqrt(27)
    print("%-16s %+9.4f %10.4f %10.0f %12.4f %12s"%(lab,a,asd,need,mde,'OK' if mde<=1.5 else 'MDE>MES 금지'))
print("\nMDE<=MES(1.5) 를 만족하는 최소 블록 수:")
for cap,mode,lab in [(1e9,'none','정제 없음')]:
    s=base.copy()
    s=s.assign(dec=s.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop')))
    uni=s.groupby('date')['f0'].mean(); b9=s[s['dec']==9].groupby('date')['f0'].mean()
    df=pd.DataFrame({'u':uni,'b':b9}).dropna().sort_index()
    beta,a=np.polyfit(df['u'],df['b'],1)
    asd=(df['b']-(a+beta*df['u'])).values[::H].std(ddof=1)
    for n in [27,40,60,80,100,140]:
        print("   n=%3d블록 (%.2f년)  MDE=%.4f%%p  %s"%(n,n*H/247,2.8*asd/sqrt(n),'OK' if 2.8*asd/sqrt(n)<=1.5 else '금지'))
