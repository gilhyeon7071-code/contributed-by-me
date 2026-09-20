# D4b - 십분위 알파 전체 모양 + 중간대 집중이 도움이 되는가 (EXPLORATION)
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
d['r1m']=d['close']/g['close'].shift(20)-1.0
H=10
d['f']=(g['close'].shift(-H)/d['close']-1.0)*100
d.loc[d['f'].abs()>50,'f']=np.nan
d=d.dropna(subset=['f']); d=d[d['value']>=1e9]
uni=d.groupby('date')['f'].mean()

def alpha_of(mask_series, s):
    b=s[mask_series].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':b}).dropna().sort_index()
    if len(df)<500: return None
    beta,a=np.polyfit(df['u'],df['b'],1)
    resid=df['b']-(a+beta*df['u'])
    asd=resid.values[::H].std(ddof=1)
    n=(2.8*asd/abs(a))**2 if a else float('nan')
    cnt=s[mask_series].groupby('date')['f'].size().mean()
    return a,asd,n,cnt

for col,lab in [('vol60','변동성60'),('r1m','1개월 수익률')]:
    s=d.dropna(subset=[col]).copy()
    s['dec']=s.groupby('date')[col].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
    print("\n=== %s : 십분위별 알파 (h%d) ==="%(lab,H), flush=True)
    print("%5s %10s %9s %10s %7s"%("분위","알파(%p)","알파sd","필요블록","종목/일"), flush=True)
    for q in range(10):
        r=alpha_of(s['dec']==q, s)
        if r: print("D%-4d %+10.4f %9.4f %10.0f %7.0f"%(q,r[0],r[1],r[2],r[3]), flush=True)
    print("  -- 집중이 도움이 되는가 (양수 구간을 좁혀본다) --", flush=True)
    for lo,hi,nm in [(1,8,'D1~D8'),(2,7,'D2~D7'),(3,6,'D3~D6'),(4,5,'D4~D5'),(4,4,'D4만'),(5,5,'D5만')]:
        r=alpha_of(s['dec'].between(lo,hi), s)
        if r: print("  %-8s %+9.4f  sd %7.4f  필요블록 %8.0f (%5.1f년) 종목/일 %4.0f"%(nm,r[0],r[1],r[2],r[2]*H/247,r[3]), flush=True)
