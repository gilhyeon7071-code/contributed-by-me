# D5 - 리밸런싱 주기별 실제 알파 (비겹침 시뮬레이션, EXPLORATION)
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
d=d.dropna(subset=['vol60'])
elig=(d['value']>=1e9)
d=d[elig].copy()
d['dec']=d.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
COST=0.358
print("패널 %d행"%len(d), flush=True)
print("\n리밸런싱 주기별 - 비겹침 실제 보유 시뮬레이션", flush=True)
print("%-10s %6s %10s %9s %10s %10s %10s %10s"%(
      "주기","기간수","알파/기간","알파sd","연환산알파","연비용","연 순","필요기간"), flush=True)
TO={5:0.181,10:0.207,20:0.245,60:0.299,120:0.320}   # (92) 실측 단측 회전율
for S in [5,10,20,60,120]:
    cap=50 if S<=10 else (100 if S<=20 else 200)
    d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
    s=d[d['f'].abs()<=cap].dropna(subset=['f'])
    dates=sorted(s['date'].unique())[::S]                 # 비겹침 리밸런싱 시점
    sub=s[s['date'].isin(dates)]
    uni=sub.groupby('date')['f'].mean()
    port=sub[sub['dec']<=8].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':port}).dropna().sort_index()
    if len(df)<20: print("%-10s 표본부족 %d"%(S,len(df)), flush=True); continue
    beta,a=np.polyfit(df['u'],df['b'],1)
    resid=df['b']-(a+beta*df['u']); asd=resid.std(ddof=2)
    per=247.0/S
    ann=a*per; cost=TO[S]*per*COST
    need=(2.8*asd/abs(a))**2 if a else float('nan')
    print("%-10d %6d %+10.4f %9.4f %+10.2f %10.2f %+10.2f %8.0f기간(%.1f년)"%(
        S,len(df),a,asd,ann,cost,ann-cost,need,need*S/247), flush=True)
print("\n(92) 의 h10 근사는 연 5.47%p 였다. 위 '연환산알파'와 비교하면 낙관 정도를 알 수 있다.", flush=True)
