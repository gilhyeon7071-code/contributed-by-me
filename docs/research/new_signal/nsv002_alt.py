# 대안 가설: 배제 포트폴리오의 초과수익 (광범위 바스켓)
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
d['f']=(g['close'].shift(-H)/d['close']-1.0)*100          # 정제 없음
d=d.dropna(subset=['f','vol60']); d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
dates=sorted(d['date'].unique())[::H]
sub=d[d['date'].isin(dates)].copy()
uni=sub.groupby('date')['f'].mean().sort_index()
print("비겹침 %d기간, 정제 없음, h%d"%(len(uni),H))
print("\n%-22s %10s %9s %9s %10s %9s %8s"%("가설","효과/기간","sd","필요블록","달력","MDE(n)","종목/일"))
print("-"*84)
for x,nm in [(0.10,'상위 10% 배제'),(0.20,'상위 20% 배제'),(0.30,'상위 30% 배제')]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean().sort_index().reindex(uni.index)
    ex=(p-uni).dropna()
    m=ex.mean(); sd=ex.std(ddof=1)
    need=(2.8*sd/abs(m))**2
    cnt=sub[sub['pv']<=1-x].groupby('date')['f'].size().mean()
    print("%-22s %+10.4f %9.4f %9.0f %8.2f년 %9.4f %8.0f"%(
        nm,m,sd,need,need*H/247,2.8*sd/sqrt(max(need,1)),cnt))
print("\n비교: D9 바스켓 알파(현 등록 가설, 정제 없음) 필요블록 91 = 3.7년")
print("\n%-22s %12s"%("배제율","MDE 가 각 기간에서"))
for x in [0.10,0.20,0.30]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean().sort_index().reindex(uni.index)
    ex=(p-uni).dropna(); sd=ex.std(ddof=1); m=ex.mean()
    row=[]
    for n in [25,50,75,100]:
        row.append("n=%d:%.3f"%(n,2.8*sd/sqrt(n)))
    print("  상위%2d%% 배제 (효과 %+.4f)  %s"%(x*100,m,"  ".join(row)))
