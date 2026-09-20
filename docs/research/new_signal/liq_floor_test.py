# -*- coding: utf-8 -*-
"""거래대금 하한을 올려도 초과수익이 견디는가.
스프레드 게이트가 실질적으로 저유동성 꼬리를 자르는 것과 같은 효과라고 보고 대리 검정한다."""
import sys, numpy as np, pandas as pd
from math import sqrt
sys.path.insert(0, r'E:\1_Data\tools')
from load_merged_panel import load_merged, add_vol60
S=10; LIMIT=30.5; EXCL=0.10; COST=0.358
d=load_merged()
g=d.groupby('code')
r1=(d['close']/g['close'].shift(1)-1.0)*100
gap=(pd.to_datetime(d['date'])-pd.to_datetime(g['date'].shift(1))).dt.days
d['ca']=((r1.abs()>LIMIT)&(gap<=7)).fillna(False).astype(int)
rev=d.iloc[::-1]
d['ca_win']=((rev.groupby('code')['ca'].transform(lambda s:s.rolling(S,min_periods=1).sum()).iloc[::-1])-d['ca'])>0
d=add_vol60(d)
d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
d=d.dropna(subset=['f','vol60'])
d=d[~d['ca_win']]
print("거래대금 하한별 (vol60 상위 %d%% 배제, h10, 비겹침)"%(EXCL*100))
print("%10s %8s %10s %10s %10s %8s %8s"%("하한","보유수","유니버스","포트","효과/기간","sd","필요블록"))
print("-"*74)
for minv,lab in [(1e9,'10억'),(2e9,'20억'),(3e9,'30억'),(5e9,'50억'),(10e9,'100억')]:
    s=d[d['value']>=minv].copy()
    s['pv']=s.groupby('date')['vol60'].rank(pct=True)
    dates=sorted(s['date'].unique())[::S]
    ss=s[s['date'].isin(dates)]
    uni=ss.groupby('date')['f'].mean().sort_index()
    p=ss[ss['pv']<=1-EXCL].groupby('date')['f'].mean().sort_index().reindex(uni.index)
    df=pd.DataFrame({'u':uni,'b':p}).dropna()
    ex=df['b']-df['u']; m=ex.mean(); sd=ex.std(ddof=1); n=len(df)
    cnt=ss[ss['pv']<=1-EXCL].groupby('date').size().mean()
    ny=n*S/247.0; cg=lambda z:((1+z/100).prod()**(1/ny)-1)*100
    need=(2.8*sd/abs(m))**2 if m else float('nan')
    print("%10s %8.0f %9.2f%% %9.2f%% %+10.5f %8.5f %6.0f(%.1f년)"%(
        lab,cnt,cg(df['u']),cg(df['b']),m,sd,need,need*S/247))
