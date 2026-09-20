# -*- coding: utf-8 -*-
"""배제 비율 격자: 효과·분산·필요표본이 매끄럽게 변하는가."""
import sys, numpy as np, pandas as pd
from math import sqrt
sys.path.insert(0, r'E:\1_Data\tools')
from load_merged_panel import load_merged, add_vol60
S=10; COST=0.358
d=add_vol60(load_merged())
d['f']=(d.groupby('code')['close'].shift(-S)/d['close']-1.0)*100
d=d.dropna(subset=['f','vol60']); d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].rank(pct=True)
dates=sorted(d['date'].unique())[::S]
sub=d[d['date'].isin(dates)]
uni=sub.groupby('date')['f'].mean().sort_index()
ny=len(uni)*S/247.0
cg=lambda x: ((1+x/100).prod()**(1/ny)-1)*100
print("비겹침 %d기간 %.1f년  유니버스 CAGR %.2f%%"%(len(uni),ny,cg(uni)))
print()
print("%7s %8s %10s %9s %9s %8s %10s"%("배제%","보유수","CAGR","효과/기간","sd","t","필요블록"))
print("-"*70)
for x in [0.02,0.05,0.08,0.10,0.12,0.15,0.20,0.25,0.30]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean().sort_index().reindex(uni.index)
    df=pd.DataFrame({'u':uni,'b':p}).dropna()
    ex=df['b']-df['u']; m=ex.mean(); sd=ex.std(ddof=1); n=len(df)
    cnt=sub[sub['pv']<=1-x].groupby('date').size().mean()
    need=(2.8*sd/abs(m))**2 if m else float('nan')
    print("%6.0f%% %8.0f %9.2f%% %+9.5f %9.5f %8.2f %6.0f(%4.1f년)"%(
        x*100,cnt,cg(df['b']),m,sd,m/(sd/sqrt(n)),need,need*S/247))
