# -*- coding: utf-8 -*-
"""표본에서 얻은 '유동성 층별 통과율' 을 그대로 적용해 게이트 효과를 재현한다.
하한 상향(층 전체 제거)과 달리, 각 층에서 관측된 비율만큼만 남긴다."""
import sys, numpy as np, pandas as pd
from math import sqrt
sys.path.insert(0, r'E:\1_Data\tools')
from load_merged_panel import load_merged, add_vol60
S=10; LIMIT=30.5; EXCL=0.10
# 2026-08-25 실측 (60종목 층화표본, 30bp 기준)
PASS={0:0.40, 1:0.67, 2:1.00, 3:0.67, 4:0.83, 5:0.67, 6:1.00, 7:0.67, 8:1.00, 9:1.00}
d=load_merged()
g=d.groupby('code')
r1=(d['close']/g['close'].shift(1)-1.0)*100
gap=(pd.to_datetime(d['date'])-pd.to_datetime(g['date'].shift(1))).dt.days
d['ca']=((r1.abs()>LIMIT)&(gap<=7)).fillna(False).astype(int)
rev=d.iloc[::-1]
d['ca_win']=((rev.groupby('code')['ca'].transform(lambda s:s.rolling(S,min_periods=1).sum()).iloc[::-1])-d['ca'])>0
d=add_vol60(d)
d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
d=d.dropna(subset=['f','vol60']); d=d[~d['ca_win']]; d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].rank(pct=True)
dates=sorted(d['date'].unique())[::S]
ss=d[d['date'].isin(dates)].copy()
uni=ss.groupby('date')['f'].mean().sort_index()
base=ss[ss['pv']<=1-EXCL].copy()
base['liq_d']=base.groupby('date')['value'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
rng=np.random.default_rng(20260825)
base['u01']=rng.random(len(base))
base['pass']=base.apply(lambda r: r['u01'] < PASS.get(int(r['liq_d']) if pd.notna(r['liq_d']) else 9, 1.0), axis=1)
def rep(sel,label):
    p=sel.groupby('date')['f'].mean().sort_index().reindex(uni.index)
    df=pd.DataFrame({'u':uni,'b':p}).dropna()
    ex=df['b']-df['u']; m=ex.mean(); sd=ex.std(ddof=1); n=len(df)
    cnt=sel.groupby('date').size().mean()
    ny=n*S/247.0; cg=lambda z:((1+z/100).prod()**(1/ny)-1)*100
    need=(2.8*sd/abs(m))**2 if m else float('nan')
    print("%-22s %7.0f %9.2f%% %9.2f%% %+10.5f %8.5f %6.0f(%.1f년)"%(
        label,cnt,cg(df['u']),cg(df['b']),m,sd,need,need*S/247))
print("게이트 대리 재현 (층별 실측 통과율 적용)")
print("%-22s %7s %10s %10s %10s %8s %8s"%("조건","보유수","유니버스","포트","효과/기간","sd","필요블록"))
print("-"*80)
rep(base,'① 게이트 없음 (505)')
rep(base[base['pass']],'② 30bp 게이트 통과')
