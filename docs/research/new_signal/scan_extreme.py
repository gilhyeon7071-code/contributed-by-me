# -*- coding: utf-8 -*-
"""가격제한(±30%)을 넘는 1일 변동 = 기업행위 미조정 구간 전수 조사."""
import sys, pandas as pd, numpy as np
sys.path.insert(0, r'E:\1_Data\tools')
from load_merged_panel import load_merged
d=load_merged()
g=d.groupby('code')
d['r1']=(d['close']/g['close'].shift(1)-1.0)*100
d['gap']=(pd.to_datetime(d['date'])-pd.to_datetime(g['date'].shift(1))).dt.days
v=d[d['r1'].notna() & (d['gap']<=7)]        # 연속 거래일만
n=len(v)
print("연속 거래일 1일 수익률 %d개  (한국 가격제한 ±30%%)"%n)
for lo in [30.5,50,100,300,1000]:
    m=(v['r1'].abs()>lo)
    print("  |r1|>%6.1f%%  %6d건 (%.4f%%)  고유종목 %4d"%(lo,m.sum(),100*m.sum()/n,v.loc[m,'code'].nunique()))
print()
ext=v[v['r1'].abs()>100]
ext=ext.assign(yr=ext['date'].str[:4])
print("|r1|>100%% 연도별:")
for y,gg in ext.groupby('yr'):
    print("   %s  %4d건  %3d종목"%(y,len(gg),gg['code'].nunique()))
print()
print("가장 큰 10건:")
for r in ext.reindex(ext['r1'].abs().nlargest(10).index).itertuples():
    print("   %s %s  r1=%+12.1f%%  close %9.0f"%(r.date,r.code,r.r1,r.close))
