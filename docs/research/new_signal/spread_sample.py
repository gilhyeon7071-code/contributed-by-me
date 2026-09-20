# -*- coding: utf-8 -*-
"""목표 포트폴리오의 스프레드 분포를 층화 표본으로 추정."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, r'E:\1_Data\tools')
from load_merged_panel import load_merged, add_vol60
from rebalance_portfolio import build_target

CAP=100_000_000; EXCL=0.20; MINV=1e9
d=add_vol60(load_merged())
as_of=str(d['date'].max())
target, meta = build_target(d, as_of, CAP, EXCL, MINV)
print("기준일 %s  목표 %d종목  슬롯 %s원"%(as_of, meta['target_count'], format(meta['slot_krw'],',')))
t=target.copy()
t['liq_decile']=pd.qcut(t['value'].rank(method='first'),10,labels=False)
# 층별 6종목씩
rng=np.random.default_rng(20260825)
picks=[]
for dcl,g in t.groupby('liq_decile'):
    k=min(6,len(g))
    picks.append(g.sample(k,random_state=int(dcl)+1))
s=pd.concat(picks)
print("표본 %d종목 (10개 유동성 층 x 6)"%len(s))
print("  거래대금 범위 %.0f억 ~ %.0f억"%(s['value'].min()/1e8, s['value'].max()/1e8))
s[['code','close','value','vol60','liq_decile']].to_csv(
    r'C:\Users\jjtop\AppData\Local\Temp\claude\C--Windows-System32\cf91d743-9309-4e83-bcdf-fa14129f427c\scratchpad\spread_sample_codes.csv',
    index=False, encoding='utf-8-sig')
print(','.join(s['code'].tolist()))
