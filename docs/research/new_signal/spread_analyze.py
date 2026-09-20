# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
D=r'C:\Users\jjtop\AppData\Local\Temp\claude\C--Windows-System32\cf91d743-9309-4e83-bcdf-fa14129f427c\scratchpad'
q=pd.read_csv(r'E:\1_Data\2_Logs\measure\spread_sample_093929.csv',dtype=str,encoding='utf-8-sig')
s=pd.read_csv(D+r'\spread_sample_codes.csv',dtype={'code':str})
q['code']=q['code'].str.zfill(6); s['code']=s['code'].str.zfill(6)
for c in ('price','ask1','bid1'): q[c]=pd.to_numeric(q[c],errors='coerce')
m=q.merge(s,on='code',how='inner')
ok=m[(m['status']=='OK')&(m['ask1']>0)&(m['bid1']>0)].copy()
ok['mid']=(ok['ask1']+ok['bid1'])/2
ok['spread_bps']=(ok['ask1']-ok['bid1'])/ok['mid']*10000
print("조회 %d / 표본 %d   유효 %d"%(len(q),len(s),len(ok)))
print()
print("=== 스프레드 분포 (bp) ===")
q_=ok['spread_bps']
print("  중앙 %.1f   평균 %.1f   p25 %.1f   p75 %.1f   최대 %.1f"%(
    q_.median(),q_.mean(),q_.quantile(.25),q_.quantile(.75),q_.max()))
print()
for th in [25,30,40,50,100]:
    p=(q_<=th).mean()
    print("  <= %3dbp 통과 %2d/%2d = %5.1f%%   -> 505종목 중 약 %3.0f개"%(
        th,(q_<=th).sum(),len(q_),100*p,505*p))
print()
print("=== 유동성 층별 (0=최저 ... 9=최고) ===")
print("%5s %10s %10s %8s %8s"%("층","거래대금(억)","중앙spread","<=30bp","종목수"))
for dcl,g in ok.groupby('liq_decile'):
    print("%5d %10.0f %10.1f %7.0f%% %8d"%(dcl,g['value'].median()/1e8,g['spread_bps'].median(),100*(g['spread_bps']<=30).mean(),len(g)))
