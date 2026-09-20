# D4 - 수익률 모멘텀/리버설 축 (EXPLORATION). D2/D3 와 동일 규약
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
g=d.groupby('code')
c5,c20,c120,c250=[g['close'].shift(k) for k in (5,20,120,250)]
d['r1w']  = d['close']/c5   -1.0
d['r1m']  = d['close']/c20  -1.0
d['r6m']  = d['close']/c120 -1.0
d['r12m'] = d['close']/c250 -1.0
d['r12_1']= c20/c250 -1.0                  # 표준 모멘텀(최근 1개월 제외)
H=10
d['f']=(g['close'].shift(-H)/d['close']-1.0)*100
d.loc[d['f'].abs()>50,'f']=np.nan
d=d.dropna(subset=['f']); d=d[d['value']>=1e9]
uni=d.groupby('date')['f'].mean()
print("패널 %d행  %s~%s   h%d"%(len(d),d['date'].min(),d['date'].max(),H), flush=True)
print("\n합격선: 매수 신호는 알파 >= +1.0%p (82종목 바스켓, 1년 판정)", flush=True)
print("%-22s %4s %10s %9s %10s %8s %8s"%("축","분위","알파(%p)","알파sd","필요블록","달력","연도+"), flush=True)
print("-"*80, flush=True)
for col,lab in [('r1w','1주 수익률'),('r1m','1개월 수익률'),('r6m','6개월 수익률'),
                ('r12m','12개월 수익률'),('r12_1','모멘텀 12-1')]:
    s=d.dropna(subset=[col])
    if len(s)<300000: print("%-22s 표본부족 %d"%(lab,len(s)), flush=True); continue
    s=s.assign(dec=s.groupby('date')[col].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop')))
    for q in [0,9]:
        b=s[s['dec']==q].groupby('date')['f'].mean()
        df=pd.DataFrame({'u':uni,'b':b}).dropna().sort_index()
        if len(df)<500: continue
        beta,alpha=np.polyfit(df['u'],df['b'],1)
        resid=df['b']-(alpha+beta*df['u'])
        blk=resid.values[::H]; asd=blk.std(ddof=1)
        need=(2.8*asd/abs(alpha))**2 if alpha else float('nan')
        ex=(df['b']-df['u'])
        yr=ex.groupby(ex.index.str[:4]).mean(); pos=(yr>0).sum()
        mark='  <= 합격' if alpha>=1.0 else ''
        print("%-22s D%-3d %+10.4f %9.4f %10.0f %6.1f년 %5d/%-2d%s"%(
            lab,q,alpha,asd,need,need*H/247,pos,len(yr),mark), flush=True)
