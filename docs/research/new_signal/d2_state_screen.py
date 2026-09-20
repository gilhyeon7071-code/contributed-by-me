# D2 - 상태형 축 탐색 스크린 (EXPLORATION. 확증 아님)
import pandas as pd, numpy as np, glob
from math import sqrt
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','open','high','low','close','volume','value','market']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
for c in ['open','high','low','close','volume','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date']).reset_index(drop=True)
g=d.groupby('code')

# ---- 상태형 축 ----
pc=g['close'].shift(1)
ret1=d['close']/pc-1.0
tr=pd.concat([(d['high']-d['low']).abs(),(d['high']-pc).abs(),(d['low']-pc).abs()],axis=1).max(axis=1)
d['atr14']  = tr.groupby(d['code']).transform(lambda s:s.rolling(14,min_periods=14).mean())/d['close']*100
d['vol60']  = ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
d['rng20']  = ((d['high']-d['low'])/d['close']).groupby(d['code']).transform(lambda s:s.rolling(20,min_periods=15).mean())*100
d['liq']    = np.log10(d['value'].clip(lower=1))
d['illiq']  = (ret1.abs()/d['value'].clip(lower=1)*1e9).groupby(d['code']).transform(lambda s:s.rolling(20,min_periods=15).mean())
d['price']  = np.log10(d['close'].clip(lower=1))
d['ma120']  = d['close']/g['close'].transform(lambda s:s.rolling(120,min_periods=90).mean())-1.0
d['hi250']  = d['close']/g['close'].transform(lambda s:s.rolling(250,min_periods=180).max())
d['vturn']  = np.log10(d['volume'].clip(lower=1))

d['f']=(g['close'].shift(-5)/d['close']-1.0)*100
d.loc[d['f'].abs()>50,'f']=np.nan
d=d.dropna(subset=['f'])
d=d[d['value']>=1e9]
print("패널 %d행  %s~%s" % (len(d),d['date'].min(),d['date'].max()), flush=True)

uni=d.groupby('date')['f'].mean()
AXES=[('atr14','변동성 ATR14'),('vol60','변동성 60일'),('rng20','일중변동폭 20일'),
      ('liq','유동성 거래대금'),('illiq','비유동성 Amihud'),('price','주가 수준'),
      ('ma120','120일선 위치'),('hi250','52주 고가 위치'),('vturn','거래량 수준')]
MES=0.358
print("\n%-16s %4s %8s %8s %7s %6s %8s" % ("축","분위","excess","t(일클러스터)","연도+","종목/일","MES"), flush=True)
rows=[]
for col,lab in AXES:
    sub=d.dropna(subset=[col])
    if len(sub)<200000: 
        print("%-16s  표본부족 %d" % (lab,len(sub)), flush=True); continue
    dec=sub.groupby('date')[col].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
    sub=sub.assign(dec=dec)
    for q in [0,9]:
        b=sub[sub['dec']==q]
        bm=b.groupby('date')['f'].mean()
        cnt=b.groupby('date')['f'].size().mean()
        ex=(bm-uni).dropna()
        if len(ex)<200: continue
        m=ex.mean(); se=ex.std()/sqrt(len(ex)); t=m/se
        yr=ex.groupby(ex.index.str[:4]).mean()
        pos=(yr>0).sum(); tot=len(yr)
        flag='**' if m>=MES else ('+' if m>0 else '')
        print("%-16s D%-3d %+8.4f %8.2f %5d/%-2d %6.0f %6s" % (lab,q,m,t,pos,tot,cnt,flag), flush=True)
        rows.append((lab,q,m,t,pos,tot,cnt))
print("\n(EXPLORATION. 전 구간 이미 열람됨 -> 확증 아님. MES=%.3f%%p)"%MES, flush=True)
