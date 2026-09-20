# D2b - 보유기간별 축적 (EXPLORATION)
import pandas as pd, numpy as np, glob
from math import sqrt
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','high','low','close','volume','value']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
for c in ['high','low','close','volume','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date']).reset_index(drop=True)
g=d.groupby('code'); pc=g['close'].shift(1); ret1=d['close']/pc-1.0
d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
tr=pd.concat([(d['high']-d['low']).abs(),(d['high']-pc).abs(),(d['low']-pc).abs()],axis=1).max(axis=1)
d['atr14']=tr.groupby(d['code']).transform(lambda s:s.rolling(14,min_periods=14).mean())/d['close']*100
d['rng20']=((d['high']-d['low'])/d['close']).groupby(d['code']).transform(lambda s:s.rolling(20,min_periods=15).mean())*100
HS=[1,5,10,20,40,60]
for h in HS:
    cap=50 if h<=10 else (100 if h<=20 else 200)
    d['f%d'%h]=(g['close'].shift(-h)/d['close']-1.0)*100
    d.loc[d['f%d'%h].abs()>cap,'f%d'%h]=np.nan
d=d[d['value']>=1e9]
print("패널 %d행"%len(d), flush=True)
COST=0.358
print("\n%-14s %3s %9s %9s %9s %9s %9s %9s"%("축","분위","h1","h5","h10","h20","h40","h60"), flush=True)
for col,lab in [('vol60','변동성60'),('atr14','ATR14'),('rng20','일중변동폭')]:
    sub=d.dropna(subset=[col]).copy()
    sub['dec']=sub.groupby('date')[col].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
    for q in [0,9]:
        out=[]
        for h in HS:
            c='f%d'%h
            s2=sub.dropna(subset=[c])
            uni=s2.groupby('date')[c].mean()
            bm=s2[s2['dec']==q].groupby('date')[c].mean()
            ex=(bm-uni).dropna()
            out.append(ex.mean() if len(ex)>200 else float('nan'))
        print("%-14s D%-2d %9.3f %9.3f %9.3f %9.3f %9.3f %9.3f"%((lab,q)+tuple(out)), flush=True)
print("\n비용 1회 %.3f%%p 차감 후 (양수면 회전 가치 있음)"%COST, flush=True)
print("%-14s %3s %9s %9s %9s %9s %9s %9s"%("축","분위","h1","h5","h10","h20","h40","h60"), flush=True)
for col,lab in [('vol60','변동성60'),('atr14','ATR14'),('rng20','일중변동폭')]:
    sub=d.dropna(subset=[col]).copy()
    sub['dec']=sub.groupby('date')[col].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
    out=[]
    for h in HS:
        c='f%d'%h
        s2=sub.dropna(subset=[c])
        uni=s2.groupby('date')[c].mean()
        bm=s2[s2['dec']==0].groupby('date')[c].mean()
        ex=(bm-uni).dropna()
        out.append(ex.mean()-COST if len(ex)>200 else float('nan'))
    print("%-14s D0  %9.3f %9.3f %9.3f %9.3f %9.3f %9.3f"%((lab,)+tuple(out)), flush=True)
