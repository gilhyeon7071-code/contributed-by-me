# -*- coding: utf-8 -*-
"""측정용 실매매 대상 선정. 새 전략 모집단에서 유동성 3구간 x 2종목."""
import pandas as pd, numpy as np, glob
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','close','value','market']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
for c in ['close','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date']).reset_index(drop=True)
g=d.groupby('code'); r=d['close']/g['close'].shift(1)-1.0
d['vol60']=r.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
last=d['date'].max()
s=d[(d['date']==last)].dropna(subset=['vol60'])
s=s[s['value']>=1e9]
s['pv']=s['vol60'].rank(pct=True)
s=s[s['pv']<=0.90]                       # 새 전략 모집단: vol60 상위 10% 배제
s['pl']=s['value'].rank(pct=True)
print("기준일 %s   모집단 %d종목"%(last,len(s)))
print("\n%-6s %-8s %10s %12s %8s %8s"%("구간","종목","종가","거래대금(억)","vol60","시장"))
print("-"*62)
picks=[]
for lo,hi,nm in [(0.10,0.30,'하위'),(0.45,0.55,'중위'),(0.85,0.95,'상위')]:
    band=s[(s['pl']>=lo)&(s['pl']<=hi)].sort_values('value')
    take=band.iloc[[len(band)//3, 2*len(band)//3]] if len(band)>=2 else band
    for _,x in take.iterrows():
        print("%-6s %-8s %10.0f %12.0f %8.2f %8s"%(nm,x['code'],x['close'],x['value']/1e8,x['vol60'],x['market']))
        picks.append((nm,x['code'],int(x['close'])))
print("\n1주씩 매수 시 총 소요: %,d원"%sum(p[2] for p in picks) if picks else "")
