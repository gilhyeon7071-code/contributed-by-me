# -*- coding: utf-8 -*-
"""예산 70,000원. 유동성 3구간 x 2종목, 종목당 예산 배분(수량>=2 확보)."""
import pandas as pd, numpy as np, glob
BUDGET = 70000
RESERVE = 12000                  # 내일 주가 변동·호가 여유
PER = (BUDGET - RESERVE) // 6    # 종목당 예산
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
s=d[d['date']==last].dropna(subset=['vol60'])
s=s[s['value']>=1e9]
s['pv']=s['vol60'].rank(pct=True); s=s[s['pv']<=0.90].copy()
s['pl']=s['value'].rank(pct=True)

def tick(px):
    for lim,t in [(2000,1),(5000,5),(20000,10),(50000,50),(200000,100),(500000,500)]:
        if px < lim: return t
    return 1000

print("기준일 %s  모집단 %d  예산 %s원(예비 %s)  종목당 %s원"
      % (last,len(s),format(BUDGET,','),format(RESERVE,','),format(PER,',')))
print("\n%-6s %-8s %9s %5s %10s %10s %8s %7s"
      % ("구간","종목","종가","수량","금액","거래대금(억)","vol60","호가%"))
print("-"*72)
rows=[]; total=0
for lo,hi,nm in [(0.05,0.35,'하위'),(0.40,0.60,'중위'),(0.80,0.98,'상위')]:
    band = s[(s['pl']>=lo)&(s['pl']<=hi)].copy()
    band = band[band['close'] <= PER/2]          # 최소 2주는 살 수 있게
    band = band.nlargest(2,'value') if len(band)>=2 else band
    for _,x in band.iterrows():
        px=int(x['close']); qty=max(1,PER//px); amt=px*qty
        total+=amt
        print("%-6s %-8s %9s %5d %10s %10.0f %8.2f %6.2f%%"
              % (nm,x['code'],format(px,','),qty,format(amt,','),x['value']/1e8,x['vol60'],100*tick(px)/px))
        rows.append((nm,x['code'],px,qty,amt))
print("-"*72)
print("합계 %s원 / 예산 %s원  잔여 %s원"%(format(total,','),format(BUDGET,','),format(BUDGET-total,',')))
