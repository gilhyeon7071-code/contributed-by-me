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
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date'])
g=d.groupby('code')
d['f']=(g['close'].shift(-5)/d['close']-1.0)*100      # h5 %
d.loc[d['f'].abs()>50,'f']=np.nan                      # 분할 미조정 방어
d=d.dropna(subset=['f'])
d=d[d['value']>=1e9]                                   # 집행 가능성 제약
print("패널: %d행  %s ~ %s" % (len(d), d['date'].min(), d['date'].max()), flush=True)

per=d.groupby('date')['f'].agg(['count','var'])
per=per[per['count']>=30].dropna()
N=per['count'].values.astype(float); S2=per['var'].values
print("유효 거래일 %d일   일평균 적격종목 %.0f개 (중앙값 %.0f)" % (len(per), N.mean(), np.median(N)), flush=True)
print("일중 횡단면 수익 표준편차 평균 %.2f%%p" % np.sqrt(S2).mean(), flush=True)
print(flush=True)
print("무작위 m종목 바스켓의 기준선 대비 일별 초과수익 표준편차 (노이즈 하한)")
print("%6s %12s %14s" % ("m", "sd(%p)", "NSV_001 대비"))
for m in [3,5,10,20,30,50,100,200,400]:
    v = np.mean(np.maximum(1.0/m - 1.0/N, 0.0) * S2)
    sd = sqrt(v)
    print("%6d %12.4f %13.1f배" % (m, sd, sd/0.5578), flush=True)
