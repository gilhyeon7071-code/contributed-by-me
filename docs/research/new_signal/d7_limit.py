# D7 - 가격제한을 이용한 기업행위 검출 (외부 데이터 불필요)
import pandas as pd, numpy as np, glob
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
d['r1']=(d['close']/g['close'].shift(1)-1.0)*100
d['gap']=(pd.to_datetime(d['date'])-pd.to_datetime(g['date'].shift(1))).dt.days
n=d['r1'].notna().sum()
print("1일 수익률 %d개"%n)
print("\n가격제한(±30%%) 초과 = 기업행위 후보")
for lo in [30.5,31,35,40,50]:
    m=(d['r1'].abs()>lo)
    print("  |r1|>%4.1f%%  %7d건 (%.4f%%)  그중 연속거래일(gap<=4) %d건"%(
        lo,m.sum(),100*m.sum()/n,(m&(d['gap']<=4)).sum()))
print("\n초과 건의 방향")
m=(d['r1'].abs()>30.5)&(d['gap']<=4)
print("  하락(분할·병합 의심) %d건   상승 %d건"%((d.loc[m,'r1']<0).sum(),(d.loc[m,'r1']>0).sum()))
print("\n분할 비율로 흔한 값 (하락분, close_t/close_t-1 의 역수)")
sub=d[m&(d['r1']<0)].copy()
sub['inv']=1.0/(1.0+sub['r1']/100.0)
print(sub['inv'].round(1).value_counts().head(10).to_string())
print("\n영향 추정: 이 %d건을 0%%로 두면 유니버스 평균 1일 수익률이 어떻게 변하나"%m.sum())
elig=d[d['value']>=1e9]
raw=elig.groupby('date')['r1'].mean().mean()
fix=elig.assign(r1=elig['r1'].mask(elig['r1'].abs()>30.5,0.0)).groupby('date')['r1'].mean().mean()
print("  원본 %.5f%%/일  ->  보정 %.5f%%/일   (연 %.2f%%p -> %.2f%%p)"%(raw,fix,raw*247,fix*247))
