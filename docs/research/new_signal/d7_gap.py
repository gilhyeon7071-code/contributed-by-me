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
v=d[d['r1'].notna()]
print("=== gap 분포 (직전 관측과의 달력일 간격) ===")
print(v['gap'].value_counts().head(8).to_string())
print("  gap>7  %d건 (%.2f%%)"%((v['gap']>7).sum(),100*(v['gap']>7).mean()))
print("  gap>30 %d건 (%.2f%%)"%((v['gap']>30).sum(),100*(v['gap']>30).mean()))
print("\n=== 거래일 커버리지: 날짜별 종목 수 ===")
cnt=d.groupby('date')['code'].size()
print("  날짜 %d개  종목수 평균 %.0f 중앙 %.0f 최소 %d 최대 %d"%(len(cnt),cnt.mean(),cnt.median(),cnt.min(),cnt.max()))
print("  종목수 하위 10개 날짜:"); print(cnt.nsmallest(6).to_string())
print("\n=== r1 평균, gap 별 ===")
for lo,hi,lab in [(1,1,'gap=1'),(2,4,'gap 2~4'),(5,7,'gap 5~7'),(8,30,'gap 8~30'),(31,10000,'gap>30')]:
    m=v[(v['gap']>=lo)&(v['gap']<=hi)]
    if len(m): print("  %-10s n=%8d  평균 r1 %+8.3f%%  중앙 %+7.3f%%"%(lab,len(m),m['r1'].mean(),m['r1'].median()))
print("\n=== gap<=4 로 제한하고 |r1|<=30.5 로 자른 뒤 ===")
w=v[(v['gap']<=4)&(v['r1'].abs()<=30.5)]
e=w[w['value']>=1e9]
m=e.groupby('date')['r1'].mean()
print("  적격 %d행  일평균 %+.5f%%  ->  연 %+.2f%%p (산술)"%(len(e),m.mean(),m.mean()*247))
print("  기하 누적 CAGR %.2f%%"%(((1+m/100).prod()**(247/len(m))-1)*100))
