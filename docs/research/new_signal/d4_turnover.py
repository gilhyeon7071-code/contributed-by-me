# D4c - 배제 규칙의 회전율 = 비용 (EXPLORATION)
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
g=d.groupby('code'); ret1=d['close']/g['close'].shift(1)-1.0
d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
d=d.dropna(subset=['vol60']); d=d[d['value']>=1e9]
d['dec']=d.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
keep=d[d['dec']<=8]                       # D9 제외 포트폴리오
sets={dt:set(v) for dt,v in keep.groupby('date')['code']}
dates=sorted(sets)
print("거래일 %d일  평균 보유종목 %.0f개"%(len(dates),np.mean([len(sets[x]) for x in dates])), flush=True)
print("\n리밸런싱 주기별 단측 회전율과 연간 비용", flush=True)
print("%-12s %10s %12s %14s"%("주기","회전율","연 리밸런싱","연 비용(%p)"), flush=True)
COST=0.358
for step,lab in [(1,'매일'),(5,'주 1회'),(10,'2주 1회'),(20,'월 1회'),(60,'분기 1회'),(120,'반기 1회')]:
    idx=dates[::step]
    tos=[]
    for a,b in zip(idx[:-1],idx[1:]):
        A,B=sets[a],sets[b]
        if not A: continue
        tos.append(len(A-B)/len(A))        # 빠져나간 비율 = 단측 회전
    to=np.mean(tos); nreb=247/step
    print("%-12s %9.1f%% %12.1f %14.2f"%(lab,to*100,nreb,to*nreb*COST), flush=True)
print("\n알파는 +0.22%%p/10일 = 연 %.2f%%p (h10 기준 24.7회 누적)"%(0.2216*24.7), flush=True)
