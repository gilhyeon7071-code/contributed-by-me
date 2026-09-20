# D6b - 대조: 단일 축에서 배제 비율만 늘리면? (합집합이 특별한가)
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
d['r1m']=d['close']/g['close'].shift(20)-1.0
S=10; COST=0.358
d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
d=d[d['f'].abs()<=50].dropna(subset=['f','vol60','r1m'])
d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
d['pr']=d.groupby('date')['r1m'].transform(lambda x: x.rank(pct=True,method='first'))
dates=sorted(d['date'].unique())[::S]
sub=d[d['date'].isin(dates)].copy()
uni=sub.groupby('date')['f'].mean()
def ev(mask,nm):
    p=sub[mask].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':p}).dropna().sort_index()
    beta,a=np.polyfit(df['u'],df['b'],1)
    asd=(df['b']-(a+beta*df['u'])).std(ddof=2)
    cnt=sub[mask].groupby('date')['f'].size().mean()
    ss={dt:set(v) for dt,v in sub[mask].groupby('date')['code']}
    ds=sorted(ss); to=np.mean([len(ss[x]-ss[y])/len(ss[x]) for x,y in zip(ds[:-1],ds[1:]) if ss[x]])
    per=247.0/S; need=(2.8*asd/abs(a))**2 if a else float('nan')
    excl=100*(1-cnt/sub.groupby('date')['f'].size().mean())
    print("%-34s %6.1f%% %+9.2f %8.2f %+9.2f %7.0f기간"%(nm,excl,a*per,to*per*COST,a*per-to*per*COST,need), flush=True)
print("리밸런싱 %d일. '배제율'은 유니버스 대비 제외 비중"%S, flush=True)
print("%-34s %7s %9s %8s %9s %8s"%("규칙","배제율","연알파","연비용","연 순","판정"), flush=True)
print("-"*88, flush=True)
print("[단일축 vol60 상위 X% 배제]", flush=True)
for x in [0.10,0.15,0.20,0.25,0.30,0.40]:
    ev(sub['pv']<=1-x, "vol60 상위 %d%% 배제"%(x*100))
print("[2축 합집합 - 각 축 상위 10%]", flush=True)
ev((sub['pv']<=0.9)&(sub['pr']<=0.9), "vol60 D9 또는 r1m D9 제외")
print("[대조: 무작위로 같은 비율 배제]", flush=True)
rng=np.random.default_rng(7)
sub['rnd']=rng.random(len(sub))
sub['pn']=sub.groupby('date')['rnd'].transform(lambda x: x.rank(pct=True,method='first'))
ev(sub['pn']<=0.81, "무작위 19% 배제")
