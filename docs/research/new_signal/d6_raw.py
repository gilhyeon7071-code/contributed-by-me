# D6c - 실제로 손에 쥐는 것: 원시 초과수익 (베타조정 아님) + 시장 방향별
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
S=10; COST=0.358
d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
d=d[d['f'].abs()<=50].dropna(subset=['f','vol60']); d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
dates=sorted(d['date'].unique())[::S]
sub=d[d['date'].isin(dates)].copy()
uni=sub.groupby('date')['f'].mean()
per=247.0/S
TO={0.10:0.207,0.15:0.212,0.20:0.216,0.25:0.222,0.30:0.228,0.40:0.244}
print("리밸런싱 %d일  비겹침 %d기간   유니버스 연평균 %+.2f%%p"%(S,len(uni),uni.mean()*per), flush=True)
print("\n%-22s %9s %9s %9s | %9s %9s %8s"%("배제","연 원시초과","연 비용","연 순(원시)","베타","상승장","하락장"), flush=True)
print("-"*92, flush=True)
for x in [0.10,0.15,0.20,0.25,0.30,0.40]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':p}).dropna().sort_index()
    raw=(df['b']-df['u'])
    beta,a=np.polyfit(df['u'],df['b'],1)
    up=raw[df['u']>0].mean(); dn=raw[df['u']<=0].mean()
    cost=TO[x]*per*COST
    print("%-22s %+9.2f %9.2f %+9.2f | %9.3f %+9.3f %+8.3f"%(
        "vol60 상위 %d%%"%(x*100), raw.mean()*per, cost, raw.mean()*per-cost, beta, up, dn), flush=True)
print("\n(상승장/하락장은 기간당 %%p. 비겹침 %d기간 중 상승 %d / 하락 %d)"%(
    len(uni),(uni>0).sum(),(uni<=0).sum()), flush=True)
