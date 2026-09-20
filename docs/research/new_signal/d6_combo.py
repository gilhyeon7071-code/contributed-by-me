# D6 - 축 2개 배제 조합 (EXPLORATION). 조합 수 사전 고정: 기준1 + 2축조합 6 = 7
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
d['liq']=np.log10(d['value'].clip(lower=1))
d['ma120']=d['close']/g['close'].transform(lambda s:s.rolling(120,min_periods=90).mean())-1.0
S=10; COST=0.358; TO_BASE=0.207
d['f']=(g['close'].shift(-S)/d['close']-1.0)*100
d=d[(d['f'].abs()<=50)].dropna(subset=['f','vol60','r1m','liq','ma120'])
d=d[d['value']>=1e9]
for c in ['vol60','r1m','liq','ma120']:
    d['q_'+c]=d.groupby('date')[c].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
dates=sorted(d['date'].unique())[::S]
sub=d[d['date'].isin(dates)].copy()
uni=sub.groupby('date')['f'].mean()
def ev(mask,nm):
    p=sub[mask].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':p}).dropna().sort_index()
    beta,a=np.polyfit(df['u'],df['b'],1)
    asd=(df['b']-(a+beta*df['u'])).std(ddof=2)
    cnt=sub[mask].groupby('date')['f'].size().mean()
    need=(2.8*asd/abs(a))**2 if a else float('nan')
    # 회전율 실측
    ss={dt:set(v) for dt,v in sub[mask].groupby('date')['code']}
    ds=sorted(ss); tos=[len(ss[a1]-ss[b1])/len(ss[a1]) for a1,b1 in zip(ds[:-1],ds[1:]) if ss[a1]]
    to=np.mean(tos); per=247.0/S
    print("%-30s %+8.4f %8.4f %7.0f %+9.2f %8.2f %+9.2f %6.0f기간"%(
        nm,a,asd,cnt,a*per,to*per*COST,a*per-to*per*COST,need), flush=True)
print("리밸런싱 %d일  비용 %.3f%%p/왕복"%(S,COST), flush=True)
print("%-30s %8s %8s %7s %9s %8s %9s %8s"%("배제 규칙","알파/기간","알파sd","종목/일","연알파","연비용","연 순","판정"), flush=True)
print("-"*100, flush=True)
ev(sub['q_vol60']<=8, "[기준] vol60 D9 제외")
for c,nm in [('r1m','1개월수익'),('liq','유동성'),('ma120','120일선')]:
    ev((sub['q_vol60']<=8)&(sub['q_'+c]<=8), "vol60 D9 또는 %s D9 제외"%nm)
for c,nm in [('r1m','1개월수익'),('liq','유동성'),('ma120','120일선')]:
    ev(~((sub['q_vol60']==9)&(sub['q_'+c]==9)), "vol60 D9 이면서 %s D9 만 제외"%nm)
