# D6d - 절대수익: 복리 누적. 이게 진짜 답이다
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
uni=sub.groupby('date')['f'].mean().sort_index()
nyr=len(uni)*S/247.0
def cagr(series_pct, cost_per_period=0.0):
    r=(series_pct/100.0)-cost_per_period
    return ((1+r).prod()**(1/nyr)-1)*100
TO={0.10:0.207,0.15:0.212,0.20:0.216,0.25:0.222,0.30:0.228,0.40:0.244}
print("비겹침 %d기간 = %.1f년   리밸런싱 %d일"%(len(uni),nyr,S), flush=True)
print("\n%-22s %12s %12s %12s %10s"%("포트폴리오","CAGR(비용전)","CAGR(비용후)","유니버스대비","기간승률"), flush=True)
print("-"*76, flush=True)
print("%-22s %12.2f%% %12s %12s %10.1f%%"%("유니버스 전체",cagr(uni),"-","-",100*(uni>0).mean()), flush=True)
for x in [0.10,0.15,0.20,0.25,0.30,0.40]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean().sort_index()
    p=p.reindex(uni.index).dropna()
    c=TO[x]*COST/100.0
    print("%-22s %12.2f%% %12.2f%% %12.2f%%p %10.1f%%"%(
        "vol60 상위 %d%% 배제"%(x*100),cagr(p),cagr(p,c),cagr(p,c)-cagr(uni),100*(p>0).mean()), flush=True)
# 저변동성 하위만 담기
print(flush=True)
for x in [0.5,0.3,0.2,0.1]:
    p=sub[sub['pv']<=x].groupby('date')['f'].mean().sort_index().reindex(uni.index).dropna()
    print("%-22s %12.2f%% %12s %12.2f%%p %10.1f%%"%(
        "vol60 하위 %d%%만 보유"%(x*100),cagr(p),"(회전율 미측정)",cagr(p)-cagr(uni),100*(p>0).mean()), flush=True)
