# D8 - winsorization 없이 재측정 (아카이브가 수정주가임이 확인됨)
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
d=d.dropna(subset=['f','vol60']); d=d[d['value']>=1e9]
d['pv']=d.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
dates=sorted(d['date'].unique())[::S]
sub=d[d['date'].isin(dates)].copy()
print("비겹침 %d기간  적격행 %d  winsorization 없음"%(len(dates),len(sub)))
uni=sub.groupby('date')['f'].mean().sort_index()
ny=len(uni)*S/247.0
def cg(x): return ((1+x/100).prod()**(1/ny)-1)*100
def trim(s,k):   # 각 기간에서 극단 k개 종목 제외한 평균으로 재계산할 때 쓰지 않고, 기간 단위 이상치 진단용
    return s
print("\n%-24s %11s %11s %11s %9s %8s"%("포트폴리오","CAGR","비용후","유니버스대비","베타","기간승률"))
print("-"*82)
print("%-24s %10.2f%% %11s %11s %9s %7.1f%%"%("유니버스 전체",cg(uni),"-","-","1.000",100*(uni>0).mean()))
TO={0.10:0.207,0.15:0.212,0.20:0.216,0.25:0.222,0.30:0.228,0.40:0.244}
per=247.0/S
for x in [0.10,0.20,0.30,0.40]:
    p=sub[sub['pv']<=1-x].groupby('date')['f'].mean().sort_index().reindex(uni.index)
    c=TO[x]*COST
    beta=np.polyfit(uni,p,1)[0]
    print("%-24s %10.2f%% %10.2f%% %10.2f%%p %9.3f %7.1f%%"%(
        "vol60 상위 %d%% 배제"%(x*100),cg(p),cg(p)-c*per,cg(p)-c*per-cg(uni),beta,100*(p>0).mean()))
print("\n=== 이상치 민감도: 기간별 유니버스 수익 상위/하위 k개 제외 ===")
for k in [0,1,3,5,10]:
    idx=uni.sort_values().index
    keep=uni.index.difference(idx[:k].union(idx[-k:])) if k else uni.index
    u2=uni.loc[keep]; ny2=len(u2)*S/247.0
    p2=sub[sub['pv']<=0.8].groupby('date')['f'].mean().reindex(uni.index).loc[keep]
    cu=((1+u2/100).prod()**(1/ny2)-1)*100; cp=((1+p2/100).prod()**(1/ny2)-1)*100
    print("  극단 %2d기간씩 제외  유니버스 %+7.2f%%  상위20%%배제 %+7.2f%%  초과 %+6.2f%%p"%(k,cu,cp,cp-cu))
