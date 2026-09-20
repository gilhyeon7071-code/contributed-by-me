# winsorization 민감도 - 절대수익 vs 초과수익 어느 쪽이 견고한가
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
S=10
d['f0']=(g['close'].shift(-S)/d['close']-1.0)*100
base=d.dropna(subset=['f0','vol60']); base=base[base['value']>=1e9]
print("전체 %d행"%len(base), flush=True)
for cap in [50,100,200,1000]:
    n=(base['f0'].abs()>cap).sum()
    print("  |f|>%4d%% 인 행 %7d (%.3f%%)"%(cap,n,100*n/len(base)), flush=True)
print("\n%-10s %14s %16s %16s"%("cap","유니버스 CAGR","상위20%배제 CAGR","초과(%p)"), flush=True)
print("-"*62, flush=True)
for cap,mode in [(50,'drop'),(100,'drop'),(200,'drop'),(50,'clip'),(100,'clip'),(1e9,'none')]:
    s=base.copy()
    if mode=='drop': s=s[s['f0'].abs()<=cap]
    elif mode=='clip': s=s.assign(f0=s['f0'].clip(-cap,cap))
    s['pv']=s.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
    dts=sorted(s['date'].unique())[::S]
    sb=s[s['date'].isin(dts)]
    u=sb.groupby('date')['f0'].mean().sort_index()
    p=sb[sb['pv']<=0.8].groupby('date')['f0'].mean().sort_index().reindex(u.index)
    ny=len(u)*S/247.0
    cu=((1+u/100).prod()**(1/ny)-1)*100
    cp=((1+p/100).prod()**(1/ny)-1)*100
    lab='%s %s'%(('%d%%'%cap) if cap<1e8 else '무제한',mode)
    print("%-10s %13.2f%% %15.2f%% %15.2f%%p"%(lab,cu,cp,cp-cu), flush=True)
