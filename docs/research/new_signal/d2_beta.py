# D2c - 저변동성 초과수익이 베타 축소의 산물인가 (EXPLORATION)
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
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date']).reset_index(drop=True)
g=d.groupby('code'); ret1=d['close']/g['close'].shift(1)-1.0
d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
H=20
d['f']=(g['close'].shift(-H)/d['close']-1.0)*100
d.loc[d['f'].abs()>100,'f']=np.nan
d=d.dropna(subset=['f','vol60']); d=d[d['value']>=1e9]
d['dec']=d.groupby('date')['vol60'].transform(lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop'))
uni=d.groupby('date')['f'].mean()
b0=d[d['dec']==0].groupby('date')['f'].mean()
b9=d[d['dec']==9].groupby('date')['f'].mean()
df=pd.DataFrame({'uni':uni,'d0':b0,'d9':b9}).dropna()
df['ex0']=df['d0']-df['uni']; df['ex9']=df['d9']-df['uni']
print("h%d  신호일 %d일"%(H,len(df)), flush=True)
print("\n--- 연도별 D0 초과수익 ---", flush=True)
yr=df.groupby(df.index.str[:4]).agg(n=('ex0','size'),ex0=('ex0','mean'),ex9=('ex9','mean'),uni=('uni','mean'))
for y,r in yr.iterrows():
    print("  %s n=%3d  유니버스%+8.2f   D0%+8.3f   D9%+8.3f"%(y,r['n'],r['uni'],r['ex0'],r['ex9']), flush=True)
print("\n  D0 양수 연도 %d/%d   D9 음수 연도 %d/%d"%((yr['ex0']>0).sum(),len(yr),(yr['ex9']<0).sum(),len(yr)), flush=True)
print("\n--- 시장 방향별 (유니버스 h%d 수익 부호) ---"%H, flush=True)
for lab,mask in [('상승장 (uni>0)',df['uni']>0),('하락장 (uni<=0)',df['uni']<=0)]:
    s=df[mask]
    print("  %-16s n=%4d(%4.1f%%)  유니버스%+7.2f  D0초과%+7.3f  D9초과%+7.3f"%(
        lab,len(s),100*len(s)/len(df),s['uni'].mean(),s['ex0'].mean(),s['ex9'].mean()), flush=True)
b=np.polyfit(df['uni'],df['d0'],1)
print("\n--- 베타 회귀  D0수익 = a + b*유니버스수익 ---", flush=True)
print("  기울기 b = %.3f   절편 a = %+.4f%%p   (b<1 이면 저베타)"%(b[0],b[1]), flush=True)
b9=np.polyfit(df['uni'],df['d9'],1)
print("  D9: 기울기 %.3f  절편 %+.4f%%p"%(b9[0],b9[1]), flush=True)
print("\n  -> 절편(a)이 알파. 기울기가 1보다 작으면 초과수익 일부는 베타 차이다.", flush=True)
