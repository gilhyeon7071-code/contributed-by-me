import pandas as pd, numpy as np, glob
parts=[]
for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
    try: parts.append(pd.read_parquet(f, columns=['date','code','close','value']))
    except Exception: pass
d=pd.concat(parts,ignore_index=True)
d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
d['code']=d['code'].astype(str).str.zfill(6)
for c in ['close','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last').sort_values(['code','date'])
last=d.groupby('code')['date'].max(); end=d['date'].max()
gone=last[last<'20260101']
print("총 종목 %d개   2026년 이전에 사라진 종목 %d개 (%.1f%%)"%(len(last),len(gone),100*len(gone)/len(last)))
print("  -> 상장폐지·합병 등이 기록에 남아 있다는 뜻. 완전한 생존편향은 아니다.")
g=d.groupby('code'); d['r1']=d['close']/g['close'].shift(1)-1
tail=d[d['code'].isin(gone.index)].groupby('code').tail(5)
print("\n사라진 종목의 마지막 5거래일 평균 수익률 %.3f%%  (전체 평균 %.3f%%)"%(
    tail['r1'].mean()*100, d['r1'].mean()*100))
# 사라진 종목이 고변동성에 몰려 있나
ret1=d['close']/g['close'].shift(1)-1
d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
mv=d.groupby('code')['vol60'].mean()
print("\n평균 vol60:  사라진 종목 %.2f%%   존속 종목 %.2f%%"%(
    mv[mv.index.isin(gone.index)].mean(), mv[~mv.index.isin(gone.index)].mean()))
