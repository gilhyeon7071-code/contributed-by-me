# -*- coding: utf-8 -*-
"""NSV_20260824_002 전진 관측 수집·판정. 사전등록과 함께 동결한다.

기준서 §4 사전등록표대로만 계산한다. 인자로 동작을 바꿀 수 없다.
27블록 미만에서는 알파를 출력하지 않는다 - 중간 엿보기를 코드가 막는다.
"""
import pandas as pd, numpy as np, glob, sys
from math import sqrt

START   = "20260825"   # 관측 개시일 (사전등록 고정)
H       = 10           # 주 보유기간 (사전등록 고정)
NEED    = 27           # 최소 비겹침 블록 (사전등록 고정)
MES     = 1.5          # %p (사전등록 고정)
SD_PRE  = 2.7382       # 사전등록 분산 추정치 %p

def load():
    parts=[]
    for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
        try: parts.append(pd.read_parquet(f, columns=['date','code','close','value']))
        except Exception: pass
    d=pd.concat(parts,ignore_index=True)
    d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
    d['code']=d['code'].astype(str).str.zfill(6)
    for c in ['close','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
    d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last')
    return d.sort_values(['code','date']).reset_index(drop=True)

def main():
    d=load()
    g=d.groupby('code')
    ret1=d['close']/g['close'].shift(1)-1.0
    d['vol60']=ret1.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
    d['f']=(g['close'].shift(-H)/d['close']-1.0)*100
    d.loc[d['f'].abs()>50,'f']=np.nan
    s=d.dropna(subset=['f','vol60'])
    s=s[(s['value']>=1e9) & (s['date']>=START)]
    if s.empty:
        print("관측 표본 0. 개시일 %s 이후 h%d 수익이 확정된 신호일이 아직 없다."%(START,H)); return
    s=s.assign(dec=s.groupby('date')['vol60'].transform(
        lambda x: pd.qcut(x.rank(method='first'),10,labels=False,duplicates='drop')))
    uni=s.groupby('date')['f'].mean()
    b9 =s[s['dec']==9].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':b9}).dropna().sort_index()
    blocks=df.iloc[::H]                      # 비겹침
    n=len(blocks)
    print("NSV_20260824_002  전진 관측")
    print("  개시일 %s   확정 신호일 %d일   비겹침 블록 %d / %d"%(START,len(df),n,NEED))
    if n < NEED:
        print("  -> DEFERRED_INSUFFICIENT_SAMPLE. 알파를 출력하지 않는다(사전등록 판정 절차 3).")
        print("     남은 블록 %d개 (약 %.1f개월)"%(NEED-n,(NEED-n)*H/247*12))
        return
    beta,alpha=np.polyfit(blocks['u'],blocks['b'],1)
    resid=blocks['b']-(alpha+beta*blocks['u'])
    se=resid.std(ddof=2)/sqrt(n)
    lo,hi=alpha-1.96*se, alpha+1.96*se
    print("  기울기 b = %.4f"%beta)
    print("  알파 a   = %+.4f%%p   se %.4f   95%%CI [%+.4f, %+.4f]"%(alpha,se,lo,hi))
    print("  사전등록 MES = -%.1f%%p (음의 효과이므로 부호 주의)"%MES)
    if alpha >= 0:
        print("  판정: NOT_SUPPORTED (알파가 0 이상 - 명확한 실패)")
    elif hi > -MES:
        print("  판정: NOT_SUPPORTED (CI 상한 %+.4f > %-.1f - MES 만큼 크다고 말할 수 없다)"%(hi,-MES))
    else:
        print("  판정: SUPPORTED (CI 상한 %+.4f <= %-.1f)"%(hi,-MES))

if __name__=="__main__":
    main()
