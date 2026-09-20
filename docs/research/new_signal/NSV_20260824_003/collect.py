# -*- coding: utf-8 -*-
"""NSV_20260824_003 전진 관측 수집·판정. 사전등록과 함께 동결한다.

사전등록표대로만 계산한다. 인자로 동작을 바꿀 수 없다.
83블록 미만에서는 초과수익을 출력하지 않는다 - 중간 엿보기를 코드가 막는다.
판정은 (1) CI 하한 > 0 과 (2) 점추정 >= MES 를 모두 만족할 때만 SUPPORTED.
정제(winsorization/clip/gap 필터)를 하지 않는다 - PLANS (94) 참조.
"""
import pandas as pd, numpy as np, glob
from math import sqrt

START = "20260825"      # 관측 개시일 (고정)
H     = 10              # 보유기간 = 리밸런싱 주기 (고정)
EXCL  = 0.10            # 배제 비율 vol60 상위 (고정)
NEED  = 83              # 최소 비겹침 블록 (고정)
MES   = 0.14            # %p/기간 (고정)
SD_PRE= 0.452852        # 사전등록 분산 추정치 %p (고정)
MINV  = 1e9             # 거래대금 하한 (고정)

def main():
    parts=[]
    for f in sorted(glob.glob(r'E:\1_Data\krx_daily_archive\*_clean.parquet')):
        try: parts.append(pd.read_parquet(f, columns=['date','code','close','value']))
        except Exception: pass
    d=pd.concat(parts,ignore_index=True)
    d['date']=d['date'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
    d['code']=d['code'].astype(str).str.zfill(6)
    for c in ['close','value']: d[c]=pd.to_numeric(d[c],errors='coerce')
    d=d[d['close']>0].drop_duplicates(subset=['date','code'],keep='last')
    d=d.sort_values(['code','date']).reset_index(drop=True)
    g=d.groupby('code')
    r=d['close']/g['close'].shift(1)-1.0
    d['vol60']=r.groupby(d['code']).transform(lambda s:s.rolling(60,min_periods=45).std())*100
    d['f']=(g['close'].shift(-H)/d['close']-1.0)*100          # 정제 없음
    d=d.dropna(subset=['f','vol60'])
    d=d[(d['value']>=MINV) & (d['date']>=START)]
    if d.empty:
        print("관측 표본 0. 개시일 %s 이후 h%d 수익이 확정된 신호일이 아직 없다."%(START,H)); return
    d['pv']=d.groupby('date')['vol60'].transform(lambda x: x.rank(pct=True,method='first'))
    uni=d.groupby('date')['f'].mean()
    port=d[d['pv']<=1-EXCL].groupby('date')['f'].mean()
    df=pd.DataFrame({'u':uni,'b':port}).dropna().sort_index()
    blocks=df.iloc[::H]
    n=len(blocks)
    print("NSV_20260824_003  전진 관측")
    print("  개시일 %s   확정 신호일 %d일   비겹침 블록 %d / %d"%(START,len(df),n,NEED))
    if n < NEED:
        print("  -> DEFERRED_INSUFFICIENT_SAMPLE. 초과수익을 출력하지 않는다(판정 절차 3).")
        print("     남은 블록 %d개 (약 %.1f개월)"%(NEED-n,(NEED-n)*H/247*12))
        return
    ex=blocks['b']-blocks['u']
    m=ex.mean(); se=ex.std(ddof=1)/sqrt(n)
    lo,hi=m-1.96*se, m+1.96*se
    beta=np.polyfit(blocks['u'],blocks['b'],1)[0]
    print("  베타 %.4f   보유종목/기간 평균 %.0f"%(beta, d[d['pv']<=1-EXCL].groupby('date').size().mean()))
    print("  초과수익 %+.5f%%p/기간   se %.5f   95%%CI [%+.5f, %+.5f]"%(m,se,lo,hi))
    print("  연환산 %+.3f%%p   사전등록 MES %.2f%%p/기간"%(m*247/H, MES))
    sig = lo > 0.0            # (1) 통계적 유의: CI 하한 > 0
    mat = m >= MES            # (2) 실질적 유의: 점추정 >= MES
    print("  (1) 통계적 유의 CI하한 %+.5f > 0        : %s"%(lo, "충족" if sig else "미충족"))
    print("  (2) 실질적 유의 점추정 %+.5f >= %.2f    : %s"%(m, MES, "충족" if mat else "미충족"))
    if m <= 0:
        print("  판정: NOT_SUPPORTED (평균이 0 이하 - 명확한 실패)")
    elif sig and mat:
        print("  판정: SUPPORTED")
    else:
        print("  판정: NOT_SUPPORTED")

if __name__=="__main__":
    main()
