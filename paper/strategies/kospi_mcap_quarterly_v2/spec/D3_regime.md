# D3 국면

- 상태: `DRAFT` (2026-09-17 16시대)
- 층: 판단 층
- 게이트: G1 초안 / G2 코드 있음 / G3 합성 시험 통과 / G4 실행기에서 D1 산출 파일을 다시 읽어 연결 (실데이터 전 구간은 지수 수집 뒤 확인)
- 코드: `src/targets.py` `compute_regime`, 설정 `config/strategy_v1.json`
- 블록: C5 (판정 t 종가 → 효력 t+1, close == MA 는 above)

## 입력
D1 산출 `kospi200_<선정일>.csv` (date, close). 선정일 행 포함 최근 200 개

## 판정
```
ma = 최근 200개 종가 단순평균 (선정일 포함)
close < ma  -> regime below, exposure 0.5
close >= ma -> regime above, exposure 1.0
효력: 다음 거래일부터
```
근거: 정의 8(사용자 09-13), C5 블록. 임계값 근거 = 전략 전제

## 출력
`status, observation_date, close, ma, ma_window, regime, exposure, effective_from`

## 경계
- 종목·비중을 정하지 않아요. 노출 비율 하나만 내요
- 분기 사이 일별 노출 전환의 주문은 E 층 몫이에요(이 칸은 날짜 하나의 판정만)

## 안전장치
| 상황 | 판정 |
|---|---|
| 마지막 행 날짜 ≠ 선정일 | 멈춤 `REGIME_INDEX_DATE` |
| 200개 미만 | 멈춤 `REGIME_WINDOW_SHORT` |

## 실측 (2026-09-17)
- 2026-09-16 종가 1,060.05, MA200 945.96 → above, 노출 100% (MA 대비 +12.1%)
