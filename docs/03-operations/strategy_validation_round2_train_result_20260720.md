# 전략 검증 탐색 라운드 2 — 상황 × 전략 문법 Train 결과

## 지시사항

라운드 1의 단일 축 결과 뒤, 사전등록한 상황 × 전략 문법 15개 조합의 증분 효과를 읽기 전용으로 검증한다.

## 고정 계약

라운드 1과 동일하다: KOSPI/KOSDAQ 보통주의 일자별 모집단, `PRICE_HISTORY_INTEGRITY_V1`, 다음 실제 전역 거래일 시가 진입, h5 실제 거래일 종가 청산, 매수·매도 수수료 각 0.5%, 매수·매도 슬리피지 각 0.1%, 매도세 0.2%.

Train(2020-01-02~2023-12-29)만 사용했고, 기존 후보·final score·Gate·주문·체결·paper 매매기록은 사용하지 않았다.

## 직접 실행 결과

- 적격 행: 415,001
- 적격 신호일: 987
- 조합 수: 사전등록 15개 전부 실행
- 레짐 truncate-replay: 세 절단점 모두 0일 불일치

| 조합 | 일별 순 h5 평균 | 시장 대비 평균 초과수익 | 상태 |
|---|---:|---:|---|
| BEAR × MR_BOLLINGER | -134.51bp | +10.10bp | 기술 통계 |
| BULL × MA_CROSS_UP | -82.92bp | +67.06bp | 기술 통계 |
| SIDEWAYS × BREAKOUT_252D | -63.65bp | +52.77bp | 기술 통계 |
| SIDEWAYS × MA_CROSS_UP | -94.03bp | +32.89bp | 기술 통계 |
| TRANSITION × MA_CROSS_UP | -124.30bp | +5.68bp | 기술 통계 |
| STRESS × MR_BOLLINGER | +67.19bp | +145.67bp | 표본 보류: 29 신호일·반기 커버리지 0개 |

나머지 9개 조합도 비용 차감 순 h5 평균이 음수이거나 표본 기준에 미달했다. 전체 값은 CSV에 보존했다.

## 해석

- 단일 축을 상황과 결합해도, 충분한 시간 커버리지를 가진 양의 절대 순수익 조합은 없었다.
- BULL × MA 교차와 SIDEWAYS × 돌파는 시장 대비 초과수익은 양수지만, 비용 차감 절대 순수익은 각각 -82.92bp, -63.65bp다. 그러므로 확증 대상이 아니다.
- STRESS × 평균회귀는 유일하게 양의 절대 순수익이지만, 29 신호일 및 시간 커버리지 미달로 `DEFERRED_INSUFFICIENT_SAMPLE`이다.
- 이 결과는 현재 로직 유지 또는 변경의 결론이 아니다. 이번에 고정한 상황 × 전략 문법 15개가 다음 확증 단계로 승격되지 않았다는 탐색 결과다.

## 검증 상태

| 항목 | 상태 | 근거 |
|---|---|---|
| 기능 | PASS | 15개 결과가 CSV·JSON·Markdown으로 생성, JSON `status=OK` |
| 정합성 | PASS | 가격 무결성·일자별 모집단·실제 거래일 h5·레짐 truncate-replay 확인 |
| 운영 반영 | NA | 읽기 전용 연구, 운영 경로 미변경 |
| 정책 | PASS | Gate·임계값·주문 정책 변경 없음 |
| FAIL-CLOSED | PASS | 주문·체결·원장·브로커 경로 미변경 |
| 회귀 | PASS | 연구 스크립트 문법 및 최종 산출물 확인 |

## 증거

- `E:\1_Data\2_Logs\strategy_validation_round2_train_regime_strategy_20260720.csv`
- `E:\1_Data\2_Logs\strategy_validation_round2_train_regime_strategy_20260720.json`
- `E:\1_Data\2_Logs\strategy_validation_round2_train_regime_strategy_20260720.md`
- `E:\1_Data\2_Logs\strategy_validation_round2_train_regime_strategy_20260720.run2.log`
- `E:\1_Data\tools\run_strategy_validation_round2_regime_strategy.py`