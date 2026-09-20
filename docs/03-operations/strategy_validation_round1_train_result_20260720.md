# 전략 검증 탐색 라운드 1 — Train 결과

## 지시사항

기존 운영 로직을 정답으로 사용하지 않고, 상황·전략 문법·신호의 독립 기여만 읽기 전용으로 검증한다.

## 고정 계약

- KOSPI/KOSDAQ 보통주: `krx_population_static_membership_latest.parquet`의 일자별 회원 자격을 가격 이력과 조인.
- 가격: `PRICE_HISTORY_INTEGRITY_V1` 적용 후 2,083,264행.
- 신호일: 2020-01-02 ~ 2023-12-29 Train만 사용.
- 진입/청산: 다음 실제 전역 거래일 시가 진입, h5 실제 거래일 종가 청산.
- 비용: 매수·매도 수수료 각 0.5%, 매수·매도 슬리피지 각 0.1%, 매도세 0.2%.
- 주 지표: 신호일 동일가중 후보 바스켓의 비용 차감 h5 수익률.
- 운영 후보·final score·Gate·주문·체결·paper 매매기록: 사용하지 않음.

## 직접 실행 결과

- 적격 행: 415,001
- 적격 신호일: 987
- 레짐 truncate-replay: 2021-12-31, 2022-12-30, 2023-12-29 세 절단점 모두 불일치 0일.
- 모든 결과는 단일 축 탐색 결과이며, Validation/OOS 선택·축 결합·운영 적용은 하지 않았다.

| 축 | 조건 | 일별 순 h5 평균 | 시장 대비 평균 초과수익 | 상태 |
|---|---|---:|---:|---|
| 기준선 | 전체 적격 모집단 | -136.33bp | 0.00bp | 기술 통계 |
| 상황 | BEAR | -145.26bp | 0.00bp | 기술 통계 |
| 상황 | BULL | -154.59bp | 0.00bp | 기술 통계 |
| 상황 | SIDEWAYS | -116.41bp | 0.00bp | 기술 통계 |
| 상황 | STRESS | +34.31bp | 0.00bp | 표본 보류: 반기 커버리지 1개 |
| 상황 | TRANSITION | -151.28bp | 0.00bp | 기술 통계 |
| 전략 문법 | 볼린저 평균회귀 | -166.46bp | -22.16bp | 기술 통계 |
| 전략 문법 | MA20/MA60 상향교차 | -121.53bp | +9.47bp | 기술 통계 |
| 전략 문법 | 252일 돌파 | -161.88bp | -34.56bp | 기술 통계 |
| 신호 | stretch 하위 20% | -123.30bp | +13.20bp | 기술 통계 |
| 신호 | RS 상위 20% | -136.03bp | -0.82bp | 기술 통계 |
| 신호 | RS slope 상위 20% | -143.74bp | -7.64bp | 기술 통계 |
| 신호 | 거래대금 가속 상위 20% | -160.65bp | -23.70bp | 기술 통계 |

## 해석

- 이번 고정 h5·비용 계약에서는, 다음 확증 단계로 보낼 양의 절대 순수익 단일 축이 없다.
- MA 상향교차와 stretch 하위 20%는 기준선보다 소폭 높았지만 비용 차감 절대 순수익은 여전히 음수다. 따라서 통과 후보가 아니다.
- STRESS의 양의 평균은 반기 커버리지 기준을 충족하지 못해 `DEFERRED_INSUFFICIENT_SAMPLE`이다.
- 이는 현재 로직 유지 결론도, 전략 전체 실패 결론도 아니다. 등록한 단일 축 후보들이 이 실행 계약에서 다음 단계로 승격되지 않았다는 탐색 결과다.

## 검증 상태

| 항목 | 상태 | 근거 |
|---|---|---|
| 기능 | PASS | CSV·JSON·Markdown 산출물 생성, JSON `status=OK` |
| 정합성 | PASS | 가격 무결성, 일자별 KOSPI/KOSDAQ 모집단, 실제 거래일 h5, 레짐 truncate-replay 확인 |
| 운영 반영 | NA | 읽기 전용 연구, 운영 경로 미변경 |
| 정책 | PASS | Gate·임계값·주문 정책 변경 없음 |
| FAIL-CLOSED | PASS | 주문·체결·원장·브로커 경로 미변경 |
| 회귀 | PASS | 연구 스크립트 문법 및 최종 산출물 검증 |

## 증거

- `E:\1_Data\2_Logs\strategy_validation_round1_train_axis_summary_20260720.csv`
- `E:\1_Data\2_Logs\strategy_validation_round1_train_axis_summary_20260720.json`
- `E:\1_Data\2_Logs\strategy_validation_round1_train_axis_summary_20260720.md`
- `E:\1_Data\2_Logs\strategy_validation_round1_train_axis_summary_20260720.run4.log`
- `E:\1_Data\tools\run_strategy_validation_round1.py`

## 무효 시도 기록

첫 실행 결과는 Markdown 의존성 부재와 표본 에피소드 의미 오류가 있어 판정에 사용하지 않았다. 해당 파일은 백업 경로에 보존했다.