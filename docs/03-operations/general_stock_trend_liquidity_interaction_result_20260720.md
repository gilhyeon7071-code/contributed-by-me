# 일반종목 추세 × 구조적 유동성 증분 검증 결과

## 질문과 범위

`UPTREND` 상태인 일반종목에서 구조적 유동성이 후보 품질을 추가로 높이는지 확인했다.

- 연구 전용 탐색 결과다. 운영 후보생성기, Gate, LOCK, 주문, 체결, 원장, 브로커, paper runtime은 변경하지 않았다.
- `PRICE_HISTORY_INTEGRITY_V1`, 시점일치 KOSPI/KOSDAQ 보통주 모집단, 실제 글로벌 거래일 기준 미래 h20/h60/h120 결과를 사용했다.
- 미래 결과가 같은 날짜·같은 시장에서 상위 10%인지를 `leader`로 정의했다. 이는 종목 선별 품질 지표이며 매매 수익률이나 매매 규칙이 아니다.
- 기준은 `UPTREND` 단독이다. 비교 대상은 `UPTREND × HIGH_LIQUID`, `× MID_LIQUID`, `× LOW_LIQUID`로 사전 고정했다.

## 사실

전체 2020-01-02~2025-12-31, h60 기준:

| 조건 | 후보 행 | 고유 신호일 | 미래 leader 비율 | 전체 기준 대비 | UPTREND 대비 증분 |
|---|---:|---:|---:|---:|---:|
| UPTREND | 386,934 | 1,404 | 11.7877% | 1.1694 | 1.0000 |
| UPTREND × HIGH_LIQUID | 235,891 | 1,404 | 12.5703% | 1.2470 | 1.0664 |
| UPTREND × MID_LIQUID | 113,546 | 1,404 | 10.1854% | 1.0104 | 0.8641 |
| UPTREND × LOW_LIQUID | 37,497 | 1,387 | 9.8793% | 0.9800 | 0.8391 |

기간별 `UPTREND × HIGH_LIQUID`의 UPTREND 대비 증분은 다음과 같다.

| 보유 결과 구간 | 2020-21 | 2022-23 | 2024 | 2025 | 1 초과 기간 수 |
|---|---:|---:|---:|---:|---:|
| h20 | 1.0072 | 1.1261 | 1.1678 | 1.2142 | 4 / 4 |
| h60 | 0.9728 | 1.0986 | 1.1407 | 1.0867 | 3 / 4 |
| h120 | 0.9276 | 1.0719 | 1.1730 | 0.8049 | 2 / 4 |

독립 재계산으로 위 `HIGH_LIQUID leader 비율 ÷ UPTREND leader 비율`이 출력된 증분 수치와 각 기간·구간에서 일치함을 확인했다.

## 해석

`UPTREND × HIGH_LIQUID`는 단기(h20)와 중기(h60) 후보 품질을 높일 가능성이 있는 첫 번째 양성 구조다. 반면 `MID_LIQUID`와 `LOW_LIQUID`는 전체 h60에서 UPTREND보다 낮아, 추가 후보 조건으로 지지되지 않았다.

이 결과는 장기 h120까지 일관된 보유 우위나 실제 매매 기대값을 뜻하지 않는다. 따라서 다음 세부 검증의 출발 가설은 `UPTREND × HIGH_LIQUID` 하나로 제한할 수 있지만, 아직 후보 생성 규칙·진입·보유·비용을 결정하거나 운영에 반영할 근거는 아니다.

## 검증 상태

| 항목 | 상태 | 근거 |
|---|---|---|
| 기능 | PASS | 실행 종료 코드 0, JSON `status=OK`, stderr 비어 있음 |
| 정합성 | PASS | 시점일치 모집단·가격 무결성 계약·같은 신호일 UPTREND 기준을 사용했고 증분 비율을 독립 재계산함 |
| 운영 반영 | NA | 연구 산출물만 생성, 전체로직 미적용 |
| 정책 | PASS | 탐색 전용, 승격 금지 및 운영 변경 없음 |
| FAIL-CLOSED | PASS | Gate·주문·체결·원장·브로커 경로를 호출하지 않음 |
| 회귀 | PASS | 실행 파일 문법 검사와 범위 내 `git diff --check` 통과 |

## 증거

- 실행기: `E:\1_Data\tools\run_general_stock_trend_liquidity_interaction.py`
- 요약: `E:\1_Data\2_Logs\general_stock_trend_liquidity_interaction_20260720_summary.csv`
- 계약·전체 결과: `E:\1_Data\2_Logs\general_stock_trend_liquidity_interaction_20260720.json`
- 실행 로그: `E:\1_Data\2_Logs\general_stock_trend_liquidity_interaction_20260720.stdout.log`
- 실행 계획: `E:\1_Data\docs\exec-plans\active\20260720_general_stock_trend_liquidity_interaction.md`

## 검증하지 않은 것

- 다음 시가 진입, 보유·청산 방식, 거래비용·슬리피지, 체결 가능성
- 현재 후보 생성기·일반매매·급등매매 로직에 적용했을 때의 결과
- 새 연구 후보 생성기로 누적할 가상매매 표본
