# RootA 전략 목표 계약 정리 초안

- 작성일: 2026-09-14
- 상태: `DRAFT_NON_OPERATIONAL`
- 정책 변경: 없음
- 운영 반영: 없음
- 목적: 현재 운영 목표와 신규 구조 제안을 분리하고, 구현 전에 결정해야 할 항목을 고정한다.

## 0. 승인된 방향

- C1 승인: 2026-09-14 사용자 `진행해`.
- C2 승인: 2026-09-14 사용자 `네`.
- C3 승인: 2026-09-14 사용자 `네`.
- C4 승인: 2026-09-14 사용자 `네`.
- C5 승인: 2026-09-14 사용자 `네`.
- C6 승인: 2026-09-14 사용자 `네`.
- C7 승인: 2026-09-14 사용자 `승인`.
- C8 승인: 2026-09-14 사용자 `네`.
- C9 승인: 2026-09-14 사용자 `네`.
- C10 승인: 2026-09-14 사용자 `네`.
- C11 승인: 2026-09-14 사용자 `진행해`.
- C12 승인: 2026-09-15 사용자 `진행해`.
- 기존 O6 운영을 유지한다.
- 신규 분기·시총가중 구조는 격리된 가상매매의 차기 구현 목표로 둔다.
- 신규 구조의 E2E와 성과 검증 전에는 기존 운영을 교체하지 않는다.
- N 산정 자본은 분기 구성일 직전 거래일 종가 기준 전략 전용 순자산을 사용한다.
- 첫 검증 라운드는 초기 전략자본 1억원, N=100을 라운드 종료까지 동결한다.
- 종목 선정은 전 분기 마지막 KRX 거래일 종가, 예정 집행은 다음 분기 첫 KRX 거래일로 분리한다.
- MA200은 일별 종가로 판정하고 다음 거래일부터 동일 바스켓의 목표 노출을 50% 또는 100%로 적용한다.
- 시총가중 상한은 바스켓 내부 20%이며 초과분은 비상한 종목에 시총비례로 반복 재배분한다.

이 문서는 전략을 승인하거나 교체하지 않는다. 미결 항목이 승인되기 전에는 기존 O6 운영 목표와
FAIL-CLOSED, Gate, LOCK, 주문, 체결, 원장 정책을 그대로 유지한다.

## 1. 현재 효력이 있는 운영 목표

`OBJECTIVE_LEDGER.md`의 현재 목적 O6가 운영 기준이다.

```text
신호 -> 후보 -> 진입판정 -> 주문 -> 발주 -> 체결 -> 원장 -> 통계
```

- 한 거래일에 위 사슬이 한 번 끝까지 실행되고 6개 검증축이 모두 PASS여야 종료된다.
- 신규 분기·시총가중 전략은 이 운영 사슬에 아직 구현되거나 승인되지 않았다.
- 현재 주문·체결 산출물은 신규 전략의 운영 반영 증거로 사용하지 않는다.

## 2. 신규 구조 제안

`SYSTEM_DEFINITION.md`에서 추출한 연구·제안 상태의 구조다.

| 단계 | 제안 내용 | 현재 상태 |
|---|---|---|
| 시장 | KOSPI | 제안 |
| 유니버스 | 시가총액 상위 N | N 규칙 미확정 |
| 종목 신호 | 사용하지 않음 | 제안 |
| 구성 시점 | 분기 1회 | 정확한 달력 규칙 미확정 |
| 배분 | 시가총액 가중 | 제안 |
| 종목 상한 | 20% | 제안 |
| 시장 방어 | KOSPI200이 MA200 아래면 총 노출 50% | 전환 시점·주문 규칙 미확정 |
| 보유·청산 | 다음 분기 구성일까지 보유, 이탈 종목 청산 | 예외 처리 미확정 |
| 목표 | CAGR 8% 이상 및 위험조정 성과가 KOSPI200의 90% 이상 | 연구 검증 미완료 |
| 중단 | 원금 대비 -25% 또는 3년 목표 미달 | 기준 자본·평가 주기 미확정 |

## 3. 교체 승인 시에만 폐기 검토할 기존 규칙

아래 항목은 현재 운영에서 폐기된 것이 아니다. 신규 구조 교체가 승인될 경우에만 대상이 된다.

- `vol60` 상위 20% 제외
- 동일가중 슬롯 배분
- 10거래일 재조정
- 개별 종목 진입 점수와 후보 축소기
- 개별 종목 청산 신호와 최대 보유기간
- 신규 구조와 중복되는 개별 종목용 risk gate 및 regime override

## 4. 유지할 기반 구조

- `orders(D) -> fills(D) -> ledger -> stats` SSOT
- D 규칙과 `exec_date`, `as_of`, `run_id` 정합성 검사
- paper와 broker 날짜·출처 분리
- 주문 전 FAIL-CLOSED
- 중복 주문, 재시도, 부분체결, 원장 멱등성 검사
- 운영 로그, 런타임 상태, 회귀 검증
- 연구 경로와 운영 경로 분리

## 5. 구현 전 필수 결정

| ID | 결정 항목 | 현재 충돌 또는 누락 | 결정 전 동작 |
|---|---|---|---|
| C1 | 신규 구조의 운영 교체 승인 여부 | `APPROVED_PARALLEL_PAPER`: O6 유지, 신규 구조는 격리 가상매매로 구현 | 즉시 운영 교체 금지 |
| C2 | N 산정에 쓰는 자본 | `APPROVED`: 분기 구성일 직전 거래일 종가 기준 전략 전용 순자산 | 동일 `as_of/run_id/strategy_id` 강제 |
| C3 | 검증 라운드의 N | `APPROVED`: 초기 전략자본 1억원, N=100, 등록 검증 라운드 동안 동결 | 라운드 중 N 재계산 금지 |
| C4 | 분기 구성일 | `APPROVED`: 전 분기 마지막 거래일 종가 선정, 다음 분기 첫 거래일 예정 집행 | 데이터 결측 시 연기, 기준일 불변 |
| C5 | MA200 관측·집행 시점 | `APPROVED`: 일별 종가 판정, 다음 거래일 동일 바스켓 노출 전환 | 결측 시 상태 유지·노출 증가 차단 |
| C6 | 20% 상한 초과분 재배분 | `APPROVED`: 바스켓 내부 20% 상한과 반복 시총비례 재배분 | 합계·상한 불변식 위반 시 중단 |
| C7 | 정수주와 잔여현금 | `APPROVED`: 내림 후 추적오차 개선 1주 보충, 잔여현금 명시 | 오차 기준 위반 시 `FAIL_INTEGERIZATION` |
| C8 | 유니버스 예외 | `APPROVED`: 선정 전 적격성 제외와 선정 후 보유 사건 처리를 분리 | 공식 시점별 상태가 없으면 목표·주문 생성 금지 |
| C9 | 주문·체결 방식 | `APPROVED`: 09:05 이후 신선 호가 기반 시장성 지정가, DAY, 익일 잔량 1회 재시도 | broker 연결 금지, paper 전용 |
| C10 | -25% 중단 기준 | `APPROVED`: 첫 paper 라운드 원금 1억원, 전략 순자산 7,500만원 이하에서 영구 래치 | 자동 재개 금지 |
| C11 | 운영 provenance | `APPROVED`: 전략 전용 SSOT, 일일 체인 키, 이벤트·재시도 계보, 신규 전략 한정 `strategy_D` | AGENTS 범위 정합화·구현 전 신규 전략 E2E PASS 금지 |
| C12 | 연구 승인 근거 | `APPROVED`: 역사 재현과 미래 paper 분리, 1년 pilot 검토와 3년 목표 판정 단계화 | 자동 승격·일반 R&D 재개 금지 |

## 6. 현재 코드와의 대응

| 구성요소 | 현재 코드 | 신규 구조 적합성 |
|---|---|---|
| 목표 종목 산출 | `tools/rebalance_portfolio.py` | `CONFLICTING`: 변동성 제외·동일가중·523종목 |
| 실행 주기 | `tools/rebalance_daily.py` | `CONFLICTING`: 기본 10거래일 |
| 가상 체결 | `tools/rebalance_paper_fill.py` | `PARTIAL`: 별도 장부·종가 체결 |
| 브로커 발주 | `tools/kis_order_dispatch_from_exec.py` | `PRESERVE`: 현재 SSOT 주문만 소비 |
| 시총 패널 | `tools/build_backtest_symbol_panel_csv.py` | `FAIL`: 최신 종목별 스냅샷을 과거 행에 결합 |
| 신규 전략 직접 테스트 | 없음 | `MISSING` |

## 6.1 C2 승인 자본 기준

2026-09-14 산출물은 서로 다른 기준시각과 정의를 사용한다.

| 출처 | 기준시각 | 확인 값 | 한계 |
|---|---|---|---|
| `capital_operation_decision_report_latest.json` | 15:30 | 설정자본 100,000,000원, 평가자산 104,315,371원 | 기존 전략용 판단 보고서 |
| `broker_account_basis_latest.json` | 09:42 | 평가자산 107,864,061원, 현금 3,342,301원 | 계좌 전체이며 시각이 다름 |
| `kis_account_snapshot_latest.json` | 20:20 | 보유 평가액 102,821,580원 | 현금과 총 순자산을 함께 제공하지 않음 |

따라서 현금, 설정자본, 시각이 다른 계좌 평가액 중 하나를 그대로 N 기준으로 쓰면 안 된다.

### C2 승인안

```text
strategy_allocable_equity_at_rebalance
  = 해당 전략에 귀속된 결제완료 현금
  + 해당 전략 보유 종목의 동일 as_of 종가 평가액
  - 미결제 의무금액
  - 사전 고정한 주문비용·잔여현금 준비금
```

- 기준시각은 분기 구성일 직전 거래일 종가로 고정한다.
- 동일 `as_of`, `run_id`, `strategy_id`로 현금과 보유평가액을 묶는다.
- 다른 전략의 자산, 신용·레버리지 한도, 단순 매수가능금액은 제외한다.
- 분기 중 가격 변동으로 N을 다시 계산하지 않는다.
- 이 산식은 신규 격리 가상매매에만 적용하며 기존 O6 운영에는 반영하지 않는다.

## 6.2 C3 승인 종목 수

기존 자료는 3,000만원에서 N=200이 정수주 제약 때문에 물리적으로 불가능하고, N=20~200의
성과 차이는 단조적이지 않아 성과로 N을 고를 수 없다고 판정했다. 1억원·N=200도 평균 31종목,
평균 목표비중 4.38%를 사지 못했고 최악은 79종목·11.02%였다.

신규 구조는 격리 가상매매로 검증해야 하므로 검증 도중 N을 자산 변화에 따라 바꾸면 두 전략을
동시에 측정하게 된다. 따라서 첫 검증 라운드는 다음과 같이 고정하는 것이 적절하다.

```text
initial_strategy_capital = 100,000,000 KRW
N = 100
N_lock_scope = one registered validation round
N_recalc_during_round = false
```

- 과거에 언급된 현금 98,153,000원은 C2의 전략 전용 순자산이 아니므로 N 판정에 직접 쓰지 않는다.
- 라운드 시작 시 전략 전용 순자산과 초기자본 1억원을 별도 기록한다.
- N=100은 성과 우위 주장이 아니라 1억원 검증계좌의 집행 가능성과 비교 안정성을 위한 값이다.
- 라운드 종료 후에만 50/100/200 자본 구간 또는 동적 N 규칙을 별도 검정한다.
- N=100은 신규 격리 가상매매에만 적용하며 기존 O6 운영에는 반영하지 않는다.

## 6.3 C4 승인 분기 구성일

기존 자료는 `분기 형성일 27개`를 사용했다고 기록하지만, 형성일 산출과 가격 시점을 재현할
스크립트가 남아 있지 않다. 첫 거래일 종가로 종목을 고른 뒤 같은 날 체결한 것으로 계산했다면
선견이 된다. 따라서 신규 가상매매는 선택 시점과 집행일을 분리해야 한다.

### C4 권고안

```text
selection_as_of
  = 3월, 6월, 9월, 12월의 마지막 KRX 거래일 종가

scheduled_execution_date
  = selection_as_of 다음 KRX 거래일
  = 다음 분기의 첫 KRX 거래일
```

- 종목 순위, 시가총액 가중치, 전략 전용 순자산은 `selection_as_of`로 고정한다.
- 종가 데이터와 유니버스 검증이 끝난 뒤에만 주문을 생성한다.
- 자료가 늦거나 결측이면 예정 집행일에는 주문을 만들지 않고 FAIL-CLOSED한다.
- 검증이 끝난 다음 거래일로 집행을 연기하되 `selection_as_of`를 새 날짜로 바꾸지 않는다.
- `rebalance_id=전략ID+selection_as_of`를 사용해 같은 분기 중복 생성을 막는다.
- 실제 주문 유형과 가격 기준은 C9에서 별도로 확정한다.
- 기존 27개 형성일 성과는 이 일자 계약으로 재현되기 전까지 연구 참고값으로만 둔다.

## 6.4 C5 승인 MA200 관측·집행 시점

기존 정의는 KOSPI200이 MA200 아래면 50%, 위로 올라오면 100%라고 했지만 판정 가격과
효력 시점이 없다. 당일 종가로 판정해 당일 수익률에 적용하면 선견이며, 분기 사이 매수를
전부 금지하면 50%에서 100%로 복귀할 수 없다.

현재 `index_daily_history.csv`의 `index_code=2001`은 KOSPI200으로 확인된다. 2026-09-14 기준
종가 1,050.83, 200거래일 단순이동평균 940.96875로 현재 계산 상태는 `FULL_100`이다.

### C5 권고안

```text
observation_time = KRX 장 마감 후 일별 종가 확정 시점
index_source = index_code 2001, KOSPI200
ma200 = 최근 200개 유효 KRX 거래일 종가의 단순평균

if close_t < ma200_t:
    target_exposure_t_plus_1 = 0.50
else:
    target_exposure_t_plus_1 = 1.00
```

- 당일 종가 `t`로 계산한 상태는 다음 KRX 거래일 `t+1`부터 효력이 있다.
- `close == ma200`은 100% 구간으로 정의한다.
- 분기 사이에도 노출 전환은 일별로 허용한다.
- 노출 전환 때 종목 구성과 목표 가중치는 바꾸지 않고 동일 바스켓 수량만 비례 조절한다.
- 따라서 “분기 사이 매수 없음”은 “분기 사이 신규 구성종목 선정 없음”으로 좁혀 해석한다.
- 지수 데이터가 없거나 200개 유효 종가가 부족하거나 날짜가 낡으면 상태를 전환하지 않는다.
  현재 노출을 유지하되 50%에서 100%로 늘리는 주문은 유효 데이터가 생길 때까지 차단한다.
- `exposure_event_id=전략ID+observation_date+target_exposure`로 중복 주문을 막는다.
- 기존 MA200 성과는 `t+1` 효력 규칙으로 재현되기 전까지 연구 참고값으로만 둔다.
- 실제 주문 유형, 체결 가격, 수량 반올림은 C7과 C9에서 확정한다.

## 6.5 C6 승인 20% 상한 재배분

시가총액 비중을 20%에서 단순 절사하면 초과분이 현금으로 남아 목표 노출과 실제 노출이 달라진다.
또한 상한을 계좌 전체 기준으로 적용하면 MA200 50% 국면에서 상위 종목이 투자 바스켓의 40%까지
커질 수 있어 100% 바스켓을 절반으로 줄인 구조가 아니다.

### C6 권고안

1. N개 종목의 양수 시가총액을 합쳐 원시 바스켓 비중을 계산한다.
2. 원시 비중이 20%를 넘는 종목은 바스켓 비중 20%로 고정한다.
3. 남은 비중을 아직 상한에 닿지 않은 종목에 시가총액 비율로 재배분한다.
4. 새로 20%를 넘는 종목이 있으면 2~3단계를 반복한다.
5. 최종 바스켓 비중 합계가 1.0인지 검증한 뒤 목표 노출 1.0 또는 0.5를 곱한다.

```text
raw_basket_weight_i = market_cap_i / sum(market_cap)
capped_basket_weight_i <= 0.20
sum(capped_basket_weight) = 1.00

account_target_weight_i
  = capped_basket_weight_i * target_exposure
```

- 상한 20%는 계좌 전체가 아니라 노출 적용 전 투자 바스켓 내부 기준이다.
- 정상 100% 국면의 종목별 계좌비중 상한은 20%, 방어 50% 국면은 10%다.
- 초과분은 동일 `selection_as_of`의 시가총액 비율로만 재배분하고 동일가중으로 바꾸지 않는다.
- 계산 순서는 `market_cap 내림차순, code 오름차순`으로 고정한다.
- 유효 시가총액 종목이 5개 미만이면 20% 상한으로 100% 배분할 수 없으므로 목표 생성을 중단한다.
- 최종 비중 합계 오차가 `1e-10`을 넘거나 종목별 상한을 넘으면 FAIL-CLOSED한다.
- 원시 비중, 상한 적용 전후 비중, 재배분 횟수, 잔여 비중을 산출물에 기록한다.
- 정수주 변환 뒤 생기는 현금과 실제 비중 오차는 C7에서 처리한다.
- 기존 20% 상한 성과는 이 반복 재배분 방식으로 재현되기 전까지 참고값으로만 둔다.

## 6.6 C7 승인 정수주와 잔여현금

목표금액을 가격으로 나눈 뒤 단순 내림하면 안전하게 예산 안에 들어오지만 현금이 과도하게 남을 수
있다. 반대로 0주 종목을 제거하고 남은 종목을 재정규화하면 검증하지 않은 대형주 집중이 생긴다.
따라서 목표 비중은 유지한 채 정수주 추적오차를 최소화해야 한다.

### C7 권고안

```text
target_budget
  = strategy_allocable_equity_at_rebalance * target_exposure

target_value_i
  = strategy_allocable_equity_at_rebalance * account_target_weight_i

initial_target_qty_i
  = floor(target_value_i / sizing_price_i)
```

1. 모든 종목을 내림 수량으로 시작한다.
2. 남은 예산으로 1주를 추가했을 때 절대 목표금액 오차가 실제로 줄어드는 종목만 후보로 둔다.
3. 오차 개선액이 큰 순서, 동률이면 시총 내림차순·코드 오름차순으로 1주씩 추가한다.
4. 추가 후 종목별 계좌비중 상한과 전체 예산을 넘으면 해당 추가를 금지한다.
5. 더 이상 오차를 개선하면서 살 수 있는 종목이 없으면 종료하고 남은 금액을 현금으로 기록한다.

- `sizing_price`의 정확한 가격 계약은 C9에서 정한다.
- 수수료·세금·미결제 의무·주문 준비금은 매수 가능 예산에서 먼저 제외한다.
- 목표금액보다 1주 가격이 큰 종목도 1주 추가 후 오차가 줄고 상한·예산을 지키면 허용한다.
- 0주 종목을 삭제하거나 남은 종목 비중을 재정규화하지 않는다.
- MA200 50%로 의도된 현금과 C2 준비금은 정수주 잔여현금에서 제외한다.

### C7 검증 기준

```text
zero_qty_target_weight
  = sum(target account weight for names with target_qty == 0)

integerization_cash_drift
  = unspent integerization cash / target_budget

zero_qty_target_weight < 0.10
integerization_cash_drift < 0.10
actual_notional + estimated_cost <= target_budget
actual_account_weight_i <= 0.20 * target_exposure
```

- 두 10% 기준은 기존 문서의 “못 담은 비중과 정수 오차를 한 자릿수로 제한”한 집행 기준을 따른다.
- 기준을 넘으면 N이나 비중을 자동 변경하지 않고 해당 검증 라운드를 `FAIL_INTEGERIZATION`으로 둔다.
- 0주 종목 수·목표비중, 최초 내림 수량, 추가 수량, 목표·실제 금액, 잔여현금, 추적오차를 기록한다.
- 기존 정수주 성과는 이 알고리즘으로 재현되기 전까지 참고값으로만 둔다.

## 6.7 C8 승인 유니버스 예외

분기 선정 전에 알 수 있는 적격성 정보와 선정 뒤 발생한 보유 사건을 같은 제외 필터로 처리하면
선견 또는 분기 중 임의 종목 교체가 생긴다. 신규 가상매매는 두 단계를 분리해야 한다.

### C8 권고안 A: 분기 선정 시점 적격성

`selection_as_of`에 다음 조건을 모두 만족한 종목만 시가총액 순위 모집단에 넣는다.

1. 공식 시점별 KRX 자료에서 `KOSPI` 소속이다.
2. 공식 종목 유형이 `COMMON`이다. 우선주·리츠·SPAC·ETF·ETN·기타 증권은 제외한다.
3. `listed_date <= selection_as_of < delisted_date`이며, 상장폐지일이 없으면 계속 상장으로 본다.
4. 기준일 현재 거래정지·관리종목·상장폐지 절차·합병 소멸 예정 상태가 아니다.
5. 기준일의 양수 종가와 양수 시가총액이 있고 두 값의 `as_of`가 일치한다.

- 신규상장 종목에 별도 상장기간 문턱은 두지 않는다. 기준일에 위 조건을 만족하면 다음 분기
  선정에서만 경쟁하며 분기 중에는 추가하지 않는다.
- 제외는 시가총액 순위 계산 전에 적용하고, 남은 적격 모집단에서 상위 N개를 뽑는다.
- 적격 종목이 N개 미만이면 N을 줄이지 않고 해당 리밸런싱을 중단한다.
- 상태가 `unknown`이거나 출처 날짜·해시가 없으면 정상으로 추정하지 않고 목표 생성을 중단한다.

### C8 권고안 B: 선정 후 보유 사건

| 사건 | 신규 주문 | 기존 보유 | 분기 중 대체·재투자 |
|---|---|---|---|
| 신규상장 | 다음 분기까지 금지 | 해당 없음 | 금지 |
| 거래정지 | 해당 종목 주문 금지 | 수량 동결, 마지막 유효 종가로 별도 stale 평가 | 대체 금지 |
| 관리·상폐 절차 지정 | 미체결 매수 취소 | 첫 거래 가능 세션에 전량 매도 대기 | 처분대금은 다음 분기까지 `exception_cash` |
| 실제 상장폐지 | 주문·가상체결 생성 금지 | 공식 회수액이 없으면 연구 평가는 0원, paper는 미해결 사건으로 기록 | 금지 |
| 합병 | 검증된 조건 전 주문 금지 | 공식 교환비율·현금대가·효력일로만 승계 | 임의 대체 금지 |

- 거래정지 보유분은 0원으로 보지 않는다. 마지막 유효 종가와 가격일을 기록하되 자유롭게 쓸 수
  없는 `locked_position_value`로 분리하여 매수 가능 예산에 중복 반영하지 않는다.
- 관리·상폐 지정은 알파 청산신호가 아니라 생존·거래가능성 사건이다. 공식 지정 이후 첫 거래 가능
  가격으로만 처분하며, 거래가 없는데 가상 체결을 만들지 않는다.
- 실제 상장폐지의 역사 연구는 생존편향을 막기 위해 공식 회수액이 없으면 0원으로 평가한다.
  가상매매 원장에는 존재하지 않은 체결을 만들지 않고 `UNRESOLVED_DELISTING`으로 남긴다.
- 합병은 종목코드, 수량, 원가, 단주 현금의 승계 근거가 모두 있을 때만 반영한다. 하나라도 없으면
  `UNRESOLVED_CORPORATE_ACTION`으로 동결하고 목표·주문 생성을 중단한다.
- 분기 중 강제 처분으로 생긴 현금은 MA200 의도현금, C2 준비금, C7 정수주 잔여현금과 분리한다.

### C8 필수 provenance와 현재 데이터 판정

각 판정에는 `strategy_id`, `rebalance_id`, `selection_as_of`, `status_as_of`, `source`,
`source_sha256`, `security_type`, `listing_status`, `trade_status`, `event_type`, `event_effective_date`,
`decision`, `reason`, `valuation_price`, `valuation_price_date`를 기록한다.

현재 자료는 C8 구현 입력으로 부족하다.

- `_cache/krx_point_in_time_listing_history_latest.csv`: 2026-07-15 생성, 0행.
- `2_Logs/krx_point_in_time_listing_history_status_latest.json`: `FAIL_SOURCE_EMPTY_OR_BELOW_MIN_ROWS`.
- `_cache/krx_population_static_input_manual_v1.csv`: 3,177행 검증 PASS지만 2026-07-15 수동
  스냅샷이며 거래정지·관리종목·합병 사건 필드가 없다.
- `2_Logs/market_master_latest.json`: 2026-09-14 생성, 3,037코드의 시장 매핑만 제공하며
  시점별 현재 상장 여부와 사건 상태를 증명하지 않는다.

따라서 C8 정책을 승인해도 공식 시점별 상태 소스가 구축·검증되기 전까지 구현 데이터 준비도는
`FAIL_C8_UNIVERSE_SOURCE`이고 신규 목표·주문을 만들 수 없다.

## 6.8 C9 승인 주문·체결 방식

C4의 다음 거래일 집행과 C7의 예산·정수주 불변식을 함께 지키려면 전일 종가로 시장가 수량을
만들거나 체결 뒤 현금에 맞춰 수량을 조용히 줄여서는 안 된다. 신규 전략은 실제 집행 시점의
호가로 가격과 수량을 확정하고, 가상 체결의 근거를 별도로 남겨야 한다.

### C9 권고안 A: 적용 범위와 주문 가격

```text
execution_scope = ISOLATED_PAPER_ONLY
broker_dispatch = FORBIDDEN
execution_window = 09:05:00 <= KST <= 15:20:00
order_type = LIMIT
time_in_force = DAY
quote_max_age_seconds = 5

BUY  sizing_price = limit_price = valid ask1 rounded up to KRX tick
SELL limit_price = valid bid1 rounded down to KRX tick
```

- 분기 리밸런싱과 MA200 노출 전환 모두 예정 집행일 09:05 이후 첫 유효 호가부터 시작한다.
- 시장가 매수는 사용하지 않는다. KIS 시장가 매수가능수량이 상한가 기준으로 계산되어 C7의
  목표 예산과 실제 가용수량이 구조적으로 어긋나는 문제도 피한다.
- 호가에는 종목코드, 거래일, 거래소 시각, ask1·bid1, 잔량, 출처, 원문 해시를 기록한다.
- 호가가 없거나 5초보다 낡거나 ask1·bid1·잔량이 양수가 아니면 해당 종목 주문을 만들지 않는다.
- 예정 목표비중 중 주문 불가 종목의 합이 10% 이상이면 매도 전에 전체 정기 리밸런싱을
  `FAIL_EXECUTION_COVERAGE`로 중단한다. 10% 미만이면 해당 종목만 건너뛰고 부족 비중을
  `execution_shortfall_weight`로 기록하며 다른 종목에 재배분하지 않는다.
- C8 관리·상폐 강제 처분은 정기 리밸런싱보다 우선하는 별도 사건 주문이므로 위 매수 커버리지
  실패 때문에 취소하지 않는다.

### C9 권고안 B: 주문 순서와 체결

1. 같은 종목의 BUY·SELL 동시 존재, 음수·0 수량, 보유수량 초과 매도, 예산 초과 매수를 사전 차단한다.
2. 매도 주문을 먼저 제출하되, 체결되지 않은 매도대금을 매수 가능 현금으로 가정하지 않는다.
3. 확인된 매도 체결과 기존 가용현금 범위에서만 C7 알고리즘으로 매수 수량을 확정한다.
4. 제출 시점의 반대 호가 잔량 중 지정가 이내 물량만 즉시 가상 체결하고 가격은 소비 호가의
   수량가중평균으로 기록한다.
5. 잔량은 DAY 주문으로 유지하며, 이후 체결은 `order_submitted_at` 이후 관측된 체결 근거가
   지정가를 만족할 때만 추가한다. 주문 전 가격이나 장 마감 종가로 소급 체결하지 않는다.
6. 누적 체결수량은 주문수량을 넘을 수 없고, 부분체결마다 별도 `fill_id`를 만들되 같은
   `order_id` 아래 합산한다.

### C9 권고안 C: 취소·재시도·멱등성

- 15:20에 미체결 잔량을 취소하고 `CANCELLED_UNFILLED` 또는 `PARTIAL_CANCELLED`로 확정한다.
- 남은 목표수량은 다음 KRX 거래일에 신선 호가로 한 번만 다시 계산·제출한다.
  `replay_policy=NEXT_SESSION_REPLAY_ONCE`; 두 번째 날 잔량은 다음 분기까지
  `execution_shortfall_cash`로 남긴다.
- 재시도 때도 종목별 계좌비중 상한, C7 예산, 이미 체결된 누적수량을 다시 검증한다.
- 전송 결과가 불명확하면 `UNKNOWN_PENDING`으로 두고 주문 조회로 미접수 사실이 확인되기 전까지
  재전송하지 않는다.
- `intent_id=strategy_id+event_id+code+side+target_version`, `order_id=intent_id+attempt_no`를
  결정적으로 만들고 동일 키의 중복 실행을 건너뛴다. 강제 재전송 옵션은 신규 전략에서 금지한다.
- 각 행에 `strategy_id`, `rebalance_id` 또는 `exposure_event_id`, `selection_as_of`, `exec_date`,
  `run_id`, `intent_id`, `order_id`, `attempt_no`, `order_submitted_at`, `quote_ts`, `limit_price`,
  `requested_qty`, `filled_qty`, `remaining_qty`, `status`, `reason`을 기록한다.

### C9 현재 구현 대응

- `tools/kis_order_dispatch_from_exec.py`에는 지정가, resting limit, LOB 잔량, 중복 차단,
  `UNKNOWN_PENDING` 처리 일부가 있어 보존 가능한 기반이다.
- `tools/kis_sync_fills_from_api.py`에는 주문별 부분체결 합산과 초과체결·날짜 정합성 검사가 있다.
- 기존 `tools/rebalance_paper_fill.py`는 집행일 종가로 즉시 체결하고 현금 부족 시 수량을 줄이며,
  취소·부분체결·익일 재시도 계약이 없어 신규 C9와 `CONFLICTING`이다.
- 현재 `paper/fills.csv`와 기존 주문 파일은 신규 전략의 C11 provenance를 갖추지 않았으므로
  신규 전략 증거로 사용할 수 없다.
- N=100에 대한 신선 호가 수집, paper 체결 소비자, 잔량 상태기가 아직 없으므로 구현 준비도는
  `PARTIAL_C9_EXECUTION_INFRA`다.

## 6.9 C10 승인 원금 대비 -25% 중단 기준

`SYSTEM_DEFINITION.md`의 손실 한도는 고점 대비 낙폭이 아니라 원금 대비 손실이다. 따라서 기존
O6의 계좌 전체 MDD, DDM 단계, 일일손실, Kelly, ES를 신규 전략에 연결하지 않는다.

### C10 권고안 A: 기준 원금과 평가 자산

```text
initial_loss_limit_capital = 100,000,000 KRW
loss_limit_ratio = -0.25
loss_limit_equity = 75,000,000 KRW

strategy_nav_for_loss_limit
  = strategy_owned_settled_cash
  + strategy_owned_unsettled_sell_receivable
  + strategy_owned_dividend_receivable
  + sum(strategy_position_qty_i * valid_mark_price_i)
  - strategy_owned_unsettled_buy_obligation
  - accrued_fee_tax_and_other_liability

loss_from_initial
  = strategy_nav_for_loss_limit / initial_loss_limit_capital - 1

trigger if loss_from_initial <= -0.25
```

- 첫 격리 가상매매 라운드의 C3 초기자본 1억원을 분모로 동결한다. 고점이 올라가도 분모를
  올리지 않으며, 분기마다 재설정하지 않는다.
- C2의 `strategy_allocable_equity_at_rebalance`는 매수 예산이고 C10의 순자산은 손실 판정값이다.
  주문 준비금, MA200 의도현금, C7 정수주 잔여현금, C8 `exception_cash`는 보유 자산이므로
  C10 순자산에 포함한다.
- 실제 지급할 수수료·세금·미결제 매수대금은 차감하고, 미결제 매도대금과 확정 배당채권은
  중복 없이 한 번만 포함한다.
- 다른 전략이나 broker 계좌 전체 자산·손익은 포함하지 않는다.
- 검증 라운드 중 외부 입출금과 자본 추가를 금지한다. 발생하면 한도를 재계산하지 않고
  `INVALIDATED_BY_CAPITAL_FLOW`로 라운드를 종료한다. 배당·합병대가·매매결제는 외부 입출금이 아니다.

### C10 권고안 B: 평가 시점과 데이터 실패

- 주문 생성 직전, 각 체결 반영 직후, 장 마감 후 공식 종가 확정 시점에 평가한다.
- 장중에는 모든 보유 종목의 5초 이내 유효 호가가 있을 때만 확정 판정을 허용한다.
- 장 마감 판정은 해당 거래일 공식 종가를 사용하고, C8 거래정지 보유는 마지막 유효 종가와
  가격일을 표시한 `locked_position_value`로 포함한다.
- 보유 종목, 현금, 미결제 채권·채무, 가격의 `strategy_id/as_of/run_id`가 다르거나 하나라도
  설명 없이 빠지면 `RISK_EVAL_INCOMPLETE`로 둔다. 안전하다고 추정하지 않고 신규 매수와
  50%에서 100%로의 노출 증가를 차단한다.
- 장중 불완전 자료로 계산한 값이 7,500만원 이하이면 `PROVISIONAL_LOSS_LIMIT`로 매수부터
  중단하되, 완전한 평가 없이 가상 청산 체결을 만들지 않는다.

### C10 권고안 C: 중단 동작과 재개

1. 완전한 평가에서 7,500만원 이하가 확인되면 `TERMINATED_LOSS_LIMIT`를 영구 래치한다.
2. 모든 미체결 BUY를 취소하고 신규 BUY·노출 증가·다음 분기 재구성을 차단한다.
3. 거래 가능한 보유분은 C9 주문 계약으로 전량 처분하고, 거래정지·상폐·합병 미해결분은 C8로 넘긴다.
4. 트리거 이후 가격이 7,500만원 위로 회복해도 같은 라운드를 자동 재개하지 않는다.
5. 재개는 사용자 명시 승인, 새 `round_id`, 새 초기자본·데이터 스냅샷·계약 등록이 모두 있을 때만 가능하다.

- -25%는 청산 완료 손실을 보장하는 선이 아니라 중단 발동선이다. 갭 하락, 거래정지, 상폐,
  슬리피지와 부분체결 때문에 최종 회수자산은 7,500만원보다 낮을 수 있다.
- 트리거와 청산 주문은 `loss_limit_event_id=strategy_id+round_id+first_trigger_ts`로 멱등 처리한다.
- 3년 목표 미달 조건은 긴급 손실 중단과 섞지 않고 C12 연구·운영 승인 근거에서 별도로 확정한다.

### C10 현재 구현 대응

- `capital_operation_decision_report_latest.json`과 `broker_account_basis_latest.json`은 기존 O6 또는
  broker 계좌 전체 기준이라 신규 전략 전용 손실 판정 근거가 아니다.
- `risk_orchestration_latest.json`의 MDD·DDM·일일손실·Kelly·ES는 C10에서 사용하지 않는다.
- `2_Logs/rebalance/rebal_state.json`은 505~514종목 동일가중 구형 시험 상태이며 신규 N=100
  전략의 초기자본·채권·채무·중단 래치를 증명하지 않는다.
- 신규 전략 전용 NAV 원장과 중단 이벤트 원장이 없으므로 구현 준비도는
  `MISSING_C10_STRATEGY_EQUITY_LEDGER`다.

## 6.10 C11 승인 운영 provenance

신규 전략은 기존 O6와 파일·식별자·통계를 섞지 않는 별도 namespace에서 다음 SSOT를 유지한다.

```text
target_manifest(selection_as_of, target_version_id)
  -> orders(strategy_D)
  -> fills(strategy_D)
  -> ledger(strategy_D)
  -> stats(as_of=strategy_D)
```

### C11 권고안 A: 격리 경로와 필수 식별자

- 구현 경로는 `paper/strategies/kospi_mcap_quarterly_v1/` 아래로 격리한다. 기존
  `paper/fills.csv`, `paper/trades_calc.csv`, O6 통계는 신규 전략의 입력이나 PASS 증거로 사용하지 않는다.
- 모든 단계는 `strategy_id`, `strategy_version`, `contract_version`, `round_id`, `event_type`,
  `event_id`, `execution_mode`, `source`, `manifest_id`를 보존한다.
- `event_type`은 `REBALANCE`, `EXPOSURE`, `LOSS_LIMIT`, `UNIVERSE_EXCEPTION` 중 하나이며,
  `event_id`는 각각 C4·C5·C10·C8 사건을 하나의 키로 연결한다.
- 목표 산출물은 `selection_as_of`, `target_version_id`, 목표 종목·수량·비중, 입력 데이터
  스냅샷 ID와 해시를 기록한다. 목표가 바뀌면 같은 ID를 덮어쓰지 않고 새 `target_version_id`를 만든다.
- `run_id`와 `as_of`는 하나의 거래일 체인에서 orders, 당일 fills, 당일 추가 ledger,
  stats metadata가 정확히 같아야 한다. 누적 ledger의 과거 행은 각 행이 생성된 원래 `run_id/as_of`를
  유지하고, stats는 포함한 ledger의 최대 `as_of`와 전체 입력 해시를 기록한다.
- `intent_id`는 `strategy_id+round_id+event_id+target_version_id+code+side`에 대해 결정적으로
  생성하고 재시도에도 유지한다. `order_id`는 `intent_id+exec_date+attempt_no`, `fill_id`는
  `source+order_id+fill_ts+qty+price+event_seq`, `ledger_entry_id`는 `strategy_id+fill_id`로
  결정적으로 생성한다.

### C11 권고안 B: 거래일과 익일 재시도

- 기존 O6의 AGENTS.md D 규칙은 변경하지 않는다.
- 신규 전략에는 C5 노출 축소와 C10 손실 중단처럼 BUY 없이 SELL만 발생하는 사건이 있다. 따라서
  신규 격리 namespace에 한해 `strategy_D = 해당 전략 fills에서 BUY/SELL 구분 없이 최신 실제 체결 ymd`를
  사용한다. 이는 현재 전역 `D=최신 BUY ymd`와 다른 신규 전략 한정 승인 정책이다. AGENTS.md의 명시적
  범위 정합화와 구현 전에는 운영에 적용하거나 E2E PASS로 판정하지 않는다.
- 체결이 없는 주문일은 D를 전진시키지 않는다. 주문·취소·미체결 상태만 기록하고
  `NO_NEW_STRATEGY_D/NA`로 두며 최신 ledger·stats를 새 성공본으로 발행하지 않는다.
- C9 익일 재시도는 같은 `event_id/intent_id/target_version_id`를 유지하되 새 `exec_date`, 새
  `run_id/as_of`, `attempt_no=1`을 사용한다. 전일 `run_id`에 익일 체결을 소급 추가하지 않는다.
- 해당 일자의 체결이 생기면 같은 날짜의 `orders_{strategy_D}_exec`가 반드시 존재하고, 그 주문의
  `exec_date`, fills 날짜, 신규 ledger 행의 `as_of`, stats `as_of`가 `strategy_D`와 일치해야 한다.

### C11 권고안 C: 단계별 계보와 발행 중단

| 단계 | 필수 연결 | FAIL-CLOSED 조건 |
|---|---|---|
| orders | 목표 manifest, event, intent, 일일 run | 목표에 없는 주문, 빈 필수 키, 다른 전략·라운드·날짜 |
| fills | 기존 order, code, side, 가격, 수량, 시각 | orphan fill, side 불일치, 초과체결, 중복 또는 변조된 `fill_id` |
| ledger | 검증된 fill 1건당 멱등 행 1건 | 누락·중복 `ledger_entry_id`, fill과 수량·가격 불일치 |
| stats | 검증된 누적 ledger와 입력 해시 | 직접 fills 집계, 최대 ledger 날짜와 `as_of` 불일치, 입력 해시 변동 |

- 각 실행은 `lineage_manifest_{run_id}.json`에 입력·출력 경로, SHA256, 행 수, 종목별 주문·체결
  수량, 미체결 수량, 포함 ledger ID, 판정과 실패 사유를 기록한다.
- `execution_mode=PAPER`, `source=PAPER_LOB_SIM_V1`만 허용한다. broker 주문번호나 broker source가
  한 행이라도 섞이면 `PAPER_BROKER_MIX`로 전체 일일 체인을 중단한다.
- 필수 키 불일치, `orders_exec` 부재, 날짜 불일치, orphan fill, 초과체결, 중복·변조 ID,
  해시 불일치, stats `as_of` 불일치 중 하나라도 있으면 timestamped 실패 보고서만 남긴다.
  `*_latest` 성공본, NAV, 통계는 덮어쓰지 않고 마지막 검증 성공본을 유지한다.

### C11 최악조건 판정

| 입력 | 기대 동작 | 판정 |
|---|---|---|
| 전일 주문의 잔량이 익일 체결 | 같은 event·intent, 새 일일 run과 order attempt로만 수용 | 그렇지 않으면 STOP |
| BUY 이후 다른 날 SELL만 체결 | 전역 최신 BUY D로 소급하지 않고 승인된 `strategy_D`로 분리 | AGENTS 범위 정합화 전 구현 금지 |
| 존재하지 않는 order의 fill | ledger·stats 미발행 | STOP |
| 동일 fill 재수신 또는 수량 초과 | 완전 동일 행은 멱등 건너뜀, 내용 변경·초과는 중단 | STOP |
| paper 행에 broker source 혼입 | 해당 일일 체인 전체 실패 | STOP |
| ledger 최대일 D인데 stats가 D+1 | 최신 통계 미발행 | STOP |

### C11 현재 구현 대응

- 현재 `paper/fills.csv`는 `order_id` 외에 전략·라운드·이벤트·일일 run 계보가 없고,
  `paper/trades_calc.csv`는 `order_id/fill_id`도 없다.
- 현재 `paper/orders_20260914_exec.xlsx`는 `intent_id/order_id/trace_id` 일부만 있고
  `strategy_id/round_id/run_id/as_of/event_id/execution_mode/source`가 없다.
- `paper_sync.py`는 통계 생성 시 `run_id/as_of`를 새로 만들며, `tools/kis_sync_fills_from_api.py`도
  ledger 입력에서 `run_id/as_of`를 재할당하므로 상류 provenance 보존 계약과 맞지 않는다.
- 기존 `build_order_fill_e2e_chain_report.py`는 날짜·수량·일부 fill ID를 검사하지만 신규 전략
  namespace와 전체 필수 키를 검사하지 않는다. 최신 기존 O6 보고서도 `stats_pnl_asof` 불일치로 FAIL이다.
- 따라서 C11 구현 준비도는 `MISSING_C11_END_TO_END_PROVENANCE`다.

## 6.11 C12 승인 연구 근거

이 전략은 2026-09-13~14에 이미 열람한 역사 결과를 바탕으로 선택됐다. 같은 역사 구간을 다시
정확하게 계산하는 것은 구현 재현성 증거이지 OOS 성과 증거가 아니다. 과거 자료의 최고 판정은
`REPRODUCIBLE_RESEARCH_ONLY`로 제한하고, 성과 승인 근거는 계약 동결 이후의 미래 paper에서만 만든다.

### C12 권고안 A: 증거 단계와 일반 R&D 분리

| 상태 | 충족 근거 | 허용되는 판단 |
|---|---|---|
| `IMPLEMENTATION_EVIDENCE_ONLY` | 합성·역사 replay에서 C1~C11 불변식과 E2E 6축 PASS | 코드·계약 적합성만 인정 |
| `REPRODUCIBLE_RESEARCH_ONLY` | 고정 스냅샷에서 역사 결과 재현, 누수·비용·상폐·다중시도 공개 | 과거 결과 설명만 허용 |
| `FORWARD_PAPER_OBSERVATION` | 사전등록 뒤 미래 데이터 적재 중 | 승격·실전 승인 금지 |
| `PAPER_PILOT_ELIGIBLE` | 1년 최소 표본과 운영·성과 점검 통과 | 소액 실계좌 검토만 허용 |
| `OPERATING_TARGET_PASS_RESEARCH_UNCERTAIN` | 3년 목표는 통과했으나 MDE/불확실성 기준 미달 | 목표 달성은 인정, 확증 표현 금지 |
| `CONFIRMED` | 미래 OOS, 두 목표, MDE/MES, 불확실성, 데이터·E2E 모두 통과 | 사용자 승인 절차 개시 가능 |
| `NOT_SUPPORTED_3Y_TARGET` | 3년 시점에 두 목표 중 하나라도 미달 | 전략 중단 |
| `TERMINATED_LOSS_LIMIT` | C10 원금 대비 -25% 영구 래치 | 즉시 중단, C12보다 우선 |

- 현재 일반 `rd_authorization=SUSPENDED`와 O6는 유지한다. 승인된 C12는 일반 탐색·HPO를 재개하는
  승인이 아니라 이 신규 전략 하나의 역사 재현과 미래 격리 paper 라운드를 등록할 수 있는
  범위 한정 승인으로 본다.
- 새 신호·필터·N·상한·MA 기간·노출비율·비용·체결 계약을 탐색하거나 바꾸려면 별도 탐색 라운드와
  사용자 승인이 필요하다. 그 결과를 진행 중인 확증 라운드에 합치지 않는다.

### C12 권고안 B: 사전등록과 다중비교

- 구현 실행 전에 `tools/round_preflight.py --new ... --kind CONFIRMATION`으로 신규 `round_id`를
  만들고, C1~C12 계약·코드·설정·데이터 스냅샷 SHA256을 동결한 뒤 `--freeze`와 `--check`를 통과한다.
- 첫 예정 집행일을 `round_start_date`로 사전 고정한다. 첫 체결이나 좋은 수익을 확인한 뒤 시작일을
  뒤로 미루지 않는다.
- 역사 재현에는 2026-09-10 기준 `research_trial_ledger.json`의 기록 가능한 하한 1,036회를 포함해
  이후 모든 시도를 누적한다. DSR·CPCV/PBO는 과거 선택편향 진단으로 기록하되 역사 결과를 OOS로
  승격시키는 수단으로 쓰지 않는다.
- 미래 확증은 고정 전략 1개와 아래 두 목표만 평가한다. 진단용 변형을 추가하면 그 변형은
  `EXPLORATION`으로 분리하고 원 라운드의 대체 결과로 사용하지 않는다.
- 전략·주문·수익에 영향을 주는 코드 또는 정책이 바뀌면 새 버전·새 `round_id`로 다시 시작한다.
  표시·로깅만 바뀐 경우에는 전후 입력 해시와 replay 결과가 동일함을 증명해야 기존 표본을 유지한다.

### C12 권고안 C: 성과 계산 계약

```text
strategy_cagr_net
  = (ending_strategy_nav / initial_strategy_nav) ** (365.2425 / calendar_days) - 1

strategy_risk_adjusted
  = strategy_cagr_net / (sample_std(strategy_daily_return) * sqrt(252))

benchmark_risk_adjusted
  = benchmark_cagr / (sample_std(kospi200_daily_return) * sqrt(252))
```

- 전략 수익은 C9 실제 가상체결과 실제 반영 비용을 사용한 C10 전략 전용 NAV만 쓴다. 미체결 목표를
  목표가격으로 가상 보충하거나 기존 O6 원장을 섞지 않는다.
- KOSPI200 공식 종가와 전략 NAV가 둘 다 유효한 동일 날짜만 위험조정 비교에 사용한다.
- 운영 목표는 `strategy_cagr_net >= 8%`와
  `strategy_risk_adjusted >= 0.90 * benchmark_risk_adjusted`를 모두 만족해야 통과한다.
- 벤치마크 변동성이 0이거나 가격·NAV가 불완전하면 상대 조건은 `NA`이며 통과로 간주하지 않는다.
- 월별·분기별 수익, MDD, 회전율, 체결률, 추적오차는 보조 진단이며 두 주 목표를 대신하지 않는다.
- 수치를 인용할 때 기간, 유효 거래일, 시작·종료 NAV, 비용, benchmark 날짜, 전략·round·run ID를
  함께 적는다.

### C12 권고안 D: 1년 paper와 3년 목표 판정

1년 중간 검토는 다음을 모두 만족해야 `PAPER_PILOT_ELIGIBLE`로 둘 수 있다.

1. 예정 시작일부터 KRX 거래일 252일 이상, 예정 분기 재구성 4회 이상
2. 전략 NAV와 KOSPI200의 paired valid day가 전체 대상 거래일의 95% 이상
3. 각 분기 재구성에서 C7 목표 주문금액 대비 C9 누적 체결금액이 90% 이상
4. 모든 실제 사건의 C11 provenance와 E2E 6축 PASS, orphan·중복·초과체결 0건
5. C1~C12의 전략·계약 버전 불변, 외부 입출금 없음, C10 미발동
6. 1년 점추정에서 절대 8%와 위험조정 90%를 모두 충족

- N=100의 주문 100건은 분기 독립 표본 100건이 아니라 재구성 사건 1건이다.
- 1년 통과는 성과 확증이 아니라 최대 3,000만원 소액 실계좌 pilot을 사용자에게 별도로 검토할 수
  있는 조건이다. 자동 broker 연결, O6 교체, 자본 투입을 허용하지 않는다.
- 3년은 예정 시작일부터 달력 3년과 예정 분기 재구성 12회가 모두 끝난 시점에 판정한다. 둘 중
  하나라도 미달이면 표본을 조기 완료로 간주하지 않는다.
- 3년 시점에 절대 8% 또는 위험조정 90% 중 하나라도 미달이면 `NOT_SUPPORTED_3Y_TARGET`로 종료한다.
  둘 다 통과해도 사전등록한 MDE/MES와 불확실성 기준이 미달이면
  `OPERATING_TARGET_PASS_RESEARCH_UNCERTAIN`이며 `CONFIRMED`라고 쓰지 않는다.

### C12 권고안 E: 검정력과 최악조건

- 각 라운드는 실제 고정 전략의 블록 수익 분산으로 두 주 목표의 MDE와 불확실성 계산법을
  사전등록해야 한다. `MDE > MES`이면 확증 검정을 시작하지 않고 paper 관측만 지속한다.
- 계획 민감도 예시로, 역사에서 관측된 MA200·50% 구조의 연변동 19.03%, 독립 정규 연수익 가정을
  놓으면 3년 MDE는 `(1.96+0.84)*19.03%/sqrt(3)=30.76%`로 절대 목표 8%보다 크다.
  이는 확정 수치가 아니라 3년 목표 통과와 통계적 확증을 분리해야 하는 이유를 보여주는 계획값이다.

| 최악조건 | 처리 |
|---|---|
| 역사 최고 결과를 OOS라고 재명명 | `INVALIDATED_BY_DATA_REUSE` |
| 부진 뒤 N·MA·비중·비용을 수정하고 이전 표본과 합침 | 기존 라운드 종료, 새 round 시작 |
| 한 분기 N=100 주문을 독립 표본 100건으로 집계 | `INVALID_SAMPLE_UNIT` |
| 1년 점추정만 좋고 검정력 부족 | pilot 검토까지만, 확증 금지 |
| NAV·benchmark paired coverage 95% 미만 | `DEFERRED_DATA_COVERAGE`, 조기 통과 금지 |
| 분기 체결금액 90% 미만 | `EXECUTION_NOT_REPRESENTATIVE`, 성과 승인 금지 |
| 3년 목표 하나 미달 | `NOT_SUPPORTED_3Y_TARGET` |
| 원금 대비 -25% 선도 발동 | C10 `TERMINATED_LOSS_LIMIT` 우선 |

### C12 현재 구현 대응

- `round_preflight.py --list`에는 `RD_20260831_flow_h10`, `RD_20260831_index_gap`만 있고 신규
  분기·시총가중 전략의 등록 라운드는 없다.
- `round_preflight_daily_latest.json`은 2026-09-14 09:43:53 기준 기존 frozen round의 코드·과거 행
  변경 때문에 FAIL이다. 이는 신규 전략 결과는 아니지만 현재 연구 동결 관리가 자동 PASS가 아님을 뜻한다.
- `research_trial_ledger.json`은 2026-09-10 기준 27개 기록, 누적 시도 하한 1,036회다.
- 신규 전략 전용 데이터 스냅샷, 재현 스크립트, MDE/MES 등록, 미래 paper 표본이 모두 없다.
- 따라서 C12 준비도는 `MISSING_C12_REGISTERED_FORWARD_ROUND`다.

## 7. 구현 착수 조건

다음을 모두 충족하기 전에는 코드 교체를 시작하지 않는다.

1. C1의 병렬 가상매매 범위를 구현 ExecPlan에 고정
2. 시점별 시가총액 데이터 계약과 데이터 스냅샷 고정
3. `NEW_SIGNAL_VALIDATION_STANDARD.md`에 맞는 연구 라운드 등록
4. `.agent/PLANS.md`에 구현 ExecPlan 작성
5. paper 전용 경로에서 최악조건 및 회귀 검증
6. `orders(D) -> fills(D) -> ledger -> stats` E2E 6축 PASS

## 8. 현재 판정

```text
계약 상태        TARGET_CONTRACT_APPROVED_IMPLEMENTATION_PENDING
신규 구조 구현   미착수
현재 운영 변경   없음
전체로직 적용    전체로직 미적용
다음 진행 지점   구현 ExecPlan 작성
```
