# KOSPI 시가총액 상위 100 분기 리밸런싱 격리 가상매매 ExecPlan

- 작성일: 2026-09-15
- 상태: `SUSPENDED_2026-09-16` (이전: `APPROVED_CONTRACT / M0_IMPLEMENTED / M1_AUDITOR_IMPLEMENTED / C8_DATA_FAIL`)
- 보류 사유: 2026-09-16 사용자 결정 ③. v41.1 부품 단위 검증·판정 절차를 먼저 진행한다(OBJECTIVE_LEDGER 개정 2026-09-16). C1~C12 승인과 이 문서의 내용은 유효하며, 재개는 별도 결정이다. 보류 중 코드·정책·산출물은 변경하지 않는다.
- 분류: 신규 전략 구현, 격리 가상매매, 정책 계약 구현
- 계약 기준: `docs/references/STRATEGY_TARGET_CONTRACT_RECONCILIATION_20260914.md` C1~C12
- 구현 네임스페이스: `paper/strategies/kospi_mcap_quarterly_v1/`
- 운영 영향: 없음. 기존 O6, Gate, LOCK, 주문·체결·원장·통계는 변경하지 않는다.

## 1. 목적

승인된 C1~C12를 하나의 재현 가능한 전략 체인으로 구현한다. 구현 목표는 좋은 결과를 만드는 수치 조정이
아니라, 고정된 계약에 따라 후보 선정부터 목표 비중, 가상 주문, 체결, 원장, 통계, 검증까지 같은 계보로
연결하는 것이다.

이 문서는 구현 순서와 판정 기준을 고정한다. 이 문서 작성만으로 전략이 구현되거나 전체로직에 적용된
것으로 판정하지 않는다.

## 2. 고정 계약

1. 기존 O6는 그대로 운영하고 신규 전략은 별도 격리 가상매매로만 실행한다.
2. 자본 기준은 리밸런싱 시점의 `strategy_allocable_equity_at_rebalance`다.
3. 최초 paper 원금은 1억 원이고, 종목 수 `N=100`은 라운드 동안 고정한다.
4. 직전 분기 마지막 KRX 거래일 종가로 선정하고 다음 분기 첫 KRX 거래일에 실행한다.
5. KOSPI200 일봉 종가 MA200 아래는 익 거래일부터 노출 50%, 이상은 100%를 적용한다.
6. 종목별 목표 비중 상한은 20%이며 초과분은 상한 미도달 종목에 반복 비례 재배분한다.
7. 정수 주식화는 내림 후 추적오차를 줄이는 경우에만 1주씩 추가한다. 잔여 현금과 0주 종목의 목표
   비중이 각각 10% 이상이면 `FAIL_INTEGERIZATION`이다.
8. 유니버스는 시점별 KRX 보통주와 당시 거래·관리·기업행위 상태를 사용한다. 선정 전 자격과 선정 후
   기업행위 처리를 분리한다.
9. 실행은 09:05~15:20, 호가 나이 5초 이하, 매수 매도 1호가의 시장성 지정가, DAY, 부분체결 허용,
   15:20 미체결 취소, 다음 세션 1회 재시도다. 브로커 주문은 금지한다.
10. 전략 NAV가 7,500만 원 이하가 되면 영구 종료 래치를 건다. 자동 재시작은 없다.
11. 전략 전용 ID·계보·원장을 사용한다. `strategy_D`는 이 전략 네임스페이스의 최신 실제 BUY 또는 SELL
    체결일이며, 기존 O6의 전역 최신 BUY 기준 D는 변경하지 않는다.
12. 과거 자료의 최고 자격은 `REPRODUCIBLE_RESEARCH_ONLY`다. 계약 동결 이후 미래 격리 paper만 OOS
    근거로 사용하며, 1년 조건은 소액 실계좌 pilot 검토를 여는 조건일 뿐 자동 승인이 아니다.

## 3. 제외 범위

- 기존 O6 코드, 설정, 상태, 주문, 체결, 원장, 통계 변경
- Gate, LOCK, risk orchestration, DDM, HPO, 일반 R&D 상태 변경
- 실브로커 API 호출, 실주문, 계좌 자금 이동
- Windows 스케줄러 등록 또는 기존 배치 편입
- RootB 상태 빌더와 대시보드 표시
- 성과 목표를 맞추기 위한 임계값·점수·상태 하드코딩
- 과거 재현 결과를 OOS 또는 미래 성과로 승격

## 4. 목표 구조와 소유 경계

모든 신규 전략의 가변 산출물은 `paper/strategies/kospi_mcap_quarterly_v1/` 아래에 둔다. 기존
`paper/fills.csv`, `paper/trades_calc.csv`, `paper/orders_exec.xlsx` 및 O6 최신 산출물을 이 전략의 쓰기
대상으로 사용하지 않는다.

예정 구조:

```text
paper/strategies/kospi_mcap_quarterly_v1/
  config/
  data/source/
  data/snapshots/
  targets/
  orders/
  fills/
  ledger/
  stats/
  state/
  manifests/
  reports/
  src/
tools/
  kospi_mcap_quarterly_data.py
  kospi_mcap_quarterly_run.py
  kospi_mcap_quarterly_validate.py
run_kospi_mcap_quarterly_paper.bat
```

`src`는 순수 계산과 전략 전용 저장 계약을 소유한다. `tools`는 CLI 진입점만 제공한다. 배치는 수동
실행 전용으로 만들며 스케줄러에는 등록하지 않는다. 상세 파일 수는 구현 시 기존 저장소 패턴에 맞춰
최소화하되, 위 소유 경계는 바꾸지 않는다.

## 5. 구현 순서

번호는 C8~C11의 문서 번호가 아니라 실제 의존성 순서다. 후속 단계는 선행 단계의 검증 PASS 전에는
시작하지 않는다.

### M0. 공통 계약, 네임스페이스, ID와 스키마

목표:

- C1~C12의 기계 판독 가능한 설정과 계약 해시를 만든다.
- `strategy_id`, `contract_version`, `round_id`, `run_id`, `as_of`, `event_id`, `intent_id`,
  `order_id`, `fill_id` 생성 규칙을 먼저 고정한다.
- 일별 `run_id/as_of`와 분기 간 `event_id/intent_id`의 수명을 분리한다.
- C11의 전략 한정 `strategy_D`가 기존 전역 D 규칙을 변경하지 않도록 문서와 코드 경계를 명시한다.
- 원자적 파일 쓰기와 검증 성공 후에만 `*_latest`를 교체하는 발행 규칙을 고정한다.

산출물:

- 전략 설정, JSON 스키마, ID 규약, manifest 규약
- 전략 한정 D 규칙과 기존 O6 전역 D 비변경을 확인하는 계약 테스트
- 신규 네임스페이스 쓰기 경계 검사

STOP:

- 같은 이벤트가 다른 ID를 만들거나 서로 다른 이벤트가 같은 ID를 만드는 경우
- 전략 파일이 기존 O6 산출물 경로를 쓰기 대상으로 참조하는 경우
- C11 전략 한정 D와 저장소 전역 D의 적용 범위를 구분하지 못한 경우

### M1. C8 시점별 데이터 원천과 불변 스냅샷

목표:

- 실제 데이터 writer와 공식 원천을 먼저 확인한다.
- 선정일 당시의 KRX 시장, 보통주 유형, 거래정지·관리 상태, 상장·상장폐지·합병 등 기업행위,
  종가, 시가총액을 시점 정합적으로 수집한다.
- 종목명 문자열 추정으로 보통주를 확정하지 않는다.
- 원천 파일, 조회 조건, 시간, 행 수, 해시를 manifest에 남긴다.

산출물:

- 날짜별 원천 스냅샷과 정규화 스냅샷
- 기업행위 이벤트 테이블
- 중복, 누락, 날짜 역전, 미래정보 사용 여부를 포함한 품질 보고서

STOP:

- 공식 보통주 구분 또는 당시 상태를 증명할 수 없는 경우
- `selection_as_of` 이후 정보를 선정 입력으로 사용한 경우
- `(as_of, symbol)` 중복, 필수 값 누락, 원천 해시 누락이 있는 경우
- 적격 종목이 100개 미만인 경우

현재 선행 결함:

- `_cache/krx_point_in_time_listing_history_latest.csv`는 기존 최신 실행에서 유효 행이 없었다.
- 기존 종목명 기반 `_infer_security_type`은 C8 증거로 인정하지 않는다.
- 현재 스냅샷 성격의 시가총액 자료는 역사 시점별 선정 증거로 인정하지 않는다.

### M2. C2~C7 순수 목표 포트폴리오 계산

목표:

- KRX 거래일 달력으로 선정일과 실행일을 계산한다.
- 적격 유니버스에서 시가총액 상위 100개를 결정하고 시총가중한다.
- 20% 상한과 반복 비례 재배분, KOSPI200 MA200 노출, 정수 주식화를 순수 함수로 구현한다.
- 선정 전 자격 판정과 선정 후 기업행위 처리를 별도 단계로 둔다.

산출물:

- 선정 스냅샷, 가중치 계산 단계, 목표 수량, 잔여 현금, 예외 사유
- 입력 해시와 계약 해시가 연결된 목표 manifest
- 고정 fixture와 경계값 테스트

필수 불변식:

- 종목 수는 계약상 100개이며 누락 시 조용히 축소하지 않는다.
- 비중 합계는 적용 노출과 일치하고 종목별 비중은 20%를 넘지 않는다.
- MA200 판정은 당일 장중 노출을 소급 변경하지 않는다.
- 정수화 후 음수 현금, 음수 수량, 목표금액 초과, 10% 예외 위반이 없어야 한다.

STOP:

- 선정일·실행일 달력 불일치
- 데이터 `as_of`와 선정일 불일치
- 상한 재배분이 수렴하지 않거나 정수화 불변식 위반
- KOSPI200 200거래일 이력 또는 기준 종가가 불완전한 경우

### M3. C9 격리 가상 주문과 체결 상태기계

목표:

- 목표 수량과 현재 전략 보유량의 차이만 주문 의도로 만든다.
- 09:05~15:20와 호가 나이 5초 이하를 강제한다.
- 시장성 지정가, DAY, 부분체결, 15:20 취소, 다음 세션 1회 재시도를 상태기계로 구현한다.
- 브로커 주문 모듈을 import하거나 호출하지 않는다.

산출물:

- 전략 전용 intent, orders, fills, cancel, retry 이벤트
- 호가 시각과 사용 가격이 남은 실행 manifest
- 중복 실행, 부분체결, 만료, 재시도 fixture

STOP:

- 신선한 호가가 없거나 나이가 5초를 초과한 경우
- 주문량 초과 체결, 고아 체결, side 불일치, 중복 ID가 있는 경우
- 15:20 이후 신규 주문 또는 1회를 넘는 재시도가 발생한 경우
- 브로커 전송 경로 접근이 감지된 경우

장중 의존 검증:

- 실제 5초 이하 호가 수신과 100종목 커버리지는 KRX 장중에만 판정한다.
- 장외에는 기록된 fixture로 상태기계와 FAIL-CLOSED만 검증하며 운영 반영 PASS로 승격하지 않는다.

### M4. C10 전략 전용 원장, NAV와 영구 종료 래치

목표:

- 최초 현금 1억 원, 보유 수량, 체결 금액, 비용, 미체결, 평가가격을 전략 원장으로 연결한다.
- 외부 입출금과 전략 손익을 분리한다.
- 완전한 NAV가 7,500만 원 이하가 된 경우 영구 종료 래치를 원자적으로 기록한다.
- 종료 뒤 신규 BUY를 차단하되 원장 정합성과 필요한 청산 처리는 별도 상태로 유지한다.

산출물:

- 현금·포지션·거래·평가·NAV 원장
- `termination_latch` 이벤트와 재시작 거부 증거
- 자산 보존식, 반복 실행 멱등성, 누락 평가가격 fixture

STOP:

- 현금이나 수량이 음수가 되거나 원장 보존식이 맞지 않는 경우
- 평가가격·체결·기업행위가 불완전한데 완전 NAV로 판정한 경우
- 외부 입출금을 수익률로 섞은 경우
- 종료 래치 뒤 신규 BUY가 생성되거나 자동 해제된 경우

### M5. C11 전 구간 계보 검증과 발행

목표:

- `data -> universe -> target -> intent -> order -> fill -> ledger -> stats`를 같은 계약 해시와 ID 계보로
  연결한다.
- 전략 전용 `strategy_D`를 최신 실제 BUY 또는 SELL 체결일로 계산한다.
- 일별 `run_id/as_of`, 분기 이벤트, 교차일 재시도 계보를 모두 추적한다.
- 검증 실패 시 신규 `latest`를 발행하지 않고 직전 유효본을 보존한다.

산출물:

- E2E manifest, 검증 보고서, 전략 전용 latest 포인터
- 고의 결함을 넣은 FAIL-CLOSED 회귀 fixture
- 기존 O6 산출물 비변경 해시 비교

강제 STOP:

- 전략 `orders_exec`에 해당하는 주문 산출물이 없음
- `exec_date != strategy_D`인 당일 체결 체인
- 같은 체인의 `as_of/run_id/contract_version` 불일치
- paper와 broker 데이터 혼용
- 고아 주문·체결, side·수량·가격 불일치
- 원장 또는 통계 반영 누락
- 기존 O6 경로의 내용 또는 의미 변경

### M6. 과거 재현과 회귀 검증

목표:

- 동결된 코드·데이터·계약으로 같은 입력이 같은 목표와 원장을 만드는지 확인한다.
- 역사 구간은 구현 재현과 결함 발견에만 사용한다.
- 기존 O6 관련 회귀와 파일 비변경을 확인한다.

판정:

- 과거 결과의 최고 자격은 `REPRODUCIBLE_RESEARCH_ONLY`다.
- 이 단계 통과를 성과 OOS, 운영 가능, 실거래 준비 완료로 표현하지 않는다.

### M7. C12 미래 라운드 등록

선행 조건:

- M0~M6의 적용 가능한 6개 검증 축이 모두 PASS
- 데이터·코드·설정·계약 해시 고정
- 신규 전략 전용 round가 기존 일반 R&D와 분리됨

작업:

- 새 round를 등록하고 동결한다.
- 시작일, 측정 창, KOSPI200 벤치마크, CAGR, 위험조정 수익, 커버리지, 분기별 체결금액,
  외부 자금흐름, C10 종료 상태, MDE/MES 계산법을 사전 등록한다.
- 일반 R&D `SUSPENDED`는 유지한다.

현재 상태:

- `MISSING_C12_REGISTERED_FORWARD_ROUND`
- 이 ExecPlan 작성 단계에서는 round를 만들거나 동결하지 않는다.

### M8. 수동 격리 paper 런타임 검증

선행 조건:

- M7 등록 완료
- 장중 데이터 원천과 가상 체결 경로 준비
- 실행 전 백업과 dry-run PASS

작업:

- 수동 배치로만 최초 paper 라운드를 기동한다.
- 장중 주문·부분체결·취소·재시도와 장후 원장·통계를 실제 산출물로 검증한다.
- 첫 성공 이벤트는 구현 E2E 증거이며 성과 검증 증거는 아니다.

제외:

- 자동 스케줄 등록, RootB 표시, 브로커 연결은 각각 별도 계획과 사용자 승인 없이는 진행하지 않는다.

## 6. 검증 계획

각 구현 단계는 다음 3단계와 6개 축을 모두 보고한다.

### 3단계

1. 문법: Python compile, JSON/CSV 스키마 파싱, 배치 구문 점검
2. 실행: 공식 CLI 또는 수동 배치의 dry-run, fixture replay, 실패 주입
3. 결과물: 실제 파일 내용, manifest 해시, ID 계보, 최신 포인터와 로그 확인

### 6개 축

| 축 | 최소 PASS 근거 |
|---|---|
| 기능 | 계약 fixture의 후보·비중·수량·주문·체결·원장 결과 일치 |
| 정합성 | 날짜, ID, side, 수량, 가격, 현금, NAV, 해시 불변식 PASS |
| 운영 반영 | 수동 공식 경로가 신규 전략 산출물을 실제 생성하고 검증기가 읽음 |
| 정책 | C1~C12 고정 fixture와 금지 경계 PASS |
| FAIL-CLOSED | 누락·불일치·오래된 호가·중복·혼용 고의 주입 시 발행 및 BUY 차단 |
| 회귀 | 기존 O6 테스트 PASS와 대상 운영 산출물 비변경 증거 |

장외 실행만으로 C9 장중 운영 반영을 PASS 처리하지 않는다. 실제 미래 성과가 쌓이기 전에는 C12 성과
판정도 `NA`다.

## 7. 구현 단계별 백업과 변경 통제

- 기존 파일을 수정하기 전 `E:\1_Data\backup\YYYYMMDD_작업명\YYYYMMDD_HHMMSS\`에 원본을 백업한다.
- 신규 파일은 신규임을 보고하고, 기존 계약·가드·O6 파일의 변경과 분리한다.
- 한 단계의 코드, 테스트, 산출물 계약만 수정하고 다음 단계로 넘어가기 전에 검증한다.
- 임시 fixture는 운영 산출물과 다른 경로에 두며, 운영 최신 파일을 테스트 입력으로 덮어쓰지 않는다.
- 스키마 변경 시 구버전 행 처리 규칙과 migration 또는 명시적 비호환 판정을 함께 둔다.

## 8. 롤백

- 신규 전략 실행을 중지하고 마지막 유효 산출물과 실패 산출물을 감사용으로 보존한다.
- 수정한 기존 문서는 단계별 백업본으로 복원한다.
- 신규 네임스페이스는 자동 삭제하지 않는다. 삭제가 필요하면 별도 승인과 dry-run을 거친다.
- 기존 O6에는 이 전략의 상태나 종료 래치를 전파하지 않으므로 O6 롤백은 발생하지 않아야 한다.
- `latest` 발행 전 실패한 경우 직전 유효 포인터를 유지한다.

## 9. 현재 결함과 선행 위험

1. C8 공식 시점별 유니버스·증권종류·상태·기업행위 원천이 아직 확정되지 않았다.
2. 기존 시점별 상장 이력 최신 산출물은 유효 행이 없고 종목명 추정은 C8을 충족하지 않는다.
3. C9의 100종목 신선 호가와 부분체결 상태기계가 없다.
4. C10의 전략 전용 NAV 원장과 영구 종료 래치가 없다.
5. C11의 전략 전용 D, 공통 계보, 원자적 발행 검증기가 없다.
6. C12 신규 전략 round와 미래 paper 표본이 없다.
7. 기존 일반 R&D preflight FAIL은 신규 전략 등록과 분리해 다뤄야 하며, 신규 전략 증거로 재사용하지 않는다.

## 10. 첫 구현 작업

첫 구현 범위는 M0와 M1의 read-only 선행 감사 및 계약 골격으로 제한한다.

1. C8 관련 실제 writer, 원천, 스키마, 날짜 범위를 추적한다.
2. 공식 보통주·상태·기업행위·시가총액의 시점별 증명 가능 여부를 PASS/FAIL로 판정한다.
3. 기존 O6를 참조하지 않는 신규 네임스페이스와 ID·manifest 스키마를 작성한다.
4. 데이터 원천이 부족하면 후보 계산으로 넘어가지 않고 `FAIL_C8_UNIVERSE_SOURCE` 산출물을 남긴다.
5. M0/M1의 문법·실행·결과물 검증 뒤에만 M2 구현을 시작한다.

첫 구현 결과 보고 기준:

- 수정 파일과 백업 경로
- 확정된 실제 writer·원천·날짜 범위
- C8 품질 핵심 값과 증거 경로
- M0/M1의 6개 축 PASS/FAIL/NA
- 전체로직 적용 여부와 M2 착수 가능 여부

## 11. 완료 판정

- ExecPlan 완료: 이 문서와 RootA PLANS 기록이 저장되고 문서 검증이 PASS한 상태
- 구현 완료: M0~M6의 적용 가능한 6개 축이 모두 PASS한 상태
- paper 기동 완료: M7 등록 뒤 M8에서 실제 장중·장후 E2E 증거가 확인된 상태
- 성과 검토 가능: 계약 동결 이후 1년 최소 관측 조건을 모두 충족한 상태
- 3년 목표 판정: 3년과 분기 재구성 12회 뒤 C12 기준으로 별도 판정

현재는 첫 번째 문서 단계만 대상이며, 신규 전략은 전체로직 미적용이다.

## 12. 2026-09-15 M0·M1 실행 기록

### 구현된 것

- `paper/strategies/kospi_mcap_quarterly_v1/config/strategy_contract_v1.json`
  - C1~C12의 전략 ID, 격리 paper, 자본, N=100, 20% 상한, MA200, C9 실행, C10 종료,
    C11 전략 D를 기계 판독 가능한 계약으로 고정했다.
- `paper/strategies/kospi_mcap_quarterly_v1/src/contracts.py`
  - 결정적 run/event/intent/order/fill ID, 전략 전용 BUY·SELL 체결일 D, 전략 경로 쓰기 가드,
    원자적 JSON 쓰기, 검증 PASS 전 data latest 발행 거부를 구현했다.
- `paper/strategies/kospi_mcap_quarterly_v1/src/c8_source_audit.py`
  - 기존 C8 관련 원천·writer·해시·열·행·기준일을 read-only로 감사하고 전략 전용 상태 보고서만
    발행하도록 구현했다.
- `tools/kospi_mcap_quarterly_data.py`
  - 기본 C8 실패는 종료코드 2로 반환하며, 검증 시에는 예상 실패를 명시적으로 assertion할 수 있는
    CLI를 구현했다.
- `tests/test_kospi_mcap_quarterly_m0_m1.py`
  - ID 멱등성, 재시도 한도, 전략 D 범위, O6 쓰기 차단, C8 누락 FAIL-CLOSED, 유효 N=100 양성
    fixture, 수동 상태 원천의 부분 증거 처리, 계약 상수를 검증한다.

### 실제 실행 증거

- 집중 테스트: 7개 PASS.
- 실제 감사 산출물:
  `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_universe_source_status_latest.json`
- 감사 실행: `PASS`; C8 데이터 준비도: `FAIL_C8_UNIVERSE_SOURCE`; `m2_allowed=false`.
- 후보 계산, 목표 계산, 주문 생성: 모두 `false`.
- 기본 CLI 실패 종료코드: 2.
- 최악 조건 검증: 분리된 현재 원천과 기준일 불일치에서 `PASS_FAIL_CLOSED`.

### 확인된 C8 핵심 값

| 조건 | 출처 | 확인 값 | 판정 |
|---|---|---|---|
| 시점별 상장 이력 | `_cache/krx_point_in_time_listing_history_latest.csv` | 0행 | FAIL |
| 수동 상장·상폐 정적 자료 | `_cache/krx_population_static_input_manual_v1.csv` | 3,177행, KOSPI 1,025행, COMMON 2,781행, 상폐 415행 | PARTIAL |
| 현재 시총 | `_cache/pykrx_fundamental_latest.csv` | 2,873행, `market_cap_as_of=20260911` | PARTIAL |
| 최신 일봉 | `krx_daily_archive/krx_daily_20260914_20260914_clean.parquet` | 2,597행, `date=20260914`, market UNKNOWN 1,367행 | PARTIAL |
| 상태·사건 | 수동 KRX 원본 7개 | 현재 상태·정지·상폐·합병 단서는 있으나 행별 기준일과 통합 이력 없음 | PARTIAL |
| 종가·시총 시점 정합 | 위 두 파일 | 20260914 대 20260911 | FAIL |

최신 일봉의 실제 writer 체인은
`run_paper_daily.bat -> krx_update_clean_incremental.py -> tools/sync_krx_archive_from_sources.py`로
확인했다. `market_master`와 시총 갱신은
`run_decision_sources_daily.bat -> tools/build_market_master.py -> tools/refresh_market_cap_from_panel.py`
호출점을 확인했다.

### 6개 검증 축

| 축 | 판정 | 증거와 한계 |
|---|---|---|
| 기능 | PASS | 실패·양성 C8 fixture, 수동 상태 원천 부분 증거와 계약 함수 7개 테스트 PASS |
| 정합성 | PASS | 계약 JSON 파싱, ID·D·필수열·기준일 검사 PASS |
| 운영 반영 | NA | 전략 paper 실행·주문·체결은 이번 범위가 아니며 생성하지 않음 |
| 정책 | PASS | 격리 paper, broker 금지, N=100, O6 전역 D 비변경 고정 |
| FAIL-CLOSED | PASS | 통합 원천 누락 시 `m2_allowed=false`, 후보·목표·주문 false, 기본 rc=2 |
| 회귀 | PASS | 감사 전후 기존 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256 동일 |

### 현재 판정과 다음 조건

- M0 계약 골격: 구현·집중 검증 PASS.
- M1 C8 감사기: 구현·실제 실행 PASS.
- M1 C8 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

다음 작업은 공식 시점별 KRX 보통주·시장·거래·관리·상폐절차·합병 상태와 같은 기준일의 종가·시총을
한 행 계보로 제공하는 C8 통합 원천을 확보·정규화하는 것이다. 종목명 추정이나 현재 스냅샷 결합으로
이 FAIL을 우회하지 않는다.

## 13. 2026-09-15 M1 수동 KRX 필드 감사 보완

### 변경 및 실행

- 백업: `backup/20260915_c8_manual_field_audit/20260915_062603/`.
- `src/c8_source_audit.py`가 수동 KRX CSV를 헤더 기준으로 상장 마스터, 상태 플래그, 거래정지 사건,
  상폐 이력, 가격·시총 스냅샷으로 분류하고 해시·열·행·파일명 날짜 힌트를 기록하도록 보완했다.
- 파일명의 날짜는 `FILENAME_ONLY_UNVERIFIED_ASOF`로만 기록하며 행별 공식 기준일로 승격하지 않는다.
- 감사 보고서에 `manual_raw_source_inventory` 17개와 `c8_field_matrix` 7개 필드를 추가했다.
- 수동 상태 원천이 발견되어도 통합 C8 원천이 아니면 M2가 열리지 않는 회귀 테스트를 추가했다.

### 실제 원본 확인 값

| 분류 | 파일 수 | 확인 값 | 판정 |
|---|---:|---|---|
| 상장 마스터 | 2 | 시장·증권종류·상장일 열 존재, 날짜는 파일명 힌트 | PARTIAL |
| 상태 플래그 | 1 | 거래정지 124, 정리매매 1, 관리 103, 투자주의환기 90, 투자경고 13, 투자위험 0 | PARTIAL |
| 거래정지 사건 | 1 | 126행, 2021/03/26~2026/07/15, 합병 사유 5행 | PARTIAL |
| 상폐 이력 | 5 | 총 1,297행, 2017/07/18~2026/07/15, 합병 사유 131행 | PARTIAL |
| 가격·시총 스냅샷 | 8 | 종가·시총 열 존재, 행별 기준일 없음 | PARTIAL |
| 행별 기준일·통합 계보 | 0 | 하나의 검증된 C8 행 계보 없음 | FAIL |

### 검증과 판정

- 문법 검증 PASS, 집중 테스트 7개 PASS, 실제 감사 CLI 예상 판정 실행 PASS.
- 결과물: `reports/c8_universe_source_status_20260915063137.json` 및
  `reports/c8_universe_source_status_latest.json`.
- 공식 문자열 dry-run 배치는 기존 `2_Logs/mojibake_text_scan_latest.json` 쓰기 권한 오류로 rc=1이었다.
  같은 스캐너의 대상 파일 검사에서 코드·테스트·ExecPlan은 0건, PLANS는 백업본과 동일한 기존
  비복구형 6건이었으며 이번 변경으로 추가된 깨짐은 0건이다.
- 감사 실행은 PASS지만 데이터 준비도는 `FAIL_C8_UNIVERSE_SOURCE`, `m2_allowed=false`다.
- 후보·목표·주문은 계산하지 않았고 기존 `paper/fills.csv`, `paper/trades_calc.csv` 해시는 유지됐다.
- 결론: C8 상태 데이터가 전혀 없는 것은 아니지만 시점별 통합 원천은 없다. M1은 보완됐고 M2는 STOP,
  전체로직은 미적용이다.

## 14. 2026-09-15 C8 canonical 입력 계약과 manifest 검증

### 변경 경계

- 백업: `backup/20260915_c8_canonical_contract/20260915_063815/`.
- 변경 분류: 정책 변경이 아닌 C8 입력 계약 강화와 FAIL-CLOSED 검증이다.
- 기존 O6, 승인된 전략 정책값, 후보 M2, 주문·체결·원장·브로커는 변경하지 않았다.

### 구현된 계약

- `config/c8_canonical_source_contract_v1.json`을 추가했다.
- canonical 기본키는 `(as_of_date, code)`이며 종목명 조인은 금지했다.
- 행의 `source`는 저장소 상대경로 manifest, `source_sha256`은 manifest 실제 바이트 해시로 고정했다.
- 필수 manifest 역할을 `listing_security_as_of`, `trade_management_status_as_of`,
  `corporate_actions_as_of`, `close_market_cap_as_of` 네 개로 고정했다.
- 허용 날짜 권한은 공식 행 필드, 공식 조회 파라미터, 공식 불변 데이터셋 날짜뿐이다.
  `FILENAME_ONLY_UNVERIFIED_ASOF`, 파일 mtime, 후행 스냅샷 추정은 금지했다.
- manifest 구성 파일은 존재, 양수 행 수, PASS 상태, canonical과 같은 기준일, 실제 SHA-256 일치를
  모두 충족해야 한다.
- 현재 재사용 가능한 수동 필드 5분류, 추가 수집 필드 5개, M2 해제 조건 7개를 계약에 고정했다.

### 검증 결과

- `src/c8_source_audit.py`가 계약 자체와 manifest·구성 파일을 실제 검증하도록 연결됐다.
- 양성 fixture는 네 원천 파일과 manifest의 실제 해시가 모두 맞을 때만 N=100 C8을 통과한다.
- 파일명 날짜 권한 fixture는 `SOURCE_ROLE_DATE_AUTHORITY_INVALID`와
  `SOURCE_MANIFEST_INVALID`로 차단한다.
- manifest 아래 원천 파일을 사후 변조한 fixture는 `SOURCE_COMPONENT_HASH_MISMATCH`로 차단한다.
- 첫 테스트에서 fixture 디렉터리 재생성 결함 2건이 발생했고 `exist_ok=True` 한 줄 수정 후 재실행했다.
- 문법 검증 PASS, 집중 테스트 9개 PASS.
- 대상 파일 문자열 검사에서 신규 계약·코드·테스트·ExecPlan은 0건, PLANS의 6건은 백업본과
  같은 기존 비복구형 기록으로 확인됐다.
- 실제 감사 산출물: `reports/c8_universe_source_status_20260915064236.json` 및
  `reports/c8_universe_source_status_latest.json`.
- 계약 상태는 PASS, 계약 SHA-256은
  `84269edd014e4a0856b40e8b45d593f9a3c77c8efe0bb9a18e91c88cb0bd1753`다.
- 실제 canonical은 아직 없으므로 `CANONICAL_C8_SOURCE_MISSING`, 최종
  `FAIL_C8_UNIVERSE_SOURCE`, `m2_allowed=false`다.
- 후보·목표·주문 계산은 모두 false이며 O6 fills·trades 해시는 유지됐다.

### 단계 판정

- C8 계약 명세와 검증기: 구현·집중 검증 PASS.
- C8 실제 데이터 준비도: FAIL.
- 운영 반영: NA.
- M2 착수: STOP.
- 전체로직: 미적용.

## 15. 2026-09-15 C8 source builder 구현

### 변경 경계

- 백업: `backup/20260915_c8_source_builder/20260915_064700/`.
- 변경 분류: M1 데이터 writer 구현이며 매매 정책 변경이 아니다.
- 기존 O6, M2 후보 계산, 주문·체결·원장·브로커는 변경하지 않았다.

### 구현된 것

- C8 계약에 `builder_request_contract`를 추가했다.
- writer 입력은 전략 전용 `data/inbox/components/` 아래의 정규화 CSV 네 개로 제한했다.
- 각 입력은 역할, 저장소 상대경로, 요청 해시, 공식 원천 ID, 기준일, 허용 날짜 권한을 가져야 한다.
- 역할별 필수 열과 네 입력의 정확한 코드 집합 일치를 계약으로 고정했다.
- `src/c8_source_builder.py`를 추가해 다음 순서를 구현했다.
  1. request·역할·열·코드·기준일·날짜 권한·입력 SHA-256 검증.
  2. 검증된 원본을 content hash가 포함된 불변 snapshot으로 복사.
  3. 원본·snapshot 해시와 공식 기준일 권한을 포함한 manifest 생성.
  4. 네 역할을 `code`로만 결합해 versioned canonical 생성.
  5. 기존 C8 감사기가 PASS한 경우에만 canonical latest 발행.
- `tools/build_kospi_mcap_quarterly_c8_source.py`에 공식 CLI와 예상 실패 assertion을 추가했다.
- `tests/test_kospi_mcap_quarterly_c8_builder.py`에 정상 4원천 발행, 파일명 날짜 차단,
  코드 집합 불일치 차단을 추가했다.

### 검증 결과

- 문법 검증 PASS.
- 기존 M0·M1과 builder 집중 테스트 총 12개 PASS.
- 양성 fixture는 100개 canonical과 manifest를 생성하고 기존 C8 감사 `C8_UNIVERSE_SOURCE_READY`를
  통과했다.
- 파일명 날짜 권한과 99/100 코드 집합 불일치는 snapshot·manifest·canonical latest 발행 전에 차단됐다.
- 실제 CLI 예상 실패 실행 rc=0, 기본 실행은 `C8_BUILD_REQUEST_MISSING`으로 rc=2였다.
- 실제 상태 보고서:
  `reports/c8_source_build_status_20260915065402.json`,
  `reports/c8_source_build_status_latest.json`.
- 실제 경로에는 request, snapshot, manifest, canonical latest가 모두 없고 `published=false`,
  `m2_allowed=false`, 후보·목표·주문 계산 false다.
- C8 감사 재실행 결과는 `CANONICAL_C8_SOURCE_MISSING`, `FAIL_C8_UNIVERSE_SOURCE`다.
- 현재 C8 계약 SHA-256은
  `f9a1e1159363d8f5634fc567db801535511b46878de2b4e88717171d28fdce2a`다.
- O6 fills·trades 해시는 유지됐고 `orders_exec.xlsx`는 생성하지 않았다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 공식 입력이 없다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

## 16. 2026-09-15 C8 수동 원천 adapter capability 감사

### 변경 경계

- 백업: `backup/20260915_c8_adapter_capability/20260915_070000/`.
- 변경 분류: M1 read-only 원천 적합성 감사이며 매매 정책 변경이 아니다.
- 기존 O6, M2 후보 계산, 주문·체결·원장·브로커는 변경하지 않았다.

### 구현된 것

- `src/c8_adapter_audit.py`를 추가해 현재 KRX 수동 원천을 네 필수 역할별로 구조, 의미,
  코드 범위, 공식 기준일 권한으로 분리 판정한다.
- `tools/audit_kospi_mcap_quarterly_c8_adapters.py`에 실제 감사 CLI와 예상 판정 assertion을 추가했다.
- `tests/test_kospi_mcap_quarterly_c8_adapter_audit.py`에 현재형 원천 차단과 원천 부재 차단을 추가했다.
- 감사기는 역할이 네 조건을 모두 PASS한 경우에만 `adapter_ready_roles`에 포함하며, 이번 단계에서는
  adapter, build request, canonical을 생성하지 않는다.

### 실제 원천 판정

| 역할 | 구조 | 의미 | 범위 | 공식 기준일 | 결과 |
|---|---|---|---|---|---|
| 상장·증권종류 | PASS | PARTIAL | PASS | FAIL | BLOCKED |
| 거래·관리·상폐절차 | PASS | PARTIAL | PASS | FAIL | BLOCKED |
| 기업행위 | PARTIAL | FAIL | FAIL | FAIL | BLOCKED |
| 종가·시가총액 | PASS | PASS | FAIL | FAIL | BLOCKED |

- 상장 마스터와 상태 파일의 코드 집합은 2,872개로 일치했고 KOSPI 보통주는 834개였다.
- 선택된 가격·시총 파일은 KOSPI 보통주 834개 중 832개를 포함했고 `031440`, `057050`이 없었다.
- Zone.Identifier에서 `data.krx.co.kr` 다운로드 URL을 확인해 KRX 출처는 확인했지만, 다운로드 조회일과
  원본 해시를 연결하는 공식 `selection_as_of` 권한은 확인하지 못했다.
- 동일 SHA-256 `d9ffffa6bb0557dbede527d1e2549c867af798b7fc4a0ede394803bb55d4afbf` 바이트가
  `krx_price_20260821.csv`와 `data_1628_20260822.csv`로 존재했다. 파일명 날짜를 공식 기준일로
  승격하지 않는 최악 조건은 `PASS_FAIL_CLOSED`였다.

### 검증 결과

- 최초 `py_compile`은 `tests/__pycache__` 쓰기 권한으로 중단됐고, 바이트코드를 쓰지 않는 `compile()`
  대체 문법 검증에서 신규 3개 파일이 PASS했다. 생성된 pyc 1개는 경로 확인 후 제거했다.
- 기존 M0·M1·builder와 신규 감사 테스트 총 14개 PASS.
- 공식 문자열 dry-run은 고정 latest 쓰기 권한으로 rc=1이었지만 지정 출력은 생성됐다. 신규 코드·테스트·
  보고서·ExecPlan은 0건이고, PLANS의 비복구형 6건은 백업과 동일해 신규 깨짐은 0건이다.
- 실제 CLI 예상 판정 실행 rc=0, 결과는 `C8_ADAPTERS_BLOCKED`, ready 0개, blocked 4개다.
- 결과물: `reports/c8_adapter_capability_status_20260915070358.json` 및
  `reports/c8_adapter_capability_status_latest.json`.
- `build_request_possible=false`, `adapter_files_generated=false`, `m2_allowed=false`, 후보·목표·주문
  계산 false다.
- 기존 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 유지됐고 `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 공식 기준일 입력과 adapter가 없다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

## 17. 2026-09-15 C8 공식 원천 획득 증거 계약

### 변경 경계

- 백업: `backup/20260915_c8_acquisition_evidence/20260915_071208/`.
- 변경 분류: M1 원천 provenance 계약 구현이며 매매 정책 변경이 아니다.
- 기존 O6, 원천 adapter, build request, canonical, M2, 주문·체결·원장·브로커는 변경하지 않았다.

### 공식 경로 확인

- 2026-09-15 KRX 공식 OPEN API 안내에서 인증키 신청과 서비스별 이용신청이 선행 조건임을 확인했다.
- 공식 서비스 목록에는 유가증권 일별매매정보와 유가증권 종목기본정보가 2010년 이후 자료로
  제공된다고 표시돼 있다.
- 같은 공개 서비스 목록에서는 거래정지·관리·상폐절차·합병 상태를 네 C8 역할에 맞게 함께 제공하는
  API를 확인하지 못했다. 따라서 OPEN API만으로 네 역할 충족을 추정하지 않는다.
- 현재 날짜는 2026-09-15이고 첫 미래 paper 선정에 필요한 2026년 3분기 마지막 KRX 거래일은 아직
  도래하지 않았다. 실제 해당 기준일 원천 획득은 현재 `NA`다.

### 구현된 것

- `config/c8_acquisition_evidence_contract_v1.json`에 네 역할, 공식 도메인, 허용 날짜 파라미터,
  원본 경로, 비밀정보 금지, 획득 시간창과 발행 규칙을 고정했다.
- `src/c8_acquisition_evidence.py`가 다음 조건을 모두 검증한 경우에만 불변 획득 증거를 발행한다.
  1. 네 역할이 정확히 한 개씩 있고 모두 같은 `selection_as_of`를 사용한다.
  2. KRX 공식 HTTPS 도메인과 원천 ID, 조회 날짜 파라미터가 기록된다.
  3. 조회 날짜, 역할별 `as_of_date`, 전체 `selection_as_of`가 일치한다.
  4. 원본 파일이 전략 raw inbox에 있고 요청 SHA-256과 실제 바이트 해시가 일치한다.
  5. 원본 mtime과 retrieval 시간이 선언된 획득 시간창 안에 있다.
  6. 인증키·Authorization·Cookie·OTP·password·secret·token을 기록하지 않는다.
- 증거에는 비밀값이 제거된 실제 조회 파라미터와 공식 페이지·다운로드 URL, 원본 해시·크기·mtime을
  직접 포함해 증거 파일만으로 조회조건을 재현할 수 있게 했다.
- `tools/register_kospi_mcap_quarterly_c8_acquisition.py`에 실제 CLI와 예상 판정 assertion을 추가했다.
- 획득 증거 PASS도 adapter, build request, canonical, M2를 자동으로 열지 않도록 분리했다.

### 검증 결과

- Python 3개 문법과 계약 JSON 파싱 PASS.
- 기존 M0·M1·builder·adapter 감사 포함 총 17개 테스트 PASS.
- 양성 fixture는 네 원천 획득 증거만 발행하고 adapter·canonical·M2는 생성하지 않았다.
- 원본 변조는 `RAW_FILE_HASH_MISMATCH`, 인증키 기록과 미래 기준일은
  `SECRET_MATERIAL_PERSISTED`, `SELECTION_ASOF_IN_FUTURE`로 증거 발행 전에 차단됐다.
- 실제 CLI 예상 실패 실행 rc=0, 현재 판정은 `ACQUISITION_REQUEST_MISSING`이다.
- 결과물: `reports/c8_acquisition_capture_status_20260915072200.json` 및
  `reports/c8_acquisition_capture_status_latest.json`.
- 실제 획득 증거, adapter, build request, canonical은 없고 `m2_allowed=false`다.
- 기존 O6 fills·trades SHA-256은 유지됐고 `orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 공식 선정일 원천과 실제 획득 요청이 없다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

## 18. 2026-09-15 C8 공식 원천 획득 세션 절차

### 변경 경계

- 백업: `backup/20260915_c8_acquisition_session/20260915_072551/`.
- 변경 분류: M1 원천 획득 절차 구현이며 매매 정책 변경이 아니다.
- 기존 O6, adapter, build request, canonical, M2, 주문·체결·원장·브로커는 변경하지 않았다.

### 구현된 것

- `config/c8_acquisition_descriptor_template_v1.json`에 네 역할의 로컬 원본 경로, 공식 원천 ID,
  공식 페이지·다운로드 URL, 조회 파라미터, 날짜 파라미터와 기준일 입력 틀을 고정했다.
- `src/c8_acquisition_session.py`에 다음 순서를 구현했다.
  1. `selection_as_of`와 timezone이 있는 시작 시각을 기록해 ACTIVE 세션을 먼저 연다.
  2. 세션 시작 이후 생성된 네 역할 원본만 descriptor로 받는다.
  3. 역할·공식 도메인·원천 ID·조회일·기준일·비밀키 부재를 raw 복사 전에 검증한다.
  4. 원본 바이트를 역할·기준일·세션·SHA-256 이름의 전략 전용 불변 raw snapshot으로 복사한다.
  5. versioned request를 기존 획득 증거 검증기에 전달하고 PASS 뒤에만 request latest와 SEALED
     세션을 발행한다.
- 세션 이전 파일은 `SOURCE_PREDATES_CAPTURE_SESSION`, 미래 기준일은 `SELECTION_ASOF_IN_FUTURE`,
  역할 중복·누락은 `DESCRIPTOR_SOURCE_ROLES_MISSING_DUPLICATE_OR_EXTRA`, 인증정보는
  `SECRET_MATERIAL_PRESENT`로 발행 전에 차단한다.
- `tools/prepare_kospi_mcap_quarterly_c8_acquisition.py`에 `start`, `finalize`, `status` CLI를 추가했다.
- 세션 봉인 PASS도 adapter, build request, canonical, M2를 자동으로 열지 않는다.

### 검증 결과

- 신규 Python 3개는 바이트코드를 쓰지 않는 `compile()` 문법 검사 PASS, template JSON 파싱 PASS.
- 최초 `py_compile`은 기존 `tests/__pycache__` 쓰기 권한으로 중단됐으며 소스 오류 판정에 사용하지
  않았다.
- 신규 경계 테스트와 기존 M0·M1 전체 집중 회귀 총 21개 PASS.
- 양성 fixture는 세션 시작 뒤 생성한 정확한 네 원천만 raw·request·evidence로 봉인했다.
- 세션 이전 파일, 미래 기준일, 역할 중복, 인증키 포함 fixture는 raw·request·evidence 발행 전에
  각각 차단됐다.
- 실제 RootA CLI status 예상 실패 assertion은 rc=0이고 판정은
  `ACQUISITION_SESSION_MISSING`, `FAIL_C8_ACQUISITION_SESSION`이다.
- 실제 RootA에는 session, request, evidence latest가 없고 `m2_allowed=false`, 후보·목표·주문 계산
  false다.
- 대상 신규 코드·template·test·ExecPlan 문자열 검사는 5개 파일 0건이다. `PLANS.md`의 비복구형
  6건은 수정 전 백업에도 같은 원문이 있어 이번 변경의 신규 깨짐은 0건이다.
- 공식 문자열 배치는 지정 보고서를 생성했지만 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기
  권한에서 rc=1이었으며, 지정 보고서의 검사 결과와 분리해 기록했다.
- 문자열 검사 결과물:
  `reports/c8_acquisition_session_mojibake_scan_latest.json`,
  `reports/c8_acquisition_session_plans_scan_current.json`.
- 기존 O6 `paper/fills.csv` SHA-256은
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`,
  `paper/trades_calc.csv` SHA-256은
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`로 유지됐고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 미래 선정일 원천을 획득하지 않았다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

## 19. 2026-09-15 C8 증거 기반 source adapter

### 변경 경계

- 백업: `backup/20260915_c8_source_adapter/20260915_074025/`.
- 변경 분류: M1 원천 정규화 writer 구현이며 매매 정책 변경이 아니다.
- 기존 O6, M2 후보 계산, 주문·체결·원장·브로커는 변경하지 않았다.

### 구현된 것

- `config/c8_source_adapter_contract_v1.json`에 adapter request, 지원 CSV 인코딩, 역할별 허용 상수,
  날짜·양수 숫자 필드, component·build request 발행 규칙을 고정했다.
- `src/c8_source_adapter.py`는 기존 획득 증거의 request를 다시 검증하고 evidence SHA-256이 같을 때만
  역할별 raw 원본을 읽는다.
- 원천 열 이름과 원천값 변환은 adapter request에 명시하도록 하고, 보통주·거래상태·기업행위 값을
  종목명이나 파일명으로 추정하지 않는다.
- 네 역할의 필수 열, 허용 상수, 날짜, 코드, 양수 종가·시총, 허용 상태값, 중복 코드와 코드 집합을
  모두 검증한 뒤에만 정규화 component 네 개와 `c8_build_request_latest.json`을 발행한다.
- adapter는 기존 builder를 자동 실행하지 않으며 canonical과 M2를 열지 않는다.
- `tools/adapt_kospi_mcap_quarterly_c8_sources.py`에 실제 CLI와 예상 실패 assertion을 추가했다.

### 검증 결과

- Python 3개 `compile()` 문법 검사와 adapter 계약 JSON 파싱 PASS.
- 신규 adapter 테스트 4개와 기존 M0·M1 전체 집중 회귀 총 25개 PASS.
- 100코드 양성 fixture는 `획득 세션 -> 증거 재검증 -> component 4개 -> build request -> 기존
  builder`까지 통과했다. adapter 자체는 canonical을 만들지 않았고 builder는 별도 호출했다.
- 증거 부재, 증거 이후 raw 변조, 역할별 99/100 코드 집합 불일치는 component와 build request 발행
  전에 차단됐다.
- 실제 RootA CLI 예상 실패 실행 rc=0, 판정은 `ADAPTER_REQUEST_MISSING`,
  `FAIL_C8_SOURCE_ADAPTER`다.
- 실제 결과물:
  `reports/c8_source_adapter_status_20260915074459.json`,
  `reports/c8_source_adapter_status_latest.json`.
- 실제 RootA에는 adapter request, component, build request, canonical이 없고 `m2_allowed=false`다.
- 신규 코드·계약·테스트·ExecPlan 문자열 검사는 5개 파일 0건이다. `PLANS.md`의 비복구형 6건은
  수정 전 백업에도 동일해 이번 변경의 신규 깨짐은 0건이다.
- 공식 문자열 배치는 지정 보고서를 생성한 뒤 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기
  권한에서 rc=1이었다. 지정 보고서는
  `reports/c8_source_adapter_mojibake_scan_latest.json`,
  `reports/c8_source_adapter_plans_scan_current.json`이다.
- 기존 O6 fills·trades SHA-256은 유지됐고 `orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 공식 원천과 역할별 원천 열·값 매핑이 없다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

## 20. 2026-09-15 C8 공식 원천 시간형태 적합성 감사

### 변경 경계

- 백업: `backup/20260915_c8_temporal_source_fit/20260915_080336/`.
- 변경 분류: M1 원천 준비도 진단 결함 수정이며 C8 적격성 정책 변경이 아니다.
- 기존 O6, C8 canonical 계약, source adapter writer, M2, 주문·체결·원장·브로커는 변경하지 않았다.

### 공식 원천 대조 결과

- 종가·시가총액은 KRX 전종목 시세의 조회일자 기반 역사 스냅샷 경로가 확인됐다.
- 관리종목·정리매매 현황은 현재 상태 화면이고, 관리종목 지정·해제와 거래정지는 종목별 사건 이력
  형태다.
- 신규상장·상장폐지는 기간 조회 사건 이력이며, 합병 소멸 예정부터 효력 발생까지 전 종목 상태를
  완결하는 단일 공식 원천은 확인하지 못했다.
- 따라서 현재의 `네 raw 역할 모두 동일 기준일·동일 전체 코드 집합` 계약은 실제 공식 원천 형태와
  맞지 않는다. 단순 열·값 매핑만으로는 해결할 수 없다.
- 정확한 코드 집합 일치 검사는 sparse 사건 원천 자체가 아니라, 공식 증거로 기준일 상태를 재구성해
  base universe에 결합한 뒤 수행해야 한다.
- 사건이 없다는 이유만으로 `NORMAL` 또는 `NONE`을 기본값으로 채우지 않는다. 공식 원천이 해당
  조회범위의 완전성을 증명하지 못하면 기존 unknown FAIL-CLOSED를 유지한다.

### 구현된 것

- `src/c8_adapter_audit.py`에 네 역할별 `source_shape`, 역사 기준일 능력, 공식 경로 확인 상태,
  mapping-only 가능 여부와 상태 재구성 필요 여부를 추가했다.
- 최상위 감사 결과에
  `FULL_SNAPSHOT_CONTRACT_MISMATCH_WITH_OFFICIAL_SOURCE_TEMPORAL_SHAPES`,
  `mapping_only_resolution_possible=false`, `FAIL_SOURCE_SHAPE_MISMATCH`를 고정했다.
- 가격·시총만 mapping-only 역할로 분류하고, 상장·거래관리·기업행위 세 역할은 시점 상태 재구성
  대상으로 분류했다. 기업행위는 완전 원천 미확인 상태를 별도로 남겼다.
- `tools/audit_kospi_mcap_quarterly_c8_adapters.py`가 새 원인과 계약 적합성 상태를 CLI에 직접 표시한다.
- 테스트는 빈 원천과 현재형 수동 원천 모두에서 매핑만으로 build request가 열리지 않는지 검증한다.

### 검증 결과

- 수정한 Python 3개 `compile()` 문법 검사 PASS.
- 어댑터 감사 단위 테스트 2개 PASS, 기존 M0·M1 집중 회귀 총 25개 PASS.
- 실제 RootA 감사 실행 rc=0, 예상 판정 `C8_ADAPTERS_BLOCKED` 일치.
- 실제 최신 보고서는 `reports/c8_adapter_capability_status_20260915080727.json`과
  `reports/c8_adapter_capability_status_latest.json`이다.
- 핵심 값은 `mapping_only_resolution_possible=false`,
  `contract_fit.status=FAIL_SOURCE_SHAPE_MISMATCH`, ready 0개, blocked 4개,
  `build_request_possible=false`, `m2_allowed=false`다.
- actual adapter component, build request, canonical, 후보·목표·주문은 생성하지 않았다.
- 수정 코드·테스트·CLI·ExecPlan 문자열 검사는 4개 파일 0건이다. `PLANS.md`의 비복구형 6건은
  수정 전 백업의 같은 원문에서도 확인되어 이번 변경의 신규 깨짐은 0건이다.
- 문자열 검사기는 지정 보고서를 생성한 뒤 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기 권한에서
  rc=1이었다. 지정 보고서는 `reports/c8_temporal_source_fit_mojibake_scan_latest.json`과
  `reports/c8_temporal_source_fit_plans_scan_current.json`이다.
- 기존 O6 `paper/fills.csv`와 `paper/trades_calc.csv` SHA-256은 각각
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`,
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`로 유지됐고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 선정일 공식 원천과 상태 재구성 결과가 없다.
- 정책: PASS. 기존 C8 적격성 의미와 unknown FAIL-CLOSED를 유지했다.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 상장·거래관리·기업행위 사건 이력을 기준일 전 종목 상태로 재구성하는 계약과 구현이 없다.
- 합병 소멸 예정부터 효력일까지 완결하는 공식 원천이 확인되지 않았다.
- 2026년 3분기 최초 미래 선정일이 도래하지 않아 실제 동시 획득과 운영 반영은 아직 검증할 수 없다.

### 다음 진행

기존 C8 적격성 의미를 유지하는 시점 상태 재구성 계약을 먼저 작성한다. 각 사건 원천의 조회범위
완전성 증거, 유효 시작·해제·효력 구간, base universe left join, unknown 차단을 기계 검증한 뒤에만
source adapter와 builder의 코드 집합 검사를 재구성 이후 단계로 이동한다.

## 21. 2026-09-15 C8 시점 상태 재구성 코어

### 변경 경계

- 백업: `backup/20260915_c8_state_reconstruction_core/20260915_083218/`.
- 신규 계약·순수 계산·테스트만 추가했다. 기존 파일 수정은 이 ExecPlan과 RootA PLANS뿐이다.
- 변경 분류: M1 원천 형태 결함을 처리하는 구조 구현이며 C8 적격성 정책 변경이 아니다.
- 기존 acquisition evidence, source adapter, builder, canonical, M2와 O6는 변경하지 않았다.

### 구현된 것

- `config/c8_state_reconstruction_contract_v1.json`에 full snapshot과 두 sparse 모드를 고정했다.
  sparse 모드는 `EXHAUSTIVE_POINT_IN_TIME_EXCEPTIONS`와 `EXHAUSTIVE_INTERVAL_HISTORY`만 허용한다.
- sparse 부재를 정상 상태로 확장하려면 조회범위, 전체 base universe 범위, provenance PASS,
  공식 원천 ID와 SHA-256이 모두 있어야 한다.
- 사건 이력은 `query_start <= earliest listed_date`, `query_end == selection_as_of`를 만족해야 한다.
- `src/c8_state_reconstruction.py`가 listing base와 같은 기준일의 가격·시총 코드 집합을 먼저 확인하고,
  거래관리·기업행위 sparse 상태를 base code 전체로 재구성한다.
- 기준일 이후 사건, 불완전 이력, 검증되지 않은 provenance, 기본상태 불일치, 미등록 코드,
  중복 활성 구간, 가격 코드 누락은 전체 component 반환 전에 차단한다.
- 성공 fixture에서도 이 코어는 메모리 내 component만 반환하며 `publication_allowed=false`,
  `m2_allowed=false`다. 실제 파일 발행 책임은 기존 writer와 분리했다.

### 검증 결과

- 신규 계약 JSON 파싱 PASS, 신규 Python 2개 `compile()` 문법 검사 PASS.
- 신규 경계 테스트 5개 PASS, 기존 C8 M0·M1 집중 회귀 포함 총 30개 PASS.
- 양성 fixture는 100개 base code에 거래정지 1개와 합병대기 1개를 겹쳐 4개 component 모두
  정확히 100개로 재구성했다. 나머지 코드는 완전성 증거가 있는 경우에만 계약 기본상태가 적용됐다.
- provenance 미검증, 이력 시작일 부족과 기준일 이후 사건, 중복 활성 구간, 가격 코드 1개 누락은
  각각 `components={}`와 M2 차단으로 확인했다.
- 신규 계약·코드·테스트·ExecPlan 문자열 검사는 4개 파일 0건이다. `PLANS.md`는 기존 비복구형
  6건만 검출됐고 이번 추가 구간의 신규 깨짐은 0건이다.
- 문자열 검사기는 지정 보고서를 생성한 뒤 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기 권한에서
  rc=1이었다. 지정 보고서는 `reports/c8_state_reconstruction_core_mojibake_scan_latest.json`과
  `reports/c8_state_reconstruction_core_plans_scan_current.json`이다.
- 기존 O6 fills·trades SHA-256은 각각
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`,
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`로 유지됐다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 공식 원천과 provenance writer 연결이 없다.
- 정책: PASS. C8 상태 의미와 unknown FAIL-CLOSED를 유지했다.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 기존 acquisition evidence가 sparse 원천의 조회범위 완전성 메타데이터를 생성하지 않는다.
- 기존 source adapter와 builder는 이 재구성 코어를 호출하지 않는다.
- 합병 lifecycle을 완결하는 실제 공식 원천은 여전히 확인되지 않았다.

### 다음 진행

재구성 코어의 coverage·hash 입력을 기존 acquisition evidence가 실제 원천에서 생성하도록 계약을
확장하고, 그 검증 결과만 source adapter가 받아 component를 발행하도록 연결한다. builder의 exact code
set 검사는 재구성된 component에 유지한다.

## 22. 2026-09-15 C8 다중 원천 provenance·coverage 증거

### 변경 경계

- 백업: `backup/20260915_c8_multisource_evidence/20260915_084312/`.
- 변경 분류: M1 획득 증거 계약 확장이며 C8 적격성 정책 변경이 아니다.
- 기존 source adapter, 재구성 코어, builder, canonical, M2와 O6는 변경하지 않았다.

### 구현된 것

- acquisition evidence 계약을 `1.1.0`으로 올리고, 한 논리 역할에 여러 공식 파일을 허용하되 중복
  역할에는 명시적·고유한 `source_id`를 요구한다.
- 원천별 `FULL_SNAPSHOT`, `EXHAUSTIVE_POINT_IN_TIME_EXCEPTIONS`,
  `EXHAUSTIVE_INTERVAL_HISTORY`를 검증하고, interval은 시작일과 종료일을 별도 query parameter에서
  읽어 종료일이 `selection_as_of`와 같은지 확인한다.
- sparse 역할에는 전체 base universe 범위, 조회기간, 원천 ID 집합, 상태 필드 집합과 실제 raw
  SHA-256을 묶은 `role_coverage_checks`가 PASS일 때만 evidence를 발행한다.
- 단일 full snapshot 기존 요청은 역할당 한 파일인 경우에만 기존 형식과 호환되는 exact snapshot
  coverage를 생성한다.
- acquisition session은 역할과 `source_id`를 raw 파일명에 함께 넣어 같은 역할의 여러 공식 파일이
  서로 덮어쓰지 않게 하고, sparse coverage 선언의 source ID 집합을 raw 복사 전에 검증한다.
- descriptor template에 기본 source ID·temporal mode와 `role_coverage` 입력 위치를 추가했다.

### 검증 결과

- 변경 Python 4개 `py_compile` PASS, 계약·template JSON 2개 파싱 PASS.
- 기존 M0·M1과 C8 전체 집중 회귀 33개 PASS. 결과물은
  `reports/c8_multisource_evidence_pytest_latest.xml`이며 errors 0, failures 0이다.
- 두 개의 거래관리 interval history fixture는 서로 다른 raw 경로와 SHA-256으로 봉인됐고, evidence의
  해당 역할 coverage에 source ID 2개와 raw hash 2개가 보존됐다.
- sparse coverage에서 상태 필드 하나를 누락하면 `ROLE_COVERAGE_FIELDS_INCOMPLETE`로 evidence 발행
  전에 차단됐다.
- 성공 fixture도 adapter component, canonical, 후보·목표·주문을 만들지 않았고 `m2_allowed=false`를
  유지했다.
- 수정 코드·계약·테스트·ExecPlan 문자열 검사 7개 파일은 0건이다. `PLANS.md`의 비복구형 6건은
  수정 전 백업의 같은 행에서도 확인되어 이번 변경의 신규 깨짐은 0건이다.
- 문자열 검사기는 지정 보고서를 생성한 뒤 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기 권한에서
  rc=1이었다. 지정 보고서는 `reports/c8_multisource_evidence_mojibake_scan_latest.json`과
  `reports/c8_multisource_evidence_plans_scan_current.json`이다.
- 기존 O6 `paper/fills.csv`와 `paper/trades_calc.csv` SHA-256은 각각
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`,
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`로 유지됐고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 공식 다중 원천을 획득·봉인하지 않았다.
- 정책: PASS. 적격성 의미와 unknown 차단을 변경하지 않았다.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- source adapter는 아직 한 역할당 한 full snapshot을 전제로 하므로 이 coverage 증거와 재구성 코어를
  소비하지 않는다.
- 실제 거래관리·기업행위 원천의 열·값 mapping과 합병 lifecycle 완전 원천은 확인되지 않았다.
- 실제 공식 원천으로 재구성된 component와 builder PASS가 없으므로 M2는 계속 STOP이다.

### 다음 진행

source adapter가 evidence의 `role_coverage_checks`와 원천별 hash를 입력 권한으로 사용해 재구성 코어를
호출하도록 연결한다. 원천별 explicit mapping, 재구성 후 base code exact set, component 발행 전
FAIL-CLOSED를 검증하되 실제 원천 부재 시 canonical과 M2는 열지 않는다.

## 23. 2026-09-15 C8 adapter·상태 재구성 연결

### 변경 경계

- 백업: `backup/20260915_c8_adapter_reconstruction_bridge/20260915_090419/`.
- 변경 분류: M1 provenance 보존 결함 수정이며 C8 적격성 정책 변경이 아니다.
- 기존 builder 계약, M2, 주문·체결·원장·브로커와 O6는 변경하지 않았다.

### 발견된 문제와 수정

- 기존 adapter는 evidence source를 역할별 단일 딕셔너리로 축약해 같은 역할의 여러 공식 원천 중 한
  파일만 남길 수 있었다.
- 기존 재구성 코어는 sparse 사건 한 행이 역할의 모든 상태 필드를 가진다고 가정해 거래정지 이력과
  관리종목 이력처럼 독립 원천을 정확히 결합할 수 없었다.
- adapter 계약을 `1.1.0`으로 올리고 sparse transform에 명시적 `source_id`와 `covered_fields`를
  요구했다. 각 transform의 field 합집합은 evidence role coverage와 정확히 같아야 한다.
- adapter가 evidence의 source ID·raw SHA-256 coverage를 다시 확인한 뒤 원천별 transform을 실행하고,
  그 결과만 시점 상태 재구성 코어에 전달하도록 연결했다.
- 재구성 코어는 사건 행의 `covered_fields`만 상태에 반영한다. 같은 종목에서도 서로 다른 필드의 활성
  구간은 병합하고, 같은 필드의 겹치는 활성 구간만 기존 overlap 차단으로 처리한다.
- 재구성 PASS와 네 component의 exact code set이 모두 확인된 뒤에만 component와 build request를
  발행한다. build request에는 역할별 공식 source ID 목록, raw SHA-256 목록과 재구성 계약 버전을
  함께 보존한다.
- 기존 단일 full snapshot transform은 source ID 생략 호환을 유지한다. code-set mismatch의 기존 상위
  reason code도 유지하면서 재구성 세부 원인을 추가했다.

### 검증 결과

- 변경 Python 4개 `py_compile` PASS, adapter 계약 JSON 파싱 PASS.
- 기존 M0·M1과 C8 전체 집중 회귀 37개 PASS. 결과물은
  `reports/c8_adapter_reconstruction_bridge_pytest_latest.xml`이며 errors 0, failures 0이다.
- fixture 조건은 base universe 102개, 거래정지 1개와 관리·상폐절차 1개다. 두 개의 독립 interval
  원천을 결합한 뒤 102개 상태 component를 만들고, 기존 builder에서 부적격 2개를 제외해 적격
  100개 canonical 검증을 통과했다.
- coverage 필드 합집합 누락은 `SPARSE_TRANSFORM_COVERAGE_MISMATCH`, 복수 원천 transform의 source ID
  누락은 `TRANSFORM_SOURCE_MAPPING_MISSING_DUPLICATE_OR_EXTRA`로 component 발행 전에 차단됐다.
- 동일 종목에서 서로 다른 상태 필드의 겹치는 구간은 정상 병합되고, 동일 필드의 겹치는 활성 구간은
  기존 `OVERLAPPING_ACTIVE_INTERVALS`로 차단됐다.
- 실제 RootA CLI 실행은 rc=0으로 예상 판정 `FAIL_C8_SOURCE_ADAPTER`와
  `ADAPTER_REQUEST_MISSING`을 확인했다. 실제 component, build request, canonical은 없고
  `m2_allowed=false`다.
- 실제 보고서는 `reports/c8_source_adapter_status_20260915091504.json`과
  `reports/c8_source_adapter_status_latest.json`이다.
- 수정 코드·계약·테스트·ExecPlan 문자열 검사 6개 파일은 0건이다. `PLANS.md`는 기존 비복구형
  6건만 검출됐다.
- 문자열 검사기는 지정 보고서를 만든 뒤 고정 `2_Logs/mojibake_text_scan_latest.json` 쓰기 권한에서
  rc=1이었다. 지정 보고서는 `reports/c8_adapter_reconstruction_bridge_mojibake_scan_latest.json`과
  `reports/c8_adapter_reconstruction_bridge_plans_scan_current.json`이다.
- 기존 O6 fills·trades SHA-256은 각각
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`,
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`이며
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 실제 공식 원천과 adapter request가 없다.
- 정책: PASS. C8 상태·적격성·unknown 차단 의미를 변경하지 않았다.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 실제 KRX 원천별 열·값 mapping과 해당 원천으로 봉인된 adapter request가 없다.
- 합병 lifecycle을 완결하는 실제 공식 원천은 여전히 확인되지 않았다.
- fixture E2E는 통과했지만 실제 공식 원천의 component·builder PASS가 없으므로 C8은 준비되지 않았다.

### 다음 진행

현재 실제 원천 부재 상태에서 임의 mapping을 만들지 않는다. 다음 단계는 공식 원천 파일이 확보됐을 때
각 source ID별 열·값을 read-only로 조사해 adapter request를 작성하고, 실제 evidence 재검증부터
builder까지 실행하는 것이다.

## 24. 2026-09-15 C8 실제 원천 매핑 준비도·모집단 감사 교정

### 변경 경계

- 백업: `backup/20260915_c8_mapping_readiness_audit/20260915_093134/`.
- 변경 분류: 승인된 C8 모집단 정의를 잘못 집계한 감사기 결함 수정이다. 적격성·M2·주문 정책은
  변경하지 않았다.
- 수정 파일은 `src/c8_adapter_audit.py`, 해당 테스트와 이 ExecPlan·RootA PLANS다.

### 발견된 문제와 수정

- 기존 감사기는 KOSPI 행 중 `주식종류`에 `보통`이 들어가면 보통주로 집계해, `증권구분`이 리츠,
  투자회사, 외국주권, 주식예탁증권인 28개도 포함했다.
- 승인 계약에 맞춰 `시장구분=KOSPI AND 증권구분=주권 AND 주식종류=보통주`를 모두 만족할 때만
  `COMMON` 모집단으로 계산하도록 교정했다.
- 감사 산출물에 KOSPI 전체 행, 승인 모집단 수, 제외 수와 증권·주식 종류별 제외 분포를 추가했다.
- 역할별 실제 열 매핑 준비도를 추가했다. 상장·상태·가격은 직접 열 매핑이 가능하지만 공식
  `as_of` 권한과 시점 재구성이 부족해 `PARTIAL`, 기업행위는 합병 pending-to-effective lifecycle이
  완결되지 않아 `BLOCKED`로 유지했다.

### 실제 입력과 결과

- listing 원천: `_cache/manual_static_population_source/data_4425_20260715.csv`, SHA-256
  `a1c83586ad0316ade1612a0a6a3817ce8ddc73fc2a1a9ce9772f4d52468ac59d`.
- status 원천: `_cache/manual_static_population_source/data_4446_20260715.csv`, SHA-256
  `5fe67a488983e956b3677dd8f876e0508a172e52db85f2103ff1969ff468ad69`.
- price 원천: `_krx_manual/_inbox/data_1628_20260822.csv`, SHA-256
  `d9ffffa6bb0557dbede527d1e2549c867af798b7fc4a0ede394803bb55d4afbf`.
- 실제 집계는 KOSPI 944행, 승인된 KOSPI 보통주 806개, 제외 138개다. 제외에는 리츠 23,
  사회간접자본투융자회사 2, 투자회사 1, 외국주권 1, 주식예탁증권 1과 주권 내 우선·종류주 110개가
  포함된다.
- 가격은 승인 모집단 806개 중 804개를 포함하며 `031440`, `057050` 2개가 누락됐다. listing과 price가
  서로 다른 파일 기준일이고 둘 다 `FILENAME_ONLY_UNVERIFIED_ASOF`라 coverage는 PASS로 승격하지
  않았다.
- 실제 보고서: `reports/c8_adapter_capability_status_20260915093906.json` 및
  `reports/c8_adapter_capability_status_latest.json`.
- 실제 감사 결과는 `C8_ADAPTERS_BLOCKED`, adapter ready 역할 0개, component·build request 미생성,
  `m2_allowed=false`다.

### 검증 결과

- 변경 Python 2개 `compile()` 문법 검사 PASS.
- 리츠가 가격 파일에 존재해도 COMMON 모집단에서 제외되는 회귀를 포함한 C8·M0·M1 테스트 37개
  PASS. 증거: `reports/c8_mapping_readiness_full_pytest_latest.xml`.
- 최악조건인 서로 다른 파일 날짜명에 동일 bytes가 있는 경우 conflict 1개를 탐지했고, 공식
  `as_of`로 승격하거나 adapter 파일을 만들지 않아 `PASS_FAIL_CLOSED`다.
- 이번 코드 수정 전 고정 O6 해시는 fills
  `55d4d1289b7f47d28b51b2be8db571ada3fdf06f7d3cb4bbda2e4e6b5d2f8505`, trades
  `ad20150f5d0fd9944935470bb0e82e71a07746497bfecdffe13bb70c225a1bff`였다. 그러나 09:27 신규 BUY와
  09:32 SELL이 실제 paper 파일에 추가되어 09:39 해시는 각각
  `06d66ffc1fac56fb15cc3a3592c2372d2df0bbee1e9a7f01d58efa6e792bad1a`,
  `546c60b6ecd578b883b0c0625c8417039e68fd43b6e9e5449b0da3c402f204f8`로 바뀌었다. 이번 수정은 O6
  writer를 호출하지 않았지만 동시 운영 갱신 때문에 단계 전체의 O6 불변성은 검증하지 못했다.
- 수정 코드·테스트·ExecPlan 문자열 검사는 3개 파일 0건이다. `PLANS.md`의 비복구형 6건은 수정 전
  백업의 같은 행·문자열에서도 확인되어 이번 추가 구간의 신규 깨짐은 0건이다. 증거는
  `reports/c8_mapping_readiness_mojibake_scan_latest.json`과
  `reports/c8_mapping_readiness_plans_scan_current.json`이다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS. C8 모집단과 가격 coverage 계산 기준.
- 운영 반영: NA. 실제 공식 query evidence·adapter request가 없고 O6는 동시 갱신됐다.
- 정책: PASS. C8 승인 모집단 의미를 정확히 적용했고 완화하지 않았다.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 네 역할 모두 공식 query parameter 기반 `as_of` 증거가 없어 실제 adapter request를 작성할 수 없다.
- `031440`, `057050`의 listing-price 기준일 불일치가 해소되지 않았다.
- 합병 lifecycle 완전 공식 원천과 revision-aware 수집 경로가 확인되지 않았다.
- 이번 범위와 무관한 paper O6가 작업 중 갱신되어 O6 불변성은 별도 시점 격리 검증이 필요하다.

### 다음 진행

공식 KRX 조회에서 동일 `selection_as_of`로 listing/status/price를 동시 봉인하고, 기업행위는
pending-to-effective 변경 이력을 완전하게 제공하는 revision-aware 원천을 먼저 확정한다. 그 전에는
현재 로컬 파일로 adapter request를 만들거나 M2를 열지 않는다.

## 25. 2026-09-15 C8 공식 원천 경로 계약·읽기 전용 API preflight

### 변경 경계

- 백업: `backup/20260915_c8_official_source_preflight/20260915_095807/`.
- 변경 분류: M1 원천 연결 진단 구현이며 C8 적격성·M2·주문 정책 변경이 아니다.
- 신규 원천 경로 계약, preflight 모듈·CLI·테스트만 추가했다. 기존 acquisition, adapter, builder,
  canonical과 O6 writer는 수정하지 않았다.

### 공식 경로 확인 결과

- KRX OPEN API `stk_isu_base_info`는 요청 인자 `basDd`로 유가증권 종목기본정보를 조회하고,
  `ISU_SRT_CD`, `LIST_DD`, `MKT_TP_NM`, `SECUGRP_NM`, `KIND_STKCERT_TP_NM` 등을 반환하는 계약이다.
- KRX OPEN API `stk_bydd_trd`도 `basDd`를 사용하며 응답에 `BAS_DD`, `ISU_CD`, `TDD_CLSPRC`,
  `MKTCAP`을 포함한다.
- KRX Data Marketplace 실제 화면 DOM에서 가격·시총 `MDCSTAT01501_OUT`, 상장 이력
  `MDCSTAT20001`, 상폐 이력 `MDCSTAT23801`, 거래정지 `MDCSTAT21301`, 관리종목 `MDCSTAT21501`,
  정리매매 현재상태 `MDCSTAT23701`의 BLD와 조회 입력을 확인했다.
- 거래정지·관리종목 이력은 종목별 `isuCd + strtDd + endDd` 조회라 base universe 전체 순회 coverage가
  필요하다. 정리매매 화면은 시장 전체 현재상태이지만 query date가 없다.
- 합병 pending-to-effective를 revision-aware하게 완결하는 공식 경로는 확인되지 않아 기업행위 역할은
  계속 `BLOCKED`다.

### 구현된 것

- 신규 `config/c8_official_source_route_contract_v1.json`에 공식 API 2개와 수동 이력 화면 5개의 실제
  source ID, endpoint/BLD, 날짜·시장·종목 query parameter, coverage shape를 고정했다.
- 신규 `src/c8_official_source_preflight.py`는 키를 메모리에서만 사용해 두 API의 HTTP 상태, 최소 행,
  필수 필드, 코드 공백·중복, 응답 `BAS_DD`를 검증한다.
- 신규 CLI `tools/preflight_kospi_mcap_quarterly_c8_official_sources.py`는 키 값은 출력·보고서 저장하지
  않고 진단 결과만 versioned/latest JSON에 쓴다.
- preflight는 성공해도 raw, acquisition evidence, adapter request, component, canonical, M2, 후보,
  목표·주문을 생성하지 않는다.

### 실제 실행 결과

- 기존 `_cache/krx_api_key.txt`는 존재하지만 값을 출력하거나 산출물에 기록하지 않았다.
- 직전 거래일 `20260914` 조회에서 `stk_isu_base_info`는 HTTP 401 `Unauthorized API Call`로 실패했다.
- 같은 키의 `stk_bydd_trd`는 HTTP 200, 943행, 응답 `BAS_DD=20260914`, 필수 가격·시총 필드와 코드
  고유성 검증을 통과했다.
- 실제 판정은 `FAIL_C8_OFFICIAL_SOURCE_PREFLIGHT`, 원인은
  `KRX_OPENAPI_STK_ISU_BASE_INFO_HTTP_STATUS_401`이다. 키 원문이 보고서에 없음을 재검사했다.
- 실제 보고서: `reports/c8_official_source_preflight_status_20260915100458.json` 및
  `reports/c8_official_source_preflight_status_latest.json`.

### 검증 결과

- 신규 Python 3개 `compile()` PASS, 경로 계약 JSON 파싱 PASS.
- 권한 401, 가격만 성공, 빈 응답, 응답 기준일 불일치, 키 없음, 미래일, 비밀 미저장을 포함한 신규
  테스트 5개 PASS.
- 기존 C8·M0·M1 회귀를 합친 42개 PASS. 증거:
  `reports/c8_official_source_preflight_pytest_latest.xml`.
- 최악조건은 listing API가 401인데 price API만 정상인 실제 상황이다. 전체를 FAIL로 유지하고 raw와
  adapter·M2를 만들지 않아 FAIL-CLOSED PASS다.
- 작업 중 paper runtime이 계속 갱신되어 현재 fills/trades SHA-256은 각각
  `f742a4a08e9783b72b35c56a62eac5549287ead06f1097ac50ee16057aa925b7`,
  `37adb4b9c806da4c617c2b74ff0b67983feb582081ccf5ee6dfaf307603ddd3b`다. preflight는 O6 writer를
  호출하지 않았지만 동시 갱신 때문에 O6 불변성은 이번 단계에서도 검증하지 못했다.
- 신규 계약·코드·테스트·ExecPlan 문자열 검사는 5개 파일 0건이다. `PLANS.md`의 비복구형 6건은
  수정 전 백업에도 같은 행·문자열로 존재해 이번 추가 구간의 신규 깨짐은 0건이다. 증거는
  `reports/c8_official_source_preflight_mojibake_scan_latest.json`과
  `reports/c8_official_source_preflight_plans_scan_current.json`이다.

### 단계 판정

- 기능: PASS.
- 정합성: PASS.
- 운영 반영: NA. 진단 전용이며 실제 acquisition·adapter 경로에는 연결하지 않았다.
- 정책: PASS.
- FAIL-CLOSED: PASS.
- 회귀: PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 현재 KRX 키에 `stk_isu_base_info` 서비스 권한이 없다.
- 거래정지·관리종목의 base universe 전체 종목 순회와 query coverage 증거가 구현되지 않았다.
- 정리매매 현재상태는 query date가 없어 기준일 권한을 별도로 해결해야 한다.
- 합병 lifecycle 완전 원천이 확인되지 않았다.

### 다음 진행

기본정보 API 이용 권한이 확보되기 전에는 listing 원천 E2E를 진행할 수 없다. 현재 코드로 가능한 다음
구현은 거래정지·관리종목 종목별 조회의 completeness manifest와 재시작 가능한 수집 계약이지만, 실제
806개 종목 호출 및 Data Marketplace 내부 BLD 자동화는 호출량·운영 방식 승인이 필요하다.

## 26. 2026-09-15 C8 상태 원천 완전성 manifest·재시작 계약

### 변경 경계

- 백업: `backup/20260915_c8_status_traversal_manifest/20260915_101322/`.
- 변경 분류: M1 C8 상태 원천 수집 준비 계약이다. C8 적격성·M2·주문·운영 정책 변경이 아니다.
- 실제 KRX 조회, 806개 종목 순회, raw 저장, acquisition evidence, adapter, component, canonical 생성은
  수행하지 않았다.

### 확인된 원천 범위와 판단

- KRX OPEN API 공식 서비스 목록에는 종목 기본정보와 일별매매정보는 있지만, 거래정지·관리종목·정리매매
  상태를 기준일별로 완결하는 별도 Open API는 확인되지 않았다.
- 거래정지 `MDCSTAT213`과 관리종목 `MDCSTAT215`는 종목별 기간 조회이므로 승인 모집단의 정확한 코드
  집합 전체를 순회해야 한다.
- 정리매매 `MDCSTAT237`은 시장 전체 현재상태 조회이지만 query date와 공식 응답 기준일이 없다. 따라서
  거래정지·관리종목 순회를 전부 마쳐도 `delisting_procedure_status`의 `selection_as_of` 권한은 생기지
  않는다.

### 구현된 것

- 신규 `config/c8_status_traversal_contract_v1.json`에 세 상태 필드와 세 공식 화면 원천의 coverage shape,
  필수 query parameter, 날짜 권한, 완료 조건을 고정했다.
- 신규 `src/c8_status_traversal.py`는 exact base-universe code set과 source SHA-256, query range를 봉인하고,
  원천별 종목 결과를 재시작 가능한 checkpoint로 기록한다.
- 동일 성공 응답 재기록은 no-op, 실패 후 재시도는 허용하고, 성공 후 서로 다른 응답·미등록 종목·범위
  불일치·manifest hash 또는 request fingerprint 변조는 차단한다.
- 거래정지·관리종목 전 종목이 PASS여도 현재 정리매매 원천의 기준일 권한이 없으면 판정을
  `C8_STATUS_HISTORY_TRAVERSAL_COMPLETE_ROLE_BLOCKED`로 유지한다.
- 신규 CLI `tools/manage_kospi_mcap_quarterly_c8_status_traversal.py`는 실제 수집기가 아니라 계약 또는 기존
  manifest 감사기다. raw·evidence·adapter·M2·후보·목표·주문을 만들지 않는다.

### 실제 실행과 검증

- 실제 계약 상태 CLI는 rc=0으로 예상 판정 `C8_STATUS_TRAVERSAL_CONTRACT_BLOCKED`를 확인했다.
  원인은 `LIQUIDATION_ASOF_AUTHORITY_UNRESOLVED` 하나이며 `bulk_requests_executed=0`, `role_ready=false`,
  `selection_evidence_eligible=false`, `m2_allowed=false`다.
- 실제 보고서:
  `reports/c8_status_traversal_status_20260915101945.json`,
  `reports/c8_status_traversal_status_latest.json`.
- 신규 Python 3개 `py_compile` PASS, 신규 fixture 테스트 7개 PASS다.
- 기존 C8·M0·M1을 포함한 집중 회귀 49개가 failures 0, errors 0으로 PASS했다. 증거:
  `reports/c8_status_traversal_pytest_latest.xml`.
- 최악조건은 두 이력 원천의 전 종목 성공을 만든 뒤 정리매매 응답에 공식 응답 기준일이라고 임의로
  표시한 경우다. 실제 route contract가 `WITHOUT_QUERY_DATE`이므로 역할 전체를 계속 BLOCKED로 유지했다.
- 또 다른 최악조건으로 성공 checkpoint의 `endDd`를 사후 변경했을 때
  `KRX_MDCSTAT213_SUSPENSION_HISTORY_RESULT_CONTRACT_INVALID`로 FAIL-CLOSED했다.
- 회귀 전후 O6 해시는 fills
  `bf1406e663f58f3682e5ce1ccfb3ee2f059bb5a6fb03fdc5be0658f846b302df`, trades
  `82146cbffb700b323f30daf95090691476040863431d590c7d553a07d2ce5a87`로 동일했고
  `paper/orders_exec.xlsx`는 없었다.
- 신규·수정 산출물 6개 문자열 검사는 0건이다. `PLANS.md`의 비복구형 6건은 수정 전 백업에도 같은
  문장이 모두 있어 이번 추가 구간의 신규 깨짐은 0건이다. 증거:
  `reports/c8_status_traversal_mojibake_scan_latest.json`,
  `reports/c8_status_traversal_plans_scan_current.json`.

### 단계 판정

- 기능: PASS. 완전성 manifest 생성·재시작·감사 함수와 CLI 계약 상태 실행.
- 정합성: PASS. exact code set·source hash·query range·payload hash·fingerprint 재검증.
- 운영 반영: NA. 실제 KRX 대량 조회와 수집기 연결은 수행하지 않았다.
- 정책: PASS. C8·M2·주문 정책을 변경하지 않았다.
- FAIL-CLOSED: PASS. 불완전 coverage·변조·정리매매 기준일 부재를 통과시키지 않는다.
- 회귀: PASS. C8·M0·M1 49개 PASS, O6 핵심 파일 해시 불변.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- `stk_isu_base_info` API 권한 401이 해소되지 않아 동일 기준일의 공식 base universe를 봉인할 수 없다.
- 실제 Data Marketplace BLD 호출을 수행하는 수집기와 806개 종목 순회 checkpoint는 아직 없다.
- 정리매매 현재상태의 공식 기준일 권한이 없고, 합병 pending-to-effective lifecycle 원천도 미확인이다.
- 실제 acquisition evidence·adapter request·재구성 component·builder PASS가 없다.

### 다음 진행

대량 호출 전 선행조건은 공식 base universe 확보와 정리매매 기준일 권한 해결이다. 두 조건 없이
거래정지·관리종목 806개를 먼저 순회하면 두 필드의 부분 coverage만 생기고 C8은 계속 BLOCKED다.
실제 내부 BLD 대량 수집은 호출량·간격·재시도·원천 저장 범위를 사용자 승인한 뒤 별도 단계로 진행한다.

## 27. 2026-09-15 C8 공식 현재 스냅샷·당일 기준일 권한 후보

### 변경 경계

- 백업: `backup/20260915_c8_same_day_authority/20260915_103332/`.
- 변경 분류: M1 원천 권한 진단과 정책 후보 설계다. 기존 acquisition evidence 계약, C8, M2, 주문
  정책은 변경하지 않았다.
- 실제 KRX 데이터 조회·다운로드, 로그인, raw 봉인, evidence·adapter·canonical 생성은 수행하지 않았다.

### 공식 화면 확인 사실

- KRX `전종목 기본정보` 화면은 `MDCSTAT019`, BLD
  `dbms/MDC/STAT/standard/MDCSTAT01901`, 응답 키 `OutBlock_1`이다.
- 조회 인자는 `mktId`이며 날짜 인자가 없다. 응답 열은 표준·단축코드, 종목명, 상장일, 시장구분,
  증권구분, 주식종류, 상장주식수를 포함한다.
- 현재 브라우저에서는 `로그인 또는 회원가입이 필요합니다`가 표시되어 실제 응답 row를 받지 못했다.
- KRX `정리매매종목 현황`은 `MDCSTAT237`, BLD `dbms/MDC/STAT/issue/MDCSTAT23701`, 응답 키
  `output`이다. 조회 인자는 `mktId`이며 날짜 인자가 없다.
- 응답 열은 종목코드, 시장·증권구분, 정리매매 시작·종료일, 상장폐지 예정일·사유를 포함한다.
  화면은 시세정보가 당일 정규시장 기준이라고 명시한다.

### 판단

- 두 화면 모두 공식 현재 스냅샷이므로 과거 `selection_as_of`를 재조회하는 원천으로 쓸 수 없다.
- 다만 `selection_as_of` 당일 장후에 인증 세션으로 응답을 즉시 봉인하면 retrieval timestamp가 당일
  현재상태 권한 후보가 될 수 있다.
- 현재 evidence v1.1은 full/point-in-time snapshot에도 공식 query date parameter를 요구하므로 이
  방식을 아직 승인하지 않는다. 실제 적용에는 제한된 `RETRIEVAL_DATE_SNAPSHOT` 정책 승인과 새 계약
  버전이 필요하다.

### 구현된 것

- 공식 경로 계약에 `KRX_MDCSTAT019_CURRENT_BASIC_INFO`와
  `KRX_MDCSTAT237_LIQUIDATION_CURRENT`의 실제 화면·BLD·응답 키·필수 열·현재 전용 성격을 추가했다.
- 신규 `config/c8_same_day_snapshot_authority_contract_v1.json`은 동일 KST 날짜, 15:40 이후, 인증 세션,
  동일 캡처 세션 raw·query·timestamp·SHA-256 봉인을 정책 승인 후보 조건으로 고정한다.
- 신규 `src/c8_same_day_snapshot_authority.py`와 CLI는 현재 날짜 조건·접근성·기존 evidence 계약 지원 여부를
  진단한다. 조건이 모두 맞아도 기존 계약이 승인하지 않으면 `CANDIDATE`까지만 허용하며 M2는 항상 false다.
- JSON 중복 키가 한 차례 삽입된 것을 원문 재확인에서 발견해 즉시 제거했고, 현재 snapshot route 키가
  정확히 1개이고 source ID 집합이 정확한지 회귀 테스트를 추가했다.

### 실제 실행과 검증

- 실제 `selection_as_of=20260914` preflight는 `BLOCKED`다. 원인은 캡처일 불일치, 15:40 이전,
  인증 세션 없음, evidence 계약 미승인 네 가지다.
- 실제 보고서:
  `reports/c8_same_day_snapshot_authority_status_20260915103833.json`,
  `reports/c8_same_day_snapshot_authority_status_latest.json`.
- 신규 Python 3개 `compile()` PASS, 신규 fixture 7개 PASS다.
- C8·M0·M1 전체 집중 회귀 56개가 failures 0, errors 0으로 PASS했다. 증거:
  `reports/c8_same_day_snapshot_authority_pytest_latest.xml`.
- 최악조건은 당일 장후·인증 세션 조건을 모두 맞추고도 기존 evidence 계약에 없는 권한을 통과시키는
  경우다. 판정은 `C8_SAME_DAY_AUTHORITY_CANDIDATE_REQUIRES_POLICY_APPROVAL`, evidence·M2·주문 false로
  유지됐다.
- 회귀 전후 O6 fills/trades SHA-256은 각각
  `bf1406e663f58f3682e5ce1ccfb3ee2f059bb5a6fb03fdc5be0658f846b302df`,
  `82146cbffb700b323f30daf95090691476040863431d590c7d553a07d2ce5a87`로 동일했고
  `paper/orders_exec.xlsx`는 없었다.
- 신규·수정 범위 7개 문자열 검사는 0건이다. `PLANS.md`의 기존 비복구형 6건은 수정 전 백업에도
  동일하게 존재했다. 증거: `reports/c8_same_day_snapshot_authority_mojibake_scan_latest.json`,
  `reports/c8_same_day_snapshot_authority_plans_scan_current.json`.

### 단계 판정

- 기능: PASS.
- 정합성: PASS. 실제 DOM의 화면·BLD·query·output 열과 계약을 대조했다.
- 운영 반영: NA. 실제 로그인·응답 row·raw 봉인 경로는 실행하지 않았다.
- 정책: PASS. 기존 증거 허용 범위를 바꾸지 않고 후보로 분리했다.
- FAIL-CLOSED: PASS.
- 회귀: PASS. 56개 PASS, O6 해시 불변.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제

- 20260914 과거 기준일은 현재 스냅샷으로 복구할 수 없다.
- KRX Data Marketplace 인증 세션에서 두 BLD의 실제 응답 row·HTTP timestamp·raw hash를 확인하지 못했다.
- `RETRIEVAL_DATE_SNAPSHOT`은 정책 미승인이고 evidence v1.1에서 지원하지 않는다.
- 합병 pending-to-effective lifecycle 공식 원천은 계속 미확인이다.

### 다음 진행

전문가 판단으로는 과거 기준일 복구를 시도하지 않고, 첫 실제 분기 선택일에만 적용되는 좁은
`RETRIEVAL_DATE_SNAPSHOT`을 승인·버전 분리하는 것이 현실적이다. 승인 범위는 KRX 두 source ID,
`selection_as_of` 당일 15:40 이후, 인증 응답 raw·timestamp·SHA-256 봉인, 과거 replay 금지로 제한한다.

## 28. 2026-09-15 C8 당일 retrieval-date 증거 정책 승인·구현

### 승인 및 변경 경계

- 사용자 승인: KRX `MDCSTAT019` 기본정보와 `MDCSTAT237` 정리매매 현재 스냅샷만 대상으로,
  `selection_as_of` 당일 KST 15:40 이후 인증 응답을 raw·query·retrieval/HTTP timestamp·SHA-256과
  같은 획득 세션에 봉인한다. 과거 replay는 금지한다.
- 백업: `backup/20260915_c8_retrieval_date_policy/20260915_105135/`.
- 변경 분류: M1 C8 증거 인정 정책 변경이다. C8 적격성 계산, M2, Gate, LOCK, O6 주문·체결·원장·통계는
  변경하지 않았다.

### 구현된 것

- `config/c8_acquisition_evidence_contract_v1.json`을 `1.2.0`으로 올리고
  `RETRIEVAL_DATE_SNAPSHOT`을 추가했다. 허용 official source ID는
  `KRX_MDCSTAT019_CURRENT_BASIC_INFO`, `KRX_MDCSTAT237_LIQUIDATION_CURRENT` 두 개뿐이다.
- 당일 KST 날짜, 15:40 이후, 인증 확인, HTTP 200, HTTP timestamp, date query 부재, 과거 replay false,
  raw mtime·retrieved_at·captured_at의 동일 세션 범위를 모두 강제한다.
- 기본정보는 listing 역할 전체 필드를 담당한다. 정리매매는 `delisting_procedure_status`만 담당하며,
  거래정지·관리 이력과 승인된 mixed composition으로 결합되고 세 원천의 필드 합집합이 정확하며 중복이
  없을 때만 역할 coverage를 PASS한다.
- `c8_acquisition_session.py`가 retrieval 메타데이터를 raw와 함께 request에 봉인하고, 장후·인증·소스·
  필드 조건을 raw 복사 전에 사전검증하도록 연결했다.
- same-day authority 계약을 `APPROVED_NARROW_SCOPE`로 전환했다. preflight PASS는 실제 evidence나 M2
  허용을 뜻하지 않으며 `m2_allowed=false`를 유지한다.

### 직접 실행한 검증

- Python 수정 파일 6개 `py_compile` PASS, JSON 계약 2개 parse PASS.
- 획득 evidence·session·authority 집중 테스트 24개 PASS.
- C8·M0·M1 전체 집중 회귀 63개 PASS. JUnit:
  `reports/c8_retrieval_date_policy_pytest_latest.xml`.
- 양성 preflight: `selection_as_of=20260915`, `captured_at=2026-09-15T16:00:00+09:00`, 인증 true에서
  `C8_SAME_DAY_AUTHORITY_PREFLIGHT_PASS`, 이유 없음, 계약 지원 true, `m2_allowed=false`를 확인했다.
- 최악조건 preflight: `selection_as_of=20260914`, 캡처일 20260915, 인증 false에서
  `SELECTION_ASOF_NOT_CAPTURE_DATE`, `AUTHENTICATED_KRX_SESSION_UNAVAILABLE`로 BLOCKED했다. 보고서:
  `reports/c8_same_day_snapshot_authority_status_20260915110024.json`,
  `reports/c8_same_day_snapshot_authority_status_latest.json`.
- 실제 acquisition 경로는 request가 없어 `ACQUISITION_REQUEST_MISSING`, evidence 미발행,
  `m2_allowed=false`다. 보고서:
  `reports/c8_acquisition_capture_status_20260915110042.json`,
  `reports/c8_acquisition_capture_status_latest.json`.
- O6 `trades_calc.csv` 해시는 변경 전후
  `82146cbffb700b323f30daf95090691476040863431d590c7d553a07d2ce5a87`로 동일하고
  `orders_exec.xlsx`는 없다. `fills.csv`는 동시 운영 갱신으로 10:58:26에 변경되어 이번 단계의 O6
  전체 불변성은 NA다.
- 수정 코드·계약·테스트·ExecPlan 9개 문자열 dry-run은 issues 0이다. 증거:
  `reports/c8_retrieval_date_policy_mojibake_scan_latest.json`.
- `PLANS.md`에는 기존 비복구형 6건이 남아 있으나 같은 행과 문자열이 수정 전 백업에도 존재한다. 증거:
  `reports/c8_retrieval_date_policy_plans_scan_current.json`과 수정 전 백업 원문 대조.

### 단계 판정

- 기능: PASS. 승인된 양성 fixture가 세션→raw→request→evidence까지 통과하고 M2는 닫혔다.
- 정합성: PASS. source ID·시간·인증·HTTP·hash·필드 coverage 계약을 교차검증했다.
- 운영 반영: NA. 실제 인증 KRX 응답과 실제 선택일 raw가 없다.
- 정책: PASS. 사용자 승인 범위를 계약 `1.2.0`에 한정해 반영했다.
- FAIL-CLOSED: PASS. 과거일·장중·비인증·미등록 source·date query·필드 누락을 차단했다.
- 회귀: PASS. C8·M0·M1 63개 PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 실제 KRX 인증 응답 raw·HTTP timestamp·SHA-256이 없으므로 현재 selection evidence는 없다.
- `src/c8_source_adapter.py`는 sparse mode로 point-in-time과 interval history만 처리하며
  `RETRIEVAL_DATE_SNAPSHOT` transform은 아직 `SPARSE_TRANSFORM_TEMPORAL_MODE_INVALID`가 된다.
- 합병 pending-to-effective lifecycle 공식 원천도 미확인이다.
- 다음 단계는 실제 KRX 조회가 아니라 adapter의 retrieval snapshot 변환 계약을 같은 두 source ID와
  covered fields에 한정해 구현·검증하는 것이다. 그 뒤에도 실제 raw 전에는 M2를 열지 않는다.

## 29. 2026-09-15 C8 retrieval-date adapter 변환 경계 구현

### 변경 경계

- 백업: `backup/20260915_c8_retrieval_adapter/20260915_111231/`.
- 변경 분류: M1 adapter 변환 계약 확장이다. state reconstruction, builder date authority, canonical,
  M2, 후보, 주문·체결·원장·통계는 변경하지 않았다.
- 실제 KRX 조회와 실제 adapter request 생성은 수행하지 않았다.

### 구현된 것

- `config/c8_source_adapter_contract_v1.json`을 `1.2.0`으로 올리고 승인된 두 official source ID별
  transform shape와 exact covered fields를 고정했다.
- `KRX_MDCSTAT019_CURRENT_BASIC_INFO`는 listing 전체 snapshot으로 변환한다.
- `KRX_MDCSTAT237_LIQUIDATION_CURRENT`는 `delisting_procedure_status` 당일 point-in-time exception으로
  변환한다.
- adapter는 evidence의 retrieval authority가 승인 source, 인증 true, HTTP 200, historical replay false인지
  다시 확인하고 source·transform covered fields가 계약과 정확히 같은지 검사한다.
- retrieval sparse row에는 source별 `temporal_mode`와 `covered_fields`를 보존해 다음 reconstruction 단계가
  서로 다른 시간 형태를 구분할 수 있게 했다.
- mixed temporal state reconstruction은 이번 범위에서 열지 않았다. 따라서 retrieval 변환 role check가
  모두 PASS해도 reconstruction FAIL 뒤 component와 build request는 발행되지 않는다.

### 직접 실행한 검증

- Python adapter·테스트 `py_compile` PASS.
- adapter 집중 테스트 9개 PASS. 신규 양성 fixture는 evidence revalidation PASS, 두 retrieval transform
  shape `FULL_ROLE_SNAPSHOT`·`POINT_IN_TIME_EXCEPTIONS` PASS 후 `STATE_RECONSTRUCTION_FAILED`로 중단했고
  adapter files·build request·M2는 false다.
- 미승인 source ID와 source/transform field claim 불일치는 각각
  `RETRIEVAL_TRANSFORM_SOURCE_NOT_APPROVED`, `RETRIEVAL_*_COVERED_FIELDS_MISMATCH`로 차단했다.
- 구현 중 adapter 전역 `date_authority`를 retrieval 문구로 바꿨을 때 기존 builder 2개 회귀가
  `COMPONENT_DATE_AUTHORITY_INVALID`로 실패했다. 전역 값은 원래 `OFFICIAL_QUERY_PARAMETER`로 복원하고
  retrieval 권한은 전용 계약 키로 분리해 기존 7개가 다시 PASS했다.
- C8·M0·M1 전체 집중 회귀 65개 PASS. JUnit:
  `reports/c8_retrieval_adapter_pytest_latest.xml`.
- 실제 기본 adapter 경로는 `ADAPTER_REQUEST_MISSING`, evidence revalidation false, component·build request·
  M2 false다. 보고서: `reports/c8_source_adapter_status_20260915111623.json`,
  `reports/c8_source_adapter_status_latest.json`.
- O6 `trades_calc.csv` 해시는 변경 전후
  `82146cbffb700b323f30daf95090691476040863431d590c7d553a07d2ce5a87`로 동일하고
  `orders_exec.xlsx`는 없다. `fills.csv`는 동시 운영 갱신으로 11:17:01에 변경되어 O6 전체 불변성은 NA다.
- adapter 계약·코드·테스트·ExecPlan 문자열 dry-run은 issues 0이다. 증거:
  `reports/c8_retrieval_adapter_mojibake_scan_latest.json`. `PLANS.md`의 기존 비복구형 6건은 수정 전
  백업에도 같은 행으로 존재한다. 현재 scan: `reports/c8_retrieval_adapter_plans_scan_current.json`.

### 단계 판정

- 기능: PASS. retrieval evidence를 승인된 shape로 변환하는 adapter 경계가 fixture에서 작동했다.
- 정합성: PASS. source ID·authority·covered fields를 evidence와 adapter 계약 사이에서 재검증했다.
- 운영 반영: NA. 실제 adapter request와 실제 KRX raw가 없다.
- 정책: PASS. 승인된 두 source의 adapter 변환만 추가했고 다음 단계 권한은 열지 않았다.
- FAIL-CLOSED: PASS. 미승인 source·field mismatch와 미지원 mixed reconstruction에서 발행을 차단했다.
- 회귀: PASS. C8·M0·M1 65개 PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- `c8_state_reconstruction.py`는 역할 전체에 temporal mode 하나만 받으므로 interval history와 retrieval
  point-in-time exception을 한 상태로 합성하지 못한다.
- 현재 builder의 component `date_authority`는 query-date 단일 값만 허용한다. reconstruction을 열기 전에
  역할·source별 혼합 권한을 허위 없이 전달하는 계약이 필요하다.
- 다음 단계는 mixed state reconstruction과 per-source date authority 전달을 함께 설계·구현하되,
  실제 raw가 없으면 canonical·M2는 계속 STOP하는 것이다.

## 30. 2026-09-15 C8 mixed state reconstruction과 source별 날짜 권한 계보

### 변경 경계

- 백업: `backup/20260915_c8_mixed_reconstruction/20260915_112648/`.
- 변경 분류: M1 reconstruction·builder 계약 확장이다. 실제 원천 취득, canonical 운영 발행, M2,
  후보·목표·주문·체결·원장·통계 정책은 변경하지 않았다.
- 연구 측정 라운드가 아니므로 round type은 `NA_NO_PERFORMANCE_MEASUREMENT`이고 고정 목표와 방향은 유지했다.

### 구현된 것

- state reconstruction 계약 `1.1.0`은 `trade_management_status_as_of`에만
  `MIXED_APPROVED_TEMPORAL_COMPONENTS`를 허용한다.
- interval history는 `trade_status`·`management_status`, 승인 당일 retrieval snapshot은
  `delisting_procedure_status`만 담당하며 source별 mode·field·official source ID가 coverage와 정확히
  맞아야 기본 상태를 확장한다.
- adapter 계약 `1.3.0`은 혼합 coverage mode를 reconstruction에 전달하고, sparse row마다
  `temporal_mode`·`covered_fields`·`official_source_id`를 보존한다.
- canonical 계약 `1.1.0`과 builder는 query-date, 승인된 당일 retrieval, 검증된 혼합 authority를 구분한다.
  component에 `source_date_authorities`를 남기고 builder가 source ID·mode·as-of·raw hash·query window·
  retrieval 인증·HTTP 200·응답시각을 재검증한 뒤 manifest에도 계보를 보존한다.
- adapter는 builder를 자동 실행하지 않으며 `m2_allowed=false`를 유지한다.

### 직접 실행한 검증

- 수정 Python 7개 `py_compile` PASS.
- reconstruction·adapter·builder 집중 테스트 27개 PASS.
- 혼합 양성 fixture에서 103개 base code 중 3개 제외 상태를 필드별로 합성해 적격 100개를 유지했고,
  adapter PASS 뒤 격리된 builder fixture까지 PASS했다. adapter 자체 M2는 false다.
- field overlap·미래 retrieval as-of·행 source lineage 불일치·retrieval HTTP 위조·source hash summary
  불일치는 각각 FAIL-CLOSED로 발행되지 않았다.
- 승인된 retrieval source ID의 role 바꿔치기와 timezone 없는 HTTP response timestamp도 builder에서
  FAIL-CLOSED로 차단했다.
- 최초 집중 회귀는 canonical 계약 버전 고정 때문에 7 PASS·11 FAIL이었다. `c8_source_audit.py`의 계약
  버전 검사를 `1.1.0`으로 맞춘 뒤 25개가 PASS했다.
- 전체 C8·M0·M1 회귀는 기본 temp ACL 때문에 59 PASS·13 FAIL이었고 코드 실패는 없었다. 동일 명령을
  권한 확장으로 재실행한 뒤 최종 보강 포함 74개 모두 PASS했다. JUnit:
  `reports/c8_mixed_reconstruction_pytest_latest.xml`.
- 실제 기본 adapter는 `ADAPTER_REQUEST_MISSING`, 실제 builder는 `C8_BUILD_REQUEST_MISSING`으로 중단했다.
  component·canonical·M2·후보·주문은 모두 생성되지 않았다. 증거:
  `reports/c8_source_adapter_status_20260915114152.json`,
  `reports/c8_source_build_status_20260915114156.json`과 각 latest.
- O6 baseline은 fills `f3bd898b...`, trades `82146cbf...`, orders 없음이었다. 운영 프로세스가 11:30~11:31에
  fills `5707d72b...`, trades `0515c2f1...`로 갱신해 이번 단계의 O6 전체 불변성은 NA다.
- 수정 코드·계약·테스트·ExecPlan 11개 문자열 dry-run은 issues 0이다. 증거:
  `reports/c8_mixed_reconstruction_mojibake_scan_latest.json`.
- PLANS current scan은 비복구형 6건이며 직전 단계 scan과 행 `3623, 10662, 30751, 30803, 42294,
  42390`이 동일하다. 이번 기록 구간의 신규 깨짐은 0건이다. 증거:
  `reports/c8_mixed_reconstruction_plans_scan_current.json`,
  `reports/c8_retrieval_adapter_plans_scan_current.json`.
- 검증용 `state/pytest_tmp_phase431`은 절대 경로 확인 후 삭제했고 부재를 확인했다.

### 단계 판정

- 기능: PASS. 승인된 혼합 입력의 필드별 재구성과 격리 builder 흐름이 작동했다.
- 정합성: PASS. source별 mode·field·date authority·hash 계보를 adapter와 builder가 교차검증했다.
- 운영 반영: NA. 실제 인증 KRX raw와 실제 adapter/build request가 없다.
- 정책: PASS. 승인된 두 retrieval source와 status role에만 범위를 제한했고 운영 정책은 바꾸지 않았다.
- FAIL-CLOSED: PASS. overlap·미래일·coverage/lineage·인증/hash 불일치가 발행 전에 차단됐다.
- 회귀: PASS. C8·M0·M1 74개 PASS.
- C8 실제 데이터 준비도: FAIL.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 실제 선택일의 인증 KRX raw·HTTP timestamp·SHA-256과 adapter request가 없다.
- 합병 pending-to-effective lifecycle의 공식 원천과 실제 corporate action coverage가 미확인이다.
- 다음 단계는 실제 원천 호출이 아니라, 현재 acquisition descriptor 작성에 필요한 source별 입력 목록과
  수동 수집 체크리스트를 고정하는 것이다. 실제 raw 취득은 별도 승인 전 수행하지 않는다.

## 31. 2026-09-15 C8 acquisition 수동 수집 입력 계약과 준비도 판정

### 변경 경계

- 백업: `backup/20260915_c8_manual_collection_contract/20260915_115821/`.
- 신규 입력 계약·read-only 판정기·CLI·테스트·체크리스트만 추가했다. 기존 acquisition, adapter,
  reconstruction, builder, canonical, M2, 후보·주문 코드는 변경하지 않았다.
- 실제 KRX 호출, acquisition session 시작, descriptor finalize, raw 복사, evidence 발행은 수행하지 않았다.

### 구현된 것

- `config/c8_manual_collection_contract_v1.json`에 descriptor 공통 필드, 6개 source 입력, status role의
  exact field ownership, 수집 시각, pre-finalize 10개 점검, 실행 권한 false를 고정했다.
- `src/c8_manual_collection_readiness.py`는 manual contract를 acquisition evidence, official route,
  adapter retrieval, status traversal 계약과 교차검증한다.
- `tools/check_kospi_mcap_quarterly_c8_manual_collection.py`는 네트워크 없이 현재 준비도를 판정한다.
- `docs/references/C8_ACQUISITION_MANUAL_COLLECTION_CHECKLIST.md`에 입력 순서와 descriptor 필수값,
  pre-finalize 절차, 차단 사유를 고정했다.

### 직접 실행한 검증

- JSON parse PASS: contract `1.0.0`, source 6개, declared blocker 5개.
- 신규 Python 3개 py_compile PASS.
- read-only CLI는 `BLOCKED_C8_MANUAL_COLLECTION`을 반환했고 network collection, descriptor finalize,
  canonical, M2, candidate, order를 모두 false로 유지했다.
- 실제 reason codes는 다음 5개다.
  - `CORPORATE_ACTION_SOURCE_UNRESOLVED`
  - `OPENAPI_ACQUISITION_HOST_CONTRACT_MISMATCH`
  - `LISTING_STATUS_TRANSFORM_UNRESOLVED`
  - `LIQUIDATION_STATUS_TRANSFORM_UNRESOLVED`
  - `STATUS_TRAVERSAL_RETRIEVAL_MODE_STALE`
- 최초 판정에 추가된 `OFFICIAL_ROUTE_QUERY_PARAMETERS_MISMATCH`는 API route가 배열 대신
  `date_parameter=basDd`를 쓰는 스키마 차이를 판정기가 잘못 해석한 것이었다. API route 분기를 수정해
  허위 차단을 제거했다.
- 신규 계약 테스트 3개 PASS. 필드 소유권 exact/non-overlap과 route parameter drift 차단을 확인했다.
- 신규 테스트 포함 C8·M0·M1 전체 77개 PASS. JUnit:
  `reports/c8_manual_collection_contract_pytest_latest.xml`.
- 신규 계약·판정기·CLI·테스트·체크리스트·ExecPlan 6개 문자열 dry-run은 issues 0이다. 증거:
  `reports/c8_manual_collection_contract_mojibake_scan_latest.json`.
- PLANS current scan의 비복구형 6건은 기존과 같은 행이며 이번 기록 구간의 신규 깨짐은 0건이다.
  증거: `reports/c8_manual_collection_contract_plans_scan_current.json`.
- O6는 운영 프로세스가 12:06에 fills와 trades를 다시 갱신해 전후 불변성은 NA이고 orders_exec는 없다.
- `c8_official_source_route_contract_v1.json`에는 같은 값의 `last_extended_at` 키가 두 번 있으나 현재 JSON
  해석값과 이번 준비도 판정에는 영향이 없는 사소한 기존 흠으로 남겼다.

### 단계 판정

- 기능: PASS. source별 입력과 차단 사유가 read-only CLI에서 재현된다.
- 정합성: PASS. 6개 source, 4개 role, status 3개 필드의 소유권과 계약 참조를 교차검증했다.
- 운영 반영: NA. 실제 수집·session·descriptor·raw가 없다.
- 정책: PASS. 수집과 downstream 권한은 모두 false이며 기존 정책은 변경하지 않았다.
- FAIL-CLOSED: PASS. blocker가 하나라도 있으면 finalize·canonical·M2·candidate·order가 열리지 않는다.
- 회귀: PASS. C8·M0·M1 77개 PASS.
- C8 실제 데이터 준비도: FAIL.
- 수동 수집 착수: STOP.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 다음 해결 순서는 정책 완화가 아니라 계약 불일치 제거다.
- 1순위는 `STATUS_TRAVERSAL_RETRIEVAL_MODE_STALE`을 현재 승인된 same-day retrieval 계약에 맞추는 것이다.
- 2순위는 listing과 liquidation raw를 canonical 상태값으로 바꾸는 명시적 transform 근거다.
- 3순위는 OpenAPI 실제 endpoint host와 acquisition allowed host의 계약 일치다.
- corporate action lifecycle 공식 원천은 외부 확인이 필요하므로 위 세 내부 문제와 분리한다.

## 32. 2026-09-15 C8 status traversal retrieval 계약 정렬

### 변경 경계

- 백업: `backup/20260915_c8_status_retrieval_alignment/20260915_121549/`.
- 변경 분류: 이미 승인·구현된 same-day `RETRIEVAL_DATE_SNAPSHOT` 정책과 이전 status traversal 계약의
  드리프트를 바로잡는 결함 수정이다. 신규 원천 허용, Gate·LOCK·M2·후보·주문 정책 변경은 아니다.
- 실제 KRX 호출, acquisition session, raw·evidence·adapter·canonical 생성은 수행하지 않았다.

### 발견된 문제와 수정

- 이전 status traversal 계약은 정리매매 현재 원천을 `EXHAUSTIVE_POINT_IN_TIME_EXCEPTIONS`와
  `UNRESOLVED_NO_QUERY_OR_RESPONSE_DATE`로 유지하고 있었다. 이후 승인된 acquisition evidence `1.2.0`과
  adapter `1.2.0`은 같은 원천을 `RETRIEVAL_DATE_SNAPSHOT`으로 승인했으므로 계약 간 의미가 충돌했다.
- status traversal 계약을 `1.1.0`으로 올리고 정리매매 원천의 mode를 `RETRIEVAL_DATE_SNAPSHOT`, 날짜
  권한을 `APPROVED_SAME_DAY_RETRIEVAL_TIMESTAMP_VIA_ACQUISITION_EVIDENCE`로 정렬했다.
- traversal manifest는 snapshot handoff만 기록하고 날짜 권한을 자체 부여하지 않도록 유지했다. 완전한
  이력과 snapshot이 있어도 acquisition evidence가 없으면 `LIQUIDATION_RETRIEVAL_EVIDENCE_REQUIRED`로
  BLOCKED다.
- manual collection 계약을 `1.1.0`으로 올리고 오래된
  `STATUS_TRAVERSAL_RETRIEVAL_MODE_STALE` 차단만 제거했다. 나머지 네 차단과 모든 실행 권한 false는
  유지했다.
- 체크리스트와 회귀 테스트를 같은 의미로 맞췄다.

### 직접 실행한 검증

- JSON 계약 2개 parse PASS, Python 수정 파일 3개 py_compile PASS.
- status traversal·manual collection 집중 테스트 10개 PASS. JUnit:
  `reports/c8_status_retrieval_alignment_focused_pytest_latest.xml`.
- C8·M0·M1 전체 집중 회귀 77개 PASS. JUnit:
  `reports/c8_status_retrieval_alignment_pytest_latest.xml`.
- status traversal CLI는 `C8_STATUS_TRAVERSAL_CONTRACT_BLOCKED`, reason
  `LIQUIDATION_RETRIEVAL_EVIDENCE_REQUIRED`, `role_ready=false`, `m2_allowed=false`, bulk request 0을 반환했다.
- manual collection CLI는 `BLOCKED_C8_MANUAL_COLLECTION`을 유지하며 reason이 다음 네 개로 정확히 줄었다.
  `CORPORATE_ACTION_SOURCE_UNRESOLVED`, `OPENAPI_ACQUISITION_HOST_CONTRACT_MISMATCH`,
  `LISTING_STATUS_TRANSFORM_UNRESOLVED`, `LIQUIDATION_STATUS_TRANSFORM_UNRESOLVED`.
- 최악조건은 traversal snapshot이 selection date와 공식 응답 필드 권한을 주장하는 경우다. 실제 fixture는
  이를 넣어도 acquisition evidence가 없으므로 `LIQUIDATION_RETRIEVAL_EVIDENCE_REQUIRED`로 차단했다.
- O6 fills/trades SHA-256은 수정 전후 각각
  `f0d120f075a630a6db8cba65af992afa1037ac65e0e71a0eab862cb261c9c475`,
  `6cc6148fb9b1c83f8a413411cb0ca638f1e6b65f42d688e2f8bc51a4c0e0d412`로 동일했고
  `paper/orders_exec.xlsx`는 없었다.
- 문자열 검증 증거 경로:
  `reports/c8_status_retrieval_alignment_mojibake_scan_latest.json`,
  `reports/c8_status_retrieval_alignment_plans_scan_current.json`.

### 단계 판정

- 기능: PASS. 새 시간모드와 상태 판정이 CLI에서 재현된다.
- 정합성: PASS. acquisition evidence·adapter·manual collection·status traversal의 source ID와 mode가 맞는다.
- 운영 반영: NA. 실제 KRX raw와 acquisition evidence가 없다.
- 정책: PASS. 기존 승인 범위의 계약 드리프트만 수정했고 실행 권한은 열지 않았다.
- FAIL-CLOSED: PASS. traversal snapshot만으로 날짜 권한·M2·주문이 열리지 않는다.
- 회귀: PASS. C8·M0·M1 77개 PASS, O6 해시 불변.
- C8 실제 데이터 준비도: FAIL.
- 수동 수집 착수: STOP.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 내부 차단은 네 개가 남았다: listing/liquidation 상태 transform 근거, OpenAPI host 계약 불일치,
  corporate action 공식 원천 미확인이다.
- 다음 내부 단계는 listing과 liquidation raw 필드를 canonical 상태값으로 바꾸는 명시적 transform 계약을
  고정하고, 입력 누락·미등록 값·중복 필드 소유권이 발행 전에 차단되는지 검증하는 것이다.

## 33. 2026-09-15 C8 공식 listing·liquidation 상태 transform 계약

### 변경 경계

- 백업: `backup/20260915_c8_official_status_transform/20260915_123043/`.
- 변경 분류: 승인된 두 retrieval source의 raw 필드와 canonical 상태값 사이의 미고정 변환 근거를
  source별 exact profile로 고정하는 구현 보완이다. 신규 원천·M2·Gate·LOCK·주문 정책 변경은 아니다.
- 실제 KRX 호출, 운영 raw·evidence·adapter request·canonical 생성은 수행하지 않았다.

### 구현된 것

- source adapter 계약을 `1.4.0`으로 올리고 두 exact profile을 추가했다.
- `MDCSTAT019_ACTIVE_LISTING_MEMBERSHIP_V1`은 `ISU_SRT_CD`, `ISU_NM`, `MKT_TP_NM`,
  `KIND_STKCERT_TP_NM`, `LIST_DD`만 canonical listing 열로 매핑한다. 동일 선택일 현재 전체 스냅샷의
  행 존재만 `listing_status=ACTIVE`로 만들고, adapter가 부재 종목을 합성하지 못하게 했다.
- `MDCSTAT237_LIQUIDATION_MEMBERSHIP_V1`은 `ISU_CD` 행 존재만
  `delisting_procedure_status=PROCEDURE`로 만든다. `NONE`은 mixed coverage가 완전한 state reconstruction에서만
  비예외 종목에 공급된다.
- profile ID, column map, constants, value maps는 계약과 정확히 같아야 한다. `KOSPI`와 `보통주` 외
  미등록 listing 값은 `UNMAPPED_SOURCE_VALUES`로 발행 전에 차단한다.
- manual collection 계약을 `1.2.0`으로 올리고 두 transform source를
  `CONDITIONAL_EXACT_TRANSFORM`으로 바꿨다. readiness 판정기는 manual profile과 adapter profile을
  교차검증한다.
- 체크리스트에 exact raw 열, membership 의미, default 공급 위치를 기록했다.

### 직접 실행한 검증

- JSON 계약 2개 parse PASS, Python 4개 py_compile PASS.
- 최초 집중 테스트는 14개 중 2개 FAIL이었다. 원인은 JSON 패치가 동일 이름의 앞선
  `sparse_transform_contract.required_fields`에 `transform_profile_id`를 넣은 치환 오류였다.
- 해당 필드를 sparse에서 제거하고 retrieval required fields로 옮긴 뒤 집중 테스트 14개 PASS. JUnit:
  `reports/c8_official_status_transform_focused_pytest_latest.xml`.
- 양성 fixture는 공식 기술 열에서 listing `ACTIVE/COMMON`, liquidation row `PROCEDURE`, 비예외 종목
  `NONE`을 만들고 adapter·reconstruction·builder PASS를 확인했다.
- 최악조건 1은 미등록 증권종류다. `UNMAPPED_SOURCE_VALUES`, `TRANSFORMED_ROWS_INVALID`로 component와
  build request를 발행하지 않았다.
- 최악조건 2는 listing membership 상수를 `DELISTED`로 바꾸는 경우다.
  `RETRIEVAL_TRANSFORM_CONSTANTS_MISMATCH`로 발행 전에 차단했다.
- C8·M0·M1 전체 집중 회귀 79개 PASS. JUnit:
  `reports/c8_official_status_transform_pytest_latest.xml`.
- 실제 manual readiness CLI는 두 transform source PASS, 전체는 corporate action source와 OpenAPI host
  두 reason 때문에 `BLOCKED_C8_MANUAL_COLLECTION`을 유지했다. 모든 실행 권한은 false다.
- 실제 adapter CLI는 `ADAPTER_REQUEST_MISSING`, evidence/component/build request/canonical/M2 모두 false다.
- O6 fills/trades SHA-256은 수정 전후 각각
  `f0d120f075a630a6db8cba65af992afa1037ac65e0e71a0eab862cb261c9c475`,
  `6cc6148fb9b1c83f8a413411cb0ca638f1e6b65f42d688e2f8bc51a4c0e0d412`로 동일했고
  `paper/orders_exec.xlsx`는 없었다.
- 수정 파일 10개 문자열 dry-run은 issues 0이다. PLANS 전체의 6건은 기존 비복구 항목이며
  line `3623, 10662, 30751, 30803, 42294, 42390`으로 동일하다. 증거:
  `reports/c8_openapi_host_contract_mojibake_scan_latest.json`,
  `reports/c8_openapi_host_contract_plans_scan_current.json`.
- 문자열 검증 증거 경로:
  `reports/c8_official_status_transform_mojibake_scan_latest.json`,
  `reports/c8_official_status_transform_plans_scan_current.json`.

### 단계 판정

- 기능: PASS. exact profile 양성 변환과 두 최악조건 차단이 fixture에서 재현됐다.
- 정합성: PASS. 공식 route field, adapter profile, manual readiness의 source ID와 covered fields가 일치한다.
- 운영 반영: NA. 실제 인증 KRX 응답 row·header·값은 없다.
- 정책: PASS. 기존 승인 source 범위 안에서 변환 근거만 좁게 고정했다.
- FAIL-CLOSED: PASS. profile·상수·미등록 값 불일치가 발행 전에 차단된다.
- 회귀: PASS. C8·M0·M1 79개 PASS, O6 해시 불변.
- C8 실제 데이터 준비도: FAIL.
- 수동 수집 착수: STOP.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 전체 준비도 blocker는 `OPENAPI_ACQUISITION_HOST_CONTRACT_MISMATCH`와
  `CORPORATE_ACTION_SOURCE_UNRESOLVED` 두 개다.
- 실제 `MDCSTAT019/237` 응답 row가 없으므로 `KIND_STKCERT_TP_NM=보통주`와 `ISU_CD` 6자리 형태의
  운영 적합성은 NA이며, 다른 값은 현재 안전하게 차단된다.
- 다음 내부 단계는 실제 OpenAPI endpoint host와 acquisition allowed host 계약을 공식 route 기준으로
  일치시키되 다른 호스트 허용 범위를 넓히지 않는 것이다.

## 34. 2026-09-15 C8 OpenAPI URL 필드별 host 계약 정렬

### 변경 경계

- 백업: `backup/20260915_c8_openapi_host_contract/20260915_124941/`.
- 변경 분류: 공식 route에 이미 고정된 documentation URL과 API endpoint를 acquisition 계약의
  `source_page_url`·`download_url` 권한에 정확히 연결한 계약 결함 수정이다.
- 실제 KRX 수집, raw·evidence·adapter request·canonical 생성, M2·후보·주문 권한은 실행하지 않았다.

### 구현된 것

- acquisition evidence 계약 `1.3.0`은 공용 `allowed_hosts`를 제거하고 method별
  `source_page_hosts`와 `download_hosts`를 분리했다.
- `KRX_OPEN_API` 설명 페이지는 `openapi.krx.co.kr`, 실제 다운로드 endpoint는
  `data-dbg.krx.co.kr`만 허용한다. 수동 Data Marketplace는 두 필드 모두 `data.krx.co.kr`만 허용한다.
- acquisition session과 evidence 검증기는 각 URL 필드의 전용 host 목록을 사용한다. host 목록 누락,
  필드 교차 사용, 유사 도메인은 fail-closed한다.
- manual readiness는 `KRX_OPENAPI_STK_BYDD_TRD`의 documentation host와 endpoint host를 acquisition
  계약과 교차검증한다. close/market-cap 원천은 `CONDITIONAL_AUTHENTICATED_OPENAPI_PREFLIGHT`로 바뀌고
  정적 host blocker는 제거됐다.
- 체크리스트에 OpenAPI 필드별 exact host를 기록했다.

### 직접 실행한 검증

- JSON 계약 2개 parse PASS, Python 6개 py_compile PASS.
- 최초 집중 실행은 fixture 기대값 2개가 구 `trdDd` 전용·schema `1.2.0`을 고정해 FAIL했고,
  시스템 임시 폴더 ACL로 setup ERROR 1건이 있었다. 기대값을 새 계약으로 고치고 프로젝트 내부
  `--basetemp`에서 재실행해 집중 테스트 22개 PASS를 확인했다. JUnit:
  `reports/c8_openapi_host_contract_focused_pytest_latest.xml`.
- 정상 fixture는 설명 host `openapi.krx.co.kr`, download host `data-dbg.krx.co.kr`, `basDd=20260914`로
  session과 evidence를 통과했다.
- 최악조건 fixture는 설명 URL에 endpoint host를 교차 사용하고 download URL을
  `data-dbg.krx.co.kr.evil.example`로 바꿨다. session은 raw copy 전, evidence는 publication 전에
  `SOURCE_PAGE_URL_INVALID`와 `DOWNLOAD_URL_INVALID`로 차단했다.
- C8·M0·M1 전체 회귀 81개 PASS. JUnit: `reports/c8_openapi_host_contract_pytest_latest.xml`.
- 실제 manual readiness CLI는 close/market-cap source PASS, 전체 reason은
  `CORPORATE_ACTION_SOURCE_UNRESOLVED` 하나, 모든 실행 권한 false를 반환했다.
- 실제 adapter CLI는 `ADAPTER_REQUEST_MISSING`, evidence/component/build request/canonical/M2 모두 false다.
- O6 fills/trades SHA-256은 수정 전후 각각
  `f0d120f075a630a6db8cba65af992afa1037ac65e0e71a0eab862cb261c9c475`,
  `6cc6148fb9b1c83f8a413411cb0ca638f1e6b65f42d688e2f8bc51a4c0e0d412`로 동일했고
  `paper/orders_exec.xlsx`는 없었다.

### 단계 판정

- 기능: PASS. 정상 field-host 조합을 허용한다.
- 정합성: PASS. route documentation·endpoint host와 acquisition field별 host가 일치한다.
- 운영 반영: NA. 실제 인증 KRX raw와 acquisition session이 없다.
- 정책: PASS. 공식 KRX 두 host의 역할만 고정했고 실행 권한은 열지 않았다.
- FAIL-CLOSED: PASS. 교차·유사·미등록 host가 raw copy와 evidence 발행 전에 차단된다.
- 회귀: PASS. C8·M0·M1 81개 PASS, O6 해시 불변.
- C8 실제 데이터 준비도: FAIL.
- 수동 수집 착수: STOP.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- readiness blocker는 `CORPORATE_ACTION_SOURCE_UNRESOLVED` 하나다.
- 실제 KRX row 부재로 listing/liquidation 변환의 운영 적합성은 NA다.
- 다음 단계는 corporate action의 pending-to-effective merger lifecycle을 완결하는 공식 원천과
  revision/date authority를 확인하는 read-only 원천 조사다. 임의 원천이나 mapping은 만들지 않는다.

## 35. 2026-09-15 C8 corporate action 공식 원천 적합성 조사

### 변경 경계

- 백업: `backup/20260915_c8_corporate_action_source_assessment/20260915_131729/`.
- 변경 분류: `CORPORATE_ACTION_SOURCE_UNRESOLVED`에 대한 read-only source contract 조사다.
- 실제 API 수집, raw·evidence·adapter request·canonical 생성, 계약 JSON 변경, M2·후보·주문 권한 변경은
  수행하지 않았다.

### 확인된 것

- OpenDART 공시검색 `list.json`은 정정보고서 전체, 6자리 종목코드, 접수번호·접수일, 정정·철회 비고와
  `E003 합병등종료보고서` 상세유형을 제공한다.
- OpenDART `cmpMgDecsn.json`은 회사별 합병결정과 합병기일·종료보고 총회일·등기예정일 등 예정 일정을
  구조화하지만 최초접수일 기준 검색이며 2015년 이후만 제공한다.
- OpenDART `document.xml`은 접수번호별 원문을 제공한다. KRX KIND 공식 사례로 다중 정정과 합병결정
  취소 시 일정 삭제가 실제 발생함을 확인했다.
- 따라서 공식 source 후보는 존재하지만 단일 source로는 revision-aware pending-to-effective lifecycle을
  완결하지 못한다. `list -> cmpMgDecsn -> document/E003` 복합 체인이 필요하다.
- 상세 판정은 `docs/references/C8_CORPORATE_ACTION_SOURCE_ASSESSMENT_20260915.md`에 고정했다.

### 직접 실행한 검증

- 새 평가 문서·ExecPlan·PLANS를 UTF-8로 다시 읽고 필수 문구와 endpoint를 assertion했다.
- 변경 평가 문서와 ExecPlan 문자열 dry-run은 issues 0이다. 전체 PLANS 스캔의 6건은 기존 비복구
  line `3623, 10662, 30751, 30803, 42294, 42390`이며 이번 추가 구간에는 신규 issue가 없다. 증거:
  `reports/c8_corporate_action_source_assessment_changed_files_mojibake_latest.json`,
  `reports/c8_corporate_action_source_assessment_mojibake_scan_latest.json`.
- 실제 manual readiness CLI는 `CORPORATE_ACTION_SOURCE_UNRESOLVED` 한 건으로
  `BLOCKED_C8_MANUAL_COLLECTION`을 유지했다. network/finalize/canonical/M2/candidate/orders는 모두 false다.
- 실제 adapter audit `--no-write`는 `C8_ADAPTERS_BLOCKED`, build request와 M2 false, output 0을 반환했다.
- 최초 회귀 실행은 소스 결함이 아니라 sandbox와 시스템 임시폴더 ACL 때문에 13 failure·1 setup error가
  났다. 정상 권한과 전략 namespace basetemp로 동일 81개를 재실행해 모두 PASS했다. JUnit:
  `reports/c8_corporate_action_source_assessment_pytest_latest.xml`.
- O6 fills/trades는 이번 작업이 쓰지 않았지만 장중 paper writer가 13:20에 신규 SELL 행을 추가해 조사 중
  해시가 바뀌었다. 따라서 이 단계에서는 O6 원장 불변성을 PASS로 주장하지 않고 NA로 둔다.
- `paper/orders_exec.xlsx`는 없었다.

### 단계 판정

- 기능: PASS. 필요한 공식 endpoint와 제공 필드를 문서로 확인했다.
- 정합성: PASS. 현재 canonical 필드·coverage·unknown 규칙과 source 기능을 대조했다.
- 운영 반영: NA. 인증키 호출과 실제 raw 수집을 하지 않았다.
- 정책: PASS. 정책·계약·권한을 변경하지 않았다.
- FAIL-CLOSED: PASS. source blocker와 모든 downstream false를 유지했다.
- 회귀: PASS. C8·M0·M1 81개 PASS, 생산 코드·계약 JSON 미변경.
- O6 운영 원장 불변성: NA. 장중 동시 writer 갱신으로 시작·종료 해시가 달라졌다.
- C8 실제 데이터 준비도: FAIL.
- 수동 수집 착수: STOP.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 같은 사건의 revision identity 규칙, 취소 판정 규칙, 실제 효력일 권한, `EFFECTIVE` 종료 의미가 미정이다.
- `cmpMgDecsn`의 2015년 시작과 현재 `query_start <= earliest listed_date` 계약도 충돌한다.
- 다음 단계는 source를 등록하는 코드 구현이 아니라 위 네 정책·계약 결정을 먼저 확정하는 것이다.

## 36. 2026-09-15 C8 corporate action lifecycle 계약 및 fail-closed 전이

### 변경 경계

- 가드 읽음. 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 라운드는
  `NA_SOURCE_CONTRACT_IMPLEMENTATION`, 방향 변경 없음이다.
- 백업: `backup/20260915_c8_corporate_action_lifecycle_contract/20260915_133902/` (14개 기존 파일).
- 변경 분류: 기업행위 source·상태 전이에 대한 정책·계약 구현이다.
- 실제 OpenDART 호출, raw·evidence·adapter output·canonical 생성, M2·후보·주문은 수행하지 않았다.

### 고정한 계약

- 범위는 `COMPANY_MERGER_ONLY`다. 분할합병·주식교환은 임의로 확장하지 않았다.
- canonical event key는 `corp_code + initial_rcept_no`다. 정정·취소·완료는 원문에서 최초 접수번호로
  연결되는 명시적 lineage가 없으면 `EVENT_IDENTITY_UNRESOLVED`로 차단한다.
- 상태 변경 지식 시점은 예정일이 아니라 공식 `rcept_dt`다. 장래 공시와 동일 일자 내 순서가 불명확한
  복수 공시는 차단한다.
- 최초 결정은 `PENDING`, 정정은 `PENDING` 유지, 취소는 접수일부터 `NONE`, 완료는 완료 공시 접수일에
  `EFFECTIVE`로 전환한다.
- `event_effective_date`는 연결된 완료 원문의 실제 날짜만 허용한다. 예정 합병기일은 raw 보존만 가능하고
  canonical 필드로 승격할 수 없다.
- `EFFECTIVE`는 완료 접수일 하루의 terminal marker이고 다음 날 `NONE`으로 복귀한다. 존속회사가 영구
  제외되지 않도록 하며 소멸회사는 listing/delisting 상태가 별도로 통제한다.
- history는 모든 base-universe 종목의 공식 corp code 일대일 매핑, `query_start <= earliest listed_date`,
  `query_end == selection_as_of`, 종목별 전체 pagination, `last_reprt_at=N`, 공식 원본 hash·provenance PASS를
  모두 요구한다. 하나라도 없으면 사건 부재를 `NONE`으로 기본화하지 않는다.

### 구현한 것

- 신규 계약:
  `paper/strategies/kospi_mcap_quarterly_v1/config/c8_corporate_action_lifecycle_contract_v1.json`.
- 신규 순수 변환기:
  `paper/strategies/kospi_mcap_quarterly_v1/src/c8_corporate_action_lifecycle.py`.
  사건 chain을 검증해 state reconstruction 호환 interval row를 만들며 reason이 하나라도 있으면 rows를
  전부 비워 fail-closed한다.
- 신규 교차검사 CLI:
  `tools/check_kospi_mcap_quarterly_c8_corporate_action_contract.py`.
- 공식 route에 `DART_CORPORATE_ACTIONS_COMPOSITE`를 등록했다. 필수 구성은 corpCode, list, document이고
  `cmpMgDecsn`은 2015년 제한 때문에 optional enrichment로만 둔다.
- manual readiness blocker는 `CORPORATE_ACTION_SOURCE_UNRESOLVED`에서
  `CORPORATE_ACTION_LIFECYCLE_ADAPTER_UNIMPLEMENTED`로 바뀌었다. 이는 차단 해제가 아니라 원인 단계가
  source 조사에서 adapter 구현으로 이동한 것이다.
- reconstruction은 `NONE/PENDING/EFFECTIVE`와 `NONE/MERGER`, blank/actual date 조합을 검증하고
  `EFFECTIVE` interval이 정확히 1일이 아니면 차단한다.
- adapter audit의 공식 source 평가는 `UNCONFIRMED_COMPLETE_SOURCE`에서
  `CONTRACT_DEFINED_ADAPTER_PENDING`으로 갱신했다. adapter ready 역할은 여전히 0이다.

### 직접 실행한 검증

- JSON 5개 parse PASS, Python 10개 py_compile PASS.
- 계약 교차검사 CLI: `status=PASS`,
  `verdict=C8_CORPORATE_ACTION_CONTRACT_DEFINED_ADAPTER_PENDING`, reason 0.
- manual readiness CLI: blocker는 `CORPORATE_ACTION_LIFECYCLE_ADAPTER_UNIMPLEMENTED` 1개,
  network/finalize/canonical/M2/candidate/orders 모두 false.
- adapter audit `--no-write`: `C8_ADAPTERS_BLOCKED`, adapter ready 0, build request false, M2 false,
  output 0.
- 정상, 정정 후 취소, 완료 1일 marker, lineage 누락, 동일일 순서 불명, pagination 누락,
  완료일 역전과 reconstruction 조합을 테스트했다.
- C8 전체 회귀 81개 PASS. 증거:
  `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_corporate_action_lifecycle_contract_pytest_latest.xml`.
- 변경 파일군 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존 비복구 행이다. 증거:
  `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_corporate_action_lifecycle_changed_files_mojibake_latest.json`,
  `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_corporate_action_lifecycle_contract_mojibake_latest.json`.
- `paper/orders_exec.xlsx`는 없다. fills는 장중 paper writer가 계속 갱신해 O6 불변성은 NA다.

### 단계 판정

- 기능: PASS. 계약과 순수 lifecycle 변환기가 정상·취소·완료 상태 전이를 만든다.
- 정합성: PASS. route·manual·acquisition·reconstruction 계약의 source ID와 상태 규칙이 일치한다.
- 운영 반영: NA. OpenDART adapter와 실제 raw가 없다.
- 정책: PASS. 네 정책 결정을 계약으로 명시했고 범위를 회사합병으로 제한했다.
- FAIL-CLOSED: PASS. identity·날짜·pagination·coverage 오류에서 interval output이 0이다.
- 회귀: PASS. C8 81개 PASS.
- O6 운영 원장 불변성: NA. 장중 동시 writer가 fills를 갱신했다.
- C8 실제 데이터 준비도: FAIL.
- manual collection readiness: BLOCKED.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- `DART_CORPORATE_ACTIONS_COMPOSITE` 실제 수집 adapter가 없다.
- corpCode exact mapping, 종목별 list pagination, document ZIP parsing, original receipt lineage와 완료 실제일
  추출을 실제 raw로 증명하지 못했다.
- 다음 단계는 현재 계약만 소비하는 격리된 OpenDART adapter를 구현하고 synthetic fixture와 실제 인증
  read-only capture를 분리 검증하는 것이다. adapter runtime PASS 전에는 blocker와 모든 downstream
  permission을 유지한다.

## 37. 2026-09-15 C8 OpenDART corporate action 격리 어댑터

### 변경 경계

- 가드 읽음. 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 라운드는
  `NA_SOURCE_ADAPTER_IMPLEMENTATION`, 방향 변경 없음이다.
- 백업: `backup/20260915_c8_corporate_action_adapter/20260915_140831/` (기존 파일 11개).
- 변경 분류: 고정된 lifecycle 계약만 소비하는 격리 OpenDART 어댑터와 read-only 연결 probe 구현이다.
- raw·acquisition evidence·canonical·M2·후보·target·주문은 생성하지 않았고 모든 downstream 권한은 false다.

### 구현한 것

- 신규 계약 `config/c8_corporate_action_adapter_contract_v1.json`에 corpCode/list/document endpoint,
  B001 결정·E003 종료 두 검색 series, `last_reprt_at=N`, page_count 100, 전 페이지·전 문서 완결성,
  원문 lineage·실제 합병기일 규칙을 고정했다.
- 신규 `src/c8_corporate_action_adapter.py`는 corpCode ZIP, list JSON, document ZIP을 파싱하고 모든 base code의
  일대일 corp code 매핑과 종목별 두 series 전 페이지를 확인한 뒤 기존 lifecycle interval 변환기로 넘긴다.
  누락·불일치 reason이 하나라도 있으면 interval rows를 전부 비운다.
- 신규 `tools/probe_kospi_mcap_quarterly_c8_corporate_action_adapter.py`는 명시적 `--live`가 있을 때만 한 종목,
  최대 31일 read-only 요청을 허용한다. 키와 raw는 저장하지 않고 status·HTTP·hash만 보고한다.
- lifecycle·route·manual readiness가 새 adapter contract ID와 세 endpoint를 교차검증한다.
- blocker를 `CORPORATE_ACTION_LIFECYCLE_ADAPTER_UNIMPLEMENTED`에서
  `CORPORATE_ACTION_ADAPTER_FULL_CAPTURE_EVIDENCE_MISSING`으로 바꿨다. 이는 차단 해제가 아니라 남은 원인을
  전 종목·전 기간 운영 증거로 좁힌 것이다.
- adapter audit 표시도 `ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING`으로 갱신했지만 role status는 BLOCKED다.

### 직접 실행한 검증

- JSON 4개 parse PASS, 변경 Python·테스트 py_compile PASS.
- synthetic 8개 테스트는 corpCode ZIP, status 013 empty history, 정정·취소·완료, 실제일 1일 marker,
  pagination 누락, lineage 누락, 비저장 probe를 검증해 PASS했다.
- 실제 OpenDART probe: `005930`, `20260901..20260915`; corpCode와 B001/E003 list가 HTTP 200,
  공시 0건, sample interval 0건, `status=PASS`다. key/raw 비저장, full capture false다. 증거:
  `reports/c8_corporate_action_adapter_probe_latest.json`.
- `--live` 없는 실행은 `LIVE_PROBE_NOT_EXPLICITLY_ENABLED`로 실패했다.
- 계약 교차검사: `C8_CORPORATE_ACTION_ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING`, reason 0.
- manual readiness: blocker는 `CORPORATE_ACTION_ADAPTER_FULL_CAPTURE_EVIDENCE_MISSING` 한 건이며
  network/finalize/canonical/M2/candidate/orders는 모두 false다.
- adapter audit `--no-write`: `C8_ADAPTERS_BLOCKED`, ready role 0, build request false, M2 false, output 0.
- 연결 예외는 예외문·키를 노출하지 않고 구성요소별 request failure로 변환한다.
- 집중 회귀 26개, C8 전체 12개 파일 90개 PASS. 증거:
  `reports/c8_corporate_action_adapter_pytest_latest.xml`, `reports/c8_full_pytest_latest.xml`.
- `paper/fills.csv`와 `paper/trades_calc.csv`의 최종 수정은 각각 13:38, 13:20으로 14:28 probe보다 앞선다.
  `paper/orders_exec.xlsx`는 없다. 주문 체인 E2E는 실행하지 않았다.

### 단계 판정

- 기능: PASS. 공식 응답 파싱과 lifecycle interval 변환을 synthetic 및 단일 live 연결로 확인했다.
- 정합성: PASS. adapter·lifecycle·route·manual 계약 ID와 endpoint가 교차 일치한다.
- 운영 반영: NA. 전 종목·전 기간 capture와 acquisition evidence 발행을 하지 않았다.
- 정책: PASS. 회사합병 범위와 fail-closed 의미를 유지했고 실행 권한을 열지 않았다.
- FAIL-CLOSED: PASS. 누락 page·document·lineage와 무명시 live 요청이 차단된다.
- 회귀: PASS. C8 90개 PASS.
- O6 운영 원장 불변성: NA. 시작 해시를 별도 고정하지 않았고 주문 체인을 실행하지 않았다.
- C8 실제 데이터 준비도: FAIL.
- manual collection readiness: BLOCKED.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- live probe는 한 종목·15일 연결 증거일 뿐 base universe 전체와 earliest-listed 이후 전 기간을 대표하지 않는다.
- 실제 합병 공시가 0건이어서 real document ZIP의 최초접수 lineage와 완료 실제일 추출은 아직 NA다.
- 다음 단계는 확정된 base-universe code 집합을 입력으로 전 종목·전 페이지·전 accepted document capture를
  acquisition evidence 규약에 맞게 수행하는 별도 단계다. 그 증거가 없으면 blocker와 downstream false를 유지한다.

## 38. 2026-09-15 C8 corporate action 전체 수집 preflight와 manifest

### 변경 경계

- 가드 읽음. 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 라운드는
  `NA_ACQUISITION_PIPELINE_IMPLEMENTATION`, 방향 변경 없음이다.
- 백업: `backup/20260915_c8_corporate_action_capture_preflight/20260915_143622/` (기존 파일 3개).
- 변경 분류: 전 종목 OpenDART 수집을 시작하기 전 base universe authority, 요청량, 재시작 지점을 검증하는
  fail-closed capture preflight 구현이다.
- 현재 2026-07-15 파일명 추정 universe는 `OFFICIAL_ASOF_AUTHORITY_MISSING`이므로 전체 수집 입력으로
  사용하지 않는다. 2026-09-15 같은 날 15:40 KST 이후의 승인된 KRX current snapshot이 없으면 manifest를
  만들지 않는다.
- 실제 OpenDART 대량 호출, raw·acquisition evidence·canonical·M2·후보·target·주문 생성은 수행하지 않는다.

### 구현 계획

1. 승인된 KRX `MDCSTAT019` same-day snapshot의 경로·SHA-256·capture timing·인증·HTTP 200을 모두 검증한다.
2. raw CSV에서 `KOSPI AND 주권 AND 보통주`를 정확히 선별하고 코드 중복·형식·상장일을 검증한다.
3. exact code set과 source hash로 결정적 manifest를 만들고 corpCode 1회 및 종목별 2개 list series의 최소
   요청량을 계산한다.
4. 성공 재실행은 idempotent, 성공 후 충돌은 reject, 실패 재시도는 허용하는 checkpoint 계약을 둔다.
5. base evidence가 없거나 stale·pre-close·hash mismatch면 `bulk_requests_executed=0`과 모든 downstream
   false를 확인한다.
6. 집중 테스트, C8 전체 회귀, 문자열 스캔, O6 산출물 비변경 확인 뒤 PASS/FAIL/NA를 기록한다.

### 구현한 것

- 신규 계약 `config/c8_corporate_action_capture_contract_v1.json`은 승인된 당일 KRX `MDCSTAT019`
  snapshot만 base universe 입력으로 허용한다. source identity, 인증 확인, HTTP 200, query, 정확한 raw 경로,
  SHA-256, size, nanosecond mtime, capture window, 15:40 KST 이후 조건을 모두 요구한다.
- 신규 `src/c8_corporate_action_capture.py`는 한국어 수동 CSV와 등록된 API CSV profile 중 정확히 하나를
  식별하고 `KOSPI AND 주권 AND 보통주`만 선별한다. 6자리 숫자 code, 중복 없음, 유효 상장일,
  최소 100종목을 통과해야 exact code set과 hash를 만든다.
- manifest는 `query_start=선별 종목의 earliest listed_date`, `query_end=selection_as_of`를 고정하고
  `corpCode 1회 + 종목별 B001/E003 첫 페이지`만 최소 호출량으로 계산한다. 추가 페이지와 accepted
  document 호출은 응답에서 동적으로 확장한다.
- checkpoint는 failed retry, identical replay no-op, successful result conflict reject를 적용한다.
  `dart_status=013`은 first page·0 page·0 row·0 receipt 조합에서만 무자료 PASS이고, `000`은 total page가
  1 이상이어야 한다. 이 구분 없이 빈 이력을 성공 처리하지 않는다.
- 신규 CLI `tools/prepare_kospi_mcap_quarterly_c8_corporate_action_capture.py`는 기본 실행에서 네트워크를
  호출하지 않는다. base evidence가 없으면 manifest도 만들지 않고 diagnostic report만 남긴다.
- adapter contract가 새 capture contract ID를 참조하도록 교차 연결했다. 기존 manual blocker와 모든
  downstream permission은 유지했다.

### 직접 실행한 검증

- JSON 계약 2개 parse PASS, Python 3개 cache-free compile PASS.
- 집중 테스트 10개 PASS. 정상 당일 입력, 15:40 이전, 7월 stale mtime, as-of 불일치, hash·mtime·경로
  변조, 중복·비정상·100개 미만 code, deterministic manifest, retry/idempotency/conflict, dynamic page,
  document, `000/013` 조합, namespace 밖 write 차단을 검증했다. 증거:
  `reports/c8_corporate_action_capture_preflight_focused_pytest_latest.xml`.
- C8 전체 13개 파일 100개 PASS. 증거:
  `reports/c8_corporate_action_capture_full_pytest_latest.xml`.
- 변경 코드·계약·ExecPlan 6개 파일의 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존
  비복구 행 `3623, 10662, 30751, 30803, 42294, 42390`이며 새 439구간에는 신규 issue가 없다. 증거:
  `reports/c8_corporate_action_capture_changed_files_mojibake_latest.json`,
  `reports/c8_corporate_action_capture_plans_mojibake_latest.json`.
- 2026-09-15 14:52 KST 현재 실제 CLI는 `BASE_UNIVERSE_ACQUISITION_EVIDENCE_MISSING`,
  `manifest_created=false`, `bulk_requests_executed=0`, `network_execution_authorized=false`다. 증거:
  `reports/c8_corporate_action_capture_status_latest.json`.
- corporate-action 계약 교차검사는 기존 verdict
  `C8_CORPORATE_ACTION_ADAPTER_IMPLEMENTED_FULL_CAPTURE_PENDING`을 유지했다.
- manual readiness는 `CORPORATE_ACTION_ADAPTER_FULL_CAPTURE_EVIDENCE_MISSING` 한 건을 유지하고
  adapter audit는 `C8_ADAPTERS_BLOCKED`, ready role 0, build request false, M2 false를 반환했다.
- 공식 OpenDART 공시검색 문서상 corp code가 없을 때만 검색기간이 3개월로 제한되고, page_count는 최대
  100이다. status 020은 일반적으로 20,000건 초과에서 발생하지만 실제 제한은 다를 수 있으므로 계약의
  수치는 실행 허가가 아닌 advisory로만 기록했다:
  `https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001`.
- `paper/fills.csv`, `paper/trades_calc.csv`의 mtime은 각각 13:38, 13:20으로 이번 구현보다 앞서며
  `paper/orders_exec.xlsx`는 없다. 주문 체인은 실행하지 않았다.

### 단계 판정

- 기능: PASS. 권위 있는 base 입력에서 exact manifest와 재시작 가능한 요청 순서를 만든다.
- 정합성: PASS. capture·adapter source ID와 series 순서, raw hash·code set·query window가 교차 일치한다.
- 운영 반영: NA. 15:40 이후 당일 KRX raw와 전 종목 OpenDART capture가 아직 없다.
- 정책: PASS. 기존 lifecycle·manual blocker·downstream false를 유지했다.
- FAIL-CLOSED: PASS. base evidence 부재·stale·pre-close·변조와 페이지 상태 오류에서 manifest 또는 완료
  판정을 차단한다.
- 회귀: PASS. C8 전체 100개 PASS.
- C8 실제 데이터 준비도: FAIL.
- corporate action full capture: BLOCKED.
- M2 착수: STOP.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 현재 14:52 KST로 당일 retrieval snapshot 허용 시각인 15:40 이전이다. 같은 날 15:40 이후 인증된 KRX
  `MDCSTAT019` raw와 capture evidence를 먼저 만들어야 한다.
- 그 입력이 PASS하면 exact code count와 최소 요청량이 처음 확정된다. 2026-07-15 추정 806종목을 조건부로
  대입하면 최소 1,613회지만 현재 universe 수치로 인용하거나 실행 근거로 사용하지 않는다.
- 그 다음 manifest의 default budget 1,000회 단위로 corpCode, 전 종목 두 series, 추가 page, accepted
  document를 checkpoint 재시작 방식으로 수집한다. 실제 complete report 전에는 acquisition evidence와
  canonical·M2를 열지 않는다.

## 39. 2026-09-15 C8 당일 acquisition session과 base raw capture [부분 진행]

### 변경 경계

- 가드 읽음. 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 방향 변경 없음이다.
- 백업: `backup/20260915_c8_acquisition_session_start/20260915_150529/` (계획 문서 2개).
- 15:05 KST 확인 결과 기존 acquisition session은 `ACQUISITION_SESSION_MISSING`이다.
- 현재 단계는 2026-09-15 capture session 시작시각을 먼저 봉인하는 데 한정한다.
- KRX `MDCSTAT019` raw는 15:40 KST 이전에 받지 않으며, acquisition finalize·full evidence·canonical·M2·후보·주문은 열지 않는다.

### 실행 순서

1. `selection_as_of=20260915` acquisition session을 공식 CLI로 시작한다.
2. session latest와 versioned JSON의 `ACTIVE`, 시작시각, downstream false를 원문 검증한다.
3. 15:40 KST 이후 인증된 KRX `MDCSTAT019` raw를 session window 안에서 확보한다.
4. listing-only capture evidence를 전 단계 preflight에 넣어 exact manifest를 생성한다.
5. 전 종목 OpenDART 수집과 full acquisition evidence는 별도 후속 단계로 유지한다.

### 진행된 것

- 공식 CLI로 `selection_as_of=20260915` session을 15:06:31 KST에 시작했다.
- latest와 versioned session JSON은 모두 `ACTIVE`, `source_count=0`, `sealed_at=null`이며 SHA-256
  `C2424DBE90BC9639B6A7CBF4B32A5968560F7F33C6D5FE7018B2D25CB6DA1286`으로 동일하다.
- 상태 CLI 재검증은 `C8_ACQUISITION_SESSION_ACTIVE`, reason 0, raw·request·evidence·adapter·canonical·M2·후보·target·주문 false다.
- 15:11 KST에 KRX 로그인 상태(`로그아웃` 표시)와 `MDCSTAT019` 전체시장 조회 결과를 확인했다.
- 15:43:05 KST에 전체시장 조회를 갱신하고, 15:44:41 KST에 CSV 다운로드를 완료했다. raw는
  `data/inbox/raw/krx_mdcstat019_all_20260915_154441.csv`, 394,765 bytes, CP949, 2,871행이며 SHA-256은
  `7ba37bc5423391509fca7818ebfcb72fbedcbbabad66793fb0de40fc2d211cba`다.
- 실측 선택 집합은 `KOSPI AND 주권 AND 보통주` 804개다. 공식 KRX 단축코드 중 `0120G0`, `0126Z0`,
  `0220W0` 세 개가 영문 대문자를 포함해 기존 숫자 6자리 validator가 `BASE_UNIVERSE_CODE_INVALID`로
  801개만 인정하는 구현 버그를 재현했다.
- 계약 버전 1.0.1에 `^[0-9A-Z]{6}$` 형식을 명시하고 validator와 실제 세 종목 회귀 fixture를 수정했다.
  이후 실제 raw preflight는 804개 전부를 보존해 manifest를 생성했다.
- manifest 최소 요청량은 `corpCode 1 + 804 * 2 = 1,609`, 기본 1,000회 budget 기준 최소 2회다.
- 집중 회귀 11개와 C8 전체 13개 파일 101개가 failures 0, errors 0으로 PASS했다. 증거:
  `reports/c8_alphanumeric_code_focused_pytest_20260915_1557.xml`,
  `reports/c8_alphanumeric_code_full_pytest_20260915_1555.xml`.
- `paper/fills.csv`, `paper/trades_calc.csv` mtime은 각각 13:38, 13:20으로 session 시작보다 앞서며 `paper/orders_exec.xlsx`는 없다.

### 단계 판정

- 기능: PASS. 당일 session, post-close raw, evidence descriptor, exact manifest가 작동한다.
- 정합성: PASS. 2,871 source행에서 804개를 선별했고 raw hash·size·mtime과 manifest code count가 일치한다.
- 운영 반영: PASS(base raw·manifest 범위). strategy namespace에 실제 raw와 manifest가 생성됐다.
- 정책: PASS. 선택 규칙을 바꾸지 않고 공식 KRX 유효 식별자를 보존하는 validator 버그만 수정했다.
- FAIL-CLOSED: PASS. 다음 단계는 `CORP_CODE_PACKAGE_PENDING`, network false, M2·주문 false다.
- 회귀: PASS. 집중 11개, C8 전체 101개 PASS이며 O6 mtime은 변경되지 않았다.
- base raw capture: PASS.
- exact manifest: PASS.
- full acquisition evidence: BLOCKED.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- base raw와 exact manifest는 확보됐다. 현재 실제 STOP은 `CORP_CODE_PACKAGE_PENDING`이다.
- OpenDART corpCode package에서 KRX 804개를 정확히 매핑해야 하며, 특히 영문 포함 세 코드가 직접 매핑되지
  않으면 별도 공식 식별자 연결 규칙 없이는 다음 list 호출을 열지 않는다.
- 현 계약은 `network_execution_authorized=false`이므로 실제 OpenDART 호출은 수행하지 않았다. 다음 단계는
  corpCode 1회 수집·SHA-256 봉인·804개 coverage 판정의 제한된 네트워크 실행 승인이다.
- full capture PASS 전에는 session finalize, acquisition evidence, canonical, M2, 후보·주문을 열지 않는다.

## 40. 2026-09-15 OpenDART corpCode package 1회 제한 수집 [완료: 승인 범위]

### 승인 및 변경 경계

- 사용자 승인: OpenDART `corpCode.xml` endpoint 1회 호출과 strategy raw 저장까지만 허용한다.
- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 방향 변경 없음, 성과 측정 없는 acquisition 단계다.
- 백업: `backup/20260915_c8_corp_code_one_call/20260915_160240/` (계획 문서 2개).
- 두 corporate-action series, document, manifest checkpoint 갱신, acquisition finalize, canonical, M2,
  후보·target·주문은 이번 범위에서 실행하지 않는다.
- API key는 `_cache/dart_api_key.txt`에서 읽되 값·URL query·로그·보고서에 기록하지 않는다.

### 실행 계획

1. 기존 adapter의 공식 endpoint와 `parse_corp_code_package`를 재사용한다.
2. dry-run에서 manifest 상태, API key 존재, output 충돌 여부를 확인하고 네트워크 호출은 0으로 유지한다.
3. 명시적 apply flag 두 개가 있을 때만 재시도 없는 GET 1회를 실행하고 응답 raw를 strategy namespace에
   immutable 파일로 저장한다.
4. HTTP status, payload size·SHA-256, package parser 상태와 manifest 804개 matched/missing/duplicate를
   보고서에 기록한다. credential 값은 금지한다.
5. coverage가 불완전해도 추가 호출이나 자동 식별자 보정은 하지 않고 STOP으로 남긴다.
6. 문법·fixture·실제 1회 실행·raw/report·O6 비변경을 검증한 뒤 단계 판정을 기록한다.

### 실제 실행 결과

- dry-run은 `C8_CORP_CODE_PACKAGE_ONE_CALL_READY`, request 0, network false, manifest update false다.
- 16:08:48 KST에 OpenDART `corpCode.xml`을 redirect·retry 없이 정확히 1회 호출했다. HTTP 200,
  server Date `Tue, 15 Sep 2026 07:08:47 GMT`, 응답 완료 16:08:50 KST다.
- raw는 `data/inbox/raw/opendart_corp_code_20260915T160850+0900.zip`, 3,614,110 bytes, SHA-256
  `47cb3e114aba7b7f4319a5d3ca10b0d5230d9bd4c3edde4d7a66ae66ba296fac`다.
- package ZIP integrity와 parser는 PASS했고 유효 stock_code-corp_code 매핑은 3,990개다.
- exact manifest 804개는 matched 804, missing 0이다. 영문 포함 코드는 `0120G0=01965333`,
  `0126Z0=01965324`, `0220W0=02042949`로 모두 직접 매핑됐다.
- 보고서: `reports/c8_corp_code_package_capture_20260915160850.json`,
  `reports/c8_corp_code_package_capture_latest.json`.
- 승인 범위대로 manifest checkpoint는 갱신하지 않았다. 실행 전후 manifest SHA-256은
  `F1FB117ADDF4B4631F2649A859484FDA95575E423A827C57C504E591FC17E931`로 동일하다.
- 재실행 가드가 부족한 것을 사후 점검에서 발견해 latest report의 동일 selection 1회 실행을 확인하면
  다음 실행을 request 0으로 차단하도록 보완했다. 실제 재검증은
  `CORP_CODE_CAPTURE_ALREADY_EXECUTED_FOR_SELECTION`, request 0이다.
- API key 값은 raw·보고서에 없고 길이 40의 구성 여부만 확인했다.

### 직접 검증

- 신규 Python 2개 cache-free compile PASS.
- 단일 호출 도구 집중 4개 PASS. dry-run no-call, apply exact-one-call, mapping gap no-extra-call,
  기존 attempt·latest report 재실행 차단을 검증했다. 증거:
  `reports/c8_corp_code_replay_guard_pytest_20260915_1612.xml`.
- C8 전체 14개 파일 105개 PASS, failures 0, errors 0. 증거:
  `reports/c8_corp_code_one_call_full_pytest_20260915_1613.xml`.
- `paper/fills.csv`, `paper/trades_calc.csv`의 mtime은 13:38, 13:20으로 유지됐고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정과 다음 진행

- 기능: PASS. 승인된 1회 수집과 raw·보고서 저장이 작동했다.
- 정합성: PASS. raw hash·ZIP·parser와 804/804 mapping coverage가 일치한다.
- 운영 반영: PASS(corpCode raw capture 범위). 실제 strategy namespace에 raw와 보고서가 생성됐다.
- 정책: PASS. 승인 범위 1회만 실행했고 series·document·manifest checkpoint는 변경하지 않았다.
- FAIL-CLOSED: PASS. 동일 selection 재실행은 request 0으로 차단된다.
- 회귀: PASS. 집중 4개, C8 전체 105개 PASS, O6 비변경이다.
- corpCode raw capture: PASS.
- corpCode manifest checkpoint: 미적용.
- full acquisition evidence: BLOCKED.
- 전체로직: 미적용.
- 다음 단계는 저장된 raw를 다시 호출하지 않고 parse해 `record_corp_code_package`로 manifest checkpoint에
  804개 mapping·hash·row count를 반영하는 오프라인 작업이다. 이 단계 전까지 list series 호출은 금지한다.

## 41. 2026-09-15 corpCode raw 오프라인 checkpoint 반영 [완료: 승인 범위]

### 승인 및 변경 경계

- 사용자 승인: 이미 저장된 OpenDART corpCode raw를 네트워크 요청 없이 재검증하고 현재 exact manifest의
  `checkpoint.corp_code_package`에 mapping·payload hash·package row count를 반영한다.
- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 방향 변경 없음, 성과 측정 없는 acquisition
  checkpoint 단계다.
- 백업: `backup/20260915_c8_corp_code_checkpoint/20260915_162615/` (manifest·status·계획 문서 4개).
- 이번 단계의 네트워크 요청은 0회다. list series·document·acquisition finalize·canonical·M2·후보·target·
  주문은 실행하지 않는다.

### 실행 계획

1. capture report의 승인된 1회 호출·HTTP 200·coverage PASS와 raw 상대경로를 확인한다.
2. raw가 strategy namespace 안에 있는지 확인하고 실제 size·SHA-256을 capture report와 대조한다.
3. raw를 다시 parse해 package 전체 mapping 수와 현재 manifest 804개 exact coverage·one-to-one을 검증한다.
4. `record_corp_code_package`로 메모리상 checkpoint를 구성하고 `evaluate_capture_manifest`의 다음 상태가
   `CORPORATE_ACTION_REQUESTS_PENDING`인지 dry-run에서 확인한다.
5. 명시적 apply·offline confirm이 함께 있을 때만 manifest와 capture status를 atomic write한다.
6. idempotent replay, 변조·coverage gap·확인 flag 누락 차단, 전체 C8 회귀와 O6 비변경을 검증한다.

### 실제 실행 결과

- 신규 `tools/apply_kospi_mcap_quarterly_c8_corp_code_checkpoint.py`는 dry-run 기본이며
  `--apply --confirm-offline-checkpoint`를 함께 지정해야만 manifest와 capture status를 쓴다. HTTP 모듈을
  사용하지 않고 `network_request_count=0`을 고정한다.
- dry-run은 `C8_CORP_CODE_CHECKPOINT_READY`, raw 3,614,110 bytes, package mapping 3,990개, current manifest
  matched 804·missing 0, checkpoint attempt `0 -> 1`을 메모리에서 확인했다.
- 16:33:41 KST 적용은 `C8_CORP_CODE_CHECKPOINT_APPLIED`다. manifest checkpoint에는 PASS attempt 1건,
  payload SHA-256 `47cb3e114aba7b7f4319a5d3ca10b0d5230d9bd4c3edde4d7a66ae66ba296fac`, package row
  count 3,990, exact mapping 804개가 기록됐다.
- 영문 포함 코드는 `0120G0=01965333`, `0126Z0=01965324`, `0220W0=02042949`로 저장 원문에서 다시
  확인했다.
- manifest SHA-256은 적용 전
  `F1FB117ADDF4B4631F2649A859484FDA95575E423A827C57C504E591FC17E931`, 적용 후
  `2D84AC3450015B50974461B62649264A691B7C0658DA11A6EAE10F9D1B5D8F72`다.
- capture status는 `C8_CORPORATE_ACTION_CAPTURE_PENDING`, reason
  `CORPORATE_ACTION_REQUESTS_PENDING`, `bulk_requests_executed=1`이다. evaluator가 노출하는 다음 요청은
  100개이며 첫 요청은 `000020 / MERGER_DECISION_HISTORY / page 1`이다. 최소 요청량 1,609 중 corpCode
  1회를 제외한 최소 잔여 요청은 1,608회다.
- 동일 apply 재실행은 `C8_CORP_CODE_CHECKPOINT_ALREADY_APPLIED`, attempt `1 -> 1`, manifest·status update
  false, manifest 전후 hash 동일, network request 0으로 no-op 됐다.
- 상세 증거:
  `reports/c8_corp_code_checkpoint_apply_20260915163341.json`,
  `reports/c8_corp_code_checkpoint_apply_latest.json`,
  `reports/c8_corporate_action_capture_status_20260915163341.json`,
  `reports/c8_corporate_action_capture_status_latest.json`.

### 직접 검증과 판정

- 신규 Python 2개 cache-free compile PASS. 집중 테스트 7개 PASS, C8 전체 15개 파일 112개 PASS다.
  증거: `reports/c8_corp_code_checkpoint_focused_pytest_20260915_1650.xml`,
  `reports/c8_corp_code_checkpoint_full_pytest_20260915_1651.xml`.
- 신규 코드·테스트·ExecPlan 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존 비복구 행
  `3623, 10662, 30751, 30803, 42294, 42390`이며 새 443 구간에는 신규 issue가 없다. 증거:
  `reports/c8_corp_code_checkpoint_changed_files_mojibake_20260915_1644.json`,
  `reports/c8_corp_code_checkpoint_plans_mojibake_20260915_1645.json`.
- 기능: PASS. 저장 raw에서 exact checkpoint와 다음 요청 상태가 생성됐다.
- 정합성: PASS. raw size·hash·parser 3,990개와 current manifest 804/804, one-to-one mapping이 일치한다.
- 운영 반영: PASS(corpCode checkpoint 범위). 실제 manifest와 capture status가 갱신됐다.
- 정책: PASS. 추가 네트워크·series·document·downstream 실행 없이 승인 범위만 반영했다.
- FAIL-CLOSED: PASS. hash·size 변조, coverage gap, downstream-open report, 시간대 없는 capture 시각,
  확인 flag 누락을 차단했다.
- 회귀: PASS. C8 112개 PASS이며 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 적용 전후 동일하고
  `paper/orders_exec.xlsx`는 없다.
- corpCode manifest checkpoint: PASS.
- full corporate-action capture: BLOCKED.
- acquisition evidence·canonical·M2·후보·target·주문: 미적용.
- 전체로직: 미적용.

### 남은 실제 문제와 다음 진행

- 현재 실제 STOP은 804개 종목별 두 series의 첫 페이지 최소 1,608회와 응답에 따른 추가 page·accepted
  document 수집이 없다는 점이다.
- 다음 단계는 별도 네트워크 승인과 호출 budget을 고정한 뒤 checkpoint 재시작 방식으로 list series를
  수집하는 것이다. 이번 승인에는 포함되지 않으므로 실행하지 않는다.
- full capture와 acquisition evidence PASS 전에는 canonical·M2·후보·주문을 열지 않는다.

## 42. 2026-09-15 OpenDART list-page checkpoint 수집기 [부분 완료: 구현]

### 지시사항과 변경 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 탐색/확증 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해` 지시는 corpCode checkpoint 다음 단계인 두 corporate-action list series 수집 경로의
  구현과 dry-run 검증으로 적용한다.
- 백업: `backup/20260915_c8_list_page_collector/20260915_164545/` (계획 문서·manifest·status 4개).
- 계약의 기본 호출 상한은 실행당 1,000회지만 `network_collection_allowed=false`이므로 실제 list 호출은
  이번 범위에서 수행하지 않는다. 실제 호출은 `list pages only`, 최대 요청 수를 다시 명시 승인받은 뒤 한다.
- document·acquisition finalize·canonical·M2·후보·target·주문은 열지 않는다.

### 구현 계획

1. current manifest의 integrity, exact corp mapping PASS, pending kind가 LIST_PAGE인지 확인한다.
2. 기존 request builder·response parser·checkpoint recorder를 재사용하고 API key는 요청 메모리에서만 결합한다.
3. dry-run 기본, apply·list-only confirm 동시 요구, max requests는 1..1,000 범위로 제한한다.
4. 각 응답 raw를 strategy namespace에 immutable 저장하고, 같은 응답 hash를 manifest에 기록한 뒤 atomic
   write가 성공해야 다음 요청으로 진행한다.
5. 매 요청 후 pending을 다시 계산해 first page의 `total_page`가 만든 추가 page를 순서대로 포함한다.
6. HTTP 오류, request exception, parser FAIL, `020`, identity·pagination 오류에서 해당 FAIL checkpoint를
   남기고 즉시 중단한다. document pending을 만나면 호출 없이 list-stage 완료로 중단한다.
7. credential·query key를 raw 이름·보고서·로그에 남기지 않고, 재실행은 기존 PASS checkpoint를 건너뛴다.
8. fixture, current dry-run, C8 전체 회귀, 문자열 검사와 O6 불변성을 검증한다.

### 구현한 것

- 신규 `tools/collect_kospi_mcap_quarterly_c8_list_pages.py`는 dry-run 기본이며
  `--apply --confirm-list-pages-only`를 함께 요구한다. 요청 budget은 계약 기본값 1,000이며 1..1,000 밖의
  값은 실행 전에 차단한다.
- 기존 `build_list_request_spec`, `parse_disclosure_list_page`, `record_list_page`,
  `next_pending_requests`, `evaluate_capture_manifest`를 그대로 연결했다. 별도 series 의미나 판정 규칙을
  만들지 않았다.
- 성공 응답은 payload hash가 포함된 immutable raw와 request별 PASS recovery journal을 먼저 저장한 뒤
  manifest를 atomic write한다. journal과 manifest 사이에 중단되면 다음 실행에서 raw hash·parser·request
  identity를 다시 검증해 네트워크 재호출 없이 checkpoint를 복구한다.
- manifest는 매 요청 후 저장하지만 versioned capture status는 실행 종료 시 한 번만 쓴다. 1,000개 중간
  보고서 생성을 피하면서 요청별 재시작 지점은 보존한다.
- `020`, HTTP 오류, transport exception, empty response, parser·identity·pagination 실패는 FAIL attempt를
  기록하고 즉시 중단한다. document pending을 만나면 네트워크를 호출하지 않고 scope 밖으로 종료한다.
- 공식 OpenDART 문서는 `020`을 요청 제한 초과로 정의하고 일반적으로 20,000건 이상이지만 실제 제한은
  다를 수 있다고 명시한다. 이 수치는 호출 허가가 아니라 hard-stop 근거로만 사용한다:
  `https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2026002`.

### 직접 실행한 검증

- 신규 Python 2개 cache-free compile PASS. 집중 테스트 8개 PASS다. dry-run no-call, contract order,
  dynamic page 우선순위, status 020 hard-stop, identity mismatch, confirmation·budget, PASS skip, recovery
  journal을 검증했다. 증거: `reports/c8_list_page_collector_focused_pytest_20260915_1702.xml`.
- C8 전체 16개 파일 120개 PASS, failures 0, errors 0이다. 증거:
  `reports/c8_list_page_collector_full_pytest_20260915_1704.xml`.
- 신규 수집기·테스트·ExecPlan 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존
  비복구 행이며 새 444 구간에 신규 issue가 없다. 증거:
  `reports/c8_list_page_collector_changed_files_mojibake_20260915_1710.json`,
  `reports/c8_list_page_collector_plans_mojibake_20260915_1711.json`.
- current dry-run은 `C8_LIST_PAGE_COLLECTION_READY`, reason 0, `selection_as_of=20260915`, query
  `19560303..20260915`, default/max request 1,000이다. 첫 pending은
  `000020 / 00119195 / MERGER_DECISION_HISTORY / page 1`이다.
- current dry-run의 network request 0, manifest/status update false이며 실행 전후 두 파일 hash가 같다.
  실제 strategy namespace의 `opendart_list_*` raw는 0개, recovery journal과 collection latest report도 없다.
- `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`로 유지됐고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정과 남은 문제

- 기능: PASS. list-only checkpoint 수집·중단·복구 흐름을 fixture에서 실행했다.
- 정합성: PASS. current manifest query, exact corp mapping, contract series·순서와 request spec이 일치한다.
- 운영 반영: NA. 실제 OpenDART list 요청과 실제 raw·checkpoint 생성은 수행하지 않았다.
- 정책: PASS. contract의 network false를 유지했고 document·downstream을 열지 않았다.
- FAIL-CLOSED: PASS. budget·confirmation·020·응답·identity·journal 변조 경계를 차단한다.
- 회귀: PASS. C8 120개 PASS, O6와 current manifest/status 비변경이다.
- list collector 구현: PASS.
- 실제 list capture: 미실행.
- full corporate-action capture: BLOCKED.
- 전체로직: 미적용.
- 다음 단계는 actual `list pages only`와 실행당 최대 요청 수를 명시 승인받아 실행하는 것이다. 계약 기본
  1,000회를 그대로 사용할지 더 작은 1차 batch를 쓸지는 실행 전에 사용자 결정으로 고정한다.

## 43. 2026-09-15 OpenDART list-page 1차 batch 최대 1,000회 [완료: 승인 범위]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자가 실제 네트워크 실행을 승인했다. 이번 승인 범위는 OpenDART corporate-action `list pages only`,
  실행당 최대 1,000회다.
- document 원문 호출, acquisition evidence finalize, canonical, M2, 후보, target, 주문은 승인 범위에서 제외한다.
- 실행 전 백업은 `backup/20260915_c8_list_page_batch_1000/20260915_170004/`에 생성했다. active ExecPlan,
  `.agent/PLANS.md`, current manifest/status, O6 기준 해시를 포함한다.
- 실행 전 current manifest SHA-256은
  `2D84AC3450015B50974461B62649264A691B7C0658DA11A6EAE10F9D1B5D8F72`, status SHA-256은
  `745650EB86D9B229D5582784AB25AE52E44F5EBEA9A86607B1DE7097BAC342CF`다.

### 실행 절차와 중단 기준

1. actual 실행 직전 dry-run으로 manifest integrity, exact corp mapping, query window, 첫 pending, network 0을 재확인한다.
2. `tools/collect_kospi_mcap_quarterly_c8_list_pages.py --apply --confirm-list-pages-only --max-requests 1000`을
   공식 실행 경로로 사용한다.
3. 각 성공 응답은 immutable raw와 recovery journal을 먼저 남긴 뒤 manifest checkpoint에 반영한다.
4. OpenDART status `020`, HTTP 오류, transport exception, empty response, parser·identity·pagination 실패 중
   하나라도 발생하면 즉시 중단하고 자동 재시도하지 않는다.
5. 다음 pending kind가 DOCUMENT가 되면 문서 호출 없이 승인 범위 종료로 판정한다.
6. 실행 후 actual request 수, PASS/FAIL, DART/HTTP status, 마지막 완료 요청, 다음 pending, manifest/status,
   raw/journal 수를 확인한다.
7. replay dry-run, 집중·전체 C8 회귀, O6 해시 불변성, 문자열 검사를 수행하고 6개 검증축으로 판정한다.

### 예정 산출물

- `data/inbox/raw/opendart_list_*`
- `data/acquisition/checkpoints/c8_list_page_response_journal/*.json`
- `data/acquisition/checkpoints/c8_corporate_action_capture_manifest_latest.json`
- `reports/c8_corporate_action_capture_status_latest.json`
- `reports/c8_list_page_collection_latest.json`

### 실제 실행 결과

- 실행 전 dry-run은 `C8_LIST_PAGE_COLLECTION_READY`, network 0, reason 0이었다. 첫 pending은
  `000020 / 00119195 / MERGER_DECISION_HISTORY / page 1`, query는 `19560303..20260915`였다.
- 17:02:55~17:13:04 KST 실제 list-only batch를 실행했다. network 1,000회, PASS 1,000, FAIL 0,
  HTTP 200 1,000건, DART `000` 1,000건, hard-stop false다. 승인 budget 도달로 종료됐으며 verdict는
  `C8_LIST_PAGE_BUDGET_EXHAUSTED_CHECKPOINT_SAVED`다.
- series별 완료 page는 `MERGER_DECISION_HISTORY` 647개, `MERGER_COMPLETION_HISTORY` 353개다.
  accepted row 합계와 고유 receipt는 모두 786건이다.
- raw·recovery journal·manifest list checkpoint는 각각 1,000개다. 1,000건 전수 raw SHA-256,
  journal payload hash, manifest payload hash, 요청 identity, pagination, PASS·HTTP·DART 상태를 대조했고
  오류는 0건이다.
- 804개 종목·2개 series 중 287개 종목, 574개 series를 시작했다. 두 series가 모두 완료된 종목은
  286개, 완료된 series는 573개다. 완료 page는 first page 574개와 additional page 426개다.
- 현재까지 발견된 total_page와 미조회 series의 최소 page 1을 합산하면 최소 필요 list page는 2,035개,
  완료 1,000개, 최소 잔여 1,035개다. 아직 미조회 first page에서 additional page가 발견되면 늘 수 있다.
- next pending은 `008600 / 00104999 / MERGER_COMPLETION_HISTORY / page 2`다. replay dry-run은 이
  checkpoint를 첫 pending으로 반환했고 network 0, manifest/status update false였다.
- capture status는 `BLOCKED / C8_CORPORATE_ACTION_CAPTURE_PENDING`, reason
  `CORPORATE_ACTION_REQUESTS_PENDING`, manifest integrity PASS다. corpCode 1회와 list 1,000회를 합친
  bulk requests executed는 1,001이다.
- 고유 accepted receipt 786건의 document 호출은 이번 범위에서 수행하지 않았다. full capture,
  acquisition evidence, canonical, M2, 후보, target, 주문은 모두 false다.
- 증거:
  `reports/c8_list_page_collection_20260915170255490661.json`,
  `reports/c8_list_page_collection_latest.json`,
  `reports/c8_corporate_action_capture_status_20260915171304.json`,
  `reports/c8_corporate_action_capture_status_latest.json`.

### 검증과 불변성

- cache-free compile 2개 PASS다. 일반 `py_compile`은 기존 `tests/__pycache__` ACL 때문에 `.pyc` 쓰기에서
  실패했으며, 소스 문법은 cache-free compile과 pytest import 실행으로 확인했다.
- 집중 회귀 8개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_collector_focused_pytest_20260915_1720.xml`.
- C8 전체 16개 파일 120개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_collector_full_pytest_20260915_1721.xml`.
- 수집기·테스트·ExecPlan 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존
  비복구 행 3623, 10662, 30751, 30803, 42294, 42390이며 새 445 구간의 신규 issue는 없다. 증거:
  `reports/c8_list_page_batch_1000_changed_files_mojibake_final_20260915_1729.json`,
  `reports/c8_list_page_batch_1000_plans_mojibake_final_20260915_1730.json`.
- 실행 후 manifest SHA-256은
  `FFF6F5B7507605F99C0981926B2C31AE81C16E6201D5B063382FCE92AD9ABE1D`, capture status는
  `C3ACD48363E3DEB98494AF4C1E6CA3CBEDED41177AD86A95776B5EBE880D5237`, collection latest는
  `8AA8FACA2010EAA3BE595C7EAECDDC86BDC2233625F5E0FD4D978EC6252281FF`다.
- O6 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`로 실행 전과 같고
  `paper/orders_exec.xlsx`는 없다.

### 단계 판정과 남은 문제

- 기능: PASS. 승인된 list-only 1,000회가 checkpoint로 저장됐다.
- 정합성: PASS. 실제 1,000건 raw·journal·manifest 전수 대조 오류 0이다.
- 운영 반영: PASS(1차 list-page checkpoint 범위). current manifest/status/report가 실제 갱신됐다.
- 정책: PASS. 승인 budget과 list-only 경계를 지켰고 document·downstream은 실행하지 않았다.
- FAIL-CLOSED: PASS. 실제 오류는 없었고 020·HTTP·transport·parser·identity·pagination 중단 경로는
  집중 회귀로 확인했다.
- 회귀: PASS. 집중 8개, C8 전체 120개 PASS이며 O6는 불변이다.
- 1차 list-page batch: 완료(승인 범위).
- full corporate-action capture: BLOCKED. list page가 최소 1,035개 남았고 이후 document도 미수집이다.
- acquisition evidence·canonical·M2·후보·target·주문·전체로직: 미적용.
- 다음 실제 네트워크 단계는 별도 승인으로 list-only 2차 batch budget을 다시 고정해야 한다.

## 44. 2026-09-15 OpenDART list-page 2차 batch 최대 1,000회 [완료: 승인 범위, recovery 적용]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `다음 진행해`는 직전 문서의 다음 단계인 corporate-action `list pages only` 2차 batch 실행으로
  적용한다. 계약 상한과 1차 batch 기준을 유지해 이번 실행 상한은 1,000회로 고정한다.
- document 원문 호출, acquisition evidence finalize, canonical, M2, 후보, target, 주문은 계속 제외한다.
- 실행 전 백업은 `backup/20260915_c8_list_page_batch_2_1000/20260915_173125/`에 생성했다. active
  ExecPlan, `.agent/PLANS.md`, current manifest/status, 1차 collection latest, mojibake latest, O6 기준 해시를
  포함하며 7개 파일 존재를 확인했다.
- 실행 전 manifest SHA-256은
  `FFF6F5B7507605F99C0981926B2C31AE81C16E6201D5B063382FCE92AD9ABE1D`, status SHA-256은
  `C3ACD48363E3DEB98494AF4C1E6CA3CBEDED41177AD86A95776B5EBE880D5237`다.
- 실행 전 raw·journal은 각각 1,000개, full capture는 false다. next pending은
  `008600 / 00104999 / MERGER_COMPLETION_HISTORY / page 2`다.

### 실행 절차와 중단 기준

1. `--max-requests 1000` dry-run으로 manifest integrity, next pending, network 0, 비변경을 재확인한다.
2. `tools/collect_kospi_mcap_quarterly_c8_list_pages.py --apply --confirm-list-pages-only --max-requests 1000`을
   실행한다.
3. PASS checkpoint는 건너뛰고 current next pending부터 raw·journal·manifest를 요청별로 봉인한다.
4. OpenDART status `020`, HTTP 오류, transport exception, empty response, parser·identity·pagination 실패 중
   하나라도 발생하면 즉시 중단하고 자동 재시도하지 않는다.
5. DOCUMENT pending에 도달하면 문서 호출 없이 list-only 범위 종료로 판정한다.
6. 실행 후 2차·누적 요청 수, PASS/FAIL, DART/HTTP 상태, completed code·series·page, accepted receipt,
   next pending, raw·journal·manifest 정합성을 확인한다.
7. replay dry-run, cache-free compile, 집중·전체 C8 회귀, O6 불변성, 문자열 검사를 수행한다.

### 현재 판정

- 백업: PASS.
- 실행 전 current manifest/status integrity: PASS.
- 2차 actual list-page batch: 승인 budget 1,000회 완료.
- document·downstream: 미적용 유지.
- 전체로직: 미적용.

### 중간 장애와 복구 결정

- 2차 첫 실행은 network 819번째 응답의 raw·PASS journal을 저장한 뒤 manifest atomic replace에서
  `PermissionError: [WinError 5]`로 중단됐다. 자동 재시도하지 않았다.
- 중단 직후 누적 raw·journal은 각각 1,819개, current manifest list checkpoint는 1,818개로 정확히
  한 건 차이다. latest collection/status는 1차 시점 그대로라 2차 성공으로 표시되지 않았다.
- 마지막 request는 `058430 / 00155258 / MERGER_COMPLETION_HISTORY / page 1`이다. journal HTTP 200,
  DART `000`, PASS이고 raw SHA-256과 journal SHA-256은
  `f438810fc56d9e991c1dbc8aeaf4889055dcf75e971d3b63e3692aa7b1eca997`로 일치한다.
- 실패 임시 manifest는 정상 JSON이며 list checkpoint 1,819개와 마지막 request를 포함한다. 진단 증거로
  `backup/20260915_c8_list_page_batch_2_1000/20260915_173125/failed_manifest_tmp_pid17632.json`에 보존했다.
- 대상과 임시 파일의 속성·owner·ACL은 동일하다. 현재 대상 manifest exclusive open PASS, 같은 폴더의
  별도 atomic replace probe PASS, probe cleanup PASS다. 지속 권한 손상은 재현되지 않았고 순간 파일 점유가
  가장 타당한 해석이지만 점유 주체는 확인되지 않아 원인은 확정하지 않는다.
- 기존 수집기의 PASS journal recovery는 raw hash·parser·request identity를 다시 확인하고 네트워크 호출 없이
  누락 checkpoint 한 건을 manifest에 복구한다.
- 승인된 2차 network budget 1,000회 중 819회를 사용했다. recovery는 network 0회이므로 재개 명령은
  `--max-requests 181`로 제한한다. 이후 실제 network는 최대 181회이며 2차 누적은 최대 1,000회를 넘지 않는다.
- recovery manifest write 또는 후속 atomic replace가 다시 실패하면 즉시 중단하고 추가 재개하지 않는다.

### 실제 실행 결과

- 첫 실행은 network 819회까지 진행한 뒤 위 `WinError 5`로 중단됐다. raw·journal 1,819개와 manifest
  1,818개를 확인해 마지막 PASS journal 한 건이 manifest보다 앞선 상태임을 확인했다.
- current exclusive open과 별도 atomic replace probe가 PASS한 뒤 `--max-requests 181`로 수동 재개했다.
  recovery journal 1건이 network 0회로 manifest에 먼저 반영됐고, 이후 network 181회를 실행했다.
- 2차 누적 network는 `819 + 181 = 1,000`회로 승인 상한과 정확히 같다. 실패한 마지막 응답은 다시
  요청하지 않았다.
- 2차 page 1,000개는 decision 603개, completion 397개다. HTTP 200 1,000건, DART `000` 994건,
  정상 무자료 `013` 6건이며 모두 PASS다. `013` 6건은 page 1, total_page 0, row 0, receipt 0 계약을
  만족한다.
- 2차에서 본 고유 receipt와 기존 대비 신규 고유 receipt는 모두 949건이다.
- 누적 list checkpoint·raw·journal은 각각 2,000개다. 누적 2,000건의 raw hash, journal·manifest hash,
  request identity, pagination, HTTP·DART 상태를 전수 대조했고 오류는 0건이다.
- 누적 page는 decision 1,250개, completion 750개다. 804개 중 618종목을 시작했고 617종목의 두 series를
  완료했다. 시작·완료된 series는 1,235개, first page 1,235개, additional page 765개다.
- 현재까지 발견된 total_page와 미조회 series 최소 page 1 기준 최소 필요 list page는 2,373개다.
  2,000개 완료, 최소 373개가 남았으며 미조회 first page에서 additional page가 발견되면 늘 수 있다.
- 누적 고유 accepted receipt는 1,735건이다. next pending은
  `092780 / 00117726 / MERGER_COMPLETION_HISTORY / page 1`이다.
- replay dry-run은 위 next pending, network 0, manifest/status update false를 확인했다.
- 결합 검증 증거:
  `reports/c8_list_page_batch_2_verification_20260915_1754.json`, SHA-256
  `B453E316ACB54F8AE1BAC8AC07F3C90D98D08B16A3C5D3E3E10E8947162FC526`.

### 검증과 불변성

- cache-free compile 2개 PASS다.
- 집중 회귀 8개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_batch_2_focused_pytest_20260915_1750.xml`.
- C8 전체 16개 파일 120개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_batch_2_full_pytest_20260915_1751.xml`.
- ExecPlan·결합 검증 JSON 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존 비복구 행
  3623, 10662, 30751, 30803, 42294, 42390이며 새 446 구간의 신규 issue는 없다. 증거:
  `reports/c8_list_page_batch_2_changed_files_mojibake_final_20260915_1800.json`,
  `reports/c8_list_page_batch_2_plans_mojibake_final_20260915_1801.json`.
- 실행 후 manifest/status/collection latest SHA-256은 각각
  `0F7A192F2A9CF0D71B7A40F34B50A010CC26DD7B20FE2391D8F43ADBD91B0147`,
  `351DB28A91D8C32E478A74D74BE4CBE9E43FA6A740927B66198023AAED07B433`,
  `7E7A9CD5F414D1FE57AEA6CCC7BA7531A84F9C2034E74681868F447B02DA334B`다.
- O6 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`로 실행 전과 같고
  `paper/orders_exec.xlsx`는 없다.
- 실패 `.tmp`는 현재 manifest가 1,819개 checkpoint를 모두 포함하고 백업 hash가 일치함을 확인한 뒤
  정리했다. 운영 경로 temp는 0개이며 백업 사본은 유지한다.

### 단계 판정과 남은 문제

- 기능: PASS. 승인된 2차 list-only 1,000회를 recovery 포함해 저장했다.
- 정합성: PASS. 누적 2,000건 전수 대조 오류 0이다.
- 운영 반영: PASS(2차 list-page checkpoint 범위). current manifest/status/report가 갱신됐다.
- 정책: PASS. 2차 network 1,000회 상한과 list-only 경계를 지켰고 document·downstream은 실행하지 않았다.
- FAIL-CLOSED: PASS. 파일 교체 실패 시 프로세스가 중단됐고 current manifest는 직전 정상본을 유지했으며,
  journal 복구에서 네트워크 재요청이나 downstream 개방이 없었다.
- 회귀: PASS. 집중 8개, C8 전체 120개 PASS이며 O6는 불변이다.
- 2차 list-page batch: 완료(승인 범위, recovery 적용).
- full corporate-action capture: BLOCKED. list page가 최소 373개 남았고 document도 미수집이다.
- acquisition evidence·canonical·M2·후보·target·주문·전체로직: 미적용.
- 남은 결함: manifest replace의 순간 점유 주체는 미확인이다. 예외가 최상위까지 전파돼 첫 실행의 FAIL
  collection/status 보고서가 생성되지 않은 관측성 문제도 남아 있다. 데이터 복구는 완료됐지만 원인 해결
  완료로 판정하지 않는다.
- 다음 네트워크 단계는 별도 승인으로 list-only 3차 batch budget을 고정해야 한다. 현재 확인된 최소 잔여는
  373회지만 additional page가 동적으로 늘 수 있다.

## 45. 2026-09-15 OpenDART list-page 3차 batch 최대 1,000회 [완료: 목록 단계]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 직전 문서의 다음 단계인 corporate-action `list pages only` 3차 batch 실행 승인으로
  적용한다. 미조회 first page에서 additional page가 늘 수 있어 계약 상한 1,000회로 고정한다.
- list 단계가 먼저 끝나 DOCUMENT pending에 도달하면 즉시 종료한다. document 원문 호출, acquisition
  evidence finalize, canonical, M2, 후보, target, 주문은 제외한다.
- 실행 전 백업은 `backup/20260915_c8_list_page_batch_3_1000/20260915_175905/`에 생성했다. active
  ExecPlan, `.agent/PLANS.md`, current manifest/status, collection latest, 2차 결합 검증, mojibake latest,
  O6 기준 해시를 포함하며 8개 파일 존재를 확인했다.
- 실행 전 manifest/status SHA-256은
  `0F7A192F2A9CF0D71B7A40F34B50A010CC26DD7B20FE2391D8F43ADBD91B0147`,
  `351DB28A91D8C32E478A74D74BE4CBE9E43FA6A740927B66198023AAED07B433`다.
- 실행 전 list checkpoint·raw·journal은 각각 2,000개, temp 0개다. 현재 발견 기준 최소 잔여 list page는
  373개이고 next pending은 `092780 / 00117726 / MERGER_COMPLETION_HISTORY / page 1`이다.

### 실행 절차와 중단 기준

1. `--max-requests 1000` dry-run으로 manifest integrity, next pending, network 0, 비변경을 확인한다.
2. `tools/collect_kospi_mcap_quarterly_c8_list_pages.py --apply --confirm-list-pages-only --max-requests 1000`을
   실행한다.
3. OpenDART status `020`, HTTP·transport·empty response, parser·identity·pagination 실패가 발생하면 즉시
   중단하고 자동 재시도하지 않는다.
4. manifest atomic replace `WinError 5`가 재발하면 raw·journal·manifest gap과 temp를 확인하고 즉시
   중단한다. 이번 실행 안에서 추가 재개하지 않는다.
5. DOCUMENT pending에 도달하면 문서 호출 없이 `C8_LIST_PAGE_STAGE_COMPLETE_DOCUMENTS_PENDING`으로
   종료한다.
6. 실행 후 3차·누적 호출 수, PASS/FAIL, DART/HTTP 상태, 완료 종목·series·page, accepted receipt,
   next pending과 list stage 완료 여부를 확인한다.
7. 누적 raw·journal·manifest 전수 정합성, replay dry-run, cache-free compile, 집중·전체 C8 회귀,
   O6 불변성, 문자열 검사를 수행한다.

### 현재 판정

- 백업: PASS.
- 실행 전 current manifest/status integrity: PASS.
- 3차 actual list-page batch: 378회 PASS로 목록 단계 완료.
- document·downstream: 미적용 유지.
- 전체로직: 미적용.

### 실제 실행 결과

- actual 실행은 network 378회, PASS 378, FAIL 0, hard-stop false다. HTTP 200은 378건,
  DART `000`은 360건, 정상 무자료 `013`은 18건이다.
- 3차 page는 decision 189개, completion 189개다. 3차 신규 고유 receipt는 413건이다.
- 누적 list checkpoint·raw·journal은 각각 2,378개다. 누적 HTTP 200은 2,378건, DART `000`은
  2,354건, `013`은 24건이다.
- 804종목의 1,608개 code-series가 모두 완료됐다. first page 1,608개와 동적 additional page 770개를
  합친 최소 필요 list page는 2,378개이며 잔여 list page는 0개다.
- 누적 고유 accepted receipt는 2,148건이다. next pending은 list page가 아니라 DOCUMENT
  `receipt_no=20041020000158`이다.
- list-only 수집기는 DOCUMENT를 호출하지 않고
  `C8_LIST_PAGE_STAGE_COMPLETE_DOCUMENTS_PENDING / DOCUMENT_REQUESTS_OUTSIDE_SCOPE`로 종료했다.
- replay dry-run은 같은 DOCUMENT pending에서 network 0, manifest/status update false로 종료했다.
- 누적 2,378건의 raw SHA-256, journal·manifest 내용, request identity, pagination, HTTP·DART 계약을
  전수 대조했고 오류는 0건이다. 운영 checkpoint temp도 0개다.
- capture status는 `BLOCKED / C8_CORPORATE_ACTION_CAPTURE_PENDING`, reason
  `CORPORATE_ACTION_REQUESTS_PENDING`, full capture false, M2 false다. acquisition evidence·canonical·후보·
  target·주문은 모두 false다.
- 결합 검증 증거:
  `reports/c8_list_page_batch_3_verification_20260915_1813.json`, SHA-256
  `003200A843B3ED8EF8AF0BCD8BA7A231AC3FCFE12F0D823C6858E278EFD4BE83`.

### 검증과 불변성

- cache-free compile 2개 PASS다.
- 집중 회귀 8개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_batch_3_focused_pytest_20260915_1810.xml`.
- C8 전체 16개 파일 120개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_list_page_batch_3_full_pytest_20260915_1811.xml`.
- ExecPlan·결합 검증 JSON 문자열 dry-run은 issue 0이다. `.agent/PLANS.md` 전체 6건은 기존 비복구 행
  3623, 10662, 30751, 30803, 42294, 42390이며 새 447 구간의 신규 issue는 없다. 증거:
  `reports/c8_list_page_batch_3_changed_files_mojibake_final_20260915_1819.json`,
  `reports/c8_list_page_batch_3_plans_mojibake_final_20260915_1820.json`.
- O6 `paper/fills.csv`, `paper/trades_calc.csv` SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`로 실행 전과 같고
  `paper/orders_exec.xlsx`는 없다.
- 실행 후 manifest/status/collection latest SHA-256은 각각
  `B1B540FE69BFCE6A86E2DA13B069719103689FAA68BD37950F6F70585E60F295`,
  `ED775FAB410B14D5E9199975B65F43D78A16978390779A511861891722ECCC11`,
  `E21952CD9BC7ECE12E73EB71EF32D7EACCAB7B2DFCA7B1FFF795006AD7D9C507`다.

### 단계 판정과 남은 문제

- 기능: PASS. 3차 승인 범위 378건을 저장하고 목록 끝에서 중단했다.
- 정합성: PASS. 누적 2,378건 전수 대조 오류 0이다.
- 운영 반영: PASS(목록 checkpoint 범위). current manifest/status/report가 실제 갱신됐다.
- 정책: PASS. 최대 1,000회 안에서 목록 종료 즉시 멈췄고 document·downstream을 실행하지 않았다.
- FAIL-CLOSED: PASS. DOCUMENT 경계에서 호출을 차단했고 집중 회귀의 오류·identity·pagination 중단 경로도
  통과했다.
- 회귀: PASS. 집중 8개, C8 전체 120개 PASS이며 O6는 불변이다.
- list-page stage: 완료.
- full corporate-action capture: BLOCKED. 고유 DOCUMENT 2,148건이 미수집이다.
- acquisition evidence·canonical·M2·후보·target·주문·전체로직: 미적용.
- 이전 2차의 manifest replace 순간 점유 주체와 예외 시 FAIL report 미생성 관측성 결함은 이번 3차에서
  재발하지 않았지만 원인 해결 완료로 판정하지 않는다.
- 다음 네트워크 단계는 document 수집의 별도 승인 범위·요청 budget·중단 기준을 먼저 고정해야 한다.

## 46. 2026-09-15 OpenDART document 전용 수집기 준비 [부분 적용: dry-run 검증]

### 지시사항과 고정 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 목록 완료 후 다음 단계인 document 전용 수집기 준비로 적용한다.
- 이번 범위는 수집 계약, 회차 상한, 재시작 checkpoint, hard-stop 구현과 dry-run 검증까지다.
- 실제 document 네트워크 호출, acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.
- 기존 capture contract의 `default_max_requests_per_run=1000`과 hard stop `020`을 그대로 사용한다.
- 구현 파일은 신규 `tools/collect_kospi_mcap_quarterly_c8_documents.py`와 신규 집중 테스트로 제한한다.
  기존 계약, capture·adapter 본체, list collector, O6 파일은 수정하지 않는다.
- 기존 파일 백업은
  `backup/20260915_c8_document_collector_prepare/20260915_204447/`에 9개 파일로 생성했다.
  신규 수집기·테스트는 선행 파일이 없어 backup NA다.

### 구현 절차와 즉시 중단 조건

1. 현재 manifest integrity PASS와 첫 pending `DOCUMENT`를 확인한 경우에만 준비 상태를 연다.
2. dry-run은 API key·budget·manifest를 검사하되 네트워크와 checkpoint·report 쓰기를 모두 금지한다.
3. apply는 `--confirm-documents-only`와 `1..1000` 요청 budget을 동시에 요구한다.
4. 성공 응답은 original package 원문과 PASS journal을 불변 경로에 먼저 저장한 뒤 manifest를 갱신한다.
5. 재시작 시 PASS journal의 request identity, 원문 경로, SHA-256, package member 수를 다시 확인한 뒤
   네트워크 0회로 checkpoint를 복구한다.
6. HTTP 오류, transport exception, empty response, DART 오류 status, invalid·empty document package,
   journal 불일치, manifest write 실패 중 하나라도 발생하면 즉시 중단한다.
7. DART `020`은 `DART_REQUEST_LIMIT_EXCEEDED`로 분리하고 자동 재시도하지 않는다.
8. 결과 보고서는 document collector 범위만 기록하며 downstream 상태는 계속 false로 둔다.

### 종료 조건

- 신규 소스 원문 재확인, cache-free compile, dry-run network 0·비변경, 집중 회귀, C8 전체 회귀,
  O6 불변성, 문자열 검사가 모두 증거로 남아야 준비 완료로 판정한다.
- 이번 단계는 실제 document 수집 완료가 아니라 **수집기 준비 완료**까지만 판정한다.

### 구현 및 dry-run 결과

- 신규 `tools/collect_kospi_mcap_quarterly_c8_documents.py`를 추가했다. 실제 writer는 기존
  `record_document`와 `write_capture_manifest`를 사용하며, 운영 로직과 별도 실행 파일로 격리했다.
- 성공 응답은 `opendart_document_<receipt>_<timestamp>_<hash>.zip` 원문과 receipt별 PASS journal을
  먼저 봉인한 뒤 manifest에 반영한다. 재시작은 두 파일의 identity·SHA-256·member 수를 재검증한다.
- HTTP·transport·empty·invalid package·DART error·journal 불일치·manifest write 실패는 첫 건에서
  즉시 중단한다. `020`은 `DART_REQUEST_LIMIT_EXCEEDED`로 분리한다.
- 신규 집중 테스트 9개는 위 정상·차단·재시작 경로를 포함한다.
- current manifest dry-run은 `READY / C8_DOCUMENT_COLLECTION_READY`, budget 1,000, 첫 pending
  `DOCUMENT / 20041020000158`, network 0이다. manifest·capture status SHA-256은 변하지 않았고
  `c8_document_collection_latest.json`도 생성되지 않았다.
- 현재 조건은 목록에서 확인한 고유 receipt 2,148건, document PASS 0건, pending 2,148건이다.
- 구체적 최악 조건으로 정상 원문·PASS journal 저장 뒤 manifest 쓰기 `PermissionError`를 주입했다.
  결과는 `FAIL / C8_DOCUMENT_MANIFEST_WRITE_FAILED / MANIFEST_WRITE_FAILED`, 첫 receipt pending 유지,
  같은 실행 안 네트워크 재요청 없음, 실패 collection report 생성 PASS다.
- cache-free compile 2개, 집중 회귀 9개, C8 전체 회귀 129개 PASS다. 증거:
  `reports/c8_document_collector_focused_pytest_20260915_2050.xml`,
  `reports/c8_document_collector_full_pytest_20260915_2052.xml`.
- 결합 검증 증거:
  `reports/c8_document_collector_preparation_verification_20260915_2051.json`, SHA-256
  `5D3F821A7711110BC621CF1DA76A3A17965FE751C1CED5F3F7D4B4DC63BDD7FC`.
- 수집기·테스트·ExecPlan·결합 JSON 문자열 검사는 issue 0이다. `.agent/PLANS.md` 6건은 기존 비복구
  행 3623, 10662, 30751, 30803, 42294, 42390이며 새 448 구간 issue는 없다. 증거:
  `reports/c8_document_collector_changed_files_mojibake_20260915_2055.json`,
  `reports/c8_document_collector_plans_mojibake_20260915_2056.json`.

### 단계 판정

- 기능: PASS. 정상 수집·checkpoint·재시작 동작을 실행 검증했다.
- 정합성: PASS. request·raw·journal·manifest 계약과 해시 검증 경로를 확인했다.
- 운영 반영: NA. current manifest 대상 dry-run만 했고 실제 document 네트워크 호출은 하지 않았다.
- 정책: PASS. 최대 1,000회, 명시 확인 플래그, document-only와 downstream false를 유지한다.
- FAIL-CLOSED: PASS. `020`, HTTP, 빈·손상 package, journal·manifest 쓰기 실패가 즉시 중단된다.
- 회귀: PASS. 집중 9개와 C8 전체 129개가 통과했다.
- 따라서 수집기 구현과 dry-run 검증은 됐지만 실제 document 수집 및 전체 capture 완료는 아니다.
- 다음 단계는 actual 1차 document-only batch를 실행하기 전에 동일 budget 1,000과 즉시 중단 조건을
  다시 고정하고 승인 범위를 기록하는 것이다.

## 47. 2026-09-15 OpenDART document-only 1차 batch 최대 1,000회 [중단: 입력 범위 결함 발견]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 document-only 실제 1차 batch 최대 1,000회 실행 승인으로 적용한다.
- 최초 1회 실제 호출을 먼저 수행해 응답 package·raw·journal·manifest를 검증하고, PASS일 때만 잔여
  999회를 실행한다. 두 실행의 실제 network 합계는 1,000회를 넘지 않는다.
- HTTP·transport·empty response·DART 오류·invalid package·journal 불일치·manifest write 실패가
  발생하면 첫 건에서 즉시 중단하고 같은 실행 안 자동 재시도하지 않는다.
- `020`은 요청 한도 초과로 분리해 잔여 실행을 금지한다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 이번 범위에서 실행하지 않는다.
- 실행 전 백업은
  `backup/20260915_c8_document_batch_1_1000/20260915_205528/`에 9개 파일로 생성했다.

### 실행 전 기준

- current manifest/status SHA-256은 각각
  `B1B540FE69BFCE6A86E2DA13B069719103689FAA68BD37950F6F70585E60F295`,
  `ED775FAB410B14D5E9199975B65F43D78A16978390779A511861891722ECCC11`다.
- 목록 checkpoint는 2,378건, document checkpoint·raw·journal은 각각 0건이다.
- 고유 accepted receipt와 pending document는 2,148건이며 첫 pending은 `20041020000158`이다.
- O6 fills·trades SHA-256은
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`이고
  `paper/orders_exec.xlsx`는 없다.

### 종료 조건

1. probe 1건이 PASS이고 raw·journal·manifest hash와 package member 계약이 일치해야 잔여 999회를 연다.
2. actual 합계, PASS/FAIL, HTTP·DART 상태, package member 분포, recovery 수를 확인한다.
3. 누적 document raw·journal·manifest를 전수 대조하고 replay dry-run에서 network 0을 확인한다.
4. cache-free compile, 집중·C8 전체 회귀, O6 불변성, 문자열 검사를 수행한다.
5. 1,000건이 PASS여도 잔여 document가 있으면 full capture와 downstream은 BLOCKED로 유지한다.

### 실제 실행과 중단 결과

- probe 1건은 HTTP 200, DART `000`, ZIP member 1건으로 PASS했다.
- 잔여 999회 실행은 network 572회에서 첫 FAIL을 만나 즉시 중단했다. 해당 실행은 PASS 571,
  FAIL 1이며 probe 포함 누적 actual network 573회, document PASS 572, FAIL 1이다.
- 실패 receipt는 `20150720000298`, 응답은 DART `014 / 파일이 존재하지 않습니다`였다. 자동 재시도와
  후속 문서 호출은 하지 않았다.
- 원 목록 행이 `[기재정정]합병등종료보고서(분할)`임을 확인해 단순 API 오류로 재시도하지 않고
  accepted receipt 범위 결함을 후속 48절에서 수정했다.
- 따라서 1차 batch 자체는 목표 1,000회에 도달하지 못해 완료가 아니며, FAIL-CLOSED 중단은 정상이다.

## 48. 2026-09-15 E003 합병 종료보고서 정확 필터와 checkpoint migration [완료: 결함 수정 범위]

### 문제와 판단

- document 1차 batch는 probe 1건 PASS 뒤 잔여 실행에서 network 572회, PASS 571건, FAIL 1건으로
  첫 오류에서 중단했다. probe 포함 누적 document는 PASS 572건, FAIL 1건이다.
- 실패 receipt `20150720000298`은 OpenDART `014 / 파일이 존재하지 않습니다`이며 원 목록 행은
  `[기재정정]합병등종료보고서(분할)`이다.
- `E003` 목록에서 내부 부분문자열 `합병등종료보고서`를 사용해 합병 외 자산양수도·분할·영업양수도·
  주식의 포괄적 교환·이전 보고서까지 accepted receipt에 포함한 것이 원인이다.
- 전수 재계산 결과 기존 accepted receipt 2,148건은 decision 1,029건과 completion 1,119건이며 중복은
  0건이다. completion 중 정확한 합병은 471건, 비합병은 648건이다. 수정 후 대상은 1,500건이다.
- 기존 document PASS 572건 중 수정 후 유지 대상은 354건, 비합병 제외 대상은 218건이다. FAIL 1건도
  비합병 제외 대상이다. 수정 후 pending은 1,146건이다.
- 이는 Gate나 거래 정책 완화가 아니라 선언된 merger-only 원천 범위를 복구하는 결함 수정이다.

### 수정 경계와 불변조건

1. completion 보고서는 revision prefix 제거 후 `합병등종료보고서(합병)`과 정확히 일치할 때만 수용한다.
2. 현재 필터 계약의 결정적 SHA-256을 manifest에 저장하고 평가 때 대조한다.
3. 기존 2,378개 list raw·구 journal과 573개 document raw·journal은 삭제·변경하지 않는다.
4. 모든 list raw를 새 필터로 재파싱해 versioned journal namespace를 만들고 manifest를 원자 교체한다.
5. 새 accepted set에 포함되는 기존 document PASS 354건만 manifest에 이관한다. 제외된 218 PASS와
   1 FAIL은 원본 증거로 보존하되 전략 입력과 pending에서 제외한다.
6. migration은 dry-run 기본, 명시 확인 플래그가 있는 apply만 허용하며 네트워크 호출은 0회다.
7. acquisition evidence, canonical, M2, 후보, target, 주문은 계속 금지한다.

### 백업과 종료 조건

- 백업: `backup/20260915_c8_merger_completion_filter_fix/20260915_210956/`.
- 계약·adapter·capture·list collector·current manifest·테스트·ExecPlan·PLANS 11개 파일의 원본/백업
  SHA-256 일치를 확인했다.
- 종료조건은 raw 2,378개 전수 해시·identity 재검증, 새 eligible 1,500건, 기존 PASS 354건 재사용,
  pending 1,146건, manifest integrity PASS, document dry-run 첫 pending 변경, cache-free compile,
  집중·C8 전체 회귀, O6 불변, 문자열 검사다.

### 구현과 운영 반영

- adapter contract를 `1.0.1`로 올리고 completion은 revision prefix 제거 후
  `합병등종료보고서(합병)` 정확 일치만 수용하도록 변경했다. decision 필터 의미는 유지했다.
- capture manifest `1.0.1`에 `accepted_report_filter_sha256`을 추가하고 현재 계약과 다르면
  `CAPTURE_MANIFEST_REPORT_FILTER_HASH_MISMATCH`로 FAIL-CLOSED하게 했다.
- list collector의 복구 journal namespace를
  `c8_list_page_response_journal_merger_only_v1`으로 분리했다.
- 신규 `tools/migrate_kospi_mcap_quarterly_c8_merger_only_filter.py`는 네트워크 없이 구 raw·journal을
  전수 재생하고 새 journal과 corrected manifest를 만든다. 재실행 시 새 journal을 전수 비교하고
  manifest와 이력을 변경하지 않는 멱등 경로를 포함한다.
- actual migration은 PASS, network 0, 새 journal 2,378개, current manifest atomic update PASS다.
  구 journal 2,378개와 raw·document 증거는 그대로 보존했다.
- corrected manifest는 accepted 1,500건, document PASS 354건, pending 1,146건이다. 비합병 receipt
  648건, 그중 기존 document PASS 218건과 FAIL 1건은 전략 checkpoint에서 제외됐다.
- current manifest는 `manifest_version=1.0.1`, filter hash
  `1268334e0142ba27d164f11c55ba0cb3624a80dd6be9666747c1db505debbe10`, integrity PASS다.
- document collector dry-run은 network 0, 첫 pending `20150730000093`, manifest/status 비변경이다.
  list collector dry-run도 DOCUMENT 경계에서 network 0으로 종료했다.
- 멱등 재실행은 `C8_MERGER_ONLY_FILTER_ALREADY_APPLIED_PASS`, migrated journal integrity true,
  manifest update false, 새 journal write 0, network 0이다.

### 검증과 판정

- cache-free compile 7개 PASS, 집중 회귀 22개 PASS다.
- C8 전체 18개 파일 131개 PASS, failures·errors·skipped 0이다. 증거:
  `reports/c8_merger_only_filter_full_pytest_20260915_2124.xml`, SHA-256
  `CD75DFDA85B78E76DCF1016205473557BB5D8A0D6354C34A03D231C8F24F7C86`.
- migration·멱등성 증거:
  `reports/c8_merger_only_filter_migration_latest.json`.
- 변경 파일 문자열 dry-run은 files 9, issues 0이다. 증거:
  `reports/c8_merger_only_filter_changed_files_mojibake_20260915_2127.json`.
- O6 fills·trades SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`로 기준과 같고
  `paper/orders_exec.xlsx`는 없다. checkpoint temp는 0개다.
- 기능 PASS, 정합성 PASS, 운영 반영 PASS(checkpoint migration 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- **결함 수정 범위는 완료**다. full corporate-action capture는 문서 1,146건이 남아 BLOCKED이며,
  acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.
- 다음 단계는 corrected pending을 대상으로 document-only 수집을 재개하되 별도 실행 회차와 budget을
  고정하는 것이다.

## 49. 2026-09-15 corrected document-only 2차 batch 최대 1,000회 [중단: 유효 공시 문서 014]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 merger-only migration 이후 corrected pending document 수집 재개 승인으로 적용한다.
- 이번 회차는 최대 network 1,000회이며 첫 HTTP·transport·empty·DART 오류, invalid package,
  journal 불일치, manifest write 실패에서 즉시 중단하고 자동 재시도하지 않는다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- current manifest/status/document collection latest SHA-256은 각각
  `4D730FD83A184E5E362D0F8C69367374124A133E696B9FB55064057C61151E73`,
  `584282EE3B24E4DFA5ADBCD84D2CD71A67FF5427F5947A3A9D2EAEB0FDF19921`,
  `C2C458DDD5EFDA5AF1A7E2DFDD1789FCF3F286F504145CC48E6EC3ED0C8922C1`이다.
- eligible document checkpoint는 PASS 354건, pending 1,146건이고 첫 pending은 `20150730000093`이다.
- 운영 document journal은 572개, raw는 573개다. 이 중 비합병 제외 증거도 삭제하지 않고 보존한다.
- dry-run은 `C8_DOCUMENT_COLLECTION_READY`, network 0, manifest/status 비변경이다.
- 백업은 `backup/20260915_c8_document_batch_2_1000/20260915_213211/`에 6개 파일로 생성했고
  source/backup SHA-256 일치를 확인했다.
- O6 fills·trades SHA-256은
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`이며 orders_exec는 없다.

### 종료 조건

1. actual network, PASS/FAIL, DART·HTTP, package member, recovered journal 수를 확인한다.
2. 누적 eligible PASS와 pending, 전체 raw·PASS journal 수를 분리한다.
3. manifest integrity, document dry-run 재실행, raw·journal·manifest 정합성, temp 0을 확인한다.
4. cache-free compile, 집중·C8 전체 회귀, O6 불변, 문자열 검사를 수행한다.
5. 잔여 문서가 있으면 full capture와 downstream은 BLOCKED로 유지한다.

### 실제 실행과 중단 결과

- 기본 sandbox 실행 1회는 네트워크 차단으로 HTTP 0에서 실패했다. 외부 OpenDART 응답은 아니며
  actual 외부 요청 수에 포함하지 않는다.
- 승인된 외부 호출은 probe 1회와 잔여 실행 10회로 합계 11회다. PASS 10건, FAIL 1건이며 첫 오류에서
  즉시 중단했다.
- 실패 receipt는 `20151214000216`, OpenDART 응답은 HTTP 200,
  `014 / 파일이 존재하지 않습니다`다. 해당 응답 XML 원문은 해시와 함께 보존됐다.
- 원 목록을 재검증한 결과 이 receipt는 `012160 / 영흥 / [기재정정]주요사항보고서(회사합병결정)`이며,
  같은 공시 흐름에 선행 `20151204000180`과 후행 정정본 `20151217000199`가 존재한다. 따라서 입력 범위
  오류로 제외할 수 없고, 동일 receipt 재시도만으로는 다음 문서 수집이 진행되지 않는다.
- 2차 batch는 최대 1,000회에 도달하지 못했으므로 완료가 아니다. acquisition evidence 이후 단계는
  계속 미실행이다.

## 50. 2026-09-15 OpenDART document 014 unavailable checkpoint [완료: 결함 수정 범위]

### 문제와 정책 판단

- 유효한 merger-only receipt의 과거 원문이 OpenDART에서 `014`로 제공되지 않을 수 있다.
- 이를 accepted 대상에서 제외하거나 PASS로 간주하면 원천 범위와 완결성 의미가 훼손된다.
- 반대로 FAIL을 계속 pending으로 두면 동일 receipt가 전체 수집 순회를 영구 정지시킨다.
- 따라서 exact HTTP 200·DART `014` 원문을 해시 검증한 경우에만 `UNAVAILABLE` 종료 checkpoint로
  기록한다. 이는 순회에서는 건너뛰지만 full capture와 downstream은 명시적으로 BLOCKED한다.
- 거래·Gate·LOCK·후보·주문 정책은 변경하지 않는다. acquisition checkpoint 운용 결함 수정이다.

### 구현 경계

1. capture contract를 `1.0.2`로 올리고 `UNAVAILABLE`의 허용 조건과 완료 차단 의미를 명시한다.
2. document writer는 `UNAVAILABLE`을 `package_member_count=0`,
   `error_code=DART_DOCUMENT_STATUS_014`일 때만 허용한다.
3. pending 순회는 최신 PASS 또는 UNAVAILABLE receipt를 건너뛴다.
4. evaluator는 UNAVAILABLE 개수를 보고하고 manifest integrity는 유지하되
   `CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE`로 full capture를 BLOCKED한다.
5. collector는 014 raw XML·hash·request identity를 immutable journal로 봉인한다. 새 014는 checkpoint 저장 후
   현재 실행을 hard-stop하고, 다음 실행부터 후속 receipt를 처리한다.
6. 현재 manifest의 기존 FAIL 014는 저장된 raw XML과 hash를 재검증해 네트워크 0으로 UNAVAILABLE로
   복구한다. 다른 FAIL은 자동 변환하지 않는다.
7. acquisition evidence, canonical, M2, 후보, target, 주문은 계속 실행하지 않는다.

### 백업과 종료 조건

- 백업: `backup/20260915_c8_document_014_unavailable_checkpoint/20260915_213611/`.
- 계약·capture·collector·테스트·current manifest/status/report·ExecPlan·PLANS 10개 파일의 원본/백업
  SHA-256 일치를 확인했다.
- 종료 조건은 원문 재확인, cache-free compile, 집중·C8 전체 회귀, 현재 014의 network 0 복구,
  manifest/status/report 정합성, O6 불변, 문자열 검사다.

### 구현 및 운영 반영

- capture contract를 `1.0.2`로 올리고 exact HTTP 200·OpenDART `014`만 UNAVAILABLE로 허용하는 조건과
  full capture 차단 의미를 명시했다.
- `record_document`는 `package_member_count=0`, `error_code=DART_DOCUMENT_STATUS_014` 조합만
  UNAVAILABLE로 수용한다. PASS와 UNAVAILABLE 이후 충돌 재기록은 거부한다.
- pending 순회는 PASS·UNAVAILABLE을 종료 상태로 건너뛴다. evaluator는 UNAVAILABLE receipt를 별도
  집계하고 manifest integrity는 유지하되 full capture와 downstream을 BLOCKED한다.
- collector는 014 raw XML의 SHA-256·receipt·status를 검증한 immutable unavailable journal을 기록한다.
  새 014 응답은 현재 실행을 즉시 중단하며 다음 실행부터 후속 receipt로 진행한다.
- 현재 FAIL `20151214000216`은 기존 raw XML을 network 0으로 재검증해 UNAVAILABLE로 복구했다.
  이어진 실제 외부 요청 1건 `20151216000322`는 HTTP 200, DART `000`, member 1로 PASS했다.
- 재실행 dry-run은 network 0, manifest/status/document report hash 비변경, 다음 pending
  `20151216000326`으로 PASS했다.

### 검증 결과

- current accepted 1,500건, latest document PASS 365건, UNAVAILABLE 1건, FAIL 0건, pending 1,134건이다.
- current evaluator는 `manifest_integrity_pass=true`, status BLOCKED, verdict
  `C8_CORPORATE_ACTION_CAPTURE_DOCUMENTS_UNAVAILABLE`, 이유는
  `CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE`과 `CORPORATE_ACTION_REQUESTS_PENDING`이다.
- unavailable journal 1개와 raw XML SHA-256
  `03f4385d883fd756de28e791dde6f153baad5771898af36fcd2f07b479a84a8f` 일치, PASS journal 583개,
  document raw 585개, checkpoint temp 0개다.
- cache-free syntax·JSON PASS, 집중 회귀 24개 PASS, C8 전체 회귀 135개 PASS다. JUnit:
  `reports/c8_document_014_focused_pytest_20260915_2150.xml`,
  `reports/c8_document_014_full_pytest_20260915_2151.xml`.
- 변경 소스·계약·테스트·ExecPlan 6개 문자열 검사는 issue 0이다. PLANS의 6건은 기존 비복구 행
  3623, 10662, 30751, 30803, 42294, 42390이며 새 451 구간 문제는 없다.
- O6 fills·trades SHA-256은 기준과 같고 orders_exec는 없다.

### 단계 판정과 잔여 문제

- 기능 PASS, 정합성 PASS, 운영 반영 PASS(014 checkpoint·후속 1건 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- 한 누락 문서가 전체 수집 순회를 영구 정지시키는 결함 수정은 완료다.
- full capture는 UNAVAILABLE 1건과 pending 1,134건 때문에 BLOCKED이며 acquisition evidence,
  canonical, M2, 후보, target, 주문 및 전체로직은 미적용이다.
- 다음 단계는 별도 document-only 회차로 pending 수집을 재개하고, 모든 pending 종료 후 UNAVAILABLE
  gap 처리 가능성을 별도 정책 검증한다.

## 51. 2026-09-15 corrected document-only 3차 batch 최대 1,000회 [중단: 새 document 014]

### 지시사항과 실행 경계

- 가드 읽음. 현재 고정 목표는 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드는 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 014 checkpoint 수정 후 pending document 수집 재개 승인으로 적용한다.
- 이번 회차는 최대 network 1,000회이며 첫 HTTP·transport·empty·DART 오류, invalid package,
  journal 불일치, manifest write 실패에서 즉시 중단하고 자동 재시도하지 않는다.
- 새 OpenDART `014`는 UNAVAILABLE checkpoint를 저장한 뒤 현재 실행을 즉시 중단한다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- accepted 1,500건, document PASS 365건, UNAVAILABLE 1건, FAIL 0건, pending 1,134건이며
  첫 pending은 `20151216000326`이다.
- manifest integrity true, capture status BLOCKED, full capture false다.
- backup은 `backup/20260915_c8_document_batch_3_1000/20260915_215350/`에 5개 파일로 생성했고
  source/backup SHA-256 일치를 확인했다.
- 실행 전 O6 기준 SHA-256은 fills
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`, trades
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`이며 orders_exec는 없다.

### 종료 조건

1. actual network, PASS/FAIL/UNAVAILABLE, HTTP·DART 상태와 package member를 확인한다.
2. 누적 document 상태, pending, raw·PASS journal·UNAVAILABLE journal 수를 검증한다.
3. manifest integrity, checkpoint journal hash, 후속 dry-run network 0과 temp 0을 확인한다.
4. C8 집중·전체 회귀, O6 불변, 문자열 검사를 수행한다.
5. pending 또는 UNAVAILABLE이 남으면 full capture와 downstream은 BLOCKED로 유지한다.

### 실제 실행과 정상 중단

- actual network 2회에서 첫 오류로 중단했다. PASS 1건, UNAVAILABLE 1건, FAIL 응답 집계 1건이다.
- `20151216000326`은 HTTP 200, DART `000`, package member 1로 PASS했다.
- `20151217000199`는 HTTP 200, DART `014 / 파일이 존재하지 않습니다`로 UNAVAILABLE checkpoint와
  immutable journal을 저장한 뒤 같은 실행의 후속 요청을 중단했다.
- 다음 pending은 `20151218000371`이다. 목표 상한 1,000회에 도달하지 않았으므로 3차 batch 수집은
  완료가 아니라 정상 중단이다.

### 검증 결과와 판정

- current accepted 1,500건, latest document PASS 366건, UNAVAILABLE 2건, FAIL 0건,
  pending 1,132건이다.
- 두 unavailable journal은 각각 HTTP 200·DART 014이며 연결 raw XML SHA-256이 모두 일치한다.
- PASS journal 584개, unavailable journal 2개, document raw 587개, checkpoint temp 0개다.
- current evaluator는 manifest integrity true, status BLOCKED, full capture false다. 이유는
  `CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE`과 `CORPORATE_ACTION_REQUESTS_PENDING`이다.
- 후속 dry-run은 network 0, 다음 pending `20151218000371`, manifest/status/document report 세 파일의
  실행 전후 SHA-256 비변경 PASS다.
- 집중 회귀 24개, C8 전체 회귀 135개 PASS다. 증거:
  `reports/c8_document_batch3_focused_pytest_20260915_2158.xml`,
  `reports/c8_document_batch3_full_pytest_20260915_2159.xml`.
- O6 fills·trades SHA-256은 기준과 같고 orders_exec는 없다.
- 기능 PASS, 정합성 PASS, 운영 반영 PASS(3차 checkpoint 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- full capture와 acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.

### 다음 단계

- 새 별도 document-only 회차에서 `20151218000371`부터 수집을 재개한다.
- pending 전부 종료 후 UNAVAILABLE 2건의 동일 공시 흐름 대체 가능성은 별도 정책 검증한다.

## 52. 2026-09-15 corrected document-only 4차 batch 최대 1,000회 [중단: 새 document 014]

### 지시사항과 실행 경계

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 3차 정상 중단 이후 별도 document-only 수집 재개 승인으로 적용한다.
- 최대 network 1,000회이며 첫 HTTP·transport·empty·DART 오류, invalid package, journal·manifest 오류
  또는 새 014에서 즉시 중단한다. 자동 재시도하지 않는다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- accepted 1,500건, PASS 366건, UNAVAILABLE 2건, FAIL 0건, pending 1,132건이다.
- 첫 pending은 `20151218000371`, manifest integrity true, full capture false다.
- backup: `backup/20260915_c8_document_batch_4_1000/20260915_220802/` 5개 파일이며
  source/backup SHA-256이 모두 일치한다.
- O6 기준 fills·trades SHA-256은 각각
  `EB39246ADD3F8954DC76FDAEC7BA2406CF1BC930F3DD3060BAA93FE8363C0F17`,
  `BCDBCB10ADDDEF251B3A14EB0055993885DD5A02A07AA7FCA991A72A8FDEE387`이고 orders_exec는 없다.

### 종료 조건

1. actual network와 PASS/FAIL/UNAVAILABLE, 첫 중단 응답을 확인한다.
2. 누적 상태·pending·raw·journal·manifest 정합성을 검증한다.
3. 후속 dry-run network 0·상태파일 비변경, checkpoint temp 0을 확인한다.
4. 집중·C8 전체 회귀, O6 불변, 계획 문자열을 확인한다.
5. pending 또는 UNAVAILABLE이 남으면 full capture와 downstream을 BLOCKED로 유지한다.

### 실제 실행과 정상 중단

- actual network 3회에서 첫 오류로 중단했다. PASS 2건, UNAVAILABLE 1건, FAIL 응답 집계 1건이다.
- `20151218000371`, `20151228000283`은 HTTP 200·DART 000·package member 1로 PASS했다.
- `20160104000454`는 HTTP 200·DART `014 / 파일이 존재하지 않습니다`로 UNAVAILABLE checkpoint와
  immutable journal을 저장하고 같은 실행의 후속 요청을 중단했다.
- 다음 pending은 `20160105000439`다. 1,000회 상한 전에 중단됐으므로 4차 batch 수집은 완료가 아니다.

### 검증 결과

- current accepted 1,500건, PASS 368건, UNAVAILABLE 3건, FAIL 0건, pending 1,129건이다.
- unavailable journal 3개는 HTTP 200·DART 014이고 각각 연결 raw XML SHA-256이 일치한다.
- PASS journal 586개, unavailable journal 3개, document raw 590개, checkpoint temp 0개다.
- manifest integrity true, status BLOCKED, full capture false이며 이유는 문서 누락과 잔여 요청이다.
- 후속 dry-run은 network 0, 다음 pending `20160105000439`, manifest/status/document report SHA-256
  비변경 PASS다.
- 집중 회귀 24개, C8 전체 회귀 135개 PASS다. 증거:
  `reports/c8_document_batch4_focused_pytest_20260915_2212.xml`,
  `reports/c8_document_batch4_full_pytest_20260915_2213.xml`.

### O6 외부 갱신 분리 판정

- fills SHA-256은 기존 기준과 같다. trades_calc SHA-256은 기존 기준과 달라졌지만 LastWriteTime은
  21:59:54로 4차 실행 시작 22:09 이전이다.
- 같은 시각 공식 `run_paper_daily`의 `paper_sync.py`가 fills에서 trades_calc를 재생성했다.
  `2_Logs/paper_sync_lineage_audit_latest.json`은 status PASS, lineages 401, issue 0이다.
- fills 1,052행을 현재 config로 메모리 재계산한 trades 654행의 직렬화 SHA-256
  `19C5B90D06DDA66F399A59449EE7A44DDA09204CFF7299317D23CC573ADF58ED`가 현재 파일과 바이트 단위로
  정확히 일치한다.
- 따라서 trades_calc 변화는 이번 C8 수집의 부작용이 아니라 실행 전 공식 운영 재계산이다.
  orders_exec는 계속 없다.

### 단계 판정과 다음 단계

- 기능 PASS, 정합성 PASS, 운영 반영 PASS(4차 checkpoint 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- full capture와 acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.
- 다음 별도 document-only 회차는 `20160105000439`부터 재개한다.

## 53. 2026-09-15 corrected document-only 5차 batch 최대 1,000회 [중단: 새 document 014]

### 지시사항과 실행 경계

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 4차 정상 중단 이후 별도 document-only 수집 재개 승인으로 적용한다.
- 최대 network 1,000회이며 첫 HTTP·transport·empty·DART 오류, invalid package, journal·manifest 오류
  또는 새 014에서 즉시 중단하고 자동 재시도하지 않는다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- accepted 1,500건, PASS 368건, UNAVAILABLE 3건, FAIL 0건, pending 1,129건이다.
- 첫 pending은 `20160105000439`, manifest integrity true, full capture false다.
- backup: `backup/20260915_c8_document_batch_5_1000/20260915_221655/` 5개 파일이며
  source/backup SHA-256이 모두 일치한다.

### 종료 조건

1. actual network와 PASS/FAIL/UNAVAILABLE, 첫 중단 응답을 확인한다.
2. 누적 상태·pending·raw·journal·manifest 정합성을 검증한다.
3. 후속 dry-run network 0·상태파일 비변경, checkpoint temp 0을 확인한다.
4. 집중·C8 전체 회귀, O6 현재 상태, 계획 문자열을 확인한다.
5. pending 또는 UNAVAILABLE이 남으면 full capture와 downstream을 BLOCKED로 유지한다.

### 실제 실행과 정상 중단

- actual network 2회에서 첫 오류로 중단했다. PASS 1건, UNAVAILABLE 1건, FAIL 응답 집계 1건이다.
- `20160105000439`은 HTTP 200·DART 000·package member 1로 PASS했다.
- `20160105000521`은 HTTP 200·DART `014 / 파일이 존재하지 않습니다`로 UNAVAILABLE checkpoint와
  immutable journal을 저장하고 같은 실행의 후속 요청을 중단했다.
- 다음 pending은 `20160106000253`이다. 1,000회 상한 전에 중단돼 5차 batch 수집은 완료가 아니다.

### 검증 결과와 판정

- current accepted 1,500건, PASS 369건, UNAVAILABLE 4건, FAIL 0건, pending 1,127건이다.
- unavailable journal 4개와 연결 raw XML SHA-256이 모두 일치한다.
- PASS journal 587개, unavailable journal 4개, document raw 592개, checkpoint temp 0개다.
- manifest integrity true, status BLOCKED, full capture false이며 이유는 문서 누락과 잔여 요청이다.
- 후속 dry-run은 network 0, 다음 pending `20160106000253`, manifest/status/document report SHA-256
  비변경 PASS다.
- 집중 회귀 24개, C8 전체 회귀 135개 PASS다. 증거:
  `reports/c8_document_batch5_focused_pytest_20260915_2220.xml`,
  `reports/c8_document_batch5_full_pytest_20260915_2221.xml`.
- O6 fills·trades hash와 mtime은 5차 실행 동안 불변이고 orders_exec는 없다.
- 기능 PASS, 정합성 PASS, 운영 반영 PASS(5차 checkpoint 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- full capture와 acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.

### 다음 단계

- 다음 별도 document-only 회차는 `20160106000253`부터 재개한다.

## 54. 2026-09-15 corrected document-only 6차 batch 최대 1,000회 [중단: 새 document 014]

### 지시사항과 실행 경계

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 5차 정상 중단 이후 별도 document-only 수집 재개 승인으로 적용한다.
- 최대 network 1,000회이며 첫 HTTP·transport·empty·DART 오류, invalid package, journal·manifest 오류
  또는 새 014에서 즉시 중단하고 자동 재시도하지 않는다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- accepted 1,500건, PASS 369건, UNAVAILABLE 4건, FAIL 0건, pending 1,127건이다.
- 첫 pending은 `20160106000253`, manifest integrity true, full capture false다.
- dry-run은 READY, network 0, 상태파일 미변경이다.
- backup: `backup/20260915_c8_document_batch_6_1000/20260915_222521/` 5개 파일이며
  source/backup SHA-256이 모두 일치한다.

### 종료 조건

1. actual network와 PASS/FAIL/UNAVAILABLE, 첫 중단 응답을 확인한다.
2. 누적 상태·pending·raw·journal·manifest 정합성을 검증한다.
3. 후속 dry-run network 0·상태파일 비변경, checkpoint temp 0을 확인한다.
4. 집중·C8 전체 회귀, O6 현재 상태, 계획 문자열을 확인한다.
5. pending 또는 UNAVAILABLE이 남으면 full capture와 downstream을 BLOCKED로 유지한다.

### 실제 실행과 정상 중단

- actual network 8회에서 첫 오류로 중단했다. PASS 7건, UNAVAILABLE 1건, FAIL 응답 집계 1건이다.
- `20160106000253`부터 `20160201000394`까지 7건은 HTTP 200·DART 000·package member 1로 PASS했다.
- `20160217000383`은 HTTP 200·DART `014 / 파일이 존재하지 않습니다`로 UNAVAILABLE checkpoint와
  immutable journal을 저장하고 같은 실행의 후속 요청을 중단했다.
- 다음 pending은 `20160218000201`이다. 1,000회 상한 전에 중단돼 6차 batch 수집은 완료가 아니다.

### 검증 결과와 판정

- current accepted 1,500건, PASS 376건, UNAVAILABLE 5건, FAIL 0건, pending 1,119건이다.
- PASS·UNAVAILABLE journal 599개와 연결 raw XML/ZIP SHA-256이 모두 일치한다.
- PASS journal 594개, unavailable journal 5개, document raw 600개, checkpoint temp 0개다.
- manifest integrity true, status BLOCKED, full capture false이며 이유는 문서 누락과 잔여 요청이다.
- 후속 dry-run은 network 0, 다음 pending `20160218000201`, manifest/status/document report SHA-256
  비변경 PASS다.
- 첫 집중 회귀 시도는 pytest 기본 임시 폴더 ACL 거부로 setup 오류가 났다. 권한 있는 명시 basetemp로
  같은 테스트를 재실행해 집중 회귀 24개, C8 전체 회귀 135개 PASS를 확인했다. 증거:
  `reports/c8_document_batch6_focused_pytest_20260915_2233.xml`,
  `reports/c8_document_batch6_full_pytest_20260915_2234.xml`.
- O6 fills·trades hash와 mtime은 6차 실행 동안 불변이고 orders_exec는 없다.
- 계획 문자열은 ExecPlan issue 0, PLANS 기존 비복구 6건·신규 0건이다.
- 기능 PASS, 정합성 PASS, 운영 반영 PASS(6차 checkpoint 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- full capture와 acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.

### 다음 단계

- 다음 별도 document-only 회차는 `20160218000201`부터 재개한다.

## 55. 2026-09-15 exact 014 checkpoint-and-continue 실행 정책 정합화 [완료: 연속수집 원인 해결]

### 지시사항과 분류

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자는 정확한 HTTP 200·DART 014를 UNAVAILABLE로 저장한 뒤 같은 batch를 계속하도록 진행을 승인했다.
- 분류는 계약과 실행의 불일치 수정이다. full capture와 downstream 완전성 정책은 변경하지 않는다.

### 실행 전 사실

- 계약은 UNAVAILABLE receipt를 traversal에서 건너뛰도록 정했지만 collector는 checkpoint 저장 후에도
  `not passed` 공통 hard-stop 분기에서 중단했다.
- 현재 PASS 376건, UNAVAILABLE 5건, FAIL 0건, pending 1,119건이며 다음 pending은
  `20160218000201`이다.
- backup: `backup/20260915_c8_014_continue_policy/20260915_223840/` 8개 파일이며
  source/backup SHA-256이 모두 일치한다.

### 변경 경계와 종료 조건

1. 정확한 014 checkpoint 성공만 현재 batch의 다음 요청으로 진행한다.
2. 014를 fail_count에서 분리하고 unavailable_count로만 집계한다.
3. HTTP·transport·empty·014 외 DART 오류, invalid package, journal·manifest 오류와 020은 계속 hard-stop한다.
4. UNAVAILABLE이 남으면 full capture false와 모든 downstream false를 유지한다.
5. 원문 재확인, 문법, 집중·전체 C8 회귀, mock 실행, 실제 document-only 실행과 산출물 검증을 수행한다.

### 변경 내용

- collector는 정확한 014의 raw·immutable UNAVAILABLE journal·manifest checkpoint 저장이 모두 성공하면
  `continue`로 같은 batch의 다음 pending을 처리한다.
- UNAVAILABLE은 `unavailable_count`로만 집계하고 `fail_count`에서는 분리했다.
- pending이 없지만 UNAVAILABLE 때문에 full capture가 false인 종료를
  `C8_DOCUMENT_COLLECTION_NO_PENDING_INCOMPLETE`로 명시했다.
- 계약의 `document_unavailable_effect`를 same-run continuation까지 명문화했다.
- 014 다음 PASS 연속 처리와 014 다음 020 hard-stop 회귀를 테스트에 추가했다.

### 직접 실행과 검증

- 원문 재확인 PASS, Python AST 2파일과 계약 JSON parse PASS다. 최초 py_compile은 기존 tests
  `__pycache__` ACL 때문에 쓰기 실패하여 성공으로 계산하지 않고 AST parse로 대체했다.
- 집중 회귀 25개 PASS, C8 전체 회귀 136개 PASS·216개 deselected다. 증거:
  `reports/c8_014_continue_focused_pytest_20260915_2244.xml`,
  `reports/c8_014_continue_full_pytest_20260915_2245.xml`.
- 운영 dry-run은 READY·network 0이며 manifest/status/document latest SHA-256 비변경이다.
- 실제 document-only batch는 network 1,000회, PASS 752건, UNAVAILABLE 248건, FAIL 0건,
  hard_stop false, `REQUEST_BUDGET_REACHED`로 종료했다.
- 248개 UNAVAILABLE 모두 뒤에 실제 다음 요청이 이어졌다. 첫 014 `20160429001685` 다음은
  `20160502001491:PASS`, 마지막 014 `20251017000451` 다음은 `20251023000245:PASS`다.
- 누적 PASS 1,128건, UNAVAILABLE 253건, FAIL 0건, pending 119건, next `20251024000420`이다.
- journal 1,599개와 연결 raw hash 전부 일치, checkpoint temp 0, manifest integrity true다.
- capture status BLOCKED, full capture false, downstream 전부 false다.
- O6 fills·trades hash와 mtime은 불변이고 orders_exec는 없다.
- 수정 파일 5개 문자열 검사는 신규 issue 0건이다. PLANS의 기존 비복구 6건만 재검출됐다.

### 판정과 남은 범위

- 기능 PASS, 정합성 PASS, 운영 반영 PASS(014 연속수집 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- 반복적인 신규 014 조기중단 원인은 해결됐다.
- pending 119건 수집과 UNAVAILABLE 253건의 데이터 완전성 해결은 남아 있다.
- acquisition evidence·canonical·M2·후보·target·주문·전체로직은 미적용이다.

## 56. 2026-09-15 document-only 잔여 119건 terminal 수집 [완료: traversal terminal, full capture BLOCKED]

### 지시사항과 실행 경계

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자 `진행해`를 exact 014 연속수집 정합화 이후 잔여 document-only 수집 승인으로 적용한다.
- 최대 network 1,000회이며 exact 014는 UNAVAILABLE checkpoint 후 계속한다.
- HTTP·transport·empty·014 외 DART 오류, invalid package, journal·manifest 오류와 020은 즉시 중단한다.
- acquisition evidence, canonical, M2, 후보, target, 주문은 실행하지 않는다.

### 실행 전 기준과 백업

- PASS 1,128건, UNAVAILABLE 253건, FAIL 0건, pending 119건이다.
- 첫 pending은 `20251024000420`, manifest integrity true, full capture false다.
- backup: `backup/20260915_c8_document_remaining_119/20260915_230237/` 5개 파일이며
  source/backup SHA-256이 모두 일치한다.

### 종료 조건

1. dry-run network 0과 상태파일 비변경을 확인한다.
2. actual network·PASS·UNAVAILABLE·FAIL과 terminal pending 0 여부를 확인한다.
3. 전체 journal/raw SHA-256, manifest integrity, checkpoint temp를 확인한다.
4. full capture와 downstream 차단이 UNAVAILABLE 때문에 유지되는지 확인한다.
5. 집중·전체 C8 회귀, O6 비변경, 계획 문자열을 확인한다.

### 직접 실행과 검증

- 실행 전 dry-run은 READY, network 0이었고 manifest·capture status·document latest 3개 파일은
  바이트 단위로 변하지 않았다.
- 실제 document-only 실행은 network 119회, PASS 80건, UNAVAILABLE 39건, FAIL 0건,
  hard-stop false, `NO_PENDING_REQUESTS`로 종료했다.
- 누적 document 1,500건은 PASS 1,208건, UNAVAILABLE 292건, pending 0건이다.
- PASS journal 1,426개와 UNAVAILABLE journal 292개, 합계 1,718개 모두 연결 raw가 존재하고
  payload SHA-256 불일치는 0건이다. document raw 1,719개, checkpoint temp 0개다.
- manifest integrity는 true다. capture status는 `BLOCKED`, full capture는 false이며 사유는
  `CORPORATE_ACTION_DOCUMENTS_UNAVAILABLE` 하나다.
- acquisition evidence·canonical·M2·후보·target·주문은 모두 false다.
- 사후 dry-run은 `C8_DOCUMENT_COLLECTION_NO_PENDING_REQUESTS`, network 0이고 최신 3개 상태파일은
  다시 바이트 단위로 유지됐다.
- 집중 회귀 25개 PASS, 전체 C8 회귀 136개 PASS·216개 deselected다. 증거:
  `reports/c8_document_terminal_focused_pytest_20260915_2310.xml`,
  `reports/c8_document_terminal_full_pytest_20260915_2311.xml`.
- O6 fills·trades SHA-256은 실행 전과 같고 `orders_exec.xlsx`는 없다.
- 계획 문서 문자열 검사는 이 ExecPlan 신규 issue 0건이다. RootA PLANS의 기존 비복구 가능 6건만
  같은 줄에서 재검출됐다. 증거: `reports/c8_document_terminal_final_plans_mojibake_20260915_2313.json`.

### 판정과 남은 범위

- 기능 PASS, 정합성 PASS, 운영 반영 PASS(document traversal 범위), 정책 PASS,
  FAIL-CLOSED PASS, 회귀 PASS다.
- 잔여 119건의 terminal 수집과 pending 제거는 완료했다.
- OpenDART가 원문을 제공하지 않은 292건은 해결되지 않았다. 따라서 full capture와 downstream은
  계속 BLOCKED이며 acquisition evidence 이후 단계와 전체로직은 미적용이다.

## 57. 2026-09-15 C8 UNAVAILABLE 공식 대체 근거 조사 [완료: 정책 후보 식별, 미적용]

### 지시사항과 범위

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- 사용자 요청에 따라 UNAVAILABLE 292건을 대체할 수 있는 공식 원천만 read-only로 조사했다.
- 운영 계약, capture 상태, downstream은 변경하지 않았다.
- backup: `backup/20260915_c8_unavailable_official_alternatives/20260915_232316/` 2개 파일이며
  source/backup SHA-256 일치 PASS다.

### 확인 결과

- 292건은 전부 정정 공시다. 첨부정정 174건, 기재정정 117건,
  정정명령부과+첨부정정 1건이다.
- merger decision 281건, merger completion 11건이며 161개 종목에 걸쳐 있다.
- DART 공식 공시뷰어는 연속 접속 제한 전 100/100건에서 HTTP 200, title, dcmNo,
  본문 viewer 경로를 제공했다. 나머지 192건은 부재가 아니라 `NA_CONNECTION_LIMIT`다.
- 대표 `20251024000420`은 DART viewer에서 최초 본문과 정정 첨부 트리가 확인됐다.
- OpenDART `cmpMgDecsn.json`은 대표 사례에서 최초 접수 `20251023000245`만 반환해 정정 접수와
  completion lineage의 단독 대체로는 부족하다.
- 1순위는 DART viewer의 본문·첨부 전체 bytes 캡처, 2순위는 KIND 공식 전송본 교차 확인이다.
  구조화 API와 KRX 시장 통계는 보조 근거로만 적합하다.

### 판정과 남은 범위

- 기능 PASS, 정합성 PASS, 운영 반영 NA, 정책 NA, FAIL-CLOSED PASS, 회귀 NA다.
- 대체 공식 근거의 경로는 찾았지만 수용 계약은 승인·구현되지 않았다.
- full capture false, downstream BLOCKED, 전체로직 미적용을 유지한다.
- 상세 증거:
  `reports/c8_unavailable_official_alternative_source_assessment_20260915.md`.

## 58. 2026-09-16 C8 DART viewer exact-receipt shadow collector [완료: shadow 전수 수집, 정책 미적용]

### 지시사항과 실행 경계

- 가드 읽음. 고정 목표 `KOSPI_MCAP_QUARTERLY_V1`, 연구 측정 라운드 NA, 방향 변경 없음이다.
- OpenDART `document.xml`이 014를 반환한 exact receipt 292건만 대상으로 DART 공식 공시뷰어의
  landing, 본문, 첨부 bytes와 식별자·SHA-256을 별도 shadow namespace에 보존한다.
- 기존 capture manifest/status, acquisition evidence, canonical, M2, 후보, target, 주문은 변경하지 않는다.
- shadow 결과는 기존 `PASS`, `full_capture`, downstream 허용 근거로 자동 승격하지 않는다.
- backup: `backup/20260916_c8_dart_viewer_shadow_collector/20260916_083212/` 2개 파일이며
  ExecPlan SHA-256 `4874DC47DF27CBB84346041C65F5D625463B6C5172821062BC7688D0ACC2F15E`,
  PLANS SHA-256 `E7B79996AB8D1E35A05DE160732BA69C530B183ED70EF46EB5A9B966E8F1A35B`다.

### 종료 조건

1. apply는 `--confirm-shadow-only` 없이는 거부하고 dry-run은 network·상태 변경이 0이어야 한다.
2. 입력 대상은 현재 capture status의 unavailable receipt와 정확히 일치해야 한다.
3. receipt별 landing과 document tree, 각 본문·첨부 raw bytes, 식별자, SHA-256을 보존한다.
4. 재시작 checkpoint와 immutable journal을 검증하고 transport·HTML·tree 오류는 승격 없이 남긴다.
5. 집중 테스트, C8 회귀, O6 비변경, 문자열 검사를 수행한다.
6. 실제 DART 연결이 제한되면 구현 검증과 live 수집 상태를 분리해 `NA_CONNECTION_LIMIT`로 보고한다.

### 현재 상태

- collector, shadow 계약, 집중 테스트를 구현했다.
- 실제 DART 구조의 JavaScript node 목차와 exact-receipt 첨부 landing을 반영했다.
- 292건 전부 `SHADOW_PASS`, pending 0, raw artifact 1,814개, 재계산 해시 오류 0건이다.
- landing 494개(첨부 landing 202개), viewer 문서 1,320개, 총 87,697,256 bytes다.
- 관련 접수 링크 1,884개는 계보 후보로만 보존했고 정책상 계보 확정으로 승격하지 않았다.
- 완료 후 dry-run은 network 0, no write, `SHADOW_COMPLETE` 292/292였다.
- 집중 회귀 9개 PASS, 전체 C8 회귀 145개 PASS·216개 deselected다.
- 변경 파일 신규 문자열 issue는 0건이며 RootA PLANS 기존 비복구 가능 6건만 재검출됐다.
- 수집 중 `paper/fills.csv`는 다른 운영 경로에서 갱신됐다. collector에는 O6 writer가 없고,
  완료 후 dry-run 전후 현재 fills·trades·capture·shadow report 해시는 동일했다.
- 기능 PASS, 정합성 PASS, 운영 반영 PASS(shadow 범위), 정책 NA,
  FAIL-CLOSED PASS, 회귀 PASS다.
- 기존 full capture false, downstream BLOCKED, 전체로직 미적용이다.
- 상세 증거: `reports/c8_dart_viewer_shadow_validation_20260916.md`.

## 59. 2026-09-16 C8 DART viewer fallback authority validation [완료: 검증 FAIL, 정책 미승격]

### 지시사항과 정책 경계

- 사용자 `진행해`를 shadow 전수 증거의 fallback 권위 수용 검증 승인으로 적용한다.
- 정책 변경 후보는 OpenDART `document.xml` exact HTTP 200 status 014 접수에만 한정한다.
- 현재 manifest 1,500건 중 OpenDART PASS는 기존 package, 014 292건은 검증된 shadow viewer
  문서만 결정적 package로 재구성해 동일 adapter/lifecycle에 입력한다.
- adapter/lifecycle이 전수 PASS하기 전에는 capture status, acquisition evidence, canonical, M2,
  후보, target, 주문을 변경하거나 열지 않는다.
- 연구 측정 라운드 NA, 고정 목표와 방향 변경 없음이다.

### 백업

- `backup/20260916_c8_dart_viewer_fallback_validation/20260916_094008/`
- PLANS, ExecPlan, capture/adapter/source-route 계약 5개 파일의 SHA-256을 백업 출력으로 확인했다.

### 종료 조건

1. manifest/list/document/shadow journal과 모든 raw hash를 재검증한다.
2. 292건 shadow 문서를 content-addressed deterministic package로 메모리에서 재구성한다.
3. 기존 1,208건과 fallback 292건을 합친 exact 1,500건을 동일 adapter/lifecycle에 입력한다.
4. receipt별 text, 합병 식별, 명시적 initial receipt 계보, completion 실제일 실패를 분리한다.
5. adapter/lifecycle PASS 전에는 정책 계약과 capture 상태를 바꾸지 않는다.
6. 집중·전체 C8 회귀, O6 비변경, 문자열 검사를 수행한다.

### 현재 상태

- validation-only 계약, fallback validator, 집중 테스트를 추가했다.
- list page 2,378건과 exact receipt/document 1,500건의 raw·journal·manifest hash 및 집합 정합성을
  확인했다. OpenDART PASS 1,208건과 viewer fallback 292건이 정확히 결합됐다.
- viewer fallback은 landing을 제외한 `VIEWER_DOCUMENT` 1,320개만 deterministic ZIP으로 메모리
  재구성했다. package persistence와 network request는 0이다.
- 동일 adapter 결과는 FAIL이다. 804/804 mapping, list 2,378/2,378, document 1,500/1,500은
  맞지만 `EVENT_IDENTITY_UNRESOLVED`, `DART_DOCUMENT_NOT_COMPANY_MERGER`로 lifecycle interval은 0건이다.
- fallback 292건 중 현재 계약 전수 PASS는 1건, 실패는 291건이다. event kind는 revision 273,
  cancellation 7, completion 11, initial 1이다.
- landing related receipt 진단은 동일 종목 initial 후보 exact 1개 251건, 0개 41건, 복수 0건이다.
  기존 explicit-original-reference 권위가 아니므로 정책 승격에 사용하지 않았다.
- viewer 본문 `합병` 부재 첨부정정 8건, completion 실제일 부재 11건이다.
- 집중 회귀 6개 PASS, 전체 C8 회귀 151개 PASS·216개 deselected다.
- 문자열 dry-run은 신규/변경 대상 5개 issue 0건이고 PLANS 기존 비복구형 6건만 재검출했다.
- 최종 관찰 구간 fills·trades hash는 재확인 전후 동일했고 orders_exec는 부재했다. 이전 shadow 종료
  값과 비교한 외부 운영 갱신은 있었으나 validator에는 O6 writer가 없다.
- 기능 PASS, 정합성 PASS, 운영 반영 NA, 정책 FAIL, FAIL-CLOSED PASS, 회귀 PASS다.
- 기존 capture `BLOCKED`, full capture false, downstream과 전체로직 미적용을 유지했다.
- 상세 증거: `reports/c8_dart_viewer_fallback_authority_validation_20260916.md`,
  `reports/c8_dart_viewer_fallback_validation_latest.json`.

### 판정

- 검증 구현과 전수 실행은 종료했다.
- 현재 계약으로 viewer fallback authority를 수용하는 정책 변경은 부적합하다.
- 251건 landing 계보 후보 수용, 41건 무계보 처리, 첨부정정·정정 완료 상속 규칙은 각각 별도
  정책 결정 전에는 구현하지 않는다.

## 60. 2026-09-16 C8 DART viewer official family policy scope analysis [완료: 분석 PASS, 정책 미승격]

### 지시사항과 경계

- section 59에서 남은 251/41/8/11 집합의 중복과 공식 근거 강도를 전수 분해한다.
- 기존 parser가 버린 DART landing `select#family` 본문 계보를 raw에서 다시 읽는다.
- fallback 292건뿐 아니라 기존 OpenDART PASS 1,208건의 현재 adapter 계보·날짜 실패도 함께 잰다.
- read-only 분석이며 capture, adapter policy, source route, downstream은 변경하지 않는다.

### 백업

- `backup/20260916_c8_dart_viewer_policy_scope/20260916_102000/`
- PLANS, ExecPlan, adapter, adapter test를 SHA-256 확인 후 백업했다.

### 종료 조건

1. official family option의 selected receipt와 원본 표시 receipt를 exact하게 파싱한다.
2. fallback 292건을 직접 상태 근거, attachment no-op 후보, completion family-only, 근거 부족으로 분리한다.
3. OpenDART PASS 1,208건의 기존 explicit lineage와 completion 실제일 실패를 별도 집계한다.
4. 날짜 label 뒤 여러 날짜가 있는 실제 표에서 현재 unique-window parser 오판을 재현하고 영향 건수를 잰다.
5. 정책을 바꾸지 않고 추정 없는 수용 후보와 계속 fail-closed할 집합을 JSON/Markdown으로 고정한다.
6. 문법, 집중·전체 C8 회귀, O6 비변경, 문자열 검사를 수행한다.

### 현재 상태

- 첫 무계보 표본 `20160429001685`의 공식 landing에는 `select#family`로 최초 본문
  `20160407000678`이 명시돼 있었다.
- 기존 shadow parser는 `dcmNo`가 있는 attachment option만 보존해 `rcpNo`만 있는 family option을
  누락했다. 41건 전부가 실제 무계보라는 이전 해석은 철회하고 전수 재측정한다.
- 원본 completion `20160105000439`에는 `합병기일 2016.01.01`이 있으나 현재 parser는 뒤 120자 안의
  다른 일정 날짜까지 함께 모아 빈 값으로 반환했다. 날짜 원천 부재와 parser 결함을 분리 측정한다.

### 완료 결과

- official `select#family` 원본 receipt는 fallback `292 / 292`에서 확인됐고 모두 기존
  OpenDART PASS source다. 이전 `251 / 41` 분류는 parser 누락 결과이므로 철회했다.
- fallback은 직접 family 상태 후보 106건, attachment-only no-op 후보 175건,
  completion family-only 차단 11건으로 분리됐다. 추정 없는 후보는 281건이다.
- 기존 OpenDART PASS 1,208건은 initial 471건과 비초기 737건이며, 비초기 문서의 현재 계약상
  명시적 최초결정 receipt 연결은 `0 / 737`이다.
- completion 날짜는 현재 parser가 OpenDART `67 / 460`, viewer `0 / 11`을 읽었지만 label 인접
  날짜 방식은 각각 `444 / 460`, `9 / 11`을 읽었다. false missing은 총 386건이다.
- 기능 PASS, 정합성 PASS, 운영 반영 NA, 정책 FAIL, FAIL-CLOSED PASS, 회귀 PASS다.
- 집중 회귀 7개 PASS, 전체 C8 회귀 158개 PASS·216개 deselected다.
- capture `BLOCKED`, full capture false, source route·adapter·downstream·전체로직 미적용을 유지했다.
- 증거: `reports/c8_dart_viewer_policy_scope_latest.json`,
  `reports/c8_dart_viewer_policy_scope_analysis_20260916.md`.

### 판정

- policy scope 분석과 검증은 종료했다.
- 281건만 수용해도 completion 11건과 기존 비초기 737건의 계보 계약 문제가 남으므로 정책은
  승격하지 않는다.
- 다음 범위는 정책 변경이 아니라 확인된 날짜 parser 결함 수정과 전수 재검증이다.

## 61. 2026-09-16 C8 corporate-action date parser defect repair [완료: 버그 수정·검증 PASS, 정책 미승격]

### 지시사항과 경계

- section 60에서 전수 확인한 completion 날짜 false missing 386건의 parser 원인을 수정한다.
- `합병기일` 뒤 가장 가까운 유효 날짜만 선택하고 같은 거리에서 값이 충돌하면 fail-closed한다.
- 계보 authority, family 정책, capture, source route, downstream은 변경하지 않는다.

### 백업

- `backup/20260916_c8_date_parser_fix/20260916_104634/`
- adapter, adapter test, ExecPlan, PLANS, 수정 전 latest 분석 JSON의 SHA-256을 확인했다.

### 종료 조건

1. 복수 후행 날짜가 있어도 label 인접 날짜를 선택하는 회귀 테스트가 PASS한다.
2. 동일 최단 거리에서 날짜가 충돌하면 빈 값으로 차단하는 동작을 유지한다.
3. 1,500건 전수 재분석에서 current parser와 nearest-date 진단이 일치한다.
4. 날짜가 여전히 확인되지 않는 completion을 source별로 분리한다.
5. 집중·전체 C8 회귀, O6 비변경, 문자열 검사를 수행한다.
6. 계보 정책 실패가 남으면 capture와 downstream을 열지 않는다.

### 완료 결과

- `_date_after_label()`을 전체 120자 날짜 유일성에서 label 최단 거리 날짜 선택으로 수정했다.
- 같은 최단 거리에서 서로 다른 날짜가 나오면 빈 값을 반환하는 fail-closed를 유지했다.
- OpenDART completion은 `67 / 460 -> 444 / 460`, viewer는 `0 / 11 -> 9 / 11`로 회복했다.
- false missing은 OpenDART 377건과 viewer 9건, 총 386건에서 0건으로 감소했다.
- 남은 18건은 label 없음 1건, 현재 계약상 유효 날짜 없음 11건, 동거리 날짜 충돌 6건이다.
- adapter는 1,492 events, 0 intervals이며 잔여 실패는 계보 미해결과 합병 본문 부재다.
- 집중 회귀 19개 PASS, 전체 C8 회귀 160개 PASS·216개 deselected다.
- 기능 PASS, 정합성 PASS, 운영 반영 NA, 정책 FAIL, FAIL-CLOSED PASS, 회귀 PASS다.
- capture `BLOCKED`, full capture false, source route·downstream·전체로직 미적용을 유지했다.
- 증거: `reports/c8_date_parser_fix_validation_20260916.md`,
  `reports/c8_dart_viewer_policy_scope_20260916104852.json`.

### 판정

- 확인된 날짜 parser 결함과 386건 false missing은 해결됐다.
- 남은 18건은 자동 추정하지 않으며, 기존 계보 정책 실패와 분리해 계속 fail-closed한다.

## 62. 2026-09-16 C8 completion date residual official-structure scope [완료: 분석 PASS, adapter·정책 미적용]

### 지시사항과 경계

- section 61 이후 날짜 미확인 18건을 공식 원문 표와 viewer 목차 구조로 재분류한다.
- `Ⅰ. 일정` 표·노드에 완전한 연월일이 하나인 경우만 추정 없는 수용 후보로 분류한다.
- 연도 생략, 일정 부재, 구조적으로 해소되지 않는 다중 날짜는 계속 fail-closed한다.
- adapter, 계약 정책, capture, source route, downstream은 변경하지 않는다.

### 백업

- `backup/20260916_c8_date_residual_scope/20260916_110410/`
- ExecPlan과 PLANS의 SHA-256을 확인했다.

### 종료 조건

1. 18건 집합이 section 61 산출물과 exact하게 일치한다.
2. table row와 viewer official schedule node 근거를 문서 hash와 함께 보존한다.
3. 완전 날짜 단일 후보, 부분 날짜, 비합병 내용, 구조적 모호성을 분리한다.
4. validation-only JSON/Markdown과 집중·전체 C8 회귀를 남긴다.
5. O6와 capture 불변성, 문자열 검사를 확인한다.
6. 별도 승인 전 adapter와 정책은 변경하지 않는다.

### 완료 결과

- residual 18건을 section 61 산출물과 exact하게 일치시켰다.
- 공식 `Ⅰ. 일정` 표의 단일 완전 날짜 14건과 viewer official schedule node 2건을
  추정 없는 adapter 반영 후보로 분류했다.
- `20180605000425`는 원문 셀이 `6/1`이라 연도 추론이 필요해 차단했다.
- `20211223000598`은 list report name과 달리 실제 문서가 타법인 주식 장내매도 내용이라 차단했다.
- 후보 16건, 계속 차단 2건이며 모든 row에 document hash와 viewer landing hash를 기록했다.
- 집중 회귀 6개 PASS, 전체 C8 회귀 166개 PASS·216개 deselected다.
- 기능 PASS, 정합성 PASS, 운영 반영 NA, 정책 FAIL, FAIL-CLOSED PASS, 회귀 PASS다.
- capture `BLOCKED`, full capture false, adapter·downstream·전체로직 미적용을 유지했다.
- 증거: `reports/c8_date_residual_scope_latest.json`,
  `reports/c8_date_residual_official_structure_validation_20260916.md`.

### 판정

- 18건 공식 구조 범위 분석은 종료했다.
- 16건을 실제 adapter authority로 반영하는 작업은 별도 단계이며, 현재는 정책 미적용이다.
- 2건은 동일 규칙으로 자동 보정하지 않는다.

## 63. 2026-09-16 C8 structured completion authority design validation [완료: 설계 PASS, adapter·정책 미적용]

### 지시사항과 경계

- section 62의 정확한 날짜 후보 16건을 adapter authority로 수용하기 위한 최소 구조를 검증한다.
- 현재 adapter와 수집 경로의 입력·출력 계약을 추적하고 단순 정규식 확대로 구조 권한을 위조하지 않는다.
- 부분 날짜·비합병 문서·viewer landing 부재/변조는 fail-closed한다.
- adapter, capture, source route, lifecycle, downstream은 이번 단계에서 변경하지 않는다.

### 백업

- `backup/20260916_c8_completion_authority_design/20260916_112529/`
- ExecPlan과 PLANS의 SHA-256을 확인했다.

### 종료 조건

1. 현재 adapter가 구조화 권한을 표현하지 못하는 정확한 인터페이스 결손을 기록한다.
2. OpenDART 460건과 viewer 11건, 총 471건 completion 전수에서 제안 규칙의 날짜 변화와 기존 날짜 보존 여부를 계산한다.
3. viewer official schedule node는 landing hash와 exact node identity가 없으면 수용하지 않는다.
4. 부분 날짜와 비합병 내용은 계속 차단한다.
5. 계보 실패, capture BLOCKED, full capture false와 O6 산출물 불변성을 확인한다.
6. 집중·전체 C8 회귀와 문자열 검사를 수행한다.

### 검증 산출물

- validation-only 설계 계약 JSON
- 전수 시뮬레이션 도구와 latest/versioned JSON
- 최악 사례 집중 테스트와 상세 Markdown 보고서

### 완료 결과

- 현재 adapter 인터페이스는 source context, structured table parser, viewer tree parser가 없고
  문서를 평문화한 뒤 날짜를 찾으므로 제안 authority를 표현하지 못한다.
- 471건 중 현재 non-empty 453건은 그대로 보존하고 residual 18건만 구조화 fallback으로
  평가하면 OpenDART 14건과 viewer 2건을 추가해 469건이 된다.
- 부분 날짜 1건과 비합병 내용 1건은 계속 차단된다.
- viewer 2건은 raw landing hash 검증과 official `Ⅰ. 일정` node identity가 필수다.
- 최악 사례 테스트는 landing hash 변조, node 부재, node/member 날짜 불일치를 차단했다.
- 집중 7개와 전체 C8 173개가 모두 PASS했다.
- 계보는 OpenDART `0 / 737`, viewer `0 / 291`; capture는 `BLOCKED`, full capture는 false다.
- adapter, contract, capture, source route, lifecycle, downstream은 변경하지 않았다.
- 증거: `reports/c8_completion_authority_design_latest.json`,
  `reports/c8_completion_authority_design_validation_20260916.md`.

### 판정

- 구조화 completion authority 설계는 유효하다.
- 현재 adapter 인터페이스는 불충분하므로 구현은 완료되지 않았다.
- 다음 단계는 기존 453건을 보존하는 receipt별 source/landing context 최소 구현이다.

## 64. 2026-09-16 C8 structured completion authority implementation [완료: 날짜 authority PASS, C8 전체 BLOCKED]

### 지시사항과 경계

- section 63 설계대로 current date가 비어 있는 completion에만 structured fallback을 구현한다.
- fallback validator가 검증한 receipt source와 raw viewer landing payload를 adapter에 전달한다.
- 기존 날짜, event lineage, capture completeness, lifecycle, downstream permission은 완화하지 않는다.

### 백업

- `backup/20260916_c8_completion_authority_impl/20260916_121247/`
- 수정 대상 8개 파일의 SHA-256을 확인했다.

### 종료 조건

1. 기존 453개 날짜는 override 0으로 보존한다.
2. OpenDART 14개와 viewer 2개 structured fallback만 추가한다.
3. partial date와 non-merger content는 날짜 미확정 상태를 유지한다.
4. viewer landing hash, official schedule node, member eleId가 모두 일치해야 한다.
5. lineage와 capture 차단은 날짜 권한과 독립적으로 유지한다.
6. focused, actual full-input validator, full C8 regression을 모두 실행한다.

### 완료 결과

- adapter contract `1.1.0`에 empty-current-date-only structured fallback을 추가했다.
- receipt source context 1,500건과 hash-verified viewer landing 292건이 실제 validator에서 adapter로 전달됐다.
- actual completion 결과는 total 471, current authority 453, OpenDART schedule 14,
  viewer official schedule node 2, missing 2다.
- structured 16건은 사전 검증 집합과 exact 일치했고 기존 날짜 override는 0이다.
- missing은 `20180605000425`, `20211223000598`로 고정됐다.
- tampered landing, missing node, node/member mismatch, partial date는 집중 테스트에서 차단됐다.
- 집중 32개와 전체 C8 180개가 모두 PASS했다.
- full validator는 기존 `EVENT_IDENTITY_UNRESOLVED`, 비합병 내용 때문에 FAIL이며 interval 0이다.
- capture `BLOCKED`, full capture false, downstream permission false를 유지했다.
- 증거: `reports/c8_dart_viewer_fallback_validation_latest.json`,
  `reports/c8_completion_authority_implementation_validation_20260916.md`.

### 판정

- completion 날짜 authority 구현 범위는 원인 해결과 실제 전수 검증까지 완료했다.
- C8 lifecycle 전체는 계보가 해결되지 않아 완료가 아니다.

## 65. 2026-09-16 C8 initial-decision lineage authority validation [완료: 부분 PASS, C8 전체 BLOCKED]

### 지시사항과 경계

- original/correction family와 list history가 비초기 사건의 최초 합병결정 receipt를 명시적으로
  증명하는지 validation-only로 검증한다.
- list history 단일 후보를 공식 authority로 간주하지 않는다.
- capture, adapter policy, canonical, M2, candidate, order permission은 변경하지 않는다.

### 백업

- `backup/20260916_c8_lineage_authority_validation/20260916_124508/`
- PLANS와 ExecPlan의 원본·백업 SHA-256 일치를 확인했다.

### 종료 조건

1. 직접 receipt 참조 또는 hash 검증된 viewer official family만 authority로 인정한다.
2. official family root가 실제 initial decision인지 accepted list와 교차 검증한다.
3. completion family와 list-only inference는 fail-closed한다.
4. source별 해소·미해소·비합병 분리를 전수 계산한다.
5. focused, actual full-input, full C8 regression을 실행한다.

### 완료 결과

- 비초기 1,028건은 OpenDART 737건과 viewer 291건이다.
- authority 해소는 OpenDART 0건, viewer 252건이다.
- viewer policy-scope 후보 280건 중 family root가 정정·첨부추가·취소인 28건을 추가 차단했다.
- viewer completion 11건은 종료보고서 family만 있어 initial-decision lineage가 미해소다.
- OpenDART list history는 단일 후보 293건, 복수 후보 266건, 선행 후보 없음 178건이지만
  모두 추론이므로 authority로 승격하지 않았다.
- 자동 확정 불가는 총 776건이며 비합병 attachment no-op 8건은 별도 분리했다.
- syntax/JSON 3개, focused 8개, full C8 188개가 PASS했다.
- actual validator는 audit PASS이지만 verdict는 `LINEAGE_AUTHORITY_PARTIAL_C8_REMAINS_BLOCKED`다.
- capture `BLOCKED`, full capture false, network 0, downstream permission false를 유지했다.
- 증거: `reports/c8_lineage_authority_validation_latest.json`,
  `reports/c8_lineage_authority_validation_20260916.md`.

### 판정

- viewer revision/cancellation 252건은 official family authority로 해소 가능하다.
- OpenDART 737건과 viewer 39건은 현재 증거로 해소되지 않는다.
- C8 전체는 완료가 아니며 다음 단계는 미해소 receipt의 landing acquisition 범위 산정이다.

## 66. C8 OpenDART landing lineage cause repair (2026-09-16)

### Fixed objective and boundary

- Fixed objective: preserve official DART landing evidence for the 737 unresolved `OPENDART_PASS` non-initial receipts and determine whether official family lineage reaches one accepted initial merger decision.
- Change class: data-lineage defect repair and validation-only evidence collection. This is not a strategy research round and does not change Gate, LOCK, capture status, canonical inputs, M2, candidate selection, orders, fills, ledger, or stats.
- Direction change: none.
- Worst case: a family chain cycles, crosses stock codes, points to a future receipt, or ends at a non-initial document. Every such case must remain unresolved and fail closed.

### Implementation sequence

1. Add a landing-only shadow collection contract and collector whose exact input is the unresolved `OPENDART_PASS` receipt set from the frozen lineage report.
2. Dry-run first, then collect immutable official landing bytes and per-receipt SHA-256 journals with bounded retries, delay, and restartable checkpoints.
3. Extend the lineage analyzer only after backing it up. Resolve a receipt only when the official family chain terminates at exactly one accepted initial decision for the same stock and does not violate date, cycle, or integrity guards.
4. Keep completion-report authority separate. A completion report is not linked to an initial decision merely by list order or same-stock proximity; any completion rule needs independently validated official event identity evidence.
5. Run syntax, focused unit tests, artifact integrity checks, full C8 regression, and the actual fallback validator.

### End condition

- Landing collection target count and immutable journal integrity are both PASS.
- The analyzer reports measured resolved and unresolved counts without hardcoded promotion.
- All ambiguous, cross-stock, future-root, missing-root, and cyclic cases remain blocked.
- Full C8 may be called complete only if the existing six verification axes all PASS with runtime artifacts. Otherwise the exact residual blocker is recorded and downstream remains closed.

### Execution result

- Added a restartable landing-only shadow collector and completed 737/737 immutable official DART landings with zero integrity issues.
- Strict official-family analysis resolved 274 OpenDART receipts and, by recognizing 53 official attachment-wrapper originals, resolved 27 additional viewer follow-ups through their OpenDART roots.
- Combined authority validation is PASS as an audit: OpenDART 274 resolved / 463 unresolved; viewer 279 resolved / 12 unresolved.
- Added raw-package receipt-link extraction before HTML tag stripping. Focused regression passed, but the current sample had no additional qualifying links.
- Completion date replay found 276 unique matches, but it remains validation-only because lifecycle contract v1.1 forbids same issuer/date-only lineage.
- Official broad-type probe proved a legacy source gap: corp `00104698` has merger decision `20090403001135` in broad type B, but the current B001-only capture omitted it.
- Full C8 regression: 208 passed. Actual fallback validator remains FAIL with interval count 0; capture remains BLOCKED and all downstream permissions remain false.

### Residual blocker and next exact step

- Residual cause: legacy OpenDART merger decisions are not exhaustively represented by B001.
- Next exact step: define and execute an immutable broad-B legacy decision supplement, reconcile it against existing B001 receipts, collect missing official documents, and replay lineage plus lifecycle. This is a source-route contract change and must not be replaced by date-only inference.
- Detailed evidence: `reports/c8_lineage_landing_cause_repair_20260916.md`.

## 67. C8 broad-B legacy decision supplement (2026-09-16)

### Fixed objective and boundary

- Repair the measured legacy source-coverage defect where the detailed `B001` route omits official merger-decision receipts that are present in OpenDART broad type `B`.
- The frozen input is `c8_completion_lineage_scope_latest.json` SHA-256 `612c93c7037da13a67dab227a3e54939ed962d2f5091a06553d016afa2640522`: 195 unresolved completion rows across 130 stock codes.
- Query each affected corp code from the frozen capture manifest start date through that stock's latest unresolved completion receipt date. Keep `last_reprt_at=N`, ascending date order, and full pagination.
- Persist list and document responses as immutable shadow artifacts with byte length and SHA-256. Reconcile every accepted merger-decision receipt against the existing capture receipts before document collection.
- This supplement cannot modify the capture manifest, capture status, acquisition publication, canonical, M2, candidates, target portfolio, Gate, LOCK, or orders.

### Expected values before execution

- Target unresolved completion rows: 195.
- Target stock/corp mappings: 130 / 130, with no missing mapping.
- A broad-B page may contain unrelated major-event disclosures; only report names containing merger decision after normalized revision-prefix handling are accepted.
- Existing B001 receipts are duplicates, not new evidence. Duplicate receipt metadata disagreement, cross-stock identity, incomplete pagination, hash mismatch, missing document, or non-package document response must fail closed.
- The known positive control is corp `00104698`, stock `000680`, where broad B must include receipt `20090403001135`.

### Verification order

1. Contract and target-window replay.
2. Dry-run with zero network and zero write.
3. One-corp positive-control collection.
4. Full list pagination and receipt-level deduplication.
5. Missing official document collection and immutable replay.
6. Adapter/validator integration only after supplement status is `SHADOW_COMPLETE`.
7. Focused tests, actual validator, full C8 regression, and six-axis verdict.

### End condition

- The supplement phase is complete only when all 130 targets have complete broad-B pagination, all new accepted merger-decision receipts have verified official document packages, and the latest supplement report has no integrity issue.
- Full C8 is complete only if the actual adapter lifecycle and all six verification axes pass. Otherwise the remaining exact blocker stays recorded and downstream remains closed.

### Execution result

- The shadow supplement completed all 130 target stocks and 195 frozen unresolved completion rows. It verified 257 broad-B list pages with zero pending list request.
- The broad-B replay accepted 510 merger-decision receipts: 221 existing B001 duplicates and 289 new receipts. All 289 new documents are reproducible from 268 OpenDART packages plus 21 official DART viewer packages. Integrity issues and pending document/viewer requests are zero, and positive control `20090403001135` passed.
- The fallback validator now hash-verifies and injects the 289 supplemental decisions only into the isolated adapter replay. Accepted disclosures increased from 1,500 to 1,789 and events from 1,492 to 1,781.
- Among the 195 unresolved completion rows, the supplement supplies at least one prior same-stock decision candidate to 160 rows: 62 have one candidate and 98 have multiple candidates; 35 still have none. The candidates cover 108 stock codes.
- Direct official receipt linkage from those 195 completion documents to any of the 289 supplemental decisions is 0. Therefore the existing no-date-only/no-list-order lineage contract correctly leaves completion lineage unresolved.
- Actual fallback validation remains `FAIL`: adapter reasons are `DART_DOCUMENT_NOT_COMPANY_MERGER` and `EVENT_IDENTITY_UNRESOLVED`; interval count remains 0. The explicit additional validator reason `BROAD_B_SUPPLEMENT_TARGETED_SCOPE_ONLY` prevents this targeted 130-stock repair from being misrepresented as full-universe source coverage.
- Focused tests: 34 passed. Full C8 regression: 216 passed. Python compile and both modified JSON contracts passed.
- Post-run dry replay returned `SHADOW_COMPLETE` with zero network requests, zero pending requests, integrity pass, and all mutation flags false. The 10 changed/new source, contract, test, plan, and report files had zero mojibake findings.
- Capture remains `BLOCKED`; canonical, M2, candidates, and orders remain disallowed. No strategy `orders_exec*` artifact exists.

### Verdict and residual blocker

- The measured legacy B001 source-coverage defect is repaired for the frozen 130-stock supplement scope. This phase is complete as a shadow source repair.
- Full C8 is not complete. The remaining blocker is no longer missing decision candidates alone: completion reports do not carry an official initial-decision receipt link, while same-stock/date or list-order pairing is explicitly non-authoritative.
- Resolving that blocker requires a separately approved lineage-authority policy and evidence contract, or another official DART source that exposes event identity. It must not be implemented as candidate proximity inference.
- Detailed evidence: `reports/c8_broad_b_legacy_supplement_cause_repair_20260916.md`.

## 68. C8 event-identity blind shadow validation (2026-09-16)

### Fixed objective and boundary

- Objective: measure whether deterministic structured merger fields can reproduce an already official initial-decision lineage after the official receipt/family link is hidden.
- This is a validation-only data-lineage tool. It cannot change the adapter contract, lineage authority policy, capture status, canonical, M2, candidates, Gate, LOCK, or orders.
- The output for unresolved completion rows is a diagnostic candidate set only. It is never authority and cannot create lifecycle intervals.
- Research round classification: NA. This is a data-lineage validator, not a trading-signal or return hypothesis.

### Frozen inputs

- Official lineage report SHA-256: `e1e333829a169afdd9f550177c735b9cf79e448476a31cc7aa4f5a69178785a2`.
- Completion scope report SHA-256: `612c93c7037da13a67dab227a3e54939ed962d2f5091a06553d016afa2640522`.
- Broad-B supplement report SHA-256: `fe72009769dcaf93d7871840b5618796ec473d3c6173b0d51288eafad103355b`.
- Actual fallback validation report SHA-256: `b33138712cf3a5182b2f561815f2324d0a59229952e79572b43c75fcad343633`.
- Official-authority gold rows before exclusion: 553. The two inspected exploration rows `20060104000094` and `20060119000236` are excluded from evaluation, leaving 551 blind gold rows.
- Unresolved completion projection set: the frozen 195 rows from the completion scope report.

### Pre-registered matcher

1. Candidate roots are earlier initial-decision documents for the same stock code only.
2. Extract normalized exact values for counterparty company name, merger method, merger ratio, planned merger date, and board-resolution date.
3. A candidate requires at least two comparable exact matches, including at least one identity field among counterparty, method, and ratio.
4. Any conflicting non-empty identity field or planned merger date rejects that candidate.
5. Resolve only when exactly one candidate passes. Zero or multiple candidates remain unresolved.
6. Do not use receipt links, DART family links, list order, nearest date, or the gold initial receipt as matcher input.
7. Do not change the matcher after seeing the one-shot result in this phase.

### Acceptance and worst case

- PASS requires wrong-link count `0`, precision `1.0` among resolved blind rows, and blind gold coverage at least `0.50` (`>=276 / 551`).
- Coverage below 0.50 is `NOT_SUPPORTED`; zero resolved rows is not a vacuous precision PASS.
- Worst case: two prior mergers for one stock share dates or ratios. If more than one candidate passes, expected behavior is unresolved with no selected receipt.
- Missing fields, cross-stock roots, future roots, conflicting fields, input hash drift, or duplicate gold identity must fail closed.

### Verification order

1. Contract/hash replay and frozen-population checks.
2. Feature extraction unit tests and conflict/ambiguity/worst-case tests.
3. One-shot blind evaluation on 551 official gold rows.
4. Diagnostic-only projection on 195 unresolved completion rows.
5. Syntax, focused tests, full C8 regression, report replay, mutation flags, and mojibake scan.

### End condition

- The phase ends after the single pre-registered evaluation is recorded, even if acceptance fails.
- Passing this validator proves only that the shadow matcher can reproduce part of official lineage. It does not authorize a policy change or operational use.

### Execution result

- The one-shot blind evaluation completed on all 551 frozen gold rows after excluding the two inspected exploration receipts.
- The first run exposed an implementation defect: `prior_receipt_only` compared dates but did not exclude the current receipt. It produced 53 self-matches. That run (`20260916162156`) is retained as invalid evidence and is excluded from judgment.
- After backup, the selector was corrected to require `(candidate_date, candidate_receipt) < (event_date, event_receipt)`. A regression test proves current and later same-day receipts are excluded.
- Corrected result: correct links 111; wrong links 0; unresolved 440. Resolved precision is 1.0, but coverage is `111 / 551 = 0.2014519056`, below the pre-registered 0.50 and 276-correct minimum. Self-match count is 0.
- The frozen acceptance verdict is `SHADOW_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE` with `BLIND_GOLD_COVERAGE_BELOW_MINIMUM` and `CORRECT_LINK_COUNT_BELOW_MINIMUM`.
- Among the 440 unresolved gold rows, exact-field conflicts blocked 244 rows, fewer than two exact matches blocked 143, and 53 official roots were the current receipt itself and therefore correctly absent from the prior-only candidate pool. Conflict counts are merger ratio 185, merger method 116, planned effective date 50, and counterparty 18; fields can overlap within one row.
- All 195 unresolved completion rows remained `NO_STRUCTURED_MATCH`: 143 had some field matches but no identity-field match, 47 had zero field match, and 5 had no prior initial candidate.
- The matcher granted zero authority and created zero intervals. All operational and downstream mutation flags remained false; network requests were zero.
- Focused tests: 10 passed. Full C8 regression: 226 passed. A no-write deterministic replay reproduced status, reasons, counts, precision, coverage, and completion classifications exactly.

### Verdict

- Implementation and measurement are complete, but the pre-registered matcher hypothesis is not supported at the required coverage.
- Exact structured fields are high precision for the 111 rows they resolve, but they do not provide sufficient general coverage and provide no match for the unresolved completion population.
- The result must not be used to relax lineage policy. Full C8 remains blocked under the unchanged policy.
- Detailed evidence: `reports/c8_event_identity_shadow_validation_20260916.md`.

## 69. C8 completion timeline blind shadow validation (2026-09-16)

### Fixed objective and boundary

- Objective: test whether two dates explicitly stated in official completion and initial-decision documents, board resolution date and merger contract date, can reproduce an independently resolved completion-to-initial lineage.
- This is a validation-only data-lineage tool. It cannot change the adapter contract, lineage authority policy, capture status, canonical, M2, candidates, Gate, LOCK, or orders.
- The existing completion gold was resolved by exact effective-date state matching. The shadow matcher is forbidden from reading actual or planned effective dates, so scoring cannot reuse the field that created the gold label.

### Frozen population

- Input hashes are frozen in `config/c8_completion_timeline_shadow_validation_contract_v1.json`.
- The prior event-identity blind gold has 551 receipts. The resolved completion population has 276 receipts and overlap between the two sets is zero.
- Four completion rows printed during source inspection are excluded: `20090410000559`, `20090515000400`, `20090602000285`, `20090609000210`.
- Blind gold is therefore 272 completion receipts. The unresolved projection population remains the frozen 195 receipts.

### Frozen matcher and acceptance

- Candidate pool: same-stock initial-decision receipts that are strictly earlier than the completion receipt. Existing official family linkage is not an input.
- Extract only board resolution date and merger contract date from official document text using the exact label lists frozen in the contract.
- Both fields must be non-empty and exactly equal. A mismatch, missing field, cross-stock row, current/future receipt, or multiple passing candidates remains unresolved.
- Receipt links, family links, list order, nearest-date logic, actual effective date, and planned effective date are forbidden inputs.
- Acceptance requires wrong links 0, resolved precision 1.0, coverage at least `136 / 272 = 50%`, and at least 136 correct links.
- Worst case: one issuer has multiple merger decisions sharing the same two dates. The matcher must return ambiguous and grant no authority.
- The one-shot result is recorded without tuning the labels or thresholds after measurement.

### End condition

- Syntax, focused tests, deterministic one-shot execution, artifact checks, full C8 regression, and no-write replay are recorded.
- Regardless of PASS or FAIL, authority granted and intervals created remain zero. A supported shadow hypothesis still requires a separate policy decision and is not an operational approval.

### Execution result

- The frozen 272-row blind completion gold has zero overlap with the prior 551-row event-identity gold.
- One-shot result: correct 16, wrong 0, unresolved 256, precision 1.0, coverage `16 / 272 = 0.0588235294`.
- Classification counts: 236 event timeline fields missing, 20 no timeline match, and 16 unique timeline matches.
- The unresolved 236 rows contain 192 board-date misses and 167 contract-date misses; 123 rows miss both fields. The counts overlap by field.
- Projection over the frozen 195 unresolved completions produced 35 unique, 1 ambiguous, 13 no-match, and 146 event-field-missing rows. Authority granted and intervals created remain zero.
- Focused tests: 9 passed. The first full run was invalid because of pytest base-temp ACL failure; the permission-correct rerun passed all 235 C8 tests.
- A no-write replay matched the saved status, reasons, population, blind rows, projection rows, and operation flags exactly.

### Verdict

- The result is `COMPLETION_TIMELINE_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE` because coverage is below 50% and correct links are below 136.
- The matcher is precise for the 16 rows it resolves, but the two-date source grammar is not sufficiently available for general lineage authority.
- No label, parser priority, or threshold was changed after the result. Full C8 remains blocked under the unchanged policy.
- Detailed evidence: `reports/c8_completion_timeline_shadow_validation_20260916.md`.

## 70. 2026-09-16 C8 KIND stock-issue merger shadow validation [completed: hypothesis failed, C8 remains closed]

### Objective and fixed boundary

- Test whether the independent official KIND stock-issuance history can connect a completion report to its initial merger decision without changing C8 lineage policy.
- This is validation-only. Adapter policy, capture status, canonical, M2, candidates, Gate, LOCK, and orders remain unchanged.
- The prior 272-row completion blind gold and 195 unresolved projection are reused without changing labels or population membership.

### Official-source qualification

- KIND `mergeListingCompany` is SPAC-survival/SPAC-extinction merger listing data and is not a general replacement for the unresolved completion population.
- KIND `stockissuelist` exposes listing date, listing method, issued shares, par value, issue reason, and KRX process key `bzProcsNo` from 1999 onward.
- A source-grammar probe found official issue reasons containing `합병`, including `타법인흡수합병` and `유상증자(흡수합병)`. Probe rows are not scored.

### Frozen acquisition contract

- Query the 260 unique common-stock codes present in blind gold plus projection, one request per code, from `1999-01-01` through `2026-09-16`, page size 3,000.
- Bind the six-digit stock code to the KIND five-digit issuer code. All 260 frozen codes end in zero.
- Reject non-200 status, redirects, malformed HTML, raw-hash mismatch, duplicate stock captures, and any response reaching 3,000 rows as possible truncation.
- Store raw HTML and a hash-sealed manifest only in the isolated strategy namespace. Do not alter the existing OpenDART capture manifest or adapter inputs.

### Frozen matcher

- Accept only KIND rows whose issue reason contains the exact substring `합병`.
- A completion may use only KIND listing dates from its receipt date through receipt date plus 45 calendar days.
- For same-stock strictly prior initial decisions, extract only `신주의 상장예정일`, `신주 상장예정일`, `신주상장예정일`, or `신주권상장예정일`.
- Resolve only when exactly one KIND row exists and exactly one prior initial decision has an exact scheduled-listing-date match.
- Forbid actual/planned merger effective date, receipt/family lineage, list order, nearest-date selection, and fuzzy company-name matching.
- Multiple issue rows, multiple decisions, missing/conflicting dates, cross-stock candidates, and current/future decisions remain unresolved.
- Do not alter the 45-day window, labels, cause grammar, or thresholds after observing the result.

### Acceptance and worst case

- Require wrong links 0, resolved precision 1.0, blind coverage at least 0.5, and at least 136 correct links out of 272.
- Worst case: one stock has multiple merger issue rows inside 45 days or multiple decisions with the same scheduled listing date. Expected behavior is ambiguous with no selected receipt.
- Even a PASS grants no authority and creates no intervals in this round. Any promotion requires a separate policy approval.

### Planned evidence

- Contract, collector, analyzer, focused tests, raw capture manifest, versioned/latest result JSON, markdown report, full C8 regression, no-write replay, and mojibake scans.
- Pre-edit backup: `backup/20260916_c8_kind_stock_issue_shadow_validation/20260916_172143/`; both plan files matched their backups by SHA-256.

### Executed evidence and result

- Contract: `paper/strategies/kospi_mcap_quarterly_v1/config/c8_kind_stock_issue_shadow_validation_contract_v1.json`.
- Capture: 260 requested, 260 captured, 0 failures, 8,834 total rows, 198 merger-cause rows, 0 page-limit hits. Raw path and raw hash cardinality are both 260.
- Capture manifest SHA-256: `4f4c547ce55cf2eae30d27c727ed454c9bb3090dbacc66695294eecfd9334591`.
- Blind gold: 27 correct, 0 wrong, 245 unresolved out of 272; precision 1.0; coverage 0.09926470588235294.
- Blind classifications: 237 `NO_KIND_MERGER_ROW`, 8 `NO_INITIAL_LISTING_DATE_MATCH`, 27 `UNIQUE_KIND_LISTING_MATCH`.
- Projection: 6 unique, 2 ambiguous initial decisions, 1 ambiguous KIND row, 13 missing initial listing dates, 10 no initial date match, and 163 no KIND merger row out of 195.
- Acceptance failed because coverage was below 0.5 and correct links were below 136. Verdict: `KIND_STOCK_ISSUE_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE`.
- No-write replay matched the saved result exactly after excluding `generated_at`; normalized SHA-256 on both sides was `c601609d5b533b42982f917f6b5bff0226ce9eed7e0cbf49ba30c66f1f7ed3c6`; replay network requests were 0.
- Full C8 regression: 244 passed, 214 deselected in 57.31 seconds; JUnit errors 0 and failures 0.
- Focused synthetic tests cover parser success, unique matching, ambiguous KIND rows, ambiguous initial decisions, non-merger/out-of-window filtering, cross-stock isolation, and listing-date extraction.
- Cache-free AST parsing passed for collector, analyzer, and focused test modules. Scoped tracked-file `git diff --check` had no errors.
- Mojibake dry-run scanned 7 files. All new artifacts and the active ExecPlan had zero issues; only 6 pre-existing, non-repairable lines in `.agent/PLANS.md` were reported and were not guessed or changed.
- Focused and full pytest temporary directories were removed after their JUnit evidence was preserved.
- Result-record backup: `backup/20260916_c8_kind_stock_issue_shadow_validation/20260916_173758_result_record/`; all 9 source/backup SHA-256 pairs matched.

### Final boundary

- Functional, consistency, validation-artifact persistence, policy, fail-closed, and regression checks passed for this isolated validator.
- The matcher hypothesis itself failed. Authority remains 0, intervals remain 0, and all adapter/policy/canonical/M2/candidate/order permissions remain false.
- No window, labels, cause grammar, or thresholds were retuned after the result. The source remains evidence only and C8 is not complete.
- Detailed report: `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_kind_stock_issue_shadow_validation_20260916.md`.

### Post-audit repair: pre-capture feasibility guard [implemented and verified]

- Defect: the frozen DART-side input can supply scheduled listing dates for only 38 blind rows, below the existing minimum of 136 correct links, so the capture could not reach PASS before any KIND request was made.
- Classification: validation-only defect repair. Acceptance thresholds, matcher semantics, policy, adapter, canonical, M2, candidates, Gate, LOCK, and orders remain unchanged.
- Implementation: add a no-write/no-network source-ceiling evaluator and require it to PASS before the collector creates its network thread pool.
- End condition: the current frozen input reports `38 < 136`, the capture path returns fail-closed with zero network calls and no manifest rewrite, and focused plus full C8 regression pass.
- Pre-edit backup: `backup/20260916_c8_kind_feasibility_guard/20260916_180840/`; all 6 source/backup SHA-256 pairs matched.
- Implementation: `assess_source_ceiling()` and `evaluate_pre_capture_feasibility()` calculate the necessary DART-side upper bound without network or writes. The collector requires a PASS before constructing its request thread pool and returns exit code 3 on a feasibility block.
- Actual preflight: 38 of 272 blind rows can supply the required official-initial scheduled listing date, below the frozen minimum of 136; shortfall 98; verdict `KIND_PRE_CAPTURE_SOURCE_CEILING_BELOW_ACCEPTANCE_MINIMUM`.
- Runtime proof: capture entry returned exit code 3, network requests 0, artifact writes 0, and the existing manifest hash and mtime remained unchanged.
- Regression: focused 10 passed; full C8 247 passed and 214 deselected; failures 0 and errors 0.
- Existing matcher replay remained byte-equivalent after excluding `generated_at`; normalized SHA-256 remained `c601609d5b533b42982f917f6b5bff0226ce9eed7e0cbf49ba30c66f1f7ed3c6`.
- Final status: the feasibility defect is fixed. The matcher result and C8 fail-closed state are unchanged, and no operational permission was opened.
- Evidence: `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_kind_feasibility_guard_validation_20260916.md`.

### Post-audit repair: dual bottleneck diagnostics [implemented and verified]

- Defect: the saved matcher result exposed final classifications but did not separate the independently insufficient DART and KIND required inputs, so the failure could be misread as KIND-only.
- Classification: validation-only diagnostic repair. Matcher behavior, window, acceptance, reason codes, policy, adapter, canonical, M2, candidates, Gate, LOCK, and orders remain unchanged.
- Implementation: derive and persist DART official-initial listing-date availability, exact-one KIND forward-window row availability, resolved matches, and a three-way decomposition of `NO_KIND_MERGER_ROW`.
- Attribution is calculated from current counts versus the frozen minimum rather than hardcoded.
- Actual result: DART 38/272, KIND 35/272, resolved 27/272, wrong 0; attribution `DUAL_REQUIRED_INPUT_SHORTFALL`.
- `NO_KIND_MERGER_ROW=237`: no merger row anywhere 140, no row in the absolute 45-day window 96, pre-completion near miss 1.
- Existing core result is unchanged after removing only the new diagnostics and `generated_at`.
- Runtime and regression: analyzer expected exit 2, network 0, operational change false; focused 17 passed; full C8 252 passed and 216 deselected.
- No-write replay normalized SHA-256: `b878ac9b57a948e0d1336728a01c6a8c95a49943a5f1285d5c76ee529a7d743c` on both sides.
- Final status: the failure attribution defect is resolved. C8 matcher status remains FAIL and downstream remains fail-closed.
- Evidence: `paper/strategies/kospi_mcap_quarterly_v1/reports/c8_kind_bottleneck_diagnostics_validation_20260916.md`.

## 71. 미래 전용 개인 로직 구현 절차 작성 (2026-09-16)

- 사용자 요청: 과거 C8 공식 계보 복원이 불가능한 경우에도 개인 로직을 구현할 수 있도록 작업절차를 세부적으로 작성한다.
- 판단: 전략 전체를 종료하지 않는다. 과거 계보 복원만 `CLOSED_DATA_SOURCE_UNAVAILABLE`로 닫고, 기존 C1~C12를 유지한 미래 공식 시점 snapshot 기반 격리 paper로 진행한다.
- 방향 변경: 전략 규칙·정책값 변경 없음. 증거 생성 경로를 과거 소급 복원이 아닌 미래 시점 직접 적재로 제한한다.
- 상세 ExecPlan: `docs/exec-plans/active/20260916_kospi_mcap_quarterly_forward_only_implementation.md`.
- 단계: F0 역사 종료 분리 -> F1 미래 공식 원천 계약 -> F2 불변 snapshot -> F3 목표 계산 -> F4 가상 주문·체결 -> F5 원장·NAV -> F6 E2E -> F7 replay -> F8 사전등록 -> F9 실제 분기 paper -> F10 표본 판정.
- 현재 상태: 계획만 작성. 코드·계약·정책·운영 산출물 미변경, 전체로직 미적용.
- 최초 착수 범위: 사용자 승인 후 F0~F1만 진행하며 F1 PASS 전에는 F2 이후 실제 산출물을 만들지 않는다.
