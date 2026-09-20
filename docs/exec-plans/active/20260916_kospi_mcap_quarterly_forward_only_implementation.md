# KOSPI 시가총액 상위 100 미래 전용 개인 로직 구현 절차

- 작성일: 2026-09-16
- 상태: `SUSPENDED_2026-09-16` (이전: `DRAFT_FOR_USER_REVIEW`, 미승인)
- 보류 사유: 2026-09-16 사용자 결정 ③. 상위 ExecPlan(20260915)과 함께 보류. 2026-09-16 독립 검증 지적(F1 필드 누락, 매일 운영 흐름 부재, 불완전 분기 규칙·일정 부재, M7 조건 불일치, C9 규칙 2건 누락)은 재개 시 먼저 반영한다.
- 전략: `KOSPI_MCAP_QUARTERLY_V1`
- 실행 범위: `ISOLATED_PAPER_ONLY`
- 구현 기준: 기존 승인 C1~C12와 `strategy_contract_v1.json`
- 운영 영향: 없음. 기존 O6, Gate, LOCK, 브로커, 공용 주문·체결·원장·통계는 변경하지 않는다.

## 1. 시작 선언

- 가드 읽음: 예.
- 현재 고정 목표: 역사 C8 계보를 추정으로 복원하지 않고, 앞으로 생성되는 공식 시점 자료만으로 개인 분기 리밸런싱 로직을 구현하고 격리 가상매매 표본을 축적한다.
- 탐색/확증 라운드: `NA`(구현 단계). 미래 성과 측정 라운드는 F8에서 `CONFIRMATION`으로 별도 사전등록한다.
- 방향 변경 여부: 부분 변경. 전략 규칙은 유지하고 증거 생성 경로만 `과거 복원`에서 `미래 시점 직접 적재`로 제한한다.
- 이번 범위: 구현 절차 작성만 수행한다. 코드, 설정, 정책, 주문, 원장, 스케줄러는 변경하지 않는다.
- 운영 변경: 없음.

## 2. 이번 판단의 정확한 의미

종료하는 것은 `과거 C8 completion-to-decision 계보 복원`이다. 종료하지 않는 것은 개인 전략 구현이다.

```text
과거 C8 계보 복원          CLOSED_DATA_SOURCE_UNAVAILABLE
과거 성과의 OOS 자격       금지
미래 공식 시점 스냅샷      구현 대상
순수 포트폴리오 계산       구현 대상
격리 가상 주문·체결        구현 대상
전략 전용 원장·NAV         구현 대상
실브로커 주문              금지
기존 O6 교체               금지
```

미래 전용 전환은 C8 적격성 조건을 완화하지 않는다. 선정 시점에 공식 상태를 확인할 수 없는 종목은
정상으로 추정하지 않고 해당 리밸런싱 전체를 FAIL-CLOSED한다.

## 3. 변경하지 않는 고정 계약

1. 초기 전략자본 1억원, 첫 등록 라운드 `N=100`.
2. 직전 분기 마지막 KRX 거래일 종가로 선정하고 다음 분기 첫 KRX 거래일에 집행.
3. KOSPI 보통주만 사용하고 선정 시점 거래·관리·상폐절차·기업행위 상태를 확인.
4. 시가총액 가중, 종목별 20% 상한, 초과분 반복 비례 재배분.
5. KOSPI200 종가 MA200 아래는 다음 거래일부터 50%, 이상은 100% 노출.
6. 정수화는 내림 후 추적오차가 줄어드는 경우만 1주씩 추가.
7. 주문 시간 09:05~15:20, 호가 나이 5초 이하, 시장성 지정가, DAY, 부분체결 허용.
8. 15:20 잔량 취소, 다음 세션 1회만 재시도.
9. 전략 NAV 7,500만원 이하에서 영구 종료 래치.
10. 전략 전용 ID·파일·원장·통계·`strategy_D` 사용.
11. 브로커 전송, 기존 O6 파일 쓰기, 자동 스케줄 등록 금지.
12. 미래 paper 결과만 성과 근거로 사용하고 자동 승격하지 않음.

## 4. 전체 의존 순서

```text
F0 역사 경로 종료 고정
 -> F1 미래 공식 원천 계약과 사전점검
 -> F2 미래 불변 스냅샷 수집기
 -> F3 목표 포트폴리오 순수 계산
 -> F4 격리 가상 주문·체결 상태기계
 -> F5 전략 전용 원장·NAV·종료 래치
 -> F6 E2E 계보·검증·원자적 발행
 -> F7 결정론적 구현 재현
 -> F8 미래 라운드 사전등록·동결
 -> F9 실제 분기말/장중 수동 paper 실행
 -> F10 미래 표본 누적과 1년·3년 판정
```

앞 단계가 PASS가 아니면 다음 단계의 운영 산출물을 만들지 않는다. 다만 F3~F6의 순수 코드와 fixture는
F2 실제 분기말 스냅샷 전에도 개발할 수 있으며, 그 결과의 자격은 `IMPLEMENTATION_EVIDENCE_ONLY`다.

## 5. 단계별 구현 절차

### F0. 역사 C8 경로 종료와 상태 분리

#### 입력

- C8 DART/KIND 검증 결과
- `c8_kind_stock_issue_shadow_validation_latest.json`
- 기존 active ExecPlan과 C1~C12 계약

#### 작업

1. 과거 계보 복원 상태를 `CLOSED_DATA_SOURCE_UNAVAILABLE`로 고정한다.
2. 과거 검증의 27개 고정밀 연결은 참고 증거로 보존하고 운영 authority는 0으로 유지한다.
3. 미래 수집 상태를 별도 키 `FORWARD_CAPTURE_PENDING`으로 시작한다.
4. 과거 FAIL을 미래 현재시점 데이터의 자동 FAIL로 전파하지 않되, 과거 자료로 미래 PASS를 대신하지 않는다.

#### 산출물

- 역사 종료 판정 JSON
- 미래 구현 상태 JSON
- 두 상태를 섞지 않는 상태 전이 계약

#### PASS

- 과거 `CLOSED`와 미래 `PENDING`이 서로 다른 필드와 이유를 가짐.
- authority, canonical, M2, 주문 권한이 자동으로 열리지 않음.

#### STOP

- 과거 27건을 전체 유니버스 authority로 승격하는 경우.
- 과거 FAIL을 숨기거나 `PASS`로 바꾸는 경우.

### F1. 미래 공식 원천 계약과 사전점검

#### 목표

실제 분기 선정일에 C8 필수열을 공식 자료로 채울 수 있는지, 네트워크 대량 수집 전에 확인한다.

#### 필수 필드

```text
selection_as_of, code, name, market, security_type,
listed_date, delisted_date, listing_status, trade_status,
management_status, delisting_procedure_status, merger_status,
event_type, event_effective_date,
close, close_as_of, market_cap, market_cap_as_of,
source, source_query, retrieved_at, source_sha256
```

#### 작업

1. KRX 시장·종목종류·거래상태·관리/상폐절차·종가·시가총액의 공식 원천을 필드별로 매핑한다.
2. DART 기업행위는 발생 이후의 공식 접수번호와 원문을 미래 시점부터 직접 적재한다.
3. 원천별 조회 가능 시각, 기준일 의미, 정정 가능성, 재조회 정책을 계약으로 기록한다.
4. 소량 positive control로 스키마·날짜·해시·중복·페이지 제한을 확인한다.
5. 필수 필드 중 하나라도 공식 원천이 없으면 `FAIL_FORWARD_SOURCE_CONTRACT`로 중단한다.

#### 예정 파일

- `config/forward_source_contract_v1.json`
- `src/forward_source_preflight.py`
- `tools/preflight_kospi_mcap_quarterly_forward_sources.py`
- `tests/test_kospi_mcap_quarterly_forward_source_preflight.py`
- `reports/forward_source_preflight_latest.json`

#### PASS

- 각 필드에 공식 source와 `as_of` 의미가 하나 이상 고정됨.
- positive control의 raw bytes, SHA-256, 조회조건, 행 수가 재현됨.
- 미래정보 사용과 현재 상태의 과거 소급 적용이 없음.

#### STOP

- 종목명 문자열로 보통주를 추정해야 하는 경우.
- 현재 스냅샷을 과거 시점 데이터처럼 사용하는 경우.
- 필수 상태가 `unknown`인데 정상으로 처리하는 경우.

### F2. 미래 불변 스냅샷 수집기

#### 목표

선정 시점의 공식 자료를 수정 불가능한 raw와 정규화 snapshot으로 저장한다.

#### 작업 순서

1. `--dry-run`: 예정 조회, 요청 수, 출력 경로, 기존 파일 충돌만 계산한다.
2. `--probe`: 소량 코드로 공식 응답 구조와 해시 저장을 확인한다.
3. `--capture`: 실제 선정일 전체 KOSPI 대상 raw를 저장한다.
4. raw 검증 후에만 normalized snapshot을 생성한다.
5. quality validator PASS 후에만 `*_latest`를 원자적으로 교체한다.

#### 예정 산출물

```text
data/inbox/raw/forward/<selection_as_of>/...
data/snapshots/universe_<selection_as_of>.csv
data/snapshots/corporate_actions_<selection_as_of>.jsonl
manifests/forward_capture_<selection_as_of>.json
reports/forward_snapshot_quality_<selection_as_of>.json
```

#### 품질 불변식

- `(selection_as_of, code)` 중복 0.
- KOSPI·보통주 여부의 공식값 누락 0.
- `close_as_of`와 `market_cap_as_of`가 `selection_as_of`와 일치.
- `retrieved_at >= source_as_of`; 미래 시점 값 사용 0.
- raw/normalized/manifest 해시 모두 존재.
- 적격 모집단 100개 이상.
- capture 실패를 이전 성공 snapshot으로 조용히 대체하지 않음.

#### PASS

`FORWARD_M1_PASS`는 실제 선정일 snapshot의 기능·정합성·정책·FAIL-CLOSED 검증이 모두 PASS일 때만 부여한다.

#### STOP

- 100개 미만, 필수값 누락, 중복, 기준일 불일치, 해시 불일치.
- 정정된 원천이 이전 raw를 덮어쓰는 경우. 정정은 새 버전으로만 저장한다.

### F3. 목표 포트폴리오 순수 계산(M2)

#### 입력

- `FORWARD_M1_PASS` snapshot 또는 테스트 전용 고정 fixture
- 전략 계약 SHA-256
- 전략 전용 직전 NAV와 가용자본
- KRX 거래일 달력과 KOSPI200 종가 시계열

#### 순수 함수 순서

1. 선정일·예정 집행일 결정.
2. C8 적격성 필터 적용.
3. 시가총액 내림차순 상위 100개 고정.
4. 시가총액 비중 계산.
5. 20% 상한과 초과분 반복 비례 재배분.
6. MA200 노출 50%/100% 적용.
7. 수수료·세금·주문 준비금을 제외한 매수 예산 계산.
8. 수량 내림과 추적오차 개선 1주 추가.
9. 목표 비중·금액·수량·잔여현금·0주 비중 검증.

#### 예정 파일

- `src/calendar.py`
- `src/universe.py`
- `src/weights.py`
- `src/exposure.py`
- `src/integerization.py`
- `tools/build_kospi_mcap_quarterly_targets.py`
- `tests/test_kospi_mcap_quarterly_targets.py`

#### 산출물

```text
targets/target_<rebalance_id>_<target_version_id>.json
manifests/target_<rebalance_id>.json
reports/target_validation_<rebalance_id>.json
```

#### PASS 불변식

- 대상 종목 정확히 100개.
- 목표 비중 합계가 적용 노출과 허용오차 안에서 일치.
- 개별 비중 20% 초과 0.
- 음수 현금·음수 수량·예산 초과 0.
- 0주 종목 목표비중과 잔여현금이 각각 10% 미만.
- 같은 입력·계약 해시에서 결과 SHA-256 동일.

#### STOP

- F2 실제 snapshot이 FAIL인데 실제 target을 생성하는 경우.
- fixture 결과를 실제 리밸런싱 결과로 발행하는 경우.

### F4. 격리 가상 주문·체결 상태기계(M3/C9)

#### 입력

- 검증된 target
- 전략 전용 현재 포지션과 가용현금
- 5초 이내 호가 snapshot

#### 상태 전이

```text
TARGET_READY
 -> INTENT_CREATED
 -> ORDER_READY
 -> SUBMITTED_PAPER
 -> PARTIAL_FILLED / FILLED
 -> CANCELLED_UNFILLED / PARTIAL_CANCELLED
 -> RETRY_READY(다음 세션 1회)
 -> TERMINAL
```

#### 작업

1. 목표수량과 현재수량 차이만 intent로 생성한다.
2. SELL을 먼저 처리하되 미체결 매도대금을 BUY 예산으로 쓰지 않는다.
3. 09:05 이전·15:20 이후 신규 주문을 차단한다.
4. 호가 나이가 5초를 넘으면 주문하지 않는다.
5. ASK1/BID1 시장성 지정가와 관측 잔량 범위에서만 가상 체결한다.
6. 부분체결마다 결정적 `fill_id`를 만든다.
7. 15:20에 잔량을 취소하고 다음 세션에 1회만 재계산한다.
8. 브로커 모듈 import·호출을 정적·런타임 양쪽에서 차단한다.

#### 예정 파일

- `src/execution_state.py`
- `src/paper_order_engine.py`
- `tools/run_kospi_mcap_quarterly_paper_execution.py`
- `tests/test_kospi_mcap_quarterly_paper_execution.py`

#### 최악조건 fixture

- 오래된 호가, 중복 실행, BUY/SELL 동시 생성, 초과체결, 부분체결 후 재시도, 재시도 2회 요청,
  15:20 이후 주문, broker source 혼입.

#### PASS

- 주문수량 초과 체결·고아 체결·중복 ID 0.
- fixture 상태기계 PASS.
- 실제 장중 100종목 호가·부분체결 검증 전 운영 반영은 `NA`.

### F5. 전략 전용 원장·NAV·영구 종료 래치(M4/C10)

#### 원장 구조

```text
fills -> cash ledger -> position ledger -> valuation -> NAV -> stats
```

#### 작업

1. 최초 현금 1억원을 round 시작 이벤트로 한 번만 기록한다.
2. fill별 현금·수량·비용 변화를 복식에 준하는 보존식으로 기록한다.
3. 미결제 채권·채무, 의도 현금, 정수주 잔여현금, exception cash를 구분한다.
4. 거래정지는 마지막 유효가격과 가격일을 가진 locked value로 분리한다.
5. 완전한 평가에서 NAV가 7,500만원 이하이면 영구 래치를 기록한다.
6. 래치 뒤 BUY와 노출 증가는 차단하고 자동 해제하지 않는다.

#### 예정 파일

- `src/ledger.py`
- `src/nav.py`
- `src/termination.py`
- `tools/update_kospi_mcap_quarterly_ledger.py`
- `tests/test_kospi_mcap_quarterly_ledger.py`

#### PASS 불변식

- 현금·수량 음수 0.
- `초기자본 + 누적손익 + 외부흐름 = 현금 + 평가자산 + 채권 - 채무` 보존.
- 외부 입출금이 발생하면 `INVALIDATED_BY_CAPITAL_FLOW`.
- 래치 이벤트 멱등성 및 자동 재개 0.

### F6. E2E 계보·검증·원자적 발행(M5/C11)

#### SSOT

```text
snapshot -> target -> intent -> order -> fill -> ledger -> stats
```

#### 필수 연결키

```text
strategy_id, contract_version, round_id, rebalance_id,
event_id, target_version_id, run_id, as_of,
intent_id, order_id, fill_id, manifest_id
```

#### 작업

1. 각 단계의 입력·출력 SHA-256과 상위 ID를 검증한다.
2. 신규 전략 `strategy_D`를 BUY/SELL 실제 체결의 최신일로 계산한다.
3. `orders(exec_date=strategy_D) -> fills(strategy_D) -> ledger -> stats`를 검증한다.
4. `as_of/run_id/contract_version` 불일치, paper/broker 혼용, 고아 행을 고의 주입한다.
5. 실패 시 timestamped 실패 보고서만 남기고 성공 `*_latest`는 보존한다.
6. 기존 O6 핵심 파일의 전후 SHA-256을 비교한다.

#### 예정 파일

- `src/e2e_validator.py`
- `tools/validate_kospi_mcap_quarterly_e2e.py`
- `tests/test_kospi_mcap_quarterly_e2e.py`
- `run_kospi_mcap_quarterly_paper.bat`

#### PASS

- AGENTS의 STOP 조건을 전략 namespace에 맞게 모두 충족.
- 기존 O6 파일 내용·의미 변화 0.
- 실패 주입마다 latest 발행 차단.

### F7. 결정론적 구현 재현(M6)

#### 목적

성과를 재평가하는 단계가 아니라 같은 고정 입력에서 같은 target·orders·ledger가 나오는지 확인한다.

#### 작업

1. 고정 fixture와 최초 미래 snapshot을 읽기 전용으로 replay한다.
2. 시간·정렬·부동소수점·ID 생성의 비결정성을 제거한다.
3. 결과를 확인한 뒤 전략값을 조정하지 않는다.
4. 과거 자료 결과는 `REPRODUCIBLE_RESEARCH_ONLY` 이상으로 승격하지 않는다.

#### PASS

- 두 번의 replay 결과가 생성시각 제외 동일.
- snapshot, 계약, 코드 해시가 다르면 동일 round 결과로 병합하지 않음.

### F8. 미래 라운드 사전등록·동결(M7/C12)

#### 선행 조건

- F1~F7의 적용 가능한 검증 축 PASS.
- 실제 미래 snapshot이 없으면 데이터 관련 축은 PASS가 아니라 `PENDING/NA`.

#### 등록 내용

- `round_id`, 계약·코드·설정 SHA-256.
- 최초 예정 집행일과 평가 시작일.
- 초기자본 1억원, N=100, 분기 재구성 규칙.
- KOSPI200 비교 방법, 비용, NAV 산식.
- 1년 최소 252거래일·4회 재구성과 3년 최종 판정.
- 절대 연 8%와 위험조정 KOSPI200 대비 90% 조건.
- paired coverage 95%, 분기 체결금액 90% 기준.
- MDE, MES, 불확실성 계산법.
- 전략·정책 변경 시 새 버전·새 round로 재시작하는 규칙.

#### 작업

1. `round_preflight.py --new`로 `CONFIRMATION` round 생성.
2. 등록 문서와 manifest를 작성.
3. `--freeze` 후 `--check` PASS 확인.
4. 실제 수익을 본 뒤 시작일을 이동하지 않는다.

#### STOP

- `MDE > MES`인데 성과 확증을 시작하는 경우.
- 동결 뒤 전략 규칙·비용·시작일을 같은 round에서 변경하는 경우.

### F9. 최초 실제 분기 수동 격리 paper(M8)

#### 분기말 장후

1. 선정일 공식 자료 capture.
2. snapshot quality 검증.
3. target 생성과 validation.
4. 다음 집행일 intent 준비. 주문은 아직 만들지 않는다.

#### 다음 분기 첫 거래일 장중

1. 09:05 이후 호가 freshness 확인.
2. SELL intent부터 가상 주문.
3. 확인된 매도 체결과 기존 현금 범위에서 BUY 수량 확정.
4. 부분체결과 잔량 기록.
5. 15:20 잔량 취소.

#### 익 거래일

- 미체결 잔량이 있으면 같은 intent로 1회만 재시도.

#### 장후

1. fills 반영.
2. ledger·NAV·종료 래치 평가.
3. stats 생성.
4. E2E validator PASS 후 latest 발행.

#### 최초 성공의 의미

- 구현 E2E 증거일 뿐 성과 검증 PASS가 아니다.
- 실제 1년·3년 판정 전까지 `FORWARD_PAPER_OBSERVATION`이다.

### F10. 표본 누적·점검·판정

#### 매일 자동 기록 대상

- NAV, KOSPI200, 데이터 coverage, 미해결 사건, 래치 상태.

#### 분기별 기록 대상

- 목표금액, 체결금액, 체결비율, 미체결·재시도, 회전율, 비용.

#### 월 1회 사용자 점검

- 데이터 누락, E2E 실패, 계약 해시 변화, 외부 입출금, 미해결 기업행위.
- 성과가 좋거나 나쁘다는 이유로 분기 중 규칙을 바꾸지 않는다.

#### 판정 시점

- 1년: `PAPER_PILOT_ELIGIBLE` 검토만 가능.
- 3년: 두 목표와 데이터·실행 대표성·불확실성을 함께 판정.
- NAV 7,500만원 이하: 기간과 무관하게 `TERMINATED_LOSS_LIMIT` 우선.

## 6. 지금 구현 가능한 것과 기다려야 하는 것

| 구분 | 지금 가능 | 실제 시점 필요 |
|---|---|---|
| F0 상태 분리 | 가능 | 없음 |
| F1 원천 계약·probe | 가능 | 공식 원천 응답 필요 |
| F2 수집기 dry-run·fixture | 가능 | 실제 선정일 full capture 필요 |
| F3 순수 목표 계산 | fixture로 가능 | 실제 snapshot target은 선정일 필요 |
| F4 주문 상태기계 | fixture로 가능 | 5초 호가 E2E는 장중 필요 |
| F5 원장·NAV·래치 | fixture로 가능 | 실제 fill/NAV는 장중·장후 필요 |
| F6 E2E 검증기 | fixture로 가능 | 실제 체인의 운영 반영은 장중·장후 필요 |
| F7 결정론 replay | 가능 | 최초 snapshot 후 실제 replay 추가 |
| F8 라운드 등록 | 구현 검증 후 가능 | 시작일 전에 동결 필요 |
| F9 최초 paper | 불가 | 실제 분기말과 다음 분기 첫 거래일 필요 |
| F10 성과 판정 | 불가 | 1년·3년 표본 필요 |

## 7. 각 단계 공통 작업 순서

1. 사용자 승인 범위와 단계 종료조건을 PLANS에 기록한다.
2. 수정 대상 기존 파일을 백업하고 SHA-256을 비교한다.
3. 현재 writer와 consumer를 다시 추적한다.
4. 계약·schema·fixture를 먼저 작성한다.
5. 최소 코드만 구현한다.
6. 수정 코드 원문을 재확인한다.
7. 문법 검증을 실행한다.
8. 공식 CLI 또는 fixture 실행 검증을 수행한다.
9. JSON/CSV/로그 산출물을 검증한다.
10. 기능·정합성·운영 반영·정책·FAIL-CLOSED·회귀 6축을 판정한다.
11. 실패하면 다음 단계로 넘어가지 않고 정확한 blocker를 기록한다.
12. 임시값과 pytest 임시폴더를 정리하고 원복 여부를 확인한다.

## 8. 전체 완료 기준

개인 로직의 **구현 완료**는 F0~F7의 코드·fixture·E2E 검증이 PASS인 상태다. 이때도 실제 운영 반영은
`NA`일 수 있으며 `IMPLEMENTATION_EVIDENCE_ONLY`다.

개인 로직의 **최초 실행 완료**는 F8 사전등록 후 F9에서 실제 미래 snapshot, 실제 장중 호가, 실제 가상
체결, 원장, NAV, stats가 같은 계보로 한 번 끝까지 연결되고 6축이 모두 PASS인 상태다.

개인 로직의 **성과 확인 완료**는 F10의 1년 또는 3년 판정 시점에만 가능하다. 구현 성공과 수익성 성공을
같은 완료로 표현하지 않는다.

## 9. 최초 착수 범위

계획 승인 후 첫 구현은 F0와 F1로만 제한한다.

1. 역사 C8 종료와 미래 `PENDING` 상태를 분리한다.
2. 미래 필수 필드별 공식 원천 계약을 작성한다.
3. 네트워크 대량 수집 없이 positive control과 feasibility를 검증한다.
4. F1 PASS/FAIL을 보고한 뒤에만 F2 수집기를 구현한다.

첫 보고에는 다음을 포함한다.

- 실제 원천과 필드별 authority
- official `as_of` 의미와 조회 가능 시각
- 누락 필드와 STOP 여부
- 예정 요청 수와 출력 경로
- 6축 PASS/FAIL/NA
- F2 착수 가능 여부

## 10. 사용자 승인 지점

이 문서 작성은 구현 승인이 아니다. 아래 단계는 각각 직전 결과 보고 후 진행한다.

1. F0~F1: 상태 분리와 공식 원천 계약.
2. F2~F3: 미래 수집기와 목표 포트폴리오 계산.
3. F4~F7: 가상 실행·원장·E2E·재현.
4. F8: 미래 확인 라운드 등록과 동결.
5. F9: 실제 분기말·장중 수동 paper 실행.

정책값 변경, 브로커 연결, 스케줄 등록, RootB 표시가 필요해지면 이 계획의 자동 후속이 아니라 별도 승인
대상으로 분리한다.
