# 2026-04-24 Partial Validation Evidence
- Latest `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`:
  - `status=WARN`
  - `pre_validation=WARN`
  - `lifecycle=PASS`
  - `amend_cancel=PASS`
  - `chain_ssot=PASS`
  - `recovery_replay=PASS`
  - `session_rule=PASS`
- WARN reason:
  - `pre_validation:max_new_zero_with_candidates`
  - `pre_validation:entry_ready_zero_after_caps`
  - linked current blocker: `max_positions_full`
- Runtime order row:
  - `PAPER_SELL_006360_20260424_FUNDAMENTAL_CRITICAL`
  - state `FILLED_FULL`
  - session `REGULAR_DAILY_EXIT`
  - `time_in_force=DAY`

# 2026-04-23 Order Stage Paper V1

## 1. 목표
- 가상매매 단계에서 실브로커 전송 없이도 주문 단계 1~7을 검증 가능한 구조로 구현한다.
- 대상은 `orders -> fills -> ledger -> stats` 체인의 paper 계층이다.

## 2. 비목표
- 실브로커 OAuth/IP/실계좌 주문 전송 구현
- KRX/NXT/SOR 실주문 라우팅 활성화
- Gate 의미, LOCK 의미, FAIL-CLOSED 의미 변경
- 기존 전략 점수/정책 의미 변경

## 3. 작업 범위
- `E:\1_Data\paper_engine.py`
- `E:\1_Data\.agent\PLANS.md`
- 산출물
  - `E:\1_Data\2_Logs\pending_entry_status_latest.json`
  - `E:\1_Data\2_Logs\paper_recovery_status_latest.json`
  - `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`

## 4. 현재 동작
- 매수는 후보/게이트/캡/섹터/리스크 필터 후 fills에 바로 기록된다.
- 매도는 sell lifecycle / recovery / validation report까지 구현되어 있다.
- 주문 전 검증 게이트, 주문 상태머신, 정정/취소 체인, 주문 SSOT, 복구 체인, 주문 검증 리포트, 시장/세션 규칙이 매수 포함 주문 단계 전체 기준으로는 구조화되어 있지 않다.

## 5. 위험 요소
- `orders(D) -> fills(D) -> ledger -> stats` 체인과 D 규칙을 흔들면 안 된다.
- 기존 buy/sell 원장 형식과 pending/recovery 문서 스키마를 깨지 않도록 추가 필드 방식으로만 붙인다.
- 실제 주문 전송처럼 보이는 동작을 만들지 않는다.

## 6. 설계
- 기존 paper 엔진의 실제 체결 로직은 유지한다.
- 새 구현은 모두 `paper_engine.py` 내부 helper + 상태 요약 문서 형태로 추가한다.
- 구현 항목
  1. 주문 전 검증 게이트 요약
  2. 주문 상태머신 요약
  3. 정정/취소 체인 모델 요약
  4. 주문/체결/원장/상태 SSOT 요약
  5. 복구/리플레이 체인 요약
  6. 주문 검증 리포트 자동화
  7. 시장/세션 규칙 검증 요약

## 7. 구현 단계
1. 공통 order artifact collector 추가
2. 1~5 요약 helper 추가
3. 6 검증 리포트 helper 추가
4. 7 시장/세션 요약 helper 추가
5. pending/recovery/report 출력 연결

## 8. 검증 계획
1. 기능 검증
- `paper_engine.py` 문법 검증
- `paper_engine.py` 직접 실행

2. 정합성 검증
- fills/trades/pending/recovery/report 숫자 일치 확인

3. 운영 반영 검증
- 최신 JSON 산출물에 신규 1~7 필드 반영 확인

4. 정책 검증
- Gate/LOCK/FAIL-CLOSED 의미 불변 확인

5. FAIL-CLOSED 검증
- 차단/경고 상태가 하드코딩 상승하지 않았는지 확인

6. 회귀 검증
- 기존 sell 1~6 summary/status 유지 확인

## 9. 롤백 계획
- `paper_engine.py` 백업 복원
- `PLANS.md` 백업 복원

## 10. 수집할 증거
- `pending_entry_status_latest.json`
- `paper_recovery_status_latest.json`
- `paper_order_validation_report_latest.json`
- `sell_validation_report_latest.json`
- 실행 stdout/stderr
