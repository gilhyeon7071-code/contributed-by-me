# 2026-04-24 Verification Evidence
- Latest `E:\1_Data\2_Logs\sell_validation_report_latest.json`:
  - `status=PASS`
  - `sell_fill_rows=1`
  - `sell_trade_rows=1`
  - `full_sell_rows=1`
  - `partial_sell_rows=0`
  - `issues=[]`
- Latest `E:\1_Data\2_Logs\pending_entry_status_latest.json`:
  - `sell_order_lifecycle_summary.status=PASS`
  - `partial_exit_policy_summary.status=PASS`
  - `sell_recovery_chain_summary.status=PASS`
  - `sell_adapter_summary.status=PASS`
  - `order_lifecycle_summary.status=PASS`
  - `order_chain_ssot_summary.status=PASS`
- Runtime sell row:
  - `paper\fills.csv`: `PAPER_SELL_006360_20260424_FUNDAMENTAL_CRITICAL`
  - `paper\trades.csv`: `T000277`, `exit_reason=FUNDAMENTAL_CRITICAL`

# 2026-04-23 Sell Logic Upgrade V1

## 1. 목표
- 현재 `paper_engine.py` 매도 로직을 보고서 기준으로 비교한 뒤, 실제 구현 가능한 업그레이드 범위를 6개 축으로 나눠 순차 적용한다.
- 이번 계획의 직접 목표는 `매도 판단 엔진`을 `주문/체결/원장/상태` 정합성이 더 강한 구조로 끌어올리는 것이다.

## 2. 비목표
- 브로커 실주문 OMS/EMS 전체를 한 번에 구현하지 않는다.
- KRX/NXT/SOR 실브로커 주문 라우팅 정책을 이번 단계에서 바꾸지 않는다.
- 매도 정책 의미 자체를 임의로 바꾸지 않는다.
- Gate 의미, LOCK 의미, FAIL-CLOSED 기본 의미를 바꾸지 않는다.

## 3. 작업 범위
- RootA: `E:\1_Data`
- RootB: `E:\vibe\buffett`
- 직접 대상 파일
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\paper\paper_engine_config.json` (필요할 때만 최소 수정)
  - `E:\1_Data\tests\...` 또는 `E:\1_Data\tools\...` 내 검증 보조 파일 (필요할 때만 추가)
  - `E:\1_Data\.agent\PLANS.md`
- 직접 산출물
  - `E:\1_Data\2_Logs\pending_entry_status_latest.json`
  - `E:\1_Data\2_Logs\after_close_summary_last.json`
  - `E:\1_Data\paper\fills.csv`
  - `E:\1_Data\paper\trades.csv`

## 4. 현재 동작
- 현재 매도 로직은 다층 청산 사유를 이미 지원한다.
  - `STOP`, `STOP_GAP`, `STOP_PREEMPTIVE_CLOSE`
  - `TRAIL`, `TRAIL_GAP`
  - `REVERSAL_NEXT_OPEN`
  - `SURGE_TP`, `SURGE_TP_GAP`
  - `FUNDAMENTAL_CRITICAL`
  - `TECHNICAL`
  - `MARKET_RISK`
  - `CANDIDATE_DROPOUT`
  - `TIME`
  - `DDM_LIQUIDATE_*`
- 현재 엔진은 `fills.csv` / `trades.csv` / `pending_entry_status_latest.json` / `after_close_summary_last.json` 중심으로 동작한다.
- 현재는 매도 판단은 강하지만, 주문 단위 수명주기 상태머신과 브로커형 주문 엔터티는 없다.

## 5. 위험 요소
- `orders(D) -> fills(D) -> ledger -> stats` 체인을 깨면 안 된다.
- `D 규칙`, `FAIL-CLOSED`, `Gate 의미`, `LOCK 의미`는 유지해야 한다.
- 주문/체결/원장 정합성 집계가 바뀌면 기존 대시보드/검증 산출물과 어긋날 수 있다.
- 부분청산 모델 변경은 중복 체결/중복 거래 기록을 만들 위험이 있다.

## 6. 설계

### 6-1. 1순위: 주문 단위 상태머신 추가
- 목표
  - 현재 `exit_reason` 중심 매도 결과를 `sell_order` 단위 상태로 승격한다.
- 최소 범위
  - `paper_engine.py` 내부에서만 시작
  - 새 브로커 인터페이스를 바로 강제하지 않고, paper 전용 주문 상태 레코드를 먼저 만든다.
- 기본 상태
  - `DECIDED`
  - `SUBMITTED`
  - `FILLED_PARTIAL`
  - `FILLED_FULL`
  - `CANCELED`
  - `REJECTED`
  - `RECONCILED`

### 6-2. 2순위: 체결/원장/상태 SSOT 통일
- 목표
  - 상태 문서 집계는 항상 persisted `fills/trades` 기준으로 계산한다.
- 직접 반영 대상
  - `entry_exit_lifecycle`
  - `symbol_stop_summary`
  - 이후 추가될 sell order summary

### 6-3. 3순위: 부분청산 정책 구조화
- 목표
  - `sell_ratio_pct`, `partial_exit`, `prior_stop_count`, `sell_tag`를 note 문자열에만 의존하지 않고 구조 필드로 분리한다.
- 단계
  - 우선 새 summary 문서에 구조화
  - 이후 csv 스키마 확장 여부 검토

### 6-4. 4순위: 복구 경로 강화
- 목표
  - `entry_order_id`, `source_order_id`, `replay_chain_id`, `lineage_origin`을 주문 체인 키로 명시적으로 쓰도록 강화한다.
- 비목표
  - 브로커 조회 API 연동 자체는 이번 단계에서 하지 않는다.

### 6-5. 5순위: 매도 검증 리포트 추가
- 목표
  - 사람이 `fills/trades/pending/after_close`를 수동 대조하지 않아도 되게 한다.
- 최소 리포트 항목
  - 오늘 매도 fill 수
  - 오늘 closed trade 수
  - stop 계열 매도 수
  - partial exit 수
  - state/fill/trade mismatch 수

### 6-6. 6순위: 브로커형 인터페이스 준비층
- 목표
  - 현재 paper 경로를 유지하면서 향후 실브로커 확장 포인트를 명시한다.
- 최소 인터페이스
  - `submit_sell_order`
  - `query_order_status`
  - `query_fills`
  - `reconcile_positions`

## 7. 구현 단계

### Phase 1
- sell order state summary 추가
- persisted 기준 lifecycle/stop summary와 결합

### Phase 2
- partial exit 구조 필드 분리
- 중복 방지 키 재점검

### Phase 3
- sell validation report 추가
- `pending_entry_status`와 `after_close_summary` 자동 대조

### Phase 4
- 브로커형 adapter interface 뼈대 추가
- paper adapter 기본 연결

## 8. 검증 계획
1. 기능 검증
- `paper_engine.py` 문법과 직접 실행
- sell summary / validation report 생성 확인

2. 정합성 검증
- `fills.csv`, `trades.csv`, `pending_entry_status_latest.json`, `after_close_summary_last.json` 숫자 일치

3. 운영 반영 검증
- 가능하면 `run_paper_daily.bat` 기준 확인

4. 정책 검증
- 매도 사유 의미와 Gate 의미가 바뀌지 않았는지 확인

5. FAIL-CLOSED 검증
- 집계 실패/상태 불일치 시 완화가 아니라 경고/차단 방향 유지 확인

6. 회귀 검증
- 기존 `STOP`, `STOP_GAP`, `STOP_PREEMPTIVE_CLOSE`, `TIME`, `FUNDAMENTAL_CRITICAL` 사례 재대조

## 9. 롤백 계획
- 수정 전 파일별 백업 유지
- 각 단계별로 문법/실행 실패 시 즉시 해당 파일만 롤백
- csv 스키마 확장 시 구버전 마이그레이션 없이 덮어쓰지 않는다

## 10. 수집할 증거
- `paper_engine.py` 실행 stdout/stderr
- `pending_entry_status_latest.json`
- `after_close_summary_last.json`
- `paper/fills.csv`
- `paper/trades.csv`
- 필요 시 `run_paper_daily_last.txt`
