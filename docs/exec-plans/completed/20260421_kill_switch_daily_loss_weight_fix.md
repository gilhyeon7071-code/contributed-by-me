# 20260421_kill_switch_daily_loss_weight_fix

## Goal
- `kill_switch DAILY_LOSS`가 분할청산 행을 동일 가중 평균으로 계산해 과도하게 발동되는 문제를, `paper_pnl_report.py`와 같은 일자별 가중 수익률 기준으로 정렬한다.

## Non-Goal
- `kill_switch` 임계값, `risk_off` 정책 의미, `FAIL-CLOSED`, `LOCK`, 배치 엔트리포인트는 바꾸지 않는다.
- `paper_engine.py`의 거래 생성 방식은 바꾸지 않는다.

## In-Scope
- `E:\1_Data\p0_daily_check.py`
- `E:\1_Data\PLANS.md`
- `E:\1_Data\docs\exec-plans\active\20260421_kill_switch_daily_loss_weight_fix.md`

## Current Facts
- 증거: `E:\1_Data\2_Logs\p0_daily_check_20260421_111219.json`
- 증거: `E:\1_Data\2_Logs\paper_pnl_summary_last.json`
- 증거: `E:\1_Data\2_Logs\pending_entry_status_latest.json`
- 확인값:
  - `kill_switch.metrics.last_day_ret = -0.08927559632620512`
  - `paper_pnl_summary_last.equity.last_day_ret = -0.1104741289542701`
  - `pending_entry_status_latest.max_new = 0`
  - `pending_entry_status_latest.max_new_zero_reason = risk_off`
- 사실:
  - `trades_calc.csv`의 `2026-04-21` 종료 행 12건 중 `048410`이 동일 `net_ret`로 10건 분할 기록되어 있다.
  - 현재 `p0_daily_check.py`는 `exit_date`별 단순 평균을 사용한다.
  - 현재 `paper_pnl_report.py`는 `qty * entry_price` 가중 평균을 사용한다.

## Risks
- `kill_switch` 산식 변경은 정책 민감 구간이므로 임계값 변경 없이 산식 정렬만 허용한다.
- `orders(D) -> fills(D) -> ledger -> stats` 체인을 건드리지 않도록 `p0_daily_check.py` 내 rolling 계산 블록만 수정한다.
- STOP 조건(`orders_exec 없음`, `exec_date != D`, `as_of / run_id 불일치`, `paper / broker 날짜 혼용`)이 보이면 즉시 중단한다.

## Design
- `p0_daily_check.py`의 rolling daily return 계산에서 `qty`, `entry_price`가 있으면 `qty * entry_price` 가중 평균을 사용하고, 없으면 기존 평균으로 fallback 한다.
- DD provenance 재계산도 동일 산식을 쓰도록 맞춘다.
- provenance의 `exit_ts` 컬럼도 읽을 수 있게 해 현재 스키마(`trades_calc.csv`)에서 재계산이 동작하도록 맞춘다.

## Steps
1. 실행계획 문서 생성.
2. `p0_daily_check.py` rolling 계산과 provenance 재계산 최소 수정.
3. 문법 검증.
4. 실행 검증.
5. 결과물 JSON 검증.
6. `PLANS.md` 진행 업데이트 반영.

## Validation Plan
1. 기능 검증: `p0_daily_check.py` 재실행 후 `kill_switch.metrics.last_day_ret` 값 확인
2. 정합성 검증: `paper_pnl_summary_last.equity.last_day_ret`와 일치 여부 확인
3. 운영 반영 검증: `pending_entry_status_latest.json` 또는 관련 최신 로그가 새 산식을 반영하는지 확인
4. 정책 검증: `kill_switch` 임계값/모드/정책 의미 불변 확인
5. FAIL-CLOSED 검증: `risk_off` 및 `kill_switch` 판단 경로가 예외 없이 산출되는지 확인
6. 회귀 검증: 기존 `paper_pnl_report.py` 출력과 다른 날짜에서 비정상 에러가 생기지 않는지 확인

## Rollback
- 코드 롤백 백업:
  - `E:\1_Data\_bak\p0_daily_check.py.bak_20260421_113710_kill_switch_daily_loss_weight_fix.py`
- 문서 롤백 백업:
  - `E:\1_Data\_bak\PLANS.md.bak_20260421_113710_kill_switch_daily_loss_weight_fix.md`

## Evidence To Collect
- `E:\1_Data\2_Logs\p0_daily_check_*.json`
- `E:\1_Data\2_Logs\paper_pnl_summary_last.json`
- `E:\1_Data\2_Logs\pending_entry_status_latest.json`
- 핵심 값: `last_day_ret`, `max_drawdown_pct`, `risk_off.reasons`, `max_new`, `max_new_zero_reason`, `run_id`, `as_of`
