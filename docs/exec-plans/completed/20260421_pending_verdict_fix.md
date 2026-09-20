# 20260421_pending_verdict_fix

## Goal
- `run_paper_daily.bat`의 `[16/16] pending_report verdict` 실패 원인을 최소 범위로 수정한다.
- 현재 `pending_entry_status_latest.json`의 `max_new_zero_reason='entry_gate_decision'`를 기존 허용 조건이 받아들이지 못하는 문제를 해소한다.

## In-Scope
- `E:\1_Data\run_paper_daily.bat`
- `E:\1_Data\PLANS.md`

## Out-of-Scope
- `paper_pending_report.py` 리포트 형식 변경
- `pending_entry_status_latest.json` 생성 로직 변경
- `paper_engine.py` 정책 의미 변경

## Current State
- 최신 `paper_pending_report_20260421_144036.json`은 `pending=7`, `active=11`, `prices_date_max=20260421`.
- 최신 `pending_entry_status_latest.json`은 `max_new=0`, `entry_ready=0`, `max_new_zero_reason='entry_gate_decision'`.
- 현재 batch verdict는 `risk_off` 문자열만 허용해 `entry_gate_decision` fail-closed를 놓치고 있다.

## Risks And Fail-Closed Checks
- 허용 범위를 과도하게 넓히면 실제 pending 이상을 숨길 수 있다.
- 따라서 허용 조건은 `max_new<=0` and `entry_ready<=0` and pending 전부 `NO_PRICES_AFTER_ENTRY_DATE` and `last_price_date_for_code == entry_date` 로 제한한다.
- 기존 `same-day pending` 허용 조건은 유지한다.

## Steps
1. verdict one-liner를 최소 범위로 수정한다.
2. 배치 재실행으로 `[16/16] pending_report verdict` 통과 여부를 확인한다.
3. 관련 로그와 최신 JSON 값을 대조한다.

## Validation
1. 기능 검증: `[16/16] pending_report verdict` 통과
2. 정합성 검증: `pending_report JSON`과 `pending_entry_status_latest.json` 조건 일치 확인
3. 운영 반영 검증: `run_paper_daily.bat` 공식 경로 재실행
4. 정책 검증: same-day pending 허용, fail-closed 의미 유지
5. FAIL-CLOSED 검증: `max_new>0` 또는 `entry_ready>0`이면 그대로 실패
6. 회귀 검증: 전체 배치 최종 `[OK] finished`

## Rollback Point
- `E:\1_Data\_bak\run_paper_daily.bat.bak_20260421_144618_pending_verdict_fix.bat`
- `E:\1_Data\_bak\PLANS.md.bak_20260421_144618_pending_verdict_fix.md`
