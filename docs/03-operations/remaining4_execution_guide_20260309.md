# 남은 4개 항목 실행 가이드 (2026-03-09, Virtual-First)

## 우선순위 원칙
- 현재 상태: **가상매매 미시도**
- 따라서 실전(소액) 전환은 금지하고, 반드시 1->2->3->4 순서로 진행한다.

## 1) 장중 E2E 시나리오 자동 실행 묶음 (가상)
- 배치: `E:\1_Data\run_kis_intraday_e2e.bat`
- 권장값
  - `set E2E_MOCK=true`
  - `set E2E_ITERATIONS=3`
  - `set E2E_APPLY=0`
- 산출물: `2_Logs/kis_intraday_e2e_latest.json`

## 2) 강제 장애(fault-injection) 시나리오 자동화 (가상)
- 배치: `E:\1_Data\run_kis_fault_injection.bat`
- 통과 기준
  - fail-closed/가드/알림 내결함 케이스 PASS
- 산출물: `2_Logs/kis_fault_injection_latest.json`

## 3) 소액 실전 첫 테스트 운영 실행 (가상 통과 후)
- 배치: `E:\1_Data\run_live_canary_first_test.bat`
- 안전 게이트
  - `kis_live_canary_first_test.py`는 기본적으로
    `2_Logs/kis_intraday_e2e_latest.json` 성공 증거 없으면 `--execute`를 차단한다.
- 드라이런(권장)
  - `set CANARY_EXECUTE=0`
  - `run_live_canary_first_test.bat`
- 실제 실행
  - `set CANARY_EXECUTE=1`
  - `set CANARY_CONFIRM=LIVE_CANARY`
  - `run_live_canary_first_test.bat`

## 4) 정기 성과점검 스케줄 고정(작업스케줄러)
- 배치: `E:\1_Data\run_register_perf_review_task.bat`
- 등록 절차
  1. `set PERF_TASK_DRY_RUN=1` 후 실행
  2. `set PERF_TASK_DRY_RUN=0` 후 실행
  3. 필요 시 `set PERF_TASK_RUN_NOW=1`
- 산출물: `2_Logs/perf_task_register_latest.json`

## 사전 환경 변수(필수)
- `KIS_APP_KEY_FILE=E:\1_Data\.secrets\kis_app_key.txt`
- `KIS_APP_SECRET_FILE=E:\1_Data\.secrets\kis_app_secret.txt`
- `KIS_ACCOUNT_NO=12345678-01`
