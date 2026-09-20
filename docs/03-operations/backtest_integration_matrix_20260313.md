# 백테스트 통합 분류표 (2026-03-13)

## 1) 분류표
| 분류 | 사용자 자료 | 현재 메인 로직 대응 | 정합도 | 처리 방식 | 순차 단계 |
|---|---|---|---|---|---|
| 데이터소스 구분 | 데이터소스구분.txt, api 기반 백테스트 구조.txt | `paper/trades.csv`, `12_Risk_Controlled/report_backtest_*`, `live_vs_bt_paper_daily.py` | 높음 | 즉시 적용 | S1 |
| 7단계 검증 구조 | api 기반 백테스트 구조.txt, 백테스트 목록.txt | `tools/backtest_validation_framework.py`, `run_backtest_validation_*.bat` | 높음 | 즉시 적용 | S2 |
| 최적화 제약(검증 통과 기반) | 최적화 전략.txt, 최적화전략 구현코드.txt | `optimize_if_due_v41_1.py`, `optimize_params_v41_1.py`, `stable_params_v41_1.json` | 중간 | 수정 후 적용 | S3 |
| 최적구간(robust zone) 선택 | 최적구간 정의.txt | `param_candidates_v41_1_*.json`, `stable_params_v41_1.json` | 중간 | 수정 후 적용 | S3 |
| 단계 전환(Historical→Paper→Live) | 단계별 적용.txt, 단계별 적용 구현코드.txt, 사용예시.txt | `run_paper_daily.bat`, `live_vs_bt_paper_daily.py`, `tools/build_trading_stage_validation_report.py` | 중간 | 수정 후 적용 | S4 |
| UI 탭형 백테스트 대시보드 | UI 구조.txt, 백테스트 검증 대시보드.txt | `tools/build_backtest_validation_screen.py`, 통합검증 UI(8501) | 중간 | 수정 후 적용 | S5 |
| 전체 오케스트레이션/자동화 예시 | 사용예시.txt | `full_auto.bat`, `run_paper_daily.bat`, 스케줄러 | 중간 | 수정 후 적용 | S6 |

## 2) 순차 적용 계획
- S1. 운영 스코프 고정 (샘플 제외)  
  - 상태: **완료**  
  - 반영: `PAPER_OPER_START_YMD=20260301` 기준으로 paper/live_vs_bt/kill-switch 집계 동기화
- S2. 검증 결과 표준화 (데이터소스/스코프 메타 노출)  
  - 상태: **완료**  
  - 반영: `tools/build_trading_stage_validation_report.py`에 `meta.operational_scope`/`paper.sources.operational_scope` 추가
- S3. 최적화 제약 강화 (검증 통과 조합만 후보화 + robust zone 우선)  
  - 상태: **1차 완료**  
  - 반영: `optimize_if_due_v41_1.py`에 validation gate(체크리스트 PASS/최신성/robust_ratio) 추가
- S4. 단계 전환 게이트 명문화 (Paper->Live 진입 조건 고정)  
  - 상태: **완료**  
  - 반영: `tools/build_trading_stage_validation_report.py`의 `transition_gate.paper_to_live` + `tools/build_backtest_validation_screen.py` 2단계 잠금기준 연동
- S5. UI 통합 (운영관리 화면에 백테스트 검증 탭/지표 정리)  
  - 상태: **완료**  
  - 반영: `dashboard_stock_v2.py` 운영(관리) 탭에 `백테스트 검증` 추가, 전환게이트/Go-NoGo/PASS-FAIL-NE/강건성 지표 연동
- S6. 자동화 정리 (일배치/주배치 역할 분리)
## 3) 이번 턴 실행 내역
- `run_paper_daily.bat`에 `PAPER_OPER_START_YMD` 기본값 강제
- `run_live_vs_bt_paper_daily.bat`에 동일 스코프 기본값 강제
- `tools/build_trading_stage_validation_report.py`에 운영범위 메타(`operational_scope`) 노출 추가
- `optimize_if_due_v41_1.py`에 검증 게이트(백테스트 체크리스트/robust_ratio) 추가
- `tools/build_trading_stage_validation_report.py`에 `transition_gate.paper_to_live`(고정 전환체크) 추가
- `tools/build_backtest_validation_screen.py`에 2단계 잠금기준을 `transition_gate.paper_to_live.ready` 우선 사용으로 연동

- dashboard_stock_v2.py 운영(관리) 탭에 백테스트 검증 추가(운영요약/백테스트 검증/원장/상세), 하단 상태바에 전환/백테스트 판정 반영

