﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿# PLANS.md

## 지금까지 작업 내용
- [x] 2026-03-27 현황판(Overview) 첫 탭 추가
- [x] 현황판 기준을 실제 산출물 기준으로 연결
  - 시스템 상태 / 활성 포지션 / 실시간 매수대기 / 시장 레짐
  - 핵심 지표 요약 / 이상 징후 즉시 확인 / 하단 안내 카드
- [x] 현황판 외부 지표 카드 추가 및 표시 방식 보강
  - KOSPI / KOSDAQ / 미국 10년 국채금리 / 한국은행 기준금리
  - 현재값 + 전일대비 변화 중심 표시로 조정
- [x] 현황판 가독성 조정
  - 상단 설명 문구 대비 보강
  - 시장 레짐 중복 표시 완화
  - 외부 지표 하단 문구 밀도 축소
  - 상하단 카드 텍스트 크기 기준 정리
- [x] 투자/운영 탭 인덱스 뒤바뀜 원인 수정
- [x] 투자 탭 `오늘 액션` 카드 기준을 실제 가상매매 기준으로 수정
- [x] 투자 탭 `비용 분석`, `누적 수익`, `성과지표`를 한국투자증권 가상매매 API 연결 시점 기준으로 보정
- [x] 포지션 히트맵 입력층 원인 해결
  - holdings_rows에 가격/손익/보유일 보강
  - 전체 보유 종목이 히트맵 조건을 만족하도록 수정
- [x] 종목 표시 공통 기준 정리
  - `종목명`과 `코드`를 분리해서 표시
  - synthetic name이 실제 종목명을 덮어쓰지 않도록 보강
- [x] 가상매매 배치 안정화
  - `run_paper_daily.bat` 단일 인스턴스 lock 적용
  - `paper_update_prices_parquet.py` timeout/retry/skip guard 적용
  - 배치 완료 후 lock cleanup 수정
  - 전체 배치 정상 완료 재검증
- [x] 새로고침 정책 파일 추가 및 부분 적용
  - [dashboard_refresh_policy.json](E:/1_Data/dashboard_refresh_policy.json)
  - 화면 자동 새로고침과 hourly state loop에 공통 정책 적용
- [x] `실전준비 보류` 원인 해결
  - 원인: `live_e2e_freshness FAIL`
  - 최신 E2E 재실행 후 [trading_stage_validation_latest.json](E:/1_Data/2_Logs/trading_stage_validation_latest.json) 기준 `실전전환 가능` 확인
- [x] 사용자 표시 알림 개수 기준을 `state.alerts`로 통일
- [x] 프로젝트 루트 [AGENTS.md](E:/1_Data/AGENTS.md) 생성 및 인코딩 정상화
- [x] 프로젝트 루트 [PLANS.md](E:/1_Data/PLANS.md) 생성 및 인코딩 정상화
- [x] 투자탭 카드 입력값 원인 해결
- [x] 투자탭 샤프 fallback 계산 연결
- [x] 투자탭 성과지표 원인분석 섹션 표시 로직 연결
- [x] 통합 점검 결과물 생성기 추가
- [x] [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) 생성 및 구조 검증
- [x] 통합 점검 결과물의 계산항목 문제 상태 표시 규칙 추가
- [x] 운영 탭에 통합 점검 결과물 화면 부착 및 화면 확인
- [x] [AGENTS.md](E:/1_Data/AGENTS.md) 로직개발 전용 기준 보강
- [x] 하위 폴더용 AGENTS 분리
  - [AGENTS.md](E:/1_Data/tools/AGENTS.md)
  - [AGENTS.md](E:/1_Data/paper/AGENTS.md)
  - [AGENTS.md](E:/1_Data/tests/AGENTS.md)
- [x] 통합 점검 결과물 실행 시 [PLANS.md](E:/1_Data/PLANS.md) 자동 갱신 스냅샷 추가
- [x] 메인 가상매매 시장판단 과승격 수정
  - `RATE_HIKE_FEAR -> CRASH` 즉시 승격 제거
  - `p0 우선 / macro 보조` 최종 판단으로 정리
- [x] sector 진입 허용 로직 분리
  - `sector_score > 0` 대신 `sector_entry_allowed = True` 사용
  - `WAIT >= 0.40`만 조건부 허용
- [x] FX 후단 필터 완화
  - `fx_score >= 0` 일괄 차단 제거
  - `EXTREME_HARD`만 하드 차단 유지
- [x] `NO_NEXT_DAY` carryover 보고/누적 문제 해결
  - pending report에 carryover queue 표시 추가
  - 오래된 queue 자동 누적 원인 제거
- [x] 최소 펀더멘털 입력층 연결
  - [paper_engine.py](E:/1_Data/paper_engine.py)에 `fundamentals`, `sector`, `asset_type` 연결
  - 최소 펀더멘털 위험 매도 분기 추가
- [x] 현재 매수 점수층에 펀더멘털 점수 연결
  - [final_score_merge_daily.py](E:/1_Data/tools/final_score_merge_daily.py)
  - [signal_integration_daily.py](E:/1_Data/tools/signal_integration_daily.py)
- [x] 후보 생성 단계 펀더멘털 직접 반영 보강
  - [generate_candidates_v41_1.py](E:/1_Data/generate_candidates_v41_1.py)
  - `BALANCED` 모드 추가
  - 치명 결함 필터 과차단 수정
  - `chosen_level`을 `L6 -> L5`로 낮춤 실검증 완료
- [x] 로직판정 `세부검증` 공통 줄바꿈/문구 가독성 보강
  - 테이블 줄바꿈 규칙 조정(`word-break: keep-all`)
  - 영문 항목/코드(`Module boundaries`, `INT_SCHEMA` 등) 사용자 문구 매핑
- [x] 로직판정 `세부검증`에 `현재값/변경값` 표 추가
  - 기준: `auto_signal_tune_candidate_latest.json`
  - 적용 이력: 최신 `paper_engine_config.change_*.json`
- [x] 로직판정 `세부검증` 화면 구조를 4블록으로 재편(적용됨)
  - `결론 -> 판단근거 -> 세부검증(운영문제/변경값/모듈진행) -> 원본근거`
- [x] `live_vs_bt` 품질게이트 임계값 정책 조정 및 실행 검증
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) `--min-oos-pf 0.80 -> 0.75` 변경
  - [live_vs_bt_feedback_latest.json](E:/1_Data/2_Logs/live_vs_bt_feedback_latest.json) 기준 `quality_gate.ok=true`, `gate_ok=true`, `optimize.executed=true` 확인
- [x] 데이터 흐름 정리 우선순위 1~5 적용 (2026-04-03)
  - 우선순위 1: `data/orders`, `data/live`의 `.bak_*`를 각 `_bak`으로 격리
  - 우선순위 2: `2_Logs` 상위 dated(non-latest) 파일을 `2_Logs/_archive/YYYYMM`으로 월별 아카이브
  - 우선순위 3: `runs`의 `SSOT_SYNC*` 중 30일 초과분을 `runs/_archive/YYYYMM`으로 이동
  - 우선순위 4: 루트 경로를 `config.yaml > roots`로 집중하고 주요 스크립트 로더를 해당 값 기반으로 전환
  - 우선순위 5: `12_Risk_Controlled` 상위 `.bak_*`를 `_bak`으로 격리
- [x] 배치 재실행 후 정책 유지 보강 (2026-04-03)
  - [dashboard_point_today.py](E:/vibe/buffett/tools/dashboard_point_today.py)에서 `config.yaml` 갱신 시 `roots` 블록 보존하도록 수정
  - 공식 배치 경로 재검증: `[16.7b/16]`, `[16.8/16]`, `[16.95/16]` 정상 완료
- [x] 보안/QA 관점 취약점 비판 점검 수행 (2026-04-04)
  - 데이터 주입/인증 우회 관점으로 배치/설정/주문 디스패치 경계 검토
  - 핵심 취약점 축 확인: `config.yaml roots 신뢰`, `PY_FORCE 실행 경로`, `BROKER_VALIDATION_MODE 우회`, `POST_CHAIN_FAIL_SOFT 성공 처리`
- [x] 후보/최종 산출물 `code+date` 중복행 원인 해결 (2026-04-06)
  - 원인: `final_score_merge_daily.py` lineage 복원 시 many-to-many merge로 행 증폭(25행)
  - 원인 해결: lineage 기준키 집약 후 merge로 수정, `generate_candidates_v41_1.py` 출력 직전 `code+date` 중복 제거
- [x] 배치 경로 재검증 완료 (2026-04-06)
  - 공식 배치 `run_paper_daily.bat` 실행 기준 `candidates_latest_data*` 전 단계 `rows=7`, `dup_code_date_groups=0`
  - 계약 로그 확인: `sector/news/final in_rows=7 out_rows=7`, `final_score ok=True`
- [x] 보유기간 분류 연계 보정 및 상태 가시성 보강 (2026-04-06)
  - [paper_engine.py](E:/1_Data/paper_engine.py) CSV 원자저장 경로에서 `code` 6자리 정규화 고정
  - [after_close_summary.py](E:/1_Data/after_close_summary.py) `candidate_status` 매칭을 6자리 코드 기준으로 통일
  - [p0_daily_check.py](E:/1_Data/p0_daily_check.py) `horizon_counts`, `horizon_hold_days(min/max)` 리포트 추가
  - [dashboard.py](E:/vibe/buffett/dashboard.py) 검증 문구를 `최대보유일 -> 기본 최대보유일`로 정리
- [x] 섹터 조인 집계 정의 불일치 원인 해결 (2026-04-06)
  - [signal_integration_daily.py](E:/1_Data/tools/signal_integration_daily.py) `phase2_sector`에 집계 정의/커버리지 필드 추가
  - 상태파일에서 후보/조인 모수 분리 확인: `candidate_rows`, `joined_rows`, `overlap_code_count`, `overlap_date_count`, `overlap_code_date_count`, `coverage_status`
- [x] P0 자동 연결 및 최종 재검증 완료 (2026-04-07)
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)에 `paper_validate`, `reconcile_paper_state_from_fills`, `check_entry_room`, `prices_update_paper_incremental` fail-closed 단계 연결
  - [paper_validate.py](E:/1_Data/paper_validate.py) `trades.csv` 운영 헤더(`pnl_krw`) 기준으로 보정 및 불일치 non-zero 반환
  - [krx_update_clean_incremental.py](E:/1_Data/krx_update_clean_incremental.py) 기본 종료일을 직전 세션 기준으로 수정해 장중 `[6.23/9]` 실패 원인 해결
  - 공식 배치 재실행에서 `[6.23/9]` 및 `[6.95a~6.95d]` 연속 통과 확인
- [x] P1 자동 연결 및 실행 재검증 완료 (2026-04-07)
  - [run_daily.bat](E:/1_Data/run_daily.bat)에 P1 core/tuner 단계(`P1.1~P1.5`) 연결
  - 괄호 블록 `%RC%` 고정확장으로 인한 WARN 오탐 원인 제거(`if errorlevel 1` 판정으로 보정)
  - [tools/build_krx_index_constituents_snapshot.py](E:/1_Data/tools/build_krx_index_constituents_snapshot.py)에서 최신 clean 소스 선택을 기준일 동률 시 `_krx_manual` 우선으로 보정
  - `krx_index_constituents_status_latest.json` 기준 `quality=PASS`, `symbol_total=350`, `fallback.used=false` 확인
  - 공식 배치 재실행(`BROKER_MODE=OFF`) 기준 P1 포함 전체 체인 `RC=0` 확인
- [x] 실측 슬리피지 체인 원인 해결 및 재검증 완료 (2026-04-08)
  - [paper_engine.py](E:/1_Data/paper_engine.py), [tools/kis_sync_fills_from_api.py](E:/1_Data/tools/kis_sync_fills_from_api.py)에서 `ref_close/slippage_actual_bps/slippage_actual_cost_krw/slippage_ref_source` 기록 경로 보강
  - [ledger_cost_model.py](E:/vibe/buffett/tools/ledger_cost_model.py)에서 legacy `ref_close` 보유행의 `slippage_ref_source` 누락 원인 수정
  - 결과물 검증: [paper_fills_ledger.csv](E:/vibe/buffett/data/ledger/paper_fills_ledger.csv) `rows=230`, `ref_close=147`, `slippage_actual_bps=147`, `slippage_actual_cost_krw=147`, `slippage_ref_source=147`
- [x] `PLANS.md` 체크리스트 `1~62` 대조 완료 (2026-04-09)
  - 대조 결과: 완료 `57건`, 미완료 `5건(55, 59, 60, 61, 62)`

## 최근 업데이트 (2026-04-13)
- [x] 실시간 진입 기준 정합화 (intraday same_close)
  - [intraday_paper_loop.py](E:/1_Data/intraday_paper_loop.py)에서 엔진 호출 시 `PAPER_INTRADAY_REALTIME_MODE=1` 전달
  - [paper_engine.py](E:/1_Data/paper_engine.py)에서 intraday 실시간 모드일 때 `signal_date=today_ymd`로 정규화
  - 결과물 검증: [intraday_loop_status_latest.json](E:/1_Data/2_Logs/intraday_loop_status_latest.json) `steps_ok=5/5`, `signal_date=20260413 effective=20260413` 확인
- [x] `run_intraday_paper.bat` 단독 실행 경로 복구 및 로그 충돌 분리
  - Python 탐지 경로 보강: `PAPER_PYTHON`, `PYTHON_EXE`, `%LOCALAPPDATA%\\Programs\\Python\\Python312/314`
  - `LOOP_ONCE=1` 실행 시 전용 로그 [run_intraday_paper_once_last.txt](E:/1_Data/2_Logs/run_intraday_paper_once_last.txt) 사용으로 상시 루프 로그 잠금 충돌 분리
  - 배치 실행 검증: `cmd /c run_intraday_paper.bat`(mock/once) `exitcode=0` 확인
- [x] 수급 데이터 소스 KIS 경로 복구 및 반영 검증 완료
  - [generate_candidates_v41_1.py](E:/1_Data/generate_candidates_v41_1.py) 수급 수집에서 KIS 투자자 흐름(`FHKST01010900`) 경로 활성화
  - `SUPPLY_KIS_MOCK=1` 기본 강제로 모의 자격증명 경로에서 수급 수집 가능 상태로 복구
  - 결과물 검증: [pykrx_supply_status_latest.json](E:/1_Data/_cache/pykrx_supply_status_latest.json) `provider=kis_fhkst01010900`, `status=FETCHED`, `filled_codes=8`
  - 결과물 검증: [candidates_latest_meta.json](E:/1_Data/2_Logs/candidates_latest_meta.json) `supply_signal_active=true`, `supply_nonnull_values=48`
- [x] 메인 배치 수급 설정 고정
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)에 `SUPPLY_PROVIDER=kis`, `SUPPLY_KIS_MOCK=1` 기본값 반영
  - 공식 배치 실행 로그에서 `[INFO] SUPPLY_PROVIDER=kis SUPPLY_KIS_MOCK=1`, `[SUPPLY] requested=8 filled=8` 확인
- [x] 급등주 ML fallback 학습 성능 개선(원인 해결)
  - [surge_ml_train.py](E:/1_Data/tools/surge_ml_train.py) fallback을 `linear_stat`에서 `linear_logreg_fallback`으로 보강
  - [surge_ml_score_realtime.py](E:/1_Data/tools/surge_ml_score_realtime.py) 신규 fallback 추론 분기 연동
  - 결과물 검증: [surge_ml_train_status_latest.json](E:/1_Data/2_Logs/surge_ml_train_status_latest.json) 기준 `valid_auc=0.6798533325081998` (`기존 0.6042298409555047` 대비 개선), `trainer=linear_logreg_fallback`
- [x] 급등주 자동 label threshold 검증/적용 루프 추가(부분 적용)
  - [surge_param_validator.py](E:/1_Data/tools/surge_param_validator.py) `apply` 단계에 `label_thr` 그리드 탐색(`0.10/0.12/0.15/0.20`) 후 최고 AUC 자동 선택 로직 반영
  - 결과물 검증: [surge_param_apply_latest.json](E:/1_Data/2_Logs/surge_param_apply_latest.json) `auto_label_grid.best_label_thr_ret5=0.2`, `best_valid_auc=0.6798533325081998`
- [x] 급등주 백테스트 임계값 설정 경로 복구 및 인코딩 원인 해결
  - 원인: [surge_params.json](E:/1_Data/paper/surge_params.json) BOM(UTF-8 BOM)으로 JSON 로더 실패 → 기본값(0.4/0.9)로 폴백
  - [surge_backtest_report.py](E:/1_Data/tools/surge_backtest_report.py), [surge_param_validator.py](E:/1_Data/tools/surge_param_validator.py), [surge_detector_realtime.py](E:/1_Data/tools/surge_detector_realtime.py)에서 `utf-8-sig` 로드로 보강
  - 결과물 검증: [surge_backtest_report_latest.json](E:/1_Data/2_Logs/surge_backtest_report_latest.json) `ml_prob_threshold=0.15`, `ml_topq_fallback.used=false` 확인
- [x] 급등주 백테스트 임계값 튜닝 결과 반영(부분 적용)
  - [surge_params.json](E:/1_Data/paper/surge_params.json)에 `bt_ml_prob_min=0.15`, `bt_ml_topq=0.9` 반영
  - 결과물 검증: [surge_backtest_report_latest.json](E:/1_Data/2_Logs/surge_backtest_report_latest.json) `rule_plus_ml signals=120, win_rate=0.4917, avg_ret5=0.0536, median_ret5=-0.00116`

## 최근 업데이트 (2026-04-15)
- [x] 로직개발 기준 반영: 일반/급등 실시간 체결 경로 명시
  - 기준: 일반 신호와 급등 신호 모두 `실시간 감지 + 실시간 체결` 원칙으로 운영
  - 범위: 당일마감 추종(CLOSE_AUCTION_UNFILLED 중심) 해석이 아닌 장중 실시간 체결 우선 경로 기준으로 정리
- [x] shadow 실시간 경로에서 `max_new=0` 직접 원인 1차 해소
  - 파일: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: shadow 실행 시 `DAILY_LOSS` 단독 사유를 hard block으로 즉시 0 처리하지 않고 kill_switch 정책 경로로 우회
  - 검증: shadow 실행 로그에서 `risk_off=True -> SHADOW ignore daily-loss hard block` 확인
- [x] shadow 엔트리 게이트 과차단 2차 해소(정책 분기)
  - 파일: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: shadow 실행에서 `gate BLOCK(일손실 kill_switch 파생)` 우회, `macro_news_guard` shadow bypass 적용
  - 검증: `pending_entry_status_shadow_latest.json` 기준 `max_new=1`, `max_new_zero_reason=""` 확인
- [x] shadow 급등 주입 경로 실행 검증
  - 검증: 엔진 실행 로그에서 `SURGE_IMMEDIATE applied=True` 확인
  - 상태: 체결은 `open_slots=9 / max_positions=9` 및 `entry_ready=0`로 미발생(원인 분리 확인 완료)
- [x] 운영 shadow 점검 파일 백업/검증 체계 적용
  - 백업: `E:\1_Data\_bak\shadow_oper_verify_20260415_144500`
  - 검증 산출물: [pending_entry_status_shadow_latest.json](E:/1_Data/2_Logs/pending_entry_status_shadow_latest.json), [market_ops_alert_shadow_latest.json](E:/1_Data/2_Logs/market_ops_alert_shadow_latest.json)

## 최근 업데이트 (2026-04-16)
- [x] 급등 실시간 경보를 `실시간형`으로 분리(원인 해결)
  - 파일: [surge_detector_realtime.py](E:/1_Data/tools/surge_detector_realtime.py)
  - 조치: `is_realtime_surge` 추가 후 `alerts`는 실시간형(`LIMIT_UP_NEAR`, `PRICE_*_BREAKOUT`)만 포함
  - 결과물 검증: [surge_realtime_latest.json](E:/1_Data/2_Logs/surge_realtime_latest.json) `alerts_count_realtime=3`, `alerts_count_all=3`
- [x] 급등 제외 정책(거래정지/경보/저품질) 반영(원인 해결)
  - 파일: [surge_detector_realtime.py](E:/1_Data/tools/surge_detector_realtime.py)
  - 조치: `KRX watchlist + candidate guard` 기반 제외(`KRX_ADMIN/WARNING/RISK/CAUTION`, `NO_TRADE_ACTIVITY`, `junk_risk`)
  - 결과물 검증: [surge_realtime_latest.csv](E:/1_Data/2_Logs/surge_realtime_latest.csv) `excluded_by_policy`, `exclude_reasons` 컬럼 생성 확인
  - 결과물 검증: [surge_realtime_latest.json](E:/1_Data/2_Logs/surge_realtime_latest.json) `policy_excluded_count=17`

## 최근 업데이트 (2026-04-16 추가)
- [x] 주문/체결 브로커 동기화 운영 반영 검증
  - 엔트리포인트: [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)
  - 배치 실행 로그에서 `kis_sync_fills_from_api.py --bridge-write` 실행 흔적 확인
  - 산출물 검증: [kis_fills_sync_20260416.json](E:/1_Data/2_Logs/kis_fills_sync_20260416.json) `bridge_write=true`, `bridge.ledger.rows_written_today=2`
  - 산출물 검증: `paper_fills_ledger.csv` 기준 `as_of=20260416` 반영 행 확인
- [x] `run_paper_daily.bat` fail-closed 종료코드 전파 보정(원인 해결)
  - 파일: [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)
  - 조치 1: 상단 stderr-capture 재호출 래퍼에서 하위 실행 실패 시 `exit /b 1` 강제
  - 조치 2: `:FAILED` 핸들러에서 `ERRORLEVEL=0` 유입 시 `FAIL_RC=1` 승격
  - 실행 검증: `PY_FORCE=BADPY` 강제 실패 테스트에서 프로세스 종료코드 `1` 확인
- [ ] 전체 정상 경로(장시간) 1회 재실행 후 `[OK] finished` + 종료코드 + fail-closed 로그 정합성 최종 확인

## 최근 업데이트 (2026-04-17)
- [x] `D` 규칙 정합성 원인 해결 및 배치 반영 검증
  - 파일: [tools/p0_onepass_from_fills.py](E:/1_Data/tools/p0_onepass_from_fills.py), [tools/derive_d_from_fills.ps1](E:/1_Data/tools/derive_d_from_fills.ps1), [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)
  - 조치: `D = fills.csv 최신 BUY ymd, 없으면 datetime 앞 8자리` 규칙으로 통일
  - 검증: [p0_orders_exec_contract_20260416.json](E:/1_Data/2_Logs/p0_orders_exec_contract_20260416.json) `status=PASS`, `exec_date_unique=["20260416"]`
- [x] 브로커 sync summary 누락 오탐 원인 해결
  - 파일: [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)
  - 원인: 같은 괄호 블록 내 `%BROKER_SYNC_SUMMARY%` 즉시확장으로 빈 경로 평가
  - 조치: summary 경로를 직접 경로식으로 검증하도록 보정
  - 검증: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) 기준 `[BROKER_SYNC_CHECK]`, `[BROKER_LEDGER_CHECK]`, `[OK] finished` 확인
- [x] `run_paper_daily.bat` 전체 정상 경로(장시간) 재검증 완료
  - 검증: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) 기준 fail-closed 구간 포함 전체 체인 완료 및 최종 종료 성공 확인
- [x] `needs_post_candidate_price_refresh.py` 분리 튜닝 시도 후 원복 + 재검증 완료
  - 파일: [tools/needs_post_candidate_price_refresh.py](E:/1_Data/tools/needs_post_candidate_price_refresh.py)
  - 이력: `0/10/11` 분기 시도 후 배치 인터페이스 불일치(`CHECK_POST_REFRESH_NEEDED`는 `0/10`만 허용) 확인
  - 최종 상태: 기존 반환 규약 `return 10 if need else 0`으로 원복
  - 검증: proxy 실행 `EXIT_CODE=10`, 로그 `need=1 missing_codes=1` 확인

## 남은 실제 문제 (2026-04-17 기준)
- [ ] 병목 튜닝 미적용 상태
  - 사실: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt)에서 `[1/9] elapsed_s=341`, `[6.26/9] elapsed_s=342`
  - 해석: `post-refresh need`가 켜질 때 `[6.26/9]`에서 `paper_update_prices_parquet.py` 전체 재실행으로 장시간 소요
  - 상태: 원인 확인만 완료, 정책/로직 변경은 미적용

## 결정사항 (2026-04-16 추가)
- 배치 실패 로그가 발생하면 상위 프로세스 종료코드는 반드시 non-zero로 전파한다(fail-closed 고정).
- 브로커 동기화 검증은 코드 존재가 아니라 배치 실행 로그와 산출물(`kis_fills_sync_*.json`, ledger `as_of` 반영)로 판정한다.

## 결정사항 (2026-04-17)
- `D` 기준은 운영 검증에서 항상 `fills.csv 최신 BUY ymd, 없으면 datetime 앞 8자리` 규칙을 우선 적용한다.
- `needs_post_candidate_price_refresh.py`와 `run_paper_daily.bat` 인터페이스는 당분간 `0/10` 반환 규약으로 유지한다.
- 병목 개선은 분리 튜닝을 바로 적용하지 않고, 인터페이스 정합성/실패전파 검증을 통과한 뒤 별도 승인 후 진행한다.

## 현재 기준 파일
- 지침 파일: [AGENTS.md](E:/1_Data/AGENTS.md)
- 진행 상태: [PLANS.md](E:/1_Data/PLANS.md)
- 통합 점검 결과물: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json)
- 대시보드 메인: [dashboard.py](E:/vibe/buffett/dashboard.py)

## 결정사항
- 현황판은 기존 [dashboard.py](E:/vibe/buffett/dashboard.py) 안의 첫 탭으로 유지한다.
- 현황판의 역할은 `한눈에 전체 상태 파악 / 빠른 네비게이션 / 핵심 지표 요약 / 이상 징후 즉시 확인`으로 고정한다.
- 현황판은 상세 분석이 아니라 요약 전용 첫 화면으로 사용한다.
- 현황판의 외부 지표는 `현재값 + 전일대비 변화` 중심으로 표시한다.
- 현황판의 `활성 포지션`은 실제 가상매매 오픈 포지션 기준을 우선 사용한다.
- 사용자에게 직접 보이는 알림 개수는 `health.alerts_count`가 아니라 `state.alerts` 기준으로 사용한다.
- 새로고침 정책은 공통 정책 파일 기준으로 관리한다.
- 새로고침 정책은 `기동 직후 1회 동기화 + 시간대별 정책` 구조로 본다.
- 시간대 정책은 아래 기준으로 유지한다.
  - 장전: 화면 OFF / 상태 갱신 30분
  - 장중: 화면 10분 / 상태 갱신 5분
  - 장후: 화면 OFF / 배치 완료 후 1회 갱신
  - 야간: 화면 OFF / 상태 갱신 60분
- `실전준비 보류`는 freshness 문제를 표시로 덮지 않고, E2E 최신성 재실행과 검증 리포트 재생성으로 같은 층에서 해결한다.
- 투자탭은 `성과 확인` 기준으로 사용한다.
- 통합 점검 결과물은 `운영 상태 점검` 기준으로 사용한다.
- 통합 점검 결과물은 새 탭이 아니라 현재 `운영` 탭 안의 읽기 전용 섹션으로 둔다.
- 계산항목에 문제가 있으면 숨기지 않고 `ISSUE / NORMAL` 상태로 표시한다.
- 로직개발 공통 기준은 루트 [AGENTS.md](E:/1_Data/AGENTS.md)로 관리한다.
- 폴더별 세부 기준은 하위 AGENTS로 분리한다.
- 새 엔진 재구축은 하지 않고 현재 분산 구조의 약한 부분만 보강한다.
- 매수 보강 우선순위는 `후보 생성 -> 점수층 -> 운영 게이트` 순으로 본다.
- 펀더멘털은 `후반 점수 보정`에만 두지 않고 후보 생성 단계에도 직접 반영한다.
- 후보 생성 기본 모드는 `BALANCED`로 유지한다.
- `치명적 결함 필터`는 항상 적용하되, 고부채 단독 차단은 사용하지 않는다.
- `KRX 429`는 즉시 재설계하지 않고 fallback + 호출부 완화 방식으로 대응한다.
- 로직판정 `세부검증`은 `결론/판단근거/세부검증/원본근거` 4블록 순서를 기본 구조로 사용한다.
- 로직판정 `세부검증`의 기본 상세 노출은 `운영문제/변경값/모듈진행` 3탭으로 제한하고, 원본 경로/규칙은 `원본근거` 접힘으로 분리한다.
- 신호체인 정규화 기준 6항목을 작업 시 인용 기준으로 고정한다.
  - 상태값 체계 정규화
  - 품질→차단 매핑 정규화
  - 단계 간 정합성 정규화
  - 제안값 적용 정책 정규화
  - 검증 완료 기준 정규화
  - 대시보드 표시 규칙 정규화
- `live_vs_bt` 자동최적화 품질게이트의 `min_oos_pf` 운영 기준은 현재 `0.75`로 사용한다.
- 운영 파일 옆 `.bak_*`는 상위 폴더에 두지 않고 해당 폴더의 `_bak` 하위로 격리한다.
- `2_Logs` 상위에는 `_latest`/`_last` 중심 파일만 유지하고, dated(non-latest) 파일은 `2_Logs/_archive/YYYYMM`으로 이동한다.
- `runs`의 `SSOT_SYNC*`는 30일 초과분을 `runs/_archive/YYYYMM`으로 정리한다.
- 루트 경로 기준은 `E:\vibe\buffett\config.yaml`의 `roots` 항목(`roota/rootb/logic_judgement_root/system_health_root`)을 우선 사용한다.
- `BROKER_VALIDATION_MODE`는 운영 주문 경로에서 기본 `0` 유지하고, 실주문(`mock=false`)과 동시 사용을 허용하지 않는 방향으로 관리한다.
- 후단 실패(`post-chain`)는 운영 기준에서 성공으로 승격하지 않고 실패로 처리하는 strict 기준을 우선한다.
- 실행 경로 주입 위험이 있는 `PY_FORCE`는 무제한 허용하지 않고 허용 경로(allowlist) 기준으로 제한하는 방향을 우선한다.
- 대시보드 최신성 판정은 실행 중 자기 파일 mtime 지연 오탐을 피하기 위해 실행시각 기준을 우선한다.
- 통합 스냅샷 `policy_drift_runtime`의 `runtime_chain_guard` 표시는 최신 대시보드 상태와 동기화해 해석한다.
- 섹터 상태 집계(`phase2_sector`)는 후보 모수와 조인 모수를 같은 값으로 간주하지 않고, `coverage_status`와 overlap 지표를 함께 확인한다.
- 보유기간 판정은 전역 `max_hold_days` 단일값만으로 해석하지 않고, `paper_state.open_positions`의 `horizon_*` 진단값을 함께 본다.
- 슬리피지 산출 기준은 `order_price` 단일 기준이 아니라 `ref_close` 기반 `slippage_actual_bps / slippage_actual_cost_krw`를 우선 근거로 사용한다.
- `1~62` 진행 질의의 기준 번호는 현재 [PLANS.md](E:/1_Data/PLANS.md) 체크리스트 순번으로 본다.
- 현재 `1~62` 미완료는 `55/59/60/61/62`로 고정해 추적한다.
- 주문/체결/잔고 브로커는 한국투자(KIS) 유지, 수급 데이터 소스도 당분간 KIS(`kis_fhkst01010900`) 경로를 우선 사용한다.
- 메인 배치 기본 수급 정책은 `SUPPLY_PROVIDER=kis`, `SUPPLY_KIS_MOCK=1`로 고정해 재기동 시 일관성을 유지한다.
- KRX OpenAPI 미제공 항목(투자자별 매매동향 종목/일별)은 복구 대상에서 제외하고, 대체 소스(KIS) 실패 시에만 보조 경로를 검토한다.
- 급등주 ML 재학습은 `surge_param_validator apply`에서 label threshold 그리드 검증 후 최고 AUC 값을 선택하는 경로를 우선 사용한다.
- 급등주 백테스트 ML 임계값은 현재 `bt_ml_prob_min=0.15`, `bt_ml_topq=0.9`를 기준값으로 유지한다.
- shadow 실시간 검증에서 `max_new=0`과 `entry_gate/macro` 하드차단은 분리 추적하고, 운영(main) 기준과 분리된 shadow 정책 분기로 관리한다.
- 2026-04-15 기준 shadow 미체결 직접 원인은 `max_new=0`이 아니라 `entry_ready=0 + open_slots=9/max_positions=9`로 본다.
- 일반/급등 신호의 기본 집행 기준은 `실시간 감지 + 실시간 체결`로 고정하고, 당일마감 추종은 예외/보조 경로로만 취급한다.
- 급등 경보는 실시간형만 경보/주입 대상으로 사용하고, 누적형(`REG_SHORT_5D60`, `REG_MID_15D100`)은 실시간 급등 경보에서 제외한다.
- 급등 제외 정책의 1차 기준은 `KRX watchlist(관리/경보/위험/주의) + 무거래 + junk_risk`로 유지한다.

## 남은 작업
- [ ] 일반/급등 `실시간 감지 + 실시간 체결` 기준의 장중 E2E 재현 검증 1세트(감지시각/주문시각/체결시각 로그 일치) 완료
- [ ] 장중 실시간 1사이클에서 급등 제외 정책(`exclude_reasons`)이 현황판 표시와 일치하는지 화면 검증
- [ ] `1~62` 미완료 5건(55/59/60/61/62) 순차 처리
- [ ] KIS 모의 자격증명 만료/재발급 시 `_cache` 키 파일 운영 절차를 문서화할지 결정
- [ ] 키움 수급 경로는 현재 보류 상태이며, 요청 시에만 활성 전환 여부를 재판단
- [x] [run_kis_intraday_e2e.bat](E:/1_Data/run_kis_intraday_e2e.bat)의 `python runtime not found` 원인 해결
- [x] [run_kis_intraday_e2e.bat](E:/1_Data/run_kis_intraday_e2e.bat) 기본 정책을 `E2E_MOCK=true`(미지정 시)로 고정
- [x] 새로고침 정책의 `기동 직후 1회 동기화`를 launch path까지 실제 적용할지 결정
  - 적용: [run_dashboard_easy.bat](E:/1_Data/run_dashboard_easy.bat)에서 `dashboard_refresh_policy.json`의 `boot_sync.enabled`를 읽어 on/off 적용
- [x] 장전/장중 시간대에서 새로고침 정책 문구와 실제 동작이 일치하는지 추가 검증
  - 반영: [dashboard_refresh_policy.json](E:/1_Data/dashboard_refresh_policy.json) 장전/장중 `state_refresh_minutes`를 `30/5`로 정렬(야간/주말 `60` 포함)
- [x] 현황판의 남은 경고 2건(`RELAX_HIGH`, `RUNTIME_CHAIN_GUARD_WARN`)을 별도 원인층에서 볼지 결정
  - 결정: 별도 신규 층은 만들지 않고 `dashboard_health` 원인층에서 유지 추적(최신 `dashboard_state_latest.json` 기준 alerts 0건)
- [x] 통합 점검 결과물 세부 표시를 더 줄일지 유지할지 결정
  - 결정: 현재 구조(접힘 + 6개 서브탭) 유지
- [x] 필요 시 `운영` 탭의 통합 점검 섹션 문구/가독성만 최소 조정
  - 결정: 현재 기준 추가 조정 불필요
- [x] 필요 시 [dashboard.py](E:/vibe/buffett/dashboard.py)가 `summary_cards`, `evidence_rows`를 직접 사용하도록 정리
  - 결정: 현재 산출물(`summary`, `evidence_paths`) 기준 사용 유지, 구조 변경은 보류
- [ ] 새 작업 발생 시 현재 결정사항과 자동 갱신 스냅샷 기준으로 이어서 진행
- [ ] 급등주 `rule_plus_ml` 중앙값(`median_ret5`) 음수 구간 해소
  - 현재값(2026-04-13): `median_ret5=-0.0011627906976744429`
  - 상태: 임계값 경로/학습 성능은 개선됐으나 전략 성과 기준 완전 충족 전
- [x] `KRX 429` 호출부 완화가 실제 fresh 수집까지 복구되는지 추가 검증
  - 검증 결과: `krx_clean date_max=20260402`는 최신이나, `crash_risk_off.metrics.status=ok_fallback_krx_clean_proxy` 및 `krx_index_constituents_status_latest.constituent_reason=login_required`가 남아 직접 수집 경로 완전 복구는 미확정
- [x] `FUNDAMENTAL_CRITICAL / FUNDAMENTAL_WARNING` 매도 분기가 실제 체결까지 가는지 검증
  - 검증 결과: `trades.csv`에 `FUNDAMENTAL_CRITICAL` 5건 체결 이력 확인, `after_close_summary_20260401_131628.csv`에 `PAPER_SELL_*_FUNDAMENTAL_CRITICAL` 주문 이력 확인, `FUNDAMENTAL_WARNING` 체결 표본은 현재 0건
- [x] [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) 최적화 수정의 전체 배치 완료 검증
  - 원인 해결: python 런타임 탐지 경로 보강 + `PY_RAW` 복구
  - 실행 검증: 공식 경로 실행 기준 `[OK] finished` 확인
- [ ] 후보 생성 `BALANCED` 모드가 다른 날짜들에서도 `L5 이하`를 유지하는지 추가 검증
  - 검증 결과(2026-04-09 재검증): `E:\vibe\buffett\runs\dashboard_state_*.json` 1,593건 중 `L5 초과` 41건 확인
  - 분포: `20260324=3건`, `20260325=23건`, `20260326=15건` (모두 `L8`)
  - 결론: 다일자 `L5 이하` 유지 미충족
- [ ] 로직판정 `세부검증` 4블록 재편 화면을 실제 캡처 기준으로 최종 검증
  - 코드 기준 확인: [dashboard.py](E:/vibe/buffett/dashboard.py)에서 `결론 -> 판단근거 -> 세부검증 -> 원본근거` 블록 순서 유지 확인(실제 캡처 검증은 미완료)
- [ ] 로직판정 `세부검증`에서 중복 정보(기존 세부표 잔여) 추가 정리 여부 결정
- [ ] shadow `entry_ready=0` 후보별 탈락 사유를 상태파일에 직접 기록할지(현행은 집계 중심) 결정
- [ ] `open_slots=9/max_positions=9` 해소 후 shadow에서 급등/일반 체결 재검증(동일 실행창 기준 1세트)
- [ ] 배치 실행 후 `orders_*.bak_*`가 상위 폴더에 재생성되지 않도록 생성 위치를 `_bak`으로 고정할지 결정
- [ ] `2_Logs` 월별 아카이브를 배치 종료 후 자동 실행 정책으로 고정할지 결정
- [ ] `config.yaml roots` 입력값에 대해 허용 루트/정규화(canonical path) 검증을 추가할지 결정
- [ ] `run_paper_daily.bat`의 `PY_FORCE`를 allowlist 기반으로 제한할지 결정
- [ ] `POST_CHAIN_FAIL_SOFT` 기본값을 운영 strict(`0`)로 전환할지 결정
- [ ] `BROKER_VALIDATION_MODE=1` + `mock=false` 조합 차단(실주문 우회 방지) 적용 여부 결정
- [ ] `stable_params` 품질게이트 미통과(`promoted=False`, `oos_ok=False`) 원인 해결
- [ ] 후보 필터 결과 `all_pass=0` 지속 원인 해결
- [ ] `CANDIDATE_DROPOUT` 연쇄 청산 패턴 원인 해결
- [x] `BROKER_CONFIRM` 장중 재진행 경로 차단/통과 검증 (부분 적용, 2026-04-09)
  - 검증 결과: `BROKER_MODE=APPLY`, `BROKER_CONFIRM` 미설정 실행에서 `[FAILED] BROKER_MODE=APPLY requires BROKER_CONFIRM=LIVE_APPLY` 확인
  - 검증 결과: `BROKER_MODE=APPLY`, `BROKER_CONFIRM=LIVE_APPLY` 실행에서 확인 차단 미발생 및 `kis_order_dispatch_from_exec.py` 단계 진입 확인
- [x] KIS 오류 구조화 필드 실오류 케이스 검증 (부분 적용, 2026-04-09)
  - 검증 결과: 의도적 인증 실패(`KIS_APP_KEY/SECRET` invalid) 실행에서 `type=KISApiError code=EGW00103 category=HTTP_ERROR status=403 path=/oauth2/tokenP` 로그 확인
- [x] 배치 경로에서 후보/최종 산출물 중복행 재발 여부 검증
  - 검증 결과: `candidates_latest_data.csv`, `with_sector_score.csv`, `with_news_score.csv`, `with_final_score.csv` 모두 `rows=7`, `dup_code_date_groups=0`
- [x] 대시보드 상태 생성기 재실행 후 `실측슬리피지 보정 총손익` 노출값 최신 반영 여부 확인
  - 실행 검증: `build_dashboard_state_v2.py` 재실행 완료(`[STATE_V2] wrote`)
  - 결과물 검증: `dashboard_state_latest.json`에 `account.slippage_ref_total_krw=3555082.5` 반영 확인
- [x] 가상매매 모드 상태표시/전환게이트 분리 보정 (2026-04-10)
  - [dashboard.py](E:/vibe/buffett/dashboard.py)에서 가상매매(`execute=False`) 시 현황판/하단 상태바 판단을 `paper.judgment` 기준으로 분리
  - [tools/build_trading_stage_validation_report.py](E:/1_Data/tools/build_trading_stage_validation_report.py)에서 실주문 미실행 시(`canary_execute_mode!=PASS`) `paper_quality_gate`, `canary_execute_mode`를 전환 차단 필수에서 분리
  - 결과: `trading_stage_validation_latest.json` 기준 `live_canary_gate=PASS`, `overall.judgment=실전준비 조건부` 유지(가상매매 운영 상태와 일치)
- [x] `run_live_vs_bt_paper_daily.bat` 임계값 경로 동기화 (2026-04-10)
  - 배치 기본 호출에 `--min-oos-pf 0.75 --min-stable-score 0` 명시해 `run_paper_daily.bat`와 기준 일치
- [x] `dashboard_state_freshness_status=stale` 오탐 원인 해결 (2026-04-10)
  - 원인: 대시보드 상태 파일 갱신 중 이전 mtime 기준으로 freshness가 계산되어 stale 판정
  - 해결: [build_dashboard_state_v2.py](E:/vibe/buffett/tools/build_dashboard_state_v2.py)에서 `dashboard_state` freshness를 실행시각 기준으로 계산
  - 검증: 공식 배치 재실행 후 [dashboard_state_latest.json](E:/vibe/buffett/runs/dashboard_state_latest.json) `dashboard_state_freshness_status=fresh`, `lag_seconds=0.0`
- [x] 통합 스냅샷 `policy_drift_runtime` 상태 불일치 원인 해결 (2026-04-10)
  - 원인: `runtime_chain_guard` 상태 문자열이 과거 WARN 문구로 잔존
  - 해결: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)에서 최신 `runtime_chain_guard_status`로 표시 동기화
  - 검증: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `policy_drift_runtime.current_state`에 `runtime_chain_guard=OK` 반영

<!-- AUTO-INTEGRATED-OPS:START -->
## 자동 갱신 스냅샷
- 생성 시각: `2026-04-17T19:09:18`
- 배치 상태: `PARTIAL`
- 메인 포지션: `-`
- pending 수: `0`
- 계산항목 ISSUE 수: `2`
- 최상위 병목: `대시보드 상태`
- 다음 조치: `현재 상단 차단 이슈가 적어 세부 로그를 확인`
- 막는 문제 건수: `4`
<!-- AUTO-INTEGRATED-OPS:END -->

## 대시보드방식변경창 작업 내용
- [x] 시스템헬스 구조를 실제 화면 기준 `요약 / 상세점검` 2탭으로 정리
- [x] 시스템헬스 > 요약을 상단 메타 블록, 결과 요약, 규칙 보기, 산출물 최신성 보기 구조로 정리
- [x] 시스템헬스 > 상세점검을 위반 사유 요약, 우측 요약표, 상세 점검 결과 보기 접힘 구조로 정리
- [x] 시스템헬스는 사용자 지시에 따라 현재 상태 유지로 확정
- [x] 운영 구조를 실제 화면 기준 `요약 / 세부검증` 2탭으로 정리
- [x] 운영 수정 중 발생한 `UnboundLocalError: _op_validation_label`, `NameError: op_validation_name_map` 오류 해결
- [x] 운영 화면 정상 표시 확인
- [x] 운영 > 요약에 상단 메타 4블록, 운영 SSOT 구조, 결과요약, 판정근거 반영
- [x] 운영 > 세부검증에 산출물 정합성 위반, 지연검증 모듈, 원본 아티팩트 보기, 운영 추적 보기 반영
- [x] 로직판정 구조를 실제 화면 기준 `요약 / 세부검증` 2탭으로 정리
- [x] 로직판정에서 `무엇을 보는가`, `현재 평가 범위`, `공통 사실 규칙`, `로직판정 규칙`을 세부검증으로 이동
- [x] 로직판정 관련 문법 검증 통과 및 8501 응답 200 확인
- [ ] 로직판정 > 요약의 항목 배치/위계 최종 정리

## 대시보드방식변경창 현재 판단
- 시스템헬스: 현재 상태 유지
- 운영: 구조 재편과 오류 수정까지 완료, 잔여 규격화만 남음
- 로직판정: 요약 과밀/위계 문제 남음

## 대시보드방식변경창 다음 진행사항
- [ ] 로직판정 > 요약에서 `10초 요약`, `5축 판정` 중심으로 재정리
- [ ] 로직판정 > 요약의 `최신 운영 문제 체크`, KPI 카드군, 5축 판정근거 위계 재조정
- [ ] 로직판정 > 세부검증 보조영역 정리
- [ ] 운영 탭 잔여 raw 표현 최종 점검

## 대시보드방식변경창 추가 진행사항 (2026-03-26)

### 지금까지 한 것
- [x] 로직판정 > 요약의 `10초 요약`을 운영/시스템헬스와 같은 3줄 구조로 재정리
- [x] 로직판정 > 요약의 `5축 판정`에서 `수준/진행률` 혼합 표시를 제거하고 `판정 결과` 중심으로 정리
- [x] 로직판정 > 요약에 `핵심 판단 기준` 3카드 구조 반영
- [x] `주식 로직의 구현 / 로직 구현의 정도 / 로직 구현의 전문성`을 5축 재매핑이 아닌 별도 계산 로직으로 분리
- [x] `로직 구현의 정도`를 `Stub/Prototype/Beta/Production/Optimized` 단계 판정으로 변경
- [x] `로직 구현의 전문성`에 하위 3축과 병목 축 설명 반영
- [x] `핵심 판단 기준` 카드의 단계/상태 표현 한글화 반영
- [x] 로직판정 > 세부검증 상단에 `핵심 판단 기준별 상세추적` 표 추가
- [x] `무엇을 보는가 / 현재 평가 범위 / 공통 사실 규칙 / 로직판정 규칙`을 `보조 기준 보기` 접힘 영역으로 정리
- [x] `최신 운영 문제 체크` 제목 복구 및 raw 컬럼 구조 정리
- [x] `카테고리 판정`에 `구현 확인율 / 실행 검증율 / 검증 근거율` 분리 표시 반영
- [x] auditor 산출물에 `impl_confirm_ratio`, `exec_verify_ratio` 필드 추가
- [x] 기존 `verification_ratio` 계산 정책과 readiness gate는 유지
- [x] auditor 재실행 후 실제 산출물 재생성 확인
- [x] `카테고리 판정`의 새 컬럼이 실제 화면에 보이는 것 확인
- [x] `5축별 세부검증 / 모듈별 개발 진행 > 열린 항목`에서 raw code 대신 설명 우선 표시 로직 적용
- [x] 오늘 작업 범위 문법 검증 통과
- [x] 오늘 작업 범위 `8501` HTTP 200 확인

### 남은 것
- [ ] `5축별 세부검증`의 설명 우선 표시가 실제 화면에서 충분히 정리됐는지 최종 확인
- [ ] `5축별 세부추적`의 내부 경로/현재 근거 노출을 더 줄일지 여부 판단
- [ ] `최신 운영 문제 체크`의 일부 raw 상태/이슈 문장을 더 사용자 문장으로 줄일지 여부 판단
- [ ] `단계별 로드맵`의 영문/혼합 표현(`in_progress`, `Live Readiness Hardening` 등) 정리 여부 판단

### 결정 사항
- `verification_ratio`는 정책값으로 유지하고, `구현 확인율 / 실행 검증율`은 보조 축으로만 추가한다.

## 미연결 로직 연결계획 (2026-04-06)

### P0 (운영 Fail-Closed 직결, 자동 연결 필요)
- `paper_validate.py`
  - 연결 대상: `run_paper_daily.bat`
  - 권장 위치: `paper_engine.py` 실행 직전(사전 스키마 검증), 실패 시 즉시 중단
- `tools/reconcile_paper_state_from_fills.py`
  - 연결 대상: `run_paper_daily.bat`
  - 권장 위치: `paper_engine.py` 실행 직전(상태 정합화), 실패 시 즉시 중단
- `tools/check_entry_room.py`
  - 연결 대상: `run_paper_daily.bat`
  - 권장 위치: `paper_engine.py` 실행 직전(진입 여력/차단 검증), 실패 시 즉시 중단
- `prices_update_paper_incremental.py`
  - 연결 대상: `run_paper_daily.bat`
  - 권장 위치: 가격 입력층 갱신 단계(기존 가격 갱신 단계 직후), 실패 시 즉시 중단

### P1 (운영 품질/리뷰 강화, 조건부 자동 연결)
- `tools/build_krx_index_constituents_snapshot.py`
  - 연결 대상: `run_daily.bat` 또는 장후 체인
  - 권장 위치: Ops Refresh Chain 전
- `tools/build_disclosure_risk_log.py`
  - 연결 대상: `run_daily.bat` 또는 장후 체인
  - 권장 위치: 뉴스/펀더멘털 수집 후, 상태 생성 전
- `tools/build_paper_parameter_review.py`
  - 연결 대상: `run_daily.bat` 또는 주간 체인
  - 권장 위치: `paper_pnl_report` 생성 후
- `tools/risk_recalibrate_from_pnl.py`
  - 연결 대상: `run_daily.bat`
  - 권장 위치: 파라미터 리뷰 후 승인형 적용(자동 강제 반영 금지)
- `tools/auto_signal_tuner.py`
  - 연결 대상: `run_daily.bat`
  - 권장 위치: 장후 배치, 승인형 적용(자동 강제 반영 금지)

### 수동/진단 유지 (자동 연결 불필요)
- `main_analysis.py`
- `tools/kis_account_snapshot.py`, `tools/kis_quote_poll.py`, `tools/kis_status_monitor.py`, `tools/kis_ssot_verify.py`
- `tools/kis_canary_run.py`, `tools/kis_mode_compare_report.py`
- `tools/redteam_check_v0.py`, `tools/redteam_check_v1.py`
- `tools/perf_review_weekly.py`, `tools/indicator_factor_diagnostic.py`
- `tools/prices_update_from_krx_clean.py`, `tools/backfill_sector_trades.py`, `tools/paper_entryday_gap_stop.py`, `tools/check_open_positions.py`

### 정리/레거시 (운영 체인 연결 불필요)
- `expert_system_v130_patch.py`
- `paper_dedupe_legacy.py`
- `build_runtime_logic_manifest_work.py`
- `system_completeness_auditor_work.py`
- `system_health_checker_work.py`
- `tools/fix11b_patch_p0_daily_check.py`
- `batch_utils.py`

### 현재 상태
- 본 항목은 `P0 실제 연결 + 공식 배치 재검증`까지 완료.
- P1은 `run_daily.bat` 연결 및 실행 재검증까지 완료(2026-04-07).
- 수동/진단/정리 항목은 계획 상태로 유지.
- `구현 존재`와 `실행 검증`은 같은 값으로 합치지 않고 분리해서 보여준다.
- `핵심 판단 기준` 3카드는 독립 판단 구조로 유지한다.
- `세부검증`은 `핵심 판단 기준별 상세추적`을 먼저 보여주고, 보조 기준은 접힘으로 내린다.
- `실제 화면 확인 전`에는 `적용됨`까지만 보고하고 `완료`로 쓰지 않는다.
- 오늘 마지막 수정인 `5축별 세부검증 / 열린 항목 설명 우선 표시`는 현재 `적용됨` 상태이며, 실제 화면 최종 확인은 다음 작업으로 넘긴다.

## 추가 진행사항 (2026-03-30)

### 지금까지 한 것
- [x] 로직판정 구현로직 차단 3항목 원인 해결
  - `LOGIC_ENTRY_EXIT_CLOSED`
  - `LOGIC_OVERRIDE_PRIORITY`
  - `LOGIC_STATE_MACHINE`
- [x] `로직 구현의 정도` 단계 판정 기준 실제 연결
  - `Stub / Prototype / Beta / Production / Optimized`
- [x] `로직 구현의 전문성` 단계 판정 기준 실제 연결
  - `Novice / Intermediate / Advanced / Expert / Research-grade`
- [x] `코드 품질 / 설계수준 / 성능최적화 / 테스트품질 / 예외처리 및 방어적 프로그래밍` 근거를 auditor 산출물에 반영
- [x] KIS 토큰 캐시 DPAPI 암호화 저장 반영
  - `implementation_maturity = Production` 실산출물 확인
- [x] `주식 로직의 구현`을 `7개 도메인 상세판정` 구조로 현실화
  - 시장 레짐 분류
  - 종목 필터
  - 포트폴리오 최적화
  - 리스크 관리
  - 백테스트
  - 데이터 파이프라인
  - 이벤트/뉴스
- [x] 로직판정 `세부검증`에 `주식 로직 도메인별 상세판정` 연결 및 현재 근거 한글화
- [x] 종목 필터 보강
  - fallback 경로 명시
  - 상태 산출물 추가
  - 종목군별 임계값 차등 반영
  - 한국시장 특화 조건 확대
  - 실제 실행 검증 완료
- [x] 포트폴리오 최적화 판정 원인 해결
  - `HardCap / max_per_sector / turnover_limit / fee_tax_slippage_model` 근거 반영
  - 최신 리포트 기준 `Production / Advanced`
- [x] 이벤트/뉴스 구현 정도 원인 해결
  - 뉴스 점수 stale 기준일 문제 수정
  - 최신 리포트 기준 `Production / Advanced`
- [x] 데이터 파이프라인 전문성 4축 기준 반영
  - `latency_quality`
  - `missing_data_handling`
  - `contract_enforcement`
  - `empirical_quality_standard`
- [x] 데이터 파이프라인 이상값 탐지식 과민 원인 해결
  - 전역 원시 IQR -> 종목별 파생 지표 기준으로 변경
  - 최신 산출물 기준 `data_pipeline_validation = PASS`
- [x] 뉴스 수집 보강
  - 종목 범위: 후보 + 보유 + 동일 섹터
  - 시간 범위: 세션별 기본 시간창 적용
  - 언론 소스 범위: 허용 언론사 tier 적용
  - DART / 매크로 바인딩 상태 반영
- [x] 지수 구성종목 입력원 부재 원인 해결
  - `로컬 산출 기본 + KRX 세션 보조` 구조 적용
  - `KOSPI200 200건 / KOSDAQ150 150건` 실제 산출물 생성
- [x] carryover 정책 개선 1~3단계 부분 반영
  - 상태값 세분화
  - 달력일 기준 -> 거래일 기준 보존
  - 재평가 로직
  - market gate 기준 실검증
- [x] 메인 `run_paper_daily` 금일 갱신 원인 해결
  - config lock sha256 fallback
  - prices parquet 재실행 순서 수정
  - `20260330` 기준 가격/후보 체인 실산출물 확인
- [x] 현황판 외부지표 미반영 원인 해결
  - `market_brief.macro_external` 연결
  - 상태 파일 기준 미국10년물 / 한국 기준금리 / 환율 값 반영 확인
- [x] 운영/전환 판단 자동 갱신 연결
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) 종료 후
  - [trading_stage_validation_latest.json](E:/1_Data/2_Logs/trading_stage_validation_latest.json)
  - [paper_fix_cycle_latest.json](E:/1_Data/2_Logs/paper_fix_cycle_latest.json)
  자동 갱신 연결
- [x] 운영/전환 판단 stale guard 반영
  - 오래된 전환 판단 파일이면 `go_live`를 그대로 쓰지 않도록 보정
- [x] 운영/전환 상위 판단 문구 정렬
  - `실전전환 가능` -> `실전환진행`
  - 운영탭 `go_live` 문구도 `실전환진행`으로 정렬
- [x] 운영탭 `운영상태 / 다음조치` 시점 정렬
  - `운영상태 = 실전환진행`
  - `다음조치 = 실전환진행`
  으로 같은 시점 문구로 맞춤
- [x] 운영탭 전환 판단 정책 보정
  - 당일 `NO_NEXT_DAY` 대기를 미해결 carryover로 보지 않도록 보정
  - 최신 산출물 기준 `paper.judgment = 운영가능` 재확인
- [x] 운영 요약탭 텍스트규격 부분 조정
  - 상단 메타 4블록
  - `결과요약` 5카드
  텍스트 위계 통일 적용

### 남은 것
- [ ] 현황판 화면에서 미국10년물 / 한국은행 기준금리 / 환율이 실제로 보이는지 화면 기준 확인
- [ ] 종목 필터 `ML 기반 탐지` 자산/모델 부재 문제는 아직 미해결
- [ ] carryover `REVALIDATE_FAILED` 실산출물은 현재 표본 기준 미검증
- [ ] carryover `sector gate` 단독 실산출물은 현재 표본 기준 미검증
- [ ] KRX 세션 직접 구성종목 수집은 `login_required` 보조 이슈로 남아 있음
- [ ] 운영 요약탭 텍스트규격이 실제 화면에서 충분히 같은 위계로 보이는지 최종 화면 확인
- [ ] `실전환진행` 문구가 운영탭/현황판에서 실제 의미와 자연스럽게 읽히는지 추가 판단

### 결정 사항
- carryover 보존 기준은 `달력일`이 아니라 `거래일 기준`으로 본다.
- `NO_NEXT_DAY` carryover는 단순 재시도가 아니라 `재평가 후 진입` 정책으로 본다.
- carryover 상태값은 `REVALIDATE_PENDING / REVALIDATE_FAILED / ENTRY_RETRY_READY / EXTEND_CARRYOVER` 구조로 확장한다.
- 지수 구성종목은 `로컬 산출 기본 + KRX 세션 보조` 구조로 운영한다.
- 현황판 외부지표는 상태 파일 `market_brief.macro_external`을 통해 공급한다.
- 전환 판단 파일은 메인 운영 배치와 분리하지 않고, [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) 종료 후 자동 갱신한다.
- 오래된 전환 판단 파일은 stale guard로 차단하고, 최신 파일만 운영 상태 문구에 반영한다.
- `go_live`의 사용자 문구는 현재 기준 `실전환진행`으로 사용한다.
- 운영탭 `운영상태`와 `다음조치`는 서로 다른 시점 문구를 섞지 않고 같은 시점 기준으로 맞춘다.

## 추가 진행사항 (2026-03-31)

### 지금까지 한 것
- [x] `NO_NEXT_DAY` 직접 원인 해결
  - `signal_date=당일`, `prices_date_max=당일`일 때 `next_trading_date()`가 구조적으로 실패하던 문제 수정
  - 다음 KRX 세션일 fallback 적용
- [x] 가상매매 진입 정책을 `익일 시가 진입`에서 `당일 종가 진입(same_close)`으로 전환
  - [paper_engine.py](E:/1_Data/paper_engine.py)
  - 설정 파일 및 lock 갱신
- [x] `same_close` 재실행 누수 원인 해결
  - 같은 날짜 재실행 시 신규 진입이 누적되던 문제 수정
- [x] 가상매매 상태/산출물 정합성 원인 해결
  - `pending_entry_status`의 `filled` 의미 왜곡 수정
  - `ENTRY_FILLED_OPEN_POSITION` 상태 해석 정리
  - same-close 체결가/상태 재구성 후 audit PASS 확인
- [x] 운영 산출물 정합성 점검 원인 해결
  - `paper_pnl_summary_last.equity.last_exit_date != live_vs_bt_feedback.run_ymd`
  - `ledger as_of < paper fills max date`
  - `dashboard_state_latest.status_overall = WARN`를 정합성 실패와 분리
- [x] 시스템헬스 남은 FAIL/WARN 원인 해결
  - `integration_contract_validation FAIL` 해결
  - deferred 경고 집계 정책 정리
  - 최신 [system_health_report.json](E:/vibe/health_chk/artifacts/system_health/system_health_report.json) 기준 `overall_status = PASS`
- [x] 후보 생성 `생성기 후보 풀 / 실행 후보 풀` 역할 분리
  - `candidate_origin / execution_pool / natural_pass` 컬럼 추가
  - `SECTOR_PREFILTER_UNION` 후보는 `execution_pool=False`
  - 엔진은 `execution_pool=True`만 실행 대상으로 사용
- [x] 후보 생성 후단 파이프라인 컬럼 누락 원인 해결
  - `candidates_latest_data.filtered.csv` 비어있지 않은 경로에서도 최신 갱신
  - `sector_score_daily.py`, `news_score_daily.py` 경유 후에도 새 컬럼 유지
- [x] `RELAX_HIGH / L4` 원인 해결
  - `L3` 공통 절단축이던 `v_accel`, `near_52w_high_gap_max` 최소 조정
  - 최신 [candidates_latest_meta.json](E:/1_Data/2_Logs/candidates_latest_meta.json) 기준 `chosen_level = L3`
  - 최신 [dashboard_state_latest.json](E:/vibe/buffett/runs/dashboard_state_latest.json) 기준 `signals.chosen_level = L3`, `relax_risk_tier = MID`, `alerts_count = 0`

### 남은 것
- [ ] 후보 생성 `BALANCED` 모드가 다른 날짜들에서도 `L3~L5` 범위에서 안정적으로 유지되는지 추가 검증
- [ ] `execution_pool=True` 후보 4건 중 실제 최종 진입 후보가 2건으로 줄어드는 현재 구조가 적정한지 추가 검증
- [ ] `POSCO홀딩스`처럼 후단 일부 산출물에서 `candidate_origin / execution_pool / natural_pass`가 비는 사례가 다시 발생하는지 추가 확인
- [ ] 뉴스 수집 `naver_quota_exceeded(HTTP 429)`는 원인 표기까지 정리됐으나, 실제 fresh 수집 복구는 별도 확인 필요
- [ ] 현재 `same_close` 정책 기준의 표본 축적/품질게이트는 워밍업 구간이라 추가 표본 검증 필요

### 결정 사항
- 가상매매 진입 정책은 현재 기준 `same_close(당일 종가 진입)`으로 운영한다.
- `NO_NEXT_DAY`는 정책 예외로 덮지 않고, 먼저 진입일 계산 원인을 해결한다.
- 후보 생성은 `검증용 후보 풀`과 `실행 후보 풀`을 분리해서 본다.
- `SECTOR_PREFILTER_UNION` 후보는 화면/검증용으로는 남기되, 실행 후보로 직접 사용하지 않는다.
- 후보 수 부족 문제는 `보정 후보 추가`로 덮지 않고 `자연 통과 수`를 먼저 늘리는 방향으로 해결한다.
- `RELAX_HIGH / L4`는 표시 조정이 아니라 후보 생성 절단축 원인 해결로 낮춘다.

## 추가 진행사항 (2026-04-01)

### 지금까지 한 것
- [x] 로직판정 런타임 체인 FAIL/PARTIAL 원인 해결
  - 최신 [system_completeness_report.json](E:/vibe/checking_logic/artifacts/system_completeness/system_completeness_report.json) 기준
  - `runtime_problem_detection = PASS`
  - runtime checks 집계 `fail=0`, `partial=0`
- [x] 배치 실행시간/중복실행 원인 보정
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) 단계별 `elapsed_s` 로그 추가
  - 후보 산출물 hash 비교 + 가격 최신성 판정 기반으로 `6.26/6.3/6.4` 조건부 재실행 적용
  - [needs_post_candidate_price_refresh.py](E:/1_Data/tools/needs_post_candidate_price_refresh.py) 추가
  - 최신 실행 로그 기준 `POST_REFRESH_CHECK need=0`에서 `6.26` 스킵 확인
  - 최신 [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) 기준 `[OK] finished` 확인
- [x] 후보 생성 갱신 병목 보정 (`[6.25/9]`)
  - [decide_candidate_refresh_flags.py](E:/1_Data/tools/decide_candidate_refresh_flags.py) 추가
  - 같은 날짜 재실행 시 `FUND_DART_REFRESH=0`, `FUND_KRX_WATCH_REFRESH=0` 자동 적용
  - 최신 실행 로그 기준 `[6.25/9] elapsed 30~32s -> 22s` 확인
- [x] 인디케이터 진단 병목 보정 (`[15.5/16]`)
  - [indicator_diag_and_recommend.py](E:/1_Data/tools/indicator_diag_and_recommend.py) same-day skip 적용
  - [indicator_diag_recommend_meta_latest.json](E:/1_Data/2_Logs/indicator_diag_recommend_meta_latest.json) 기준으로
    당일 동일 파라미터 재실행 시 `indicator diag` 재계산 스킵
  - 강제 재실행은 `INDICATOR_DIAG_FORCE=1`로 가능
  - 최신 실행 로그 기준 `[15.5/16]` 구간 `RUN 3회` -> `[SKIP]` 전환 확인
  - 배치 총 소요시간 기준 약 `175s -> 117s` 단축 확인
- [x] 설정파일 불필요 rewrite 원인 해결
  - [paper_update_prices_parquet.py](E:/1_Data/paper_update_prices_parquet.py)에서 설정값이 실제 변경될 때만 저장
- [x] 가격 업데이트 병목 원인 해결 (same-day fast-skip)
  - [paper_update_prices_parquet.py](E:/1_Data/paper_update_prices_parquet.py)에서
  - `parquet date_max=당일` + `코드 누락 없음`이면 재수집 없이 즉시 종료
  - 최신 배치 로그 기준 `[1/9]` elapsed `136s -> 2s` 확인
- [x] config lock 해시/JSON 파싱 취약 경로 보정
  - [task_00_config_lock.bat](E:/1_Data/tasks/task_00_config_lock.bat) fallback 경로 강화
- [x] same-close 감사 판정 기준 보정
  - [audit_daily.py](E:/1_Data/audit_daily.py) `_is_same_close_entry` 판정식 수정
- [x] 배치 말단 중단 원인 보정
  - mock 모드에서 KIS preflight 실패 시 배치를 중단하지 않고 경고 후 계속 진행하도록 보정

### 남은 것
- [ ] 런타임 PASS 상태가 다음 거래일 배치에서도 동일하게 유지되는지 1회 추가 확인
- [ ] 단계별 `elapsed_s` 기준 상위 병목 단계(현재 [6.25/9] candidate refresh) 추가 축소는 별도 차수에서 진행 필요

### 결정 사항
- 로직판정 런타임 영역은 `표시 보정`이 아니라 `원인 분기` 기준으로 PASS/FAIL을 판단한다.
- 배치 시간 개선은 기능 변경 없이 `조건부 재실행 억제 + 단계시간 계측` 방식으로 진행한다.

## 추가 진행사항 (2026-04-02)

### 지금까지 한 것
- [x] 오늘 대화 범위를 `목록화 전용`으로 고정하고 실행/수정 없이 항목 정리 진행
- [x] 보강/업그레이드/추가 요구사항을 주제별로 분류
  - 기술적 신호
  - 뉴스 신호 체계
  - 거시/금리 신호
  - 캘린더/장중 시간대 필터
  - 이벤트 게이트
  - 시스템/데이터 헬스
  - 운영 정책/정규화/검증
- [x] 누락 신호 확인 사항을 목록에 반영
  - `스토캐스틱`, `볼린저밴드`, `OBV`, `독립 SMA/EMA 추세룰`
- [x] 우선순위 목록(P0/P1/P2) 초안 정리

### 남은 것
- [x] 오늘 정리한 항목의 `임계값/창길이/차단수준` 수치 결정
  - 수치 결정표(현재 운영값 고정):
  
| 축 | 항목 | 임계값/창길이 | 차단수준 |
|---|---|---|---|
| P0 | Cross-source Integrity | `max_skew_days=1` | required source 불일치 시 차단 |
| P0 | Sigma Outlier | `lookback_files=20`, `min_history=5`, `zscore_caution=3.0`, `zscore_block=5.0` | `zscore>=5.0` 차단 |
| P0 | Execution Health (Slippage/Latency) | `slippage_lookback_rows=30`, `slippage_bps_caution=10`, `slippage_bps_block=20` | `>=20bps` 차단 |
| P0 | Macro/News Guard | `macro_stale_ratio_caution=0.5`, `macro_stale_ratio_block=0.8`, `macro_critical_bad_block=1` | `critical_bad>=1` 또는 stale `>=0.8` 차단 |
| P0 | News Quota/Quality | 품질 상태 평가 | `news_quality in FAIL/BLOCK/ERROR` 차단 |
| P1 | Calendar Gate | 옵션만기주 `week=2`, `weekday=3`, `max_new_cap=1` | reduce cap `1` |
| P1 | Intraday Gate | 오전 `0900-1000`, 점심 `1130-1330`, 파워아워 `1430-1530`, 오전 `sector_strength_min=0.8` | 점심 cap `1` |
| P1 | Event Gate | `high_risk_levels=HIGH/CRITICAL` | `REDUCE`, `max_new_cap=1` |
| P1 | Technical 4-signal | `min_pool=8`, `stoch_k=20~85`, `boll_mid>=0`, `obv_slope>=0`, `sma_ema_ok_ratio>=0.5` | low_quality 시 `REDUCE`, cap `1` |
| P2 | News NER/Trace | `max_lag_days=2` | lag 초과 시 stale 처리 |
| P2 | News Time-decay | `half_life_hours=36`, `max_age_hours=72` | max_age 초과는 가중치 0 |
| P2 | Dynamic News Weight | `target_mapped_rate=0.8`, `target_nonzero_rate=0.2`, lag factor(`1d=0.85`,`>=2d=0.70`) | factor 하락으로 news 가중치 축소 |
| P2 | BERT | 현행 미도입 | 보류 |
  - 기준 경로: [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json), [news_score_status_latest.json](E:/1_Data/2_Logs/news_score_status_latest.json), [final_score_merge_status_latest.json](E:/1_Data/2_Logs/final_score_merge_status_latest.json), [news_score_daily.py](E:/1_Data/tools/news_score_daily.py), [final_score_merge_daily.py](E:/1_Data/tools/final_score_merge_daily.py)
- [x] P0 항목(무결성/이상치/체결건강/거시금리/뉴스쿼터)의 정책값 확정
  - 확정 경로: [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json) `cross_source_integrity / sigma_outlier_guard / execution_health_guard / macro_news_guard`
- [x] P1 항목(캘린더/시간대/이벤트/기술신호 4종)의 정책값 확정
  - 확정 경로: [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json) `p1_entry_policy.calendar/intraday/event_gate/technical_gate`
- [x] P2 항목(뉴스 NER/역추적/동적가중치/BERT)의 도입 범위와 차수 결정
  - 1차 운영범위: [news_score_status_latest.json](E:/1_Data/2_Logs/news_score_status_latest.json) `ner_summary + trace`, [final_score_merge_status_latest.json](E:/1_Data/2_Logs/final_score_merge_status_latest.json) `news_dynamic_weight`
  - 2차 도입범위: `BERT`는 현행 코드 미도입(보류)
- [x] 확정 정책을 `AGENTS.md 정규화 기준 6항목`과 일치시키는 검증 기준표 작성
  - 검증 기준표:
  
| 정규화 기준(AGENTS) | 확인 산출물 | 검증 기준 |
|---|---|---|
| 상태값 체계 정규화 | [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `stage_status[].status` | 상태값이 `PASS/FAIL/PARTIAL/WARN/NOT_EVALUABLE` 체계로 기록되고 차단 항목은 `blocking_issues`에 분리 기록 |
| 품질→차단 매핑 정규화 | [final_score_merge_status_latest.json](E:/1_Data/2_Logs/final_score_merge_status_latest.json) `news_gate` | 품질 상태가 `OPEN/REDUCE/BLOCK` 게이트 의사결정으로 승격되어 반영 |
| 단계 간 정합성 정규화 | [signal_integration_status_20260403.json](E:/1_Data/2_Logs/signal_integration_status_20260403.json), [p0_daily_check_20260403_151634.json](E:/1_Data/2_Logs/p0_daily_check_20260403_151634.json) | `asof_ymd/regime/universe` 불일치 여부를 점검하고 불일치 시 `FAIL/PARTIAL`로 기록 |
| 제안값 적용 정책 정규화 | [auto_signal_tune_candidate_latest.json](E:/1_Data/2_Logs/auto_signal_tune_candidate_latest.json), [paper_engine_config.used_20260403_151239.json](E:/1_Data/2_Logs/paper_engine_config.used_20260403_151239.json), [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json) | `현재값/변경값(제안)/변경값(적용후)`를 분리 기록하고 미적용 사유를 남김 |
| 검증 완료 기준 정규화 | [trading_stage_validation_latest.json](E:/1_Data/2_Logs/trading_stage_validation_latest.json), [paper_fix_cycle_latest.json](E:/1_Data/2_Logs/paper_fix_cycle_latest.json) | `감지→승격→차단→재현검증` 확인 전에는 완료로 보지 않고, 재현검증 후에만 해결 판정 |
| 대시보드 표시 규칙 정규화 | [dashboard_state_latest.json](E:/vibe/buffett/runs/dashboard_state_latest.json), [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) | 화면 `정상` 표시가 실제 차단 상태와 모순되지 않아야 하며, 모순 시 실행 상태 기준으로 판정 |

### 수치결정표 1:1 검증 결과 (2026-04-03)
- [x] 최신 실산출물 기준 대조 완료
  
| 축 | 항목 | 검증 결과 | 근거 |
|---|---|---|---|
| P0 | Cross-source Integrity | 확인됨 | [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `stage_status=PASS`, `calc_issue_state=NORMAL` |
| P0 | Sigma Outlier | 부분 확인 | 임계값은 [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json)에서 확인, 최신 요약 산출물에 zscore 실측치는 미노출 |
| P0 | Execution Health (Slippage/Latency) | 부분 확인 | 임계값은 [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json)에서 확인, 최신 요약 산출물에 slippage bps 실측치는 미노출 |
| P0 | Macro/News Guard | 확인됨 | [p1_entry_gate_status_latest.json](E:/1_Data/2_Logs/p1_entry_gate_status_latest.json) `macro_critical_bad:1`, [gate_daily_20260403.json](E:/1_Data/2_Logs/gate_daily_20260403.json) |
| P1 | Calendar/Intraday/Event/Technical | 확인됨 | [p1_entry_gate_status_latest.json](E:/1_Data/2_Logs/p1_entry_gate_status_latest.json) `*_enabled=true`, 윈도우/파일 존재 확인 |
| P2 | News NER/Trace/Time-decay | 확인됨 | [news_score_status_latest.json](E:/1_Data/2_Logs/news_score_status_latest.json) `max_lag_days=2`, `half_life_hours=36`, `max_age_hours=72`, `ner_summary`, `trace` |
| P2 | Dynamic News Weight | 확인됨 | [final_score_merge_status_latest.json](E:/1_Data/2_Logs/final_score_merge_status_latest.json) `target_mapped_rate=0.8`, `target_nonzero_rate=0.2` |
| P2 | BERT | 확인됨(미도입) | 운영 코드 검색 기준 미도입 유지 |

### 결정 사항
- 오늘 작업은 `대화 기반 목록화`까지만 진행하고 코드/화면 수정은 진행하지 않는다.
- 우선순위는 `P0 -> P1 -> P2` 순서로 본다.
- 현재 기준 누락 핵심 기술 신호는 `스토캐스틱/볼린저밴드/OBV/독립 SMA-EMA 추세룰`로 관리한다.
- 거시/금리 축은 `Yield Curve(10Y-2Y) + Real Yield + HY Spread spike`를 별도 게이트 축으로 본다.
- 시간성 축은 `캘린더 효과 + 장중 시간대 필터 + 이벤트 게이트`를 분리해서 관리한다.
- 시스템 헬스 축은 `Cross-source Integrity + Sigma Outlier + Slippage/Latency + Global Outlier Watcher`를 핵심으로 본다.

## 추가 진행사항 (2026-04-02, P1 실행)

### 현재 완료된 것
- [x] P1-1: 캘린더/시간대/이벤트/기술신호 정책 게이트를 엔진에 반영
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
- [x] P1-2: P1 latest 상태 산출물 생성
  - 산출물: [p1_entry_gate_status_latest.json](E:/1_Data/2_Logs/p1_entry_gate_status_latest.json)
- [x] P1-3: 누락 입력(`missing_inputs`) 기록 반영
- [x] P1-4: 세부 상태(`states`) 기록 반영
- [x] P1-5: history CSV 누적 기록 반영
  - 산출물: [p1_entry_gate_status_history.csv](E:/1_Data/2_Logs/p1_entry_gate_status_history.csv)
- [x] P1-6: main/shadow 구분(`run_label`) 기록 반영 + history 스키마 정규화
- [x] P1-7: 운영 노출 경로 반영
  - 대상: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)
  - 산출물: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `stage_status`에 `p1_entry_gate` 노출 확인

### 남은 실제 문제
- [x] `entry_gate_decision_before_p1`이 실제로 `unknown` 또는 공백으로 발생한 런에서, 스냅샷 `decision=판정보류` 자동 표시 실데이터 검증 완료
  - 검증 산출물: [integrated_ops_snapshot_20260402_143023.json](E:/1_Data/2_Logs/integrated_ops_snapshot_20260402_143023.json)

### 다음 진행사항
- [x] 테스트값 원복 후 최신 스냅샷 재생성 완료
  - 원복 산출물: [p1_entry_gate_status_latest.json](E:/1_Data/2_Logs/p1_entry_gate_status_latest.json), [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json)

## 추가 진행사항 (2026-04-02, 종가진입 보완사항 GAP 체크)

### 현재 완료된 것
- [x] 종가진입 보완사항(1~5) 구현 현황 점검 완료
  - 점검 기준: 요청안(단일가/거래대금/슬리피지/폴백/분할진입/품질필터) vs 현재 코드/설정/로그
  - 점검 대상:
    - [paper_engine.py](E:/1_Data/paper_engine.py)
    - [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json)
    - [kis_order_dispatch_from_exec.py](E:/1_Data/tools/kis_order_dispatch_from_exec.py)
    - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)
    - [liquidity_filter_daily.py](E:/1_Data/liquidity_filter_daily.py)
    - [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt)
    - [pending_entry_status_latest.json](E:/1_Data/2_Logs/pending_entry_status_latest.json)
- [x] 항목별 GAP 판정 정리
  - 1) 단일가+지정가 실행 로직: 부분 구현
    - `entry_timing_mode=same_close` 및 `15:20:00` 시각 기록은 존재
    - `15:20 이전 지정가 주문/종가예상가 기준 주문/단일가 미체결 제어`는 미구현
    - 배치 브로커 호출은 `--order-type` 미지정(디스패처 기본 market)
  - 2) 슬리피지 허용범위(시총별 차등): 미구현
    - 전역 `slippage_pct` + 사후 bps 건강도 가드만 존재
    - 체결 시점 허용범위 초과 취소 로직 없음
  - 3) 종가 미체결 폴백(4단계): 미구현
    - 현재는 carryover 재검증/만기 기반(`carryover_max_age_days=2`) 중심
    - 요청안의 가격/시간 조건 폴백 체계 미구현
  - 4) 분할 진입 50/50 + 눌림목 조건: 미구현
    - 진입 수량은 단일 계산, 2차 진입 규칙 없음
  - 5) 신호 품질 강화 필터: 부분 구현
    - 구현: RSI/MACD/volcorr 기술게이트(P1)
    - 미구현: `거래량/20일평균>=1.5`, `종가위치>=0.7`, `20일선 이격도 +10%`, `당일 변동폭 7%`, `당일 공시 이벤트`

### 남은 실제 문제
- [ ] 요청안 기준으로는 종가진입 체계 핵심(지정가/미체결 폴백/분할진입/시총차등 슬리피지)이 아직 미구현
- [ ] 현재 구현은 `same_close 체결 가정` 성격이 강해, 주문 미체결/가격 허용폭 통제가 부족

### 다음 진행사항
- [ ] (대기) 사용자 승인 후 1번부터 순차 구현
  - 1순위: 단일가 지정가 주문 + 미체결 폴백 상태머신

## 추가 진행사항 (2026-04-02, gross_cap 재계산 반영)

### 현재 완료된 것
- [x] `current_open_notional` 기준을 `entry_price` 합산이 아니라 `현재가(마지막 종가, 없으면 entry_price fallback)` 기준으로 계산하도록 정리
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
- [x] 부분/전체 청산 이후 `gross_cap` 기준 노셔널 재계산 로직 반영
  - `open_notional_after_exit` 계산 후 `ops_alert`에 반영
  - `current_open_notional`을 재계산값으로 갱신해 후속 상태 산출물 기준 통일
- [x] 재계산 적용 여부 추적 필드 추가
  - `gross_recalc_applied`, `gross_recalc_delta`
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py` 통과
  - 실행: `run_paper_daily.bat` 전체 완료(`[OK] finished`)
  - 산출물: [market_ops_alert_latest.json](E:/1_Data/2_Logs/market_ops_alert_latest.json), [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) 확인

### 남은 실제 문제
- [ ] 최신 실행 표본에서는 부분청산 발생 구간이 없어 `gross_recalc_delta`가 `0.0`인 케이스만 확인됨
- [ ] 부분청산 발생일 표본 1건에서 `open_notional_after_exit < open_notional_before_entry` 실증 확인 필요

### 결정 사항
- `gross_cap` 관련 운영 기준 노셔널은 `entry_price`가 아니라 `현재가(마지막 종가)` 기준으로 본다.
- 부분청산 이후 gross 여력(`headroom/util`)은 청산 반영 후 재계산값을 기준으로 기록/노출한다.

## 추가 진행사항 (2026-04-03, 운영 흐름/판정 오탐 정리)

### 지금까지 한 것
- [x] 배치 데이터 흐름 효율 보정
  - [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat)에서 대시보드 상태 중복 생성(`16.8b`) 제거
  - [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)에 `run_ymd` 기반 동일일자 우선 결합 추가
  - 배치 실행 시 `INTEGRATED_OPS_UPDATE_PLANS=0`로 `PLANS.md` 자동쓰기 분리
- [x] 배치 차단 원인 분리 해결
  - `CONFIG LOCK MISMATCH` 원인: lock hash 불일치 → [paper_engine_config_lock.py](E:/1_Data/tools/paper_engine_config_lock.py) `init`으로 동기화
  - `JSONDecodeError(UTF-8 BOM)` 원인: [paper_engine_config.json](E:/1_Data/paper/paper_engine_config.json) BOM 인코딩 → UTF-8(BOM 없음) 정규화
- [x] 공식 경로 실행 검증
  - `cmd /c run_paper_daily.bat` 기준 `[OK] finished` 확인
  - 최신 산출물: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json)
- [x] `policy_drift_runtime` 오탐 원인 해결
  - 원인: `final_score_merge_status.weights`에서 `news_base`(참고값)까지 합산해 `weight_sum=1.1000`으로 PARTIAL 판정
  - 수정: [system_completeness_auditor.py](E:/vibe/checking_logic/system_completeness_auditor.py)에서 유효 가중치(`news`)만 합산
  - 재검증: [system_completeness_report.json](E:/vibe/checking_logic/artifacts/system_completeness/system_completeness_report.json) 기준 `policy_drift_runtime=PASS`, `weight_sum=1.0000`

### 남은 실제 문제
- [ ] 외부 실행으로 `run_paper_daily_last.txt`가 실패 로그로 덮일 때 `batch_execution`이 다시 PARTIAL로 상승
  - 기준 로그 파일: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt)
- [ ] `pending_entry_flow`의 `lifecycle=FAIL` 상태 원인 분리 필요
  - 기준 산출물: [pending_entry_status_latest.json](E:/1_Data/2_Logs/pending_entry_status_latest.json)

### 결정 사항
- `policy_drift_runtime` 판정의 `weight_sum`은 실제 런타임 가중치 기준으로만 계산한다(`news_base` 제외).
- 배치 실행 시 `integrated_ops_snapshot`는 운영 산출물 생성에만 집중하고 `PLANS.md` 자동 갱신은 기본 비활성으로 둔다.
- 대시보드 최신 상태는 배치 1회 생성 원칙으로 유지한다(중복 writer 금지).

## 추가 진행사항 (2026-04-04, checking_logic 커버리지 점검)

### 지금까지 한 것
- [x] checking_logic 점검 범위 실측 검증
  - 기준 파일:
    - [system_completeness_auditor.py](E:/vibe/checking_logic/system_completeness_auditor.py)
    - [build_runtime_logic_manifest.py](E:/vibe/checking_logic/build_runtime_logic_manifest.py)
  - 운영 분모:
    - `E:\1_Data\*.py` 208개
    - `E:\1_Data\tools\*.py` 83개
    - 합계 291개
  - 점검 참조/힌트 경로 29개 중 운영 파일 교집합 17개 확인
  - 현재 실커버리지 `5.84%` 확인
- [x] 핵심 미커버 파일 교차확인
  - `gate_daily.py`, `p0_daily_check.py`, `generate_candidates_v41_1.py`, `after_close_summary.py`, `audit_daily.py`, `liquidity_filter_daily.py`, `live_vs_bt_paper_daily.py`
  - `tools/kis_order_dispatch_from_exec.py`, `tools/kis_emergency_liquidate.py`, `tools/kis_cancel_open_orders.py`, `tools/ledger_append_from_orders_exec.py`
  - `market_guard.py`, `tools/kill_switch_validation_report.py`, `tools/integrity_gate_enforce.py`, `tools/paper_entryday_gap_stop.py`, `tools/kis_ssot_verify.py`
  - `tools/final_score_merge_daily.py`, `tools/sector_score_daily.py`, `tools/prices_update_from_krx_clean.py`, `tools/build_dart_fundamental_snapshot.py`, `tools/vibe_onepass_run.py`
  - 위 파일군 모두 현재 `MISSING` 확인

### 남은 실제 문제
- [ ] checking_logic 점검 범위가 실제 운영 루틴/주문체결/리스크/데이터 파이프라인 핵심 파일을 대부분 포함하지 못함
- [ ] 커버리지 기준(분모/분자)을 운영 파일군 중심으로 고정하고, 이후 증설 시 동일 방식으로 재검증 필요

### 결정 사항
- 커버리지 판단은 `auditor+manifest 참조 경로`와 `실제 운영 파일군(E:\1_Data 루트+tools)`의 교집합 기준으로 계산한다.
- 현재 상태를 전체 점검 완료로 보지 않고 `부분 점검`으로 분류한다.
- 다음 커버리지 확장은 `P0(운영/주문체결) -> P1(리스크/무결성) -> P2(데이터 파이프라인)` 순서로 진행한다.

## 추가 진행사항 (2026-04-03, 로직판정 모듈 진행률/5축 검증 정렬)

### 지금까지 한 것
- [x] `module_progress` 과대평가 원인 수정 적용
  - 대상: [system_completeness_auditor.py](E:/vibe/checking_logic/system_completeness_auditor.py)
  - `max_score=0` 모듈의 진행률을 `1.0` 처리하던 경로를 `0.0/UNKNOWN`으로 보정
  - `require_module` 단순 연결만으로 `verified=True, completion=1.0` 처리하던 경로를 보수 평가로 보정
- [x] 카테고리 검증률 분리(`구현 확인율/실행 검증율/검증 근거율`) 유지 상태에서 impl 과대집계 보정
  - `module_presence`는 `impl_confirm_ratio` 집계에서 제외
- [x] 전역 연동성(`global_integration`) 과대평가 보정
  - 기존 `ratio*0.7 + wiring_ratio*0.3`에서
  - 보수 기준 `min(카테고리 검증율, wiring_ratio)`로 변경
- [x] 3단계 검증 완료
  - 문법: `py_compile` 통과
  - 실행: `system_completeness_auditor.py --manifest E:\checking_logic\system_audit_manifest.runtime_auto.json` 통과
  - 산출물: [system_completeness_report.json](E:/vibe/checking_logic/artifacts/system_completeness/system_completeness_report.json) 재생성 확인
- [x] 현재 모듈 진행률 실검증
  - `data_pipeline 0.8909`, `risk_filter 0.8286`, `regime_classifier 0.8`, `optimizer 0.82`, `backtest_pipeline 0.95`, `execution_engine 0.939`, `risk_manager 0.8`, `orchestrator 0.8992`, `system_health_checker 0.0(UNKNOWN)` 확인

### 남은 실제 문제
- [ ] `global_integration`은 보정 후에도 `0.8889(STRONG)`으로 높게 유지되어, `wiring_status` 산정 자체의 보수화가 추가로 필요한지 판단 필요
- [ ] 리포트 내 일부 한글 라벨 인코딩 깨짐 문자열이 남아 있어 화면 문구 정합성 추가 점검 필요

### 결정 사항
- 모듈 연결 존재 여부(`module_presence`)와 실제 검증 완료(`verified`)는 동일하게 취급하지 않는다.
- `max_score=0`은 완료로 승격하지 않고 `UNKNOWN`으로 분리한다.
- 5축 판정은 진행률(`score/max_score`)이 아니라 검증 근거율(`verification_ratio`) 기반으로 계산한다.
- 전역 연동성은 가중 평균으로 상향 보정하지 않고 보수 기준(`min`)으로 제한한다.

## 추가 진행사항 (2026-04-04, paper_engine 유지보수 리팩토링)

### 현재 완료된 것
- [x] 진입 루프 분리(원인: `main` 단일 함수 책임 과대)
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `main` 내 `for _, r in cdf.iterrows()` 블록을 `_process_entry_rows(...)`로 분리
- [x] replay consistency 후처리 분리(원인: 상태 동기화/복구 로직이 진입/청산 루프와 결합)
  - 조치: `_refresh_replay_consistency(...)` 신설, `main`에서는 결과 수신만 수행
- [x] gross cap 재계산 블록 분리(원인: 계산/알림/출력 혼재)
  - 조치: `_recalculate_open_notional_and_alert(...)` 신설
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py` 통과
  - 실행: `python -c "import paper_engine; print('import_ok')"` 통과
  - 결과물(코드 상태): `_process_entry_rows`, `_refresh_replay_consistency`, `_recalculate_open_notional_and_alert` 정의/호출 라인 확인

### 남은 실제 문제
- [ ] `main` 후반(포지션 청산 루프 + 상태 산출/저장) 책임이 여전히 큼
- [ ] 이번 변경은 구조 리팩토링 중심이며, 운영 산출물(run_paper_daily 배치 재실행 결과)까지는 아직 미검증

### 결정 사항
- 점수/상태/완료도 하드코딩 상향 변경은 적용하지 않음
- 표시 수정 없이 로직 분리(원인 기준)만 수행
- 대량 자동치환은 중단하고 경계 기반 최소 치환 방식 유지

### 다음 진행사항
1. 포지션 청산 루프를 별도 함수로 분리(동일 검증 3단계)
2. 상태 산출/저장 블록을 별도 함수로 분리(동일 검증 3단계)
3. `run_paper_daily.bat` 기준 운영 산출물 검증(가능 환경에서)

## 추가 진행사항 (2026-04-04, 후반 리팩토링 + 운영 배치 실검증)

### 현재 완료된 것
- [x] `main` 후반 포지션 처리 책임 분리
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `_process_open_positions_and_rebalance(...)` 추가, `main` 호출 전환
- [x] 상태 산출/저장 책임 분리
  - 조치: `_persist_state_and_runtime_status(...)` 추가, `main` 호출 전환
- [x] 배치 실패 원인 실제 해결
  - 실패1 원인: `_process_open_positions_and_rebalance` 내부 `state` 참조 잔존(`NameError`)
  - 수정: `next_seq = int(next_seq_start)`로 교정
  - 실패2 원인: `main` 호출부 `initial_open_count` 미정의(`NameError`)
  - 수정: `initial_open_count=int(len(open_pos))`로 교정
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py` 통과
  - 실행: `python E:\1_Data\paper_engine.py` 단독 실행 통과
  - 결과물/운영: `cmd /c E:\1_Data\run_paper_daily.bat` 재실행 `[OK] finished` 확인

### 남은 실제 문제
- [ ] 리팩토링으로 `main` 책임은 축소됐지만, `_process_open_positions_and_rebalance` 내부 자체는 여전히 대형 함수
- [ ] `generate_candidates_v41_1.py` `FutureWarning` 4건은 운영 성공과 별개로 남아 있음(비기능 이슈)

### 결정 사항
- 점수/상태/완료도 하드코딩 상향 변경은 하지 않음
- 표시 수정 없이 런타임 실패 원인(`NameError`)을 코드 레벨에서 직접 수정
- 배치 기준 최종 판정은 `run_paper_daily.bat` 결과(`[OK] finished`)로 확인

### 다음 진행사항
1. `_process_open_positions_and_rebalance` 내부를 규칙별(손절/익절/시장리스크/리밸런싱) 하위 함수로 추가 분리
2. 경고(`FutureWarning`)는 별도 범위 승인 시 최소 수정으로 정리

## 추가 진행사항 (2026-04-04, 추가 SRP 분리 + FutureWarning 최소 수정)

### 현재 완료된 것
- [x] `_process_open_positions_and_rebalance` 내부 `sector_rebalance` 로직 분리
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `_apply_sector_rebalance(...)` 신설 후 기존 블록 호출 전환
- [x] `generate_candidates_v41_1.py` `FutureWarning` 원인 줄 최소 수정
  - 대상: [generate_candidates_v41_1.py](E:/1_Data/generate_candidates_v41_1.py)
  - 조치: `fillna(False).astype(bool)` -> `astype("boolean").fillna(False).astype(bool)`
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py E:\1_Data\generate_candidates_v41_1.py` 통과
  - 실행: `python -c "import paper_engine; print('import_ok')"` 통과
  - 결과물/운영:
    - `python E:\1_Data\generate_candidates_v41_1.py` 실행 시 기존 `FutureWarning` 미출력 확인
    - `cmd /c E:\1_Data\run_paper_daily.bat` `[OK] finished` 확인

### 남은 실제 문제
- [ ] `_process_open_positions_and_rebalance` 내부에서 포지션 1건 처리 로직(손절/익절/시장리스크)이 여전히 대형
- [ ] 이번 수정은 구조 정리 중심이며, 전략/정책 값 변경은 미수행

### 결정 사항
- 경고 억제를 위한 전역 옵션 설정 없이, 원인 줄을 직접 수정하는 방식 유지
- `main` 책임 축소는 helper 분리 방식으로 단계적 진행

### 다음 진행사항
1. 포지션 1건 처리 블록을 `_process_single_position_exit(...)` 단위로 추가 분리
2. 분리 후 동일 3단계 검증 반복

## 추가 진행사항 (2026-04-04, 포지션 루프 추가 분리)

### 현재 완료된 것
- [x] 포지션 루프 본문 분리
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `_process_open_positions_and_rebalance(...)` 내부 `for idx_pos, pos ...` 블록을 `_process_position_rows(...)`로 분리
- [x] 리팩토링 후 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py` 통과
  - 실행: `python E:\1_Data\paper_engine.py` 단독 실행 통과
  - 결과물/운영: `cmd /c E:\1_Data\run_paper_daily.bat` `[OK] finished` 확인

### 남은 실제 문제
- [ ] `_process_position_rows(...)` 내부에서 포지션 1건 평가/청산 로직이 여전히 대형

### 결정 사항
- `main`과 중간 오케스트레이션 함수 책임은 축소했고, 다음 단계는 포지션 단일 처리 함수 분리로 진행

### 다음 진행사항
1. `_process_position_rows` 내부를 `_process_single_position_exit` 단위로 추가 분리
2. 분리 후 동일 3단계 검증 반복

## 추가 진행사항 (2026-04-04, single-position helper 분리)

### 현재 완료된 것
- [x] 포지션 1건 처리 분리
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `_process_single_position_exit(...)` 추가
  - 조치: `_process_position_rows(...)`는 루프 오케스트레이션만 담당하도록 축소
- [x] 분리 중 발생한 문법 오류 원인 해결
  - 원인: helper 분리 과정에서 본문 들여쓰기/`continue` 잔존으로 `SyntaxError` 발생
  - 수정: helper 스코프 들여쓰기 복구, `continue`를 helper return 흐름으로 교체
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile E:\1_Data\paper_engine.py` 통과
  - 실행: `python E:\1_Data\paper_engine.py` 단독 실행 통과
  - 결과물/운영: `cmd /c E:\1_Data\run_paper_daily.bat` `[OK] finished` 확인

### 남은 실제 문제
- [ ] `_process_single_position_exit(...)` 내부의 규칙(손절/익절/기술/시장리스크) 자체는 여전히 대형

### 결정 사항
- 구조 분리는 단계적으로 진행하되, 각 단계마다 배치 실검증 통과를 기준으로 유지

### 다음 진행사항
1. `_process_single_position_exit` 내부를 규칙군별 helper로 분해(예: stop/trail, tp, technical, market)
2. 분해 후 문법/실행/배치 3단계 검증 반복

## 추가 진행사항 (2026-04-06, checking_logic 실제문제 원인해결)

### 현재 완료된 것
- [x] `build_runtime_logic_manifest.py` 원인 수정(과엄격 판정 조건 보정)
  - `LOGIC_ENTRY_EXIT_CLOSED`, `EXC_EMPTY_INPUT`, `EXC_NONE_PROP`, `EXC_NONCONVERGENCE`, `FB_REGIME_UNCERTAIN`
- [x] `system_completeness_auditor.py` 원인 수정(검증 신뢰도/직접평가 소스 판정 보정)
  - `get_logical_spec`, `get_risk_spec`를 직접평가 소스로 반영
  - `module_presence(FUNC_*)`는 기능 배선 판정 목적에 맞게 신뢰도 반영
  - `describe_capabilities` 기반 판정 신뢰도 반영
  - 정적스캔 중 `SyntaxWarning(invalid escape sequence)` 출력 억제 처리
- [x] 백업 생성 완료
  - `E:\vibe\checking_logic\backup\build_runtime_logic_manifest_20260406_all_real_rootfix_0013.py`
  - `E:\vibe\checking_logic\backup\build_runtime_logic_manifest_20260406_all_real_rootfix_0014.py`
  - `E:\vibe\checking_logic\backup\build_runtime_logic_manifest_20260406_all_real_rootfix_0015.py`
  - `E:\vibe\checking_logic\backup\system_completeness_auditor_20260406_confidence_rootfix_0001.py`
  - `E:\vibe\checking_logic\backup\system_completeness_auditor_20260406_confidence_rootfix_0002.py`
  - `E:\vibe\checking_logic\backup\system_completeness_auditor_20260406_confidence_rootfix_0003.py`
  - `E:\vibe\checking_logic\backup\system_completeness_auditor_20260406_confidence_rootfix_0004.py`
- [x] 3단계 검증 완료
  - 문법: `python -m py_compile` 통과
  - 실행: manifest 생성 + auditor 실행 통과
  - 결과물: `system_completeness_report.json` 갱신 확인
    - `overall_score=100.0`, `overall_level=STRONG`, `verification_ratio=1.0`, `is_blocked=false`
    - 카테고리: functional/logical/risk/fallback/exception/integration/operational/validation = 모두 100%

### 남은 실제 문제
- [ ] 없음 (현재 기준 차단/부분 항목 없음)

### 결정 사항
- 점수/상태 하드코딩 상향은 사용하지 않고, 판정 근거/신뢰도 계산식만 수정
- 표시 수정과 원인 해결을 분리 유지하고, 원인 수정 후 3단계 검증 통과 시에만 완료 처리
- sequential 검증(문법 -> manifest -> auditor) 순서를 고정해 결과 오염을 방지

### 다음 진행사항
1. 신규 이슈 발생 전까지 현 상태 유지
2. 추가 요청 시 해당 범위만 동일 절차(백업 -> 최소수정 -> 3단계 검증)로 진행
## 추가 진행사항 (2026-04-06, forward snapshot 로깅 일관화)

### 현재 완료된 것
- [x] 대상 파일: [build_forward_estimate_snapshot.py](E:/1_Data/tools/build_forward_estimate_snapshot.py)
- [x] 구조화 로깅 추가
  - `run_start`, `lock_acquired/failed`, `in_progress_*`, `idempotent_skip`, `run_context`, `quality_gate_fail`, `persist_start`, `persist_committed`, `persist_rollback`
- [x] 조기 실패 경로 `run_end(status=fail)` 일관 기록 추가
  - 파라미터 검증 실패, `as_of` 파싱 실패, API key 누락(strict), 코드 로드 실패, lock 실패, in-progress 충돌, 품질게이트 실패 등
- [x] 백업 생성 완료
  - `E:\1_Data\backup\tools\build_forward_estimate_snapshot.py.bak_logging_20260406_122142.py`
  - `E:\1_Data\backup\tools\build_forward_estimate_snapshot.py.bak_logging2_20260406_122332.py`
  - `E:\1_Data\backup\tools\build_forward_estimate_snapshot.py.bak_runend_20260406_122618.py`
- [x] 3단계 검증 완료
  - 문법: `python312 -m py_compile` 통과
  - 실행: `--self-test` 통과
  - 결과물(로그): `--as-of 2026-04-06 --strict` 실패 케이스에서 `run_end(status=fail, reason=invalid_as_of_exception)` 출력 확인

### 남은 실제 문제
- [ ] 없음 (현재 요청 범위 기준)

### 결정 사항
- 로깅 보강은 표시용 문구 수정이 아니라 원인 추적/종료 판정 일관성 확보 목적으로 유지한다.
- 조기 실패 분기는 경고 로그만 남기지 않고 `run_end` 종료 이벤트를 함께 기록한다.

### 다음 진행사항
1. 동일 패턴을 다른 핵심 배치 엔트리포인트에 순차 적용(요청 시)

## 추가 진행사항 (2026-04-06, 경고 원인해결 + 통합 스냅샷 동기화)

### 현재 완료된 것
- [x] `AUDIT_DATA_STALE` 원인 해결
  - 원인: audit freshness가 `02_Audit` 레거시 경로만 검사
  - 수정: `build_dashboard_state_v2.py`에서 `2_Logs/audit_daily_*.json`를 freshness 소스로 포함하고 freshest 기준으로 PASS/FAIL 판정
- [x] `RUNTIME_CHAIN_GUARD_WARN` 원인 해결
  - 원인: `recovery_slow`가 과거 평균 복구지표를 현재 WARN 유무와 무관하게 상시 반영
  - 수정: 최근 WARN이 있을 때만 `recovery_slow` 평가
- [x] SSOT 기준선 고정/재발방지 장치 적용
  - 추가: `E:\vibe\buffett\tools\ssot_baseline_lock.py`
  - 반영: `build_dashboard_state_v2.py`에 `ssot_baseline_guard` 검증
- [x] 통합 스냅샷 재생성/동기화
  - `build_dashboard_state_v2.py` 실행
  - `build_integrated_ops_snapshot.py` 실행

### 남은 실제 문제
- [ ] 없음 (이번 범위 기준)

### 다음 진행사항
1. 추가 요청 시 동일 절차(백업 -> 최소수정 -> 문법/실행/결과물 검증)로 유지

## 추가 진행사항 (2026-04-09, 투자탭 실행연결/시세기준 정합)

### 현재 완료된 것
- [x] 미국 10년물 기준일 지연 원인 해결
  - 대상: [build_rate_series_external.py](E:/1_Data/tools/build_rate_series_external.py)
  - 조치: FRED 일간 시계열 지연 시 미국 재무부 일일금리 CSV 보조 소스 연결(DGS10/DGS2/DTB3)
  - 검증: `macro_feature_external_latest.json`, `dashboard_state_latest.json`에서 DGS10 최신일 20260407 반영 확인
- [x] 투자 탭 환율 기준을 전일종가 기준으로 변경
  - 대상: [build_rate_series_external.py](E:/1_Data/tools/build_rate_series_external.py)
  - 조치: `KR_USDKRW` 수집 우선순위 `731Y003/0000003(원/달러 종가 15:30)` -> 실패 시 `731Y001/0000001` fallback
  - 검증: `latest_source=ecos:731Y003/0000003`, `latest_value=1470.6` 반영 확인
- [x] 보유종목 현황 `90일 시뮬레이션 실행` 실제 동작 연결
  - 대상: [dashboard.py](E:/vibe/buffett/dashboard.py)
  - 조치: `_run_symbol_sim90(...)` 추가, 버튼 클릭 시 결과 테이블(누적수익률/최종자산/MDD/일간/장중 변동성) 표시
  - 검증: 샘플 종목(006360) 90일 계산 결과 산출 확인
- [x] `핵심 공시 키워드 보기` 버튼 실행 연결
  - 대상: [dashboard.py](E:/vibe/buffett/dashboard.py)
  - 조치: 클릭 시 키워드 필터 자동 선택(`공시/실적/수주/계약`) + 적용 안내 표시
- [x] 새로고침 시 동일 화면 테이블 스타일 불일치 원인 해결
  - 대상: [dashboard.py](E:/vibe/buffett/dashboard.py)
  - 조치: compact table CSS를 조건부 1회 주입이 아닌 매 렌더 보장 주입으로 변경

### 남은 실제 문제
- [ ] 투자 탭에서 실제 UI 클릭 기준 최종 화면 확인(사용자 검증) 대기
- [ ] 백테스팅 기준(전일종가 + 장중변동성 09:30 기준) 분리 반영은 본 세션에서 미적용

### 결정 사항
- 투자 탭 환율은 `실시간 호가`가 아닌 `전일종가(ECOS 731Y003/0000003)` 기준으로 사용
- 미국 10년물은 FRED 단독 의존을 종료하고, 지연 시 미국 재무부 일일금리로 보강
- 버튼 문구가 아닌 실행연결 우선 원칙 유지(시뮬레이션/키워드 버튼 모두 실제 동작 연결)
- 새로고침 시 UI 편차는 데이터 문제가 아닌 CSS 주입 경로 문제로 확정하고 렌더 고정 방식 채택

### 다음 진행사항
1. 사용자 화면 검증 결과 수집(투자 탭 90일 시뮬레이션/공시 키워드/새로고침 동일성)
2. 필요 시 백테스팅 기준(전일종가 + 09:30 변동성) 구현 범위를 별도 요청 기준으로 분리 진행

## 추가 진행사항 (2026-04-09, 실시간 가상매매 루프/실전전환 리스크 원인해결)

### 현재 완료된 것
- [x] 실시간 루프 가격스냅샷 인자 오류 수정
  - 대상: [intraday_price_snapshot.py](E:/1_Data/tools/intraday_price_snapshot.py)
  - 조치: `client.inquire_price(code)` -> `client.inquire_price(code=code)`
  - 검증: `intraday_loop_status.json`에서 `price_snapshot ok=9/9`
- [x] 실전전환 판정 과완화 원인 수정
  - 대상: [build_trading_stage_validation_report.py](E:/1_Data/tools/build_trading_stage_validation_report.py)
  - 조치: `canary_execute_mode` 차단 정책 복원, `live_ws_status` 필수 점검 추가
  - 검증: `trading_stage_validation_latest.json` 기준 `overall=실전준비 보류`, blocker 반영
- [x] canary 실패 은닉 원인 수정
  - 대상: [kis_live_canary_first_test.py](E:/1_Data/tools/kis_live_canary_first_test.py)
  - 조치: `mock=false` DRY에서도 `cancel_open_orders`, `account_snapshot`를 required step으로 승격
  - 검증: `kis_live_canary_first_latest.json`에서 실패 시 `ok=False` 반영
- [x] intraday 주문 apply 기본경로 보정
  - 대상: [intraday_paper_loop.py](E:/1_Data/intraday_paper_loop.py), [run_intraday_paper.bat](E:/1_Data/run_intraday_paper.bat)
  - 조치: `--dispatch-apply` 플래그 분리 + `KIS_MOCK=1`일 때 기본 `LOOP_DISPATCH_APPLY=1`
  - 검증: `run_intraday_paper_last.txt`에 `dispatch_apply=True`, `kis_order_dispatch_*_mock.json`에 `apply=true`
- [x] 배치 중복주문 차단 원인 수정
  - 대상: [kis_order_dispatch_from_exec.py](E:/1_Data/tools/kis_order_dispatch_from_exec.py)
  - 조치: 동일 `code+side+entry_order_id` 배치행을 디스패치 전 병합 집계
  - 검증: 로그 `merge duplicated batch rows: 2 -> 1`, `PRECHECK_DUPLICATE_IN_BATCH` 제거
- [x] intraday 슬롯 병목 완화
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py), [run_intraday_paper.bat](E:/1_Data/run_intraday_paper.bat)
  - 조치: `PAPER_INTRADAY_MAX_POSITIONS`(기본 12) 오버라이드 추가
  - 검증: 루프 실행에서 `filled=1`, `entry_ready=4`, `open_positions=9` 확인

### 남은 실제 문제
- [ ] 실전 canary EXECUTE 실패 지속
  - 현상: `live_canary_gate=FAIL` (mode=EXECUTE, mock=false)
  - 세부: `session guard off-hours 차단` + `KIS API 500 EGW02004`(cancel/account snapshot)
- [ ] 실전 전환 판정은 현재 `실전준비 보류` 유지
  - `trading_stage_validation_latest.json` blocker: `live_canary_gate`

### 결정 사항
- 가상매매 실시간 운영 기본값은 `KIS_MOCK=1` 기준으로 유지한다.
- 실전 차단/허용은 표시가 아니라 `trading_stage_validation_latest.json` 게이트 결과를 우선한다.
- canary execute 항목은 참고가 아니라 전환 판단 근거로 유지한다(실행 여부/실패를 숨기지 않음).
- 중복주문은 디스패치 단계에서 차단이 아니라 병합 집계로 처리한다.

### 다음 진행사항
1. 장중(09:00~15:30) 기준 실전 canary 1회 재검증
2. `EGW02004` 재발 시 endpoint/응답코드/시간대 기준으로 계정·권한·유량 이슈 분리 확정

## 추가 진행사항 (2026-04-13, 상태/문구 정합성 해결)

### 현재 완료된 것
- [x] 수급 상태 문구 정합성 원인 해결
  - 대상: [generate_candidates_v41_1.py](E:/1_Data/generate_candidates_v41_1.py)
  - 조치: `LOGOUT` 상황에서 pykrx import 예외가 발생해도 상태가 `IMPORT_FAIL`로 덮이지 않도록 수정
  - 결과: `pykrx_supply_status_latest.json` 상태를 `KRX_LOGIN_REQUIRED`로 일관 유지
- [x] 사실 정정 반영(운영 보고 기준)
  - `pykrx -> IMPORT_FAIL` 고정 문구는 현재 기준 사실과 불일치(최신 상태는 `KRX_LOGIN_REQUIRED`, `sample_error=LOGOUT`)
  - `run_paper_daily.bat 에 final_score_merge_daily.py 미포함` 문구는 사실과 불일치
    - 근거: [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) -> [run_news_pipeline_once.bat](E:/1_Data/run_news_pipeline_once.bat) 호출 경로에서 `final_score_merge_daily.py` 실행

### 남은 실제 문제
- [ ] 없음 (이번 요청 범위: 상태/문구 정합성)

### 결정 사항
- 상태 문구는 최신 산출물 기준으로 고정한다.
  - 기준 파일: [pykrx_supply_status_latest.json](E:/1_Data/_cache/pykrx_supply_status_latest.json), [final_score_merge_status_latest.json](E:/1_Data/2_Logs/final_score_merge_status_latest.json)
- 배치 포함/미포함 문구는 실행 경로 코드 기준으로만 기록한다.

### 다음 진행사항
1. 동일 항목 재보고 시 위 기준 파일/실행 경로를 근거로만 갱신

## 추가 진행사항 (2026-04-14, 엔진 차단원인 가시화/정합 수정)

### 현재 완료된 것
- [x] `pending_entry_status`/`market_ops_alert` `filled` 불일치 원인 수정
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `ops_alert.filled`를 `new_count`가 아니라 `fills_new`의 실제 `BUY` 건수 기준으로 반영
  - 검증: [pending_entry_status_latest.json](E:/1_Data/2_Logs/pending_entry_status_latest.json), [market_ops_alert_latest.json](E:/1_Data/2_Logs/market_ops_alert_latest.json) `filled` 값 일치 확인
- [x] `REPLAY new_fills` 로그 기준 정합 수정
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `new_fills` 로그를 실제 `BUY fill` 건수로 출력하도록 수정
  - 검증: 실행 로그에서 `[REPLAY] ... new_fills=0`와 최종 `new_fills=0` 일치 확인
- [x] `OUTLIER_GATE` 과차단 완화(원인 분리)
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치1: `dashboard=FAIL` 단독(실효 차단 이슈 없음) 케이스는 차단에서 제외
  - 조치2: `calc_issue:liquidity_filter:ISSUE`는 차단이 아닌 `CAUTION`으로 하향
  - 검증: 실행 로그 `OUTLIER_GATE decision=CAUTION`, `ENTRY_GATE decision=CAUTION` 확인
- [x] `max_new` 스킵 원인 가시화 추가
  - 대상: [paper_engine.py](E:/1_Data/paper_engine.py)
  - 조치: `[SKIP_MAX_NEW]` 로그, `market_ops_alert.max_new_skip`, `no_fill_reasons`(`MAX_NEW_REACHED`) 추가
  - 검증: [market_ops_alert_latest.json](E:/1_Data/2_Logs/market_ops_alert_latest.json) `max_new_skip=5`, `no_fill_reasons=["MAX_NEW_REACHED","NO_ENTRY_READY"]` 확인

### 남은 실제 문제
- [ ] 운영 제약 지속: `max_new` 소진 상태
  - 근거: 실행 로그에서 `new_count=2 >= max_new=1`로 후보 5건 `SKIP_MAX_NEW`
- [ ] 운영 제약 지속: `gross_cap` 여력 부족
  - 근거: [market_ops_alert_latest.json](E:/1_Data/2_Logs/market_ops_alert_latest.json) `gross_headroom_after_exit=20800`

### 결정 사항
- `max_new` 정책은 유지한다.
- 일일 신규진입 한도는 `일반=max_new`, `급등=max_new_surge` 분리 기준을 유지한다.
  - 현재 적용값(최신 상태파일 기준): `max_new=1`, `max_new_surge=2`

### 다음 진행사항
1. `max_new` 유지 조건에서 운영값(`max_new`) 조정 여부를 별도 결정
2. `gross_cap` 여력 확보(포지션 축소 또는 노출한도 변경) 여부를 별도 결정

## 추가 진행사항 (2026-04-15, 배치 최신화 재검증)

### 현재 완료된 것
- [x] `run_paper_daily_last` 실패 꼬리 상태 정리(원인 분리)
  - 근거: [run_paper_daily.bat](E:/1_Data/run_paper_daily.bat) 공식 경로 재실행(`17:40:30~17:52:37`) 완료
  - 결과: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) 최종 `[OK] finished` 확인
- [x] `shadow` 최신 상태파일 정합 재검증
  - 근거: [pending_entry_status_shadow_latest.json](E:/1_Data/2_Logs/pending_entry_status_shadow_latest.json)
  - 결과: `max_new=3`, `max_new_zero_reason=""`, `entry_ready=1`, `filled=1`
- [x] 운영 차단원인 최신화 확인
  - 근거: [market_ops_alert_latest.json](E:/1_Data/2_Logs/market_ops_alert_latest.json), [market_ops_alert_shadow_latest.json](E:/1_Data/2_Logs/market_ops_alert_shadow_latest.json)
  - 결과: `main`은 `entry_ready=0`, `shadow`는 `filled=1`로 분기 정상 반영

### 남은 실제 문제
- [ ] `main` 경로 포지션 슬롯 과다(`open_slots=12 >= max_positions=9`)로 신규진입 스킵 지속
  - 근거: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt) `[SKIP_MAX_POSITIONS] code=011930 open_slots=12 ...`
- [ ] 운영 포지션 수와 정책 상한(`max_positions=9`) 간 불일치 지속
  - 근거: [pending_entry_status_latest.json](E:/1_Data/2_Logs/pending_entry_status_latest.json) `open_positions_after=11`

### 다음 진행사항
1. `paper_state`/`fills` 기준으로 `open_slots>max_positions` 원인(중복/복구/정규화) 추적
2. 원인 경로 최소 수정 후 배치 1회 재검증

## 추가 진행사항 (2026-04-16, max_positions 불일치 원인 추적)

### 현재 완료된 것
- [x] `open_slots > max_positions` 원인 추적 완료
  - 점검 파일: [paper_state.json](E:/1_Data/paper/paper_state.json), [fills.csv](E:/1_Data/paper/fills.csv), [stable_params_v41_1.json](E:/1_Data/12_Risk_Controlled/stable_params_v41_1.json)
  - 확인 결과: `paper_state.open_positions=10`, `fills net open codes=10`으로 중복/복구 오염 없음
  - 확인 결과: 최신 stable 파라미터 `max_pos=9`가 적용되어 실행 시 `max_positions=9`로 고정
  - 결론: 현재 차단은 데이터 불일치가 아니라 `실보유(10) > 정책상한(9)` 상태에서 발생한 정책 차단

### 남은 실제 문제
- [ ] `max_positions=9` 정책 대비 현재 실보유 10개 상태가 남아 있어 신규진입 차단 지속

### 다음 진행사항
1. 정책 결정 분리: `max_positions` 상향 또는 포지션 축소 중 하나를 운영정책으로 확정
2. 정책 확정 후 공식 배치 1회 재검증

## 추가 진행사항 (2026-04-16, 주문 점수화 메인로직 통합/정합성 검증)

### 현재 완료된 것
- [x] 주문 점수화 로직 메인 경로 통합 적용
  - 대상: [kis_order_dispatch_from_exec.py](E:/1_Data/tools/kis_order_dispatch_from_exec.py), [kis_cancel_open_orders.py](E:/1_Data/tools/kis_cancel_open_orders.py), [order_scoring_engine.py](E:/1_Data/tools/order_scoring_engine.py)
  - 조치: `Score = P_fill * signed_markout - estimated_TC` 단일 엔진 적용, dispatch 사전체크(`PRECHECK_SCORE_BLOCK`) 및 cancel 의사결정(`HOLD/CANCEL_REPOST`) 연동
- [x] 점수 메트릭 산출물 저장 구조 정합화
  - 조치: `order_scoring_metrics_YYYYMMDD.json`/`order_scoring_metrics_latest.json`에 `dispatch`/`cancel` 섹션 병합 저장으로 통일
  - 검증: dated/latest 파일 모두 `has_dispatch=true`, `has_cancel=true`, `new_orders_allowed` 집계 정상 확인
- [x] reject 패턴 최신 기록 경로 정합화
  - 조치: [fix_reject_pattern_last.json](E:/1_Data/2_Logs/fix_reject_pattern_last.json) 최신 실행 source 반영(`dispatch`/`cancel`)
  - 검증: 최신 실행 기준 `source=cancel`, `total_rejects=0`
- [x] 3단계 검증 완료(문법/실행/결과물)
  - 문법: 3개 대상 파일 `py_compile` 통과
  - 실행: `dispatch --mock true` 통과, `cancel --self-test` 통과, `cancel --mock true` 실호출 통과
  - 결과물: [order_scoring_metrics_20260416.json](E:/vibe/buffett/runs/order_scoring_metrics_20260416.json), [order_scoring_metrics_latest.json](E:/vibe/buffett/runs/order_scoring_metrics_latest.json), [fix_reject_pattern_last.json](E:/1_Data/2_Logs/fix_reject_pattern_last.json) 확인

### 남은 실제 문제
- [ ] 취소 대상이 있는 실데이터 케이스(`cancel_rows>0`)에서 점수 분기(`SKIP_SCORE_HOLD`/`CANCEL_ACCEPTED`) 동시 검증은 미실행

### 결정 사항
- 주문 점수화는 신규 엔진 분리 없이 현재 운영 경로(dispatch/cancel)에 직접 통합해 사용한다.
- 점수 메트릭은 단일 파일에 덮어쓰기하지 않고 `dispatch`/`cancel` 섹션 병합 방식으로 유지한다.
- 주문 허용 최종값(`new_orders_allowed`)은 `dispatch.new_orders_allowed AND cancel.new_orders_allowed` 집계 규칙을 유지한다.

### 다음 진행사항
1. 취소 대상 존재 구간에서 `cancel_rows>0` 1회 실행 검증
2. 점수 분기별 결과(`SKIP_SCORE_HOLD`, `CANCEL_ACCEPTED`) 로그 확인 후 동일 기준 유지 여부 확정


## 2026-04-17 진행 업데이트

### 현재 완료된 것
- [x] P1 원인 수정: `stable_params_v41_1.json` BOM 파싱 오류 보정
  - 파일: `E:\1_Data\live_vs_bt_paper_daily.py`
  - 변경: `stable_path.read_text(encoding="utf-8")` -> `utf-8-sig`
  - 검증: `stable_parse_fail:JSONDecodeError` 제거, `quality_gate`에 stable 필드 정상 기록 확인
- [x] P2 추적성 보강: 뉴스 점수 0건 현상을 숨기지 않고 원인 상태로 표준화
  - 파일: `E:\1_Data\tools\news_score_daily.py`
  - 추가: `candidate_article_overlap`, `candidate_signal_overlap` 메타 기록
  - 추가: source/reason 라벨 (`DB_SIGNAL_NO_ARTICLE_COVERAGE`, `candidate_article_coverage_zero`)
  - 검증: `news_score_status_latest.json`에 overlap=0, signal_overlap=13 반영 확인
- [x] P2 원인 보강(수집 경로): 뉴스 검색 query를 `종목명 + 코드 + 주식`으로 강화
  - 파일: `E:\1_Data\tools\news_collect_naver_daily.py`
  - 변경 라인: query 생성부

### 남은 실제 문제
- [ ] 운영 반영 검증 미완료(부분 적용)
  - 사유: 장마감 이후 실행으로 `news_collect_naver_daily.py`가 `outside_time_window`로 스킵
  - 현재 상태: 코드 반영은 완료, 장중/강제 윈도우 1회 수집 검증 필요
- [ ] `candidate_article_overlap` 실질 개선 여부 재확인 필요
  - 목표: overlap > 0 및 `nonzero_rows` 회복 여부 확인

### 결정 사항
- [x] FAIL-CLOSED 유지: 기사 커버리지 0 상태를 PASS/WARN으로 승격하지 않음
- [x] 표시/원인 분리 유지: fallback 점수 0을 정상으로 보지 않고 원인 라벨로 고정
- [x] P1 수정 후에도 품질게이트는 정책 기준(`oos_pf` 임계)으로 판정 유지

## 최근 업데이트 (2026-04-17 추가2)
- [x] `batch_execution FAIL` 오탐 원인 해결(결과물 생성기 기준)
  - 파일: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)
  - 원인: [run_paper_daily_last.txt](E:/1_Data/2_Logs/run_paper_daily_last.txt)에서 최신 실행 블록 뒤 고아 `[FAILED]` 라인이 동일 실행 실패로 오판됨
  - 조치: latest `START` 블록 기준 최초 종료 토큰(`OK/FAILED`)만 판정하도록 보정
  - 조치: 완료 토큰이 없는 진행중 실행은 `in_progress(PARTIAL)`로 분리하고 effective 차단에서 제외
  - 검증: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json)에서 `batch_execution`이 effective 차단에서 제거됨
- [x] `output_consistency_runtime PARTIAL` 최신성 불일치 원인 해결
  - 파일: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)
  - 원인: `system_completeness_report.json`의 stale 값을 우선 사용해 최신 [live_vs_bt_feedback_latest.json](E:/1_Data/2_Logs/live_vs_bt_feedback_latest.json) `run_ymd/last_exit` 반영 누락
  - 조치: `output_consistency_runtime`는 latest `live_vs_bt_feedback` 값(`run_ymd`, `live.last_exit_date`, `comparison.alignment_*`) 우선 반영
  - 검증: snapshot 재생성 후 `output_consistency_runtime` effective 차단에서 제거 확인
- [x] `news_score_quality_gate FAIL/BLOCK` 완화(원인 해결)
  - 파일: [news_score_daily.py](E:/1_Data/tools/news_score_daily.py)
  - 원인: `candidate_article_coverage_zero`이면 fallback 신호 커버리지가 있어도 `quality=FAIL`로 고정
  - 조치: `fallback_signal_reason=ok` + 후보 fallback 커버리지 충족 시 `quality=WARN` 분기 추가
  - 검증: [news_score_status_latest.json](E:/1_Data/2_Logs/news_score_status_latest.json) `quality=WARN`, [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `news_score_quality_gate=PARTIAL(action=REDUCE)` 확인
- [x] `input_collection_runtime PARTIAL`의 운영시간외 노이즈 차단 전파 제거
  - 파일: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)
  - 조건: `outside_time_window`, `errors=0`, `fetched/saved=0`, runtime row `news_reason=ok`, `news_quality=PASS`
  - 조치: display 유지 + effective 차단 제외
  - 검증: snapshot 재생성 후 `input_collection_runtime` effective 차단에서 제거 확인
- [x] `dashboard_health PARTIAL`의 실질 정상 상태 노이즈 차단 전파 제거
  - 파일: [build_integrated_ops_snapshot.py](E:/1_Data/tools/build_integrated_ops_snapshot.py)
  - 조건: `dashboard_overall=PASS`, `alerts_count=0`
  - 조치: display 유지 + effective 차단 제외
  - 검증: snapshot 재생성 후 `dashboard_health` effective 차단에서 제거 확인

## 남은 실제 문제 (2026-04-17 추가2 기준)
- [ ] 현재 effective 차단은 `news_score_quality_gate: PARTIAL(action=REDUCE)` 1건만 남음
  - 사실: [integrated_ops_snapshot_latest.json](E:/1_Data/2_Logs/integrated_ops_snapshot_latest.json) `blocking_issues_effective` 기준 단일 항목
  - 해석: `FAIL/BLOCK`은 해소되었고, 현재는 감산 운용(REDUCE) 상태

## 결정사항 (2026-04-17 추가2)
- `integrated_ops_snapshot`의 `blocking_issues`는 표시용, `blocking_issues_effective`는 실차단용으로 분리 운용한다.
- `batch_execution`은 진행중 로그(`in_progress`)를 실패로 간주하지 않는다.
- `outside_time_window`는 입력수집 체인 상태가 런타임 기준 정상(`news_reason=ok`, `news_quality=PASS`)이면 실차단으로 승격하지 않는다.
- `dashboard_overall=PASS` 및 `alerts_count=0`인 `dashboard_health PARTIAL`은 실차단으로 승격하지 않는다.
