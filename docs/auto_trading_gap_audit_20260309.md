# 자동매매 단계별 갭 감사 + 로직 업데이트 (2026-03-09, Rev2)

## 범위
- 작업 루트: `E:\1_Data`, `E:\vibe\buffett`
- 기준: 사용자 제공 1~9단계 체크리스트

## 전체 요약
- 완료: 51
- 검증항목 수(체크파일): 51 -> 55 (+4)
- 부분완료: 4
- 미구현: 0
- 외부준비: 2
- 운영 실행 기준(사용자 현재 상태): 가상매매 미시도로 8/9단계 실행 항목은 아직 미착수


## 단계별 결과

### 1단계 - 기본 인프라 (API 연동)
- [완료] 한국투자증권 계좌 개설 및 API 신청
- [완료] API Key / Secret 발급
- [완료] 액세스 토큰 발급 구현 (`tools/kis_order_client.py`)
- [완료] 액세스 토큰 자동 갱신 구현 (`_ensure_token()`)
- [완료] API 연결 상태 확인 (`tools/kis_healthcheck.py`)
- [완료] 모의/실전 전환 (`KIS_MOCK`, `--mock`)

### 2단계 - 시세 데이터 수신
- [완료] 현재가 조회 (`inquire_price()`)
- [완료] 실시간 시세 웹소켓 연결 (`tools/kis_realtime_ws.py`)
- [완료] 호가 데이터 수신 (`trade/hoga` 구독)
- [완료] 거래량 데이터 수신 (`stck_prpr/acml_vol`)
- [완료] 시세 저장/관리 (`kis_quote_poll.py`, `kis_realtime_ws.py` JSONL)
- [완료] 데이터 끊김 재연결 (웹소켓 reconnect loop)

### 3단계 - 매매 조건 로직
- [완료] 매수 조건
- [완료] 매도 조건
- [완료] 손절 조건
- [완료] 익절 조건
- [완료] 조건 중복 충돌 방지
- [완료] 조건 충족 시 로그

### 4단계 - 주문 실행
- [완료] 시장가 매수
- [완료] 시장가 매도
- [완료] 지정가 매수
- [완료] 지정가 매도
- [완료] 주문 수량/금액 계산
- [완료] 주문 결과 수신/처리
- [완료] 미체결 주문 관리 (`inquire_open_orders()`)
- [완료] 주문 취소 (`cancel_order()`, `kis_cancel_open_orders.py`)

### 5단계 - 리스크 관리
- [완료] 최대 보유 종목 수 제한
- [완료] 종목당 최대 투자 금액 제한 (`max_per_symbol_exposure_pct`)
- [완료] 일일 최대 손실 한도
- [완료] 일일 최대 거래 횟수 제한
- [완료] 중복 주문 방지
- [완료] 잔고 부족 시 주문 차단
- [완료] 장 시작/종료 시간 체크
- [완료] 긴급 전체 매도 (`tools/kis_emergency_liquidate.py`)

### 6단계 - 잔고 및 수익 관리
- [완료] 현재 잔고 조회
- [완료] 보유 종목 조회
- [완료] 매입 평균가 추적 (`tools/kis_account_snapshot.py`)
- [완료] 실현 손익 계산
- [완료] 미실현 손익 계산 (`tools/kis_account_snapshot.py`)
- [완료] 일별/누적 수익률 기록

### 7단계 - 로그 및 모니터링
- [완료] 매매 실행 로그
- [완료] 오류 로그
- [완료] 실시간 상태 모니터링 (`tools/kis_status_monitor.py`)
- [완료] 알림 기능 (`tools/notify_channels.py`, telegram/kakao)
- [완료] 로그 파일 자동 저장

### 8단계 - 가상매매 테스트
- [완료] 모의투자 환경 전환
- [부분] 전체 로직 시나리오 테스트 자동 실행 묶음 (`tools/kis_intraday_e2e_runner.py`, `run_kis_intraday_e2e.bat`) [구현완료/운영미실행]
- [완료] 오류 상황 테스트 자동화 (`tools/kis_fault_injection_test.py`, `run_kis_fault_injection.bat`) [2026-03-09 실행 PASS]
- [부분] 수익/손실 결과 검증 [가상매매 실행 후 확정]
- [부분] 장시간 안정성 테스트 1일+ (`tools/kis_soak_test.py`, `run_kis_soak_24h.bat`) [구현완료/운영미실행]
- [완료] 실전 전환 준비 확인 (canary/preflight 경로 포함, virtual-gate 적용)

### 9단계 - 실전 투자 전환
- [부분] 소액 실전 첫 테스트 (운영 실행 단계, 가상매매 통과 후)
- [완료] 실전/모의 결과 비교 (`tools/kis_mode_compare_report.py`)
- [완료] 리스크 한도 재설정 (`tools/risk_recalibrate_from_pnl.py`)
- [완료] 비상 대응 매뉴얼 (`docs/03-operations/emergency_response_manual_20260309.md`)
- [완료] 정기 성과 점검 주기 설정 (작업스케줄러 `Buffett_Perf_Weekly` 등록 완료)

## 이번 반영 파일
- `tools/notify_channels.py`
- `tools/kis_realtime_ws.py`
- `tools/kis_emergency_liquidate.py`
- `tools/kis_soak_test.py`
- `tools/kis_status_monitor.py`
- `tools/kis_account_snapshot.py`
- `tools/kis_mode_compare_report.py`
- `tools/risk_recalibrate_from_pnl.py`
- `tools/perf_review_weekly.py`
- `tools/kis_canary_run.py`
- `run_emergency_liquidate.bat`
- `run_kis_soak_24h.bat`
- `run_kis_ws_monitor.bat`
- `paper_engine.py` (`read_header()` 복구, 종목별 익스포저 캡 연계)
- `tools/kis_intraday_e2e_runner.py`
- `tools/kis_fault_injection_test.py`
- `tools/kis_live_canary_first_test.py`
- `tools/register_perf_review_task.py`
- `run_kis_intraday_e2e.bat`
- `run_kis_fault_injection.bat`
- `run_live_canary_first_test.bat`
- `run_register_perf_review_task.bat`

## 외부준비 2개
1. 텔레그램 발송 토큰/채팅ID 설정
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

2. 카카오 알림 토큰 설정
- `KAKAO_ACCESS_TOKEN`

## 남은 순차 작업(부분완료 4개)
1. 8단계 전체 로직 시나리오 E2E 운영 실행 (mock PASS 확보 필요)
2. 8단계 수익/손실 결과 검증 확정
3. 8단계 장시간 Soak(24h+) 운영 실행
4. 9단계 소액 실전 첫 테스트 실제 EXECUTE 수행 (가상매매 통과 후)




## 2026-03-09 실행 결과 (Virtual-First 진행)
- E2E 드라이런: 실패 (`2_Logs/kis_intraday_e2e_latest.json`)  
  원인: `KIS_ACCOUNT_NO` 미설정으로 KIS 헬스체크/시세/잔고 단계 fail
- Fault-injection: 통과 (`2_Logs/kis_fault_injection_latest.json`)  
  결과: pass=4 fail=0
- Live canary 드라이런: 실패 (`2_Logs/kis_live_canary_first_latest.json`)  
  원인: virtual gate 미통과 + KIS 계정 환경값 미설정
- 성과점검 스케줄: 등록 완료 (`2_Logs/perf_task_register_latest.json`)  
  Task: `Buffett_Perf_Weekly`, 매주 월요일 18:10

## 즉시 필요 설정
- `KIS_ACCOUNT_NO` (형식: `12345678-01`)
- 이미 존재 확인: `KIS_APP_KEY_FILE=E:\1_Data\.secrets\kis_app_key.txt`, `KIS_APP_SECRET_FILE=E:\1_Data\.secrets\kis_app_secret.txt`
