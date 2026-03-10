# 비상 대응 매뉴얼 (2026-03-09)

## 목적
- 실시간 장애, 급격한 손실, API 장애 등 비상 상황에서 **신규 주문 차단 + 포지션 정리 + 운영자 알림**을 표준화한다.

## 트리거 조건
- `p0_daily_check` 또는 게이트에서 `BLOCK`/`REDUCE`가 발생
- 체결/시세 장애가 연속 발생(연결 불안정, 인증 실패, 빈 시세)
- 수동 비상 명령 필요(운영자 판단)

## 즉시 조치 순서
1. 자동주문 중지
- `BROKER_MODE=OFF` 또는 주문 배치 중단

2. 미체결 정리
- `tools/kis_cancel_open_orders.py --mock auto --apply`

3. 긴급 전체 매도
- Dry-run 확인:
  - `run_emergency_liquidate.bat` (`EMERGENCY_APPLY=0`)
- 실제 실행:
  - `set EMERGENCY_APPLY=1`
  - `set EMERGENCY_REASON=RISK_EMERGENCY`
  - `run_emergency_liquidate.bat`

4. 상태 확인
- `tools/kis_healthcheck.py --mock auto --check-balance --check-open-orders`
- `tools/kis_status_monitor.py` 최신 상태 파일 확인

5. 알림 전파
- `notify_channels.py`를 통한 텔레그램/카카오 알림 확인
- 알림 로그: `2_Logs/alerts/alerts_YYYYMMDD.jsonl`

## 사후 점검
- 주문 결과: `paper/orders_*_emergency_liq_*.csv`
- 요약: `2_Logs/kis_emergency_liq_*.json`
- 손익/잔고 점검: `tools/kis_account_snapshot.py --with-quotes`
- 원인 분석: `2_Logs/kis_ws_status_latest.json`, `2_Logs/kis_soak_latest.json`

## 롤백/재개 기준
- API 헬스체크 `ok=true`
- 시세 수신 정상(빈 시세 없음, reconnect 안정)
- 미체결 0 또는 허용 수준
- 운영자 승인 후 `BROKER_MODE=DRY` -> 소액 `APPLY` 순으로 재개

## 외부 준비(필수)
- 텔레그램: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- 카카오: `KAKAO_ACCESS_TOKEN`
