# KIS Open API 핵심 제약사항 정리

기준일: 2026-03-22

## 1. 실전/모의 환경 분리

- KIS는 실전과 모의의 REST 도메인이 다르다.
- 현재 코드 반영:
  - `E:\1_Data\tools\kis_order_client.py`
  - `PROD_BASE_URL`
  - `MOCK_BASE_URL`
  - `KISConfig.mock`
- 현재 판단:
  - 필요
  - 구현됨

## 2. Access Token 발급/자동 갱신

- KIS REST 호출은 access token 기반이다.
- 장중 장시간 실행에서는 토큰 만료 전 재사용/재발급이 필요하다.
- 현재 코드 반영:
  - `E:\1_Data\tools\kis_order_client.py`
  - `_load_cached_token()`
  - `_save_cached_token()`
  - `_issue_token()`
  - `_ensure_token()`
  - `KIS_TOKEN_CACHE_FILE`
- 현재 판단:
  - 필수
  - 구현됨

## 3. WebSocket 승인키와 연결 안정성

- KIS 실시간 수신은 REST 토큰과 별도로 websocket approval key 발급이 필요하다.
- 장시간 수신에서는 재연결, backoff, ping, 실패 상태 기록이 필요하다.
- 현재 코드 반영:
  - `E:\1_Data\tools\kis_order_client.py`
  - `issue_ws_approval_key()`
  - `E:\1_Data\tools\kis_realtime_ws.py`
  - `--reconnect-max`
  - `--backoff-sec`
  - `ping_interval=20`
  - `ping_timeout=10`
  - `E:\1_Data\2_Logs\kis_ws_status_latest.json`
- 현재 판단:
  - 필수
  - 로직 구현됨
  - 실제 실연결 검증은 아직 미완료
- 현재 남은 실제 문제:
  - `websocket-client` 패키지가 없어 실연결 성공 검증은 막혀 있음
  - 2026-03-22 실행 결과 상태 파일:
    - `DEPENDENCY_FAIL`

## 4. REST 호출 제한 대응

- KIS는 호출 유량 제한이 있으므로 과호출 방지가 필요하다.
- 현재 코드 반영:
  - `E:\1_Data\tools\kis_order_client.py`
  - `min_request_interval_sec`
  - `rate_limit_retries`
  - `rate_limit_backoff_sec`
  - HTTP `429` 재시도 처리
- 현재 판단:
  - 필수
  - 구현됨

## 5. 주문 실행 모듈의 실전/모의 완전 분리

- 실전/모의는 주문 TR_ID, 계좌, 실행 여부, 산출물 구분이 필요하다.
- 현재 코드 반영:
  - `E:\1_Data\tools\kis_order_dispatch_from_exec.py`
  - `--mock`
  - `--apply`
  - `DRY_RUN`
  - `orders_{D}_broker_submit_mock.csv`
  - `orders_{D}_broker_submit_prod.csv`
  - 세션 가드, 중복 방지, 사전검증
- 현재 판단:
  - 필수
  - 구현됨

## 6. 알림 + 로깅

- 장애 시 무음 실패를 막으려면 알림 채널과 파일 로그가 필요하다.
- 현재 코드 반영:
  - `E:\1_Data\tools\notify_channels.py`
  - Telegram
  - Kakao
  - file fallback
  - `E:\1_Data\2_Logs\alerts\alerts_YYYYMMDD.jsonl`
- 현재 판단:
  - 필수
  - 구현됨
- 실제 확인 산출물:
  - `E:\1_Data\2_Logs\market_ops_alert_latest.json`
  - `E:\1_Data\2_Logs\market_ops_alert_shadow_latest.json`

## 7. 공식 제약사항 관점에서 바로 주의할 점

- 호출 유량 제한을 넘기면 REST/WS 모두 차단 위험이 있다.
- 웹소켓 무한루프/과도 재접속은 차단 대상이 될 수 있다.
- 실전/모의 환경을 혼동하면 주문 TR_ID와 엔드포인트가 어긋난다.
- 토큰/승인키는 만료와 재발급 흐름을 분리해서 관리해야 한다.

## 8. 현재 결론

- 필수 6개 중 현재 코드 기준으로 직접 미구현인 항목은 없다.
- 다만 `WebSocket 안정 연결`은 코드 구현과 상태 기록까지는 되었지만, 실연결 성공 검증은 아직 안 끝났다.
- 따라서 현재 상태 평가는 아래가 맞다.
  - 토큰 갱신: 구현 완료
  - Rate Limiter: 구현 완료
  - 주문 실행 실전/모의 분리: 구현 완료
  - 알림/로깅: 구현 완료
  - WebSocket 안정 연결: 구현됨, 실연결 검증 미완료

## 9. 공식 참고

- KIS Developers 소개/공지:
  - https://apiportal.koreainvestment.com/intro
- KIS Open API 소개:
  - https://apiportal.koreainvestment.com/about-openapi
