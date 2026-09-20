# KIS API 전체 아키텍처 설계

기준일: 2026-03-22

## 1. 목표

- KIS REST/WS를 실전/모의 환경에서 안전하게 사용한다.
- 주문 실행, 계좌 조회, 시세 수집, 장애 대응, 알림을 분리한다.
- 실전 적용 전에는 dry-run, canary, healthcheck를 먼저 통과시킨다.

## 2. 현재 실제 아키텍처 계층

### A. 인증/접속 계층

- 파일:
  - `E:\1_Data\tools\kis_order_client.py`
- 역할:
  - 실전/모의 base URL 분기
  - access token 발급/캐시/재사용
  - websocket approval key 발급
  - REST 요청 공통 처리
  - rate limit 대응

### B. 사전점검 계층

- 파일:
  - `E:\1_Data\tools\kis_healthcheck.py`
- 역할:
  - token 발급 확인
  - 시세 조회 확인
  - 잔고 조회 확인
  - 미체결 조회 확인
  - 실패 시 alert 전송

### C. 주문 디스패치 계층

- 파일:
  - `E:\1_Data\tools\kis_order_dispatch_from_exec.py`
- 역할:
  - `orders_*_exec.xlsx` 입력
  - dry-run / apply 분기
  - 실전/모의 분리
  - 세션 시간 가드
  - 중복 방지
  - 매도 가능 수량/매수 가능 수량 사전검증
  - 결과 CSV/JSON 저장

### D. Canary 계층

- 파일:
  - `E:\1_Data\tools\kis_canary_run.py`
- 역할:
  - 주문 수 제한
  - 총 수량 제한
  - apply 전 확인 문자열 강제
  - 디스패치 계층으로 제한된 실행 전달

### E. 실시간 수신 계층

- 파일:
  - `E:\1_Data\tools\kis_realtime_ws.py`
- 역할:
  - websocket approval key 발급 후 연결
  - ping_interval/ping_timeout
  - reconnect / backoff
  - 구독 코드 수 제한
  - PINGPONG echo
  - 상태 파일/JSONL 기록

### F. 알림/로깅 계층

- 파일:
  - `E:\1_Data\tools\notify_channels.py`
- 역할:
  - Telegram
  - Kakao
  - file fallback
  - alert JSONL 저장

## 3. 현재 권장 실행 순서

1. `kis_healthcheck.py`
2. `kis_canary_run.py`
3. `kis_order_dispatch_from_exec.py --apply`
4. `kis_realtime_ws.py`
5. 필요 시 `kis_cancel_open_orders.py`, `kis_account_snapshot.py`, `kis_sync_fills_from_api.py`

## 4. 현재 데이터 흐름

### 주문 흐름

1. `orders_*_exec.xlsx`
2. `kis_order_dispatch_from_exec.py`
3. `KISOrderClient`
4. KIS REST 주문 API
5. `orders_*_broker_submit_{mock|prod}.csv`
6. `kis_order_dispatch_*.json`

### 실시간 흐름

1. `KISOrderClient.issue_ws_approval_key()`
2. `kis_realtime_ws.py`
3. KIS WS
4. `kis_ws_ticks_YYYYMMDD.jsonl`
5. `kis_ws_status_latest.json`

### 운영 통제 흐름

1. `kis_healthcheck.py`
2. 실패 시 `notify_channels.send_alert()`
3. `2_Logs` 및 `alerts/*.jsonl` 저장

## 5. 현재 설계 장점

- 실전/모의 분리가 명확하다.
- 디스패치 전에 healthcheck/canary 계층이 있다.
- 주문 실행과 실시간 수신이 분리돼 있다.
- 알림 채널 실패 시에도 file fallback이 남는다.
- 최근 보강으로 websocket 상태가 실제 파일에 남는다.

## 6. 현재 남은 설계상 약점

- websocket 계층은 현재 단일 스크립트형이라, 재사용 가능한 manager 클래스로 분리돼 있지는 않다.
- subscribe/unsubscribe 구조는 보강됐지만 외부 전략 엔진과 callback 인터페이스까지 닫힌 구조는 아니다.
- 장시간 soak 관점의 WS 재연결 검증 산출물은 더 필요하다.

## 7. 현재 결론

- 현재 KIS 아키텍처는 아래 구조로 보는 것이 맞다.
  - `공통 클라이언트`
  - `사전점검`
  - `주문 디스패치`
  - `canary 가드`
  - `실시간 수신`
  - `알림/로깅`
- 즉 전체 방향은 운영형 구조로 잡혀 있다.
- 남은 보강 포인트는 구조 부재보다 WS 장시간 운용 검증 쪽이다.
