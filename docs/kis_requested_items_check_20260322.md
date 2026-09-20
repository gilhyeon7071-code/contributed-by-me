# KIS 요청 항목 체크

기준일: 2026-03-22

## 1. 체크 기준

- 필요한가
- 현재 구현돼 있는가
- 로직구현 판정에 들어가는가

## 2. 체크 결과

### 1) KIS Open API 핵심 제약사항 먼저 파악하기

- 필요한가: 예
- 현재 구현돼 있는가: 부분 구현
- 로직구현 판정에 들어가는가: 직접은 아님
- 판단:
  - 코드에는 실전/모의 URL 분리, 토큰, approval key, rate limit, dry-run 가드가 이미 반영돼 있다.
  - 하지만 “핵심 제약사항을 먼저 파악했다”는 항목은 구현 모듈이라기보다 운영 기준 정리 항목이다.
  - 따라서 필요하지만, 판정 프레임의 직접 구현 항목으로 보기는 어렵다.

### 2) KIS API 전체 아키텍처 설계

- 필요한가: 예
- 현재 구현돼 있는가: 부분 구현
- 로직구현 판정에 들어가는가: 직접은 아님
- 판단:
  - 현재 코드에는 공통 클라이언트, healthcheck, canary, dispatch, websocket, alert 계층이 있다.
  - 하지만 이 항목은 구현 모듈보다 상위의 설계 정리 항목이다.
  - 따라서 필요하지만, 로직구현 판정의 직접 점수 항목은 아니다.

### 3) Access Token 자동 갱신 모듈

- 필요한가: 예, 필수
- 현재 구현돼 있는가: 예
- 로직구현 판정에 들어가는가: 간접 포함
- 근거:
  - `E:\1_Data\tools\kis_order_client.py`
  - `_load_cached_token()`
  - `_save_cached_token()`
  - `_issue_token()`
  - `_ensure_token()`
- 판단:
  - 운영 안정성에는 필수다.
  - 현재 판정에서는 운영 안정성 쪽 간접 근거로 연결된다.

### 4) REST API 호출 제한 대응: Rate Limiter

- 필요한가: 예, 필수
- 현재 구현돼 있는가: 예
- 로직구현 판정에 들어가는가: 간접 포함
- 근거:
  - `E:\1_Data\tools\kis_order_client.py`
  - `min_request_interval_sec`
  - `rate_limit_retries`
  - `rate_limit_backoff_sec`
  - `429` 재시도 처리
- 판단:
  - 실전 운용에는 필수다.
  - 현재 판정에서는 직접 코드 항목보다 운영 안정성 근거로 간접 반영된다.

### 5) WebSocket 안정 연결 모듈

- 필요한가: 예, 사실상 필수
- 현재 구현돼 있는가: 예
- 로직구현 판정에 들어가는가: 간접 포함
- 근거:
  - `E:\1_Data\tools\kis_realtime_ws.py`
  - reconnect/backoff
  - ping_interval/ping_timeout
  - approval key 발급
  - PINGPONG echo
  - 상태 파일 기록
  - `E:\1_Data\2_Logs\kis_ws_status_latest.json`
- 판단:
  - 실시간 시세/체결 수신을 쓰면 필수다.
  - 현재 판정에는 운영 안정성 근거로는 연결되지만, 직접 독립 항목은 아니다.

### 6) 주문 실행 모듈 (실전/모의 완전 분리)

- 필요한가: 예, 필수
- 현재 구현돼 있는가: 예
- 로직구현 판정에 들어가는가: 예, 간접이 아니라 꽤 직접적
- 근거:
  - `E:\1_Data\tools\kis_order_dispatch_from_exec.py`
  - `--mock`
  - `--apply`
  - `DRY_RUN`
  - mock/prod 산출물 분리
  - precheck / duplicate guard / session guard
- 판단:
  - 실행 엔진과 운영 통제에 직접 연결된다.
  - 현재 로직구현 판정에서도 execution/operational 쪽과 가깝게 연결된다.

### 7) 알림 + 로깅 시스템

- 필요한가: 예, 필수
- 현재 구현돼 있는가: 예
- 로직구현 판정에 들어가는가: 예
- 근거:
  - `E:\1_Data\tools\notify_channels.py`
  - Telegram / Kakao / file fallback
  - `E:\checking_logic\artifacts\system_completeness\system_completeness_report.json`
  - `OPS_LOGGING`
  - `OPS_ALERTING`
- 판단:
  - 이 항목은 현재 판정 프레임에 직접 들어간다.

## 3. 최종 정리

- 1, 2: 필요함 / 직접 판정 항목은 아님 / 설계·운영 기준 정리 항목
- 3, 4, 5: 필요함 / 구현돼 있음 / 판정에는 운영 안정성 측면으로 간접 연결
- 6, 7: 필요함 / 구현돼 있음 / 판정에도 직접 가깝게 연결

## 4. 결론

- 요청 항목은 전부 필요하다.
- 미구현으로 볼 항목은 현재 없다.
- 다만 1, 2는 “구현 완료”보다 “설계/기준 정리 완료”라고 보는 것이 맞다.
- 6, 7은 현재 판정 프레임과 가장 직접 연결된다.
