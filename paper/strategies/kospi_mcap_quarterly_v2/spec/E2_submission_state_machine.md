# E2 발주 · 주문 상태기계

- 상태: `DRAFT` (2026-09-17 16시대)
- 층: 집행 층
- 게이트: G1 초안 / G2 코드 있음 / G3 **가짜 브로커** 시험 통과 / G4~G8 미착수 — **실제 KIS 모의 발주는 아직 0건** (09-22 장중 1건 시험 예정)
- 코드: `src/execution.py` (`OrderBook`, `submit_orders`, `resolve_unknown`, `cancel_remaining`, `replay_candidates`)
- 블록: C9 A(집행 시간), C9 C(15:20 취소·다음 거래일 1회·UNKNOWN_PENDING 재전송 금지·중복 건너뜀·강제 재전송 금지), `tools/kis_order_client.py` 의 `place_order_cash`·`cancel_order` (수정 없이 호출)

## 입력
- E1 주문 목록
- KIS 클라이언트 (`KISOrderClient.from_env(mock=True)`)
- 이벤트 로그 경로 (날짜 없는 append-only `order_events.jsonl`)
- 지금 시각

## 판정 — 상태

```
(없음) -> SUBMITTING -> ACCEPTED | REJECTED | REJECTED_LOCAL | UNKNOWN_PENDING
UNKNOWN_PENDING -> ACCEPTED (조회로 한 건 확인) | NOT_RECEIVED (조회 완전 + 맞는 건 없음)
ACCEPTED / PARTIALLY_FILLED -> PARTIALLY_FILLED | FILLED | CANCELLED_UNFILLED | PARTIAL_CANCELLED | CANCEL_FAILED
끝 상태: FILLED, REJECTED, REJECTED_LOCAL, CANCELLED_UNFILLED, PARTIAL_CANCELLED, NOT_RECEIVED
```
- 표에 없는 전이, 체결 수량이 주문 수량을 넘음, 체결 수량이 줄어듦 → 기록 거부(예외)
- 로그가 유일한 진실. 다시 읽으면 같은 상태가 나와요

## 판정 — 발주

| 규칙 | 값 | 출처 |
|---|---|---|
| 발주 시간 | 09:05:00 ~ 15:20:00 밖이면 **한 건도** 안 보냄 | C9 A |
| 발주 금지 구간 | 09:19:30 ~ 09:21:30 | 09:20 주문 카나리아가 같은 계좌에 1주 주문·취소(09-17 확인) — Claude 설정 |
| 모의계좌만 | 설정 `mock_only` + 클라이언트 `cfg.mock` 둘 다 확인 | 사용자 Q1 (KIS 모의), 실계좌 전환은 R5 이후 |
| 같은 order_id 가 로그에 있으면 | 건너뜀 | C9 C |
| attempt_no > 2 | 거부 | C9 C (다음 거래일 1회) |
| 발송 전 입력 오류(`ValueError`) | `REJECTED_LOCAL` — 확실히 안 나감 | 2026-09-17 코드 확인 |
| **발송 중 모든 예외** | `UNKNOWN_PENDING`, **나머지 주문 멈춤** | 아래 "왜" |
| 성공인데 주문번호 없음 | `UNKNOWN_PENDING`, 나머지 멈춤 | 귀속 불가 |
| 브로커 거절 | `REJECTED`, 다음 주문은 계속 | — |

**왜 모든 예외를 모르는 상태로 보나 (2026-09-17 `kis_order_client.py:568-632` 확인)**
- POST 타임아웃은 재시도 없이 `UNKNOWN_PENDING` 으로 올라와요 — 맞게 돼 있어요
- 그런데 연결 오류(`Connection aborted` 등)는 `NETWORK, retryable=True` 로 올라와요. 요청이 서버에 닿은 뒤 응답 중 끊기면 **주문은 접수됐는데 호출한 쪽은 실패로 알 수 있어요.** 09-16 20:25 잔고 조회에서 이 오류가 실제로 났어요
- 그래서 이 칸은 분류를 믿지 않고, 발송 중 난 예외는 전부 조회로 확인할 때까지 재전송하지 않아요

## 판정 — 모르는 상태 확인
- 당일 주문 조회 결과에서 **종목·방향·수량·가격이 정확히 한 건** 맞으면 → ACCEPTED (주문번호 채움)
- 조회가 완전하고 맞는 건이 없으면 → NOT_RECEIVED (이후 재주문 대상)
- 두 건 이상 맞으면 → 그대로 둠 (추정 금지)
- 이미 다른 주문에 귀속된 브로커 주문번호는 후보에서 빼요

## 판정 — 취소·재주문
- 15:20:00 이전 취소 요청 → 거부
- 15:20 이후: ACCEPTED·PARTIALLY_FILLED·CANCEL_FAILED 를 전량 취소 요청 → 체결 0 이면 CANCELLED_UNFILLED, 있으면 PARTIAL_CANCELLED / 실패는 CANCEL_FAILED 로 크게 남김
- 재주문 대상: 1차가 취소·거절·미접수로 끝나 남은 수량이 있는 의도. 2차가 이미 있으면 대상 아님. 재주문 수량은 E1 을 다음 날 호가로 다시 돌려서 정해요(C9 C "신선 호가로 다시 계산")

## 출력
- `order_events.jsonl` 이벤트 한 줄: order_id, intent_id, attempt_no, code, side, limit_price, requested_qty, rebalance_id, state, ts (+ broker_order_no, broker_org_no, rt_cd, msg, error, filled_qty, resolved_by)
- 호출 결과 요약: submitted / accepted / rejected / unknown / skipped_existing / reasons

## 경계
- 체결 수량을 브로커에서 **가져오지 않아요** — E3 이 조회해서 이 상태기계에 기록해요
- 원장을 고치지 않아요(E4)
- 목표·수량을 다시 계산하지 않아요(E1)

## 시험 (2026-09-17, 가짜 브로커)

`tests/test_kospi_mcap_quarterly_v2_e2_execution.py` 16개: 접수·로그 재생 / 재실행 시 재전송 없음 / 창 밖·금지 구간 3경우 / 실계좌 클라이언트 거부 / 연결 오류 → 모르는 상태·나머지 멈춤·재실행해도 재전송 없음 / 입력 오류 → REJECTED_LOCAL / 거절·주문번호 없는 성공 / 발송 중 프로세스 죽음 → 모르는 상태 복구 / 잘못된 전이·초과 체결·체결 감소 / 모르는 상태 확인(한 건·없음·두 건) / 미접수 확정 / 15:20 전 취소 거부·부분 취소 / 취소 실패 기록 / 재주문 1회만.
V2 전체 **66 통과**.

## 열린 것
- **실제 KIS 모의 발주 0건.** 09-22 장중 1건 시험에서 확인할 것: 접수 응답의 주문번호·조직번호, 당일 주문 조회 필드 이름(`sll_buy_dvsn_cd`, `ord_unpr`, `ord_gno_brno`)이 이 코드의 가정과 같은지, 취소 응답
- 매도 대금 같은 날 매수 가능 여부 (D+2 결제)
- 재주문 실행 순서(다음 날 E1 → E2) 를 묶는 실행기는 E3·E4 뒤에
