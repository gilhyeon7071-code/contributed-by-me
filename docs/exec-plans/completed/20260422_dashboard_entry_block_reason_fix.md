## 목표
- 대시보드 `진입 차단` 문구가 stale `after_close_summary_last.json`의 예전 hard block reason을 다시 표시하지 않도록 수정한다.

## 비목표
- `p0`, `gate_daily`, `paper_engine`, `after_close_summary.py` 로직은 바꾸지 않는다.
- 실제 진입 정책 의미는 바꾸지 않는다.

## 작업 범위
- `E:\vibe\buffett\dashboard.py`

## 현재 동작
- 대시보드 overview는 최신 `gate_summary.action`을 보면서도,
- 차단 이유 텍스트는 stale `after_close.gate.engine_action.hard_block`를 우선 사용한다.
- 그 결과 최신 `gate1=PASS`, `risk_off=false` 상태에서도 예전 `KRX데이터_universe_degraded(...)`가 다시 표시될 수 있다.

## 구현 계획
1. overview의 차단 사유 계산에서 `after_close.gate.engine_action.hard_block`를 최신 gate 실패 시에만 사용한다.
2. 최신 `gate_summary`에 `gate1/gate2 PASS`, `risk_off false`이면 `p1_entry_gate_reason` 또는 pending reason을 우선 표시한다.

## 검증 계획
1. 기능 검증: 문법 검증
2. 정합성 검증: 최신 dashboard_state/gate/p1 JSON과 표시 문자열 비교
3. 운영 반영 검증: dashboard 상태 재생성 후 Streamlit 코드 경로 기준 reason 확인
4. 정책 검증: 실제 BLOCK 상태는 유지하되 stale reason만 제거되는지 확인
5. FAIL-CLOSED 검증: 실제 gate hard block일 때는 기존 hard block reason 유지 확인
6. 회귀 검증: 대시보드 최신 JSON/코드 경로에서 `KRX데이터_universe_degraded` 비노출 확인
