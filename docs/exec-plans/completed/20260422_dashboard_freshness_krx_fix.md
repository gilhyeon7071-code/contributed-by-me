## 목표
- 대시보드 `진입 차단`에 남아 있는 `KRX데이터 기준일 불일치`를 제거한다.
- 범위는 dashboard freshness guard와 resilience check의 KRX 기준일 입력 정렬에 한정한다.

## 비목표
- `p0_daily_check.py`, `gate_daily.py`, `paper_engine.py` 정책 의미는 바꾸지 않는다.
- KRX 수집/정제 로직은 다시 손대지 않는다.

## 작업 범위
- `E:\vibe\buffett\tools\build_dashboard_state_v2.py`
- `E:\1_Data\tools\build_resilience_check.py`

## 현재 동작
- `build_dashboard_state_v2.py` freshness guard는 `p0.krx_clean.date_max`만 fallback으로 사용한다.
- `build_resilience_check.py`는 `krx_max`를 `p0.prices.date_max`에서 읽는 잘못된 경로가 있다.
- 그 결과 `p0/gate`는 PASS인데 대시보드만 `daily_asof_mismatch krx_clean:20260420`으로 hard block 될 수 있다.

## 위험 요소
- 대시보드 hard block 기준을 잘못 완화하면 실제 stale data를 놓칠 수 있다.
- 따라서 `tolerated_zero_ohlc soft-skip`에서만 `effective_date_max`를 우선하고, 일반 stale은 그대로 유지해야 한다.

## 구현 계획
1. `build_dashboard_state_v2.py`에서 `p0.krx_clean.effective_date_max`/`effective_ncode`를 읽고 KRX freshness 비교에 우선 사용한다.
2. `build_resilience_check.py`에서 `krx_max`를 `p0.krx_clean.effective_date_max -> p0.krx_clean.date_max` 순으로 읽도록 수정한다.

## 검증 계획
1. 기능 검증: 문법 검증
2. 정합성 검증: resilience JSON, dashboard_state JSON의 KRX 기준일 비교
3. 운영 반영 검증: `build_dashboard_state_v2.py` 직접 실행
4. 정책 검증: `p0/gate` 결과와 대시보드 action/gate1이 일치하는지 확인
5. FAIL-CLOSED 검증: 일반 stale KRX는 여전히 mismatch로 남는지 코드상 유지 확인
6. 회귀 검증: 대시보드 최신 JSON 재생성 후 차단 문구 제거 확인

## 롤백 계획
- 백업본으로 두 파일 즉시 복원
