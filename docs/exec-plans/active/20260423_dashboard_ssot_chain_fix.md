## 목표
- 대시보드 현황판이 전일 `last` 요약 파일과 최신 운영 산출물을 섞어 읽으면서 반복적으로 stale 표시를 만드는 구조를 제거한다.

## 비목표
- 투자/운영/전략 탭 전체를 재설계하지 않는다.
- 매매 정책, Gate 의미, FAIL-CLOSED 의미는 바꾸지 않는다.

## 작업 범위
- `E:\1_Data\run_paper_daily.bat`
- `E:\vibe\buffett\tools\build_dashboard_state_v2.py`
- `E:\vibe\buffett\dashboard.py`
- `E:\1_Data\.agent\PLANS.md`

## 현재 동작
- `market_rising_latest.json`이 오늘 생성되지 않으면 현황판 상승종목은 빈 상태가 된다.
- 현황판 `매수 후보`는 최신 `pending_entry_status`보다 전일 `after_close_summary_last.json.candidates.raw_count`를 먼저 사용한다.
- 공식 배치에는 `market_rising_snapshot.py`와 `after_close_summary.py`가 현황판 직전 체인에 고정 포함되어 있지 않다.

## 위험 요소
- 배치 post-chain 순서 변경 시 dashboard build 이전 산출물 생성 타이밍이 어긋날 수 있다.
- `after_close_summary.py`를 배치에 넣을 때 실패를 hard-fail로 만들면 비거래 핵심 경로까지 막을 수 있다.

## 구현 계획
1. 공식 배치에서 현황판 입력 산출물(`after_close_summary`, `market_rising_snapshot`)을 dashboard build 직전에 생성한다.
2. `build_dashboard_state_v2.py`에 현황판용 정규화 값(`overview_runtime`)을 추가한다.
3. `dashboard.py` 현황판은 raw `last` 파일 대신 `dashboard_state_latest.json`의 정규화 값만 우선 사용하도록 바꾼다.
4. stale source는 표시하지 않고, source/path를 상태 파일에 남긴다.

## 검증 계획
1. 기능 검증: 문법 검증
2. 정합성 검증: `dashboard_state_latest.json`의 `overview_runtime` 값과 source 확인
3. 운영 반영 검증: 배치 파일에 생성 단계 포함 여부와 상태 파일 갱신 확인
4. 정책 검증: Gate/FAIL-CLOSED 의미 변경 없음 확인
5. FAIL-CLOSED 검증: stale rising 데이터는 계속 숨김 처리되는지 확인
6. 회귀 검증: 현황판 후보 수/상승 현황이 stale raw count에 끌려가지 않는지 확인
## 2026-04-24 Verification Evidence
- Backup:
  - `E:\1_Data\backup\20260424_dashboard_ssot_chain_fix_verify\20260424_110929`
- Direct post-chain run order:
  - `after_close_summary.py`
  - `E:\vibe\buffett\tools\market_rising_snapshot.py`
  - `E:\vibe\buffett\tools\build_dashboard_state_v2.py`
- Runtime outputs:
  - `E:\1_Data\2_Logs\after_close_summary_20260424_111043.json`
  - `E:\1_Data\2_Logs\market_rising_latest.json`
  - `E:\vibe\buffett\runs\dashboard_state_latest.json`
- Confirmed values:
  - `dashboard_state_latest.overall=PASS`
  - `dashboard_state_latest.status_overall=PASS`
  - `overview_runtime.buy_candidates_count=0`
  - `overview_runtime.buy_candidates_source=signals.pending_candidates_after_caps`
  - `pending_entry_status_latest.candidates_after_caps=0`
  - `overview_runtime.after_close_candidates_raw=12`
  - `overview_runtime.rising_rows_count=51`
  - `market_rising_latest.status=OK`
