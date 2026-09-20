## 2026-04-24 부분 검증 및 status JSON 수정
- 백업:
  - `E:\1_Data\backup\20260424_news_gate_block_verify\20260424_1126_news_status_json_validity`
- 수정:
  - `E:\1_Data\tools\news_score_daily.py`
  - `news_score_status_*.json` 출력만 `ensure_ascii=True`로 변경
  - 뉴스 점수 계산, news gate 의미, final score 가중치 변경 없음
- 문법 검증:
  - `python -m py_compile E:\1_Data\tools\news_score_daily.py` 통과
- 실행 검증:
  - `python E:\1_Data\tools\news_score_daily.py` rc=0
  - rows=12, mapped=12, mapped_rate=100.00%, nonzero_rate=41.67%, quality=PASS
  - `INTEGRATED_OPS_UPDATE_PLANS=0 python E:\1_Data\tools\build_integrated_ops_snapshot.py` rc=0
- 결과물 검증:
  - `E:\1_Data\2_Logs\news_score_status_latest.json` PowerShell `ConvertFrom-Json` 파싱 통과
  - `news.quality=PASS`, `asof_ymd=20260423`, `used_lag_days=0`
  - `integrated_ops_snapshot_latest.summary.top_blocker_effective=현재 확인된 차단 이슈 없음`
  - `blocking_issues_effective=[]`
  - `calc_issue_rows.news_score=NORMAL`
  - `final_score_merge_status_latest.news_gate.gate=OPEN`
  - `final_score_merge_status_latest.news_dynamic_weight.effective_weight=0.12`
- 남은 검증:
  - 현재 런타임은 `news_gate=OPEN`이라 원 계획의 `CLOSED + effective_weight=0` 완화 조건은 직접 재현 검증하지 못함
  - 따라서 이 항목은 운영 현재값 기준 부분 검증 상태

## 목표
- `news_gate=CLOSED` 상태에서 `news_score calc_issue:ISSUE`가 신규진입 hard block으로 승격되지 않도록 수정한다.

## 비목표
- 뉴스 수집/점수 계산 자체를 바꾸지 않는다.
- `news_score_status_latest.json`의 `quality=FAIL` 표시를 억지로 바꾸지 않는다.

## 작업 범위
- `E:\1_Data\tools\build_integrated_ops_snapshot.py`

## 현재 동작
- `news_score_status_latest.json`은 `quality=FAIL`, `mapped_rows=0`, `nonzero_rows=0`이라 calc_issue는 ISSUE가 된다.
- 하지만 `final_score_merge_status_latest.json`과 `signal_integration_status_*.json`은 이미 `news_gate=CLOSED`, `news weight=0`로 fail-soft 처리 중이다.
- 그럼에도 integrated snapshot이 `news_score_quality_gate`를 effective blocker로 올려 entry gate가 BLOCK이 된다.

## 구현 계획
1. `news_gate=CLOSED`이고 `final_score/news_dynamic_weight.effective_weight=0`이면 `news_score_quality_gate`를 effective blocker에서 제외한다.
2. 같은 조건이면 `news_score calc_issue_state`를 ISSUE가 아닌 degraded 경고 수준으로 낮춘다.

## 검증 계획
1. 기능 검증: 문법 검증
2. 정합성 검증: integrated snapshot의 `calc_issue_rows`와 `blocking_issues_effective` 확인
3. 운영 반영 검증: snapshot 재생성 후 엔진 경로 재실행 또는 상태 파일 갱신 확인
4. 정책 검증: news gate가 OPEN인데 품질 FAIL이면 기존처럼 계속 block 유지
5. FAIL-CLOSED 검증: CLOSED+zero-weight일 때만 완화되는지 확인
6. 회귀 검증: dashboard/p1 reason에서 `news_score:ISSUE` 제거 확인
