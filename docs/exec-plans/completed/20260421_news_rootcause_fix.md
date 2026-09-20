# 20260421_news_rootcause_fix

## Goal
- 뉴스 수집/점수 경로에서 확인된 원인 4가지를 최소 범위로 수정한다.
- 대상 원인:
  - soft skip를 성공처럼 기록하는 문제
  - API 호출 전 quota 선차감 문제
  - 후보 기사 커버리지 0이어도 품질이 완화되는 문제
  - NER/entity tag mojibake 정규화 누락

## In-Scope
- `E:\1_Data\tools\news_collect_naver_daily.py`
- `E:\1_Data\tools\news_score_daily.py`
- `E:\1_Data\tests\` 하위 최소 검증 파일 추가
- `E:\1_Data\PLANS.md` 진행 업데이트 반영

## Out-of-Scope
- 뉴스 weight 정책 자체 변경
- paper engine gate 의미 변경
- 외부 API provider 변경
- 배치 엔트리포인트 변경

## Current State
- `news_collect_status_latest.json` 최신 상태는 `reason=quota_budget_guard`, `quality=WARN`, `fetched=0`, `saved=0`.
- `news_score_status_latest.json` 최신 상태는 `reason=candidate_article_coverage_zero`, `mapped_rate=0.0`, `nonzero_rate=0.0`.
- `final_score_merge_status_latest.json` 기준 뉴스 leg는 `gate=CLOSED`, `news weight=0.0`.
- 수집기는 `quota_budget_guard/session_budget_guard/naver_quota_exceeded`도 `last_success`로 기록한다.
- 수집기는 API 호출 전에 quota를 먼저 증가시킨다.

## Risks And Fail-Closed Checks
- quota 처리 순서 변경으로 status/집계 값이 달라질 수 있으므로 기존 reason 의미는 유지한다.
- 뉴스 quality 판정 강화로 downstream이 `WARN -> FAIL`로 바뀔 수 있으므로 final score와 gate 동작을 함께 검증한다.
- 정책 의미 변경 없이, 실제 기사 커버리지 부족만 더 정확히 드러내는 범위로 제한한다.

## Steps
1. `news_collect_naver_daily.py`에서 last_success 기록 조건과 quota 차감 시점을 수정한다.
2. `news_score_daily.py`에서 기사 미커버리지/fallback 0점 케이스를 별도 reason/quality로 분리한다.
3. entity tag 정규화 경로를 단일화해 mojibake 출력을 줄인다.
4. 단위 테스트를 추가한다.
5. 문법 검증, 테스트 검증, 실행 검증, 결과물 검증을 수행한다.

## Validation
1. 기능 검증: 수집기/점수기 분기 테스트
2. 정합성 검증: status JSON의 reason/quality/trace 값 확인
3. 운영 반영 검증: `run_news_pipeline_once.bat` 또는 공식 경로 실행
4. 정책 검증: news gate 의미 유지 여부 확인
5. FAIL-CLOSED 검증: 기사 커버리지 0일 때 news leg가 닫히는지 확인
6. 회귀 검증: 기존 정상 실행이 깨지지 않는지 확인

## Rollback Point
- `E:\1_Data\_bak\news_collect_naver_daily.py.bak_20260421_140722_news_rootcause_fix.py`
- `E:\1_Data\_bak\news_score_daily.py.bak_20260421_140722_news_rootcause_fix.py`
- `E:\1_Data\_bak\PLANS.md.bak_20260421_140722_news_rootcause_fix.md`
