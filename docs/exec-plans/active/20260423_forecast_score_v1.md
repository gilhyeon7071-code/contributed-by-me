# 2026-04-23 forecast score v1

## 2026-04-24 검증 및 산출물 JSON 수정
- 백업:
  - `E:\1_Data\backup\20260424_forecast_score_verify\20260424_1128_forecast_json_validity`
- 수정:
  - `forecast_score_validation_*.json` 출력만 `ensure_ascii=True`로 변경
  - 점수 계산, Gate 의미, forecast 가중치, 진입/청산 정책 변경 없음
- 문법 검증:
  - `python -m py_compile E:\1_Data\tools\final_score_merge_daily.py` 통과
- 실행 검증:
  - `python E:\1_Data\tools\final_score_merge_daily.py` rc=0
  - 입력 `candidates_latest_data.with_policy_score.csv`, rows=12, final_nonzero=12
- 결과물 검증:
  - `E:\1_Data\2_Logs\forecast_score_validation_latest.json` PowerShell `ConvertFrom-Json` 파싱 통과
  - `forecast.status=PASS`
  - `score_asof_ymd=20260423`
  - `forward_asof_ymd=20260423`
  - `asof_alignment=MATCH`
  - `counts.score_rows=12`, `forward_rows=12`, `merged_rows=12`, `forward_proxy_rows=12`
  - `coverage.merge_rate=1.0`, `coverage.forward_proxy_rate=1.0`
  - 후보 CSV rows=12
  - `forecast_score`, `forecast_label`, `forecast_score_source`, `final_score` 컬럼 존재
  - `forecast_score` 범위 `0.336684~0.711782`, nonzero=12
- FAIL-CLOSED 검증:
  - 임시 missing forward 경로로 `_build_forecast_validation` 호출
  - 결과 `status=FAIL`, `reason=forward_estimate_missing`, `forward_rows=0`
- 회귀 확인:
  - `final_score_merge_status_latest.json` 파싱 통과
  - `nonzero_rows.final=12`, `nonzero_rows.forecast=12`

## 목표
- 기존 Gate 의미를 바꾸지 않고, 예측 관점의 `forecast_score` 레이어를 추가한 뒤 보수적 가중치로 `final_score`에 연결한다.
- 결과는 후보 CSV와 상태 JSON에 분리 기록한다.

## 비목표
- 뉴스/정책/리스크 Gate 의미 변경
- 진입/청산 정책 변경
- 브로커 엔트리포인트 변경
- ML 학습 파이프라인 추가

## 작업 범위
- `E:\1_Data\tools\final_score_merge_daily.py`
- `E:\1_Data\.agent\PLANS.md`
- 신규 계획 문서: `E:\1_Data\docs\exec-plans\active\20260423_forecast_score_v1.md`

## 현재 동작
- `final_score_merge_daily.py`는 sector/regime/news/fx/fundamental/policy/forecast를 합성해 `final_score`를 생성한다.
- 후보 CSV에는 `forecast_score`, `forecast_label`, `forecast_score_source`가 기록된다.
- 상태 JSON에는 `forecast_validation` 요약이 추가되고, 별도 `forecast_score_validation_latest.json`이 생성된다.
- `build_forward_estimate_snapshot.py`는 후보 체인 날짜/소스를 우선 따라 `forward_estimate_latest.csv`를 생성한다.
- 공식 배치에서는 `build_forward_estimate_snapshot.py`를 `tools\final_score_merge_daily.py` 직전에 다시 생성해야 같은 후보 체인을 본다.
- 이 규칙은 `E:\1_Data\run_paper_daily.bat`뿐 아니라 `E:\1_Data\run_news_pipeline_once.bat`에도 동일하게 적용한다.
- 2026-04-27 기준 `FORECAST_BLEND_V1`은 trained ML 미래예측 모델이 아니라 forecast proxy score로 분류한다.
- trained ML 미래예측 모델은 별도 `future_signal_preview_YYYYMMDD.csv/json` 산출물부터 시작한다.

## 위험 요소
- `final_score`나 Gate 의미를 건드리면 정책 변경이 된다.
- 새 점수 계산이 NaN 또는 극단값을 만들면 산출물 스키마만 추가해도 downstream 혼선이 생길 수 있다.
- preview/shadow 검증 없이 forecast proxy를 실제 미래예측 성능으로 해석하면 정책 판단이 왜곡된다.

## 단계
1. 후보 CSV에 이미 존재하는 입력만 사용해 정규화 함수 기반 `forecast_score` 계산 추가
2. `forecast_label`, `forecast_score_source` 기록
3. 상태 JSON에 `forecast_score`와 `forecast_nonzero` 통계 추가
4. `forecast_score`를 보수적 가중치로 `final_score`에 연결하고 기존 Gate 의미는 유지
5. `_cache/forward_estimate_latest.csv`와 대조하는 `forecast_score_validation_latest.json` 산출물 추가

## 검증 계획
1. 기능 검증: `forecast_score` 컬럼/상태 JSON 생성 확인
2. 정합성 검증: 기존 `final_score`/Gate 값 유지 확인
3. 운영 반영 검증: 최신 CSV/JSON 산출물 갱신 확인
4. 정책 검증: Gate 의미/가중치 무변경 확인
5. FAIL-CLOSED 검증: 입력 누락 시 0~1 범위 fail-soft 처리 확인
6. 회귀 검증: 기존 `final_score_merge_status` 주요 키와 `final_score` 비영점 행 유지 확인
7. 2차 검증: `forecast_score_validation_latest.json`의 coverage / asof / proxy-correlation 생성 확인

## 롤백 계획
- 백업본으로 `final_score_merge_daily.py`, `PLANS.md` 원복
- 신규 `forecast_score` / `forecast_validation` 산출물 제거

## 수집 증거
- `E:\1_Data\2_Logs\candidates_latest_data.with_final_score.csv`
- `E:\1_Data\2_Logs\final_score_merge_status_latest.json`
- `E:\1_Data\2_Logs\forecast_score_validation_latest.json`
- `E:\1_Data\docs\exec-plans\active\20260427_future_signal_preview_spec.md`
