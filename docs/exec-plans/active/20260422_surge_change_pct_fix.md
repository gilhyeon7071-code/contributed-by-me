## 2026-04-24 검증
- 백업:
  - `E:\1_Data\backup\20260424_surge_change_pct_verify\20260424_1129_surge_change_pct_verify`
- 수정:
  - 없음
- 문법 검증:
  - `python -m py_compile E:\1_Data\tools\surge_detector_realtime.py` 통과
  - `python -m py_compile E:\1_Data\tools\surge_ml_score_realtime.py` 통과
- 실행 검증:
  - 공식 경로 `E:\1_Data\_runtime\python312-embed\python.exe` 사용
  - `surge_ml_score_realtime.py` rc=0
  - `surge_detector_realtime.py` rc=0
- 결과물 검증:
  - `E:\1_Data\2_Logs\surge_realtime_latest.json` status=OK
  - `E:\1_Data\2_Logs\surge_ml_score_latest.json` status=OK
  - expected_prev_weekday=20260423
  - realtime CSV rows=40
  - ML CSV rows=32
  - realtime stale_ref_skip_count=8
  - ML stale_ref_skip_count=8
  - impossible_move_skip_count=0
  - `change_pct > 0.305` alert rows=0
  - stale reference가 `REF_STALE`로 표시되지 않은 rows=0
  - ML `ret1 > 0.305` rows=0
  - ML stale reference rows=0
- 문제 종목 확인:
  - `010820` realtime row:
    - current_price=14960
    - prev_close=13480
    - reference_latest_date=20260417
    - expected_prev_weekday=20260423
    - calc_issue=REF_STALE
    - change_pct=0.0
    - surge_flag=False
    - exclude_reasons=`REF_STALE:20260417!=20260423`
  - `010820`은 ML CSV에 없음

## 목표
- 급등감지의 `change_pct`가 stale `prev_close` 때문에 한국시장 상한 범위를 넘는 값으로 계산되는 원인을 제거한다.

## 비목표
- 급등 전략 임계값 자체는 바꾸지 않는다.
- 뉴스/ML 가중치 정책은 바꾸지 않는다.

## 작업 범위
- `E:\1_Data\tools\surge_detector_realtime.py`
- `E:\1_Data\tools\surge_ml_score_realtime.py`

## 현재 동작
- 두 스크립트 모두 `paper/prices/ohlcv_paper.parquet`의 마지막 종가를 참조값으로 사용한다.
- 참조 가격이 최신 이전 거래일보다 밀리면 `change_pct`와 ML feature `ret1/ret3/ret5/ret10`이 비정상적으로 커질 수 있다.
- 실제 확인값: `010820`가 `20260417 close=13480`을 전일종가처럼 사용해 `20260422` 장중 `+30%` 초과 값이 계산됐다.

## 위험 요소
- stale ref를 단순 허용하면 잘못된 급등 알림이 계속 발생한다.
- stale ref를 막으면 일부 종목이 알림/ML 점수에서 제외될 수 있다.

## 구현 계획
1. 최신 `p0_daily_check`에서 기대 이전 거래일(`prev_weekday`)을 읽는다.
2. 종목별 reference latest date가 기대 이전 거래일과 다르면 fail-closed로 제외한다.
3. 추가 안전장치로 한국시장 일일 변화율 상한(`0.305`) 초과값은 계산 이상으로 차단한다.
4. detector/ML 산출물에 skip 사유와 집계 수치를 남긴다.

## 검증 계획
1. 기능 검증: 문법 검증
2. 정합성 검증: `010820`가 stale ref 또는 impossible move로 제외되는지 확인
3. 운영 반영 검증: `surge_realtime_latest.json/csv`, `surge_ml_score_latest.json/csv` 재생성 확인
4. 정책 검증: 정상 종목은 계속 알림이 남는지 확인
5. FAIL-CLOSED 검증: ref stale 시 허용이 아니라 제외되는지 확인
6. 회귀 검증: 최신 급등 알림 수와 상위 종목이 비정상적으로 전부 사라지지 않았는지 확인
