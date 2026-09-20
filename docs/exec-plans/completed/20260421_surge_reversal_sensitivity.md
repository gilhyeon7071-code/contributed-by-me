# 2026-04-21 Surge Reversal Sensitivity

## 목적
- 기존 급등 A안 비교 실험의 후속으로 `trigger_count / reversal threshold` 민감도와 `REVERSAL_NEXT_OPEN` 기여도를 검증한다.

## 범위
- 대상 파일:
  - `E:\1_Data\tools\surge_reversal_compare.py`
  - `E:\1_Data\tests\test_surge_reversal_compare.py`
  - `E:\1_Data\PLANS.md`
- 운영 엔진 정책/배치 엔트리포인트는 변경하지 않는다.

## 작업
1. 기본 A안 비교는 유지한다.
2. `trigger_count`, `volume_exhaustion_ratio_max`, `high_rejection_min_drawdown_pct` 민감도 케이스를 추가한다.
3. `REVERSAL_NEXT_OPEN` 발생 건에 대해 baseline 대비 손익 기여도를 집계한다.
4. JSON/CSV 산출물과 테스트로 검증한다.

## 검증
1. 문법 검증
2. 테스트 검증
3. 스크립트 실행 검증
4. 산출물(JSON/CSV) 검증
