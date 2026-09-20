# 20260421_surge_reversal_exp_compare

## Goal
- 현재 급등 진입 로직(`rule_plus_ml`)은 유지하고, 급등 보유 후 전환 신호 기반 방어(A안)가 현재 급등 청산 기준보다 성과 개선 가능성이 있는지 비교 실험한다.

## Non-Goal
- 운영 엔진 기본 정책 변경
- `paper_engine.py` 주문/체결/리스크 정책 반영
- SSOT 체인(`orders -> fills -> ledger -> stats`) 의미 변경

## In-Scope
- `E:\1_Data\tools\surge_reversal_compare.py`
- `E:\1_Data\tests\test_surge_reversal_compare.py`
- `E:\1_Data\PLANS.md`
- `E:\1_Data\docs\exec-plans\active\20260421_surge_reversal_exp_compare.md`

## Current Facts
- 현재 급등 진입은 `surge_backtest_report.py` 기준 `rule_plus_ml` 집합으로 검증 가능.
- 현재 최신 기준선:
  - `signals=38`
  - `win_rate=0.6842105263157895`
  - `avg_ret5=0.06687546824334699`
  - 증거: `E:\1_Data\2_Logs\surge_backtest_report_latest.json`
- 현재 운영 급등 청산 파라미터:
  - `stop_loss_pct=-0.05`
  - `take_profit_pct=0.1`
  - `max_hold_days=5`
  - `stop_sell_ratio_pct=40`
  - `preemptive_sell_ratio_pct=30`
  - 증거: `E:\1_Data\paper\paper_engine_config.json`

## Risks
- A안은 새 전환 신호 정의가 포함되어 있어 정책 민감 구간이다.
- 운영 엔진에 바로 반영하면 검증 없는 정책 변경이 되므로, 먼저 독립 비교 도구로만 실험한다.
- 실험 가정(예: 전환 신호 2개 이상 시 익일 시가 청산)은 명시적으로 기록한다.

## Design
- 진입 신호는 `surge_backtest_report.py`와 동일하게 `rule_plus_ml`을 사용한다.
- 동일 엔트리 집합에 대해 두 정책을 비교한다.
  - Baseline: 현재 급등 청산 기준(손절/익절/시간청산 중심)
  - Candidate A: Baseline + 전환 신호 2개 이상 시 익일 시가 우선 청산
- 비교 지표:
  - trades
  - win_rate
  - avg_ret
  - median_ret
  - cumulative_return
  - max_drawdown
  - exit_reason 분포

## Steps
1. 실험 도구 추가
2. 단위 테스트 추가
3. 문법 검증
4. 테스트 실행
5. 실험 실행
6. 결과 JSON/CSV 확인
7. `PLANS.md`에 사실 기반 업데이트

## Validation Plan
1. 기능 검증: 도구가 baseline/A 양쪽 결과를 모두 산출하는지 확인
2. 정합성 검증: 엔트리 수가 동일 집합에서 비교되는지 확인
3. 운영 반영 검증: NA (운영 엔진 미반영)
4. 정책 검증: 운영 설정 파일과 엔진 로직 무변경 확인
5. FAIL-CLOSED 검증: 실험 도구 실패 시 운영 경로 영향 없음 확인
6. 회귀 검증: 새 테스트 통과 확인

## Rollback
- `E:\1_Data\_bak\surge_backtest_report.py.bak_20260421_115947_reversal_exp_compare.py`
- `E:\1_Data\_bak\PLANS.md.bak_20260421_115947_surge_reversal_exp_compare.md`
- `E:\1_Data\_bak\paper_engine_config.json.bak_20260421_115947_surge_reversal_exp_compare.json`

## Evidence To Collect
- `E:\1_Data\2_Logs\surge_reversal_compare_latest.json`
- `E:\1_Data\2_Logs\surge_reversal_compare_trades_latest.csv`
- `E:\1_Data\2_Logs\surge_backtest_report_latest.json`
- `pytest` 결과
