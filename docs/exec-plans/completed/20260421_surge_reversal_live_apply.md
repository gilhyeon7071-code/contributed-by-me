# 2026-04-21 Surge Reversal Live Apply

## 목적
- 급등 포지션 청산에 reversal 기반 익일 시가 청산을 실제 운영 경로에 최소 범위로 반영한다.

## 범위
- 대상 파일:
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\paper\paper_engine_config.json`
  - `E:\1_Data\PLANS.md`
- 배치 엔트리포인트 의미, FAIL-CLOSED 의미, kill_switch 의미는 변경하지 않는다.

## 현재 동작
- 운영 경로의 `surge_exit_policy`는 stop/tp/max_hold만 처리한다.
- reversal 기반 급등 청산은 실험 스크립트에만 존재한다.

## 적용안
1. `surge_exit_policy.reversal_exit` 설정 추가
2. 급등 포지션에서 reversal 신호 2개 이상이면 익일 시가에 전량 청산
3. 운영값:
   - `trigger_count=2`
   - `volume_exhaustion_ratio_max=0.6`
   - `high_rejection_min_drawdown_pct=0.0`

## 검증
1. 문법 검증
2. 단독 실행 검증
3. 배치 실행 검증
4. 로그/JSON 결과 검증
