# Algorithm Trading Platform SSOT v1

- 기준일: 2026-03-13
- 작성 기준 코드: `E:\vibe\buffett\dashboard.py`, `E:\vibe\buffett\dashboard_stock_v2.py`
- 작성 기준 산출물: `E:\vibe\buffett\runs\dashboard_state_latest.json`, `E:\vibe\buffett\runs\SSOT_TODAY_FINAL.json`, `E:\1_Data\2_Logs\trading_stage_validation_latest.json`

## 1) 목적

이 문서는 알고리즘 트레이딩 플랫폼의 화면 구조, KPI 체계, 계산 주체를 단일 SSOT로 고정한다.

원칙:
1. 화면은 계산 주체가 아니라 표시 주체다.
2. 핵심 체인은 `orders -> fills -> ledger -> stats`로 고정한다.
3. KPI는 파일/키 기준으로 추적 가능해야 한다.
4. 값이 없어도 레이아웃은 유지하고 `NA`로 표시한다.

## 2) 메뉴 구조(확정)

상단 1차 메뉴:
1. 투자
2. 운영
3. 전략

하위 상세 9개:
1. 전략개요
2. 진입
3. 청산
4. 사이징
5. 리스크
6. 필터
7. 백테스트
8. 가상매매
9. 실전매매

## 3) 핵심 체인(확정)

`orders -> fills -> ledger -> stats`

정의:
1. `orders`: `E:\vibe\buffett\data\orders\orders_{D}_exec.xlsx`
2. `fills`: `E:\vibe\buffett\data\live\live_fills.csv`
3. `ledger`: `E:\vibe\buffett\data\ledger\paper_fills_ledger.csv`
4. `stats`: 스냅샷 stats 디렉터리 (`E:\vibe\buffett\runs\SSOT_D*_FINAL_*\stats\*.json`)

## 4) 화면별 역할(확정)

투자:
1. 투자 결과와 현재 포지션 상태를 본다.

운영:
1. 파이프라인 상태와 차단 원인을 본다.

전략:
1. 전략 성과 원인과 품질을 본다.

## 5) KPI 계산 주체 확정표

표기 규칙:
1. `확정`: 현재 파일/키로 직접 읽을 수 있음
2. `임시`: 현재는 파생 또는 품질팩 산출물에 의존
3. `미구현`: 표준 산출물이 아직 없음(신규 산출물 필요)

### 5.1 투자 KPI

| KPI | 계산 주체(파일/키) | 상태 | 비고 |
|---|---|---|---|
| 총자산 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.equity_est` | 확정 | `build_dashboard_state_v2._build_account` 산출 |
| 일수익(원) | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.realized_today_krw` | 확정 | 당일 실현손익 기준 |
| 누적수익(원) | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.realized_total_krw` | 확정 | 누적 실현손익 기준 |
| 평가손익(원) | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.eval_pnl_krw` | 확정 | 평가손익 |
| 누적수익률(%) | `E:\1_Data\2_Logs\paper_pnl_summary_last.json` / `equity.end_equity` 기반 (`end_equity-1`)*100 | 임시 | 전용 수익률 키 미고정 |
| MDD(%) | `E:\1_Data\2_Logs\paper_pnl_summary_last.json` / `equity.max_drawdown_pct`*100 | 확정 | 가상매매 기준 MDD |
| 샤프 | `E:\1_Data\2_Logs\backtest_validation_latest.json` / `artifacts.base_metrics.sharpe` | 확정 | 백테스트 기준 |
| 포지션 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `positions.rows` | 확정 | 보유 종목 수 |
| 현금 비중(%) | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.cash_ratio_est` | 확정 | 추정치 |
| 총 익스포저(%) | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `account.position_value`, `account.equity_est` | 임시 | 파생값 `position_value/equity_est*100` |
| 승률 | `E:\1_Data\2_Logs\backtest_validation_latest.json` / `artifacts.base_metrics.win_rate` | 확정 | 백테스트 기준 |
| 손익비 | `E:\1_Data\2_Logs\verification_runtime_evidence_latest.json` / `phases.RISK.verify_cost_optimization.trade_stats.avg_win`, `avg_loss_abs` | 임시 | 파생값 `avg_win/avg_loss_abs` |
| Profit Factor | `E:\1_Data\2_Logs\paper_pnl_summary_last.json` / `gross_pf` | 확정 | 가상매매 기준 |
| Sortino | `E:\1_Data\2_Logs\verification_runtime_evidence_latest.json` / `phases.STRATEGY.verify_walkforward_regime_robustness.overall_sortino` | 임시 | 표준 stats 키 미고정 |

### 5.2 운영 KPI

| KPI | 계산 주체(파일/키) | 상태 | 비고 |
|---|---|---|---|
| 기준일(D) | `E:\vibe\buffett\runs\SSOT_TODAY_FINAL.json` / `D` | 확정 | 운영 기준일 |
| run_id | `E:\vibe\buffett\runs\observer_state_last.json` / `run_id` | 임시 | 파일 신선도 점검 필요 |
| 현재 stage | `E:\1_Data\2_Logs\trading_stage_validation_latest.json` / `paper.stage`, `live.stage` | 확정 | 단계별 상태 |
| next_step | `E:\1_Data\2_Logs\trading_stage_validation_latest.json` / `overall.next_step` | 확정 | 전환 액션 |
| risk_mode | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `gate_summary.kill_switch_mode` | 확정 | `REDUCE/NORMAL` 등 |
| kill_switch | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `gate_summary.kill_switch_triggered` | 확정 | Bool |
| signal 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `signals.rows_today` | 확정 | 당일 신호량 |
| orders 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `orders.rows` | 확정 | 주문 건수 |
| fills 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `fills.rows_as_of` | 확정 | 기준일 체결 건수 |
| positions 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `positions.rows` | 확정 | 보유 포지션 수 |
| 파이프라인 경고수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `health.alerts_count` | 확정 | 경고 요약 |

### 5.3 전략 KPI

| KPI | 계산 주체(파일/키) | 상태 | 비고 |
|---|---|---|---|
| raw signal 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `signals.rows_today` | 확정 | 전략 출력량 |
| filter 탈락 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `signals.rows_today - signals.pending_candidates_after_caps` | 임시 | 파생값 |
| candidate 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `signals.pending_candidates_after_caps` | 확정 | 후보 수 |
| entered 수 | `E:\vibe\buffett\runs\dashboard_state_latest.json` / `orders.buy_count` | 확정 | 진입 주문 건수 |
| 진입 품질 | `E:\1_Data\2_Logs\verification_runtime_evidence_latest.json` / `phases.STRATEGY.verify_overfitting.*` | 임시 | 표준화 필요 |
| 청산 품질 | `E:\1_Data\paper\trades.csv` / `exit_reason`, `pnl_pct` 집계 | 임시 | 전용 요약 파일 필요 |
| 레짐별 성과 | `E:\1_Data\2_Logs\backtest_validation_latest.json` / `gate_results[name=market_regime_response].details` | 확정 | 백테스트 기준 |
| 섹터별 성과 | `E:\1_Data\2_Logs\candidates_latest_data.with_sector_score.csv` + 체결/원장 결합 | 미구현 | 표준 섹터 성과 파일 필요 |
| PF | `E:\1_Data\2_Logs\paper_pnl_summary_last.json` / `gross_pf` | 확정 | 가상매매 기준 |
| Sortino | `E:\1_Data\2_Logs\verification_runtime_evidence_latest.json` / `...overall_sortino` | 임시 | 표준 stats 키 필요 |
| 샤프 | `E:\1_Data\2_Logs\backtest_validation_latest.json` / `artifacts.base_metrics.sharpe` | 확정 | 백테스트 기준 |
| MDD | `E:\1_Data\2_Logs\backtest_validation_latest.json` / `artifacts.base_metrics.max_drawdown` | 확정 | 백테스트 기준 |

## 6) 성과 품질 지표 고정 영역(확정)

고정 카드:
1. 승률
2. 평균 수익
3. 평균 손실
4. 손익비
5. Profit Factor
6. 샤프
7. Sortino
8. MDD
9. 연속 손실 횟수
10. 최근 구간 성과

표시 소스 우선순위:
1. 1순위: 표준 stats 산출물
2. 2순위: `verification_runtime_evidence_latest.json`
3. 3순위: `paper_pnl_summary_last.json`

## 7) NA 표시 원칙(확정)

1. 카드/패널은 숨기지 않는다.
2. 값 미존재 시 `NA`를 표시한다.
3. 보조 문구는 다음 중 하나로 고정한다.
   - `데이터 없음`
   - `집계 불가`
   - `기준 부족`
4. 그래프 데이터 미존재 시 빈 그래프 대신 안내 문구를 출력한다.
5. 표 데이터 미존재 시 0행 안내 메시지를 출력한다.

## 8) 코드 반영 갭(2026-03-13 기준)

1. `dashboard_stock_v2.py`는 일부 KPI를 화면에서 직접 파생 계산함(총합/비율 집계).
2. `dashboard.py`는 9탭이 1차 구조이며, SSOT 1차 메뉴(투자/운영/전략)와 다름.
3. Sortino/손익비는 표준 stats 산출물이 없어 임시 소스 의존.
4. `dashboard_state_latest.json`는 일부 한글 인코딩 깨짐이 존재(표시 품질 이슈).

## 9) 다음 고정 작업(우선순위)

1. `dashboard_state_schema_v3`에 `gross_exposure_pct`, `win_rate`, `profit_factor`, `sortino`, `avg_win`, `avg_loss_abs`를 추가한다.
2. 상단 1차 메뉴를 `투자/운영/전략`으로 고정하고 9개 탭을 하위로 내린다.
3. `run_id` 표준 소스를 `dashboard_state_latest.json.health.run_id`로 통합한다.
4. 전략용 섹터 성과 산출물(`strategy_sector_performance_latest.json`)을 신설한다.

## 10) 최종 정의

플랫폼의 최종 구조는 `orders -> fills -> ledger -> stats` 체인을 중심으로, 상단 1차 메뉴를 `투자/운영/전략`으로 고정하고, KPI 계산 주체를 파일/키 단위로 추적 가능하게 유지하는 구조다.
