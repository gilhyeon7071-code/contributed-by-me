

## 2026-08-12 접속 끊김 후 작업 현황 재파악 및 레짐 분석 시도

### 진행된 것
- E:_Data와 E:ibe 작업 현황 파악.
  - E:_Data는 Git 저장소, 브랜치 codex/alignment-quality-next, 마지막 Git 커밋 2026-04-06.
  - tracked 파일 135개가 수정된 채 커밋 안 된 상태.
  - untracked 파일 약 45,000개.
  - 마지막 tracked 파일 수정: report_backtest_v41_1.py 2026-08-11 14:51, optimize_params_v41_1.py 2026-08-11 08:32.
  - E:ibeuffett는 Git 저장소 아님. 2026-08-12 09:27 SSOT_D20260811_FINAL_20260812_20260812_092741 스냅샷 생성 완료, 09:28 config.yaml 업데이트.
- 최근 작업 로그 확인.
  - E:_Data\logs\hpo_v41_1_unbuffered_20260810_183659.log 기준 STABLE_BASELINE 구간별 PF 편차 확인.
  - E:_Data\logs\combo_sector_exit_test_20260811_174317.log 결과 확인: sector_rs_pos + wider_exit가 overall 0.893, oos 1.059로 가장 균형 잡힌 조합.
  - E:_Data\logs
egime_exit_test_20260811_160110.log 결과 확인: 레짐별 필터 실험, stable gate 미통과(oos_pf_low).
- 레짐 분류 로직 및 산출물 현황 파악.
  - report_backtest_v41_1.py::_assign_report_research_regime() 재현.
  - 조건: STRESS/BULL/BEAR/SIDEWAYS/TRANSITION, market_regime_research 컬럼으로만 사용.
- 타겟 구간 레짐 라벨 추출.
  - 스크립트: E:_Data\_tmp_regime_audit.py.
  - 산출물: E:_Data_Logs
egime_distribution_target_windows_latest.json, E:_Data_Logs
egime_daily_market_index_latest.csv.
  - equal-weighted 평균 종가 기준으로 2020-08~2021-08은 +30.12% 상승, 2025-08~2026-08은 +21.99% 상승. 2024-08~2025-08은 데이터 품질 문제로 -36%로 왜곡됨.
- sector_rs_pos + wider_exit 연구 모드 재실행 시도.
  - sector_rs_pos를 signal_rs > 0으로, wider_exit를 TP [15%,30%,60%], trailing activation 15%, trail -15%로 추정.
  - 스크립트: E:_Data\_tmp_run_research_sector_rs_wider_exit.py.
  - 산출물: E:_Data_Logs
esearch_sector_rs_wider_exit
eport_backtest_summary_v41_1.json 등.
  - 결과: 전체 n=775, overall PF 약 0.73, OOS PF 1.136. 어제 로그(n=599, overall=0.893)와 차이 큼.

### 남은 것 / 미결정
- sector_rs_pos의 정확한 정의를 모름. signal_rs > 0 추정은 틀린 것으로 보임.
- wider_exit의 정확한 파라미터를 모름.
- 어제 combo_sector_exit_test 결과를 재현하지 못함.
- KOSPI 3000->5000->6000 구간에서 v41.1이 실제로 얼마나 먹었는지 trade-level 분석은 아직 미완료.
- equal-weighted 평균 종가의 데이터 품질 문제(0값, 토/일 데이터)로 인해 시장 수익률 지표는 신뢰할 수 없음.

### 판정
- 작업 현황 재파악: 완료.
- 레짐 분포 추출: 완료(추정 기반, 데이터 품질 제한 있음).
- sector_rs_pos + wider_exit 재현: 실패. 정확한 실험 정의 부족.
- 다음 단계: 정확한 실험 복원 또는 KOSPI 상승 구간 trade-level 분석 중 선택 필요.

### 추가: 상승장 손실 원인 분석
- 스크립트: E:_Data\_tmp_analyze_bull_loss_reasons.py
- 산출물: E:_Data_Logs41_1_bull_loss_reasons_latest.json
- 상승장(2020-03~2021-08) 진입 종목 431개 분석 결과:
  - FUNDAMENTAL_CRITICAL: 265개, 평균 -0.14%, PF 0.88 -> 빈번한 작은 손실
  - STOP: 103개, 평균 -5.38%, PF 0.00 -> 드물지만 큰 손실
  - TIME: 22개, 평균 +6.49%, PF 29.54 -> 드문 수익
  - TP_L40: 6개, 평균 +22.38% -> 매우 드문 큰 수익
- 결론: 상승장에서도 stop loss와 fundamental/technical/market_risk 조기 처분 때문에 큰 추세를 못 먹고, 대신 손절은 자주 걸림.

### 추가: 상승장 수익 가설 검증
- 스크립트: E:_Data\_tmp_run_bull_hypothesis.py
- 산출물: E:_Data_Logsesearch_bull_hypothesisull_hypothesis_results.json
- 구간: 2020-03-27 ~ 2021-08-06 (TRAIN 상승장1)
- 시나리오별 TRAIN PF:
  - base: 0.592 (n=217)
  - wider_tp [20,40,80]: 0.669 (n=217)
  - wider_tp + hold 20일: 0.724 (n=160)
  - disable paper exit rules + hold 20일: 0.780 (n=160)
  - disable paper exit rules + hold 30일: 0.974 (n=111)
  - wider_tp + hold 30일: 0.457 (n=111)
- 발견: paper exit rules를 끄고 hold 30일로 늘리면 TRAIN 상승장에서 PF 0.97까지 개선.

### 추가: OOS 상승장 및 하락/횡보 구간 추가 검증
- 스크립트: E:_Data\_tmp_run_bull_bear_validation.py
- 산출물: E:_Data_Logsesearch_bull_bear_validationull_bear_validation_results.json
- 구간별 PF:
  - base: TRAIN 상승장1 0.593, TRAIN 하락/횡보 0.600, OOS 상승장2 0.592, OOS 상승장3 1.315
  - disable_exit_hold30: TRAIN 상승장1 0.974, TRAIN 하락/횡보 0.873, OOS 상승장2 0.666, OOS 상승장3 0.863
  - wider_tp_hold20: TRAIN 상승장1 0.725, TRAIN 하락/횡보 0.361, OOS 상승장2 0.475, OOS 상승장3 2.245
- 해석:
  - disable_exit_hold30은 TRAIN에서 좋지만 OOS 상승장3에서 base보다 낮음. overfitting 가능성.
  - wider_tp_hold20은 상승장에서 강하지만 하락/횡보에서 0.361로 방어 붕괴.
  - base는 모든 구간에서 0.6 수준으로 일관되게 부진.
- 결론: 무조건 exit rules 완화는 위험. 레짐 조걶부 exit policy(BULL/SIDEWAYS에서만 완화, BEAR/STRESS에서는 유지) 설계 필요.


## 2026-08-12 접속 끊김 후 추가 확인 (사이클 로그 + 실행 중인 스윕)

### 추가 확인 사항
- 사이클 HPO 로그 재확인: E:_Data\logs\hpo_v41_1_cycle_sector_20260810_172758.log
  - 7개 rolling window(2020-03-27 ~ 2026-08-06)로 28 iteration까지 기록되어 있으나, 마지막 summary 없이 중단된 상태(line 208, ITER_28 window 6/7까지만 존재).
  - STABLE_BASELINE 대비 대부분 iteration이 하락/횡보 구간(window 3,4)에서 PF가 낮음.
  - 상대적으로 n이 많고 안정적인 iteration: ITER_4, ITER_11, ITER_13, ITER_20, ITER_27.
  - ITER_20: n=21/160/89/110/97/97/94, PF=7.71/2.39/0.64/0.63/0.84/2.53/1.84.
  - ITER_27: n=29/177/12/61/71/66/93, PF=6.04/1.72/0.52/0.41/0.96/4.06/2.51.
  - 단, 각 iteration의 파라미터 조합은 로그에 미기재. 별도 결과 파일/DB에서 추출 필요.
- 실행 중인 스윕: E:_Data_Logsesearch_sector_rs_sweep\sector_rs_wider_exit_sweep.json 생성 중.
  - 18:54:16 시작, 19:02 현재까지 output 파일 미생성. 메모리 5GB 사용 중으로 compute_factors 단계로 추정.
  - 파라미터 그리드: rs_threshold [0.0,0.1,0.2,0.3,0.5,1.0] x tp_levels 3종 x trail_act 3종 x trail_pct 2종 = 108회 시뮬레이션.
  - 목적: 어제 combo_sector_exit_test_20260811_174317.log의 sector_rs_pos + wider_exit 결과(n=599, overall PF=0.893, OOS PF=1.059) 복원.

### 판정
- 사이클 로그 분석: 완료(파라미터 미상, 단순 window별 PF 기록만 확인).
- sector_rs_wider_exit 스윕: 진행 중. 결과 대기 필요.
- 운영 반영 가능한 결론: 아직 없음. stable gate 통과 조합 미확인.

### 다음 단계
1. sector_rs_wider_exit 스윕 완료 후 결과 확인.
2. 결과가 어제 combo 로그와 다를 경우, sector_rs_pos/wider_exit의 정확한 정의 재확인.
3. 사이클 HPO iteration별 파라미터를 별도 산출물에서 추출하여, 어떤 조건이 시장 사이클 간 안정성을 높였는지 식별.
4. 두 축(섹터 모멘텀 + 사이클 조걶부 exit)을 교차 검증.


## 2026-08-12 추가 검증 결과 (combo_sector_exit 재현 + 사이클 HPO 파라미터 매칭)

### combo_sector_exit_test 재현
- 스크립트: E:_Data_Logs	est_combo_sector_exit.py
- 실행 시점: 2026-08-12 약 19:00
- 결과:
  - base: n=774, overall=0.712, train=0.564, val=0.759, oos=1.028
  - sector_rs_pos: n=600, overall=0.821, train=0.700, val=1.170, oos=0.942
  - wider_exit: n=774, overall=0.770, train=0.606, val=0.779, oos=1.142
  - sector_rs_pos + wider_exit: n=600, overall=0.887, train=0.763, val=1.216, oos=1.030
  - sector_rs_pos + wider_exit + bear_skip: n=554, overall=0.880, train=0.794, val=1.170, oos=0.955
- 정의 확인:
  - sector_rs_pos: (섹터 20일 수익률 - 시장 20일 수익률) > 0
  - wider_exit: tp_levels=(0.2, 0.5, 1.0), tp_ratios=(0.3, 0.4, 0.3), trail_pct=-0.15
- 어제 로그(combo_sector_exit_test_20260811_174317.log)와 거의 일치(n=599->600, overall=0.893->0.887, oos=1.059->1.030). 차이는 데이터 업데이트 때문.

### stable gate 상태
- [PARAM_GATE] source=research_unapproved_fallback stable_ok=False
- reason: not_promoted; stable_score_low(-99999999.6382<-20.0000); oos_pf_low(0.9257<1.0000)
- 판정: sector_rs_pos + wider_exit는 overall PF 0.89, OOS PF 1.03으로 개선되었으나, stable_params_gate 기준(rolling window 기준 oos_pf >= 1.0 등)은 아직 미충족. 운영 반영 불가.

### 사이클 HPO 파라미터 매칭
- hpo_v41_1_cycle_sector_20260810_172758.log의 iteration별 n_total과 E:_Data
_Risk_Controlled\search_report_v41_1.csv의 iter n_total이 정확히/거의 일치.
  - ITER_20: n=668 (양쪽 동일)
  - ITER_27: n=509 vs 505
- 이를 통해 파라미터 추출:
  - ITER_20: defense_bear_disable_entry=0, rs_lim=0.05, v_accel_lim=4.3, stretch_max=1.15, value_min=55B, atr_max=0.11, gap_limit=0.08, w_rs=0.31, w_rs_slope=0.51, w_v_accel=0.18
  - ITER_27: defense_bear_disable_entry=1, rs_lim=-0.04, v_accel_lim=5.2, stretch_max=1.25, value_min=55B, atr_max=0.045, gap_limit=0.20, w_rs=0.56, w_rs_slope=0.22, w_v_accel=0.22
- 인사이트:
  - ITER_27은 BEAR 진입 차단을 사용했으나 2020 V자 반등 구간(window 1)에서 PF 6.04를 기록. 레짐 분류가 V자 반등 초기를 BEAR로 잡지 않았을 가능성.
  - ITER_27의 gap_limit=0.20은 변동성 큰 구간에서도 진입 기회를 더 많이 허용.
  - ITER_20은 w_rs_slope=0.51로 모멘텀 기울기에, ITER_27은 w_rs=0.56으로 상대강도에 더 민감.

### 최종 판정
- 어제 발견된 sector_rs_pos + wider_exit 조합은 재현됨.
- 단, stable gate 미통과로 운영 반영은 불가.
- 사이클 HPO의 ITER_20/ITER_27 파라미터를 report_backtest_v41_1.py 연구 모드로 추가 검증할 가치 있음.
- 다음 단계: ITER_20/27 파라미터를 combo 조합과 결합하여 stable gate 통과 가능성 확인, 또는 stable gate 기준 자체를 명확히 재검토.


## 2026-08-12 stable_params_gate 기준 검토 (억지 맞춤 없이 검증 중심)

### gate 설정값
- 출처: E:\1_Data\paper\paper_engine_config.json 의 stable_params_quality_gate
- require_promoted: true
- min_oos_trades: 20
- min_oos_pf: 1.0
- min_stable_score: -20
- min_mean_pf: 1.0

### gate 평가 로직
- E:\1_Data\utils\stable_params_gate.py
- stable_params_v41_1.json의 windows 리스트를 순회.
- split == OOS인 window의 pf를 n_trades로 가중평균 -> oos_pf_weighted
- 모든 window의 pf를 n_trades로 가중평균 -> mean_pf_weighted
- promoted=false이면 require_promoted 실패.
- best_score < min_stable_score면 stable_score_low 실패.

### 현재 stable_params_v41_1.json의 gate 평가
- windows:
  - 2020-03-27~2020-07-03 IS n=17 pf=1.468
  - 2020-07-04~2021-07-03 IS n=121 pf=1.706
  - 2021-07-04~2022-07-03 IS n=23 pf=0.362  <- worst fold
  - 2022-07-04~2023-07-03 IS n=33 pf=0.663
  - 2023-07-04~2024-07-03 VAL n=54 pf=0.820
  - 2024-07-04~2025-07-03 OOS n=60 pf=0.536  <- OOS 실패 원인
  - 2025-07-04~2026-07-03 OOS n=110 pf=1.138
- oos_pf_weighted = (60*0.536 + 110*1.138) / 170 = 0.9257 < 1.0 -> oos_pf_low
- mean_pf_weighted = 1.108 >= 1.0 -> pass
- best_score = -99999999.638 < -20 -> stable_score_low
- promoted = false -> not_promoted
- 결론: 현재 stable 자첼도 gate 3가지 실패. v41.1은 현재 gate 기준상 운영 승인 상태가 아님.

### report_backtest_v41_1.py 기준 rolling window gate 평가
- 동일 windows 사용. 단, simulate_trades는 report_backtest 기준(entry=same_close, paper_engine_config sell_rules 등).
- 스크립트: /tmp/eval_combo_gate.py
- base:
  - mean_pf_weighted=0.6926, oos_pf_weighted=0.9380
  - mean_pf_pass=False, oos_pf_pass=False
- wider_exit:
  - mean_pf_weighted=0.7453, oos_pf_weighted=1.0413
  - mean_pf_pass=False, oos_pf_pass=True
- sector_rs_pos + wider_exit:
  - 2020-03-27~2020-07-03 IS n=21 pf=1.6327
  - 2020-07-04~2021-07-03 IS n=145 pf=0.8636
  - 2021-07-04~2022-07-03 IS n=80 pf=1.0449
  - 2022-07-04~2023-07-03 IS n=91 pf=0.2091  <- mean_pf 깨는 주범
  - 2023-07-04~2024-07-03 VAL n=85 pf=0.9364
  - 2024-07-04~2025-07-03 OOS n=85 pf=1.1395
  - 2025-07-04~2026-07-03 OOS n=80 pf=0.8517
  - mean_pf_weighted=0.8632, oos_pf_weighted=1.0000
  - mean_pf_pass=False, oos_pf_pass=False (oos_pf가 정확히 1.0이지만 < 1.0 조건)

### gate 기준에 대한 객관적 평가
- min_mean_pf=1.0, min_oos_pf=1.0은 운영 자금을 걸기 위한 최소 본전 기준. 합리적.
- require_promoted=true는 HPO 낸부 promotion 로직을 존중. 합리적.
- min_stable_score=-20은 worst_fold PF hurdle(0.75) 위반 시 -1e8 페널티와 연결. 극단적 수치이지만 의미는 명확.
- 현재 v41.1(base)은 report_backtest 기준 mean_pf 0.69, oos_pf 0.94로 gate 기준에 크게 미달. gate가 정당하게 차단 중.
- sector_rs_pos + wider_exit는 overall/yearly로 개선되었으나, rolling window 가중평균 기준으로는 여전히 mean_pf 0.86, oos_pf 1.00으로 1.0 미만.
- 2022-07-04~2023-07-03 구간 PF 0.21이 mean_pf 1.0 달성의 가장 큰 장애물.

### 최종 판정
- gate 기준 자체는 합리적이며, 현재 v41.1과 후보 조합 모두 기준 미달.
- 운영 반영을 위해서는 mean_pf >= 1.0과 oos_pf > 1.0을 동시에 달성해야 함.
- 2022-07~2023-07 구간 방어 개선이 다음 관건.

## 2026-08-13 RootA 시스템 헬스 FAIL 후보 freshness 및 hard block 잔존 복구
- Scope: E:\1_Data\2_Logs\candidates_latest_data.csv, E:\1_Data\2_Logs\candidates_latest_data.with_news_score.csv, E:\1_Data\2_Logs\candidates_latest_data.with_final_score.csv, E:\1_Data\2_Logs\paper_intraday_hard_blocked.flag
- Cause: 2026-08-11 hard block flag가 남아 있었고, 당시 원인은 후보 기준일 20260807 vs expected 20260810 lag 3이었다. 2026-08-13 현재 가격/후보 원본은 20260812로 복구됐으나 stale flag가 대시보드 FAIL을 계속 유발했다.
- Change:
  - 공식 freshness_check_v1.py 재검증으로 expected_date=20260812, cand/krx_clean/prices 모두 PASS(lag=0) 확인.
  - stale hard block flag를 삭제하지 않고 E:\1_Data\2_Logs\cleared_hard_blocks\paper_intraday_hard_blocked.flag.cleared_20260813_101954 로 이동.
  - 후보 후처리 산출물 with_news_score/with_final_score 기준일이 20260812로 회복된 것 확인.
- Validation: E:\1_Data\2_Logs\freshness_source_20260813_101921.json verdict=PASS. E:\1_Data\2_Logs\paper_intraday_hard_blocked.flag 없음. candidates_latest_data.csv/with_news_score/with_final_score 모두 20260812 기준 확인.
- Remaining: 당일 orders_20260813_exec.xlsx는 RootA/RootB 모두 없음. 이는 execution stale WARN의 원인으로 남아 있으며, 임의 생성하지 않음.


## 2026-08-13 검증 방법론 문제점 및 순차 검증 계획

### 현재 검증이 결론을 못 내리는 이유
1. 사후 분석(post-hoc)으로 가설을 만들고 다시 검증하지 않음.
   - 예: mkt_ret20 > 0 임계값을 데이터를 본 뒤에 선택.
2. 너무 많은 변수를 동시에 다룸.
   - sector_rs, wider_exit, regime, market timing, stop loss, fundamental exit 등을 섞어서 테스트.
3. trade-level 메커니즘 분석 부족.
   - exit_reason 통계만 냈지, 왜 FUNDAMENTAL_CRITICAL/STOP이 발생하는지 causal 분석이 없었음.
4. gate 통과를 검증 목표로 삼음.
   - numeric 기준 맞추기에 급급하여 전략 타당성 검증이 뒷전이 됨.
5. HPO(rolling window)와 report_backtest(yearly split) 결과를 구분하지 않고 혼용.

### 순차 검증 계획 (앞으로 진행)
각 가설은 독립적으로 검증. 하나의 결론이 나오면 다음으로 넘어감. 사후 임계값 조정 금지.

#### 가설1: 상승장 FUNDAMENTAL_CRITICAL 원인
- 가설: 2020-07-04~2021-07-03 상승장에서 FUNDAMENTAL_CRITICAL로 처분된 종목들은 특정 fundamental trigger에 집중되어 있다.
- 방법: FUNDAMENTAL_CRITICAL trades와 수익 trades의 fundamental 지표 및 trigger 분포 비교.
- 성공 기준: 특정 trigger가 FUNDAMENTAL_CRITICAL 종목의 50% 이상을 설명하고, 해당 trigger를 완화/제거했을 때 상승장 PF가 0.86 -> 1.0 이상 개선.

#### 가설2: 상승장 STOP 원인
- 가설: 상승장에서 STOP에 걸린 종목들은 진입 직전 변동성 확장 또는 섹터/시장 약세가 있었다.
- 방법: STOP trades vs 수익 trades의 진입일 시장 변동성(mkt_vol20), 섹터 수익률, 종목 ATR 분포 비교.
- 성공 기준: STOP 그룹의 mkt_vol20/ATR 중앙값이 수익 그룹보다 유의미하게 높음(중앙값 차이 > 30% 또는 Mann-Whitney p < 0.05).

#### 가설3: wider_exit 효과
- 가설: wider_exit(TP [20,50,100])이 상승장에서 큰 수익 trades를 살리고, ret 분포의 우측 꼬리를 증가시킨다.
- 방법: base vs wider_exit의 상승장 trade별 ret 분포 비교. TP_L40 발생 빈도와 평균 수익률 비교.
- 성공 기준: wider_exit의 TP_L40 빈도가 base 대비 2배 이상 증가하거나, 상승장 평균 ret이 base 대비 +3%p 이상 개선.

#### 가설4: sector_rs_pos 효과
- 가설: sector_rs > 0인 종목이 상승장/하락장 모두에서 상대적으로 강하다.
- 방법: sector_rs quantile(하위 30%, 중위 40%, 상위 30%)별 성과 분석. 상승장과 2022-07~2023-07 구간에서 각각 비교.
- 성공 기준: sector_rs 상위 30%가 하위 30%보다 PF가 1.2배 이상 높음(양 구간 모두).

#### 가설5: 시장 방어 필터 holdout 검증
- 가설: 시장 20일 수익률이 양수일 때만 진입하면 하락/횡보 구간 손실이 줄어든다.
- 방법: 임계값 0.0을 미리 고정. holdout 구간(2025-08-07~2026-08-06)과 2022-07~2023-07 구간에 대해 report_backtest_v41_1.py 평가.
- 성공 기준: 2022-07~2023-07 PF가 0.21 -> 0.50 이상 개선. holdout OOS PF >= 1.0.
- 주의: 0.0은 이미 탐색에서 발견된 값이므로, 이 가설은 "재확인"이며 holdout 구간에서만 신뢰할 수 있음.

### 주의사항
- 각 가설 검증 전에 기대 결과를 명시.
- 사후 임계값 조정 금지.
- gate 통과는 검증 결과의 부산물일 뿐, 목표가 아님.

## 2026-08-13 rule_e HPO 통합

### 선택
- 사전 정의 규칙 테스트 결과 rule_e 선택: sector_rs>0.05 & mkt_ret20>0 & mkt_ret60>0 & regime_not_bear.
- n=133, mean PF=1.2248, OOS PF=1.5497. n과 성능 균형이 가장 나음.

### HPO 통합 내용
- 수정 파일: E:/1_Data/optimize_params_v41_1.py.
- compute_factors()에 추가:
  - mkt_ret60: 시장 60일 수익률.
  - sector_ret20: 섹터별 20일 수익률(equal-weight 평균 종가).
  - sector_rs: sector_ret20 - mkt_ret20 (섹터 상대강도).
- BOUNDS에 추가:
  - mkt_ret20_min: (-0.05, 0.10, 0.01)
  - mkt_ret60_min: (-0.10, 0.10, 0.01)
  - sector_rs_min: (-0.10, 0.20, 0.01)
- _select_day_candidates_operational()에 market/sector defense 필터 추가.
- base 파라미터 초기값: mkt_ret20_min=0.0, mkt_ret60_min=0.0, sector_rs_min=0.05.
- report CSV 컬럼에 3개 파라미터 추가.

### 실행
- HPO 실행: python optimize_params_v41_1.py (background, task_id: bash-vx24z4ly).
- 출력: E:/1_Data/12_Risk_Controlled/best_params_v41_1.json, stable_params_v41_1.json, search_report_v41_1.csv.

### 주의
- sector_rs는 symbol_panel의 sector_code 기반 equal-weight 섹터 지수. 어제 combo 로그의 sector_rs_pos 정의와 동일하게 맞춤.
- HPO promotion은 stable_quality_gate를 통과해야 stable_params_v41_1.json 덮어쓰기 발생.

## 2026-08-14 RootA 전략 정리 및 향후 방향

### 지금까지 검증 요약
- v41.1은 단기 돌파 전략(RS + volume acceleration + stretch)임.
- HPO 메트릭을 trade-level PF로 변경하여 report_backtest_v41_1.py와 일치시킴.
- rule_e 필터(sector_rs>0.05 & mkt_ret20>0 & mkt_ret60>0 & regime_not_bear)를 HPO에 통합.
- wider_exit(TP [20,50,100], trail -15%, activation 20%) 적용.
- stop_loss를 HPO 탐색 공간에 추가 (-0.12 ~ -0.04).
- FUNDAMENTAL_CRITICAL 임계값 완화 테스트 후 원복.
### 검증 결과
- 현재 stable은 rolling window 기준 OOS PF 1.56, mean PF 1.31로 stable_quality_gate 통과.
- 하지만 report_backtest yearly 기준 2020-2024는 여전히 손실, 2025-2026만 수익.
- 2020-08~2021-08 상승장 손실 분석:
  - STOP 19건(34%): 전부 -5.6% 손절. signal_atr_pct가 TIME 대비 44% 높음(변동성 큰 종목).
  - FUNDAMENTAL_CRITICAL 27건(48%): signal_rs가 낮음(1.73).
  - 2021년 1-2월, 5월, 7월에 손실 집중.

### 전략적 교훈
- v41.1 단기 돌파 전략은 2020-03~2020-08 급등 초기에는 먹히지만, 2020-08~2021-08 상승 중후반에서는 단기 조정에 계속 STOP에 걸림.
- 즉, 단기 전략으로 중기 상승 추세를 포착하는 데 한계가 있음.
- stop_loss 완화, wider_exit, fundamental 완화 등 파라미터 튜닝으로는 2020-2021 상승장 손실을 해결하지 못함.
- 이 방향은 과거 데이터 fitting에 가까우며 overfit 위험이 높음.

### 운영 검증 관점 재정립
- 현재 stable은 holdout 구간(2025-08~2026-08)에서 수익. 운영 적용의 근거는 있음.
- 2020-2021 상승장을 고치려는 것은 과거 회귀이지 운영 검증이 아님.
- v41.1은 방어/하락장/변동성 구간 역할에 특화된 단기 전략으로 정의하는 것이 타당.

### 향후 방향 제안
| 전략 | 시그널 | 보유기간 | 역할 |
|------|--------|----------|------|
| v41.1 (단기) | RS + volume + stretch | 2~13일 | 변동성/급등 초기, 방어 |
| v41.2 (중기) | 20~60일 추세 + 섹터 모멘텀 | 1~3개월 | 상승장 추세 추종 |
| 장기 코어 | KOSPI/KOSDAQ 추세 | 3개월~ | 벤치마크 대응 |

- v41.1 현재 stable 운영 적용 검토.
- v41.2 중기 추세 전략 연구 시작.
- 두 전략 간 자금 배분 설계(예: v41.1 40%, v41.2 40%, 현금/ETF 20%).

### 수정된 파일
- E:/1_Data/optimize_params_v41_1.py: trade-level PF 계산, rule_e 필터, stop_loss 탐색 추가.
- E:/1_Data/report_backtest_v41_1.py: sector_rs 필터 추가.
- E:/1_Data/paper/paper_engine_config.json: wider_exit 적용, fundamental_risk 원복. 백업: paper_engine_config.json.bak.20260813.
- E:/1_Data/12_Risk_Controlled/stable_params_v41_1.json: gate 통과 상태.


## 2026-08-14 위 결론 블록 내용검증 + rule_e 생산 배선

### 위 "2026-08-14 RootA 전략 정리" 블록 검증 결과 (코드/산출물 대조)
- 수치 오류 2건.
  - "OOS PF 1.56" -> 실제 stable_params_v41_1.json meta.stable_quality_gate.oos_pf_weighted = 1.4640. 1.56은 08-13 rule_e 사전테스트값(1.5497)이 섞인 것.
  - "mean PF 1.31" -> 실제 mean_pf_weighted = 1.1224. 1.31은 selection_metrics.avg_pf(1.3312)/recent_weighted(1.3155)이며 gate 지표가 아님.
  - holdout PF 1.40은 맞음(2025-08-13~2026-08-12 OOS n=39 pf=1.4026).
- holdout이 holdout이 아님. optimize_params_v41_1.py::_fold_selection_metrics()는 IS/VAL/OOS 구분 없이 전 fold를 eligible에 넣고, HPO_RECENT_WEIGHT_HALF_LIFE=2.0으로 최신 fold에 최대 가중치를 준다. 즉 HPO가 2025-08~2026-08을 직접 최적화한 뒤 gate가 같은 창을 재채점. 순환 논증이며 현재 out-of-sample 증거는 없음.
- 표본: 7 fold 합계 229 trades / 6.4년. 2021-08~2022-08 fold는 n=4로 통째 제외(excluded_low_trade_folds=1). worst_fold 0.7981 vs hurdle 0.75, 여유 0.048.
- 논리 모순: "v41.1은 방어/하락장 특화"라 규정했으나 rule_e는 mkt_ret20>0 & mkt_ret60>0 & not bear로 구조적으로 상승장 전용 필터다. 방어 전략으로 규정 불가.
- wider_exit는 실운영에서 거의 무효. 최상위 sell_rules는 TP[20,50,100]/trail -15%가 맞지만 paper_engine.py:333의 _deep_merge_dict(cfg, regime_override)가 이를 덮어쓴다. RALLY TP[7,15,30]/max_hold 5, NORMAL/BEAR TP[10,20,40]/trail -5%/max_hold 15. 4개 레짐 중 CRASH만 TP override가 없다.
- 표의 "보유기간 2~13일"은 부정확. 2는 config min_hold_days, 13은 stable hold(백테스트 전용). 실운영 max_hold는 top-level 8 / NORMAL·BEAR 15 / RALLY 5로 13은 어디에도 없음.

### rule_e 생산 배선 (사용자 지시로 실행)
- 문제: generate_candidates_v41_1.py에 mkt_ret20_min/mkt_ret60_min/sector_rs_min이 0건이었고 PARAM_EXPORT_KEYS에도 없었다. stable에 값은 있으나 생산 코드가 읽지 않아 백테스트가 검증한 전략과 주문을 내는 전략이 달랐다.
- 수정 파일: E:/1_Data/generate_candidates_v41_1.py 단일 파일.
  - DEFAULT_PARAMS에 3키 추가. -1.0 = disabled sentinel.
  - PARAM_EXPORT_KEYS에 3키 추가.
  - _normalize_params()에 [-1.0, 1.0] clamp 추가. 0.0이 유효 임계값으로 살아남도록 `or` 패턴 미사용.
  - _compute_factors()에 mkt_ret20 / mkt_ret60 / sector_ret20 / sector_rs 계산 추가. 정의는 optimize_params_v41_1.py::compute_factors()를 그대로 따름(mkt_ret20 = 시장별 20일 수익률 + 전체 폴백, mkt_ret60 = 전체 60일, sector_rs = sector_ret20 - mkt_ret20).
  - _rule_e_threshold() 헬퍼 신규. -1.0/None/비유한값이면 None(비활성).
  - _select_candidates()에 3개 조건 추가. report_backtest_v41_1.py:778-783과 동일하게 strict >, 따라서 NaN은 제외.
  - _diag_counts()에 mkt_ret20_pass/mkt_ret60_pass/sector_rs_pass 추가. 0후보 날의 원인 귀속 가능.
  - [PARAM] rule_e 로그 라인 추가.
  - _build_sector_code_map(restrict_allowed=True) 인자 추가. 섹터 지수는 옵티마이저와 맞추기 위해 ALLOWED_SECTOR_CODES 미적용 맵으로 산출.
  - _apply_sector_prefilter_union()의 soft gate에도 rule_e 적용.
  - 같은 함수에서 pool의 기존 sector_code를 drop 후 merge하도록 수정(sector_code_x/_y 분리로 KeyError 발생했었음).
- 의도적 설계: rule_e는 _relax_ladder()가 건드리지 않는다. 래더는 명시된 키만 재작성하므로 L0-L9 전부에서 보존된다. 종목 임계값이 아니라 "오늘 이 시장에서 매매하지 말라"는 게이트이므로 완화 대상이 아니다.

### 검증
- 스크립트: scratchpad/rule_e_smoke.py, mkt_index_check.py, ladder_impact.py.
- 파라미터 왕복: stable의 0.0/0.0/0.05가 _normalize_params 통과 후 그대로 유지됨. 0.0이 삼켜지지 않고 -1.0은 비활성으로 동작.
- 래더 10개 레벨 전부에서 rule_e 3키 불변 확인.
- 래더 미적용 회귀: rule_e를 -1.0으로 끄면 3게이트 모두 2578/2578 통과(no-op)하고 산출 후보가 변경 전과 동일한 10종목으로 일치. 기존 동작 보존 확인.
- 리서치 모드 전체 파이프라인 실행 3회(운영 산출물 미변경). rule_e ON에서 0후보, OFF에서 10후보.

### 오늘 0후보의 원인 — 데이터 아티팩트 아님
- 2026-08-13 mkt_ret60 = -0.1584로 mkt_ret60_pass = 0/2578.
- 고정 유니버스 검증(양 끝점 공통 2502종목): 60일 수익률 중앙값 -16.6%, 상승 종목 비율 16.1%. 광범위한 실제 하락이며 0값/분할 아티팩트가 아니다.
- 20일 중앙값은 +7.7%. 60일 하락추세 안의 20일 반등 국면이고 rule_e가 이 구간 진입을 차단한 것. 설계대로 작동.

### 운영 영향 (측정치)
- _select_candidates 단독 기준, 최근 214 세션: 후보 발생일 29.0%(62일) -> 5.6%(12일). 총 후보 67 -> 12.
  - 주의: 이 측정은 섹터 유니온 폴백을 제외한 수치다. 폴백 포함 실제 활성일 상한은 ret20>0 & ret60>0인 날 51.6%.
- 차단된 50일 중 sector_rs가 binding인 날 30일, mkt_ret20이 binding인 날 20일.
- 백테스트는 같은 창에서 39 trades를 냈는데 생산은 12 후보 수준. 배선을 맞춘 뒤에도 생산과 백테스트는 여전히 3배 차이가 난다. 원인 미규명.

### 이번에 드러난 별개 결함 (미수정, 보고만)
1. keep_cols(generate_candidates_v41_1.py:2184)에 candidate_origin/natural_pass가 없어 CSV 내보내기에서 유실된다. 그 결과 현재 2_Logs/candidates_latest_data.with_final_score.csv의 candidate_origin은 23행 전부 NA다.
   - paper_engine/entry.py:5485-5505의 관찰전용 마스크는 origin == "SECTOR_PREFILTER_UNION"을 요구하므로 한 번도 발동하지 않는다. 즉 섹터 유니온 폴백 후보가 실제 진입 풀에 들어간다.
   - 이번에 폴백에도 rule_e를 적용해 rule_e 차단일에는 폴백이 후보를 내지 않게 했으므로 rule_e 관점의 구멍은 막혔다. 그러나 마스크 자체가 죽어 있는 문제는 그대로 남아 있다.
2. 옵티마이저와 report_backtest의 mkt_ret20 정의가 서로 다르다. 옵티마이저는 시장별(KOSPI/KOSDAQ) 20일 수익률 + 전체 폴백, report_backtest는 전체 시장 지수. 생산은 stable을 만든 쪽인 옵티마이저에 맞췄으므로 이제 report_backtest만 홀로 다르다.
3. 후보 0건이면 [FIX17] 경로가 candidates_latest_data.csv를 덮어쓰지 않고 전일 파일을 유지한다. rule_e로 0후보 날이 크게 늘어나므로 stale 후보가 상시화된다. freshness_check_v1.py의 lag 검사가 안전망이지만, 결과적으로 대시보드가 상당수의 날을 hard block/FAIL로 표시하게 된다.

### 판정
- rule_e 생산 배선: 완료, 검증됨.
- 단, 이것으로 전략이 검증된 것은 아니다. 위 "holdout이 holdout이 아님" 항목이 유효한 한 현재 stable의 1.40은 out-of-sample 증거가 아니다.
- 다음 단계 후보: (a) _fold_selection_metrics()에서 마지막 fold를 eligible에서 제외해 진짜 holdout 1개 확보 후 HPO 재실행, (b) 위 결함 1번(candidate_origin 유실) 수정, (c) 생산 12 vs 백테스트 39 괴리 원인 규명.


## 2026-08-15 HPO holdout 오염 제거 (A) + base_score 누수 발견

### 배경
- 08-14 기록의 다음 단계 (a)를 수행. 선행 작업으로 사용자가 optimize_params_v41_1.py에 `eligible[:-1]`(마지막 fold 1개 제외)와 generate_candidates_v41_1.py keep_cols에 candidate_origin/natural_pass/observe_only/observe_only_reason 4개 컬럼 복원을 적용한 상태였다.

### 선행 변경 검증 결과 (사용자 적용분)
- keep_cols 복원: 반영 확인. generate_candidates_v41_1.py:2196. 직후 `for c in keep_cols: if c not in top.columns: top[c]=np.nan` 가드가 있어 KeyError 없음. 정상 후보는 NaN, 폴백 행만 SECTOR_PREFILTER_UNION.
- candidate_origin은 관찰전용 마스크 외에 3곳에서 더 쓰인다. 전부 진입을 "허용"하는 방향이다.
  - paper_engine/entry.py:5913 union_conditional_added (execution_pool=False인 union 후보를 진입 풀로 복귀)
  - paper_engine/entry.py:5533 union_ok (execution_pool 요구 우회)
  - paper_engine/entry.py:5606 fresh_sector_allowed_fallback (origin 일치 시 후보 주입)
  - 실행 순서 확인: 관찰전용 마스크가 entry.py:5885로 5906/5913보다 먼저 돌아 union 행을 제거한다. 따라서 순 방향은 억제(fail-closed)가 맞다. entry.py:8536의 행 단위 검사가 2차 안전망.
  - 단, 5603의 fresh fallback은 5885보다 앞이라 "생성 후 즉시 제거"가 되며 로그만 새로 발생한다.
- `eligible[:-1]`는 불충분. VAL_END=2024-12-31 + 1년 롤링 창이라 OOS fold가 2개다.
  - 2024-08-13~2025-08-12 OOS n=30 pf=1.5438 (selection에 잔류)
  - 2025-08-13~2026-08-12 OOS n=39 pf=1.4026 (제외됨)
  - 게이트의 oos_pf_weighted는 n_trades 가중이므로 (1.5438*30 + 1.4026*39)/69 = 1.4639. 즉 게이트 OOS 지표의 43%가 HPO가 계속 보던 fold에서 나온다.
  - 또한 `[:-1]`는 "마지막 fold"가 아니라 "마지막 적격 fold"를 자른다. 마지막 fold가 MIN_TRADES_PER_WINDOW 미달인 조합에서는 그 앞 fold를 대신 잘라내며, 조합마다 selection 대상이 달라지고 로그에 남지 않는다.

### 이번 수정 (A)
- 파일: E:/1_Data/optimize_params_v41_1.py 단일 파일. 백업: backup/20260815_hpo_oos_holdout_exclusion/20260815_111420/
- `_fold_selection_metrics()`에서 위치 기반 슬라이스를 split 기반 분할로 교체.
  - `holdout_rows = [r for r in eligible if r.split.upper() == "OOS"]`
  - `eligible_for_selection = [r for r in eligible if r.split.upper() != "OOS"]`
- 반환 dict에 holdout_split / n_folds_holdout / holdout_windows 3키 추가. n_folds_holdout==0이면 out-of-sample 증거가 확보되지 않은 실행임을 stable json에서 바로 볼 수 있게 했다.
- `windows`에는 전 fold가 그대로 남으며 utils/stable_params_gate.py의 min_oos_pf 채점 경로는 불변.

### 검증 (런타임)
- py_compile PASS. 편집 구간 non-ASCII 없음.
- 실 live stable 7 fold 입력 기준 selection_metrics 변화:

| 지표 | 기존(저장값) | eligible[:-1] | 이번 수정 |
|---|---|---|---|
| n_folds(selection) | 6 | 5 | 4 |
| n_folds_total | 7 | 7 | 7 |
| n_folds_holdout | 없음 | 없음 | 2 |
| avg_pf | 1.3312 | 1.3169 | 1.2602 |
| recent_weighted | 1.3155 | 1.2718 | 1.1215 |
| worst_fold | 0.7981 (2020-08~2021-08, IS) | 동일 | 동일 |
| hypertime_reason | pass | pass | pass |

- 경계 케이스 7종 확인: 빈 입력 / 전부 OOS / OOS 없음 / 비OOS 2개(min_folds 미달) / worst가 OOS인 경우 / 마지막 fold 저거래 케이스. 전부 의도대로 동작하고 예외 없음. HPO_MIN_FOLDS=3이므로 selection 4 fold로 pass 유지.
- 소비처 회귀: selection_metrics를 읽는 코드는 optimize_params_v41_1.py 자신뿐. RootB(E:/vibe/buffett)에 소비처 없음(백업 .bak 1건 제외). 대시보드 영향 없음.

### 이번 수정이 만든 부작용 (측정치)
- OOS fold가 worst_fold 계산에서 빠지면서 OOS 꼬리위험 바닥이 HPO에서 게이트로 이관됐다.
  - 기존: OOS fold 1개라도 pf < 0.75면 HPO가 worst_fold_below_hurdle로 조합을 차단.
  - 현재: 게이트의 oos_pf_weighted(가중 평균)만 남으므로 다른 OOS fold가 나쁜 fold를 상쇄할 수 있다.
  - 실측: live 게이트 설정(min_oos_pf=1.0, min_mean_pf=1.0) 기준, 2025-08~2026-08 fold가 pf 0.6838까지 떨어져도 게이트는 통과한다. 기존 하드 바닥 0.75 대비 0.066 완화.
  - 게이트 기본값이 min_oos_pf=0.75였다면 구멍은 훨씬 컸을 것이다. 현재 config가 1.0이라 실피해가 작다.
- utils/stable_params_gate.py:44-45 주석이 명시하듯 게이트는 worst-fold 바닥을 "upstream"(HPO)에 위임하고 있다. 그 upstream이 이제 OOS를 보지 않으므로 위임 관계가 끊겼다.

### 결정적 발견 — base_score가 여전히 OOS를 목적함수로 쓴다 (미수정)
- optimize_params_v41_1.py:1478-1483

```
base_score = (cap(oos_pf)*1.6 + cap(val_pf)*0.6 + cap(is_pf)*0.2)
           + (oos_mean*40.0 + val_mean*10.0 + is_mean*2.0)
           - PF_STD_PENALTY*std_pf
hypertime_score = base_score + 0.01 * recent_weighted
```

- 즉 이번에 정리한 recent_weighted 항의 계수는 0.01인데, OOS는 pf 1.6 / mean_ret 40.0 계수로 base_score에 직접 들어간다. std_pf도 OOS 포함 전 fold로 계산된다.
- live stable 기준 항별 분해:

| split | avg_pf | avg_mean_ret | pf 항 | mean_ret 항 |
|---|---|---|---|---|
| IS | 0.9997 | -0.24564 | 0.1999 | -0.4913 |
| VAL | 1.0420 | 0.00143 | 0.6252 | 0.0143 |
| OOS | 1.4732 | 0.01369 | 2.3571 | 0.5478 |

- base_score 총 |항| 크기 중 OOS 몫 = 68.6%.
- 판정: A는 목적함수의 0.01짜리 항만 정화했고, 1.6/40.0짜리 항은 그대로다. 현 상태에서 HPO를 재실행해도 holdout은 여전히 holdout이 아니며 순환 논증이 유지된다. 08-14에 기록한 "현재 out-of-sample 증거는 없음"은 아직 유효하다.

### 검증 항목 판정
- 기능 검증 PASS. 실 fold + 경계 7종 런타임 확인.
- 정합성 검증 PASS. OOS 2개 모두 selection에서 분리, windows/게이트 경로 불변.
- 운영 반영 검증 NA. HPO 미재실행, stable_params_v41_1.json 미변경. 운영 파라미터는 기존 그대로다.
- 정책 검증 PASS. HPO_MIN_FOLDS=3 대비 4 fold 확보. 게이트 의미 미변경.
- FAIL-CLOSED 검증 FAIL. OOS worst-fold 하드 바닥 0.75가 소실되고 가중평균 0.6838로 완화됨. 게이트 측 보완 필요.
- 회귀 검증 PASS(코드 소비처 한정). generate_candidates 재실행 회귀는 미수행이므로 keep_cols 변경분은 별도 NA.

### 다음 단계
1. base_score의 OOS 항 처리 방침 결정. 목적함수 변경이라 사용자 승인 필요. 후보: OOS 계수를 0으로 두고 IS/VAL만으로 탐색, 또는 split 가중을 VAL 중심으로 재설계.
2. utils/stable_params_gate.py에 OOS 개별 fold 최저 pf 조건 추가(min_oos_worst_fold_pf). 게이트 의미 변경이므로 사용자 승인 필요. 위 FAIL-CLOSED 항목 해소용.
3. 1,2 완료 후 HPO 재실행. 그 전 재실행은 결과가 무의미하다.
4. generate_candidates_v41_1.py 1회 실행. 2_Logs/candidates_latest_data.csv에 candidate_origin 헤더 생성, 정상 후보 NaN, .with_final_score.csv 전파 확인. 현재 그 파일은 컬럼만 있고 23행 전부 NA다. run_A.py가 소비한다.
5. 생산 12 vs 백테스트 39 괴리 규명. 08-14 (c)항, 미착수.
6. report_backtest_v41_1.py의 mkt_ret20 정의 불일치. 08-14 결함 2번, 미수정.

## 2026-08-15 (2) HPO 목적함수 재구축 5단계 + stable 저장값 재현 불가 발견

### 배경
- 같은 날 (1) 블록에서 OOS fold를 selection에서 분리했으나, base_score가 OOS를 pf x1.6 / mean_ret x40.0으로 계속 쓰고 있어(총 항 크기의 68.6%) holdout이 여전히 holdout이 아니었다.
- 사용자 제안(base_score OOS 제거 / std_pf 제한 / 게이트 OOS worst-fold)을 검토한 결과 방향은 맞으나 계수 설계와 구현에 결함이 있어, 아래 순서로 재구성하기로 합의했다.

### 사용자 제안 검토 결과 (수정 전 지적사항)
- std_pf 제안은 실행 불가였다. `eligible_for_selection`은 `_fold_selection_metrics()` 지역변수(452행)이고 std_pf는 `eval_params()`(1441행)에서 계산된다. 다른 함수 스코프라 NameError.
- `ddof=0 -> ddof=1`은 별개 결정이다. fold 4개에서 x1.155 확대되어 PF_STD_PENALTY=0.35를 조용히 강화한다.
- 계수 VAL 4:1 제안은 당시 VAL이 fold 1개(27 trades)여서 목적함수 PF 항의 62%가 단일 창에 얹히는 구조였다.
- `is_mean = -0.24564`의 실체는 수익률이 아니라 n=4 fold의 sentinel(-1.0)이었다. 계수를 2.0->5.0으로 올리면 degenerate fold 하나의 기여가 -1.25로, 전체 base_score(+0.624)보다 커진다.
- 게이트 제안은 `n_trades` 필터가 없어 이관이 아니라 강화였다. HPO의 worst_fold는 n>=15 eligible만 봤으므로, 얇은 OOS fold의 pf=0.0 sentinel이 게이트를 영구 차단하게 된다.

### 적용 (백업: backup/20260815_hpo_objective_rebuild/20260815_113039/)

1. sentinel 집계 제외 - optimize_params_v41_1.py::eval_params()
   - `_scored(arr)` 헬퍼 신규. `n_trades >= MIN_TRADES_PER_WINDOW(15) and isfinite(pf)`.
   - mean_pf / std_pf / mean_ret 및 `_avg_pf` / `_avg_mean`이 전부 scored fold만 사용.
   - sentinel은 `results`에 그대로 남아 stable json `windows`에서 계속 보인다. 집계에서만 빠진다.
   - 죽은 지역변수 `pfs` / `means` 제거.
   - 효과: is_mean -0.24564 -> +0.01109. std_pf 0.6783 -> 0.5326.

2. split 경계 이동 - 12_Risk_Controlled/split_policy_v41_1.json
   - train_end 2023-12-31 -> 2022-12-31. val_end 무변경.
   - IS 4/VAL 1/OOS 2 -> IS 3/VAL 2/OOS 2. OOS holdout 집합은 동일하다.
   - VAL이 2 fold 60 trades가 되어 단일 창 의존 해소. 부수적으로 val_pf 1.0420(단일) -> 0.9439(2 fold 평균)로 낮아졌다. 단일 fold 값이 낙관적이었다는 뜻이다.

3. base_score에서 OOS 제거 - optimize_params_v41_1.py:1478
   - `(cap(oos_pf)*1.6 + cap(val_pf)*0.6 + cap(is_pf)*0.2) + (oos_mean*40 + val_mean*10 + is_mean*2)`
     -> `(cap(val_pf)*1.6 + cap(is_pf)*0.8) + (val_mean*35 + is_mean*17)`
   - OOS 가중을 버리지 않고 IS/VAL에 재분배해 합계를 보존(pf 2.4, mean_ret 52.0). 점수 스케일이 유지되어 게이트의 min_stable_score=-20.0과 승격 마진이 그대로 통한다.
   - VAL:IS = 2:1. VAL이 최신 증거지만 표본은 더 작다(60 vs 96 trades). 4:1은 작은 표본에 큰 발언권을 주게 되어 채택하지 않았다.
   - oos_pf / oos_mean / oos_n은 리포팅용으로 계속 계산되어 stable json에 남는다. 탐색만 하지 않는다.

4. 승격 비교 부호 반전 제거 - optimize_params_v41_1.py:1759
   - `best > stable * (1 + PROMOTION_MARGIN)` -> `best > stable + max(PROMOTION_MARGIN*abs(stable), PROMOTION_MARGIN_MIN_ABS)`
   - 곱셈형은 stable이 음수일 때 바가 낮아졌다. stable=-0.50에서 바가 -0.525가 되어 더 나쁜 후보가 승격됐다. 0 근처에서는 마진이 소멸했다.
   - PROMOTION_MARGIN_MIN_ABS = 0.02 신규.
   - 양수 구간 동작은 기존과 동일하다. stable=3.0157 -> 바 3.1665로 변화 없음.

5. 게이트 OOS worst-fold 하한 - utils/stable_params_gate.py
   - `min_oos_worst_fold_pf`(기본 0.75) / `min_oos_worst_fold_trades`(기본 15) 신규.
   - 기존 루프 안에서 수집하므로 `_row_metric_value` + malformed 처리 + n_trades 가드를 그대로 재사용한다.
   - `_gate_value` 사용. 반환 dict에 oos_worst_pf / oos_worst_window / oos_scored_folds, thresholds와 `_log_stable_gate` threshold_map에도 노출.
   - fail-closed: OOS 증거는 있는데 n>=15인 fold가 하나도 없으면 `oos_worst_fold_missing`로 차단.

### 검증
- py_compile PASS(두 파일). 편집 구간 non-ASCII 0.
- 게이트 5종 런타임:
  - G1 live stable: ok=True, oos_worst_pf=1.4026(2025-08-13~2026-08-12, n=39), scored_folds=2.
  - G2 예전 구멍 재현(holdout fold pf=0.6838): ok=False `oos_worst_pf_low(0.6838<0.7500)`. 이전에는 통과했다.
  - G3 얇은 OOS fold(n=4, pf=0.0): ok=True, oos_worst=1.5438. sentinel이 영구 차단하지 않음을 확인.
  - G4 OOS 전부 얇음: ok=False `oos_worst_fold_missing`. fail-closed 확인.
  - G5 min_oos_worst_fold_pf 미설정 config: 기본 0.75 적용, 현 worst 1.4026이므로 영향 없음.
- 승격 마진: stable 3.0157/0.6238은 기존과 동일한 바, -0.5는 -0.475(개선 요구), 0.0은 +0.02 바닥 적용.
- selection: n_folds=4~5, n_folds_holdout=2, HPO_MIN_FOLDS=3 대비 여유.

### 발견 - stable_params_v41_1.json의 저장 windows를 현재 코드로 재현할 수 없다 (미해결)
- 읽기 전용 E2E 실행(scratchpad/e2e_eval.py, 모든 writer를 예외로 스텁). main()의 `base` 구성을 그대로 복제해 live stable 파라미터로 eval_params 1회 실행.

| window | 저장 n | 재현 n | 저장 pf | 재현 pf |
|---|---|---|---|---|
| 2020-03-27~2020-08-12 | 15 | 92 | 2.3550 | 1.3580 |
| 2020-08-13~2021-08-12 | 81 | 353 | 0.7981 | 0.9079 |
| 2021-08-13~2022-08-12 | 4 | 349 | 0.0000 | 0.4714 |
| 2022-08-13~2023-08-12 | 33 | 337 | 0.8458 | 0.6545 |
| 2023-08-13~2024-08-12 | 27 | 373 | 1.0420 | 0.6048 |
| 2024-08-13~2025-08-12 | 30 | 372 | 1.5438 | 0.7910 |
| 2025-08-13~2026-08-12 | 39 | 478 | 1.4026 | 0.7806 |

- 재현 결과는 첫 fold를 빼면 전 fold PF < 1.0이다. avg_pf 0.7993, worst_fold 0.4714, hypertime_reason=worst_fold_below_hurdle.
- 게이트 재채점: ok=False. `stable_score_low(-1e8<-20)`, `oos_pf_low(0.7852<1.0)`, `mean_pf_low(0.7322<1.0)`. 즉 현재 live stable을 오늘 코드/데이터로 다시 재면 게이트를 통과하지 못한다.
- 이번 변경이 원인이 아님을 확정했다. n_trades를 만드는 경로 함수를 백업본과 해시 비교한 결과 simulate_window / compute_factors / load_data / build_windows / _trade_pf / sample_params 전부 IDENTICAL이다. 오늘 변경은 전부 시뮬레이션 이후 집계 단계다.
- 리드(미확정): optimize_params_v41_1.py:280은 import 시점에 paper/paper_engine_config.json에서 실행정책을 읽는다.
  - stable_params_v41_1.json mtime = 2026-08-14 14:59:16
  - paper_engine_config.json.bak.20260813 mtime = 2026-08-14 13:56, TP [10,20,40] / trail -10 / act 12
  - paper_engine_config.json (현재) mtime = 2026-08-14 18:30, TP [20,50,100] / trail -15 / act 20
  - stable은 13:56~18:30 사이에 기록됐으므로 저장 windows는 이전 TP 정책으로 계산된 값이다.
  - 다만 이것만으로 n_trades 12배(39 -> 478)는 설명되지 않는다. TP를 넓히면 회전이 줄어 오히려 trade가 감소해야 한다. 별도 규명이 필요하다.
- 구조적 결함: 게이트는 저장된 `windows`를 재채점할 뿐 재계산하지 않는다. 따라서 paper_engine_config.json을 고치면 stable의 인증 수치가 조용히 무효화되는데 게이트는 계속 통과시킨다.

### 검증 항목 판정
- 기능 검증 PASS. 게이트 5종 + selection + 승격 마진 런타임 확인.
- 정합성 검증 PASS. OOS 2 fold 모두 selection/base_score에서 분리, windows/게이트 재채점 경로 불변.
- 운영 반영 검증 NA. HPO 미재실행. stable_params_v41_1.json 미변경. 운영 파라미터 그대로다.
- 정책 검증 PASS. split 경계 이동은 val_end 불변으로 OOS holdout 동일. 게이트 신규 조건은 기본값에서 현 stable에 영향 없음.
- FAIL-CLOSED 검증 PASS. (1) 블록의 FAIL 항목을 5번으로 해소. G2/G4로 확인.
- 회귀 검증 PASS(코드 경로 한정). 시뮬레이션 경로 6개 함수 해시 동일. generate_candidates 재실행 회귀는 여전히 미수행이라 별도 NA.

### 다음 단계
1. stable 저장값 재현 불가 원인 규명. 이게 먼저다. 지금 HPO를 재실행하면 새 stable이 나오긴 하지만, 왜 기존 값이 재현되지 않는지 모르는 채로 덮어쓰게 된다.
2. 게이트가 저장 windows를 그대로 신뢰하는 구조 검토. exec policy나 데이터가 바뀌면 인증이 자동 무효화되도록 스탬프(설정 해시/mtime) 대조가 필요하다.
3. 1,2 이후 HPO 재실행.
4. generate_candidates_v41_1.py 1회 실행. candidate_origin CSV 전파 확인. (08-15 (1) 블록 4번, 미착수)
5. 생산 12 vs 백테스트 39 괴리 규명. (08-14 (c)항, 미착수)
6. report_backtest_v41_1.py mkt_ret20 정의 불일치. (08-14 결함 2번, 미수정)

## 2026-08-15 (3) stable 저장값 재현 불가 - 원인 확정

### 결론
- live `stable_params_v41_1.json`은 `_apply_sector_prefilter_union()`이 `KeyError: sector_code_x`를 던지던 시점에 탐색·인증됐다. optimize_params_v41_1.py:1034-1037이 그 호출을 `try/except Exception: pass`로 감싸고 있어 섹터 유니온 폴백이 조용히 0건을 기여했다.
- 2026-08-14 rule_e 생산 배선 작업에서 그 KeyError를 고쳤다(PLANS 08-14: "pool의 기존 sector_code를 drop 후 merge하도록 수정"). 최적화기는 같은 함수를 import해 쓰므로 폴백이 그날부터 실제로 동작하기 시작했다.
- 즉 저장값은 "고장난 시뮬레이션"의 산출물이고, 고친 뒤 같은 파라미터를 다시 재면 전혀 다른 결과가 나온다.

### 증거 (읽기 전용, 산출물 미변경)
- `_apply_sector_prefilter_union`을 KeyError를 던지도록 몽키패치하고 7개 창 전부 재실행. 저장값과 완전 일치했다.

| window | 저장 n | 현재(수정됨) | union KeyError 재현 |
|---|---|---|---|
| 2020-03-27~2020-08-12 | 15 | 92 | **15** |
| 2020-08-13~2021-08-12 | 81 | 353 | **81** |
| 2021-08-13~2022-08-12 | 4 | 349 | **4** |
| 2022-08-13~2023-08-12 | 33 | 337 | **33** |
| 2023-08-13~2024-08-12 | 27 | 373 | **27** |
| 2024-08-13~2025-08-12 | 30 | 372 | **30** |
| 2025-08-13~2026-08-12 | 39 | 478 | **39** |
| TOTAL | 229 | 2354 | **229** |

- 7/7 정확 일치. 다른 가설은 전부 기각됐다.

### 기각된 가설 (전부 실측)
- 파라미터 불일치: main()의 `base` 구성을 그대로 복제해 대조, 32개 키 전부 일치.
- rule_e 미배선: 주입해도 478 -> 471. 영향 2% 미만. (단, 아래 별도 결함 참조)
- exec policy 변경(08-14 18:30 TP [10,20,40]->[20,50,100]): TP를 옛 값으로 되돌려도 478 -> 557. 방향도 크기도 불일치.
- 데이터 변경: KRX parquet 163개 중 stable 기록(08-14 14:59) 이후 수정된 파일 0건.
- symbol_panel 결측: 최신 창 market_cap NaN 1.81%, `>=1e11` 통과 54.4%. panel이 없었다면 후보 0이 되어 39와도 맞지 않는다.
- bear 게이트 무력화: `market_regime` 컬럼 정상, 2021-08~2022-08 창의 89.8%가 BEAR/CRASH. 래더 L0-L9 전 레벨에서 `defense_bear_disable_entry=1.0` 보존 확인.
- import 함수 변경: `_relax_ladder` / `_risk_unit_linear` 모두 08-08 백업 대비 해시 동일.

### 영향
1. 현재 운영 중인 stable 파라미터는 폴백이 죽은 세계에서 최적화됐다. 인증 수치 `oos_pf_weighted=1.4640`, `mean_pf_weighted=1.1224`, `gate ok=true`는 그 버그의 산물이다.
2. 같은 파라미터를 오늘 다시 재면 첫 fold 빼고 전 fold PF < 1.0, avg_pf 0.7993, worst 0.4714. 게이트 재채점 결과 `oos_pf_low(0.7852<1.0)`, `mean_pf_low(0.7322<1.0)`로 FAIL이다.
3. 방향이 뒤집힌 새 불일치가 생겼다. 최적화기는 유니온 폴백 후보를 그대로 매매 대상에 넣는다(optimize_params_v41_1.py:1035에서 병합 결과를 `chosen`으로 받아 전부 `sig`로 보낸다). 반면 생산은 paper_engine/entry.py:5885에서 바로 그 후보들을 관찰전용으로 진입 풀에서 제거한다. 이제 최적화기가 생산보다 관대하다.
4. 게이트는 저장된 `windows`를 재채점만 하고 재계산하지 않으므로, 이런 종류의 무효화를 감지할 수 없다.

### 이번에 함께 드러난 별개 결함 (미수정, 보고만)
1. rule_e가 최적화기에서 비활성이다. `_select_day_candidates_operational()`(optimize_params_v41_1.py:899-905)은 `mkt_ret20_min`/`mkt_ret60_min`/`sector_rs_min`을 읽지만, `simulate_window()`가 만드는 `p0`(977-1001)에 그 3키가 없어 기본값 -1.0(비활성)이 적용된다. main()의 `base`에는 값이 있으나 도달하지 못한다. 생산에는 08-14에 배선됐으므로 이 축에서도 최적화기가 생산보다 관대하다.
2. 래더 L0-L2가 구조적 공집합이다. live stable이 `v_accel_lim=6.6`, `v_accel_max=5.0`이라 조건이 `v_accel > 6.6 AND v_accel <= 5.0`으로 항상 거짓이다. L3(4.8114)부터 성립한다. 즉 L0-L2는 실행되지 않고 실질 시작 레벨이 L3다.
3. `except Exception: pass`가 시뮬레이션 실패를 삼킨다. 이번 사고의 전달 경로다. 최소한 예외 종류와 발생 횟수를 집계해 로그로 남겨야 한다.
4. 2026-08-13/08-14 optimize_params_v41_1.py 및 generate_candidates_v41_1.py 수정 시 backup/ 백업이 남지 않았다(AGENTS.md 14). 이번 규명이 오래 걸린 직접 원인이다.

### 검증 항목 판정
- 기능 검증 PASS. 7/7 정확 재현.
- 정합성 검증 PASS. 경쟁 가설 6종 전부 실측 기각.
- 운영 반영 검증 NA. 코드/산출물 미변경. 읽기 전용 조사다.
- 정책 검증 NA. 이번 작업에서 정책 변경 없음.
- FAIL-CLOSED 검증 FAIL. `except Exception: pass`가 무증상 실패를 허용했고, 게이트는 저장값 재채점만 하므로 감지 수단이 없었다.
- 회귀 검증 NA. 변경 없음.

### 다음 단계
1. 유니온 폴백을 최적화기가 매매 대상으로 셀지 결정. 생산은 관찰전용으로 막고 있으므로, 최적화기도 동일하게 제외하는 것이 정합적이다. 결정 후 반영해야 HPO 재실행이 의미를 갖는다.
2. rule_e를 `p0`에 배선(별개 결함 1).
3. `except Exception: pass`에 예외 집계/로그 추가(별개 결함 3).
4. `v_accel_lim > v_accel_max` 모순 처리 방침 결정(별개 결함 2). BOUNDS 제약 또는 정규화.
5. 1-4 이후 HPO 재실행. 그 전에는 어떤 재실행도 의미가 없다.
6. stable json에 exec policy / 코드 스탬프를 남겨 게이트가 stale을 감지하도록 하는 건 여전히 유효한 과제다.

## 2026-08-15 (4) 최적화기-생산 정합 4건 수정

### 배경
- (3) 블록에서 확정한 원인(유니온 폴백 KeyError)과 함께 드러난 결함들을 순서대로 수정했다. 백업: backup/20260815_optimizer_production_parity/20260815_122725/
- 수정 파일: E:/1_Data/optimize_params_v41_1.py 단일 파일.

### 1. 유니온 폴백을 매매 대상에서 제외 (simulate_window)
- `_apply_sector_prefilter_union()` 호출을 제거했다. 그 함수는 candidate_origin="SECTOR_PREFILTER_UNION" / natural_pass=False 행을 덧붙이기만 하고 자연 후보는 그대로 통과시킨다.
- 생산은 paper_engine/entry.py:5885에서 바로 그 행들을 관찰전용으로 진입 풀에서 제거한다. 최적화기가 이를 매매로 세면 생산보다 관대해진다.
- 호출 후 유니온 행을 필터링하는 것과 결과가 동일하고 일별 비용만 없앤다. 예외에 의존해 우연히 맞던 상태를 명시적 설계로 바꾼 것이다.
- 결합 주의: 이 제외는 generate_candidates_v41_1.py의 keep_cols가 candidate_origin을 유지하는 동안에만 생산과 일치한다. 그 컬럼이 다시 빠지면 관찰전용 마스크가 죽고 생산이 유니온 행을 매매하게 되어, 이번 제외가 반대로 과도해진다.

### 2. rule_e를 p0에 배선 (simulate_window)
- `_select_day_candidates_operational()`은 mkt_ret20_min/mkt_ret60_min/sector_rs_min을 읽지만 p0에 그 3키가 없어 기본값 -1.0(비활성)이 전 래더 레벨에 적용되고 있었다. main()의 base에는 값이 있으나 도달하지 못했다.
- `_rule_e_param()` 헬퍼 신규. `or` 패턴을 쓰지 않는다. 0.0이 mkt_ret20_min/mkt_ret60_min의 유효 임계값이므로 `float(x or d)`는 이를 비활성 sentinel로 되돌린다.
- 검증: 0.0->0.0, 0.05->0.05, None/''/nan/'abc'/키없음 -> -1.0.
- 래더 보존 확인: L0-L9 전 레벨에서 3키 유실 없음, 값 변경 없음.
- 스케일 검증(2025-08~2026-08 창, 598387행): m_ret20 중앙값 0.0077 / m_ret60 0.0815 / sector_rs 0.0028. 전부 분수 단위이며 임계 0.0/0.0/0.05와 단위가 맞는다. 퍼센트 혼동 없음.
- 개별 통과율 54.37% / 77.64% / 29.35%, 3개 동시 14.87%. 하나라도 통과하는 날 134/243(55.1%). 축퇴가 아니라 실제 필터다.

### 3. 무증상 예외 제거
- 이번 사고의 전달 경로였던 `except Exception: pass`(구 1036행)는 1번 수정으로 사라졌다.
- AST 감사 결과 scoring 경로(eval_params / simulate_window / _select_day_candidates_operational)에 pass-only 핸들러 0건. 남은 pass-only 6건은 전부 설정 로더다.
- 그중 시뮬레이션 의미를 바꾸는 2곳에 경고 출력 추가.
  - `_load_production_exec_policy()`: 설정을 못 읽으면 entry=next_open / TP 없음으로 조용히 폴백했다. 그 폴백 자체가 생산과의 괴리다.
  - `_load_split_policy()`: 이 두 날짜가 IS/VAL/OOS를 정한다. 조용히 기본값으로 돌아가면 out-of-sample의 정의가 바뀐다.

### 4. v_accel 공집합 밴드
- 일별 필터는 `v_accel > v_accel_lim AND v_accel <= v_accel_max`를 요구한다. lim >= max는 엄격한 필터가 아니라 공집합이다.
- BOUNDS는 v_accel_lim을 [1.00, 8.00]에서 뽑는데 v_accel_max는 5.0 고정이고 탐색 대상이 아니다. 표집 구간의 37%가 죽은 L0을 만든다.
- live stable이 정확히 거기 있었다: v_accel_lim 6.6 > v_accel_max 5.0. L0-L2는 후보를 낼 수 없었고 실질 시작 레벨이 L3였다. relax_level 진단에는 드러나지 않는다.
- `_enforce_v_accel_band()` 신규. sample_params() 반환 직전과 main()의 base 구성 직후에 적용한다. 거부가 아니라 클램프라 표집 밀도를 유지한다. base가 교정되면 `[FIX]` 로그를 남긴다.
- 검증: (6.6,5.0)->4.9, (5.0,5.0)->4.9, (8.0,5.0)->4.9, (4.8,5.0)/(2.5,5.0) 무변경. sample_params 40회 공집합 0건, v_accel_lim 범위 [1.10, 4.90].

### E2E 결과 (읽기 전용, 산출물 미변경)
- live stable 파라미터를 4건 수정 후 실데이터에 재실행:

| window | 저장 n | 수정 전 | 수정 후 |
|---|---|---|---|
| 2020-03-27~2020-08-12 | 15 | 92 | 3 |
| 2020-08-13~2021-08-12 | 81 | 353 | 12 |
| 2021-08-13~2022-08-12 | 4 | 349 | 0 |
| 2022-08-13~2023-08-12 | 33 | 337 | 1 |
| 2023-08-13~2024-08-12 | 27 | 373 | 5 |
| 2024-08-13~2025-08-12 | 30 | 372 | 2 |
| 2025-08-13~2026-08-12 | 39 | 478 | 9 |
| TOTAL | 229 | 2354 | 32 |

- 전 fold가 MIN_TRADES_PER_WINDOW=15 미만이라 n_folds(selection)=0, hypertime_reason=insufficient_folds, hypertime_score=-1e9.
- 게이트: ok=False. `oos_trades_low(11<20)`, `oos_worst_fold_missing`, `mean_pf_low(0.0<1.0)`.
- 해석: 이것은 버그가 아니라 정직한 결과다. 생산과 동등한 필터(유니온 폴백 제외 + rule_e 활성) 아래에서 현재 live stable 파라미터는 6.4년간 32건밖에 거래하지 못한다. 08-14에 측정한 생산 후보 발생일 29% -> 5.6% 붕괴, 그리고 진입률 4개월 76% 감소와 같은 현상이다.

### 검증 항목 판정
- 기능 검증 PASS. 헬퍼 2종 단위테스트 + 래더 보존 + 스케일 검증 + E2E 1회.
- 정합성 검증 PASS. 유니온/rule_e 두 축 모두 생산 정의와 일치시킴. 스케일 단위 확인.
- 운영 반영 검증 NA. HPO 미재실행. stable_params_v41_1.json 미변경. 운영 파라미터 그대로다.
- 정책 검증 PASS. 게이트 의미 불변. v_accel 클램프는 공집합 제거이지 임계 완화가 아니다.
- FAIL-CLOSED 검증 PASS. 무증상 예외 경로 제거, 설정 폴백 2곳 경고화. 거래 부족은 insufficient_folds로 차단됨을 E2E로 확인.
- 회귀 검증 PARTIAL. 코드 경로/헬퍼는 확인했으나, 4건 수정 후 HPO 전체 실행 회귀는 미수행.

### 다음 단계에서 결정 필요
1. 현재 상태로 HPO를 돌리면 대부분 조합이 insufficient_folds로 떨어질 가능성이 높다. 다만 HPO는 value_min(stable 1550억), rs_lim 등을 함께 흔들므로 탐색 공간 전체가 불가능한지는 실행 전에는 알 수 없다.
2. rule_e 임계값은 현재 main()에 하드코딩된 상수(0.0/0.0/0.05)이고 BOUNDS 191-193행은 주석 처리되어 있다. 즉 HPO가 rule_e 강도를 조절할 수 없다. 주석을 풀어 탐색 대상에 넣을지는 정책 결정이다.
3. MIN_TRADES_PER_WINDOW=15 / MIN_TRADES_TOTAL=120 기준을 유지할지도 함께 봐야 한다.
4. HPO 재실행은 promoted=True일 때 stable_params_v41_1.json을 덮어쓴다. 실행 전 사용자 확인 필요.

## 2026-08-15 (5) HPO read-only 재실행 - 탐색공간 구조적 불가능 확인

### 실행 방식
- scratchpad/hpo_readonly.py. main()을 그대로 호출하되 write 3경로를 사전 무력화했다.
  - `_jsave` -> 로깅 no-op (best_params_v41_1.json)
  - `_persist_promoted_stable` -> 로깅 no-op (stable_params_v41_1.json)
  - `pandas.DataFrame.to_csv` -> 로깅 no-op (search_report_v41_1.csv)
- 종료 후 stable_params_v41_1.json 바이트 비교: **unchanged=True**. 차단된 write 3건 전부 로그에 기록됨.
- N_ITER=40, YEARS_BACK=10, 유효 창 7개. rc=0.

### 결과 - 40개 조합 중 채점 가능한 조합 0건
- best_score = -999999998.0 = `-1e9 + n_folds(2)`. 즉 최선의 조합도 selection fold 2개로 HPO_MIN_FOLDS=3 미달.
- promoted=False, gate_ok=False. `not_promoted;stable_score_low;oos_pf_low(0.6417<1.0);mean_pf_low(0.6793<1.0)`.

창별 n 통계 (40개 조합 across):

| w | 기간 | split | min | med | max | n>=15 |
|---|---|---|---|---|---|---|
| 1 | 2020-03~2020-08 | IS (부분창 4.5개월) | 0 | 1 | 10 | **0/40** |
| 2 | 2020-08~2021-08 | IS | 1 | 4 | 44 | 14/40 |
| 3 | 2021-08~2022-08 | IS (89.8% BEAR/CRASH) | 0 | 0 | **0** | **0/40** |
| 4 | 2022-08~2023-08 | VAL | 0 | 2 | 5 | **0/40** |
| 5 | 2023-08~2024-08 | VAL | 0 | 3 | 16 | 1/40 |
| 6 | 2024-08~2025-08 | OOS holdout | 0 | 1 | 9 | 0/40 |
| 7 | 2025-08~2026-08 | OOS holdout | 1 | 12 | 43 | 15/40 |

- selection 대상은 비OOS 창 5개(1~5). 조합별 적격 fold 수 분포 = {0: 26, 1: 13, 2: 1}. 3 이상 **0/40**.
- MIN_TRADES_TOTAL=120 충족 = 1/40 (ITER_35, 127건).
- STABLE_BASELINE = NEW_STABLE = [6, 27, 0, 2, 9, 5, 21], 총 70건, 적격 fold 2.
  - 참고: (4) 블록의 E2E는 총 32건이었다. 차이는 Fix 4다. main()은 base에 `_enforce_v_accel_band`를 적용해 v_accel_lim 6.6 -> 4.9로 교정하고 L0을 되살리지만, (4) 블록 스크립트는 구 main()을 복제해 클램프가 없었다. 즉 Fix 4가 현 stable의 거래량을 32 -> 70으로 약 2배 늘렸다. 그래도 부적격이다.

### 구조적 진단 - 튜닝으로 넘을 수 없다
- 창3(2021-08~2022-08)은 40/40 조합에서 n=0이다. 이 창은 89.8%가 BEAR/CRASH이고 `defense_bear_disable_entry=1.0`이 bear 진입을 전면 차단하므로, 어떤 파라미터로도 거래가 발생할 수 없다. 영구 부적격 fold다.
- 창1은 2020-03-27 데이터 시작으로 잘린 4.5개월 부분창이라 40/40에서 최대 10건. n>=15를 구조적으로 채우기 어렵다.
- 창4도 0/40 (최대 5건).
- 따라서 selection에서 실질 사용 가능한 창은 2와 5뿐이고, 최대 적격 fold 수는 2다. **HPO_MIN_FOLDS=3은 파라미터 탐색이 아니라 창 구성상 도달 불가능하다.**
- OOS도 같은 문제다. 창6은 0/40으로 n>=15를 못 채우므로, holdout 2개 중 1개는 사실상 채점 불능이고 `min_oos_trades=20`도 창7 단독에 의존한다.

### 해석
- 이번 목적함수 재구축이 만든 문제가 아니다. 생산과 동등한 필터(유니온 폴백 제외 + rule_e 활성)를 걸면 v41.1이 만들어내는 거래량 자체가 검증 프레임워크를 돌릴 수 없는 수준이라는 뜻이다.
- 08-14 측정(생산 후보 발생일 29% -> 5.6%)과 진입률 4개월 76% 감소와 같은 현상이며, 그것을 최적화기에서 정량 확인한 것이다.
- 지금까지 "gate 통과"가 가능했던 이유는 유니온 폴백 KeyError로 거래가 부풀려졌고((3) 블록), OOS가 목적함수에 직접 들어가 있었기 때문((1)(2) 블록)이다. 두 가지를 모두 제거하자 실제 표본 부족이 드러났다.

### 검증 항목 판정
- 기능 검증 PASS. main() rc=0, 40/40 조합 평가 완료.
- 정합성 검증 PASS. 창별 통계로 병목을 특정함(창1/3/4 구조적 부적격).
- 운영 반영 검증 NA(의도적). write 3건 전부 차단, stable 바이트 동일 확인.
- 정책 검증 PASS. 게이트/기준값 변경 없이 현 상태 그대로 측정.
- FAIL-CLOSED 검증 PASS. 표본 부족이 insufficient_folds -> promoted=False -> stable 미변경으로 정상 차단됐다.
- 회귀 검증 PASS. 4건 수정 후 HPO 전체 실행이 예외 없이 완주했다((4) 블록의 PARTIAL 해소).

### 결정 필요
- 검증 기준(MIN_TRADES_PER_WINDOW=15, HPO_MIN_FOLDS=3, MIN_TRADES_TOTAL=120)을 낮추는 선택지는 권하지 않는다. 이번 감사 전체가 "증거 기준이 느슨해서 잘못된 인증이 나왔다"는 문제였고, 기준을 낮추는 건 같은 실수의 반복이다.
- 창 구성 문제(창1 부분창, 창3 영구 부적격)는 기준을 낮추지 않고도 손댈 수 있는 부분이다. 다만 창 구성 변경은 IS/VAL/OOS 배치를 다시 흔든다.
- rule_e 임계값과 `defense_bear_disable_entry`를 탐색 대상으로 열면 HPO가 거래 가능한 지점을 찾을 수 있으나, 둘 다 08-14에 생산에 배선한 정책이므로 정책 변경 결정이 필요하다.
- 근본적으로는 project_1data_tradability_pivot의 결론과 같은 지점이다. v41.1을 튜닝할 것인지, 거래 가능한 로직을 새로 세울 것인지의 문제다.

## 2026-08-15 (6) rule_e 생산 복원

### 배경
- 같은 날 BLOCKER 3/4 롤백에서 `_select_candidates`를 git HEAD(2026-08-08) 판으로 되돌리면서 rule_e 3게이트가 함께 사라졌다.
- 롤백 대상은 통합이 새로 넣은 `v_accel_max`와 bear 게이트였다. rule_e는 08-14에 분석을 거쳐 의도적으로 배선한 별개 정책이므로 롤백 대상이 아니었다.
- 그 결과 두 가지 불일치가 생겼다.
  - 최적화기는 rule_e 적용(08-15 Fix 2), 생산은 미적용.
  - 생산 내부에서 `_apply_sector_prefilter_union()`의 soft gate는 rule_e를 계속 적용해, 폴백이 자기가 넓혀야 할 메인 경로보다 엄격해졌다.

### 수정
- 파일: E:/1_Data/generate_candidates_v41_1.py 단일 파일. 백업: backup/20260815_restore_rule_e_production/20260815_142210/
- `_select_candidates()`에 rule_e 3게이트 복원. `v_accel_max`와 bear 게이트는 복원하지 않았다.
- 의미는 `_apply_sector_prefilter_union()` soft gate 및 report_backtest_v41_1.py:778-783과 동일하게 맞췄다. strict `>`, 따라서 NaN 제외. `_rule_e_threshold()`를 통해 -1.0은 비활성.
- 컬럼 누락 시 조용히 넘어가지 않고 `[WARN]`을 출력하도록 했다. 활성 정책 게이트가 흔적 없이 꺼지는 것이 이번 감사에서 반복된 실패 방식이다.
- rule_e는 `_relax_ladder()`가 건드리지 않는다. 시장 단위 게이트이지 종목 임계값이 아니다.

### 검증
- py_compile PASS.
- `_rule_e_threshold` 회귀: 0.0 -> 0.0(falsy trap 없음), 0.05 -> 0.05, -1.0/None/nan/문자열 -> None.
- 합성 프레임 3행: rule_e OFF 3/3 통과, ON(0/0/0.05) 1/3 통과(의도한 행만), 컬럼 누락 시 WARN 출력 후 해당 게이트만 미적용.
- 실데이터 최근 214 세션(2025-09-25~2026-08-13), 섹터 유니온 폴백 제외:

| 방법 | rule_e OFF | rule_e ON |
|---|---|---|
| 래더 미적용(L0만) | 22일 / 23건 | 4일 / 4건 |
| 래더 적용(L0~L9) | 142일 / 191건 | 53일 / 64건 |

- 감소 배율은 두 방법 모두 약 5배로, 08-14 기록(62일 -> 12일, 5.2배)과 방향/배율이 일치한다.

### 미해결 - 후보 발생일 절대값이 기록마다 다르다
- 08-14 PLANS 기록: 62일 -> 12일
- 08-15 롤백 후 사용자 보고: 105일 / 110건
- 이번 측정: 래더 미적용 22일, 래더 적용 142일
- 네 숫자가 모두 다르고, 08-14와 사용자 보고 값은 이번 측정의 두 방법 사이에 위치한다.
- 확정된 사실 하나: 후보 발생일은 래더 적용 여부에 따라 22일과 142일로 6.5배 차이가 난다. 따라서 래더 조건을 명시하지 않은 후보 발생일 수치는 비교 근거가 될 수 없다.
- 남은 차이의 원인은 미규명이다. 후보: 측정 창 정의, 래더 종료 조건, `_compute_factors` 변경 여부. 향후 이 지표를 인용할 때는 (창, 래더 적용 여부, 섹터 유니온 포함 여부) 3개를 함께 적어야 한다.

### 함께 확인한 것
- `_apply_sector_prefilter_union()`의 rule_e와 메인 경로가 다시 일치해 폴백 역전이 해소됐다.
- `_rule_e_diag_counts()`가 보고하는 통과 건수가 다시 실제 적용되는 필터를 가리킨다.
- `[PARAM] rule_e ...` 로그가 다시 활성 상태와 일치한다.

### 남은 결함 (미수정, 보고만)
1. `_diag_counts()`가 `v_accel_max_pass`를 계속 보고한다. 생산은 `v_accel_max`를 적용하지 않으므로 이 진단은 여전히 사실과 다르다.
2. 최적화기와 생산의 필터 집합이 여전히 다르다. 생산은 9조건+macd+rule_e, 최적화기는 거기에 `v_accel_max` / bear 방어 / `sector_blacklist` / `min_market_cap` / `require_above_ma200`이 추가된다. 후보 레이어 통합(Phase 3)은 롤백된 상태이며 해소가 아니라 미해결이다.
3. 08-15 13:52 통합 작업의 backup/ 백업이 없었다. git HEAD도 08-08이라 08-14 이후 변경이 전부 미커밋이다.

### 검증 항목 판정
- 기능 검증 PASS. 단위 3종 + 실데이터 214세션 2방법.
- 정합성 검증 PASS. 메인/폴백/진단/로그가 다시 같은 상태를 가리킨다.
- 운영 반영 검증 NA. generate_candidates 정식 실행 미수행, 운영 산출물 미변경.
- 정책 검증 PASS. 08-14에 결정된 rule_e 정책으로 복귀. `v_accel_max`/bear는 복원하지 않아 롤백 의도 유지.
- FAIL-CLOSED 검증 PASS. 컬럼 누락 시 무증상 통과 대신 WARN.
- 회귀 검증 PARTIAL. rule_e OFF(-1.0) 설정에서 게이트가 no-op임을 합성 프레임으로 확인했으나, 생산 파이프라인 전체 실행 회귀는 미수행.

## 2026-08-15 (7) 감사 스레드 커밋 + .gitignore 추적 공백

### 커밋
- 브랜치 `codex/alignment-quality-next`. 기본 브랜치가 아니다.
- HEAD가 2026-08-08이었고 그 이후 변경이 전부 working tree에 쌓여 있었다. 08-14 rule_e 배선, 08-15 감사 작업 전체가 미커밋 상태였다.

| 커밋 | 내용 | 규모 |
|---|---|---|
| 421823a4 | recovery(audit): restore rule_e gates in production candidate path | 5파일 2420+/235- |
| 8068b186 | feat(backtest): add sector_rs filter and analysis scaffolding (2026-08-14) | 1파일 1141+/150- |

- 421823a4 포함 파일: generate_candidates_v41_1.py(215), optimize_params_v41_1.py(1310), utils/stable_params_gate.py(199), strategy_core.py(98 신규), .agent/PLANS.md(833 신규).
- report_backtest_v41_1.py는 mtime 08-14 09:38로 오늘 작업과 무관함을 확인하고(strategy_core / provenance / min_oos_worst_fold 참조 0건) 별도 커밋으로 분리했다.
- "rule_e 복원만의 최소 커밋"은 불가능했다. HEAD가 08-08이라 generate_candidates_v41_1.py 한 파일만 커밋해도 215라인, 즉 08-14 rule_e 배선 / sector_code merge 수정 / keep_cols candidate_origin 복원 / 08-15 통합과 롤백 / rule_e 재복원이 통째로 들어간다. 이 환경은 interactive add를 지원하지 않고, 얽힌 215라인의 수동 분리는 위험 대비 실익이 없다고 판단했다.
- 핵심 파일 working tree 잔여 0건. tools/ 미추적 608건 + 수정 73건, krx_daily_archive/ 86건은 이번에 손대지 않았다.

### 발견 - 설정 파일이 버전 관리 밖에 있다
- `.gitignore:80`이 `12_Risk_Controlled/`를 통째로 제외한다. 결과적으로 다음 두 파일이 추적되지 않는다.
  - `12_Risk_Controlled/split_policy_v41_1.json`
  - `12_Risk_Controlled/stable_params_v41_1.json`
- split_policy는 산출물이 아니라 **설정**이다. IS/VAL/OOS 경계를 정하며, 오늘 (2) 블록에서 train_end를 2023-12-31 -> 2022-12-31로 바꿨다. 그 변경이 421823a4에 들어가지 않았다.
- 결과: 코드는 커밋됐는데 그 코드의 동작을 결정하는 설정은 추적되지 않는다. 이 커밋을 체크아웃해도 같은 fold 구성이 재현되지 않는다. 원본은 backup/20260815_hpo_objective_rebuild/20260815_113039/ 에만 있다.
- 이는 (3) 블록에서 규명한 "stable 저장값 재현 불가"와 같은 계열의 문제다. 재현에 필요한 입력이 버전 관리 밖에 있으면 재현 실패의 원인을 사후에 특정할 수 없다.
- 판단: stable_params_v41_1.json은 산출물이므로 제외 유지가 맞다. split_policy_v41_1.json은 `.gitignore`에 예외(`!12_Risk_Controlled/split_policy_v41_1.json`)를 넣어 추적하는 것이 맞다고 본다. 다만 .gitignore 변경은 사용자 결정 사항이라 적용하지 않았다.

### 검증 항목 판정
- 기능 검증 PASS. 커밋 2건 생성 확인, HEAD 상세 및 포함 파일 목록 확인.
- 정합성 검증 PASS. report_backtest 분리 근거를 mtime과 참조 0건으로 확인.
- 운영 반영 검증 NA. 커밋은 이력 작업이며 운영 산출물을 변경하지 않는다.
- 정책 검증 PASS. 기본 브랜치가 아닌 작업 브랜치에 커밋.
- FAIL-CLOSED 검증 NA.
- 회귀 검증 PASS. 커밋 후 핵심 5파일 working tree 잔여 0건 확인.

### RootB 확인 (AGENTS.md 15)
- E:\vibe\buffett\PLANS.md 최종 수정 2026-08-13 10:30. 오늘 작업은 대시보드/화면/상태 JSON/React를 건드리지 않았다.
- 세션 중 RootB는 읽기만 했다(selection_metrics 소비처 조사, 결과는 백업 .bak 1건뿐으로 소비처 없음).
- 따라서 RootB PLANS에 기록할 내용 없음.

### 미기록 상태
- 이 (7) 블록 자체는 커밋 이후에 추가되었으므로 421823a4에 포함되지 않는다. 다음 커밋 대상이다.

## 2026-08-15 (8) 거래 빈도 제약 진단 검증 - 수치 정확, 해석 3건 정정

### 대상
- 다른 세션이 산출한 제약 비용 ablation. 원본: `2_Logs/frequency_constraint_diag_latest.json` (10 시나리오, mtime 08-15 15:07).
- 본 블록은 그 결과를 read-only로 검증한 기록이다. 코드/산출물 변경 없음.

### 수치 검증 - 일치
- 보고된 표는 JSON과 정확히 일치한다. 전사 오류 없음.
- 독립 재현 결과도 일치했다. 전체 세션 1624 (보고서 all_days와 동일), `current` 13일/13건, `rule_e_off` 105일/110건.
- 즉 측정 자체는 신뢰할 수 있다. 문제는 측정 조건과 그로부터 끌어낸 해석이다.

### 정정 1 - 래더 미적용. 생산 경로가 아니다
- live stable은 `use_relax_ladder=1.0`이다. 생산 `_select_candidates`는 항상 `_relax_ladder()` L0~L9를 순회해 최초로 후보가 나오는 레벨을 채택한다. 보고서는 L0만 측정했다.

| 시나리오 | 래더 미적용(보고서) | 래더 적용(생산 실제) |
|---|---|---|
| current (rule_e ON) | 13일 / 13건 | **159일 / 188건** |
| rule_e OFF | 105일 / 110건 | **1117일 / 1497건** |

- 차이가 각각 12.2배, 10.6배다.
- 따라서 다음 두 결론이 뒤집힌다.
  - "현재 생산 경로는 rule_e 때문에 연 2건 수준" -> 실제는 **연 약 23건**(159일 / 6.4년).
  - "연 20~40건 목표를 달성하려면 rule_e를 끄거나 완화해야 한다" -> **rule_e를 켠 상태로 이미 그 범위 안에 있다.** 제약 변경 없이 목표가 충족된다.
- `current` 시나리오의 연도별 2022=0 / 2023=0 / 2026=0도 L0 전용 측정의 산물이다. 실제 08-13 생산 산출 CSV에는 후보 23행이 존재했다.
- 이는 (6) 블록에서 미해결로 남긴 "후보 발생일 숫자가 기록마다 다르다"의 직접적인 원인이기도 하다. 62 / 105 / 22 / 142 가 모두 다른 이유는 래더 적용 여부가 명시되지 않았기 때문이다.

### 정정 2 - bear 게이트는 중복이 아니라 미배선
- 토글 무영향 자체는 재현된다. `defense_bear_disable_entry` 1.0 -> 13일/13건, 0.0 -> 13일/13건.
- 그러나 원인이 다르다. 오늘 13:52 통합을 롤백할 때 생산 `_select_candidates`에서 bear 게이트 코드가 제거됐고, (6) 블록에서 rule_e만 복원했다. **파라미터를 읽는 코드가 존재하지 않으므로** 토글이 아무 일도 하지 못한다.
- 보고서 해석: "rule_e가 이미 해당 날짜를 차단하고 있어 bear 전면 차단은 현재 중복 게이트".
- 실제: bear 게이트는 생산에서 **꺼져 있다**. 다시 켜면 후보는 더 줄어든다. 그리고 최적화기(strategy_core)는 여전히 적용하므로 이 축의 최적화기-생산 괴리는 미해결 상태다.
- "중복이므로 무시 가능"으로 읽으면 위험하다. 판단 근거가 성립하지 않는다.

### 정정 3 - 229건은 부풀려진 값이 아니라 눌린 값
- 보고서: "08-14 stable의 backtest 229건/6.4년은 union fallback KeyError로 부풀려진 수치".
- 방향이 반대다. (3) 블록 실측 기준:

| 상태 | 총 거래 |
|---|---|
| 저장값 (KeyError 상태) | 229 |
| KeyError 수정 후 | 2354 |
| KeyError 재현 | 229 (7/7 fold 일치) |

- KeyError는 폴백을 죽여 거래를 **줄였다**. 229는 폴백이 죽었을 때 나오는 값이다.
- 부풀려진 것은 거래 수가 아니라 **PF**다. 덜 거래해서 성과가 좋아 보였다(인증 oos_pf 1.4640 -> 수정 후 0.7852).

### 검증하지 못한 것
- "후보/년 x 0.3~0.6 = 실제 진입 건수" 환산 계수는 근거가 제시되지 않았다. `max_pos=6` / `hold=13`에서 어떻게 도출됐는지 확인 불가.
- 후보와 진입 사이에는 positive_entry_criteria, execution_pool, 섹터 적격성, top_score_cap, 각종 guard가 더 있다. 단일 계수로 환산될 가능성은 낮다.

### 수정된 판단
- 래더를 적용하면 현 제약 그대로 후보 약 23건/년이다. **"연 20~40건" 기준에서 후보 생성 단계는 이미 병목이 아니다.**
- 병목은 그 아래다. 후보 188건이 실제 진입/체결 몇 건이 되는지가 규명되지 않았다. project_1data_entry_rate_decay에 기록된 "순수 전략 진입 4개월간 76% 감소"가 가리키는 지점과 같다.
- 다음 측정 대상은 제약 완화가 아니라 **후보 -> 진입 -> 체결 구간의 감쇠**다.

### 측정 규약 (앞으로 이 지표를 인용할 때)
- 후보 발생일/후보 수를 적을 때는 반드시 다음 3개를 함께 명시한다. 하나라도 빠지면 다른 기록과 비교 불가다.
  1. 측정 창 (시작~종료, 세션 수)
  2. 래더 적용 여부 (L0 단독인지 L0~L9 순회인지)
  3. 섹터 유니온 폴백 포함 여부

### 검증 항목 판정
- 기능 검증 PASS. 1624 세션 전체에서 보고 수치 독립 재현.
- 정합성 검증 FAIL(대상 보고서 기준). 측정 경로가 생산과 다르고, 해석 3건이 사실과 어긋난다. 수치 자체는 정확하다.
- 운영 반영 검증 NA. 읽기 전용 검증이며 코드/산출물 변경 없음.
- 정책 검증 NA.
- FAIL-CLOSED 검증 NA.
- 회귀 검증 NA.

## 2026-08-15 (9) 후보 -> 체결 전환 3단 조인 검증

### 대상
- 다른 세션이 보고한 "후보 137건 -> BUY 2건(1.5%), 같은 기간 다른 전략 BUY 379건(v41.1 기여 0.6%)" 및 결정 원장 기반 차단 사유 분석.
- 본 블록은 read-only 검증이다. 코드/산출물 변경 없음.

### 사용한 소스
- 후보: `2_Logs/candidates_v41_1_YYYYMMDD.csv` 13개 파일 (20260714~20260812), 총 137행. 보고서 수치와 일치.
- 체결: `paper/fills.csv` 1007행 (BUY 381 / SELL 626).
- 조인 키: fills의 `note`에 담긴 `signal_date=YYYYMMDD` + `code`. 진입 타이밍 추정(D / D+1)에 의존하지 않는 정확 키다.

### 정정 1 - "379건"은 기간 불일치다
- 후보 signal_date 범위: 2026-07-14 ~ 08-12 (약 1개월)
- BUY signal_date 범위: 2025-12-24 ~ 2026-08-07 (약 7.5개월)
- 381건은 7.5개월 전체 수치다. 후보 창과 동일 기간으로 맞추면 BUY는 **37건**이다.

| 항목 | 보고서 | 실측 |
|---|---|---|
| v41.1 후보 | 137건 | 137건 (일치) |
| -> BUY 체결 | 2건 / 1.5% | 2건 / 1.5% (일치) |
| 같은 창의 다른 경로 BUY | 379건 | **35건** |
| BUY 중 v41.1 기여 | 0.6% | **5.4%** |

- 9배 차이다. 분모에 후보 아카이브가 존재하지도 않는 6.5개월이 포함돼 있었다.

### 정정 2 - 08-12 후보는 매칭 대상 자체가 없다
- BUY의 마지막 signal_date는 20260807이다. 20260812 후보 10건은 체결 기록이 존재할 수 없는 구간이다.
- 유효 창(signal_date <= 20260807)만 보면 후보 127건 중 체결 2건 = 1.6%.
- 전환율 결론은 조인 방식/창 보정과 무관하게 1.5~1.6%로 유지된다.

### 정정 3 - 결정 원장은 차단률의 근거가 될 수 없다 ((8) 블록 연장)
- `2_Logs/candidate_decision_outcome_ledger_history.csv` 158,995행의 decision_type 분포: WATCH 131,236 / BLOCK 27,693 / HOLD 62 / **BUY 4**.
- 15만 9천 행 중 BUY가 4건이다. 이 파일은 후보였으나 진입되지 않은 건을 사후 추적하는 미실현 기회 원장이다(`ret_5m_pct` / `ret_15m_pct` / `ret_next_day_pct` 컬럼이 그 성격을 보여준다).
- 따라서 "10개 중 0개 진입", "normal 350건이 전부 WATCH/BLOCK"은 사실이지만 차단률의 근거가 아니다. 차단된 것만 모인 파일에서 차단 사유만 나오는 것은 순환이다. 종속변수 기준 표본 선택에 해당한다.
- source_membership에 `candidate`가 포함된 행은 158,995 중 2,670 (1.68%)뿐이다. 이 원장의 98.3%는 v41.1 후보 파일에서 온 것이 아니다.
- 보고된 10개 코드 중 5개(010060, 024840, 064400, 066570, 078930)는 `strategy_group=surge`이며 사유가 `surge_candidate_no_policy_exclusion`이다. v41.1 진입 로직의 판단이 아니라 급등 전략의 제외 사유다.

### 유지되는 결론
- "병목은 후보 생성이 아니라 후보 -> 진입 전환에 있다"는 방향은 맞다. 전환율 1.5~1.6%는 조인 방식을 바꿔도 유지된다.
- (8) 블록에서 확인한 "래더 적용 시 후보 약 23건/년"과 합치면, 후보 단계는 이미 병목이 아니라는 판단도 유지된다.

### 새로 드러난 사실
- 같은 창에서 v41.1이 아닌 경로의 BUY가 35건 있다. **시스템은 거래하고 있으며, v41.1 후보를 거의 쓰지 않을 뿐이다.**
- `fills.csv`의 note는 전부 `signal_date=...;sizing=capital_slot` 형식이라 전략 구분자가 없다. 이 35건의 출처는 이 파일만으로 판별 불가다. 다음 확인 대상이다.
- `candidates_latest_data.with_final_score.csv`의 08-12 후보 10건은 전부 `execution_pool=False`다. `paper_engine/entry.py`는 `require_execution_pool_when_present=True`이므로 이 단계에서 진입 풀에서 제외된다. 이는 "missed move" 판정보다 앞선 지점이며, 실제 차단 지점 후보다.
- 같은 파일의 `candidate_origin`은 23행 전부 NaN이다. 08-12 후보 10건이 섹터 유니온 폴백이 아니라는 뜻이므로 관찰전용 마스크는 이 건들과 무관하다.

### 검증 항목 판정
- 기능 검증 PASS. 137건 / 2건 / 381건을 원본에서 독립 재현.
- 정합성 검증 FAIL(대상 보고서 기준). 379 대 2 비교가 기간 불일치이며, 결정 원장 기반 차단률 추론이 성립하지 않는다. 전환율 1.5%와 방향성 결론은 유효하다.
- 운영 반영 검증 NA. 읽기 전용.
- 정책 검증 NA.
- FAIL-CLOSED 검증 NA.
- 회귀 검증 NA.

### 다음
1. `execution_pool`을 무엇이 어떤 기준으로 False로 정하는지 추적. 현재까지 가장 앞선 차단 후보다.
2. 같은 창의 비-v41.1 BUY 35건의 출처 규명. v41.1이 아니면 무엇이 실제 매매를 만들고 있는지가 핵심이다.
3. 비교 규약: 후보와 체결을 비교할 때는 반드시 signal_date 기준 동일 창으로 맞춘다. fills.csv는 후보 아카이브보다 6.5개월 길다.

## 2026-08-15 (10) execution_pool 추적 - v41.1 일반 후보 진입 100% 차단 확인

### 배경
- (9) 블록에서 후보 -> 체결 전환율 1.5~1.6%를 확인하고, 08-12 후보 10건이 전부 `execution_pool=False`인 것을 차단 후보로 남겼다.
- 본 블록은 그 값이 어디서 정해지는지 추적한 read-only 기록이다. 코드/산출물 변경 없음.

### 결정 지점 - tools/final_score_merge_daily.py:1461-1469
- `_restore_lineage_columns()`가 `candidate_origin` / `execution_pool` / `natural_pass` 3개를 처리한다.

```
restored[col] = (
    restored[col].astype(str).str.strip().str.lower()
    .map({"true": "True", "false": "False"})
    .fillna("False")
)
```

- `.map()`이 빈 문자열과 NaN을 NaN으로 흘리고 `.fillna("False")`가 전부 False로 확정한다. 재현: `''` -> False, `nan` -> False, `'True'` -> True, `'False'` -> False.

### 왜 항상 False인가 - 값을 공급하는 쪽이 없다
- 이 함수는 머지 중인 `df`와 `IN_BASE`(= `2_Logs/candidates_latest_data.csv`) 두 곳에서 값을 찾는다.
- 베이스 CSV는 36컬럼이며 `execution_pool` / `natural_pass` / `candidate_origin` 셋 다 없다. `generate_candidates_v41_1.py`의 `keep_cols`에 `execution_pool`이 포함된 적이 없다(오늘 추가한 것은 candidate_origin/natural_pass/observe_only 3개뿐이며 execution_pool은 여전히 없다).
- 따라서 df에도 base에도 값이 없고, 전부 빈 값 -> `fillna("False")` -> 모든 정상 후보가 `execution_pool=False`가 된다.
- `True`로 설정되는 지점은 세 곳뿐이며 전부 예외 경로다.
  - `paper_engine/positions.py:1107` 분할진입 2차
  - `paper_engine/positions.py:1171` 분할진입 마스크
  - `paper_engine/entry.py:5629` fresh 섹터 폴백
- 일반 진입 경로에 `execution_pool=True`를 부여하는 코드는 존재하지 않는다.

### 차단 지점
- `paper_engine/entry.py`의 `require_execution_pool_when_present=True`(config.py:322 기본값) 하에서
  `entry_mask = execution_pool.isin(["TRUE","1","Y","YES"])` -> 전부 False -> `candidate_df[entry_mask]`로 전량 제거.

### 독립 검증 - 기존 감사 도구 로직 적용
- `tools/build_final_candidate_elimination_audit.py`(05-26)는 `FINALIST_CODES` 2개 하드코딩이라 그대로 쓸 수 없으나, 탈락 사유 판정 로직을 현재 후보 전체에 적용했다.
- `candidates_latest_data.with_final_score.csv` 23행 결과:

| 탈락 사유 | 건수 |
|---|---|
| `raw_execution_pool_false` | **23 / 23 (100%)** |
| `final_score_not_positive` | 0 |
| `union_conditional_entry_pool` | 0 |

- `final_score`는 전부 양수(0.044~0.190)다. 점수 문제도 섹터 유니온 문제도 아니다.
- 23행 중 9행은 문자열 "False", 14행은 빈 값이며 `_truthy()`가 둘 다 False로 판정한다. `fillna` 경로가 그대로 관측된다.
- 코드 추적과 감사 도구 로직이라는 독립적인 두 근거가 같은 지점을 가리킨다.

### bought_sometime 6종목 8건의 출처 - 전부 예외 경로
- 024840 / 064400 / 068270 / 080220 / 161890 / 240810의 BUY 체결을 `paper/fills.csv`의 `note`로 추적했다. note에 `entry_source_kind`가 기록돼 있다.

| 체결일 | 코드 | 경로 | 08-12 후보와 관계 |
|---|---|---|---|
| 2026-03-17 | 068270 | 구형(필드 없음) | 5개월 전, 무관 |
| 2026-04-02 | 240810 | entry_timing=same_close | 4개월 전, 무관 |
| 2026-04-13 | 240810 | **split_entry=1st**, fallback_stage=1 | 무관 |
| 2026-06-22 | 080220 | **SURGE_RUNTIME** (surge_immediate=1) | 무관 |
| 2026-07-03 | 064400 | **INTRADAY_REALTIME** | 무관 |
| 2026-07-28 | 068270 | **beta_harvest** (regime=CRASH, target_exposure=0.1) | 후보 아님 |
| 2026-07-28 | 161890 | **beta_harvest** | 후보 아님 |
| 2026-08-07 | 024840 | **INTRADAY_REALTIME** | 후보 아님 |

- 8건 중 v41.1 일반 진입 경로로 들어온 것은 **0건**이다.
- 08-12 후보의 signal_date와 일치하는 체결도 0건이다. `bought_sometime` 플래그는 fills 전체 기간(2025-12~2026-08) 기준이라 붙은 것이며, 해당 후보와 무관하다.
- 경로 분포: INTRADAY_REALTIME 2, beta_harvest 2, SURGE_RUNTIME 1, split_entry 1, 구형/미상 2.

### 판정
- v41.1 정상 후보는 `execution_pool=False`로 진입 풀에서 100% 제거된다. 실제 매매는 전부 우회 경로(INTRADAY_REALTIME / SURGE_RUNTIME / beta_harvest / split_entry)에서 발생한다.
- (9) 블록의 "같은 창 비-v41.1 BUY 35건"의 정체가 이것이다.
- `RECHECK_MISSED_MOVE_CANDIDATE` / `EARLY_BLOCKED_MOVE_WATCH` 등의 사유는 이 필터를 통과한 뒤의 단계다. 실제 차단은 그 앞에서 끝난다.
- 특히 `beta_harvest`는 전략 시그널이 아니라 레짐 기반 베타 익스포저 관리다. 즉 현재 체결의 상당 부분이 v41.1 전략 판단과 무관한 경로에서 나온다.
- 이는 project_1data_tradability_pivot 및 feedback_plumbing_fixed_is_not_logic_verified의 기록과 정확히 맞물린다. 한 달간 검증한 v41.1 파라미터/게이트/HPO는 실제 체결에 거의 영향을 주지 않고 있었다.

### 미확정
- `_restore_lineage_columns()` 뒤에 `execution_pool`을 다시 채우는 단계(`_overlay_stage_columns` 등)가 있는지는 호출 순서를 확인하지 않았다. 다만 결과물이 100% False이므로, 그런 단계가 있더라도 작동하지 않는다는 것은 확정이다.
- 03-17 / 04-02 두 건은 note에 `entry_source_kind`가 없어 경로 미상이다. 당시 스키마에 그 필드가 없었던 것으로 보인다.

### 검증 항목 판정
- 기능 검증 PASS. fillna 경로 재현, 베이스 CSV 컬럼 부재 확인, 23/23 단일 사유, 8건 note 추적.
- 정합성 검증 PASS. 코드 추적과 감사 도구 로직이 독립적으로 같은 결론에 도달.
- 운영 반영 검증 NA. 읽기 전용.
- 정책 검증 NA. 변경 없음.
- FAIL-CLOSED 검증 NA.
- 회귀 검증 NA.

### 다음
1. `execution_pool`을 정상 후보에 부여할지 결정. 부여하려면 `generate_candidates_v41_1.py`의 `keep_cols`에 추가하고 값을 산출하는 주체를 정해야 한다. 이는 진입 정책 변경이므로 사용자 결정이 필요하다.
2. 부여하지 않기로 한다면, v41.1 후보 파일은 매매 입력이 아니라 관찰용이라는 사실을 명시하고 관련 게이트/진단/HPO의 의미를 그 기준으로 다시 정의해야 한다.
3. `beta_harvest` / `INTRADAY_REALTIME` / `SURGE_RUNTIME` 각 경로의 비중과 성과를 별도로 측정할 필요가 있다. 현재 매매의 실체가 그쪽이다.

## 2026-08-15 (11) [START HERE] 세션 인계 - 상태 스냅샷과 다음 착수점

이 블록만 읽으면 다음 작업을 바로 시작할 수 있도록 정리한 인계 기록이다.
상세 근거는 같은 날 (1)~(10) 블록에 있다.

### A. 한 달 감사에서 확정된 사실 - 4개
1. **v41.1은 매매한 적이 없다.** 후보가 `execution_pool=False`로 진입 풀에서 100% 제거된다. 23/23 단일 사유. 코드 추적((10) 블록)과 기존 감사 도구 로직이 독립적으로 같은 결론.
2. **실제 매매는 다른 경로가 한다.** INTRADAY_REALTIME / SURGE_RUNTIME / beta_harvest / split_entry. 추적한 체결 8건 전부 우회 경로였고 v41.1 일반 경로는 0건.
3. **v41.1의 모든 성과 수치는 고장난 장치로 측정됐다.** 결함을 고칠 때마다 낮아졌다: oos_pf 1.4640(인증) -> 0.7852(유니온 KeyError 수정) -> 0.6417(생산 동등 필터).
4. **새 파라미터를 만들 수 없다.** HPO 40개 조합 중 채점 가능 0개. 창 구성상 `HPO_MIN_FOLDS=3` 도달 불가(창1 부분창, 창3은 40/40에서 n=0).

이 넷은 서로 모순되지 않는다. 하나의 그림이다 - 매매하지 않는 전략을 고장난 자로 재고 있었다.

### B. 아직 모르는 것
- v41.1 아이디어에 알파가 있는지. **유효하게 측정된 적이 없다.** "없다"가 증명된 것이 아니라 "잴 수 없었다"이다.
- 실제로 매매 중인 경로들(beta_harvest 등)이 돈을 벌고 있는지. **한 번도 안 봤다.**

### C. 현재 상태 (2026-08-15 종료 시점)
- 브랜치: `codex/alignment-quality-next`
- 커밋: `421823a4`(감사 5파일) / `8068b186`(report_backtest) / `5e983c4d`(split_policy 추적 전환)
- 미커밋: `.agent/PLANS.md`를 포함해 tracked 132개, untracked 874개. 08-15 종료 시점에도 이미 존재했음.
- **운영 파라미터 미변경**: `stable_params_v41_1.json` sha256 `4ed8011346787d3c`, as_of 2026-08-14, promoted=true. 오늘 손대지 않았다.
- 오늘 변경한 것은 전부 최적화기/게이트/후보생성 코드이며 운영 산출물은 그대로다.
- v41.1은 이미 꺼져 있는 상태다. 아무 결정을 하지 않으면 그대로 유지된다. 급한 결정 없음.

### D. 오늘 적용된 코드 변경 요약
- `optimize_params_v41_1.py`
  - `_fold_selection_metrics()`: OOS fold 전체를 selection에서 분리(split 기준), `n_folds_holdout`/`holdout_windows` 노출
  - `eval_params()`: `_scored()` 헬퍼로 sentinel fold(n<15)를 모든 집계에서 제외
  - `base_score`: OOS 계수 제거, `val_pf*1.6 + is_pf*0.8 + val_mean*35 + is_mean*17`
  - 승격 판정: 곱셈 -> 덧셈 (`PROMOTION_MARGIN_MIN_ABS=0.02`)
  - `simulate_window()`: 유니온 폴백 호출 제거, rule_e 3키 배선(`_rule_e_param()`)
  - `_enforce_v_accel_band()` 신규 (v_accel_lim >= v_accel_max 공집합 방지)
  - 설정 로더 2곳 무증상 폴백 -> `[WARN]`
- `utils/stable_params_gate.py`: `min_oos_worst_fold_pf`(0.75)/`min_oos_worst_fold_trades`(15) 추가, provenance 스탬프(advisory)
- `generate_candidates_v41_1.py`: `keep_cols`에 candidate_origin 등 4키 복원, `_select_candidates()`에 rule_e 3게이트 복원(+컬럼 누락 시 WARN)
- `strategy_core.py`: 신규. 현재 **최적화기 전용**이며 생산은 쓰지 않는다(Phase 3 롤백 상태)
- `12_Risk_Controlled/split_policy_v41_1.json`: train_end 2023-12-31 -> 2022-12-31  - 주의: `stable_params_v41_1.json`의 `meta.split_policy.train_end`는 2023-12-31 그대로(provenance 스탬프). 따라서 optimizer가 사용하는 라이브 split 경계(2022-12-31)와 `checkfile/build_runtime_evidence.py`가 읽는 동결 스탬프(2023-12-31)가 1년 다름. 두 OOS 수치를 직접 비교하면 안 됨.

### E. 열린 결정 - 2개
1. **`execution_pool`을 정상 후보에 부여할지.** 이건 버그 수정이 아니라 **v41.1을 처음으로 켜는 것**이다. 부여하려면 `generate_candidates_v41_1.py`의 `keep_cols`에 `execution_pool`을 추가하고 값을 산출할 주체를 정해야 한다. 현재 근거로는 켤 이유가 없다(위 A-3, A-4).
2. **08-14 stable을 그대로 둘지.** 버그가 인증한 파라미터지만, v41.1이 꺼져 있으므로 실매매에 영향은 없다. 대체 파라미터를 만들 경로도 없다.

### F. 다음 착수점 - 권장 1개
**실제로 매매 중인 경로의 실현 성과 측정.**
- 근거: 한 달간 매매하지 않는 전략을 검증했고, 매매하는 경로는 한 번도 보지 않았다. 여기엔 시뮬레이션이 아닌 실측 표본이 있다.
- 입력: `2_Logs/joined_trades_final_latest.csv` (36행, `pnl_pct`/`pnl_krw_net`/`exit_reason` 보유), `paper/fills.csv` (BUY 381 / SELL 626, 2025-12-24~2026-08-07)
- 방법: `paper/fills.csv`의 `note`에서 `entry_source_kind`를 추출해 경로별로 분류하고(INTRADAY_REALTIME / SURGE_RUNTIME / beta_harvest / split_entry / 구형), 경로별 실현 손익과 건수를 집계한다. note 파싱 예시는 (10) 블록에 있다.
- 특징: 새 인프라 불필요, 읽기 전용, 되돌릴 것 없음. 반나절 규모.

### G. 착수 전 반드시 알아야 할 함정 (오늘 실측으로 확인된 것)
- **측정 규약 3종 명시 필수**((8) 블록): 후보 발생일/후보 수를 인용할 때 (측정 창 / 래더 적용 여부 / 섹터 유니온 포함 여부)를 함께 적는다. 래더 적용 여부만으로 13일 <-> 159일, 6.5~12배가 갈린다.
- **후보와 체결 비교는 signal_date 기준 동일 창으로 맞춘다**((9) 블록). `fills.csv`는 후보 아카이브보다 6.5개월 길다. 이걸 안 맞춰서 "379 대 2"가 나왔고 실제로는 "35 대 2"였다.
- **`candidate_decision_outcome_ledger`로 차단률을 계산하지 않는다**((9) 블록). 158,995행 중 BUY 4건뿐인 미실현 기회 원장이다. 차단된 것만 모인 파일에서 차단률을 구하는 것은 순환이다.
- **가격 패널 zero-padding**: ad-hoc 분석 시 `close > 0` 필터 필수. 단, `generate_candidates_v41_1._load_data()`를 쓰면 무결성 계약이 이미 적용돼 추가 제거 0행이다.
- **섀도우 원장은 표본이 되지 못한다**: candidate 출처 2,670행 중 선행수익률 유효값은 28건(1%)이고, 고유 거래일이 7일뿐이다. `price_at_decision`은 D 종가보다 중앙값 2% 낮아 진입 기준도 불명확하다. 여기서 나온 수치는 쓰지 않는다.

### H. 미해결 백로그 (잃어버리지 않게)
| # | 항목 | 크기 |
|---|---|---|
| 1 | 최적화기/생산 필터 집합 불일치. Phase 3 롤백 상태이며 **해소가 아니라 미해결** | 큼 |
| 2 | `_diag_counts()`가 `v_accel_max_pass` 보고 - 생산 미적용 필터라 진단이 거짓 | 작음 |
| 3 | HPO 창 구조 재설계 (`HPO_MIN_FOLDS=3` 도달 불가) | 중간 |
| 4 | 데이터 기간 6.4년 (2020-03~), 그중 1년은 구조적으로 사망 | 큼 |
| 5 | `_compute_factors` vs `compute_factors` 통합 | 큼 |
| 6 | `ret_close_or_latest_pct` 89.8%가 0 - 사후 추적 배선 미작동 | 중간 |
| 7 | `report_backtest_v41_1.py`의 mkt_ret20 정의 불일치 (08-14 결함 2번) | 작음 |
| 8 | generate_candidates 정식 실행 회귀 (candidate_origin CSV 전파 확인) | 작음 |
| 9 | provenance `data_source_hash`가 parquet 추가 시마다 변함 - advisory라 차단은 없으나 상시 경고 | 작음 |

### I. 커밋
- 2026-08-18: 미커밋 정리 3건 커밋 완료.
  - `68e049e0` chore: remove stale temp file `tools/_tmp_check_summary.py`
  - `8266c7fb` feat(paper_engine): BEAR live recovery override and entry robustness
  - `66b97d6d` feat(pipeline): rewrite post-trade, broker integration, and daily batch orchestration
- 이 블록은 `b966e138`에 포함되어 커밋 완료. 이후 갱신 시점에 따라 새 커밋 대상.

## 2026-08-18 (1) F - 경로별 실현 성과 측정 (읽기 전용)

### 착수 선언 (AGENTS.md 23, 가드 12)
- 가드 읽음: `docs/references/STRATEGY_VALIDATION_GUARD.md` 전문
- 고정 목표: 매매 가능한 로직을 만든다(tradability pivot). v41.1 수리가 아님
- 검증 질문: 지금 실제로 체결을 만드는 경로들이 각각 돈을 벌고 있는가
- 라운드: 탐색도 확증도 아닌 **기술통계(descriptive)**. 가설 검정이 아니므로 가드 6 사전등록(축/조합/CPCV/PBO)은 범위 밖. 면제로 쓰지 않기 위해 **이 라운드 결과로 승격/정책변경/룰변경 없음**을 사전 고정함
- 방향 변경: 없음. 운영 경로 미변경(Gate/LOCK/주문/브로커/paper runtime 미접촉)

### 방법
- 입력: `paper/fills.csv` 1007행 (BUY 381 / SELL 626, 20251226~20260809)
- 링크: `SELL.note.entry_order_id` -> `BUY.order_id`. **적중 575 / miss 0 / 키없음 51**. 추정 페어링 아님
- 경로 분류는 우선순위 규칙으로 직접 구성함(시스템 제공 태그 아님):
  beta_harvest > entry_source_kind > surge마커 > entry_timing=intraday_realtime > split_entry > same_close > legacy
- 주 지표: 경로별 실현 손익. 체결 많은 날 과대반영 방지를 위해 거래별/체결일별 동일가중 병행

### 결과 (production 비용 0.358% 왕복 적용)
- **전체 실현 손익 -1,328,102원** (투입 원금 352,142,885원 대비 -0.38%, 승률 29.9%, 8.5개월)

| 경로 | n | 순손익 | ROI | 거래평균 | 승률 |
|---|---:|---:|---:|---:|---:|
| split_entry | 82 | +3,563,183 | +3.30% | +4.96% | 54.9% |
| beta_harvest | 23 | +162,540 | +2.10% | +1.98% | 52.2% |
| INTRADAY_REALTIME | 52 | -41,288 | -1.36% | -1.92% | 21.2% |
| INTRADAY(untagged) | 117 | -50,956 | -0.82% | -3.55% | 16.2% |
| SURGE_RUNTIME | 81 | -55,769 | -1.04% | +1.06% | 21.0% |
| same_close | 14 | -310,308 | -1.36% | -1.48% | 35.7% |
| SURGE(untagged) | 157 | -1,197,980 | -1.13% | -1.21% | 28.7% |
| legacy/unknown | 49 | -3,397,524 | -3.66% | -1.99% | 36.7% |

### 교란 2건을 분리 검증함
1. **기간 교란** - 경로별 활동 시기가 겹치지 않음(split_entry 4~5월, SURGE_RUNTIME 6~7월, beta_harvest 7~8월, legacy 12~4월). 5개 경로가 동시 활동한 2026-04만 떼어 보면 split_entry +4.5%(n=78) > 그달 전체 +1.7% > SURGE +0.8% > INTRADAY -3.1%. **기간만으로는 설명되지 않음**
2. **생존 편향** - legacy/unknown BUY 96건 중 **51건(53%)이 연결된 매도 없음**. 매수일 20251226~20260317이고 07-01 이후 0건이라 미청산이 아니라 **계보 단절**(당시 note에 entry_order_id 없음, 미연결 SELL 51건과 수 일치). legacy의 -3.4M은 절반만 본 숫자

### 강건성
| 집합 | n | 순손익 | ROI |
|---|---:|---:|---:|
| 전체 8경로 | 575 | -1,328,102 | -0.38% |
| legacy 제외 | 526 | +2,069,422 | +0.80% |
| legacy + split_entry 제외 | 444 | -1,493,761 | -0.99% |
| split_entry만 | 82 | +3,563,183 | +3.30% |

- 최근 시스템을 흑자로 붙들고 있는 것은 **split_entry 하나뿐**

### split_entry를 승자로 부를 수 없는 이유
- PF 2.27, 승률 54.9%, 중앙값 +0.7%, 상위 3건 제외해도 평균 +2.93%로 소수 대박은 아님
- **그러나 82건 중 78건이 2026-04 한 달에 집중**. 5월은 -4.2%(n=4). 상위 6거래가 경로 손익의 97.4%
- 가드 8.1은 "목표 레짐의 비연속 에피소드 2개 이상 재현"을 요구. 에피소드 1개이므로 판정은 **`DEFERRED_INSUFFICIENT_SAMPLE`**
- beta_harvest도 동일(n=23, 고유 청산일 5일) -> `DEFERRED_INSUFFICIENT_SAMPLE`

### 검증 질문에 대한 직답
**아니오. 돈을 벌고 있다고 말할 수 있는 경로는 하나도 없다.** 6개는 실현 손실, 흑자 2개는 단일 에피소드라 가드 기준상 DEFERRED. PLANS 08-15 (11)이 "v41.1은 매매 안 하고 다른 경로가 매매한다"까지 밝혔다면, 이번에 밝혀진 것은 **그 다른 경로들도 돈을 벌고 있지 않다**는 것이다. 따라서 "execution_pool을 부여할지"라는 열린 결정은 **비교 우위가 있는 대안이 없는 상태**에서의 결정이다.

### 검증 항목 판정
- 기능 PASS(링크 575/0/51, 4개 스크립트 실행). 정합성 PASS(기간 교란/생존 편향 분리 검증). 운영반영 NA(읽기 전용, `E:\1_Data` 변경 0건). 정책 NA. FAIL-CLOSED NA. 회귀 NA.

## 2026-08-18 (2) 비용 모델 검증 - (1)의 수치를 3.35배 과다 부과로 정정

### 배경
- (1) 최초 집계는 왕복 1.2%(편도 0.6%)를 적용했다. 근거는 `2_Logs/joined_trades_final_latest.csv`의 `pnl_krw_net`과 36/36 일치(오차 0.0000)였다.
- **그 파일이 틀린 쪽이었다.**

### 확정된 production 비용
- `paper/trades_calc.csv` **629행 전체**에서 역산: `fee+slippage = 0.001040`(편도), `sell_tax = 0.0015`. **편차 0(min=max)**
- 라이브 설정 `paper/paper_engine_config.json`과 일치: `fee_pct=0.00004`, `slippage_pct=0.001`, `sell_tax_pct=0.0015`
- `optimize_params_v41_1.py:209` `DEFAULT_FEE = 0.00358`("2026-07-23 confirmed" 주석)과도 일치
- **-> 왕복 0.358%가 production 비용이다**

### 1.2%의 출처 = 폴백 오염
- `pricing_engine.py:26` `build_cost_profile()`의 폴백 기본값 `fee_pct=0.005`(+ `slippage_pct=0.001`) = 편도 0.006
- `joined_trades_final_latest.csv`는 설정을 읽지 않고 이 폴백을 맞은 산출물이다

### 비용 모델이 3가지 이상 공존 (결함)
| 산출물 | fee | slippage | sell tax | 왕복 |
|---|---|---|---|---|
| `paper/paper_engine_config.json` (라이브 설정) | 0.4bps | 10bps | 15bps | 0.358% |
| `paper/trades_calc.csv` (엔진 원장) | fee+slip 10.4bps | | 15bps | 0.358% (설정과 일치) |
| `E:\vibe\buffett\data\ledger\paper_fills_ledger.csv` | 2bps | 3bps | 20bps | 0.25% |
| `2_Logs/joined_trades_final_latest.csv` | fee+slip 60bps | | 0 | **1.2%** |

- 같은 체결에 대해 네 곳이 서로 다른 비용을 기록한다. `trades_calc.csv`만 설정과 맞다.

### 정정 효과
| | 순손익 | ROI | 거래평균 | 승률 |
|---|---:|---:|---:|---:|
| (1) 최초 보고 (1.2% 왕복) | -4,292,811 | -1.22% | -1.34% | 26.8% |
| **정정 (0.358% 왕복)** | **-1,328,102** | **-0.38%** | **-0.50%** | **29.9%** |

- **부호가 뒤집힌 경로는 없다.** 손실 규모만 1/3로 줄었고 (1)의 결론 방향은 유지된다.
- 단, `legacy 제외` 집합은 -128,995 -> **+2,069,422(+0.80%)**로 바뀐다.

### 가장 중요한 발견 - 비용이 원인이 아니다
- **비용을 0으로 놓아도 거래당 평균 -0.14%**
- gross -0.14% -> production -0.50%. 즉 **총이익 단계에서 이미 마이너스**다. "비용 가정이 틀려서 져 보인다"는 가설은 **기각**. 진입/청산 판단 자체가 알파를 만들지 못하고 있다.

### 슬리피지 가정에 대한 별개 발견
- `paper_fills_ledger.csv`의 `slippage_actual_bps` 989건: 중앙값 **314bps(3.14%)**, 평균 511bps, 94.7%가 가정치 10bps 초과
- **단, 이것은 브로커 체결 슬리피지가 아니다.** `slippage_ref_source`가 900/989건 `signal_date_close`이므로 **신호일 종가 -> 실제 체결가의 가격 이동폭**이다.
- (1)의 수치에는 영향 없음(양쪽 다 실제 체결가 사용, 이미 반영됨).
- **백테스트에는 치명적**: 가드 2의 "신호일 종가 관측 후 다음 거래일 시가 진입" 가정과 실제 체결 시작가가 중앙값 3.14% 벌어져 있다.

### 검증하지 못한 것
- **브로커 실제 슬리피지 미측정**. `kis_daily_ccld_raw_*.csv` 90개 확인했으나 전부 시장가라 `ord_unpr=0`. 의도 가격이 없어 비교 대상 부재. AGENTS.md 24의 지정가 전환 후에야 측정 가능
- 설정값 fee 0.4bps가 KIS 실제 청구 요율과 맞는지 미확인(브로커 명세 대조 안 함)
- 매도세를 진입금액 기준으로 근사함(엔진 `compute_costs`가 세금을 수익률에서 정액 차감하는 방식을 따름). 매도대금 기준과 미세 차이

### 검증 항목 판정
- 기능 PASS(trades_calc 629행 역산 편차 0, 라이브 설정 직접 판독, 원장 989건 분포)
- **정합성 FAIL(대상 산출물 기준)** - 비용 모델이 4개 산출물에서 3가지 이상으로 갈림. `joined_trades_final_latest.csv`가 설정과 3.35배 불일치
- 운영반영 NA(읽기 전용). 정책 NA. FAIL-CLOSED NA. 회귀 NA.

### 다음 (제안 순서, 사용자 승인됨)
1. `joined_trades_final_latest.csv` 비용 결함 수정 - 폴백이 아니라 설정을 읽도록. 소비자 추적 필요(대시보드 표시값 가능성)
2. legacy 51건 계보 복구 - 코드/수량/날짜 FIFO 매칭. 전체 손익 최대 오차 요인 제거
3. 비용 모델 단일화 - 4곳이 다른 값을 쓰는 상태 자체가 위험

**주의**: 위 셋은 전부 배선 작업이다. 매매 판단에 닿는 실질 질문은 **"split_entry는 왜 +4.96%인가, 재현 가능한 구조인가"** 이며, 배선 3건이 끝나면 그쪽으로 간다.

## 2026-08-18 (3) 제안 1·2번 진행 - 비용 결함 추적과 legacy 계보 복구 (읽기 전용)

### 1번의 전제가 틀렸다 - "joined_trades가 폴백을 읽는다"가 아니다
- (2)에서 "`joined_trades_final_latest.csv`가 설정 대신 `pricing_engine.py:26` 폴백을 읽는다"고 적었으나, 추적 결과 **`pnl_krw_net`을 계산하는 라이브 코드는 존재하지 않는다.**
- `tools/signal_integration_daily.py`는 `JOINED`(=`joined_trades_latest.csv`)가 없으면 FATAL로 죽고(715-716), 있으면 읽어서 통과시킬 뿐이다. 컬럼을 만드는 주체가 없다.
- `2_Logs/joined_trades_latest.csv.bak_signal_integ_*` 백업이 **17,478개인데 전부 동일 크기(14,349 bytes)**다. 재계산 없이 실려 다니는 동결 컬럼이다.
- `trade_id` 형식도 다르다: joined/trades.csv는 `T000001`, trades_calc.csv는 `1`. **ID 조인 0/36.** `(code, entry_date, exit_date)`로는 조인된다.

### 진짜 지점 - SSOT인 trades.csv와 trades_calc.csv가 서로 다르다
동일 거래 비교 (production 비용 = 0.358% 왕복):

| 거래 | trades.csv `pnl_pct` | trades_calc `gross_ret` | trades_calc `net_ret` | 차이 |
|---|---:|---:|---:|---:|
| 090710 20251226->20251229 | -6.432% | -5.263% | -5.616% | -0.816pp |
| 094940 20251230->20251230 | -6.170% | -5.000% | -5.353% | -0.817pp |
| 143540 20251230->20260102 | +10.636% | +11.907% | +11.537% | -0.901pp |
| 004440 20260102->20260105 | -6.170% | -3.339% | -3.693% | -2.477pp |

- **`trades_calc.csv`가 권위 있는 쪽이다.** 629행 전체가 `fee_rate=4e-05`, `slippage_rate=0.001`, `sell_tax_rate=0.0015`를 **행마다 기록**하고 있고 라이브 설정과 일치한다.
- `trades.csv`(537행)의 `pnl_pct`에 함의된 (fee+slip)는 **중앙값 0.0152, 최소 0.00104, 최대 0.0689**로 단일 모델이 아니다. (2)에서 "1.2% 단일 모델"이라고 쓴 것은 joined의 36행에만 해당한다. 004440처럼 가격 기준 자체가 다른 행도 있어 전부를 비용 차이로 볼 수 없다.
- 즉 `trades.csv`는 비용 모델이 하나로 정의돼 있지 않다. 이것이 (2)에서 본 4개 산출물 불일치의 상류다.

### 잠복 결함 - paper_pnl_report.py 이중 차감 (미발현)
- `_sync_trades_calc_from_trades_for_d()` (paper_pnl_report.py:188-190):
  ```
  gross_ret = _to_float(row.get("pnl_pct"))        # trades.csv 값 = 이미 net
  net_ret = gross_ret - fee_rate - slippage_rate - sell_tax_rate
  ```
- 이미 비용이 반영된 `trades.csv.pnl_pct`를 gross로 취급해 비용을 **재차감**한다.
- **실행된 적 없음**: `trades_calc.csv` 629행 중 note에 `trades_calc_sync_source=trades.csv`가 있는 행 **0건**. 데이터 오염 없음, 코드 결함만 존재.
- 수정하지 않았다. SSOT 비용 산식 변경이라 AGENTS.md 5 적용 대상이며, 아래 권위 결정이 먼저다.

### 열린 결정 (신규) - 어느 원장이 권위인가
`trades.csv`(SSOT 체인 명시 대상)와 `trades_calc.csv`(설정 일치) 중 무엇을 손익 기준으로 삼을지 정해야 한다. 이걸 정하기 전에 joined_trades를 고치면 상류 불일치를 덮는 것이 된다. **미결.**

### 2번 완료 - legacy 51건 계보 복구 (매칭 품질 매우 높음)
- 고아 BUY 51건 / 고아 SELL 51건. 기간 20251226~20260317 / 20251229~20260318
- **고유 종목 46개가 양쪽 완전 일치. 한쪽에만 있는 종목 0개.**
- `code` + FIFO 날짜 순 매칭: **51/51 전량 성립, 미매칭 0건. 수량 동일 49/51**
- ID 조인이 아닌 휴리스틱이지만, 종목 집합이 완전 일치하고 잔여가 0인 점에서 우연 가능성은 낮다

| 복구분 | n | 순손익 | 원금 | ROI | 평균 | 승률 |
|---|---:|---:|---:|---:|---:|---:|
| legacy 복구 51건 | 51 | -1,542,473 | 97,747,864 | -1.58% | -1.69% | 29.4% |

### F 헤드라인 재정정
| | 순손익 |
|---|---:|
| (1) 최초 (1.2% 비용) | -4,292,811 |
| (2) 비용 정정 (0.358%) | -1,328,102 |
| **(3) + legacy 51건 복구** | **-2,870,575** |

- legacy/unknown 경로 최종: 100건, **-4,939,997원**
- legacy 제외 나머지 7경로: **+2,069,422원** (변동 없음 - 복구분은 전부 legacy)
- **결론 방향 불변**: 전체는 여전히 손실이고, 흑자는 split_entry 단일 에피소드에 의존한다. 손실의 대부분(-4.94M)이 legacy 구간에 몰려 있다는 그림이 더 선명해졌다.

### 검증 항목 판정
- 기능 PASS(백업 17,478개 동일크기 확인, trade_id 조인 0/36, 동일거래 4건 교차대조, sync note 0건, FIFO 51/51)
- **정합성 FAIL(대상 산출물 기준)** - `trades.csv`와 `trades_calc.csv`가 동일 거래에 다른 손익을 기록. `trades.csv`의 함의 비용이 단일값이 아님
- 운영반영 NA(읽기 전용, `E:\1_Data` 코드/데이터 변경 0건. PLANS 기록만)
- 정책 NA(변경 없음). FAIL-CLOSED NA. 회귀 NA.

### 다음
1. **[사용자 결정 필요]** 손익 권위 원장 확정: `trades.csv` vs `trades_calc.csv`. 이후에야 1번(joined_trades)과 3번(비용 단일화)이 의미를 갖는다
2. `paper_pnl_report.py:188-190` 이중 차감 수정 - 잠복이지만 실행되면 오염된다
3. 결정이 끝나면 **split_entry +4.96%의 재현 가능성**으로 이동

## 2026-08-18 (4) 손익 권위 원장 = trades_calc.csv 확정, 이중차감 수정

### 결정 (사용자)
- **손익 권위 원장 = `paper/trades_calc.csv`**. 근거: 라이브 설정과 정확히 일치, 요율을 행마다 기록(감사 가능), 629행 편차 0.

### 현황 조사 - 안전 필수 경로는 이미 trades_calc를 쓰고 있었다
| 소비자 | 읽는 원장 | 상태 |
|---|---|---|
| `paper_engine.py:623` risk_orchestration | `TRADES_CALC` 우선 | 이미 정합 |
| `p0_daily_check.py:1295` kill switch | `trades_calc` 우선 | 이미 정합 |
| `paper_pnl_report.py:107` sync | trades.csv -> trades_calc 기록 | **수정함(아래)** |
| `tools/signal_integration_daily.py:26` | trades.csv | 미조치 |
| `audit_daily.py:25` | trades.csv | 미조치 |
| `checkfile/build_runtime_evidence.py:1168` | trades.csv (기본 인자) | 미조치 |
| `paper_validate.py:9` | trades.csv | 스키마 검증용, 손익 무관 |

- 즉 **risk/kill 등 FAIL-CLOSED 계열은 이미 권위 원장을 보고 있었고, 보고·감사 계열만 trades.csv에 남아 있다.**

### 적용한 수정 - `paper_pnl_report.py` 이중 차감 (1건)
- 백업: `backup/20260818_trades_calc_authority/20260818_122427/paper_pnl_report.py` (원본 sha256 앞16 `084D3220174842E4`)
- `_sync_trades_calc_from_trades_for_d()` 187-190행:
  - before: `gross_ret = trades.csv의 pnl_pct` -> `net_ret = gross_ret - fee - slip - tax` (정액, 이중 차감)
  - after : `gross_ret = (exit_px - entry_px)/entry_px` -> `net_ret = gross_ret - ((e+x)/e)*(fee+slip) - tax`
  - 즉 `pricing_engine.compute_costs()`와 동일 산식. 가격이 불량하면 NaN을 내보내 기존 NaN 분기와 동일하게 처리
- 이 수정은 **비용 모델 단일화(3번)의 첫 조각**이기도 하다.

### 런타임 증거
1. `py_compile` PASS
2. **새 산식이 권위 원장 629행을 전량 재현**: reproduced 629 / mismatch 0 / worst_abs_err `1.110e-16`
3. **구 산식과의 차이 정량화**(596행 대조): `old_net - correct_net` mean `-2.675pp`, median `-2.989pp`, 95.6%(570/596)가 더 나쁜 값. 이중 차감이 실측으로 확인됨
4. **E2E 함수 호출**(스크래치패드 샌드박스, 운영 미접촉):
   - D=20260731 4행을 trades_calc에서 제거 -> 패치된 sync 실행 -> `appended_rows=4`, `status=PASS`
   - 기록된 4행 전부 산식 일치(4/4)
   - **제거했던 엔진 원본과 재대조: net_ret 4/4 완전 일치**, 최대 오차 `4.16e-17`
   - 운영 `paper/trades_calc.csv`는 629행 그대로(재판독 확인)

| code | entry | exit | sync net_ret | engine net_ret | diff |
|---|---|---|---:|---:|---:|
| 086790 | 20260728 | 20260731 | 0.01591190 | 0.01591190 | -1.73e-17 |
| 012330 | 20260728 | 20260731 | 0.07815309 | 0.07815309 | 2.78e-17 |
| 005380 | 20260728 | 20260731 | 0.06228549 | 0.06228549 | 1.39e-17 |
| 055550 | 20260728 | 20260731 | -0.00556010 | -0.00556010 | 4.16e-17 |

### 사소한 관찰 (메인 로직 무관)
- sync는 `entry_ts='2026-07-28 15:20:00'`, 엔진은 `'2026-07-28 00:00:00'`로 시:분:초가 다르다. `_ymd_to_ts()`의 기존 동작이며 이번 수정과 무관하다. 날짜 기준 조인에는 영향 없음.

### 1번(joined_trades) - 산출은 끝났고 코드 반영은 보류
- 조인 키 `(code, entry_date, exit_date)` -> trades_calc: **36/36 적중** (trade_id는 `T000001` vs `1`이라 0/36)
- 보정 규모: `pnl_pct` mean `+0.876pp` (median +0.817pp, min +0.768pp, max +2.477pp)
- `pnl_krw_net` 합계: **-2,470,737 -> -1,867,144 (delta +603,593)**
- **적용하지 않았다.** `tools/signal_integration_daily.py`(일일 배치)를 고쳐야 하는데, 이 스크립트는 전체 일일 입력이 있어야 돌아가므로 **오늘 E2E 증거를 만들 수 없다.** AGENTS.md 11에 따라 코드만 고치고 완료로 보고할 수 없어 보류한다. 다음 배치 실행 시점에 함께 반영할 것.

### 검증 항목 판정
- **기능 PASS** - py_compile, 629행 재현, E2E 4행 append 및 엔진 대조 일치
- **정합성 PASS** - 패치 후 sync 산출물이 권위 원장과 동일 산식/동일 값
- **운영 반영 PASS(부분)** - `paper_pnl_report.py` 실제 수정 및 원문 재확인 완료. 단 sync 경로는 아직 실행 이력 0건이므로 운영 데이터 변화는 없음
- **정책 검증 PASS** - 권위 원장 결정은 사용자 승인. 비용 산식은 라이브 설정을 그대로 사용하며 임계값/게이트 의미 변경 없음
- **FAIL-CLOSED NA** - 차단 로직 미접촉
- **회귀 검증 PASS** - 기존 629행 전량 재현으로 회귀 없음 확인

### 미검증 (정직하게)
- `signal_integration_daily.py` / `audit_daily.py` / `checkfile` 는 여전히 trades.csv를 읽는다. 보고 숫자가 권위 원장과 다르다
- 패치된 sync가 **실제 일일 배치에서** 도는 것은 확인하지 못했다(샌드박스 호출까지만). 실행 이력 0건이라 다음 결번 발생 시점에 처음 작동한다
- `trades.csv`의 함의 비용 편차(0.00104~0.0689)를 행별로 분해하지 않았다

### 다음
1. `signal_integration_daily.py`에 trades_calc 파생 배선 (다음 배치 때 E2E 검증 동반)
2. `audit_daily.py` / `checkfile/build_runtime_evidence.py` 원장 전환
3. 그 다음 **split_entry +4.96% 재현 가능성**

## 2026-08-18 (5) split_entry 재현성 검증 - 효과 없음, F의 해석을 정정

### 착수 선언 (가드 12)
- 가드 읽음(세션 2회차). 고정 목표 불변(tradability pivot)
- 검증 질문: split_entry의 +4.96%는 전략 구조인가, 2026-04라는 한 달인가
- **탐색 라운드**. 가드 5.1에 따라 승격/정책변경/성공 판정에 사용하지 않음. 운영 경로 미변경(읽기 전용)
- 설계 보정: (1)에서 split_entry를 **배타 버킷**(82건)으로 분류했으나 `split_entry=1st` 마커 보유 BUY는 실제 259건이고 177건이 surge/intraday에 흡수됐다. 이번엔 **속성(axis)** 으로 놓고 경로·월을 통제했다(가드 6.5 계층).

### A. split_entry는 특수 경로가 아니라 기본 모드였다
- 575 lot 중 **489건(85%)이 split_entry 보유**. 비보유는 86건뿐
- 등장 월이 **202604~202608에 한정**. 비보유는 대부분 202512~202603
- 즉 "split 유/무"는 사실상 **"4월 이후 / 이전"** 과 교란돼 있다

### B. 월 통제 - 효과가 뒤집힌다
양쪽 arm이 모두 존재하는 달만 유효 비교다.

| 월 | n(split) | avg | n(no) | avg | delta |
|---|---:|---:|---:|---:|---:|
| 202604 | 161 | +2.93% | 15 | -1.63% | **+4.55pp** |
| 202607 | 37 | -2.59% | 22 | +1.63% | **-4.22pp** |
| 202608 | 1 | +13.09% | 1 | +9.63% | (n=1, 무의미) |

- **부호가 반전된다.** 두 달에서 반대 방향이므로 split_entry 축은 효과로 볼 수 없다
- 경로 통제에서도 양쪽 arm이 있는 것은 legacy/other(+7.51pp)와 same_close(+3.90pp) 뿐인데, 둘 다 split arm은 4월 이후 / 비split arm은 3월 이전이라 같은 시간 교란이다

### C. (1)의 "+4.96% split_entry"의 정체
배타 버킷이 실제로 잡고 있던 것은 `path=legacy/other AND split_entry` 그룹이다.

| 항목 | 값 |
|---|---|
| n | 67 lot |
| avg / net | **+5.53% / +2,631,722원** / 승률 55.2% |
| **고유 매수일** | **6일** (20260407, 0408, 0409, 0410, 0413, 0416) |
| 매수일 범위 | **20260407 ~ 20260416 (단일 연속 구간, 최대 간격 3일)** |
| 고유 종목 | 21개 |
| 상위 6거래 P&L 비중 | **88.3%** (1위 007660 +830,697 단독 31.6%) |
| PF | 2.64 |

- 상위 5건 중 2건이 동일 종목(006360)이다
- **즉 이 결과 전체가 4월의 8거래일짜리 단일 구간, 21개 종목에서 나왔다**

### D. 4월 분해 - 그 달 자체가 좋았다
| path | n | avg | med | win% | net KRW |
|---|---:|---:|---:|---:|---:|
| legacy/other | 64 | +5.94% | +1.51% | 56.2% | +2,642,230 |
| SURGE | 62 | +1.61% | -1.21% | 41.9% | -869,771 |
| same_close | 29 | +0.54% | -0.55% | 44.8% | +621,152 |
| INTRADAY | 21 | -2.31% | -4.28% | 23.8% | -115,450 |
| **4월 전체** | **176** | **+2.54%** | -0.56% | 45.5% | +2,278,162 |

- 4월은 전체가 +2.54%인 달이었다. 해당 그룹이 그보다 나은 것은 맞으나, 중앙값 +1.51% / 상위 6건 88%라는 구조는 소수 종목의 큰 상승에 의존한다

### 판정
- 가드 6.4: 셀 표본은 거래 행 수가 아니라 **고유 신호일 수** 기준 -> **6일**
- 가드 8.1: 레짐 특화 조건은 **비연속 에피소드 2개 이상** 요구 -> **연속 1개**
- **판정 = `DEFERRED_INSUFFICIENT_SAMPLE`.** 통과도 기각도 아니며, 표본이 판정 자체를 지지하지 못한다
- 다만 B(월 통제 부호 반전)는 별개로 **split_entry 축 자체에 효과가 없다**는 음성 결과다. 이쪽은 표본 부족이 아니라 통제 실패다

### (1) 블록 정정
- (1)에 적은 "split_entry가 유일하게 건강한 경로(PF 2.27, 승률 53.7%)"는 **분류 방식이 만든 착시**다. split_entry 효과가 아니라 4월 8거래일 구간 효과였다
- (1)의 "최근 시스템을 흑자로 붙들고 있는 것은 split_entry 하나뿐"이라는 문장도 같은 이유로 무효다. 정확히는 **4월의 그 구간 하나뿐**이다

### 종합 상태 (2026-08-18 기준)
- 실현 손익 **-2,870,575원** (8.5개월, 626 lot 기준 - legacy 51건 복구 포함)
- 비용 0으로 놓아도 거래당 평균 **-0.14%** ((2) 블록)
- 통제를 걸면 **양(+)의 효과가 확인되는 경로/축이 하나도 없다**
- 유일한 흑자는 4월 8거래일 단일 구간이며 가드 기준 표본 미달
- **결론: 현재 시스템에 입증된 엣지가 없다.** "어느 경로를 키울까"가 아니라 "무엇을 근거로 새로 만들까"가 남은 질문이다

### 동결 가설 (가드 5.1 - 다음 확증 라운드 전까지 손대지 않음)
1. **청산 로직 가설**: 전체 승률 약 30%이고 exit mix가 STOP / STOP_PREEMPTIVE_CLOSE / DDM_LIQUIDATE_L4에 집중된다. 진입이 아니라 청산이 손실을 확정시키고 있을 가능성. 이번 라운드에서 검증하지 않았다
2. **4월 구간 가설**: 20260407~0416에 무엇이 달랐는지(시장 국면, 종목군). 사후 선택이므로 확증 라운드에서 사전등록 후에만 검증 가능

### 검증 항목 판정
- 기능 PASS(575 lot 축 분해, 월/경로 교차표, 6일 구간 및 집중도 산출)
- 정합성 PASS - (1)의 결론을 스스로 반증하고 정정함
- 운영 반영 NA(읽기 전용, `E:\1_Data` 코드/데이터 변경 0건, PLANS 기록만)
- 정책 NA. FAIL-CLOSED NA. 회귀 NA

### 미검증
- 4월 구간의 시장 국면/섹터 구성을 조사하지 않음
- 청산 로직 가설은 동결만 하고 검증하지 않음
- 미청산 포지션의 평가손익은 여전히 제외

## 2026-08-18 (6) 청산 로직 확증 라운드 - 주 가설 기각, 청산은 문제가 아니다

### 사전등록 (가드 6, 착수 전 고정)
| 항목 | 고정값 |
|---|---|
| 모집단 | 링크 575 + legacy FIFO 복구 51 = **626 청산 이벤트** |
| 스냅샷 | `paper/fills.csv`(mtime 20260810), `krx_daily_*_clean.parquet`(~20260813) |
| 주 지표 | **청산 후 5거래일 수익률**(청산가 대비), 단일 고정 |
| 주 가설(1개) | 청산이 조기라면 청산 후 가격이 평균 상승 -> pooled mean excess fwd > 0 |
| 기준선 | 동일 청산일 **전체 패널 동일가중** 동기간 수익률. **초과분**으로 판정 |
| 판정 | 청산일별 동일가중 초과수익 평균 > 0 **이고** 부트스트랩 95% CI가 0 배제 |
| 최소 표본 | 고유 청산일 >= 20일 |
| 다중비교 | 주 판정 = **pooled 단일 검정 1회**. exit_reason 분해는 보조 진단, 판정 불가 |
| 중첩 | 청산일별 집계로 동일 날짜 중복 가중 제거 |
| lookahead | 반사실은 미래 가격 사용. **사후 진단 전용, 신호 생성 금지** |
| 운영 경로 | 미변경(읽기 전용) |

### 가격 패널 구축
- canonical parquet 160개 중 20251201 이후 커버 154개 로드
- raw 971,138행 -> **`close > 0` 필터로 149,318행(15.4%) 제거** (주말 zero-padding 함정, 메모리 기록대로 적용)
- (date, code) 중복 제거 후 430,643행 / 거래일 172 / 종목 2,849 / 20251201~20260813
- 사용 가능 청산 이벤트 611 / 626 (가격·선행구간 없음 15건 제외), **고유 청산일 95일** (기준 20일 충족)

### 주 검정 결과 - 기각
```
mean excess (h=5)  : -2.543%
bootstrap 95% CI   : [-4.883%, -0.261%]   -> 0 배제
```
- **청산 후 5거래일간 해당 종목이 시장 대비 평균 2.54% 더 하락했다.**
- 즉 더 들고 있었으면 **더 나빴다**. 주 가설 "청산이 손실을 확정시킨다"는 **기각**이다.

### 보조 진단 (판정 아님)
| horizon | days | mean excess | 95% CI | raw(시장 미조정) |
|---|---:|---:|---|---:|
| h=1 | 95 | -1.581% | [-3.119%, -0.066%] | -1.508% |
| h=2 | 95 | -1.722% | [-3.630%, +0.082%] | -1.503% |
| **h=5** | 95 | **-2.543%** | **[-4.874%, -0.248%]** | -2.121% |
| h=10 | 91 | -3.525% | [-6.367%, -0.658%] | -2.789% |

- 보유기간이 길수록 더 나빠진다. 단조 감소이며 방향이 일관된다
- 강건성(청산가 대신 청산일 종가 기준): h5 -2.449%, h10 -3.074%. 결론 불변

### exit_reason별 (보조 진단, 다중비교 미통제 - 판정 불가)
| exit_reason | n | days | mean excess h5 |
|---|---:|---:|---:|
| STOP | 146 | 53 | -1.373% |
| FUNDAMENTAL_CRITICAL | 64 | 23 | +2.266% |
| STOP_GAP | 62 | 26 | **+7.673%** |
| STOP_PREEMPTIVE_CLOSE | 62 | 16 | +0.067% (days<20 DEFERRED) |
| SURGE_INTRADAY_REVERSAL | 51 | 18 | -3.262% (DEFERRED) |
| TP_L12 | 17 | 13 | -10.250% (DEFERRED) |
| TP_L10 | 15 | 11 | -9.005% (DEFERRED) |

- 표본 기준(>=20일)을 넘는 것은 STOP / STOP_GAP / FUNDAMENTAL_CRITICAL 3개뿐
- STOP은 옳았고(-1.37%), **STOP_GAP은 조기였을 가능성(+7.67%)** 이 보인다
- TP 계열이 크게 음수인 것은 익절 후 급락했다는 뜻으로 익절이 옳았다는 방향이지만 표본 미달

### 해석 - 병목은 진입이다
- (2): 비용 0으로 놓아도 거래당 gross **-0.14%**
- (5): 통제 후 양(+)의 효과가 확인되는 경로/축 **없음**
- (6): 청산 후에도 종목이 시장 대비 계속 하락 -> **청산은 자본을 지키고 있었다**
- 세 결과가 한 방향을 가리킨다. **시스템이 고르는 종목 자체가 지속적으로 시장을 하회한다.** 진입 선택에 알파가 없다는 뜻이며, 청산 파라미터를 조정해도 해결되지 않는다.

### 새로 동결한 가설 (다음 확증 라운드 전까지 손대지 않음)
1. **STOP_GAP 조기청산 가설**: 26일 표본에서 청산 후 +7.67%. 갭 손절만 별도 재검토 가치. 단 다중비교 미통제 상태의 발견이므로 사전등록 후 독립 표본에서 재검정해야 한다
2. **진입 역방향 가설**: 진입 종목이 시장을 지속 하회한다면 신호의 부호가 반대일 가능성. 검증하지 않았다

### 검증 항목 판정
- 기능 PASS(패널 43만행 구축, 611 이벤트, 95 청산일, 부트스트랩 10,000회)
- 정합성 PASS(청산가 기준/종가 기준 두 산식이 동일 결론, h=1/2/5/10 방향 일관)
- 운영 반영 NA(읽기 전용, `E:\1_Data` 변경 0건, PLANS 기록만)
- 정책 NA. FAIL-CLOSED NA. 회귀 NA

### 미검증
- 시장 기준선을 전종목 동일가중으로 잡았다. 섹터/규모 조정 안 함
- 미청산 포지션 제외 상태 유지
- STOP_GAP 발견은 사전등록 밖 다중비교 결과다. **현 시점 근거로 갭 손절을 바꾸면 안 된다**

## 2026-08-18 (7) 상태 재생성 RootA 원인 복구 - 신선도/하드블록/0행 주문 계약

### 진행된 것
- `paper_engine.entry`의 진입 포지션 생성부에서 `trail_pct=None`일 때 `float(None)`으로 즉시 실패하던 버그를 수정했다.
  - 백업: `E:\1_Data\backup\20260818_state_regen_root_cause\20260818_164125`
  - 수정: `E:\1_Data\paper_engine\entry.py`
  - 검증: 공식 Python `py_compile` PASS, `_resolve_entry_trail_pct(None/'') -> None`, `'-0.15' -> -0.15`
- 오래 중단된 기준 데이터를 공식 경로로 재생성했다.
  - `run_krx_refresh_and_candidates.bat`: `20260814`, `20260818` KRX clean 생성 완료
  - `prices_update_paper_incremental.py`: `ohlcv_paper.parquet date_max=20260818`
  - `freshness_check_v1.py`: `verdict=PASS`, `cand/krx_clean/prices lag=0`
- 과거 `paper_intraday_hard_blocked.flag`는 삭제하지 않고 보관 이동했다.
  - 이동: `E:\1_Data\2_Logs\cleared_hard_blocks\paper_intraday_hard_blocked.flag.cleared_20260818_164956`
- 오늘 당일 주문 없음 상태를 표현하기 위해 0행 주문 실행 계약 파일을 생성했다.
  - 생성: `E:\1_Data\paper\orders_20260818_exec.xlsx`
  - 생성 로그: `E:\1_Data\2_Logs\intraday_orders_exec_from_fills_latest.json`
  - 행 수: 0, `policy_change=false`
- `build_ssot_health_card.py`가 0행 당일 `orders_exec` 계약을 schema-valid no-order contract로 인식하도록 수정했다.
  - 백업: `E:\1_Data\backup\20260818_empty_orders_contract\20260818_170019`
  - 검증: 공식 Python `py_compile` PASS, `run_ssot_health_card.bat` PASS

### 남은 실제 문제
- 공식 전체 일일 배치 `run_paper_daily.bat`는 `paper_engine_config.lock.json` mismatch로 진입 차단된다.
  - current: `018c12719060348bfdda042afa01ef4b14f6400b21f774d150d0ca875bc300d3`
  - approved: `6938caa864a4f40910f85ee30d981780029c51aca631b50df8c3fcaeed122a2c`
  - config mtime: `2026-08-14 18:30:58`, lock mtime: `2026-07-30 13:49:54`
- lock 갱신은 현재 config를 승인하는 행위라 자동 처리 대상이 아니다.

### 판정
- 신선도 hard block과 과거 paper hard block은 원인 해결 및 검증 완료.
- 0행 no-order 계약은 RootA/RootB 계약 수정 및 health card PASS까지 검증.
- 전체 배치 재개는 config lock 승인/복구 판단이 필요하다.

## 2026-08-18 (8) F 검증 정정 + 방법론 진단 + AGENTS.md 16 경로 계약 수정

### F 실현 성과 정정
- 최초: `joined_trades_final_latest.csv`의 왕복 1.2% 비용 가정으로 **-429만 원**.
- 정정: `paper/trades_calc.csv` 역산 + `paper/paper_engine_config.json` 라이브 설정(fee 0.4bps, slippage 10bps, sell_tax 15bps -> 왕복 0.358%) 적용 시 **-132.8만 원(-0.38%)**.
- **추가 정정**: 여기에 (3)에서 복구한 legacy 계보 51건(-154.2만 원)을 더하면 **최종 -287.1만 원**이다. -132.8만은 legacy 복구 이전 수치이므로 인용 시 반드시 구분한다.
- 핵심: 비용 0 가정 시에도 거래당 평균 **-0.14%** -> 총이익 단계에서 이미 마이너스. **"비용 가정 틀림" 가설 기각.**

### 방법론 진단
- V1/V2/V3 모두 운영 로직을 정답으로 쓰지 말라고 명시한다. 그러나 **운영 시스템은 어떤 방법론도 통과한 적 없이 매매 중**이다. V1은 스스로 `승인 전략 없음 / 운영 적용: 없음`이라 적어두었다. 방법론과 시스템은 만난 적 없는 두 트랙이다.
- V3는 1인 운영에서 실행 불가능할 정도로 무겁다(2.9KB -> 9.9KB -> 14.5KB). 2026-07-20 작성 후 **한 달간 라운드 0회**. 등록표 템플릿만 있고 `feature_dictionary` / `parameter_registry` 산출물은 없다.
- 비용 계약 3중화: V1(왕복 1.2%), `paper_engine_config.json`(0.358%), `paper_fills_ledger.csv`(0.25%). V1의 "수수료 0.5%"는 실제 국내 요율(~0.015%)의 약 33배다.
- **검정력(power) 계산 없음** -> 매번 `DEFERRED_INSUFFICIENT_SAMPLE` 반복. 오늘 (6) 라운드도 95개 청산일에 부트스트랩을 돌렸다. 착수 전에 계산했다면 설계가 달라졌다.
- **가설 출처 층(Layer 0) 부재.** 세 버전 모두 "가설이 있다고 치고 엄격히 검증하라"에서 시작한다. `왜 이 전략인가`를 다루는 문서가 없다. rs 정의가 수년간 틀린 채(2026-07-28 수정) 방치된 배경이다.

### 재사용할 선례
- `2_Logs/c3_highvol_forward_observation_ledger_spec_latest.md`(2026-07-14, Track C)가 사전등록 기준에 **`ret_without_top5` 집중도 통제**를 이미 박아두었다. 오늘 (5)에서 사후에 발견한 상위 6거래 88.3% 집중을 **착수 전에 걸러냈을 기준**이다.
- 문제는 방법론이 없었던 게 아니라 **한 번 잘 만든 기준이 다음 라운드로 이어지지 않았다는 것**이다. V4는 새로 발명하지 말고 이걸 재사용한다.

### AGENTS.md 16 경로 계약 수정 (적용 완료)
- 백업: `backup/20260818_agents_md_alias_path_fix/20260818_190008/` (AGENTS.md 원본 sha256 앞16 `CB5EE63B3D8198BE`)
- 실제 위치는 `D:\검증프롬프트 세부`가 아니라 **`E:\TMP\검증프롬프트 세부`**다. `D:\` 쪽 폴더는 존재하지 않는다.
- 별칭 33개 전수 대조 결과:

| 구분 | 수 | 처리 |
|---|---:|---|
| 이름 그대로 존재 | 10 | 경로만 치환 |
| 개정본으로 리네임됨(공백->언더스코어) | 11 | 경로+파일명 치환 |
| 실제로 없음 | 12 | `<!-- 파일 없음 (2026-08-18 확인) - 복구 대기 -->` 주석 |
| 폴더에 있으나 별칭 없었음 | 1 | `가상매매_급등매매_연결상태를_검증.txt` 별칭 신설 |

- 언더스코어 파일 12종은 2026-05 개정본이다. `00_검토결과_및_변경내역.md`에 따르면 개정본은 공통 블록을 본문에서 제거하고 `공통_운영감사_기준.txt`를 SSOT로 참조한다. 따라서 **개정본 별칭 실행 시 공통 파일을 함께 읽어야 한다**는 주의를 16절 헤더에 추가했다.
- 검증: `D:\` 실참조 잔존 0건(설명 문구 1건만 남김), 별칭 33개 중 21개가 실제 파일로 해석, 미해석 12개 전부 `파일 없음` 마커 보유(누락 0건).

### 다음 행동
- V4 방법론 초안 작성: Layer 0(가설 출처 + 검정력 계산) 추가, V3 거버넌스 덜어냄, Track C `ret_without_top5` 집중도 통제 재사용, 비용 계약 단일화.
- 없는 프롬프트 12종의 복구 여부는 사용자 판단 대기.

### 검증 항목 판정
- 기능 PASS(별칭 33개 전수 대조, 수정 후 재검증 스크립트 통과)
- 정합성 PASS(리네임/부재 분류가 폴더 실측과 일치)
- 운영 반영 PASS(AGENTS.md 실제 수정 및 원문 재확인 완료)
- 정책 PASS(경로·파일명 정정이며 검증 의미·STOP 조건·FAIL-CLOSED 변경 없음)
- FAIL-CLOSED NA
- 회귀 PASS(해석 가능한 별칭이 0개 -> 21개, 손실된 별칭 없음)

### 미검증
- 개정본 12종의 **내용**은 읽지 않았다. `00_검토결과_및_변경내역.md`의 자기 기술만 확인했다
- 없는 12종이 어디로 갔는지(삭제/이동/미생성) 추적하지 않았다
- `check_paper_engine_health.py`가 무엇인지 확인하지 않았다

## 2026-08-18 (9) [방향 전환] 세션 종합 - 1년 진단과 "가장 작은 완결 루프"

이 블록이 오늘 세션의 결론이다. 다음에 재개할 때 여기부터 읽는다.

### 오늘 확정된 사실 5개
1. **v41.1은 매매한 적이 없다** (execution_pool 차단, 08-15 (10) 블록)
2. **실제 매매하는 경로들도 흑자가 아니다** — 실현 손익 **-287.1만 원** (8.5개월, legacy 복구 포함)
3. **비용 때문이 아니다** — 비용 0으로 놓아도 거래당 **-0.14%**
4. **청산 때문도 아니다** — 청산 후 5거래일 시장 대비 **-2.543%** (95 청산일, 부트스트랩 95% CI [-4.883%, -0.261%], 0 배제). 더 들고 있으면 더 나빴다
5. **진입 종목 선택에 알파가 없다** — 위 3·4가 배제되고 남는 결론

### 1년 진단 - 한 문장
**시스템이, 검증할 수 있는 속도보다 빨리 자랐다.**

근거:
- 핵심 신호 `rs`가 수년간 수학적으로 틀린 채(하락장 부호 반전) 돌았고 2026-07-28에야 잡혔다
- v41.1 후보가 진입 풀에 한 번도 도달한 적 없는데, 그 위에서 HPO 160조합과 게이트 튜닝을 했다
- `company_analyzer.py`가 최종점수의 17.5%였는데 PLANS 기록 0건, 98% 중복이었다
- 층(뉴스/LLM/펀더멘털/정크리스크/섹터/매크로/FX/레짐/급등/분할/베타하베스트)이 너무 많아 **고장이 보이지 않는 구조**였다

대비:
- 오늘 반나절에 가설 4개를 깨끗하게 결론냈다. **각각이 작았기 때문**이다 - 질문 하나, 지표 하나, 판정 하나

### 방향 전환
**"더 좋은 전략을 찾는다"가 아니라 "작동 여부를 알 수 있을 만큼 작은 것을 만든다".**

지난 1년은 더할수록 좋아진다는 전제였고, 그 결과 무엇도 확인할 수 없게 됐다. 방향을 반대로 놓는다.

### 다음 착수점 - 가장 작은 완결 루프
> **신호 1개. 전 종목. 게이트 없음. 보유기간 1개. 비용 적용. 6.4년. 숫자 1개.**

- 예: "20일 모멘텀 상위 20종목을 5일 보유" — 뉴스도 펀더멘털도 레짐도 없이
- **양수면**: 거기서부터 한 층씩 더한다. 더할 때마다 그 숫자가 올라가는지만 본다. 안 올라가면 그 층은 버린다
- **음수면**: 그 문법에는 알파가 없다. 다음 문법으로 간다
- 어느 쪽이든 **답이 나온다.** 지난 1년에 없었던 것이 그것이다
- 비용은 production 계약 **왕복 0.358%** 하나로 고정한다 (V1의 1.2%는 쓰지 않는다)
- 가격 패널은 오늘 만든 것을 재사용한다. `close > 0` 필터 필수(미적용 시 15.4% 오염)

### 방법론에 대한 판단
- **V4 문서를 먼저 쓰지 않는다.** V3(14.5KB)는 2026-07-20 작성 후 한 달간 실행 0회였다. 안 돌려본 상태로 쓴 방법론이 실행되지 않는다는 것이 이미 확인됐다
- 위 루프를 몇 번 돌려본 뒤에 방법론을 쓴다
- 쓸 때 반영할 것: **Layer 0(가설 출처 + 검정력 계산)**, Track C의 **`ret_without_top5` 집중도 통제 재사용**, 비용 계약 단일화, **작은 변경에 6개 검증항목을 요구하지 않는 비례 원칙**

### 작업 우선순위 판단 기준 (오늘 시간 소모의 교훈)
| 질문 | 답 | 처리 |
|---|---|---|
| 조용히 틀린 결과를 만드는가 | 예 | **필요** - 반드시 기록, 착수 시점은 별도 판단 |
| 지금 작업을 막는가 | 예 | **긴급** - 즉시 처리 |
| 둘 다 아닌 정리인가 | - | 목록에만 올리고 넘어감 |

- 오늘 AGENTS.md 16 별칭 수정은 **필요 O / 긴급 X**였는데 긴급으로 처리했다. 이것이 시간 소모의 원인이지, 작업 자체가 불필요했던 것이 아니다
- 커밋 위생은 `entry.py` 커밋(`8266c7fb`) 시점에 긴급성이 소멸했는데 그 뒤로도 계속했다

### 부수 작업 기록
- 스크립트 BOM: 수정된 `.bat`/`.cmd`/`.ps1` **29개 전수**를 HEAD blob과 첫 3바이트 비교 -> **MISMATCH 0건**. AGENTS.md 13 위반 소지 해소 (BOM 보유 6 / 미보유 23)
- 미커밋 정리 커밋 5건: `68e049e0` `8266c7fb` `66b97d6d` `b966e138` `40d36cd0`
- 잔여: tracked 미커밋 약 122개, untracked 874개. `virtual_ledger.csv`(운영 산출물) 추적 여부와 `holidays.json` 처리는 **미결**

### 현재 상태 - 급한 결정 없음
- v41.1은 꺼져 있고, 아무 것도 하지 않으면 그대로 유지된다
- 실계좌 손실 없음. -287.1만 원은 전부 가상매매다
- `stable_params_v41_1.json` sha256 `4ed8011346787d3c`, as_of 2026-08-14, 미변경

### 검증 항목 판정
- 기능 PASS / 정합성 PASS (오늘 (1)~(6) 라운드의 근거로 종합)
- 운영 반영 NA (이 블록은 방향 기록이며 코드/데이터 변경 없음)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 참조
- RootB 관련 발견(`paper_fills_ledger.csv` 비용 컬럼 불일치)은 `E:\vibe\buffett\PLANS.md` 2026-08-18 블록에 별도 기록했다

## 2026-08-18 (10) 가장 작은 완결 루프 1회차 - 20일 모멘텀 NOT_SUPPORTED

(9)에서 정한 방향의 첫 실행이다. **1년 만에 처음으로 깨끗하게 해석되는 숫자 하나를 얻었다.**

### 사전등록 (착수 전 고정, 가드 12)
| 항목 | 고정값 |
|---|---|
| 질문 | 가장 단순한 모멘텀 문법에 비용 차감 후 알파가 있는가 |
| 라운드 | 탐색. 결과로 승격/정책변경 없음. 운영 경로 미변경(읽기 전용) |
| 모집단 | KOSPI/KOSDAQ 전 종목, 거래대금 >= 1억원 (매매가능성 하한, 튜닝 아님) |
| 신호 | 20거래일 모멘텀 1개. **게이트 없음** |
| 선별 | 상위 20종목 동일가중 |
| 진입/청산 | 신호일 t 종가 관측 -> t+1 종가 진입 -> 5거래일 보유 -> t+6 종가 청산 |
| 비용 | production 계약 **왕복 0.358%** (V1의 1.2%는 쓰지 않음) |
| 기준선 | 같은 날 동일 적격 모집단 전체 동일가중, 동일 비용 |
| 주 지표 | 일별 바스켓 비용차감 순수익의 기준선 대비 초과분, 부트스트랩 95% CI |
| 기간 | 2020-01 ~ 2026-08 |

### 결과 - NEGATIVE
```
signal days       : 1,601   (20200131 ~ 20260807)
median universe   : 797 종목/일
mean excess       : -0.4772%   per 5-day holding period
bootstrap 95% CI  : [-0.7195%, -0.2236%]   -> 0 배제
naive annualized  : -24.05%
win rate          : 42.7%      info ratio(annual) = -0.669
```
- **20일 모멘텀 상위 20종목은 시장 동일가중 대비 확실히 열등하다.** 우연이 아니다.

### 강건성 3종 - 전부 버팀
1. **집중도 통제**(Track C `ret_without_top5` 선례 재사용): 상위 5일 제거 후 **-0.5436%**, CI [-0.7859%, -0.2982%]. 소수 날짜 의존이 아니며 오히려 악화된다. (5)에서 split_entry로 속았던 함정을 이번엔 사전에 걸렀다.
2. **연도별 7년 중 7년 전부 음수**

| 연도 | 초과 | 바스켓 | 기준선 |
|---|---:|---:|---:|
| 2020 | -0.0715% | +0.5446% | +0.6161% |
| 2021 | -0.2133% | -0.2007% | +0.0126% |
| 2022 | -0.7517% | -1.5042% | -0.7525% |
| 2023 | -0.0236% | -0.1965% | -0.1729% |
| 2024 | -1.2295% | -1.6131% | -0.3837% |
| 2025 | -0.4161% | -0.3450% | +0.0712% |
| 2026 | -0.7030% | -1.0846% | -0.3816% |

   - 레짐 문제가 아니다. **상승장(2020, 2025)에서도 시장에 진다.**
3. **검정력 충족**: sd 5.062%, n=1601 -> MDE **0.2479%/기간(연 12.50%)**. 관측 효과가 **MDE의 1.92배**. `DEFERRED_INSUFFICIENT_SAMPLE`이 아니라 **판정 가능한 표본**이었다. 지난 1년 내내 없던 조건이다.

### 부호 반전 가설도 여기서 죽는다
- 하위 20종목(모멘텀 최하위): **-0.3055%**, CI [-0.4663%, -0.1386%]. 지는 쪽도 진다.
- 따라서 (6)에서 동결했던 **"진입 역방향 가설"은 기각**이다. 20일 모멘텀은 방향이 틀린 것이 아니라 **극단 자체가 나쁘다**. 중간이 이긴다.

### 판정
- **`NOT_SUPPORTED`** (V1/V3 상태 용어 기준). 표본 부족이 아니라 충분한 표본에서 기준 미달이다.

### 도중에 잡은 데이터 결함 2건
1. **`change_rate`가 2025년 100%(667,208행), 2026년 99.3%(374,660행) 결측이다.** 2020~2024 아카이브에만 채워져 있고 이후 수집분에는 없다. 1차 실행에서 최근 1.7년이 통째로 빠져 20241220에서 끊겼다. **종가 기반 복원으로 6.4년 전체를 회복**했고(복원 1,039,909행), KRX 상하한을 넘는 896행은 데이터 오류로 제외했다.
   - 향후 ad-hoc 연구에서 `change_rate`를 그대로 쓰면 최근 구간이 조용히 사라진다. **복원 폴백 필수.**
2. **집중도 통제 초기 구현 오류(자체 수정)**: 1차에서 바스켓 20종목 중 상위 5개를 실현수익 기준으로 제거해 -6.27%가 나왔다. 그건 구조상 항상 음수이며 Track C 원안과 다르다. 원안은 **전체 표본에서 최대 기여분 제거**다. 수정 후 재실행했다.

### 이 루프가 지난 1년과 다른 점
| | 지난 1년 | 이번 |
|---|---|---|
| 결론 | "잴 수 없었다"(DEFERRED 반복) | **잴 수 있었고 답이 나왔다** |
| 표본 | 매번 부족 | 1,601 신호일, MDE의 1.92배 |
| 강건성 | 사후에 착시 발견 | 사전등록 통제가 통과 |
| 소요 | 수개월 | **약 1시간** |

### 다음 - 한 번에 한 축만
이 문법은 죽었다. 같은 틀로 축 하나씩 바꾼다. 매번 이 숫자 하나로 판정한다.
1. **평균회귀 방향** (최우선). 상위도 하위도 지고 중간이 이긴다는 신호가 나왔으므로 여기가 다음 후보다
2. 보유기간 (5일 -> 1 / 20 / 60일)
3. 룩백 (20일 -> 60 / 120일)
4. 문법 교체 (모멘텀 -> 돌파 / 변동성 / 거래량)

- 조합·게이트는 단일 축에서 양수가 나온 뒤에만 더한다(가드 6.5 계층)

### 검증 항목 판정
- 기능 PASS (패널 2,198,626행, 1,626 거래일, 1,601 신호일, 부트스트랩 10,000회)
- 정합성 PASS (연도별 7/7 동일 방향, 집중도 통제 통과, 상/하위 양방향 확인)
- 운영 반영 NA (읽기 전용, `E:\1_Data` 변경 0건, PLANS 기록만)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 진입을 t+1 **종가**로 근사했다. 가드 2의 "다음 거래일 **시가**" 계약과 다르다. 분할 오염 회피를 위해 수익률을 `change_rate` 복리로 계산했기 때문이며, 시가 진입 버전은 별도 검증이 필요하다
- 거래대금 1억원 하한 외 유동성·상장·거래정지 처리를 하지 않았다
- 미체결·시장충격·용량 제약을 반영하지 않았다(비용은 고정 0.358%만)
- 재현 스크립트는 스크래치패드에 있고 저장소에 넣지 않았다
