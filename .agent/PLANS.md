

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

## 2026-08-18 (11) 당일 수정분 커밋 3건

브랜치 `codex/alignment-quality-next`. RootB 변경 없음(마지막 RootB 기록은 같은 날 `paper_fills_ledger.csv` 비용 컬럼 불일치 블록).

| 커밋 | 내용 | 규모 |
|---|---|---|
| `60a5def0` | `feat(pnl)`: trades_calc sync 추가 + 비용 이중차감 수정 | +900 / -107 |
| `dd3a0328` | `docs(agents)`: AGENTS.md 추적 시작 + 16절 경로 `E:\TMP` 정정 | +275 (신규 추적) |
| `ae5a3d05` | `docs(plans)`: 2026-08-18 연구 세션 기록 (블록 1~10) | +659 |

### 커밋 시 확인한 사항 2건
1. **`paper_pnl_report.py`는 단독 수정이 아니다.** +900행 중 이번 비용 수정은 17행이고 나머지 883행은 2026-08-07~08-10에 쌓인 미커밋분이다. 다만 수정 대상인 `_sync_trades_calc_from_trades_for_d()` 함수 자체가 그 미커밋분(+484행)이라 **커밋된 적 없는 코드를 고친 것**이다. 하나로 묶되 커밋 메시지에 경위를 남겼다.
2. **`AGENTS.md`는 추적 대상이 아니었다.** 이 저장소가 따르는 운영 계약인데 한 번도 커밋된 적이 없었다. 이번에 추적을 시작했다. 되돌리려면 `git rm --cached AGENTS.md`.

### 남은 것
- `tools/AGENTS.md` untracked (2026-04-17자, 이번 작업과 무관하여 손대지 않음)
- tracked 미커밋 **122개**, untracked **874개** — 당일 이전부터 누적된 것으로 그대로 둠
- 푸시하지 않음

### 검증 항목 판정
- 기능 PASS(커밋 3건 해시 확인, 대상 3파일 모두 working tree clean)
- 정합성 PASS(커밋 메시지가 실제 변경 규모·경위와 일치)
- 운영 반영 PASS(git log 및 status 재확인)
- 정책 **주의** — `AGENTS.md` 버전 관리 편입은 추적 범위 변경이다. 내용 변경은 16절 경로/파일명 정정뿐이며 검증 의미·STOP 조건·D 규칙·FAIL-CLOSED는 불변
- FAIL-CLOSED NA
- 회귀 PASS(운영 파라미터·산출물 미변경, `stable_params_v41_1.json` 손대지 않음)

## 2026-08-19 (1) 루프 라운드 2~3, 구현 검증, 모집단 결함 발견과 수정

08-18 (10)의 후속. **결론 방향은 유지되나 크기는 폐기·대체됐다.** 읽는 순서: 결함 -> 수정 -> 재실행 결과.

### 경위
- 사용자가 라운드 2(lb120 h5), 라운드 3(lb120 h20 / h60)을 직접 실행해 결과를 전달했다.
- 사용자 문제 제기: **"로직 구현이 잘못된 상태에서 거기에 검증을 계속 맞추면 그 자체가 맞는가."**
- 이 지적을 `research_loop.py` 자신에게 적용해 구현을 검증했고, **데이터 모집단에서 결함이 나왔다.**

### A. 구현 검증 - 시점 정렬은 PASS
합성 데이터로 세 후보 창을 구분 테스트했다.
```
fwd[t]            : -0.0691499721
window t  ..t+4   : -0.0474340144
window t+1..t+5   : -0.0249420650
window t+2..t+6   : -0.0691499721  <-- 일치
```
- 신호일 종가 관측 -> 하루 뒤 진입 계약대로 작동. **미래정보 유입 없음.**
- 신호도 `prod(t-19..t)`와 일치. 룩백에 미래정보 없음.
- **1차 테스트는 무효였다**: 합성 데이터를 종목별 상수 수익률로 만들어 세 창이 같은 값을 냈다. 구분 불가능한 테스트였고 난수로 재작성했다. 검증 스크립트도 검증이 필요하다는 사례.

### B. 모집단 결함 - 2020~2024 아카이브는 KOSPI 전용이다
```
2020: 932 codes -> KOSPI 880 / KOSDAQ 0
2021: 946 -> KOSPI 908 / KOSDAQ 0
2022: 952 -> KOSPI 918 / KOSDAQ 0
2023: 964 -> KOSPI 939 / KOSDAQ 0
2024: 968 -> KOSPI 955 / KOSDAQ 0
2025: 2,884 / 2026: 2,823   <- KOSDAQ 유입으로 3배
```
- 역방향 확인: 오늘의 KOSDAQ 1,859종목 중 2023 파일 존재 **0건(0.0%)**. KOSPI는 984 중 939(95.4%) 존재.
- `market` 컬럼이 2020~2024 전체 `<NA>`이고, 2025-08 이후 파일부터 KOSPI/KOSDAQ이 채워진다.
- 현재 수집기 `krx_update_clean_incremental.py:61`은 `MARKETS = ["KOSPI","KOSDAQ"]`로 두 시장을 본다. **과거 아카이브만 KOSPI 전용이고 지금 수집은 정상이다.**
- 수집기는 `stock.get_market_ticker_list(latest_ymd, market=mkt)`(L558/L580)로 명부를 만든다. pykrx가 날짜 인자를 받으므로 **과거 KOSDAQ 재수집은 기술적으로 가능하다.**

### C. 앞선 자체 진술 정정 - 생존편향은 오판이었다
- 이전에 "2020년 종목의 93.7%가 2026년까지 생존 -> 비현실적, 현재 상장 명부로 소급 생성 의심"이라고 적었다. **틀렸다.**
- KOSPI 대형주 기준 6년 생존율 93.7%는 정상 범위다. unmapped 종목이 52(2020)->13(2024)으로 감소하는 패턴은 **상폐된 KOSPI 종목이 실제로 데이터에 남아 있다**는 신호다.
- **진짜 결함은 생존편향이 아니라 시장 커버리지 불연속이었다.**

### D. 수정(B안) - 모집단을 KOSPI로 통일
- `tools/research_loop.py`에 `--market {all,kospi,kosdaq}` 추가.
- 매핑 설계 주의: "오늘 KOSPI인 종목"으로 거르면 **상폐 KOSPI 종목이 빠져 새 생존편향이 생긴다.** 그래서
  `KOSPI = (2025년 이전 KOSPI 전용 아카이브에 등장한 모든 종목) ∪ (market 컬럼이 KOSPI인 종목)`, 명시적 KOSDAQ 라벨이 우선.
- 결과 모집단: `2020:932 2021:946 2022:952 2023:964 2024:968 2025:990 2026:979` -> **전 기간 930~990으로 연속.**

### E. 재실행 결과 - 혼합 모집단이 효과를 약 40% 부풀리고 있었다
| 라운드 | 혼합 | KOSPI 통일 | MDE배수(혼합->KOSPI) |
|---|---:|---:|---|
| lb20 h5 | -0.4772% | **-0.4447%** | 1.88 -> 1.88 |
| lb120 h5 | -0.4331% | **-0.2444%** | 1.88 -> **1.05** |
| lb120 h20 | -1.6481% | **-0.9928%** | 3.82 -> 2.12 |
| lb120 h60 | -3.3658% | **-2.1277%** | 4.51 -> 2.68 |

연환산 초과수익: lb120 h5 -21.83% -> **-12.32%** / h20 -20.77% -> **-12.51%** / h60 -14.14% -> **-8.94%**

- 이전 결론의 상당 부분은 **"KOSDAQ 고모멘텀 소형주가 나쁘다"**였고, 그것이 KOSPI 5년치와 섞여 전체를 끌어내렸다.
- **`lb120 h5`는 사실상 판정 불가에 가깝다**: MDE 배수 1.05x, CI 상단 -0.0086%. "확실한 음수"라고 부르면 과하다.

### F. 새로 보이는 것 - 절대손실이 아니라 상대열등이다
| 보유 | 바스켓 net | 기준선 net |
|---:|---:|---:|
| 5일 | -0.3478% | -0.1034% |
| 20일 | -0.4893% | +0.5035% |
| 60일 | **-0.2621%** | +1.8656% |

- 60일 보유에서 바스켓은 연환산 약 -1.1%로 **거의 본전**이다. 지는 이유는 기준선(+1.87%/60일)이 좋아서지 바스켓이 무너져서가 아니다.
- 혼합 모집단에서 봤던 "고모멘텀 바스켓 연 -6~-13% 절대 손실"은 **KOSDAQ 유입이 만든 그림이었다.**

### 판정
- **확정**: KOSPI에서 20일·120일 모멘텀 상위 20종목은 동일가중 시장을 하회한다. 룩백·보유기간·집중도 통제에 걸쳐 일관.
- **폐기**: 08-18 (10) 및 라운드 2~3의 혼합 모집단 수치(-14~-24%p). **KOSPI 기준 -8.9~-12.5%p로 대체한다.**
- **미검증**: KOSDAQ 단독. 2025~ 1.7년뿐이라 검정력 부족 가능성이 크고, 착수 전 MDE 계산이 필요하다.

### 08-18 (10) 블록에 대한 정정
(10)의 방법론·정렬·집중도 통제는 유효하다. 그러나 `-0.4772%`가 붙은 모집단 설명("KOSPI/KOSDAQ 전 종목")이 **사실과 다르다.** 실제로는 78%가 KOSPI 전용 구간이었다. (10)의 수치는 `lb20 h5` 한정으로는 KOSPI 재실행값(-0.4447%)과 거의 같아 결론이 바뀌지 않으나, **모집단 서술은 정정되어야 한다.**

### 검증 항목 판정
- 기능 PASS(정렬 3창 구분 테스트, 모집단 연도별 대조, 역방향 KOSDAQ 0건 확인, 4개 라운드 재실행)
- 정합성 **FAIL(대상 데이터 기준)** - 가격 아카이브의 시장 커버리지가 2025년에 불연속. 수집기 자체는 정상
- 운영 반영 NA(읽기 전용. `tools/research_loop.py`만 신규/수정, 운영 경로 미접촉)
- 정책 NA / FAIL-CLOSED NA
- 회귀 PASS(KOSPI 재실행에서 2020~2024 연도별 수치가 이전과 완전 동일 - 그 구간은 원래 KOSPI 전용이었으므로 기대와 일치)

### 미검증
- 2020~2024 KOSDAQ 재수집(A안)은 하지 않았다. pykrx 1,900종목 x 5년 호출이 필요하다
- `UNKNOWN` 44종목은 제외했다. 정체를 확인하지 않았다
- KOSPI 기준선 CAGR의 현실성을 외부 지수와 대조하지 않았다
- 분위 단조성(V3 3.1)은 아직 보지 않았다. top/bottom 두 점만 봤다

### 다음
1. **분위 단조성** - 신호가 유효한데 부호만 반대인지, 극단만 나쁜 U자인지, 무정보인지 구분. top/bottom 두 점으로는 판별 불가
2. KOSDAQ 단독 검정력 사전 계산 -> 가능하면 실행, 불가하면 `DEFERRED`
3. A안(과거 KOSDAQ 재수집) 착수 여부 판단

## 2026-08-19 (2) 분위 단조성, 그리고 겹침 보정이 지금까지의 모든 CI를 무너뜨림

**이 블록의 핵심은 발견이 아니라 정정이다. 지금까지 기록한 모든 신뢰구간이 너무 좁았다.**

### A. 결함 - `--deciles`가 선언만 되고 구현이 없었다
- 사용자가 발견. `L284`에 인자가 있으나 `run()` 어디에도 분기가 없었다.
- 조용히 무시되어 평범한 top-20 출력이 나온다. **받는 사람은 분위 분석을 했다고 믿는다.** 필요+긴급 둘 다 해당해 즉시 구현했다.
- 구현 후 기존 모드 회귀 확인: `lb20 h5 kospi` = -0.4447%, MDE 1.88x로 불변.

### B. 분위 결과 (KOSPI, lb120, h20, 10분위, 1,486 신호일)
| 분위 | 초과/기간 | 연환산 | 블록 CI 0배제 |
|---:|---:|---:|:---:|
| 1 (최저) | -0.2201% | -2.77% | |
| 2 | -0.0936% | -1.18% | |
| 3 | -0.0060% | -0.08% | |
| 4 | +0.1162% | +1.46% | |
| 5 | +0.1029% | +1.30% | |
| 6 | +0.2630% | +3.31% | **O** |
| 7 | +0.1669% | +2.10% | |
| 8 | +0.0748% | +0.94% | |
| 9 | +0.0033% | +0.04% | |
| 10 (최고) | -0.4082% | -5.14% | |

- 점추정만 보면 U자다: 극단 2개 평균 -0.3141% vs 중간 8개 +0.0784%, 차이 -0.3926%. spearman(분위, 초과) = +0.127로 단조성 없음.
- **그러나 블록 CI에서는 10개 중 1개(6분위)만 0을 배제한다.** 95% 수준에서 10회 검정이면 기대 오탐이 0.5개다. **6분위는 증거가 아니다.**

### C. 확증 시도 - 사전 검정력 계산 후 실행
동결 가설: `KOSPI 120일 모멘텀 4~7분위 동일가중 보유가 시장 동일가중을 초과한다`

착수 전 검정력(관측 효과 +0.1622%/20d, 일별 sd 1.0215% 가정):
| 설계 | n | MDE | 효과/MDE | 판정 |
|---|---:|---:|---:|---|
| KOSPI 2020-2023 | 847 | 0.0688% | 2.36x | 실행 가능 |
| KOSPI 2024-2026 홀드아웃 | 499 | 0.0896% | 1.81x | 실행 가능 |
| KOSDAQ 2025+ (미관측 모집단) | 254 | 0.1256% | 1.29x | 실행 가능 |

실행 결과(iid 기준)는 세 설계 모두 DETECTED였다: +0.1593% / +0.1777% / +0.3370%.

### D. 그런데 겹침을 보정하니 전부 무너진다
보유 20일에 매일 신호를 내면 인접 관측이 창의 19/20을 공유한다. **유효 독립표본은 대략 n/hold다.**

| 설계 | 명목 n | 유효 n | 효과 | iid CI | **블록 CI** | 0배제 |
|---|---:|---:|---:|---|---|:---:|
| KOSPI 전체 | 1,486 | ~74 | +0.1622% | [+0.1096, +0.2134] | [+0.0028, +0.3402] | 겨우 |
| KOSPI 2020-23 | 847 | ~42 | +0.1593% | [+0.0971, +0.2222] | **[-0.0102, +0.3914]** | X |
| KOSPI 2024-26 | 499 | ~25 | +0.1777% | [+0.0689, +0.2843] | **[-0.2219, +0.4840]** | X |
| KOSDAQ 2025+ | 254 | ~13 | +0.3370% | [+0.1519, +0.5258] | **[-0.2030, +0.9574]** | X |

- **어느 확증 설계도 0을 배제하지 못한다.** 전체 풀링만 겨우 살아남고 하한 +0.0028%는 사실상 0이다.
- iid 부트스트랩이 CI 폭을 약 3~4배 과소평가하고 있었다.
- 판정: 중간대 가설은 **`DEFERRED_INSUFFICIENT_SAMPLE`**. 기각이 아니라 **판정 불가**다.

### E. 이 결함은 지금까지의 모든 라운드에 소급 적용된다
- 08-18 (10), 08-19 (1)의 CI는 전부 iid 기준이며 **동일하게 과소평가돼 있다.**
- 특히 `lb120 h5`(MDE 1.05x)는 블록 기준으로 확실히 판정 불가다.
- 라운드 1~3의 "확실한 음수" 표현은 **약화되어야 한다.** 방향(음수)은 유지되나 유의성 주장은 재계산이 필요하다.

### F. 하네스 수정 (적용 완료)
- `block_bootstrap_ci()` 추가. moving-block, block = hold.
- 단일 바스켓 모드: iid와 BLOCK을 함께 출력하고 **판정은 BLOCK 기준**. iid만 유의하면 `UNDECIDED -- block CI spans zero (iid CI would have said otherwise)`로 명시.
- 분위 모드: 분위별 `*` 표시를 블록 CI 기준으로 변경. rank IC의 CI도 블록으로 변경.
- `effective indep. n` 출력 추가.

### G. 그래도 살아남은 것 하나
- **daily rank IC = -0.04082, 블록 CI [-0.06424, -0.01721], 0 배제.**
- 즉 신호에 횡단면 정보는 **있다.** 다만 크기가 작고(|IC| 0.04), 분위 구조가 단조가 아니며, 어떤 분위 바스켓도 블록 CI에서 살아남지 못한다.
- 정보가 있다는 것과 매매 가능한 구조가 있다는 것은 다르다. 현재는 전자만 확인됐다.

### 검증 항목 판정
- 기능 PASS(분위 구현, 회귀 확인, 검정력 사전계산, iid/블록 대조)
- 정합성 **FAIL(자체 이전 산출물 기준)** - 이전 라운드의 CI가 겹침 미보정으로 과소평가됨. 방향은 유지, 유의성 주장은 무효
- 운영 반영 NA(읽기 전용, `tools/research_loop.py`만 수정)
- 정책 NA / FAIL-CLOSED NA
- 회귀 PASS(단일 바스켓 모드 결과 불변: -0.4447%, MDE 1.88x)

### 미검증
- `--deciles`가 언제부터 미구현 상태였는지 추적하지 않았다
- 블록 길이를 hold로 고정했다. 최적 블록 길이 선택(예: 자기상관 기반)은 하지 않았다
- 라운드 1~3을 블록 CI로 전면 재계산하지 않았다. 다음 실행 시 자동 적용된다
- CPCV/PBO(가드 6.3)는 여전히 미적용. 블록 부트스트랩은 그 대체물이 아니다

### 다음 - 확증 라운드 설계
겹침 보정 후 유효 표본이 74/42/25/13로 드러났으므로, **현재 데이터로는 연 2~4% 크기의 효과를 확증할 수 없다.** 설계를 바꿔야 한다.
1. **비중첩 표본 사용**: 신호일을 hold 간격으로만 추출(20일마다 1회). 유효 n은 같지만 추론이 정직해지고 CPCV 적용이 가능해진다
2. **효과 크기 하한 사전 등록**: 유효 n=74에서 검출 가능한 최소 효과를 먼저 계산하고, 그보다 작은 목표는 착수하지 않는다
3. **과거 KOSDAQ 재수집(A안)**: 유효 표본을 늘리는 유일한 실질 수단. 1,900종목 x 5년
4. rank IC(-0.04)는 살아남았으므로 **바스켓이 아니라 IC 기반 설계**를 검토할 여지가 있다

## 2026-08-19 (3) IC 안정성 확증 라운드 - NOT_CONFIRMED, 신호가 2024년부터 소멸

(2)의 4번 후속. **모멘텀 축은 세 층 모두 실패로 닫혔다.**

### 설계 변경 (사용자 제안)
- (2)에서 rank IC를 확증 주 지표로 쓰자고 했으나, 당시 IC는 `--deciles` 출력의 **부차 통계**였다.
- 사용자 지적: 확증 주 지표로 쓰려면 전용 모드와 서식이 필요하다. 부차 통계를 재사용하면 판정 기준이 안 맞는다.
- `--mode {auto,basket,deciles,ic}` 추가. `auto`는 기존 동작 유지(`--deciles` 지정 시 deciles, 아니면 basket)로 하위호환.
- 회귀 확인: basket 모드 -0.4447% 불변, deciles 모드 출력 불변.

### 사전등록 통과 규칙 (결과 보기 전 고정)
- 모든 구간이 최소 유효 표본 `min_eff_n=20` 도달
- 모든 구간의 **블록** CI가 0 배제
- 모든 구간의 부호 일치
- 하나라도 미달이면 `NOT_CONFIRMED`, 표본이 원인이면 `DEFERRED_INSUFFICIENT_SAMPLE`

### 결과 - NOT_CONFIRMED
```
FULL PERIOD        n=1486  eff=74  IC=-0.04082  block95=[-0.06430, -0.01719]  OK
seg1 202006~202306 n= 743  eff=37  IC=-0.05703  block95=[-0.09504, -0.02813]  OK
seg2 202306~202607 n= 743  eff=37  IC=-0.02460  block95=[-0.05197, +0.00949]  spans0
```
- 전반기 통과, **후반기 0 배제 실패.** 사전등록 규칙 미달.

### 연도별 진단 - 소멸 패턴
| 연도 | IC | 블록 CI | 0배제 |
|---|---:|---|:---:|
| 2020 | -0.07545 | [-0.17869, -0.01805] | O |
| 2021 | -0.07645 | [-0.11779, -0.04411] | O |
| 2022 | -0.06075 | [-0.12878, +0.01615] | |
| 2023 | -0.05493 | [-0.10872, +0.01012] | |
| **2024** | **-0.00723** | [-0.04187, +0.02805] | |
| **2025** | **+0.00413** | [-0.05157, +0.05104] | |
| 2026 | -0.02116 | [-0.11367, +0.08248] | |

- 2020~2021 강함(-0.076) -> 2024~2026 사실상 0. 2025는 부호까지 반전(크기는 무의미).
- **"원래 없었다"가 아니라 "있었는데 사라졌다"의 모양이다.**
- 구분 불가한 두 해석:
  1. 알파 감쇠(알려진 현상)
  2. 2020~2021이 예외 국면이었고 그때만 강했다 - COVID 폭락 후 급반등은 모멘텀 역전이 구조적으로 강한 시기다. **이쪽이 유력하다.**

### 종합 판정 - 모멘텀 축 종료
| 층 (V3 3장) | 결과 |
|---|---|
| 전략 실행 (top-20 바스켓) | 음수, 단 블록 CI로는 판정 불가 |
| 후보 선별 (분위 구조) | 10개 중 1개만 생존 = 우연 수준 |
| **신호 유효성 (IC)** | **NOT_CONFIRMED, 최근 구간 소멸** |

- 가드 3.1 기준 **신호 유효성 층을 통과하지 못했으므로 후보 선별 층으로 진행할 수 없다.**
- 120일 모멘텀은 KOSPI에서 매매 가능한 신호가 아니다.

### 도구가 정상 작동하고 있다는 기록
오늘 하루 이 하네스가 **자체 결론을 세 번 반증했다.**
1. 혼합 모집단 -> KOSPI 통일: 효과 40% 축소
2. iid -> 블록 부트스트랩: 확증 전부 무너짐
3. IC 전체 -> 구간 분할: 최근 소멸 발견

세 번 모두 "찾았다"고 기록한 **뒤에** 나온 반증이다. 사용자의 `--deciles` 미구현 지적과 구현 검증 요구도 같은 성격이었다. **낙관이 계속 깎이는 것이 정상 작동이다.**

### 사소한 결함 (수정 완료)
- by-year 진단에서 `Index == y`가 ndarray를 반환해 `.to_numpy()` 호출 실패. 주 결과 출력 후 발생했고 판정에는 영향 없었다. 수정함.

### 검증 항목 판정
- 기능 PASS(전용 모드 구현, 두 기존 모드 회귀 확인, 사전등록 규칙대로 판정)
- 정합성 PASS(FULL PERIOD가 (2)의 IC 값과 일치: -0.04082)
- 운영 반영 NA(읽기 전용, `tools/research_loop.py`만 수정)
- 정책 NA / FAIL-CLOSED NA / 회귀 PASS

### 미검증
- 2020~2021 제외 시 다른 문법이 작동하는지 보지 않았다
- 알파 감쇠와 국면 특수성을 구분할 방법을 설계하지 않았다
- KOSDAQ IC는 돌리지 않았다(유효 n 13으로 판정 불가 예상)
- CPCV/PBO는 여전히 미적용

### 다음 - 세 후보 (미착수)
1. **다른 문법으로 이동** - 돌파 / 변동성 / 거래량. 모멘텀 축은 닫힘
2. **A안 KOSDAQ 과거 재수집** - 유효 n 74 -> 약 150. 다른 문법을 위한 인프라 투자
3. **2022~ 구간 한정 재검증** - 최근 국면에서 작동하는 문법 탐색. 다만 유효 n이 절반이 되어 검출 하한이 오르므로 **착수 전 MDE 계산 필수**

## 2026-08-19 (4) 다른 세션의 2022+ 재검증 결과 교차검증 - 판정 동의, 검정력 수치 정정

다른 세션이 (3)의 후보 3번(2022~ 한정 재검증)을 실행하고 `--start-date` / `--end-date`를 추가했다.
본 블록은 그 결과에 대한 **읽기 전용 교차검증**이다. 코드/PLANS 외 산출물 변경 없음.
**결함 수정은 해당 세션에 위임했다(사용자 지시).**

### A. 구현 검증 - `--start-date`는 정확하다
- `tools/research_loop.py:400-405`. 필터가 `signal`/`forward` 롤링 계산 **전에** 패널에 적용된다.
- 미래정보 유입 없음, 구간 이음새 오염 없음. **구현 자체는 PASS.**

### B. 판정 동의
| 층 | 다른 세션 판정 | 교차검증 |
|---|---|---|
| 신호 유효성(IC) | `NOT_CONFIRMED` (3구간 모두 0 배제 실패) | 동의 |
| 전략 실행(바스켓) | `DEFERRED_INSUFFICIENT_SAMPLE` | 동의 |
| "2020-2021 빼면 신호 사실상 없음" | 확정 | 동의. 연도별 표가 지지한다 |

### C. 정정 1 - MDE가 4.47배 낙관적이다 (원인은 이쪽 세션이 만든 결함)
- `tools/research_loop.py:517`: `mde = 1.96 * sd / np.sqrt(len(excess))` -> **명목 n 사용.**
- 08-19 (2)에서 블록 부트스트랩을 도입할 때 **검정력 계산을 함께 고치지 않았다.** 같은 출력 안에서 CI는 블록, 검정력은 iid로 서로 모순된다.

| 항목 | 값 |
|---|---:|
| 보고된 MDE (명목 n=990) | 0.6049%/기간 = **연 7.62%** |
| 유효 n=49 기준 MDE | 2.7052%/기간 = **연 34.09%** |
| 비율 | **4.47배** |
| 교차확인: 같은 출력의 블록 CI 반폭 | **2.1522%** -> 유효 n 쪽과 같은 크기 |

- **CI와 검정력 계산이 모순이며 믿을 것은 CI 쪽이다.**
- 08-18~08-19에 기록된 모든 `MDE` / `effect/MDE` 배수가 동일하게 낙관적이다. 인용 시 4.47배 보정이 필요하다.

### D. 정정 2 - 보유기간을 늘려도 검출 하한이 내려가지 않는다
다른 세션은 `60일 전환 시 MDE 연 4.5%`로 적었으나, 유효 n 기준으로 계산하면 반대다.

| 보유 | 유효 n | MDE/기간 | MDE 연환산 |
|---:|---:|---:|---:|
| 5일 | 200 | 0.67% | **33.9%** |
| 20일 | 50 | 2.69% | **33.9%** |
| 60일 | 16 | 8.24% | **34.6%** |
| 120일 | 8 | 16.48% | **34.6%** |

- 보유를 늘리면 **유효 관측이 줄어드는 속도와 연환산 배수가 줄어드는 속도가 같다.** 벽이 약 34%에서 움직이지 않는다.

### E. 이 블록의 가장 중요한 결과 - 바스켓 확증은 이 표본에서 원천 불가능하다
- 약 4년치 KOSPI + top-20 바스켓으로는 **연 30% 미만 효과를 어떤 보유기간에서도 확증할 수 없다.**
- 연 30% 알파는 현실적으로 존재하지 않으므로, **앞으로 바스켓 라운드는 전부 `DEFERRED`로 끝난다.**
- IC 라운드가 (3)에서 유일하게 판정을 낼 수 있었던 이유가 이것이다. IC는 매일 약 780종목을 쓰고 바스켓은 20종목이라 분산이 근본적으로 다르다.
- **운영 규약**: 신호 검증은 **IC로 하고**, 바스켓은 IC가 통과한 뒤 **효과 크기를 재는 용도로만** 쓴다. 바스켓으로 신호 유무를 판정하려 하지 않는다.

### F. 사소한 관찰 (결론 무영향)
- `--start-date 20220101`이 패널을 먼저 자르므로 첫 신호일이 `20220629`가 된다. 2022년 1~6월 신호일이 버려진다.
- **미래정보 문제가 아니다.** 2022년 1월 신호를 2021년 데이터로 계산하는 것은 과거 데이터 사용이며 정당하다. 설계 낭비다.
- 필터를 신호 계산 후 행 선택 단계로 옮기면 n이 990 -> 약 1,110이 된다. 우선순위 낮음.

### 위임 사항 (다른 세션)
1. `research_loop.py:517` 검정력을 유효 n(`len(excess) // hold`) 기준으로 수정. `observed |effect| / MDE`와 `DEFERRED` 판정도 함께. 분위/IC 모드에 같은 계산이 있으면 동시 확인
2. (선택) 날짜 필터를 신호 계산 후로 이동
3. `--start-date` 변경분이 **미커밋 상태**다. 위 수정과 함께 커밋 권장

### 검증 항목 판정
- 기능 PASS(필터 적용 지점 코드 확인, MDE 재계산, 블록 CI 반폭과 교차확인)
- 정합성 **FAIL(자체 도구 기준)** - 같은 출력 안에서 CI와 검정력이 서로 다른 독립성 가정을 쓴다
- 운영 반영 NA(읽기 전용. 결함 수정은 위임)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 다른 세션의 IC/바스켓 수치를 직접 재실행해 재현하지 않았다. 보고된 출력값의 내적 일관성만 검사했다
- 분위 모드·IC 모드의 검정력 계산 경로를 코드로 확인하지 않았다(단일 바스켓 경로만 확인)
- 보유기간별 sd 추정에 `sd ∝ sqrt(horizon)` 근사를 썼다. 실측이 아니다

## 2026-08-19 AI 퀀트 애널리스트 개인보유 Watchlist 로컬 연결

### 완료
- `tools/ai_quant_chatbot.py`에 `config/manual_holdings.json` 로컬 매칭 경로를 추가했다.
- 개인보유 종목이 후보 CSV에 없을 때는 외부 AI API 호출 없이 로컬 답변을 반환한다.
- 답변에 후보 생성 메타(`candidates_latest_meta.json`), KRX 관리/주의 스냅샷, DART 재무 스냅샷을 붙여 판단 재료를 보강했다.
- 개인보유 응답에 `후보 편입`, `매수 승인`, `적정주가`, `금일 매수적정가`, `금일 매도적정가` 필드를 추가했다.
- 매수 승인은 항상 후보/주문 승인과 분리해 `승인 아님`으로 표시하고, 금일 가격은 자동 주문 기준이 아닌 참고가로 표시한다.
- `classify_intent()` 기반 라우터를 추가해 질문 의도별 응답을 분기한다.
- 후보 CSV 외부 AI 프롬프트의 "종목 질문은 항상 상세 리포트" 규칙을 제거하고 질문 의도에 맞는 항목만 답하도록 바꿨다.
- broad intent 로컬 응답을 추가했다: `오늘 매수가능종목`, `단기/중기/장기 매수 후보`, `강세 섹터`, `현재 시장상황`, `신규매수`, `추가매수`.
- broad intent는 외부 AI API 호출 전에 로컬 산출물로 반환한다.
- 주문/매수 승인(`candidate_action_queue_latest.csv`)과 관찰 후보(`with_final_score.csv`), 기간별 read-only 후보(`future_signal_preview`), 섹터 스코어, 실시간 상승 스냅샷을 분리 표시한다.
- `TRADABLE` 또는 `trading_allowed=True`가 없으면 매수 가능 종목은 0개로 답하고, 관찰 후보는 `매수 승인 아님`으로 표시한다.
- 관찰 후보/섹터/기간별 데이터가 현재일과 불일치하면 참고용으로 표시한다.
- 후보 선정/점수/주문 로직은 변경하지 않았다.

### 검증
- 문법: `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\ai_quant_chatbot.py` PASS.
- 실행: `삼성전자 어때?` 입력 시 `success=true` PASS.
- 결과물: 응답에 `개인보유 Watchlist`, `후보 CSV에는 현재 포함되어 있지 않습니다`, `후보 편입: 미편입`, `매수 승인: 승인 아님`, `적정주가: 판정 보류`, `금일 매수적정가`, `금일 매도적정가`, DART 성장/마진, `외부 AI API 호출 없이` 포함 확인.
- 의도 분기: `삼성전자 어때?`는 요약, `왜 후보 아님?`은 게이트/탈락, `매수가?`는 매수 참고가, `오늘 팔까?`는 매도/보유 판단, `전체 분석`은 전체 리포트로 분기 확인.
- broad intent 실행: 사용자가 제시한 6개 질문 모두 `success=true` 로컬 응답 확인.
- broad intent 결과: 매수 승인 가능 종목 0개, 관찰 후보 TOP10 별도 표시, 기간별 후보는 `future_signal` read-only 및 기대수익률 음수 경고 표시, 섹터는 행 기준일 `2026-08-12` 현재일 불일치 표시, 시장상황은 대시보드 `FAIL`과 후보 최종 통과 0개 표시 확인.
- diff 확인: 백업본 `E:\1_Data\backup\ai_quant_broad_market_intents_20260819_120814\ai_quant_chatbot.py.bak` 대비 broad intent 배선/표시 변경만 확인.

### 남은 문제
- 개인보유 종목의 RS/RSI/뉴스점수 같은 후보 상세 지표는 생성하지 않는다. 적정주가는 검증된 가치평가 모델이 없으면 숫자 산출하지 않고 보류한다.
- 현재 broad intent는 매매 로직 자체를 열지 않는다. 실매수 가능 종목은 주문 승인 산출물 기준 0개이며, 관찰 후보 일부는 행 기준일이 현재일과 불일치한다.

## 2026-08-19 시스템 헬스 FAIL / RootA 배치 차단 원인
- 범위: RootA 배치 상태, config lock, 통합 운영 스냅샷 원인 진단.
- 백업: `E:\1_Data\backup\20260819_system_health_core_fail\20260819_115422`, `E:\1_Data\backup\20260819_plans_update\20260819_115737`.
- 확인: `run_paper_daily.bat`는 2026-08-19 08:30:10에 `[0/14] snapshot+hash+lockcheck paper_engine_config` 단계에서 `ERRORLEVEL=1`로 중단됨.
- 확인: `paper_engine_config.lock.json` 승인 sha는 `6938caa864a4f40910f85ee30d981780029c51aca631b50df8c3fcaeed122a2c`, 현재 `paper_engine_config.json` sha는 `018c12719060348bfdda042afa01ef4b14f6400b21f774d150d0ca875bc300d3`로 불일치.
- 차이: 승인본 `paper_engine_config.json.bak.20260813` 대비 현재 설정은 `sell_rules.stop_loss.trailing_stop_activation_profit_pct` 12→20, `sell_rules.stop_loss.trailing_stop_pct` -10→-15, `sell_rules.take_profit.levels` [10,20,40]→[20,50,100].
- 영향: config lock 차단으로 p0/gate 최신 산출물이 20260809 기준에 남고, final_score는 20260812 기준으로 남아 `integrated_ops_snapshot_latest.json`의 effective blocker가 유지됨.
- 보류: 위 3개 설정은 매매 정책값이므로 현재 설정을 승인해 lock 재초기화할지, 승인본으로 복구할지 명시 승인 필요.
- 다음: 승인 후 `run_paper_daily.bat` 재실행, `validation_scope_audit_latest.json` 및 `integrated_ops_snapshot_latest.json` 재생성, RootB 대시보드 상태 재생성.

## 2026-08-19 (5) 3문법 IC 교차검증 - 판정 동의, "레짐 의존" 해석 기각, 분할 오염 수정

다른 세션이 (4)의 위임 사항을 반영하고 2022+ KOSPI에서 momentum / breakout / bollinger 세 문법의 IC를 테스트했다. 본 블록은 그에 대한 교차검증과 **분할 오염 수정**이다.

### A. 위임 사항 2건 모두 정확히 반영됨
- `mde = 1.96*sd/sqrt(eff_n)`, `eff_n = len(excess) // hold`. **(4)의 결함 1 해소.**
- `--signal-start-date` / `--signal-end-date`가 신호 **계산 후** 결과 행을 거른다. basket/deciles/ic 세 모드 전부 적용. **(4)의 결함 2 해소.**
  - 효과 확인: 같은 조건에서 신호일이 **990 -> 1,109**로 증가((4)에서 예측한 약 1,110과 일치).

### B. 세 문법 `NOT_CONFIRMED` 판정 동의
사전등록 통과 규칙(모든 구간 0 배제 + 부호 일치) 미달이 맞다.

### C. 정정 - "시점/레짐 의존이 강하다"는 근거가 없다
연도별 IC 표에서 소멸/부활 패턴을 읽었으나, **연도간 변동이 노이즈 기댓값보다 작다.**

| 신호 | 연도간 sd | 노이즈만의 기대 sd | 비율 | 판정 |
|---|---:|---:|---:|---|
| momentum | 0.02876 | 0.04304 | **0.67x** | 상수 IC와 일치 |
| breakout | 0.03235 | 0.04796 | **0.67x** | 상수 IC와 일치 |
| bollinger | 0.02895 | 0.03943 | **0.73x** | 상수 IC와 일치 |

- 연 유효 n이 12뿐이라 연도별 IC의 95% 오차범위가 `+-0.077 ~ +-0.086`이다. 보고된 연도별 값이 전부 그 안에 든다.
- 연도별 15칸 중 0을 배제하는 것은 **bollinger 2022(-0.0789) 하나뿐**이다.
- **"momentum 소멸 / breakout 부활"은 읽어낼 수 없다.** 각 문법의 IC가 기간 내내 상수였다는 가설과 데이터가 완전히 부합한다.
- 08-19 (3)에 적은 momentum의 연도별 소멸 서술도 같은 기준으로 **약화되어야 한다.** 당시엔 2020~2021을 포함해 폭이 더 컸으나, 연 단위 오차범위를 감안하면 "소멸"을 단정할 근거는 약하다.

### D. 다중비교 미통제
- 보고 표는 3 문법 x (full + seg1 + seg2) = **9회 검정**. 95%에서 기대 오탐 0.45개.
- **통과한 것은 breakout seg2 하나 = 정확히 1개.** 증거로 쓸 수 없다.
- 연도별 표 15칸은 무보정이 추가된다.

### E. 신규 결함 발견 및 수정 - level 문법이 분할 미조정 종가를 사용
- `compute_signal()`의 breakout/bollinger가 원시 `close`를 썼다. momentum은 수익률 기반이라 무관.
- 패널의 close는 액면분할 미조정이므로, 룩백 창 안에 분할이 있으면 **하루치 수익률이 아니라 가격 레벨이 깨진다.** `|r|>31%` 필터는 수익률만 제거하고 레벨 단절은 못 고친다.

측정(KOSPI):
```
분할성 레벨 점프          : 303건 / 175 종목
120일 창 오염 셀          : 15,828 / 1,092,572 = 1.45%
breakout 최하위 분위 오염 : 10.16%  (기저율 1.45%의 7.01배)
```
- 분할 종목이 **체계적으로 극단 저분위로 밀려난다.** 무작위 노이즈가 아니다.

**수정**: `adjusted_price(rets)` 신설. `(1+ret).cumprod()`로 분할 조정 시계열을 재구성하고 원계열 결측 위치를 다시 마스킹한다. breakout(비율)과 bollinger(z-score)는 스케일 불변이라 시작 레벨이 임의여도 무관하다.

**수정 검증**:
```
분할성 레벨 점프  원시 : 303  ->  조정 후 : 0
breakout  최하위 분위 오염 10.16%(7.01x) -> 3.87%(3.32x)
bollinger 최하위 분위 오염  2.08%(1.44x) -> 1.33%(1.14x)
```
- 조정 시계열에 점프가 0건이므로 breakout에 남은 3.32배는 인공물이 아니라 **분할 종목이 실제로 저조한 경향**이다.
- **IC와 판정은 불변**: momentum -0.02882 / breakout +0.03845 / bollinger -0.03939, 셋 다 `NOT_CONFIRMED`.
- 즉 이 결함은 **IC 결론을 왜곡하지 않았다.** 다만 `--direction bottom`으로 level 문법 바스켓을 돌리면 픽의 10%가 분할 아티팩트였을 것이므로, 바스켓 사용 전 수정이 필요했다.

### F. 다음 선택지에 대한 판단
- **1번(룩백/보유 변화)에 반대.** 연 유효 n=12, 전체 49~55에서 IC 95% 오차범위가 `+-0.077`이다. 관측 IC(0.02~0.04)가 그 절반이라 **문법을 더 시도해도 계속 `NOT_CONFIRMED`가 나온다.** 다중비교만 쌓인다(이미 9회 + 15칸).
- **4번(가격 문법 종료)은 이르다.** 세 문법이 기각된 것이 아니라 **판정을 못 한 것**이다. `NOT_CONFIRMED`를 "가격 문법은 안 된다"로 읽고 비가격 축으로 가면 같은 표본 벽에 다시 부딪힌다.
- **2번(유효 표본 확대)이 선행되어야 한다.** 과거 KOSDAQ 재수집(종목 3배) 또는 2020~2021 포함(유효 n 49->74).

### 검증 항목 판정
- 기능 PASS(위임 2건 코드 확인, 분산 분해, 분할 오염 측정 및 수정 전후 대조, 세 문법 재실행)
- 정합성 PASS(수정 후 판정 3건 모두 불변, momentum 구조 불변)
- 운영 반영 PASS(`tools/research_loop.py` 수정 및 py_compile PASS, 재실행으로 확인)
- 정책 NA / FAIL-CLOSED NA
- 회귀 PASS(momentum 구조 불변, 분할 조정은 level 문법에만 적용)

### 미검증
- 다른 세션의 원 수치를 그대로 재현하지 않았다. 수정 후 값으로 재실행했다
- breakout에 남은 3.32배가 실제 현상인지 별도 검증하지 않았다
- 다중비교 보정(Bonferroni/FDR)을 적용한 재판정은 하지 않았다
- volume 기반 문법은 패널에 volume이 없어 시도하지 않았다

## 2026-08-19 (6) KOSDAQ 공백을 로컬 자료로 메움 - 재수집 없이 종목 2.6배, 정밀도 -18.5%

(5)의 결론 "유효 표본 확대가 선행되어야 한다"의 후속. **pykrx 재수집 없이 로컬에 이미 있던 파일로 해결했다.**

### A. pykrx 재수집 타당성 조사 결과 - 부분 불가
착수 전 확인 항목(사용자 제시)에 대한 실측:

| 항목 | 결과 |
|---|---|
| 호출 제한/차단 | **없음.** 5종목 x 5년 = 0.26초, 종목당 0.05초. 1,859종목 투영 약 1.6분 |
| 5년 일괄 vs 1년 분할 | **일괄이 맞다.** 종목당 0.05초라 분할 이득 없고 호출만 5배 |
| `get_market_ohlcv_by_date(code, adjusted=)` | **작동.** `등락률`(change_rate) 포함 반환 |
| `get_market_ticker_list(ymd, market)` | **n=0 반환.** 2회 재시도 동일 |
| `get_market_ohlcv_by_ticker(ymd, market)` | **빈 DataFrame** |
| `get_market_cap_by_ticker` | **KeyError** |

- pykrx 1.2.4에서 **횡단면 엔드포인트가 전부 죽어 있고 시계열만 산다.** KRX 응답 스키마 변경으로 보인다.
- 즉 **과거 시점 종목 명부를 pykrx에서 얻을 수 없다.** 오늘 명부를 쓰면 2020~2024 KOSDAQ이 생존자 전용이 되어 재수집 목적이 훼손된다. 이 시점에서 A안은 보류됐다.

### B. 로컬 자료 탐색 - 결정적 파일 발견
사용자가 "상폐 종목을 직접 다운받은 기억이 있다"고 하여 탐색한 결과:

**가져오는 자료 (출처 고정)**
```
path   : E:\1_Data\Raw\krx_daily_20221001_20251224.parquet
size   : 39,868,887 bytes
mtime  : 2025-12-28 11:01:48
sha256 : 1b843cca5c99f771 (앞 16자리)
rows   : 3,158,510
컬럼   : code, date, market, open, high, low, close, volume, value
기간   : 20221001 ~ 20251224 (거래일 1,181)
종목   : 2,976  (KOSDAQ 1,993 / KOSPI 990)
```

**왜 이 파일이 결정적인가 - 생존편향이 없다**

| 연도 | 이 파일의 종목 | 그중 "오늘도 상장된 KOSDAQ" |
|---|---:|---:|
| 2022 | 2,574 | 1,493 |
| 2023 | 2,704 | 1,616 |
| 2024 | 2,798 | 1,741 |
| 2025 | 2,861 | 1,847 |

- 2022년 KOSDAQ 1,993종목 중 오늘 명부에 남은 것은 1,493뿐. **상폐된 약 500종목이 이 파일에 보존돼 있다.** pykrx 오늘 명부로는 절대 얻을 수 없는 데이터다.

**이 파일의 한계 (명시)**
- `change_rate` 컬럼 **없음** -> 종가 비율로 수익률을 만들어야 하고, 액면분할 오염이 들어온다. (5)에서 만든 `adjusted_price()`와 `|r|>31%` 필터가 이를 처리한다.
- `close=0` 주말 패딩 있음 -> `close > 0` 필터 필수(기존 하네스에 이미 적용).
- 2022-10 이전은 없음.

### C. 통합 방식 (재현 가능하도록 규칙 명시)
`tools/research_loop.py`:
1. `RAW_WIDE` 상수 신설. 로드 목록의 **맨 앞**에 둔다.
2. 중복 제거 규칙 변경: `_has_cr = change_rate.notna()`로 정렬 후 `keep="last"`. **change_rate를 가진 행(기존 아카이브)이 겹치는 구간에서 이긴다.** RAW_WIDE는 보완용이지 대체용이 아니다.
3. `_market_map()` 수정: 기존 "2025년 이전 wide 파일 = KOSPI 전용" 휴리스틱이 RAW_WIDE에는 **적용되면 안 된다**(RAW_WIDE도 2025년 이전이지만 KOSDAQ을 담고 있다). RAW_WIDE는 명시 제외하고, 나머지도 `labelled.empty`일 때만 휴리스틱을 쓰도록 좁혔다.

### D. 통합 후 커버리지 - 연속성 확보
| 연도 | KOSDAQ | KOSPI |
|---|---:|---:|
| 2020 | 0 | 932 |
| 2021 | 0 | 946 |
| **2022** | **1,632** | 951 |
| 2023 | 1,744 | 960 |
| 2024 | 1,836 | 961 |
| 2025 | 1,901 | 983 |
| 2026 | 1,872 | 972 |

- 패널 총계: 5,924,329 raw행 -> `close>0`으로 23.8% 제거 -> 거래일 1,626 / 종목 3,063 (KOSDAQ 2,030 / KOSPI 1,032 / UNKNOWN 1)
- **전 시장 실행은 2022-10 + 룩백 이후로 제한해야 한다.** 그 이전은 여전히 KOSPI 전용이다.

### E. 정밀도 이득 - 측정됨
동일 창(20221001~), 동일 신호(momentum lb120 h20), 모집단만 교체:

| 모집단 | 일평균 종목 | IC | 일별 IC sd | SE(IC) | MDE(IC) |
|---|---:|---:|---:|---:|---:|
| KOSPI | 681 | -0.02120 | 0.12832 | 0.02029 | 0.03977 |
| **전 시장** | **1,771** | -0.02242 | **0.10461** | **0.01654** | **0.03242** |

- 일별 IC sd **-18.5%**, SE **-18.5%**, 검출 하한 0.0398 -> **0.0324**
- 이론값(sqrt(681/1771)=0.62배, -38%)에 못 미치는 이유는 KOSDAQ 종목 간 상관이 높아 유효 자유도가 종목 수만큼 늘지 않기 때문이다. **그래도 실질 이득이다.**

### F. 그래도 아직 판정 불가
```
관측 |IC| / MDE = 0.0224 / 0.0324 = 0.69x   -> 여전히 검출 하한 아래
```
- 유효 n이 **40**이다. KOSPI 전용 2022+ 구간(eff 55)보다 오히려 적다. 창이 2022-10부터라 짧아졌다.
- **남은 병목은 종목 수가 아니라 기간이다.**

### G. 다음 - 목표가 좁혀졌다
- 필요한 것은 **2020-01 ~ 2022-09 KOSDAQ 2.75년**뿐이다.
- 채우면 유효 n 40 -> 약 70, MDE(IC) 0.0324 -> 약 0.024. 관측 IC(-0.022)와 거의 맞닿는다.
- **명부 문제도 풀렸다**: RAW_WIDE의 2022년 KOSDAQ 1,632종목을 명부로 쓰면 된다. pykrx 오늘 명부(1,859, 생존자)보다 2020~2022 구간에 적합하다.
- 수집 자체는 종목당 0.05초 x 1,632 = **약 1.4분**으로 끝난다.

### 검증 항목 판정
- 기능 PASS(pykrx 6개 엔드포인트 실측, RAW_WIDE 내용 검증, 패널 재구성, 동일창 모집단 교체 비교)
- 정합성 PASS(커버리지 연도별 연속성 확인, 중복 제거 규칙으로 change_rate 보존)
- 운영 반영 PASS(`tools/research_loop.py` 수정, py_compile PASS, 패널 재생성 확인)
- 정책 NA / FAIL-CLOSED NA
- 회귀 **PASS** - 아래 H항에서 8건 전수 재현 확인. KOSPI 코드 집합은 1,039 -> 1,032로 변동(RAW_WIDE의 명시 라벨이 휴리스틱을 덮어씀)했으나 판정은 전부 불변

### H. 회귀 검증 - 새 패널 캐시에서 이전 수치 전수 재현
패널 캐시가 바뀌었으므로 08-18~08-19에 기록한 KOSPI 수치를 새 캐시로 재실행해 대조했다.

| 실행 | 기록값 | 새 캐시 | 차이 | 판정 |
|---|---:|---:|---:|---|
| basket lb20 h5 | -0.4447% | -0.4501% | -0.0054pp | 불변 |
| basket lb120 h20 | -0.9928% | -1.0253% | -0.0325pp | 불변 |
| IC lb120 h20 full | -0.04082 | -0.04035 | +0.00047 | 불변 |
| IC seg1 | -0.05703 | -0.05709 | -0.00006 | OK 불변 |
| IC seg2 | -0.02460 | -0.02361 | +0.00099 | spans0 불변 |
| IC 2022+ momentum | -0.02882 | -0.02819 | +0.00063 | 불변 |
| IC 2022+ breakout | +0.03845 | +0.03908 | +0.00063 | 불변 |
| IC 2022+ bollinger | -0.03939 | -0.03866 | +0.00073 | 불변 |

- **신호일 수는 전부 정확히 동일**(1601 / 1486 / 1109). 날짜 축은 불변이고 종목 집합 7개 차이의 영향만 나타났다.
- IC 최대 편차 0.001, 바스켓 최대 편차 0.033pp. **판정 8건 전부 불변.**

**부수 발견**: `basket lb120 h20 KOSPI`의 블록 CI가 `[-2.7021%, +0.5640%]`로 **0을 포함한다.** 08-19 (1)에 이 설정을 기록할 당시에는 블록 부트스트랩이 없었고 iid CI `[-1.4537%, -0.5185%]`만 있었다. (2)에서 소급 경고한 "이전 CI 과소평가"의 구체적 사례다.
-> **08-19 (1)의 바스켓 "음수" 서술은 `UNDECIDED`로 읽어야 한다.**

### 미검증
- RAW_WIDE의 분할 오염률을 재측정하지 않았다. `change_rate`가 없어 종가 비율 기반이라 (5)에서 KOSPI로 측정한 1.45%보다 높을 수 있다
- KOSPI 코드가 1,039 -> 1,032로 7개 줄어든 원인을 개별 확인하지 않았다
- 2022-10 이전 KOSDAQ은 여전히 없다
- 세 문법 전체를 새 패널로 재실행하지 않았다(momentum만 확인)
## 2026-08-19 integrated_ops final_score stale 체인 원인 해결

- 범위: RootA 후보 생성/최종점수 원천 체인.
- 원인: `generate_candidates_v41_1.py`가 현재 기준 후보 0개일 때 `candidates_latest_meta.json`과 versioned CSV만 20260818로 갱신하고, `candidates_latest_data.csv`는 20260812 데이터를 유지했다. 그 결과 `final_score_merge_daily.py`가 오래된 후보를 다시 읽어 `final_score_merge_status_latest.json.asof_ymd=20260812`를 생성했고, `integrated_ops_snapshot_latest.json`에서 `chain_ref=20260819, stale=final_score:20260812`로 FAIL 처리됐다.
- 조치: 후보 0개일 때도 `candidates_latest_data.csv`를 현재 기준의 schema-valid 빈 CSV로 갱신하도록 수정했다. 또한 기술 후보가 빈 상태이면 `final_score_merge_daily.py`가 뉴스 전용 행을 운영 최종점수 후보로 붙이지 않고 `reason=base_candidates_empty`로 남기도록 수정했다.
- 검증:
  - 문법: `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\generate_candidates_v41_1.py`, `E:\1_Data\tools\final_score_merge_daily.py` PASS.
  - 실행: `generate_candidates_v41_1.py` 재실행 결과 `candidates_latest_data.csv` rows=0, `candidates_latest_meta.json.latest_date=2026-08-18`, `chosen_level=NONE`.
  - 실행: `tools\final_score_merge_daily.py` 재실행 결과 `input=candidates_latest_data.csv`, rows=0, `final_score_merge_status_latest.json.asof_ymd=20260818`, `news_candidates.reason=base_candidates_empty`.
  - 결과물: `tools\build_integrated_ops_snapshot.py` 재실행 결과 `stale_latest_chain` 항목 제거. 남은 blocking 이슈는 pending 없음, BT 체크리스트 증거 부족, runtime guard WARN으로 분리됨.
- 백업:
  - 코드 백업: `E:\1_Data\backup\20260819_integrated_ops_final_score_stale\20260819_123219`
  - PLANS 백업: `E:\1_Data\backup\20260819_integrated_ops_final_score_stale\20260819_133736`

## 2026-08-19 (7) KOSDAQ 2020~2022 백필 완료 - 검출 하한 최초 돌파, bollinger CONFIRMED(단 매매 불가)

(6)의 "남은 병목은 기간"을 해소했다. **오늘 처음으로 관측 효과가 검출 하한을 넘었고, 문법 하나가 사전등록 규칙을 통과했다. 다만 정밀 검토에서 매매 가능성은 부정됐다.**

### A. 수집한 자료 (출처 고정)
```
생성 도구 : tools/fetch_kosdaq_backfill.py  (신규)
출력      : krx_daily_archive/krx_daily_20200102_20220930_kosdaq_clean.parquet
size      : 25,530,042 bytes
sha256    : bcb659160d00fe8c (앞 16자리)
rows      : 994,069
종목      : 1,588  (요청 1,632 중 44개는 응답 없음)
기간      : 20200102 ~ 20220930 (거래일 680)
소요      : 122초, 실패 0건
컬럼      : date, code, market(=KOSDAQ), open, high, low, close, volume, value, change_rate
```

**명부 출처**: RAW_WIDE의 2022년 KOSDAQ 멤버십 1,632종목. pykrx `get_market_ticker_list`가 0행을 반환해 과거 명부를 얻을 수 없고, 오늘 명부를 쓰면 생존자 전용이 된다. 2022년 명부는 대상 창과 동시대이며 이후 상폐된 종목을 포함한다.

**`value` 근사 (명시)**: 종목별 엔드포인트가 `거래대금`을 반환하지 않아 `종가 x 거래량`으로 계산했다. RAW_WIDE에서 둘 다 있는 KOSDAQ 1,300,680행으로 검증:
- 비율 중앙값 **1.0003**, p05 0.9769, p95 1.0182
- 1e8 유동성 하한 통과 여부 **99.888% 일치**, 적격 집합 **+0.035%** -> 안전

**남은 편향 (명시)**: 2020-01~2022-09 사이에 상장하고 그 안에 상폐된 종목은 2022년 명부에 없어 복구 불가다. 해당 창은 여전히 미세한 생존편향이 있다.

### B. 커버리지 - 2020년부터 연속 확보
| 연도 | KOSDAQ | KOSPI |
|---|---:|---:|
| 2020 | **1,430** | 932 |
| 2021 | **1,521** | 946 |
| 2022 | 1,632 | 951 |
| 2023 | 1,744 | 960 |
| 2024 | 1,836 | 961 |
| 2025 | 1,901 | 983 |
| 2026 | 1,872 | 972 |

- 3배 점프 소멸. 일평균 적격 종목 1,850~2,172로 전 기간 안정.
- 부수 이득: `change_rate` 보유 행 **1,156,758 -> 2,150,592**. 백필이 `등락률`을 포함해 해당 구간 분할 오염이 줄었다.

### C. 검정력 - 최초로 하한 돌파
| 모집단 | 창 | 유효 n | 일평균 종목 | IC | MDE | \|IC\|/MDE |
|---|---|---:|---:|---:|---:|---:|
| KOSPI | 2022-10~ | 40 | 681 | -0.02120 | 0.03977 | 0.53x |
| 전 시장 | 2022-10~ | 40 | 1,772 | -0.02260 | 0.03247 | 0.70x |
| **전 시장** | **2020-01~** | **74** | **1,876** | **-0.04241** | **0.02512** | **1.69x** |

### D. 세 문법 재실행 (전 시장, lb120 h20, 2020-07~)
| 문법 | FULL IC | block95 | seg1 | seg2 | 판정 |
|---|---:|---|---|---|---|
| momentum | -0.04266 | [-0.06252,-0.02233] | OK | spans0 | NOT_CONFIRMED |
| breakout | +0.03911 | [+0.01608,+0.06335] | spans0 | OK | NOT_CONFIRMED |
| **bollinger** | **-0.04513** | [-0.06389,-0.02556] | **OK** | **OK** | **CONFIRMED** |

### E. bollinger 정밀 검토 - 형식은 통과, 실질은 실패
| 검사 | 결과 |
|---|---|
| 사전등록 규칙(2구간) | **CONFIRMED** |
| 다중비교 보정 | **통과** - Bonferroni(3문법, a=0.0167) CI [-0.06735,-0.02196] 0 배제 |
| 구간 분할 민감도 | **실패** |
| 시간 안정성 | **실패** |
| 분위 단조성 | **실패** |
| 매매 가능성 | **없음** |

**구간 분할 민감도**
```
2구간: -0.0538*  -0.0365*                    -> CONFIRMED
3구간: -0.0533*  -0.0629*  -0.0191           -> NOT_CONFIRMED
4구간: -0.0422*  -0.0654*  -0.0626*  -0.0103 -> NOT_CONFIRMED
```
- **2구간에서만 통과한다.**

**연도별 - 2025~2026 소멸**
| 연도 | IC | 0배제 |
|---|---:|:---:|
| 2020 | -0.03838 | O |
| 2021 | -0.04440 | O |
| 2022 | -0.09597 | O |
| 2023 | -0.05651 | |
| 2024 | -0.04021 | O |
| **2025** | **-0.01103** | |
| **2026** | **-0.00860** | |

- 2구간 분할은 seg2가 2023-06부터라 강한 2023~2024를 포함해 통과했다. **`--segments 2`라는 기본값 선택이 결과를 만들었다.**

**분위 구조 - 또 U자**
```
D1 (가장 눌린)    -0.1864%   <- 오히려 최악
D6                +0.2631% * <- 10개 중 유일하게 유의
D10 (가장 늘어난) -0.2085%
```
- IC가 음수인 것은 **양 극단이 지기 때문이지 순위가 유효해서가 아니다.** "덜 늘어난 것을 사라"로 번역되지 않는다.
- 10개 중 1개 유의는 우연 기대치(0.5개) 수준이다.

### F. 사전등록 규칙 자체의 결함 (다음 라운드 전 수정 대상)
1. **구간 수를 고정하지 않았다.** `--segments` 기본값 2가 판정을 좌우했다. 다음부터 구간 수를 사전등록에 명시하고 **분할 민감도(2/3/4구간)를 통과 조건에 포함**해야 한다.
2. **분위 단조성이 통과 조건에 없다.** IC만으로는 U자와 단조를 구분하지 못하는데, 매매 가능성은 후자에서만 나온다.

### 판정
- **bollinger는 신호 유효성 층을 형식적으로 통과했으나 후보 선별 층으로 보낼 수 없다.** U자라 바스켓이 성립하지 않는다.
- 세 문법 모두 **매매 가능한 신호가 아니다.**
- 다만 (6)까지와 달리 **표본 부족이 원인이 아니다.** 검출 하한을 넘긴 상태에서 구조가 부정됐다. `DEFERRED`가 아니라 실질적 음성이다.

### 검증 항목 판정
- 기능 PASS(백필 122초/실패 0, value 근사 130만행 검증, 패널 재구성, 세 문법 재실행, 정밀 검토 4종)
- 정합성 PASS(커버리지 연속성, change_rate 증가 확인)
- 운영 반영 PASS(`tools/fetch_kosdaq_backfill.py` 신규 + 패널 캐시 재생성)
- 정책 NA / FAIL-CLOSED NA
- 회귀 **PASS** - 아래 G-1항 참조. (6) H항 8건을 백필 후 패널에서 전수 재현, **편차 0**

### G-1. 회귀 검증 - 백필 후 패널에서 8건 전수 재현
| # | 실행 | 기록값 | 재현값 | 일치 |
|---|---|---:|---:|:---:|
| 1 | basket lb20 h5 KOSPI | -0.4501% | -0.4501% | O |
| 2 | basket lb120 h20 KOSPI | -1.0253% | -1.0253% | O |
| 3 | IC lb120 h20 full KOSPI | -0.04035 | -0.04035 | O |
| 4 | IC seg1 | -0.05709 | -0.05709 | O |
| 5 | IC seg2 | -0.02361 | -0.02361 | O |
| 6 | IC 2022+ momentum KOSPI | -0.02819 | -0.02819 | O |
| 7 | IC 2022+ breakout KOSPI | +0.03908 | +0.03908 | O |
| 8 | IC 2022+ bollinger KOSPI | -0.03866 | -0.03866 | O |

- **소수점 5자리까지 동일.** 신호일 수(1601/1486/1109), KOSPI 종목 수(1,032), 블록 CI, 판정 전부 불변.
- (6) H항에서는 최대 0.033pp 편차가 있었으나 이번엔 **편차 0**이다. 백필이 KOSDAQ만 추가했고 `--market kospi` 경로를 건드리지 않았기 때문이며, 설계 의도대로다.

### 미검증
- 44개 응답 없음 종목의 정체를 확인하지 않았다
- 백필 구간의 분할 오염률을 재측정하지 않았다
- breakout의 seg2 강세(+0.06155)를 별도로 검토하지 않았다

## 2026-08-19 (8) 사전등록 규칙 강화 - bollinger 재판정 NOT_CONFIRMED, breakout만 단조 구조

(7) F항에서 지적한 규칙 결함 2건을 하네스에 반영했다. **강화된 규칙이 (7)의 `CONFIRMED`를 스스로 뒤집었다.**

### A. 규칙 변경 (3개 -> 5개)
기존:
```
1. 모든 구간이 min_eff_n 도달
2. 모든 구간의 블록 CI가 0 배제
3. 모든 구간의 부호 일치
```
추가:
```
4. 1~3이 --segments의 모든 구간 수에서 성립 (기본 2,3,4)
5. 분위 구조가 단조일 것 (U자/평탄 아님)
```

- `--segments`가 정수에서 **콤마 구분 목록**으로 바뀌었다(기본 `2,3,4`). 단일 분할에서만 살아남는 신호는 안정적이지 않다.
- `--min-monotone-rho` 신설(기본 0.7). 단조 판정은 두 조건 동시 충족:
  - `|spearman(분위 인덱스, 분위 초과수익)| >= 0.7`
  - **양 극단 분위의 부호가 반대일 것** — U자는 양 끝이 같은 부호로 진다
- 규칙 5를 진단이 아니라 **통과 조건**으로 올린 이유: IC가 0이 아니어도 U자면 양 극단이 둘 다 지는 것이므로 바스켓으로 번역되지 않는다.

### B. bollinger 재판정 - CONFIRMED -> NOT_CONFIRMED
```
2 segments : PASS
3 segments : FAIL (spans0)
4 segments : FAIL (n<min, spans0)
monotonicity: FAIL
  spearman = -0.139  (필요 0.7)
  tails on opposite sides = False  (B1 -0.186% vs B10 -0.209%)

>>> NOT_CONFIRMED -- 3-segment split; 4-segment split; bucket structure not monotone
```
- (7) E항에서 **수동으로** 발견한 결함 3건(2구간에서만 통과 / U자 / 양 극단 동시 하락)을 **도구가 자동으로 잡는다.** 같은 함정에 다시 빠지지 않는다.

### C. 세 문법 최종 판정 (전 시장, lb120 h20, 2020-07~)
| 문법 | 전기간 IC | block95 | 구간 안정성 | 단조성 | 판정 |
|---|---:|---|:---:|:---:|---|
| momentum | -0.04266 | [-0.06252,-0.02233] | 2/3/4 실패 | **U자** (rho -0.079) | NOT_CONFIRMED |
| **breakout** | **+0.03911** | [+0.01608,+0.06335] | 2/3/4 실패 | **단조** (rho **+0.964**) | NOT_CONFIRMED |
| bollinger | -0.04513 | [-0.06389,-0.02556] | 2구간만 통과 | **U자** (rho -0.139) | NOT_CONFIRMED |

**예상 못 한 발견 - breakout만 단조다**
```
spearman(bucket, excess) = +0.964      <- 거의 완벽한 단조
tails on opposite sides  = True        (B1 -0.716%  vs  B10 +0.418%)
monotonicity: PASS
```
- 세 문법 중 **유일하게 매매 규칙으로 번역 가능한 구조**다. 하위 분위가 지고 상위 분위가 이긴다.
- 그러나 구간 안정성에서 2/3/4 전부 실패해 최종 판정은 `NOT_CONFIRMED`다.

### D. 공통 패턴 - 셋 다 전기간은 유의한데 구간을 나누면 죽는다
- 세 문법 모두 전기간 블록 CI가 0을 배제한다. 그런데 구간을 나누면 전부 `spans0`이 된다.
- 원인은 **검정력**일 가능성이 크다. 유효 n 74를 2구간으로 나누면 각 37, 4구간이면 각 18이다. 유효 n 37에서 MDE는 약 0.035인데 관측 IC가 0.039~0.045로 거의 같은 크기다.
- **즉 규칙 4는 진짜 신호도 통과시키기 어려울 수 있다.** 규칙이 과도할 가능성을 인정한다.

### E. 그러나 지금 완화하지 않는다
- 결과를 보고 규칙을 완화하면 **사후 조정**이며 가드 5.2 위반이다.
- 규칙 4의 적정성은 **별도 사전등록**으로 다뤄야 한다. 예: "유효 n이 X 이상일 때만 구간 분할을 통과 조건으로 삼는다"를 결과 보기 전에 고정하는 방식.
- 현 시점 기록은 `NOT_CONFIRMED` 세 건으로 남긴다.

### 검증 항목 판정
- 기능 PASS(py_compile, 세 문법 재실행, bollinger 재판정이 수동 발견과 일치)
- 정합성 PASS - 강화 규칙이 (7) E항의 수동 검토 결과를 그대로 재현했다
- 운영 반영 PASS(`tools/research_loop.py` 수정)
- 정책 NA / FAIL-CLOSED NA
- 회귀 **PASS** - basket 모드 `-0.4501%` 불변, deciles 모드 rank IC `-0.04035` 불변. ic 모드만 변경했고 나머지 두 모드는 영향 없음을 실행으로 확인

### 미검증
- 규칙 4의 검정력 적정성을 정량 분석하지 않았다(구간별 MDE를 개략 추정만 함)
- breakout의 단조 구조(rho +0.964)가 무엇 때문인지 조사하지 않았다. 분할 오염 수정 후에도 남은 3.32배 집중과 관련 있을 수 있다

### 다음 후보
1. **breakout 단조 구조 규명** - 세 문법 중 유일하게 매매 가능한 모양이다. 왜 단조인지, 그리고 구간 불안정이 검정력 때문인지 실제 불안정인지 구분
2. 규칙 4 적정성을 별도 사전등록으로 재검토
3. basket/deciles 모드 회귀

## 2026-08-19 (9) breakout 정밀 조사 - 아티팩트 아님, 구조는 하락추세 회피, 규칙 4는 14.5년을 요구

(8)의 다음 후보를 우선순위대로 처리했다. **순서는 3 -> 1 -> 2**로 잡았다. breakout의 단조성이 분할 아티팩트면 나머지 조사가 무의미해지므로 그것이 선결 조건이다.

### A. 우선순위 1 - 분할 아티팩트 여부: 아님
(5)에서 `adjusted_price()` 수정 후에도 breakout 최하위 분위에 분할 영향 셀이 3.32배 남아 있었다. 그 최하위 분위가 단조성을 만드는 쪽이므로 인과를 확인해야 했다.

전 시장 패널에서 분할 영향 셀을 **전부 제거**하고 재계산:
| | 포함 | 제거 | 변화 |
|---|---:|---:|---|
| spearman | +0.964 | **+0.964** | 0 |
| B1 | -0.716% | **-0.716%** | 0 |
| B10 | +0.418% | +0.422% | +0.004 |
| IC | +0.03911 | +0.03866 | -0.00045 |

- 전 시장 기준 오염 셀은 2,787,887개 중 12,698개(**0.46%**)뿐이고, 제거해도 분위 계단이 미동도 하지 않는다.
- **아티팩트가 아니다.** 조사를 계속할 근거가 확보됐다.

### B. 우선순위 2 - 구조: "많이 빠진 것을 피하라"
`breakout = 조정종가 / 120일 최고가 - 1` (0 = 6개월 고점, -0.5 = 고점 대비 반토막)

| 분위 | 평균 신호 | 향후 20일 초과 |
|---|---:|---:|
| B1 | -0.483 | **-0.716%** |
| B2 | -0.347 | -0.472% |
| B3 | -0.287 | -0.426% |
| B4 | -0.243 | -0.166% |
| B5 | -0.207 | +0.059% |
| B6 | -0.174 | +0.097% |
| B7 | -0.141 | +0.252% |
| B8 | -0.108 | +0.439% |
| B9 | -0.071 | **+0.511%** |
| B10 | -0.022 | +0.418% |

- **6개월 고점에 가까울수록 향후 20일이 좋다.** B1~B9 단조 증가, B10만 소폭 꺾임.
- momentum과의 차이가 여기서 설명된다. momentum은 "많이 오른 것을 사라"라서 U자(양 극단이 진다)인데, breakout은 **"많이 빠진 것을 피하라"**라 단조다. 즉 상승 추종이 아니라 **하락 추세 회피**다.

### C. 우선순위 3 - 구간 불안정은 검정력 문제다
부호가 갈리면 실제 불안정, 부호가 같은데 유의성만 없으면 검정력 문제다.
```
2 seg: +0.0167   +0.0615*                        same sign=True
3 seg: +0.0180   +0.0260   +0.0733*              same sign=True
4 seg: +0.0276   +0.0058   +0.0396   +0.0834*    same sign=True
```
- **모든 분할, 모든 구간에서 양수.** 부호가 한 번도 갈리지 않는다.
- 연도별도 **7년 중 6년 양수**(2022만 -0.0117). 다만 연도별 MDE(0.057~0.118)가 관측값보다 커서 2026(+0.131)만 자기 MDE를 넘는다.

### D. 규칙 4가 요구하는 표본 크기 - 14.5년
```
관측 IC = 0.03911, 일별 IC sd = 0.1349
구간당 필요 유효 n = (1.96 * 0.1349 / 0.03911)^2 = 46

2구간: 실제 37  (8 부족)
3구간: 실제 24  (21 부족)
4구간: 실제 18  (27 부족)

4분할 통과에 필요한 총 유효 n = 183  ->  약 14.5년치 일별 신호
```
- 보유 6.4년으로는 **구조적으로 통과 불가능**하다. 규칙 4는 이 표본 크기에서 진짜 신호도 거른다.

### E. 그럼에도 지금 규칙을 완화하지 않는다
- breakout이 좋아 보이니 규칙을 낮추는 것은 **사후 조정**이며 가드 5.2가 금지한다.
- 개정안은 (10)에 **별도 사전등록**으로 남긴다. 그리고 그 규칙으로 재판정하더라도 **확증이 아니라 탐색**으로 분류해야 한다 - 규칙 설계 시점에 이미 breakout 결과를 봤기 때문이다.

### 판정
- breakout: 여전히 `NOT_CONFIRMED`. 규칙 4 미달.
- 다만 성격이 다르다. momentum/bollinger는 **구조가 부정**됐고(U자), breakout은 **표본이 모자라 판정을 못 한다**. 후자는 `DEFERRED`에 가깝다.

### 검증 항목 판정
- 기능 PASS(오염 제거 재계산, 분위-신호 대응표, 3종 분할 부호 확인, 필요 표본 산출)
- 정합성 PASS(오염 제거 전후 spearman/B1 동일, 부호 일관성이 검정력 가설과 부합)
- 운영 반영 NA(읽기 전용, 코드 변경 없음)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- B10이 B9보다 낮은 이유를 조사하지 않았다
- 하락추세 회피 해석을 다른 하락 지표(예: 고점 대비 낙폭 기간)로 교차 확인하지 않았다
- breakout을 바스켓으로 만들었을 때의 비용 차감 성과를 재지 않았다(IC 층에서 멈춤)

## 2026-08-19 (10) [사전등록] 규칙 4 개정안 - 검정력 게이트

**이 블록은 실행 전 등록이다. 아래 규칙을 코드에 반영하기 전에 여기 먼저 고정한다.**

### 배경 - 왜 개정이 필요한가
(9) D항에서 규칙 4가 구간당 유효 n 46을 요구하고, 4분할 통과에는 총 183(약 14.5년)이 필요함이 계산됐다. 보유 데이터는 6.4년(유효 n 74)이다. **규칙 4는 이 표본에서 참인 신호도 기각한다.**

### 오염 고지 (반드시 함께 읽을 것)
**이 개정안은 breakout의 결과를 본 뒤에 설계됐다.** 규칙 자체는 검정력 이론에서 유도되며 특정 문법에 유리하도록 만들지 않았으나, 설계 시점에 결과를 이미 관측했다는 사실은 지워지지 않는다.
따라서:
- 개정 규칙으로 재판정한 결과는 **`EXPLORATORY`로 분류하고 확증으로 주장하지 않는다.**
- 이 규칙 아래의 진짜 확증은 **아직 관측하지 않은 데이터**에서만 가능하다. 즉 2026-08-19 이후 새로 쌓이는 구간, 또는 아직 손대지 않은 모집단(예: 2020 이전, 또는 KOSDAQ 상장 이력 복구분).

### 개정 규칙 (사전 고정)
규칙 1~3, 5는 그대로 둔다. 규칙 4만 다음으로 대체한다.

```
규칙 4' (검정력 게이트)
  각 구간 수 k에 대해:
    required_eff(k) = ceil( (1.96 * sd_daily_IC / |IC_full|)^2 )
    if  eff_n_per_segment(k) >= required_eff(k):
        해당 k는 판정 대상이다. 규칙 1~3을 적용한다.
    else:
        해당 k는 DEFERRED_UNDERPOWERED로 분류하고 통과/실패 판정에서 제외한다.
  판정 대상인 k가 하나도 없으면 전체 판정은 DEFERRED_INSUFFICIENT_SAMPLE이다.
```

- `sd_daily_IC`와 `IC_full`은 **해당 실행에서 산출된 값**을 쓴다. 외부에서 주입하지 않는다.
- 검정력 미달을 **통과로 처리하지 않는다.** 판정 자체를 유보한다. 이것이 완화와 다른 점이다 - 미달 구간은 증거가 아니라 무증거로 취급된다.
- 부호 일관성(규칙 3)은 **검정력 미달 구간에도 계속 적용한다.** 부호가 갈리는 것은 표본 부족과 무관한 실제 불안정 신호이므로, 미달 구간이라도 부호가 갈리면 `NOT_CONFIRMED`다.

### 예상되는 결과 (등록 시점 기록, 사후 수정 금지)
현재 데이터에서 required_eff는 문법별로 다음과 같을 것으로 예상한다.
- breakout: IC 0.0391, sd 0.1349 -> required 46. 2/3/4구간 모두 미달(37/24/18) -> 전부 DEFERRED_UNDERPOWERED -> 판정 대상 k 없음 -> **전체 DEFERRED_INSUFFICIENT_SAMPLE**
- momentum / bollinger: IC가 더 크므로 required가 다소 낮을 수 있으나 역시 미달 예상. 다만 이 둘은 **규칙 5(단조성)에서 이미 탈락**하므로 결론은 `NOT_CONFIRMED`로 유지된다.

즉 개정 후에도 **breakout이 CONFIRMED가 되지는 않는다.** `NOT_CONFIRMED` -> `DEFERRED_INSUFFICIENT_SAMPLE`로 분류가 바뀔 뿐이다. 이 예상을 미리 적어두는 이유는, 개정이 특정 결과를 만들기 위한 것이 아님을 사후에 확인할 수 있게 하기 위함이다.

### 이 개정으로 무엇이 좋아지는가
- 지금까지 `NOT_CONFIRMED` 한 통에 담겨 있던 두 가지를 분리한다:
  - **구조가 부정된 것**(momentum, bollinger - U자)
  - **표본이 모자라 판정 불가인 것**(breakout)
- 후자는 데이터가 늘면 해소되는 문제이고, 전자는 늘어도 해소되지 않는다. **다음에 무엇에 투자할지가 달라진다.**

### 다음
1. 규칙 4'를 `tools/research_loop.py`에 반영
2. 세 문법 재판정 (결과는 `EXPLORATORY`로 표기)
3. 예상과 실제가 어긋나면 그 사실을 (11)에 기록

## 2026-08-19 (11) 규칙 4' 반영 및 재판정 - 등록한 예상과 실제가 일치

(10)의 사전등록을 코드에 반영하고 세 문법을 재판정했다. **결과는 `EXPLORATORY`다** - (10) 오염 고지대로 규칙 설계 시점에 이미 breakout 결과를 관측했으므로 확증으로 주장하지 않는다.

### A. 등록한 예상 vs 실제
| 문법 | (10)에 적은 예상 | 실제 | 일치 |
|---|---|---|:---:|
| breakout | 전 구간 미달 -> `DEFERRED_INSUFFICIENT_SAMPLE` | required 46, 실제 37/24/18 전부 미달 -> **`DEFERRED_INSUFFICIENT_SAMPLE`** | O |
| momentum | 규칙 5에서 탈락, `NOT_CONFIRMED` 유지 | 단조성 FAIL -> **`NOT_CONFIRMED`** | O |
| bollinger | 규칙 5에서 탈락, `NOT_CONFIRMED` 유지 | 단조성 FAIL -> **`NOT_CONFIRMED`** | O |

- **개정으로 `CONFIRMED`가 된 문법은 없다.** 등록 시점 예상과 정확히 같다. 규칙이 특정 결과를 만들기 위한 것이 아님이 사후 확인됐다.

### B. 실행 결과 상세
```
momentum   power gate: sd 0.1103, |IC| 0.04266 -> required eff/seg 26
             2seg FAIL(spans0) / 3seg DEFERRED(24<26) / 4seg DEFERRED(18<26) / monotonicity FAIL
             >>> NOT_CONFIRMED -- 2-segment split; bucket structure not monotone

breakout   power gate: sd 0.1349, |IC| 0.03911 -> required eff/seg 46
             2seg DEFERRED(37<46) / 3seg DEFERRED(24<46) / 4seg DEFERRED(18<46) / monotonicity PASS
             >>> DEFERRED_INSUFFICIENT_SAMPLE

bollinger  power gate: sd 0.1060, |IC| 0.04513 -> required eff/seg 22
             2seg PASS / 3seg FAIL(spans0) / 4seg DEFERRED(18<22) / monotonicity FAIL
             >>> NOT_CONFIRMED -- 3-segment split; bucket structure not monotone
```

### C. 구현 세부 (규칙 4'와 일치 확인)
- `required_eff = ceil((1.96 * sd_daily_IC / |IC_full|)^2)`, 값은 해당 실행에서 산출.
- 검정력 미달 구간은 `DEFERRED_UNDERPOWERED`로 판정에서 제외. **통과로 치지 않는다.**
- **부호 일관성(규칙 3)은 미달 구간에도 적용.** 부호가 갈리면 표본 문제가 아니라 실제 불안정이므로 즉시 `NOT_CONFIRMED`. 이번 세 문법에서는 부호 분열이 없었다.
- 판정 대상 구간이 하나도 없으면 전체 `DEFERRED_INSUFFICIENT_SAMPLE`.

### D. 개정의 실제 효용 - 두 종류의 실패가 분리됐다
| 문법 | 판정 | 성격 | 데이터가 늘면 |
|---|---|---|---|
| momentum | NOT_CONFIRMED | **구조 부정**(U자) | 해소되지 않음 |
| bollinger | NOT_CONFIRMED | **구조 부정**(U자) | 해소되지 않음 |
| **breakout** | **DEFERRED_INSUFFICIENT_SAMPLE** | 단조 구조 유지, **표본만 부족** | **해소 가능** |

- 지금까지 `NOT_CONFIRMED` 한 통에 섞여 있던 것이 나뉘었다. **다음 투자 대상이 달라진다.**

### E. breakout 확증에 필요한 추가 데이터
```
현재 유효 n 74 (2020-07 ~ 2026-08)
2구간 판정 필요량 : 구간당 46  ->  총 92
부족분            : 18 유효 n  ->  약 1.4년치 일별 신호
```
- 2020-01~06 및 2019년 이전을 채우거나, 1.4년을 더 축적하면 breakout에 대해 확증 판정이 가능해진다.
- **단, 그 확증도 (10) 오염 고지가 걸린다.** 진짜 확증은 규칙 설계 이후 새로 관측되는 구간에서만 가능하다.

### 검증 항목 판정
- 기능 PASS(py_compile, 세 문법 재판정, 등록 예상과 실제 일치 확인)
- 정합성 PASS(구현이 (10) 등록 문구와 일치, 부호 게이트 동작 확인)
- 운영 반영 PASS(`tools/research_loop.py` 수정)
- 정책 NA / FAIL-CLOSED NA
- 회귀 **PASS** - basket `-0.4501%` 불변, deciles rank IC `-0.04035` 불변. ic 모드만 변경했고 나머지 두 모드에 영향 없음을 실행으로 확인

### 미검증
- 부호 분열 경로(`same_sign=False`)가 실제로 트리거되는지 확인 못 함. 세 문법 모두 부호가 일관돼 해당 분기를 타지 않았다
- `required_eff`가 `|IC_full|`에 의존하므로, IC가 작을수록 요구 표본이 급증한다. 이 비선형성의 부작용을 검토하지 않았다

## 2026-08-19 (12) [사전등록] 2015~2019 봉인 OOS 검정 - breakout 확증

**실행 전 등록이다. 아래를 고정한 뒤에만 데이터를 수집하고 검정한다.**

### 왜 이 구간인가 - 유일하게 오염되지 않은 데이터
- (10)의 오염 고지: 규칙 4'는 breakout 결과를 본 뒤 설계됐으므로 2020~2026 구간의 어떤 재판정도 확증이 될 수 없다.
- **2020년 이전은 이 연구가 한 번도 관측한 적이 없다.** 패널에 넣은 적도, 어떤 문법을 돌려본 적도 없다.
- 따라서 이 구간은 V3 방법론의 `SEALED-OOS` 자격을 만족한다. 단순 표본 추가가 아니라 **진짜 out-of-sample 확증**으로 쓴다.

### 가용성 확인 (검정 전 사실 확인만 수행, 신호 미계산)
```
per-code 엔드포인트 : 2015-01-02까지 정상 반환 확인
명부                : 패널의 2020년 구성 2,362종목 (KOSPI 932 + KOSDAQ 1,430)
수집 시간           : 2,362종목 x 0.071초 = 약 2.8분
```

### 고정 사항 (수집 전 등록)
| 항목 | 값 |
|---|---|
| 수집 구간 | **2015-01-02 ~ 2019-12-30** |
| 명부 | 패널의 2020년 구성 2,362종목. 그 이전 상폐분은 복구 불가하며 이 한계를 인정한다 |
| `value` | `종가 x 거래량` (기존 백필과 동일 근사, 검증 완료) |
| 검정 대상 | **breakout 단독**. lookback 120, hold 20, market all, TopK/비용/유동성 하한 전부 기존과 동일 |
| 주 가설 | **breakout의 일별 rank IC는 0보다 크고, 분위 구조는 단조다** |
| 판정 규칙 | (11)에 구현된 5개 규칙을 **그대로** 적용. 수정 금지 |
| 구간 수 | `2,3,4` (기존 기본값 유지) |
| OOS 창 | **2015-07 ~ 2019-12만 사용.** 2020년 이후와 섞지 않는다 |

### 사전 예상 (사후 수정 금지)
- 2015~2019는 약 1,105 신호일 -> 유효 n 약 55. `required_eff`가 46 근처라면 2구간(각 27)은 여전히 미달일 수 있다.
- 즉 **이 OOS만으로 `CONFIRMED`가 나올 가능성은 낮다.** 가장 가능성 높은 결과는 `DEFERRED_INSUFFICIENT_SAMPLE`이다.
- 그러나 **부호와 단조성은 판정 가능하다.** 이 둘이 진짜 검정 대상이다:
  - OOS에서 IC 부호가 **양수로 유지**되고 spearman이 **+0.7 이상**이면 -> 구조가 독립 표본에서 재현된 것이다. 강한 증거다.
  - 부호가 뒤집히거나 단조성이 깨지면 -> **breakout은 2020~2026의 우연이었다.** 이 경우 즉시 폐기한다.

### 실패 조건 명시 (미리 못박음)
다음 중 하나라도 발생하면 breakout을 `NOT_SUPPORTED`로 종료하고 더 추적하지 않는다.
1. OOS 전기간 IC가 **음수**
2. spearman이 **+0.7 미만**이거나 양 극단 부호가 같음(U자)
3. OOS와 2020~2026의 부호가 **다름**

### 이 검정이 답하지 못하는 것
- 2015~2019가 통과해도 **매매 가능성이 입증되는 것은 아니다.** 신호 유효성 층일 뿐이며, 후보 선별/전략 실행 층은 별도다.
- 명부가 2020년 기준이라 2015~2019 구간은 **생존편향이 있다.** 그 기간에 상폐된 종목이 빠져 있으므로 결과가 낙관 쪽으로 치우칠 수 있다. 통과하더라도 이 한계를 함께 인용해야 한다.

### 다음
1. `tools/fetch_kosdaq_backfill.py`를 일반화하거나 별도 스크립트로 2015~2019 수집
2. 패널 재구성 후 **breakout만** OOS 창에서 실행
3. 결과를 (13)에 기록. 예상과 어긋나면 그 사실을 명시

## 2026-08-19 (13) 봉인 OOS 결과 - breakout 부호 반전, `NOT_SUPPORTED`로 종료

(12) 사전등록대로 2015~2019를 수집하고 breakout을 검정했다. **실패 조건 2개에 해당해 종료한다.**

### A. 수집한 자료 (출처 고정)
```
도구      : tools/fetch_kosdaq_backfill.py  (--roster panel2020 --market-label "" 로 일반화)
출력      : krx_daily_archive/krx_daily_20150102_20191230_backfill_clean.parquet
size      : 67,835,580 bytes
sha256    : 76e11d29d5123fae (앞 16자리)
rows      : 2,469,166
종목      : 2,269  (요청 2,362 중 93개 응답 없음)  KOSDAQ 1,353 / KOSPI 916
기간      : 20150102 ~ 20191230 (거래일 1,227)
소요      : 221초, 실패 0건
```
- 명부: 패널의 2020년 구성 2,362종목. **2020년 이전 상폐분은 복구 불가**이며 (12)에 명시한 한계 그대로다.
- 패널 총계: raw 9,387,564행 -> 거래일 **2,853** (20150102~20260818), 종목 3,063.
- `change_rate` 보유 행 2,150,592 -> **4,619,270**.

### B. OOS 결과 (2015-07 ~ 2019-12, breakout 단독)
```
FULL PERIOD  n=1105 eff=55  IC=-0.01930  block95=[-0.04783, +0.00882]  spans0
spearman(bucket, excess) = -0.879   tails opposite = True
  B1 +0.936%  ...  B10 -0.409%
power gate: sd 0.1356, |IC| 0.01930 -> required eff/seg 190
  2seg DEFERRED_UNDERPOWERED (27 < 190)
  3seg FAIL (sign split)
  4seg FAIL (sign split)
monotonicity PASS
>>> NOT_CONFIRMED -- 3-segment sign split; 4-segment sign split
```

### C. 사전등록 실패 조건 대조
(12)에 미리 적은 3개 조건과 실제:

| # | 실패 조건 | 실제 | 해당 |
|---|---|---|:---:|
| 1 | OOS 전기간 IC가 **음수** | **-0.01930** | **O** |
| 2 | spearman < 0.7 또는 U자 | -0.879 (단조지만 부호 반대) | 부분 |
| 3 | OOS와 2020~2026의 **부호가 다름** | +0.03911 vs **-0.01930** | **O** |

- **1번과 3번에 명확히 해당한다.** (12)에 "하나라도 발생하면 종료"라고 못박았으므로 **`NOT_SUPPORTED`로 종료하고 더 추적하지 않는다.**

### D. 부호가 완전히 뒤집혔다
| 구간 | IC | 분위 구조 |
|---|---:|---|
| 2020~2026 | **+0.03911** | B1 -0.716% -> B10 +0.418% |
| **2015~2019** | **-0.01930** | **B1 +0.936% -> B10 -0.409%** |

- 2020년 이후에는 "6개월 고점 근처가 이긴다"였고, 2015~2019에는 **"고점 대비 많이 빠진 것이 이긴다"**다. 단조 구조는 양쪽 다 있으나 **방향이 정반대**다.
- 3·4구간에서 **부호 분열**이 발생했다. 이는 검정력 게이트와 무관하게 적용되는 규칙 3 위반이며, 표본 부족이 아니라 실제 불안정이다. (11)에서 "부호 분열은 미달 구간에도 적용"하도록 만든 분기가 여기서 처음 작동했다((11) 미검증 항목 해소).

### E. 무엇이 무효화되는가
- (9) B항의 "하락 추세 회피" 해석은 **2020~2026 한정 서술**로 격하된다. 보편적 구조가 아니다.
- (9) C항의 "부호가 한 번도 갈리지 않는다"도 그 창 안에서만 참이었다.
- (11) D항에서 breakout을 "데이터가 늘면 해소 가능"으로 분류했는데, **틀렸다.** 데이터를 늘리자 구조가 부정됐다. `DEFERRED`가 `NOT_SUPPORTED`로 바뀐 사례다.

### F. 사전등록이 실제로 한 일
- 사전등록이 없었다면 이 결과를 "레짐 차이"로 해석했을 가능성이 높다. 2015~2019와 2020~2026은 실제로 시장 성격이 다르고, 사후에는 얼마든지 그런 서사를 붙일 수 있다.
- (12)에서 **결과를 보기 전에** "부호가 다르면 종료"라고 적어둔 것이 그 여지를 차단했다.
- (10)의 오염 고지도 유효했다. 이 OOS는 규칙 4' 설계 시점에 관측되지 않은 데이터이므로 **유일하게 오염되지 않은 검정**이었고, 그 검정이 기각했다.

### G. 세 문법 최종 상태
| 문법 | 판정 | 근거 |
|---|---|---|
| momentum | `NOT_CONFIRMED` | 분위 U자, 구조 부정 |
| bollinger | `NOT_CONFIRMED` | 분위 U자, 구조 부정 |
| **breakout** | **`NOT_SUPPORTED`** | **봉인 OOS에서 부호 반전. 종료** |

- **단순 가격 문법 3종이 모두 소진됐다.** 이번엔 표본 부족이 아니다.

### 검증 항목 판정
- 기능 PASS(2,469,166행 수집/실패 0, 패널 2,853 거래일 재구성, OOS 창 단독 실행)
- 정합성 PASS(사전등록 조건과 결과를 1:1 대조, 부호 분열 분기 최초 작동 확인)
- 운영 반영 PASS(신규 parquet + `fetch_kosdaq_backfill.py` 일반화)
- 정책 NA / FAIL-CLOSED NA
- 회귀 **PASS** - 패널이 2015년까지 확장돼 거래일이 1,626 -> 2,853이 됐으나, **창을 명시한 실행은 완전 불변**이다:
  - `basket lb20 h5 KOSPI` = `-0.4501%`, 신호일 1601 (동일)
  - `IC 2020-07~ 전시장 breakout` = `+0.03911`, 신호일 1482, 판정 `DEFERRED_INSUFFICIENT_SAMPLE` (동일)
  - 2020~2026 창만 보면 여전히 표본 부족이며, 그 창을 뒤집은 것은 별도 창인 OOS다. 두 결과는 모순이 아니다.

### 미검증
- 93개 응답 없음 종목의 정체
- momentum/bollinger를 2015~2019 OOS에서 돌리지 않았다. 이미 구조가 부정됐으므로 우선순위 낮음
- 2015~2019 구간의 분할 오염률 미측정

### 다음
1. 회귀 확인 (패널 확장 영향)
2. 가격 문법 3종 소진 후의 방향 결정. 남은 선택지는 (a) 다른 가격 문법(거래량 등), (b) 비가격 축, (c) 종료

## 2026-08-19 (14) [사전등록] 거래량 문법 - `vol_ratio`

**실행 전 등록이다. 코드 작성과 실행 전에 여기서 고정한다.**

### Layer 0 - 가설의 출처 (방법론 진단 5번 항목 이행)
지금까지 세 문법은 "돌려보자"로 시작했고 **왜 그 신호인지에 대한 사전 근거가 없었다.** 08-19 (8) 이전 방법론 진단에서 지적한 결함이다. 이번엔 먼저 적는다.

**경제적 근거**: 자기 자신의 평소 수준 대비 거래대금이 급증한 종목은 정보 유입 또는 관심 집중을 뜻한다. 한국 시장은 개인 비중이 높고, 관심이 몰린 종목이 이후 하회하는 현상(lottery-stock / attention effect)이 문헌에 반복 보고돼 있다.

**방향 사전 지정**: **IC는 음수일 것으로 예상한다.** 상대 거래량이 높을수록 향후 20일이 나쁘다. 이 방향을 결과 보기 전에 못박는다. 부호가 양수로 나오면 그것은 예상 실패이며, 사후에 "실은 유동성 프리미엄이었다" 같은 재해석을 붙이지 않는다.

### 신호 정의 (고정)
```
vol_ratio = mean(value, 20d) / mean(value, 120d)
```
- 단일일 RVOL은 노이즈가 커서 쓰지 않는다. 20일 평균 대 120일 평균으로 고정한다.
- `value`(거래대금)는 패널에 이미 있다. 2015~2019와 2020~2022 백필 구간은 `종가 x 거래량` 근사이며, (7) A항에서 1e8 하한 기준 99.888% 일치로 검증됐다.
- 신호는 스케일 불변이며 종목 간 비교 가능하다. 액면분할은 거래대금 레벨을 바꾸지만 20d/120d 비율에는 부분적으로만 영향을 준다 -> **미검증 항목으로 남긴다.**

### 실행 조건 (기존과 동일하게 고정)
| 항목 | 값 |
|---|---|
| lookback | 120 (120d 평균 산출용) |
| hold | 20 |
| market | all |
| 창 | **2015-07 ~ 2026-08 전 기간** |
| 판정 | (11)의 5개 규칙 그대로. 수정 금지 |
| 구간 수 | 2,3,4 |
| 비용/유동성 | 기존과 동일 (0.358% 왕복, value >= 1e8) |

### 홀드아웃에 대한 정직한 고지
- **이 문법에 대해 깨끗한 봉인 OOS는 없다.** 2015~2019는 breakout 검정에 이미 썼고, 그 과정에서 패널 구성·필터·비용 모델이 모두 관측된 상태다.
- 거래량 신호 자체는 어느 창에서도 돌려본 적이 없으므로 신호 수준의 오염은 없다. 그러나 **엄밀한 확증은 아니며**, 결과는 `EXPLORATORY`로 분류한다.
- 진짜 확증이 필요하면 2015년 이전 구간을 추가 확보해 봉인해야 한다.

### 실패 조건 (결과 보기 전 고정)
다음 중 하나라도 발생하면 `NOT_SUPPORTED`로 종료하고 추적하지 않는다.
1. 전기간 IC의 **부호가 양수** (사전 예상과 반대)
2. 분위 구조가 **U자**(단조성 규칙 5 실패)
3. 2/3/4 구간 중 **어느 하나라도 부호 분열**

### 사전 예상 (사후 수정 금지)
- 유효 n은 약 135(2,853 거래일 - 룩백/보유 소모 후 / 20)로 지금까지 중 가장 크다.
- 따라서 `required_eff`가 관측 IC 대비 낮으면 **처음으로 구간 판정이 실제로 이뤄질 수 있다.**
- 다만 세 문법의 전례를 보면 분위 U자로 탈락할 가능성이 가장 높다고 본다.

### 다음
1. `compute_signal()`에 `vol_ratio` 추가 (위 정의 그대로)
2. 전 기간 1회 실행
3. 결과를 (15)에 기록. 실패 조건 해당 시 즉시 종료

## 2026-08-19 (15) vol_ratio `CONFIRMED` - 1층 최초 통과, 규칙 자체도 최초 검증, 2층은 아직

(14) 사전등록대로 실행했다. **다섯 규칙을 전부 통과한 첫 신호다.** 다만 그것이 무엇을 뜻하는지 정확히 적어둔다.

### A. 결과
```
FULL PERIOD  n=2710 eff=135  IC=-0.04476  block95=[-0.05573, -0.03232]  OK
power gate: sd 0.0906, |IC| 0.04476 -> required eff/seg 16
  [2seg] -0.04937 / -0.04015                    PASS
  [3seg] -0.05386 / -0.05102 / -0.02942         PASS
  [4seg] -0.04318 / -0.05555 / -0.05031 / -0.03000  PASS
monotonicity PASS   spearman -0.915, 양 극단 반대 부호
>>> CONFIRMED
```
분위 구조:
```
B1:+0.354%  B2:+0.193%  B3:+0.196%  B4:+0.204%  B5:+0.088%
B6:+0.102%  B7:+0.089%  B8:-0.095%  B9:-0.219%  B10:-0.908%
```
- 거래대금이 자기 평소 대비 급증한 종목이 향후 20일에 크게 하회한다.

### B. 사전등록 대조
| 항목 | (14) 사전 기록 | 실제 | 일치 |
|---|---|---|:---:|
| **방향** | **IC 음수** (attention effect) | **-0.04476** | **O** |
| 유효 n | 약 135 | **135** | O |
| 구간 판정 가능성 | "처음으로 실제 판정될 수 있다" | required 16 vs 실제 33~67 -> 전부 판정 | O |
| 가장 가능성 높은 결과 | "분위 U자로 탈락" | **단조 통과** | **X** |

- **방향 예측이 맞았다.** 사후 해석이 아니라 경제적 근거에서 먼저 음수를 예측했고 그대로 나왔다. Layer 0을 넣은 첫 라운드이며, 그것이 작동했다.
- 마지막 예상은 빗나갔다. U자 탈락을 예상했으나 단조로 통과했다. **예상 실패를 기록으로 남긴다.**

### C. 왜 이번엔 구간 판정이 됐나
| | breakout | vol_ratio |
|---|---:|---:|
| 유효 n | 74 | **135** |
| 일별 IC sd | 0.1349 | **0.0906** |
| required eff/seg | 46 | **16** |
| 실제 eff/seg | 37/24/18 | **67/45/33** |
| 구간 판정 | 전부 DEFERRED | **전부 PASS** |

- IC 크기는 비슷한데(0.039 vs 0.045) **일별 IC sd가 33% 작다.** 그래서 필요 표본이 46 -> 16으로 떨어졌다.

### D. 규칙 집합 자체를 처음 검증했다 (음성 대조)
사용자 질문("사전등록 조건이 검증된 것인가")에 대한 대응. 지금까지 규칙은 실패를 겪을 때마다 고쳐졌을 뿐 **정답을 아는 신호로 검증된 적이 없었다.**

| 대조군 | IC | required_eff | rho | 판정 |
|---|---:|---:|---:|---|
| A. 순수 난수 | +0.00085 | 3,063 | +0.491 | **NOT_CONFIRMED** |
| B. vol_ratio **일별 셔플** | +0.00038 | 15,027 | +0.345 | **NOT_CONFIRMED** |
| C. vol_ratio 원본 | -0.04476 | 16 | -0.915 | CONFIRMED |

- B가 결정적이다. **분포를 그대로 두고 종목-수익 관계만 파괴**했더니 IC가 0.045 -> 0.0004로 죽고 required_eff가 16 -> 15,027로 폭증했다.
- **규칙 집합이 노이즈를 기각한다는 것이 처음 확인됐다.** 양성 대조(정답을 아는 진짜 신호)는 여전히 불가능하다. 정답이 없기 때문이다.

### E. 사전등록 조건이 목표하는 결론 (사용자 질문에 대한 정리)
5개 규칙은 **1층(신호 유효성)만** 판정한다. 목표 결론은 **"이 신호를 2층으로 보낼 자격이 있는가"**다.

| 층 | 질문 | 5규칙이 답하는가 |
|---|---|:---:|
| 1. 신호 유효성 | 종목 간 우열을 안정·단조적으로 구분하는가 | **예** |
| 2. 후보 선별 | 운용 가능한 일일 바스켓이 되는가 | 아니오 |
| 3. 전략 실행 | 진입/보유/청산 구조로 돈을 버는가 | 아니오 |

**`CONFIRMED`는 비용 차감 후 남는지, 계좌가 돈을 버는지를 판정하지 않는다.**

### F. 2층을 처음 봤다 - 숫자가 갈린다
| 구성 | 총수익/기간 | 순수익/기간 | 연환산 | 블록 95% CI(순) |
|---|---:|---:|---:|---|
| 롱 하위20(거래량 평온) | 1.5054% | **1.1474%** | **+14.46%** | **[+0.0306%, +2.3022%]** |
| 롱 하위분위(~270종목) | 0.9065% | 0.5485% | +6.91% | [-0.4875%, +1.5895%] |
| 롱 전체 유니버스 | 0.5530% | 0.1950% | +2.46% | [-0.6856%, +1.0839%] |
| 하위20 − 유니버스 | 0.9524% | 0.9524% | +12.00% | [+0.4141%, +1.4755%] |
| 롱/숏(하위20/상위20) | 2.7835% | 2.0675% | +26.05% | [+0.9168%, +3.0524%] |

- 비용 드래그: 0.358% x 12.6회 = **연 4.51%**
- **`--mode ic`가 재는 것은 "하위20 − 유니버스" 줄이다.** 비용이 양쪽에서 상쇄돼 그대로 남는다.
- **계좌가 겪는 것은 "롱 하위20" 줄이다.** 비용이 실제로 빠져 연 +14.46%이나 **CI 하한이 +0.0306%로 0에 거의 붙어 있다.**
- 두 숫자가 다르며 5규칙은 앞쪽만 본다. 이 간극이 지금까지 모든 결과가 죽은 지점이다.

### G. 유보 사항 (결론 내리기 전 반드시 함께 읽을 것)
1. **롱/숏 +26.05%는 실행 불가에 가깝다.** 한국 개별종목 공매도는 대부분 제한·고비용이다. 상한선이지 계획이 아니다.
2. **롱온리 하위20의 CI 하한 +0.0306%**는 사실상 0이다. "확실히 양수"라고 말할 수 없다.
3. **깨끗한 봉인 OOS가 없다.** (14)에 미리 적은 대로 이 결과는 `EXPLORATORY`다. breakout도 2020~2026 전 검사를 통과했다가 독립 표본에서 부호가 뒤집혔다.
4. 하위20은 **거래대금이 평온한 종목 20개**다. 유동성 하한(1e8)은 걸려 있으나, 실제 체결 가능성·시장충격은 검증하지 않았다.

### 검증 항목 판정
- 기능 PASS(사전등록 실행, 음성 대조 3종, 2층 경제성 산출)
- 정합성 PASS(사전 예상 4개 중 3개 적중, 빗나간 1개를 명시)
- 운영 반영 PASS(`compute_signal()`에 `vol_ratio` 추가, momentum 회귀 불변 확인 -0.04266)
- 정책 NA / FAIL-CLOSED NA
- 회귀 PASS(momentum IC 불변)

### 미검증
- 봉인 OOS 없음 (가장 큰 미검증 항목)
- 분할이 20d/120d 거래대금 비율에 미치는 영향 미측정 ((14)에 예고한 항목)
- 하위20의 실제 체결 가능성·시장충격·용량 미검증
- 하위20 종목의 성격(업종/시총 분포)을 보지 않았다. 특정 섹터 쏠림이면 다른 이야기가 된다
- 양성 대조 불가(정답을 아는 신호가 없음)

### 다음
1. **봉인 OOS 확보** - 2015년 이전 구간을 새로 수집해 봉인. 이것이 유일한 진짜 확증 경로다
2. 하위20의 종목 성격 확인 (섹터·시총 쏠림)
3. 2층 정식 판정 - 후보 선별 층의 사전등록을 별도로 작성

## 2026-08-19 (16) 규칙 3·4 설계 결함 + 보고 편향 + 구성요소별 판정

사용자 지적 3건에 대한 검토다. 세 건 모두 타당하며 이쪽 설계·보고의 결함을 가리킨다.

### A. 지적 1 - "매년 좋아야 한다"는 결론 방식이 맞는가
**맞지 않다. 규칙 3·4는 안정성이 아니라 검정력을 측정하고 있었다.**
- 참 IC -0.045, 일별 sd 0.09일 때 1년(유효 n 12)의 95% 오차범위는 **±0.05**다.
- **완벽히 안정적인 진짜 신호라도 연도별 검정은 절반쯤 실패한다.** 시장이 달라서가 아니라 1년치로 그 크기를 못 재기 때문이다.
- 규칙 4는 "신호가 불안정하다"와 "표본이 작다"를 구분하지 못한다. (9)에서 breakout이 14.5년을 요구한 것과 같은 뿌리다.

| | 질문 | 성격 |
|---|---|---|
| 현행 규칙 3·4 | 모든 구간에서 **검출되는가** | 검정력 |
| 올바름 | 구간 간 **변동이 노이즈 기대치보다 큰가** | **이질성** |

**올바른 도구를 이미 갖고 있으면서 규칙에는 넣지 않았다.** (5) C항에서 이질성 계산을 이미 했다:

| 신호 | 연도간 sd | 노이즈 기대 sd | 비율 |
|---|---:|---:|---:|
| momentum | 0.0288 | 0.0430 | 0.67 |
| breakout | 0.0324 | 0.0480 | 0.67 |
| bollinger | 0.0290 | 0.0394 | 0.73 |

셋 다 연도간 변동이 노이즈 기댓값보다 **작았다.** "연도마다 다르다"는 현상 자체가 없었다.

**이전 판정 정정**
- **breakout `NOT_SUPPORTED`는 유지.** 이질성 기준으로도 탈락한다: 2015-19 IC -0.019(SE~0.018) vs 2020-26 +0.039(SE~0.013), 차이 0.058 / SE 0.022 = **2.6시그마**. 부호 반전은 노이즈로 설명되지 않는다.
- **momentum / bollinger의 탈락 사유는 정정.** (8)에 "2/3/4구간 실패"를 사유로 함께 적었으나 이 둘은 이질성이 없다. **진짜 사유는 U자 구조(규칙 5) 하나뿐**이다.
- **vol_ratio `CONFIRMED`는 영향 없음.** 더 엄격한 규칙을 통과했으므로 완화된 규칙에서도 통과한다.

### B. 지적 2 - 게이트가 아니라 증거 축적이어야 한다
> "결과의 판단이 아니라 결과 자체를 모아 최종적으로 분석해 하나의 길을 도출해야 하는데, 지금은 좋지 않다는 결론은 배제하는 식이다."

- AGENTS.md 17: `검증 결과가 정책 변경 근거 부족이어도 원점으로 돌리지 않고, 다음 관찰, 표본, 후보 분류 산출물을 남긴다.` **이 조항을 어기고 있었다.** 라운드마다 PASS/FAIL로 닫고 실패를 버렸다.
- 버린 정보의 예 - 네 신호의 양 극단:

| 신호 | B1 | B10 | 구조 |
|---|---:|---:|---|
| momentum | -0.189% | **-0.639%** | U자 |
| bollinger | -0.186% | **-0.209%** | U자 |
| breakout 2020-26 | -0.716% | +0.418% | 단조 상승 |
| breakout 2015-19 | +0.936% | **-0.409%** | 단조 하강 |
| vol_ratio | +0.354% | **-0.908%** | 단조 하강 |

- **5개 중 4개에서 B10이 음수.** 각 B10은 "과거 수익률 최고 / 6개월 고점 최근접 / 거래대금 최대 급증"이며 **전부 주목받는 종목의 다른 표현**이다.
- 개별 판정으로는 "3실패 1통과"지만, 모으면 **네 신호가 한 현상을 다른 각도에서 쟀을 가능성**이 보인다. 미검증 가설로 남긴다.

### C. 지적 3 - "유지"도 검증의 결론이다
가드 9는 **일부 수정 / 대체 / 롤백** 세 결론을 둔다. 이쪽이 "유지"를 사실상 지웠다.

**보고 편향 자백**: PLANS 15개 블록이 전부 실패·결함·정정이다. 검증 통과로 끝난 블록이 하나도 없다. 실제로 통과한 것들:
- 청산 로직 (가설 기각 = 청산이 옳았음, (6))
- `trades_calc` 비용 모델 (629행 편차 0, (4))
- `research_loop` 시점 정렬 (미래정보 없음, 08-19 (1))
- 규칙 집합의 노이즈 기각 능력 ((15) 음성 대조)

**구성요소별 현재 판정 (지금까지 어디에도 명시되지 않음)**
| 구성요소 | 증거 | 결론 |
|---|---|---|
| 청산 로직 | (6) 사전등록 검정, 청산 후 5일 -2.54%(CI 0 배제) | **유지 - 증거 있음** |
| 비용 모델 | `trades_calc.csv` 629행 편차 0 | **유지 - 권위 확정** |
| 실행 파이프라인 | 주문->체결->원장, 2026-07-29 실체결 | **유지** |
| 리스크 계층 | risk_orch / kill switch가 권위 원장 사용 | **유지 - 이미 정합** |
| 진입 신호(final_score) | gross -0.14%, 후보 진입 풀 미도달 | 교체 필요 |
| 게이트/래더 | L7 상시 완화, 실질 무기능 | 미결 |

- **"로직을 버린다"가 아니다.** 교체 대상은 진입 신호 하나이고 나머지는 유지가 타당하다. **한 층만 갈아끼우면 되는 상태다.**

### D. 전문가 소견 - 방향 재설정 (사용자 요청)
1. **이 신호는 "고르는" 용도가 아니라 "빼는" 용도다.** B9 -0.219% -> B10 -0.908%로 **4배 점프**한다. 경제적 내용이 최상위 분위 하나에 몰려 있다. 제외 필터로 쓰면 회전율 추가 없음, 공매도 불필요, 단조성 불요, **기존 게이트에 조건 한 줄 추가**로 구현된다.
2. **비용이 진짜 제약인데 보유기간을 안 건드렸다.** 0.358% x 12.6 = 연 4.51%가 총수익의 30~100%다. 신호가 `20d/120d`로 느리게 움직이므로 **60일 보유 시 드래그 1.50%**로 떨어지고 IC는 상당 부분 유지될 가능성이 높다.
3. **B10이 기계적 현상일 위험.** 거래대금 급증은 상한가 연속 후 반전을 포함한다. **상한가에서는 매도 체결이 안 된다.** 백테스트는 종가 청산을 가정하지만 실제 호가가 없다. 메모리 `market_order_upper_limit_margin`과 같은 계열이다.
4. **표본 현실 인식**: 전문 팩터 리서치는 30년+ 다국가를 쓴다. 우리는 11.6년 단일 시장이다. **미묘한 통계적 효과는 찾을 수 없고, 크고 기계적인 효과만 찾을 수 있다.** vol_ratio(관심 효과)가 통과하고 momentum/bollinger/breakout(미묘한 부류)이 죽은 것이 우연이 아닐 수 있다.
5. **IC 0.045는 단일 raw 팩터로 나쁘지 않다**(통상 0.02~0.05). 그러나 breadth 모순이 있다: 하위20 +14.46%(CI 하한 +0.03%) vs 하위분위 270종목 +6.91%(CI 0 포함). **종목을 늘렸는데 수익이 반감되고 CI가 나빠진다.** 정상 팩터라면 IR이 개선돼야 한다. **이것은 팩터가 아니라 이상현상(anomaly)에 가깝고, 팩터처럼 다루면 안 된다.** 1번(제외 필터)의 근거가 여기서 또 나온다.

### E. 착수 순서 (사용자 승인)
기존에 "봉인 OOS 1순위"로 잡았으나 **재배치한다.** 1~3이 더 싸고 결정적이며, 특히 3번이 막히면 OOS 자체가 무의미해진다.
1. 보유기간 60일 테스트 (비용 3%p 절감 가능, 실행 한 줄)
2. B10 체결 가능성 확인 (상한가 비중)
3. 제외 필터로 재구성
4. 그 다음 봉인 OOS

### 검증 항목 판정
- 기능 PASS(오차범위 산출, 이질성 2.6시그마 계산, (5) C항 재인용)
- 정합성 **FAIL(자체 규칙 기준)** - 규칙 3·4가 의도(안정성)와 실제 측정(검정력)이 불일치
- 운영 반영 NA(읽기 전용, 코드 변경 없음)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- Q 통계량 / I^2 미계산. 개략 SE 기반 2.6시그마 추정만 했다
- B항의 "네 신호가 한 현상의 대리변수" 가설 미검증(신호 간 상관·상위분위 중복률 미측정)
- 규칙 3(부호 일관성)의 소표본 오탐률 미계산
- 규칙 개정(이질성 검정 도입)은 **아직 하지 않았다.** vol_ratio 결과를 본 뒤 바꾸면 (10)과 같은 오염이므로 별도 사전등록이 필요하다

## 2026-08-19 (17) 보유기간 종결 + 실행 가능성 + **연구와 실제 체결의 최초 연결**

(16) E항의 착수 순서 1·2를 실행했다. **2에서 연구 루프와 기존 시스템이 처음으로 연결됐고, 결과가 오늘 아침 F 검증을 설명한다.**

### A. 순서 1 종결 - 보유기간 20일 유지, 비용 절감 가설 기각
| hold | 비용/년 | B1 초과/년 | B10 초과/년 | 롱 하위20 순수익/년 | 블록 CI(기간당) |
|---:|---:|---:|---:|---:|---|
| 5 | 18.04% | +6.78% | -13.38% | 9.67% | [-0.1327%, +0.5084%] |
| **20** | 4.51% | +4.45% | **-11.44%** | **14.46%** | [+0.0357%, +2.2744%] |
| 60 | 1.50% | +2.63% | -8.81% | 10.67% | [+0.0990%, +5.3046%] |
| 120 | 0.75% | +1.10% | -5.12% | 9.79% | [+0.1588%, +10.3950%] |

- **가설 기각**: "신호가 느리니 60일로 늘리면 비용만 줄고 IC는 유지"라고 예상했으나 **비용이 줄어드는 만큼 신호도 줄어든다.** 비용 4.51->1.50%(3.0%p 절감)인데 B1 초과가 4.45->2.63%(1.8%p), B10이 -11.44->-8.81%(2.6%p) 감소한다.
- **기간당 수치에 속을 뻔했다.** B10이 -0.908%->-2.097%로 커지지만 연환산하면 약해진다.
- 순수익도 20일이 14.46%로 최고. IC도 -0.045로 60일(-0.047)과 실질 차이 없고, 판정은 20일만 `CONFIRMED`(60일은 검정력 부족).
- 단, **CI 하한은 보유가 길수록 0에서 멀어진다**(20일 +0.0357% vs 120일 +0.1588%). 관측 수 감소로 CI가 넓어진 효과와 섞여 있어 단정하지 않는다.
- **결론: 보유 20일 유지.**

### B. 순서 2 - 우려의 방향이 틀렸다
원래 우려는 "B10이 상한가 구간이라 못 판다"였다. **제외 필터로 쓰면 B10을 팔 필요가 없다. 애초에 사지 않는다.** 매도 체결 문제는 발생하지 않는다. 확인해야 할 것은 다른 셋이었다.

**1) B1을 살 수 있는가 - 살 수 있다**
```
B1  중앙 거래대금 : 394,634,981원
전체 중앙        : 932,366,705원   (B1은 0.42배)
B10 중앙         : 2,617,219,325원
```
- `vol_ratio`는 상대 지표라 "낮음 = 유동성 부족"이 아니다. 1e8 하한이 모든 분위에 걸려 있고 B1 절대액 3.9억은 20종목 매수에 충분하다.

**2) 상한가 비중 - 작다**
```
B10 중 향후 20일에 |이동| >= 28% 포함 : 7.89%
전체 유니버스                         : 3.53%
```
- B10이 2배 높으나 92%는 해당 없다.

**3) 극단일 제거 수치는 무효 (자체 스크립트 오류)**
```
B10 as-is      : -0.9079%/기간
B10 극단일 제거 : -3.0980%/기간   <- 인용 금지
```
- 극단 이동일을 포함한 종목을 제거하면서 **상한가로 크게 오른 종목까지 함께 제거**했다. B10에 섞인 급등 종목의 상승분만 잘라내니 남은 것이 나빠 보인 것이다.
- 올바른 검정은 "체결 불가능한 하락만 제외"인데 상승/하락을 구분하지 않았다. **미검증으로 남긴다.** `as-is` 수치(-11.44%/년, block95 [-1.25%, -0.47%])는 유효하다.

### C. 순서 2의 본론 - 시스템은 최악의 분위를 집중적으로 샀다
`paper/fills.csv`의 실제 BUY 381건을 각 체결일의 `vol_ratio` 분위로 채점했다(248건 채점, 133건 결측).

```
D1   4.8%    D2   4.4%    D3   2.8%    D4   4.8%    D5   4.0%
D6   6.9%    D7   9.7%    D8   8.5%    D9  16.9%    D10 37.1%
```
- **D10 비중 37.1% = 중립(10%) 대비 3.71배.** D9+D10이 **54.0%**로 절반 이상이 상위 2개 분위에 몰려 있다.
- 그리고 **D10이 연 -11.44%로 가장 나쁜 분위**다.

**경로별 - 급등 전략이 구조적 원인**
| 경로 | n | 평균 분위 | D10 비중 |
|---|---:|---:|---:|
| **SURGE_RUNTIME** | 21 | **9.00** | **61.9%** |
| SURGE | 55 | 8.09 | 41.8% |
| other | 127 | 7.27 | 36.2% |
| INTRADAY_REALTIME | 12 | 7.83 | 25.0% |
| INTRADAY | 28 | 7.00 | 25.0% |
| beta_harvest | 5 | 8.40 | 0.0% |

- SURGE_RUNTIME은 **62%가 D10**이다. 당연하다 - "급등"의 정의 자체가 거래량 급증이다.
- **시스템은 거래대금이 자기 평소 대비 가장 많이 튄 종목을 골라 샀고, 그것이 향후 20일에 가장 나쁜 분위였다.**

### D. 이것이 F 검증(08-18 (1)(2))을 설명한다
아침에 확인한 것: 실현 손익 **-287만원**, **비용 0으로 놓아도 거래당 -0.14%**.
- **왜 gross부터 마이너스였는지에 대한 첫 번째 구조적 설명이다.** 종목 선택이 체계적으로 최악의 분위로 편향돼 있었다.
- 지금까지 "진입 신호에 알파가 없다"까지만 말할 수 있었는데, **알파가 없는 것을 넘어 역방향으로 편향돼 있었다.**

### E. 제외의 가치
```
중립 포트폴리오 가정 : 연 1.14%
실제 시스템 가정(37.1%) : 연 4.24%
```
- **추가 회전 없이 연 4.24%.** 안 사기만 하면 되므로 비용이 0이다.

### 검증 항목 판정
- 기능 PASS(보유 4종 실행, 유동성/상한가 측정, 체결 381건 채점)
- 정합성 **부분** - C·E는 유효하나 B-3의 극단일 제거 수치는 자체 오류로 무효
- 운영 반영 NA(읽기 전용, 코드 변경 없음)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증 (인용 시 반드시 함께 읽을 것)
- **381건 중 248건만 채점**됐다. 133건 결측이 무작위인지 확인하지 않았다
- **`-11.44%`는 2015~2026 전 기간 수치**인데 실제 체결은 2025-12~2026-08에 몰려 있다. 그 짧은 구간의 D10 수익률을 따로 재지 않았다
- **인과가 아니다.** "D10을 빼면 4.24% 좋아진다"가 아니라 "D10 비중이 그만큼이고 D10이 평균적으로 나빴다"이다. 실제로 빼면 다른 종목이 그 자리에 들어온다
- `other` 127건(최대 그룹)의 경로가 미상이다
- B-3 극단일 제거 검정을 상승/하락 구분해 다시 해야 한다

### 다음 (열린 항목)
1. 순서 3 - 제외 필터로 재구성 (사전등록 필요)
2. 순서 4 - 봉인 OOS
3. 규칙 개정(이질성 검정) 별도 사전등록
4. 유동성 x 보유기간 축 (목록만, 착수 안 함)
5. B-3 재검정, 체결 구간 한정 D10 수익률, `other` 127건 규명

## 2026-08-19 (18) [사전등록] 제외 필터 반사실 검정 (연구 한정)

**실행 전 등록. 이것은 연구 검정이며 생산 반영이 아니다.**

### 범위 명시
- **(18)은 `③-a` 연구 검정만 다룬다.** 읽기 전용, `E:\1_Data` 변경 0건.
- **`③-b` 생산 반영(SURGE 경로에 vol_ratio 게이트 추가)은 하지 않는다.** 그것은 AGENTS.md 5의 정책 의미 변경이며 9(ExecPlan), 11(E2E), 14(백업)가 모두 걸린다. 별도 승인 사안이다.

### 검정 질문
> 실제 체결에서 D10 진입을 제외했다면 실현 손익이 어떻게 달라졌는가

- (17) C항은 **패널 선행수익** 기준이었다. 이번은 **실제 체결의 실현 손익** 기준이다. 시뮬레이션이 아니다.
- 08-18 (1)(3)의 F 검증(-287.1만원)과 직접 연결된다.

### 방법 (고정)
| 항목 | 값 |
|---|---|
| 모집단 | (17)에서 분위 채점된 BUY **248건** 중 청산이 연결된 것 |
| 연결 | `SELL.note.entry_order_id` -> `BUY.order_id` (08-18 (1)과 동일, 적중 575/miss 0) |
| 손익 | production 비용 모델. `net = (x-e)/e - ((e+x)/e)*0.00104 - 0.0015` |
| 비교 | **D10 lot vs D1~D9 lot** |
| 주 지표 | 두 집단의 lot당 평균 실현 순수익, 블록 부트스트랩 CI(block=20) |
| 보조 | D10 lot의 실현 손익 합계, 전체 대비 비중 |

### 사전 예상 (사후 수정 금지)
- (17)에서 D10의 패널 기준 초과가 연 -11.44%였으므로, **실현 기준으로도 D10이 나쁠 것으로 예상**한다.
- 다만 표본이 작다. 248건 중 D10은 92건이고 청산 연결 후 더 줄어든다. **`DEFERRED_INSUFFICIENT_SAMPLE`이 가장 가능성 높은 결과**로 본다.

### 해석 조건 (결과 보기 전 고정)
1. **D10이 D1~D9보다 유의하게 나쁨** -> 패널 발견이 실제 체결로 이전됨. 제외 필터의 근거가 강해진다
2. **차이가 표본 노이즈 범위** -> `DEFERRED`. 근거는 패널 수준에 머문다
3. **D10이 오히려 나은 경우** -> **패널 발견이 실제 매매로 이전되지 않는다.** 제외 아이디어를 폐기하고 (17) E항의 연 4.24%는 인용하지 않는다

### 이 검정이 답하지 못하는 것
- **인과가 아니다.** D10을 빼면 그 자리에 다른 종목이 들어온다. 이 검정은 "뺐을 때 남는 것"을 보는 회계이지 대체 효과를 모사하지 않는다.
- 표본이 8.5개월, 특정 시장 국면 하나다.
- 채점 불가 133건이 빠져 있고 그 결측이 무작위인지 확인되지 않았다.

## 2026-08-19 (19) 제외 필터 반사실 - `DEFERRED`, 사전 예상 빗나감, 패널 발견이 체결로 이전되지 않음

(18) 사전등록대로 실행했다. **해석 조건 2번(`DEFERRED`)에 해당하며, 사전 예상은 빗나갔다.**

### A. 결과
```
분위 채점된 BUY : 248 / 381
청산 연결 lot   : 356   (D10 157 / D1-D9 199)
```
| 집단 | lot | 순손익 | ROI | lot당 평균 | 블록 95% CI |
|---|---:|---:|---:|---:|---|
| **D10** | 157 | -708,321 | -0.68% | **+0.511%** | [-0.987%, +3.306%] |
| **D1-D9** | 199 | -694,768 | -0.38% | **-0.901%** | [-2.639%, +0.950%] |
| ALL | 356 | -1,403,089 | -0.49% | -0.279% | [-1.752%, +1.424%] |

```
D10 - D1~D9 = +1.412%/lot   SE 4.850%   t = +0.29
```

### B. 사전 예상이 빗나갔다 (기록)
(18)에 이렇게 적었다:
> D10의 패널 기준 초과가 연 -11.44%였으므로, **실현 기준으로도 D10이 나쁠 것으로 예상**한다.

**실현 기준으로 D10은 나쁘지 않았다. lot당 평균은 오히려 더 좋다.** 부호가 예상과 반대로 나왔다.

### C. 그러나 조건 3(폐기)은 아니다
- 조건 3은 "D10이 **유의하게** 나은 경우"였다. `t = 0.29`는 유의하지 않다.
- **조건 2(`DEFERRED_INSUFFICIENT_SAMPLE`)가 맞다.**
- 표본을 보면 당연하다. D10 157 lot의 유효 독립표본은 **약 7**이다. SE 4.85%로 1.4%p 차이를 구분할 수 없다.

### D. 두 지표의 부호가 갈린다 - 큰 포지션이 D10에 몰렸다
```
lot당 평균 : D10 +0.511%  vs  D1-D9 -0.901%   (D10이 나음)
ROI        : D10 -0.68%   vs  D1-D9 -0.38%    (D10이 나쁨)
```
- **금액 가중하면 D10이 나쁘고 건수 가중하면 낫다.** D10에서 **크게 산 것이 크게 졌다**는 뜻이다.
- D10이 전체 실현 손실의 **50.5%**를 차지하는 것도 같은 이야기다.
- 이 포지션 사이징 편향 자체는 별도 조사 대상이다. 이번 검정 범위 밖이다.

### E. 판정 - (17) E항을 인용 금지로 전환
- **패널 발견이 실제 체결로 이전된다는 증거가 없다.**
  - 패널 기준: D10 연 -11.44%, 블록 CI 0 배제, 2,710 신호일
  - 실현 기준: D10이 오히려 나음, 단 판정 불가(유효 n 7)
- **(17) E항의 "연 4.24% 절감"은 인용하지 않는다.** 실현 데이터가 지지하지 않는다.
- (17) C항의 "D10 비중 37.1%, 중립 대비 3.71배"는 **여전히 유효**하다. 그것은 무엇을 샀는지에 대한 사실이며 손익 주장이 아니다.

### F. 왜 갈리는가 - 전부 미검증 가설
1. **표본 부족** (유효 n 7) - 가장 유력
2. **기간 불일치** - 패널 11.6년 vs 체결 8.5개월
3. **보유기간 불일치** - 패널은 20일 고정, 실제는 STOP/TP 등 청산 로직이 결정. **평균 실제 보유일을 확인하지 않았다.** 이것이 사실이면 두 검정은 **애초에 다른 것을 재고 있다**
4. **진입 시점 불일치** - 패널은 t+1 종가, 실제는 장중/시가 등 다양

**3번이 가장 의심스럽다.** 08-18 (1) 블록에서 실제 exit_reason이 STOP / STOP_PREEMPTIVE_CLOSE / DDM_LIQUIDATE에 집중돼 있음을 이미 확인했다. 20일 선행수익과 STOP 기반 청산은 같은 대상이 아니다.

### 검증 항목 판정
- 기능 PASS(248건 채점, 356 lot 연결, 블록 CI 산출)
- 정합성 PASS - 사전등록 해석 조건과 1:1 대조, 예상 실패를 명시
- 운영 반영 NA(읽기 전용, `E:\1_Data` 변경 0건. `③-b` 생산 반영은 착수하지 않음)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 실제 평균 보유일 미확인 (F-3 가설의 핵심)
- D10의 포지션 사이징이 왜 큰지 미조사
- 채점 불가 133건의 결측 성격
- 체결 구간(2025-12~2026-08) 한정 패널 D10 수익률 미산출 - F-2를 직접 검정할 수 있는 방법

### 다음 - 우선순위 재조정
(19)로 인해 순서가 바뀐다. **제외 필터(③)는 근거가 약해졌으므로 후순위로 내린다.**
1. **F-3 검정**: 실제 평균 보유일 산출. 20일과 크게 다르면 패널 검정 전체의 실행 계약을 재설계해야 한다
2. F-2 검정: 체결 구간 한정 패널 D10 수익률
3. 봉인 OOS (④) - vol_ratio 자체의 확증
4. 규칙 개정(이질성) 사전등록 (⑤)
5. 유동성 x 보유 축 (⑥, 목록만)

## 2026-08-19 (20) [중대] 실제 보유기간 중앙값 1일 - 오늘의 모든 패널 검정이 시스템이 하지 않는 매매를 쟀다

(19) F-3 가설을 검정했다. **가설이 맞았고, 파급이 오늘 라운드 전체에 걸린다.**

### A. 실제 보유기간 (거래 세션 기준, 562 lot)
```
p05  0.0    p25  0.0    p50  1.0    p75  2.0    p90  3.0    p95  6.0
평균 1.6                              <- 패널은 20을 가정

1세션 이내 청산 : 71.2%
5세션 이내      : 94.5%
20세션 초과     :  0.0%
```
- **중앙값 1세션. 패널 가정 20세션. 20배 차이.**
- **20세션을 넘긴 포지션이 단 한 건도 없다.**

### B. 청산 사유가 원인을 설명한다
| exit_reason | n | 중앙 보유 | 평균 |
|---|---:|---:|---:|
| STOP | 138 | 1.0 | 1.7 |
| FUNDAMENTAL_CRITICAL | 69 | 3.0 | 2.5 |
| STOP_PREEMPTIVE_CLOSE | 64 | 1.0 | 1.1 |
| STOP_GAP | 55 | 1.0 | 0.8 |
| **SURGE_INTRADAY_REVERSAL** | 51 | **0.0** | 0.0 |
| DDM_LIQUIDATE_L4 | 44 | 2.0 | 4.5 |

- 대부분 손절이고 급등 반전은 **당일 청산**이다. 시간 경과가 아니라 가격/이벤트가 청산을 결정한다.

### C. 파급 - 실행 계약 불일치
| | 패널 검정 | 실제 시스템 |
|---|---|---|
| 보유 | 20세션 고정 | **중앙값 1세션** |
| 청산 결정 | 시간 경과 | **STOP / 반전 / 강제청산** |
| 연 회전 | 12.6회 | **252회 근접** |
| 비용 드래그 | 연 4.51% | **연 90% 수준** |

- 08-18 (2)에 이미 이 표가 있었다:
```
hold(d)   연간 비용 드래그
      1        90.22%
      5        18.04%
     20         4.51%
```
- **실제 시스템은 이 표의 맨 윗줄에서 돌고 있었다.** 그 표를 만들어놓고 오늘 하루 종일 세 번째 줄로 검정했다.

### D. (19)의 불일치가 설명된다
- 패널 D10(-11.44%/년)과 실현 D10(+0.511%/lot)이 갈린 것은 **표본 부족이 아니라 대상 불일치**다.
- **20일 뒤 수익률과 1일 뒤 손절 결과는 같은 것이 아니다.** (19) F-3을 "가장 의심스럽다"고 적었는데 사실로 확인됐다.

### E. 가장 큰 함의 - 비용 구조가 성립 불가다
08-18 (2)의 `비용 0으로 놓아도 gross -0.14%`와 합치면:
- 실제 시스템은 **1일 보유 / 연 90% 비용 구조**에서 돌고 있고
- 그 상태에서 **gross조차 마이너스**다

**연 90% 비용을 이기려면 gross가 연 90%를 넘어야 한다.** 어떤 신호로도 도달 불가능한 수준이다.
- 즉 진입 신호를 무엇으로 바꾸든, **현재 회전율에서는 수익 구조가 성립하지 않는다.**
- 08-18 (2) E항에서 "바스켓 확증은 이 표본에서 원천 불가능"이라 적었는데, 그보다 앞선 문제가 있었다. **회전율 자체가 불가능한 수준이다.**

### F. 오늘 라운드들의 지위 재정의
- **패널 검정(vol_ratio 포함) 결과는 폐기하지 않는다.** 20일 보유 전략에 대한 유효한 측정이다.
- **그러나 현재 시스템에 대한 서술이 아니다.** 두 가지는 분리해서 인용해야 한다:
  - `vol_ratio CONFIRMED` = "20일 보유 전략을 만든다면 이 신호가 유효하다"
  - 현재 시스템 = 1일 보유이며 이 신호로 개선되는지는 **미검증**
- (17)의 D10 편중 37.1%는 여전히 유효하다. 그것은 진입 시점 사실이며 보유기간과 무관하다.

### 검증 항목 판정
- 기능 PASS(562 lot 거래일 기준 산출, 청산 사유별 분해)
- 정합성 PASS((19) F-3 가설과 결과가 일치, 08-18 (2) 비용표와 정합)
- 운영 반영 NA(읽기 전용)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 보유 0세션(당일 청산) 건의 실제 체결 시각 미확인. 장중 진입/청산이면 일별 패널로는 애초에 모사 불가
- 회전율이 설계 의도인지 사고인지 미확인. STOP 임계가 너무 타이트해서일 수도, 급등 전략의 본질일 수도 있다
- 1일 보유 기준으로 vol_ratio를 재검정하지 않았다

### 다음 - 순서 전면 재조정
**신호 탐색보다 회전율이 선행 문제다.**
1. **회전율이 의도인지 사고인지 규명** - STOP 임계값과 실제 발동 분포. 08-18 (6)에서 청산이 자본을 지켰다고 확인했으나, 그것은 "청산 자체가 옳았나"였지 "이렇게 빨라야 하나"가 아니었다
2. 1일 보유 기준 vol_ratio 재검정 - hold=1로 돌리면 신호가 남는지
3. 봉인 OOS (④)
4. 규칙 개정(⑤), 유동성 축(⑥)

## 2026-08-19 (21) 회전율 원인 규명 - 절반은 설계, 절반은 미상, 그리고 진짜 문제는 진입이다

(20)의 후속. **"1일 회전이 의도인가 사고인가"에 답하려 했고, 답이 셋으로 갈렸다.**

### A. 설정값 (라이브)
```
sell_rules.stop_loss:
  default_pct              -12
  preemptive_close_pct      -8
  stop_sell_ratio_pct       60      <- 부분청산
  trailing_stop_activation_profit_pct  12
  trailing_stop_pct        -10

dynamic_stop_loss (enabled):
  break_even_trigger_profit_pct   4
  break_even_stop_pct        -0.002   <- -0.20%
  time_decay_start_days       2, step 0.30pp/day, cap 3.00pp
  atr_ref_pct                 4%, scale 0.5, cap 3.00pp
```

### B. 실제 발동 지점 - 설정과 3배 차이
| 청산 | 설정 | 실제 중앙값 | 설정 도달 비율 |
|---|---:|---:|---:|
| **STOP** | -12% | **-4.00%** | **0.7%** (138건 중 1건) |
| STOP_PREEMPTIVE_CLOSE | -8% | -5.56% | 35.9% |
| STOP_GAP | -12% | -8.44% | 34.5% |

전체 분포: **55.7%가 -8%~0% 구간**에서 청산된다. 어떤 설정 임계값에도 해당하지 않는 구간이다.

### C. 동적 조임은 원인이 아니다 (배제)
`_resolve_dynamic_stop_loss_pct()`를 라이브 설정으로 재현했다.
```
보유 0~1일, ATR 3%  -> -12.00% (기본값 그대로)
보유 10일           -> -9.30%  (time-decay 최대)
ATR 6%              -> -11.00%
time-decay cap 3.00pp / ATR cap 3.00pp -> 최대 조여도 -9.00%
```
- **최대 -9.00%까지만 조여진다. 관측된 -4.00%를 설명하지 못한다.** 단위 처리(`_pct01_from_config`)도 정상이다.

### D. break-even 규칙은 정확히 작동한다 - 설계가 과도한 것이다
```
peak >= +4% (BE 발동) : n=28   중앙 청산 -0.20%  (p25 -0.20%, p75 -0.20%)
peak <  +4%           : n=110  중앙 청산 -5.00%
```
- **28건 전부가 정확히 -0.20%에서 청산됐다.** `break_even_stop_pct = -0.002`가 설정대로 동작한다. **사고가 아니라 설계된 동작이다.**
- 문제는 설계 자체다. **+4%를 한 번 스치면 손절이 -12% -> -0.20%로 60배 조여진다.** 이후 정상적 되돌림 한 번에 청산된다.

### E. 나머지 110건의 -5%는 원인 미상
- BE 미발동 110건도 중앙 -5.00%로 설정 -12%의 절반 이하다. 동적 조임 최대치(-9%)로도 설명 안 된다.
- 실마리: `sell_ratio_pct` 분포
```
60%  : 63건   <- 설정 stop_sell_ratio_pct = 60
100% : 31건
50%  : 22건
partial_exit=1 : 77건
```
- **77건이 부분청산이다.** -5%에서 60%를 팔고 남은 40%가 별도 lot으로 재계산된다.
- **즉 지금까지 세던 "lot"이 실제 포지션이 아니라 부분청산 조각일 수 있다.** (20)의 보유기간 중앙값 1일도 이 영향을 받았을 가능성이 있다. **미검증.**

### F. 가장 중요한 발견 - 진입 후 즉시 하락한다
| reason | n | 중앙 peak | 중앙 청산 | **한 번도 플러스 못 봄** |
|---|---:|---:|---:|---:|
| STOP_PREEMPTIVE_CLOSE | 63 | -3.68% | -5.61% | **82.5%** |
| SURGE_INTRADAY_REVERSAL | 51 | -3.97% | -1.93% | **76.5%** |
| DDM_LIQUIDATE_L3 | 14 | -0.12% | -0.69% | 71.4% |
| STOP_GAP | 53 | -1.71% | -8.41% | 64.2% |
| STOP | 138 | +0.25% | -4.00% | 48.6% |
| TP_L12 | 17 | +11.06% | +12.00% | 0.0% |

- **`STOP_PREEMPTIVE_CLOSE`의 82.5%가 진입 후 단 한 번도 플러스를 못 봤다.** 급등 반전도 76.5%다.
- **사자마자 떨어진다.** 손절이 문제가 아니라 **진입 타이밍이 고점**이라는 뜻이며, (17)에서 확인한 "D10(거래량 급증) 37.1% 편중"과 정확히 맞물린다.
- **손절을 넓혀도 이 구조는 고쳐지지 않는다.**

### 판정 - "의도인가 사고인가"에 대한 답
| 요소 | 판정 |
|---|---|
| break-even -0.20% (28건) | **설계대로 작동. 설계가 과도** |
| 부분청산 60% (77건) | 설계대로. 단 lot 계수를 왜곡 |
| -5% 조기 청산 (110건) | **원인 미상** |
| 진입 후 즉시 하락(76~82%) | **진입 문제이지 청산 문제가 아님** |

- **회전율은 순수한 사고가 아니다.** break-even과 부분청산은 설계된 동작이다. 다만 그 설계가 회전율을 극단으로 밀어올린다.
- **그러나 회전율을 고쳐도 근본 문제는 남는다.** 진입 직후 하락하는 구조가 진짜 원인이다.

### 검증 항목 판정
- 기능 PASS(설정 판독, 동적 손절 재현, 558 lot 종가 경로 복원, BE 발동 여부별 분해)
- 정합성 PASS(BE 28건이 -0.20%에 정확히 수렴, 설정과 코드 동작 일치 확인)
- 운영 반영 NA(읽기 전용, 코드/설정 변경 0건)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- **-5% 조기 청산 110건의 원인** (가장 큰 공백)
- 부분청산이 (20)의 보유기간 통계를 얼마나 왜곡했는지. **원포지션 기준으로 재집계 필요**
- peak 계산에 종가만 썼다. 장중 고가가 없어 BE 발동 여부가 과소 추정됐을 수 있다
- `asset_type_overrides`의 한글 키가 깨져 있다(`������`). 매칭 실패 시 어떤 값이 적용되는지 미확인

### 다음
1. **부분청산을 합산해 원포지션 기준으로 보유기간 재집계** - (20)의 "중앙값 1일"이 유지되는지 확인. 이것이 아니면 (20)의 결론 자체가 흔들린다
2. -5% 청산 110건의 코드 경로 추적
3. `asset_type_overrides` 문자열 깨짐 확인 (AGENTS.md 12 대상)

## 2026-08-19 (22) 포지션 단위 재집계 - (20) 결론 유지, 그러나 분할매도 구조가 드러남

(21) 다음-1 항목. 부분청산 77건이 (20)의 "중앙값 1일"을 왜곡했을 가능성을 검정했다. **왜곡은 없었고 (20)은 견고하다.**

### A. 재집계 결과 - 결론 불변
진입 BUY부터 **최종** 매도까지를 한 포지션으로 묶었다.
```
BUY 381건 / 청산 연결 330건 / SELL 조각 575건
조각/포지션 평균 1.74, 최대 11
```
| 단위 | n | p25 | **p50** | p75 | p95 | 평균 |
|---|---:|---:|---:|---:|---:|---:|
| slice (블록 20 기준) | 562 | 0.0 | **1.0** | 2.0 | 6.0 | 1.59 |
| **position** | 322 | 0.0 | **1.0** | 2.0 | 8.0 | 1.81 |

```
1세션 이내 청산 : 69.9%  (slice 71.2%)
20세션 초과     :  0.0%
연 회전 252회 -> 비용 드래그 90.2%/년
```
- **중앙값 1일 동일, 평균만 1.59 -> 1.81.** 왜곡 없다.
- 이유: 부분청산이 40.7%로 존재하지만 **대부분 같은 날~다음 날 안에 전량 정리**된다(전량 청산 99.7%).
- **(20)의 핵심 결론 "연 90% 비용 구조"는 견고하다.**

### B. 새로 드러난 것 - 분할매도가 비용을 배가한다
```
포지션당 매도 조각 평균 1.74개, 최대 11개
2회 이상 분할 청산 40.7%
```
- 수수료는 체결 건당 발생하므로 **조각 1.74개 = 매도 비용 약 1.74배**다.
- **90.2%/년 드래그는 이를 반영하지 않은 수치이며, 실제로는 더 높다.** 정확한 재계산은 하지 않았다.
- `stop_sell_ratio_pct=60` 등 부분청산 설정이 회전 비용을 직접 증폭시키는 구조다.

### C. 청산 사유 순위가 바뀐다
| slice 기준 | position 기준 (최종 청산) |
|---|---|
| STOP 138 (1위) | **FUNDAMENTAL_CRITICAL 64 (1위)** |
| FUNDAMENTAL_CRITICAL 69 | STOP 60 |

| 최종 사유 | n | 중앙 보유 | 중앙 청산% |
|---|---:|---:|---:|
| **FUNDAMENTAL_CRITICAL** | 64 | 1.0 | **+1.65%** |
| STOP | 60 | 1.0 | -4.18% |
| STOP_GAP | 42 | 1.0 | **-9.41%** |
| DDM_LIQUIDATE_L4 | 39 | 2.0 | -1.01% |
| SURGE_INTRADAY_REVERSAL | 32 | 0.0 | -2.53% |
| STOP_PREEMPTIVE_CLOSE | 19 | 1.0 | -4.57% |

- **포지션을 최종적으로 닫는 것은 손절이 아니라 펀더멘털 청산**이며, 유일하게 중앙 청산가가 플러스다.
- `STOP_GAP`은 -9.41%로 slice 기준(-8.41%)보다 나쁘다. 갭 손절은 분할 없이 한 번에 크게 잘린다.

### 검증 항목 판정
- 기능 PASS(330 포지션 재구성, slice/position 분포 대조, 수량 검증)
- 정합성 PASS((20)과 동일 결론, 전량 청산 99.7%로 재집계 타당성 확인)
- 운영 반영 NA(읽기 전용)
- 정책 NA / FAIL-CLOSED NA / 회귀 PASS((20) 수치 재현)

### 미검증
- **분할매도를 반영한 실제 비용 드래그 미계산.** 90.2%는 하한이다
- (21) E항의 -5% 조기 청산 110건 원인 여전히 미상
- `asset_type_overrides` 한글 키 깨짐 미확인

### 다음
1. 분할매도 반영 실비용 재계산 - 90.2%가 얼마나 더 커지는지
2. -5% 청산 원인 코드 추적
3. `asset_type_overrides` 문자열 확인

## 2026-08-19 (23) 진입 타이밍 - 급등 직후·고점·거래량 급증이 한 종목에서 겹친다

(22)에서 "사자마자 떨어진다"까지 확인했다. **어디서 사는지를 봤고, 세 조건이 겹치는 구조가 드러났다.**

### A. 급등 직후에 산다
포지션 325건의 진입일 기준 직전 수익률:
```
직전  1일 : 중앙 +2.90%   (65.8% 양수)
직전  5일 : 중앙 +9.28%   (69.3% 양수)
직전 20일 : 중앙 +12.99%  (68.3% 양수)
```
- 시장 평균이 0% 근처인 것을 감안하면 **극단적 강세 추종**이다.

### B. 경로별 - SURGE 계열이 압도적이다
| 경로 | n | 20일고점 대비 | **직전 5일** | 체결-종가 | 최종 손익 |
|---|---:|---:|---:|---:|---:|
| **SURGE_RUNTIME** | 45 | **+0.00%** | **+23.67%** | **+4.16%** | -2.67% |
| **SURGE** | 80 | **+0.00%** | **+20.75%** | +0.00% | -2.69% |
| INTRADAY | 55 | -12.10% | +1.11% | +0.46% | -3.19% |
| other | 106 | -8.50% | +4.72% | +0.00% | -0.20% |
| INTRADAY_REALTIME | 27 | -23.07% | -1.75% | -0.42% | -0.20% |
| **beta_harvest** | 12 | -15.30% | **-4.42%** | +0.00% | **+2.33%** |

- **SURGE 계열은 5일간 20~24% 오른 종목을 정확히 20일 고점(+0.00%)에서 산다.**
- `SURGE_RUNTIME`은 **체결가가 그날 종가보다 +4.16% 높다.** 사는 순간 이미 약 -4% 평가손이다.
- **유일한 흑자 경로 `beta_harvest`만 반대로 한다** - 직전 5일 -4.42%(하락 종목)를 사고 +2.33%. n=12로 작지만 방향이 정반대다.

### C. 그러나 "고점 매수라서 진다"는 성립하지 않는다
20일 고점 대비 위치별 결과:
| 진입 위치 | n | 중앙 손익 | 승률 |
|---|---:|---:|---:|
| <-10% | 122 | -1.68% | 28.7% |
| -10~-5% | 34 | -2.27% | 35.3% |
| -5~-2% | 31 | -0.95% | 38.7% |
| 고점 부근 | 131 | -1.39% | 35.1% |

- **고점에서 사도, 많이 빠진 것을 사도 결과가 비슷하다.** 전부 손실이고 승률 30~39%다.
- 단순한 "고점 매수" 설명은 기각된다. **어디서 사든 지고 있다.**

### D. 판정 - 진입 가격 위치가 아니라 종목 선택이다
- (17): D10(거래량 급증) 37.1% 편중, D10이 향후 20일 최악 분위
- (23) A/B: 급등 직후 + 20일 고점에서 매수
- (23) C: 진입 위치를 바꿔도 결과가 같음

**"급등 직후 + 거래량 급증 + 고점"이 한 종목에서 겹친다.** SURGE 경로가 그 셋을 동시에 만족하는 종목만 골라내는 구조이며, 그 집단 자체가 향후 하회한다.

- 진입 타이밍을 조정해도(더 눌렸을 때 사도) 해결되지 않는다. **종목군을 바꿔야 한다.**
- `beta_harvest`의 방향(하락 종목 매수)이 유일하게 흑자라는 점은 이 진단과 정합적이다. 단 n=12로 판정 불가.

### 검증 항목 판정
- 기능 PASS(325 포지션 진입 시점 복원, 경로별/위치별 분해)
- 정합성 PASS((17) D10 편중 및 (22) 즉시 하락과 정합)
- 운영 반영 NA(읽기 전용)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 체결가가 종가보다 높은 것(SURGE_RUNTIME +4.16%)이 장중 진입 때문인지 슬리피지인지 미구분
- `beta_harvest` n=12는 판정 불가. 방향만 관찰
- 진입 위치별 분석에 보유기간 통제를 넣지 않았다
- 종가 기준이라 장중 진입가 위치는 알 수 없다

### 다음
1. **`beta_harvest` 방향 확대 검정** - 하락 종목 매수가 실제로 나은지. 유일하게 반대 방향이고 유일하게 흑자다
2. -5% 청산 원인 코드 추적 (미해결 이월)
3. `asset_type_overrides` 문자열 확인 (미해결 이월)

## 2026-08-19 (24) [사전등록] 단기 역전 축 - `beta_harvest` 방향의 패널 검정

**실행 전 등록.**

### Layer 0 - 왜 이 가설인가
두 개의 독립적 증거가 같은 방향을 가리킨다.
1. **실제 체결**: `beta_harvest`만 직전 5일 -4.42%(하락 종목)를 사고, 6개 경로 중 **유일하게 흑자**(+2.33%). n=12로 판정 불가지만 방향이 정반대다.
2. **패널 연구**: `vol_ratio` 최저 분위(거래량 평온)가 이긴다. B1 +0.354% vs B10 -0.908%.
3. **문헌**: 1개월 이하 단기 구간은 역전(short-term reversal)이 보고되는 구간이다. 08-19 (2)에서 20일 모멘텀을 시험했을 때 "1개월은 역전 구간인데 모멘텀을 봤다"고 이미 적었으나 **그 축을 직접 검정하지 않았다.**

### 신호 정의 (고정)
```
reversal_5d = -(직전 5거래일 누적수익률)
```
- 부호를 뒤집어 **높을수록 많이 하락한 종목**이 되게 한다. 이렇게 하면 다른 문법과 동일하게 "IC 양수 = 신호가 유효"로 해석된다.
- 5일을 고른 이유: `beta_harvest`의 관측치(직전 5일 -4.42%)와 동일 구간. 사후 최적화가 아니라 관측 재현이다.

### 방향 사전 지정
**IC는 양수일 것으로 예상한다.** 즉 많이 하락한 종목이 향후 20일에 상회한다. 음수가 나오면 예상 실패이며, 사후에 "실은 추세추종이었다"로 재해석하지 않는다.

### 실행 조건 (기존과 동일)
| 항목 | 값 |
|---|---|
| lookback | 5 (신호 계산 구간) |
| hold | 20 |
| market | all |
| 창 | 2015-07 ~ 2026-08 전 기간 |
| 판정 | (11)의 5개 규칙 그대로. 수정 금지 |
| 구간 수 | 2,3,4 |
| 비용/유동성 | 0.358% 왕복, value >= 1e8 |

### 실패 조건 (결과 보기 전 고정)
하나라도 발생하면 `NOT_SUPPORTED`로 종료한다.
1. 전기간 IC의 **부호가 음수** (사전 예상과 반대)
2. 분위 구조가 **U자**(단조성 규칙 5 실패)
3. 2/3/4 구간 중 어느 하나라도 **부호 분열**

### 사전 예상 (사후 수정 금지)
- 유효 n은 `vol_ratio`와 유사한 약 135로 예상. 검정력은 충분할 것이다.
- **`vol_ratio`와 상관이 높을 가능성이 크다.** 급락한 종목은 거래량도 늘어나기 때문이다. 통과하더라도 **독립적 발견이 아닐 수 있으며**, 그 경우 두 신호의 상관과 증분 기여를 별도로 봐야 한다((16) B항의 "네 신호가 한 현상의 대리변수" 가설과 연결).
- 가장 가능성 높은 결과: **통과하되 `vol_ratio`와 중복.**

### 이 검정이 답하지 못하는 것
- 봉인 OOS가 아니다. 2015~2019는 breakout 검정에 이미 사용됐다. 결과는 `EXPLORATORY`다.
- 실제 시스템은 보유 1일인데 이 검정은 20일이다. **(20)의 지적이 그대로 적용된다.**

## 2026-08-19 (25) reversal `NOT_SUPPORTED` - 또 U자, 그리고 가격 문법 5개 중 4개가 U자

(24) 사전등록대로 실행했다. **실패 조건 2번(U자) 발생으로 종료한다.**

### A. 결과
```
FULL PERIOD  n=2710 eff=135  IC=+0.02113  block95=[+0.01265, +0.02933]  OK
power gate: sd 0.0957, |IC| 0.02113 -> required eff/seg 79
  2seg 67 < 79  DEFERRED_UNDERPOWERED
  3seg 45 < 79  DEFERRED_UNDERPOWERED
  4seg 33 < 79  DEFERRED_UNDERPOWERED
monotonicity FAIL
>>> NOT_CONFIRMED -- bucket structure not monotone
```
분위 구조:
```
B1:-0.730%  B2:+0.047%  B3:+0.141%  B4:+0.160%  B5:+0.240%
B6:+0.211%  B7:+0.156%  B8:+0.175%  B9:+0.077%  B10:-0.479%
spearman = +0.164 (필요 0.7)   양 극단 부호 반대 = False
```

### B. 사전등록 대조
| 항목 | (24) 사전 기록 | 실제 | 일치 |
|---|---|---|:---:|
| **방향** | **IC 양수** | **+0.02113** (CI 0 배제) | **O** |
| 유효 n | 약 135 | 135 | O |
| 가장 가능성 높은 결과 | "통과하되 vol_ratio와 중복" | **U자로 탈락** | **X** |

- 방향 예측은 맞았다. 그러나 실패 조건 2번에 걸린다.

### C. IC 양수의 의미가 예상과 다르다
```
B1  (많이 오른 종목) : -0.730%
B5~B8 (중간)         : +0.16 ~ +0.24%
B10 (많이 빠진 종목) : -0.479%
```
- **양 극단이 둘 다 지고 중간이 이긴다.** momentum/bollinger와 같은 모양이다.
- IC가 양수인 것은 **B1이 B10보다 더 나쁘기 때문**이지 "많이 빠진 것을 사면 이긴다"가 아니다. **B10도 진다.**

### D. 검정력도 부족하다
- IC가 작아(+0.021, `vol_ratio`의 약 절반) `required_eff`가 16 -> **79**로 급증했다. 구간 판정 자체가 불가능하다.

### E. `beta_harvest` 가설 기각
- (23)에서 `beta_harvest`만 하락 종목을 사고 유일하게 흑자(+2.33%, n=12)였다. **그 방향을 패널로 재현하려 했으나 실패했다.**
- **하락 종목(B10)도 진다.** `beta_harvest`의 흑자는 "하락 종목을 사서"가 아니며, 다른 이유가 있거나 n=12의 우연이다.

### F. 누적 패턴 - 가격 문법 5개 중 4개가 U자
| 문법 | rho | 구조 | 판정 |
|---|---:|---|---|
| momentum | -0.079 | **U자** | NOT_CONFIRMED |
| bollinger | -0.139 | **U자** | NOT_CONFIRMED |
| **reversal** | **+0.164** | **U자** | **NOT_SUPPORTED** |
| breakout | +0.964 | 단조 | NOT_SUPPORTED (OOS 부호 반전) |
| **vol_ratio** | **-0.915** | **단조** | CONFIRMED (EXPLORATORY) |

- **가격 기반 문법은 전부 U자이고, 거래량 기반만 단조다.**
- (16) B항의 "네 신호가 한 현상의 대리변수" 가설이 다섯 번째 사례로 강화된다. 각 U자의 양 극단은 "많이 오른 것"과 "많이 빠진 것"이며, **둘 다 지고 중간이 이긴다**는 결과가 반복된다.
- 이는 **가격 극단 자체가 나쁜 것**이지 방향(추세/역전)의 문제가 아님을 시사한다. 다만 이 해석은 검정하지 않았다.

### 검증 항목 판정
- 기능 PASS(사전등록대로 실행, vol_ratio 회귀 불변 확인)
- 정합성 PASS(사전등록 실패 조건과 1:1 대조, 예상 실패 명시)
- 운영 반영 PASS(`compute_signal()`에 `reversal` 추가, py_compile PASS)
- 정책 NA / FAIL-CLOSED NA / 회귀 PASS(vol_ratio IC -0.04476 불변)

### 미검증
- "가격 극단이 나쁘다"는 통합 가설을 직접 검정하지 않았다. 5개 문법의 상위/하위 분위 종목 중복률을 재면 된다
- `reversal`과 `vol_ratio`의 상관 미측정. (24)에서 중복 가능성을 예고했으나 U자 탈락으로 확인 못 함
- `beta_harvest` 흑자의 실제 원인 미규명

### 다음
1. **"가격 극단 회피" 통합 가설 검정** - 5개 문법의 극단 분위가 같은 종목을 가리키는지. 상관/중복률 측정
2. -5% 청산 원인 (미해결 이월)
3. `asset_type_overrides` 문자열 (미해결 이월)

## 2026-08-19 (26) 극단성 통합 가설 - 실패 4건이 하나의 발견이었다 (단, 사후 구성)

(25) F항의 "가격 극단이 나쁘다" 가설을 직접 검정했다. **지지되며, 개별 문법을 전부 이긴다.**

### A. 극단 분위가 실제로 겹친다
각 쌍의 극단 분위(양 꼬리) 종목 중복률, 우연이면 10%:
```
momentum  & bollinger  36.5%   <- 가격끼리
momentum  & reversal   33.6%   <- 가격끼리
bollinger & reversal   33.4%   <- 가격끼리
momentum  & vol_ratio  26.7%
bollinger & vol_ratio  29.0%
reversal  & vol_ratio  28.3%
```
- **가격 문법끼리는 3.3~3.7배 중복.** 같은 종목을 다르게 부르고 있었다.

### B. 통합 지표가 개별 문법을 전부 이긴다
```
extremity = 세 가격 문법(momentum/bollinger/reversal)의 |횡단면 z| 평균

IC = -0.06779   block95=[-0.07541, -0.05946]   0 배제
spearman = -0.988   양 극단 부호 반대 = True   -> MONOTONE

B1:+0.313%  B2:+0.261%  B3:+0.188%  B4:+0.147%  B5:+0.122%
B6:+0.140%  B7:+0.039%  B8:-0.020%  B9:-0.132%  B10:-1.054%
```

| 신호 | IC | rho | B1 | B10 |
|---|---:|---:|---:|---:|
| **extremity** | **-0.06779** | **-0.988** | **+0.313%** | **-1.054%** |
| bollinger | -0.05172 | -0.612 | -0.056% | -0.418% |
| momentum | -0.04811 | -0.612 | +0.199% | -0.863% |
| vol_ratio | -0.04385 | -0.915 | +0.342% | -0.874% |
| reversal | +0.02217 | +0.164 | -0.688% | -0.443% |

- **IC -0.068로 최고, 단조성 -0.988로 가장 깨끗하다.** U자 4개가 하나의 단조 신호로 합쳐졌다.

### C. 해석 - "실패 4건"이 하나의 발견이었다
> **가격이 어느 방향으로든 극단으로 간 종목은 향후 20일에 하회한다.**

- momentum과 reversal이 각각 U자였던 이유가 같다. **둘 다 극단의 한쪽만 보고 있었다.** 합치니 단조가 된다.
- (23)의 진입 진단과 정확히 맞물린다. SURGE 경로는 "직전 5일 +20% + 20일 고점 + 거래량 급증"을 동시에 만족하는 종목을 사는데, 그것이 **극단성 최상위 분위**다.
- AGENTS.md 17("결과를 모아 분석하라")의 실제 사례다. 개별 PASS/FAIL로 닫았다면 이 발견은 나오지 않았다.

### D. 그러나 이 결과는 아직 증거가 아니다
1. **사후 구성이다.** 네 문법의 U자를 본 뒤에 만든 지표다. (10)과 같은 오염이며 **사전등록 없이 나온 결과**다.
2. **5개 규칙 정식 판정을 돌리지 않았다.** 구간 안정성/검정력 게이트 미적용.
3. **봉인 OOS 없음.**
4. z-score가 극단값에 민감한데 **winsorize를 하지 않았다.**
5. **B10 -1.054%가 상한가/하한가 종목 때문일 가능성 미확인.** 극단성 정의상 그런 종목이 몰릴 수밖에 없고, 그러면 체결 불가 구간을 재고 있는 것이다.

### E. 다음 단계 설계 (오염을 인정한 상태에서)
- 이 지표로 재판정하더라도 **`EXPLORATORY`를 넘을 수 없다.** breakout이 정확히 이 자리에서 죽었다.
- 진짜 확증 경로는 하나뿐이다: **아직 관측하지 않은 데이터**. 2015년 이전 구간을 새로 수집해 봉인해야 한다. (13)에서 2015~2019를 이미 썼으므로 그보다 이전이어야 한다.
- pykrx는 2015-01까지 확인됐다((12) A항). 그 이전 가용성은 미확인.

### 검증 항목 판정
- 기능 PASS(중복률 측정, 통합 지표 구성 및 실행, 개별 문법 대조)
- 정합성 PASS((23)(25)와 정합, 중복률이 가설을 지지)
- 운영 반영 NA(읽기 전용, 스크래치패드에서만 계산. 하네스 미반영)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- 위 D의 1~5 전부
- `extremity`를 하네스에 넣지 않았다. 정식 판정을 돌리려면 필요하다
- 세 문법의 z를 단순 평균했다. 가중이나 다른 결합 방식은 시도하지 않았다
- 실제 시스템은 보유 1일인데 이 검정은 20일이다((20) 지적 그대로 적용)

### 다음
1. **2015년 이전 데이터 가용성 확인** - 봉인 OOS 확보 가능 여부. 이것이 되어야 (26)이 증거가 된다
2. `extremity`를 하네스에 추가하고 5개 규칙 정식 판정 (결과는 EXPLORATORY)
3. B10의 상한가/하한가 비중 확인 - 체결 가능성
4. -5% 청산 원인, `asset_type_overrides` (미해결 이월)

## 2026-08-19 (27) 봉인 OOS 확보 불가 - pykrx 하한이 2014-05-28이다

(26) 다음-1. **`extremity`를 확증할 경로가 구조적으로 없다.**

### A. pykrx 데이터 하한 (실측)
```
005930: rows=2999  first=2014-05-28  last=2026-08-18
000660: rows=2999  first=2014-05-28  last=2026-08-18
035720: rows=2999  first=2014-05-28  last=2026-08-18
051910: rows=2999  first=2014-05-28  last=2026-08-18
```
- **네 종목 모두 행 수 2,999로 동일하고 시작일도 동일하다.** 날짜 제한이 아니라 **행 수 제한(약 12년)**이다.
- 2005/2008/2010/2012/2014-01 요청은 전부 0행을 반환한다.

### B. 자체 스크립트 오류 (정정)
- 최초 탐색 스크립트가 "10종목 x 5년 = 2,554행"을 출력해 마치 2010~2014가 수집된 것처럼 보였다. **실제로는 2014-05~12의 7개월분만 담겨 있었다.**
- 요청 구간과 반환 구간을 대조하지 않은 설계 결함이다. 수집 스크립트에는 항상 **반환된 실제 날짜 범위를 출력**해야 한다.

### C. 결론 - 확증 경로가 없다
| 경로 | 가능 여부 |
|---|---|
| 2015년 이전 봉인 OOS | **불가** - pykrx 하한 2014-05-28 |
| 2015~2019 재사용 | **불가** - (13) breakout 검정에 소진, 오염 |
| 새로 얻을 수 있는 구간 | 2014-05~12, 약 145 거래일. **룩백 120 제외 시 유효 n 약 1** |
| 미래 데이터 축적 | 가능하나 20거래일당 유효 n 1개 -> 수년 소요 |
| 다른 시장 | 데이터 없음 |

- **`extremity`는 현재 데이터로 `EXPLORATORY`를 벗어날 수 없다.** 표본 부족이 아니라 구조적 제약이다.
- 같은 제약이 `vol_ratio`에도 적용된다. (15)에서 "봉인 OOS가 다음 단계"라고 적었으나 **그 단계가 존재하지 않는다.**

### D. 남은 선택지 (판단 필요)
1. **`EXPLORATORY` 상태를 인정하고 전진** - 확증 없이 다음 층(후보 선별)으로. 가드 6장 위반이며 breakout이 이 자리에서 죽었다는 전례가 있다
2. **시간 분할 확증으로 대체** - 2015~2026을 탐색/확증으로 나눔. 단 이미 전 구간을 관측했으므로 진짜 홀드아웃이 아니다
3. **미래 데이터 축적을 기다림** - 정직하지만 수년
4. **다른 데이터 소스 확보** - KRX 직접, 유료 벤더 등. 미조사

### 검증 항목 판정
- 기능 PASS(4종목 하한 실측, 행 수 제한 확인)
- 정합성 **FAIL(자체 스크립트 기준)** - 요청 구간과 반환 구간을 대조하지 않아 오판했다
- 운영 반영 NA(읽기 전용, 수집 미실행)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### E. 로컬 대안도 없다 (확인 완료)
- `E:\1_Data` 전체에서 2015년 이전 가격 parquet를 탐색했다. **없다.** (매칭된 2건은 파일명의 `2020`/`2022`가 정규식에 걸린 오탐)
- `Raw/` 폴더에는 `krx_daily_20221001_20251224.parquet` 하나뿐이다.

### 미검증
- pykrx 외 데이터 소스 미조사 (KRX 정보데이터시스템 직접 조회, 유료 벤더)
- 2,999행 제한이 pykrx 구현 제약인지 KRX API 제약인지 미확인. pykrx 소스를 보면 판별 가능

## 2026-08-19 (28) -5% 조기 청산 원인 규명 - 설정 -12%는 11.7%에서만 적용된다

(21) E항의 미해결 항목. **원인이 확정됐고, (20)의 회전율과 직결된다.**

### A. 메커니즘 - `max()`가 항상 가장 좁은 손절을 고른다
`paper_engine/exit.py`에서 손절 후보가 세 번 합쳐진다.
```python
L767:  stop = _resolve_stop_loss_pct(pos, sell_rules, stop_loss)   # 설정 -12%
L779:  stop = max(_atr_stop, stop)          # ATR: max(-(ATR14 * 2), -0.20)
L824:  stop = max(float(_se_stop), stop)    # SURGE: surge_exit_policy.stop_loss_pct
```
- 음수 비율에서 `max()`는 **항상 더 좁은(0에 가까운) 쪽**을 남긴다. 주석도 `keep tighter stop, never loosen`으로 **의도된 설계**다.
- 문제는 세 규칙이 겹치면 **가장 좁은 것만 남는다**는 점이다. `-12%`는 사실상 상한선일 뿐이다.

관련 라이브 설정:
```
sell_rules.stop_loss.default_pct  : -12
atr_stop_multiplier               : 2
surge_exit_policy.enabled         : True
surge_exit_policy.stop_loss_pct   : -0.03    <- -3%
min_hold_days                     : 2  (min_hold_protect_stop_loss: False)
```

### B. 실효 손절 재구성 (573 lot)
| 구속 규칙 | 비중 | 실효 손절 중앙값 |
|---|---:|---:|
| **ATR** (`atr_stop_multiplier=2`) | **46.9%** | -7.76% |
| **SURGE** (`stop_loss_pct=-0.03`) | **41.4%** | **-3.00%** |
| config (-12%) | **11.7%** | -12.00% |

```
실효 손절 분포: p05 -12.00% / p25 -8.87% / p50 -5.68% / p75 -3.00% / p95 -3.00%
평균 -6.20%   (설정 -12%)
```
- **설정값 -12%가 구속하는 것은 11.7%뿐이다.**

### C. 예측이 실측과 일치한다
```
stop 계열 청산 262건
  재구성한 실효 손절 중앙값 : -5.66%
  실제 청산 중앙값          : -5.00%
```
| reason | n | 실효 손절 | 실제 청산 | SURGE 비중 |
|---|---:|---:|---:|---:|
| STOP | 138 | -5.66% | -4.00% | 42.0% |
| STOP_PREEMPTIVE_CLOSE | 64 | -7.88% | -5.56% | 21.9% |
| STOP_GAP | 60 | **-3.00%** | **-8.35%** | 73.3% |

- **(21)에서 "원인 미상"으로 남긴 -5% 조기 청산이 설명됐다.**
- `STOP_GAP`의 실효 -3.00% vs 실제 -8.35%는 **갭으로 손절선을 뛰어넘어 시가 체결**된 결과다. SURGE 비중이 73.3%로 가장 높다.

### D. SURGE가 구조적으로 가장 심하다
```
SURGE 포지션 손절      : -3.00%
그 종목들의 일변동(프록시): 중앙 4.43%  (14일 평균 |일간수익률|)
```
- **하루 평균 변동이 4.43%인 종목에 -3% 손절을 건다.** 정상적인 일중 변동만으로 손절이 발동한다.
- (23)에서 확인한 SURGE 진입 특성(직전 5일 +20~24% 급등)과 결합하면, **변동성이 큰 종목을 골라 가장 좁은 손절을 거는** 구조다.

### E. 이것이 (20) 회전율의 직접 원인이다
```
-3% 손절 + 일변동 4.43%  ->  하루 만에 손절
->  보유 중앙값 1일  ->  연 252회전  ->  연 90% 비용
```
- (20)에서 "회전율이 수익을 산술적으로 불가능하게 한다"고 적었고, **그 회전율의 원인이 여기다.**
- 진입 신호를 바꾸는 것과 무관하게 **설정만으로 회전율을 낮출 수 있다.** 다만 이는 매매 정책 변경이며 AGENTS.md 5 대상이다.

### 검증 항목 판정
- 기능 PASS(코드 경로 3곳 확인, 라이브 설정 판독, 573 lot 실효 손절 재구성)
- 정합성 PASS(재구성 중앙값 -5.66% vs 실측 -5.00%로 근접)
- 운영 반영 NA(읽기 전용, 코드/설정 변경 0건)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- **ATR은 프록시다.** 패널에 고가/저가가 없어 14일 평균 |일간수익률|로 대체했다. 엔진의 실제 `atr14_pct`와 다를 수 있다
- `min_hold_days=2`인데 `min_hold_protect_stop_loss=False`라 보호 기간에도 손절이 작동한다. 이 조합이 의도인지 미확인
- `surge_type_stop_pct` 포지션별 오버라이드(L819)의 실제 분포 미확인
- `asset_type_overrides` 한글 키 깨짐 여전히 미확인

### 다음 - 판단 필요
회전율을 낮추는 방법이 셋이고 **전부 매매 정책 변경**이라 사용자 승인이 필요하다.
1. `surge_exit_policy.stop_loss_pct` -0.03 완화 (SURGE 41.4%에 영향)
2. `atr_stop_multiplier` 2 -> 상향 (ATR 46.9%에 영향)
3. 위 둘을 유지하되 **SURGE 진입 자체를 줄임** - (23)(26)에 따르면 그 종목군이 문제의 근원이다

**3번이 근본적이다.** 1·2는 나쁜 종목을 더 오래 들고 있게 만들 수도 있다. 어느 쪽도 검정 없이 적용해서는 안 된다.

## 2026-08-19 (29) SURGE 진입 축소 ExecPlan 착수 - 세 방안이 모두 근거 부족으로 무너짐

(28) 다음-3(SURGE 진입 축소) 진행. **ExecPlan을 작성하고 전제를 조사한 결과 A/B/C 모두 실행 근거가 없다.**

### A. 작성물
`docs/exec-plans/active/20260819_surge_entry_reduction.md` (초안, 미적용)
- 분류: 매매 정책 변경(AGENTS.md 5). 9/11/14 전부 적용 대상
- **어떤 설정도 변경하지 않았다.**

### B. SURGE 영향 재측정 - 손실의 94.4%
청산 완료 330 포지션, production 비용:
| 경로 | n | 비중 | 순손익(원) | ROI |
|---|---:|---:|---:|---:|
| other | 107 | 32.4% | -144,650 | -0.06% |
| **SURGE** | 82 | 24.8% | **-1,197,980** | -1.13% |
| INTRADAY | 57 | 17.3% | -50,956 | -0.82% |
| **SURGE_RUNTIME** | 45 | 13.6% | **-55,769** | -1.04% |
| INTRADAY_REALTIME | 27 | 8.2% | -41,288 | -1.36% |
| beta_harvest | 12 | 3.6% | +162,540 | +2.10% |

```
전체       n=330  -1,328,102원  ROI -0.38%
SURGE 제외 n=203     -74,354원  ROI -0.03%
SURGE 기여 -1,253,748원 = 손실의 94.4%
```
- **포지션의 38.4%가 손실의 94.4%를 만든다.**
- 매매가 사라지지 않는다: 진입일 73일 중 54일 유지. 2025-12~2026-03은 SURGE 0건이었다.

### C. C안(한도 축소) 기각 - 한도가 이미 안 지켜진다
```
surge_entry_policy.max_new_surge = 6
실제 일평균 3.53건, 중앙값 3건
한도 6 도달: 36일 중 5일(13.9%)
한도 6->3으로 낮춰도 33%만 제거
```
그리고 **한도를 초과한 날이 4일 있다.**
| 날짜 | fills | 고유 종목 | 원인 |
|---|---:|---:|---|
| 20260513 | 12 | **10** | split_entry 12건 전부. **종목 10개는 분할로 설명 안 됨** |
| 20260515 | 10 | 4 | split_entry 10건. 4종목을 10번 분할 |
| 20260514 | 9 | 6 | split_entry 9건 |
| 20260416 | 7 | 6 | split_entry 7건 |

- 대부분 **분할매수가 fill 수를 부풀린 것**이지만 05-13의 고유 종목 10개는 한도 6을 넘는다. **부분 미확인.**
- 설정이 지켜지지 않는 상태에서 한도만 낮추는 것은 근거가 약하다.

### D. B안(점수 문턱 상향) 기각 - 점수가 아무것도 예측하지 못한다
```
spearman(surge_score_final, 실현 수익%) = -0.030   n=96
```
| 점수 구간 | n | ROI | 승률 |
|---|---:|---:|---:|
| 75~82 | 18 | -4.64% | 22.2% |
| 82~90 | 28 | -3.55% | 17.9% |
| 90~99 | 6 | -2.22% | 33.3% |
| **99~101** | **44** | -3.58% | **13.6%** |

- **만점(99~100)이 44건인데 승률 13.6%로 가장 낮다.**
- 문턱 상향 효과:
```
>= 75 : ROI -3.77%
>= 85 : ROI -3.34%   (42.7% 제거)
>= 95 : ROI -3.43%
```
- **42.7%를 잘라내고 0.4%p 개선.** 근거가 없다.
- 근본 원인: **96건 중 44건(46%)이 99~100점**이다. 점수 분포가 상단에 뭉쳐 변별력이 없다.
- 현행 문턱 75는 실질적으로 무의미하다. 실제 진입 최저 점수가 78.5로 문턱보다 높다.

### E. A안(vol_ratio 필터)도 약하다
- (27)에서 확인된 대로 `vol_ratio`는 봉인 OOS 확보가 **구조적으로 불가능**하며 `EXPLORATORY`를 벗어날 수 없다.
- (19)에서 실현 체결 기준 D10 제외 효과가 `DEFERRED`로 나왔다. 패널 발견이 체결로 이전된다는 증거가 없다.

### F. 부수 발견 - `LIMIT_UP_NEAR`는 설계된 우회다
- `type_policy.blocked_types`에 `LIMIT_UP_NEAR`가 있으나 **8건이 진입**했다.
- 전부 `SURGE_RUNTIME` 경로이며, `wait_reclaim_paper_probe`에 `bypass_type_block: true` + `allowed_types: ["LIMIT_UP_NEAR"]`가 있다. **명시적으로 설계된 우회다.**
- 상한가 근접 종목을 차단 목록에 넣고 프로브 경로로 다시 여는 구조이며, 의도인지 잔재인지 미확인.

### G. 판정
| 안 | 상태 |
|---|---|
| A (vol_ratio 필터) | 지표가 `EXPLORATORY`, 봉인 OOS 불가, 실현 검정 `DEFERRED` |
| B (점수 문턱) | **점수가 무의미**(rho -0.030), 만점 46% |
| C (한도 축소) | **한도가 이미 안 지켜짐**, 33%만 제거 |

- **SURGE 내부에서 좋은 것과 나쁜 것을 가를 방법이 현재 없다.** 점수도 타입도 한도도 작동하지 않는다.
- 남는 선택지는 부분 조정이 아니라 **SURGE 전체를 끄거나 유지하거나**이며, 이는 훨씬 큰 결정이다.

### 검증 항목 판정
- 기능 PASS(330 포지션 경로별 손익, 127 SURGE 진입의 한도/점수/타입 분해)
- 정합성 PASS((17)(23)(28)과 정합)
- 운영 반영 NA(읽기 전용. ExecPlan 초안 작성만, 설정 변경 0건)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- **20260513의 고유 종목 10개가 어떻게 한도 6을 넘었는지** (분할로 설명 안 됨)
- SURGE 축소 시 자본이 어디로 재배치되는지. 현재 분석은 회계이지 반사실이 아니다
- `LIMIT_UP_NEAR` 우회가 의도인지 잔재인지
- `surge_score_final` 만점 46%의 원인 - 점수 산식이 상한에 걸리는지
- config lock 해시 불일치 상태에서 설정 변경이 가능한지

### 다음 - 판단 필요
1. **SURGE 전면 비활성화 여부** - 손실의 94.4%이나 되돌리기가 크고 매매의 38%가 사라진다
2. 유지한다면 **무엇을 근거로** 유지하는지 명시 필요. 현재 점수·타입·한도 어느 것도 변별력이 없다
3. `surge_score_final` 산식 자체를 재검토 (만점 46%는 점수로서 실패)

## 2026-08-19 (30) [중대 정정] 급등매매와 일반매매를 분리하니 결론이 갈린다

사용자 지적: **"급등매매와 일반매매 로직이 따로 분리되어 있는 걸로 안다."** 맞다. 그리고 그 구분을 하지 않은 채 오늘 하루 "시스템에 엣지가 없다"를 반복했다.

### A. 분리 결과 - 일반매매는 사실상 본전이다
| 로직 | n | 비중 | 순손익(원) | ROI | 승률 | 중앙 |
|---|---:|---:|---:|---:|---:|---:|
| **NORMAL** | 203 | 61.5% | **-74,354** | **-0.03%** | 38.9% | -0.72% |
| **SURGE** | 127 | 38.5% | -1,253,748 | -1.12% | 21.3% | -3.13% |
| TOTAL | 330 | | -1,328,102 | -0.38% | | |

- **오늘 반복한 "엣지 없음"은 SURGE 이야기였다.** 일반매매는 -0.03%로 본전에 가깝다.
- (20)의 "연 90% 비용 구조에서 어떤 신호로도 수익 불가"도 **SURGE 기준 회전율**에 크게 의존한다. 일반매매의 회전율은 따로 재지 않았다.

### B. 그러나 NORMAL 안에서 시기가 갈린다
```
2025-12~2026-03 :  46건  -3,377,928원  ROI -3.601%  승률 34.8%  중앙 -5.33%
2026-04~2026-08 : 157건  +3,303,574원  ROI +2.245%  승률 40.1%  중앙 -0.56%
```
**부호가 뒤집힌다.** 그러나 "개선됐다"로 읽으면 안 된다.

**1) 소수 포지션이 지배한다**
```
post +3,303,574  ->  상위 3건 제외 시 +1,288,476   (상위 3건이 61%)
pre  -3,377,928  ->  하위 3건 제외 시 -1,694,375   (하위 3건이 50%)
```

**2) 구성이 바뀌었다**
| 하위 경로 | pre | post |
|---|---:|---:|
| plain | 45건 (-3,397,524) | 35건 (**+2,631,722**) |
| same_close | 1건 (+19,596) | 26건 (+601,556) |
| intraday(untagged) | - | 57건 (-50,956) |
| INTRADAY_REALTIME | - | 27건 (-41,288) |
| beta_harvest | - | 12건 (+162,540) |

- **5개 중 3개가 4월 이후에만 존재한다.** 단순 전후 비교는 구성 변화를 섞는다.
- 다만 `plain`은 양쪽에 있고 -3.4M -> +2.6M로 뒤집혔다. **이것만은 구성 변화가 아니다.** 단 35건이다.

### C. NORMAL 하위 경로별 - 흑자 둘
| 경로 | n | 순손익(원) | ROI | 승률 |
|---|---:|---:|---:|---:|
| **beta_harvest** | 12 | +162,540 | **+2.10%** | **75.0%** |
| **same_close** | 27 | +621,152 | **+1.27%** | 48.1% |
| plain | 80 | -765,802 | -0.44% | 43.8% |
| intraday(untagged) | 57 | -50,956 | -0.82% | 24.6% |
| INTRADAY_REALTIME | 27 | -41,288 | -1.36% | 29.6% |

- **`same_close`(종가 진입)를 오늘 한 번도 따로 보지 않았다.** n=27로 `beta_harvest`(12)보다 표본이 크고 ROI +1.27%다.
- (25)에서 `beta_harvest` 흑자를 "하락 종목 매수" 가설로 검정해 기각했으나, **`same_close`는 다른 가설이다.** 진입 타이밍(종가) 축이며 미검정이다.

### D. 청산 엔진은 공유한다
| exit_reason | NORMAL | SURGE |
|---|---:|---:|
| FUNDAMENTAL_CRITICAL | 58 | 6 |
| STOP | 34 | 26 |
| STOP_GAP | 16 | 28 |
| DDM_LIQUIDATE_L4 | 23 | 16 |
| SURGE_INTRADAY_REVERSAL | **0** | 32 |
| HOLD_CLOSE_DROP | 12 | **0** |

- 진입은 분리돼 있으나 **청산 엔진은 대부분 공유**한다. `SURGE_INTRADAY_REVERSAL`과 `HOLD_CLOSE_DROP`만 배타적이다.
- 따라서 (28)의 손절 진단(실효 -5.68%)은 NORMAL에도 적용되나, `surge_exit_policy.stop_loss_pct=-0.03`은 SURGE 전용이다. **NORMAL의 실효 손절은 따로 재지 않았다.**

### E. 오늘 기록 중 정정이 필요한 것
| 블록 | 서술 | 정정 |
|---|---|---|
| (20) | "회전율이 수익을 산술적으로 불가능하게 한다" | **SURGE 기준.** NORMAL 회전율 미측정 |
| (23) | "사자마자 떨어진다" | SURGE·INTRADAY 중심. NORMAL 전체가 아님 |
| (29) | "SURGE가 손실의 94.4%" | **유효.** 오히려 이 블록이 분리의 근거였다 |
| 전반 | "시스템에 엣지가 없다" | **SURGE에 해당.** NORMAL은 -0.03%로 판정 유보 |

### 검증 항목 판정
- 기능 PASS(330 포지션 로직별 분해, 시기·하위경로·청산사유 교차)
- 정합성 PASS((29)의 SURGE 94.4%와 일치)
- 운영 반영 NA(읽기 전용)
- 정책 NA / FAIL-CLOSED NA / 회귀 NA

### 미검증
- **NORMAL의 보유기간·회전율·실효 손절을 따로 재지 않았다.** (20)(28)은 전체 혼합 기준이다
- `plain`의 pre/post 부호 반전(-3.4M -> +2.6M, 35건)의 원인
- `same_close` 흑자(+1.27%, n=27)의 원인. 오늘 한 번도 조사하지 않았다
- 4월 전후 구성 변화가 코드 변경 때문인지 시장 때문인지
- 일반매매 로직의 정의를 코드에서 확인하지 않았다. `is_surge` 부정으로 분류했을 뿐이다

### 다음
1. **NORMAL 단독 회전율·손절 측정** - (20)(28)의 결론이 NORMAL에도 적용되는지
2. `same_close` 경로 조사 - 표본이 가장 큰 흑자 경로인데 미조사
3. 일반매매 로직의 코드상 정의 확인 - 현재 분류는 소거법이다
