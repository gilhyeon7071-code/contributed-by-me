# v41.1 부품 목록과 실측 현황 (2026-09-16)

- 용도: v41.1 부품 단위 검증·판정 절차(OBJECTIVE_LEDGER 개정 2026-09-16)의 **1단계 재료**
- 성격: 읽기 결과예요. 코드·설정은 안 건드렸어요. 새로 잰 것도 없어요
- 출처: `generate_candidates_v41_1.py` DEFAULT_PARAMS(101~135행), `paper_engine/*.py` 함수 목록,
  `paper_engine/config.py` regime_overrides, CONCLUSION_REGISTER, VERIFICATION_REGISTER, 기억 메모
- 판정 잣대(개정문): **실측에서 양의 효과가 있거나, 없으면 매매를 막는 안전장치이거나. 둘 다 아니면 지움**
- 상태 칸: `양(실측)` / `음(실측)` / `무효과(실측)` / `미시험` / `보류` / `철회` / `수리됨` / `결함`
  - "미시험" 은 존재만 확인된 것. 인용 금지. 절차에서 실측 대상이에요

## 1. 유니버스·배제형 필터 (2026-09-10 실측: 배제형 평균 +65bps, 6개 중 5개 양수)

| 부품 | 값 | 상태 | 근거 |
|---|---|---|---|
| `value_min` | 10억 (09-10 되돌림, 원래 1,550억) | 양(실측)·**경계 미측정** | 1,550억이 L7 의존의 실제 차단자였음(07-27). 10억 쪽 효과는 안 쟀음 |
| `min_listing_days` | 126 | 양(실측) | listing 배제형 |
| `atr_max` | 0.12 | 양(실측) | 배제형. 단 ATR 축은 D9 회피 신호(−1.66 t=−4.10)로도 확인 |
| `stretch_max` | 1.19 | 양(실측) | 배제형, 통과율 ~95% |
| `rsi_max` | 70 | 양(실측) | 배제형 |
| `rs_lim` | 0.05 (07-28 정의 수정 후) | 양(실측)·**구속 아님** | 53% 통과. D6 "역방향이 기아 기여" 는 철회(09-10) |
| `exclude_administrative / investment_warning / investment_risk` | 1.0 | 미시험 | 제도 사실이라 안전장치로 남길 후보 |
| `junk_risk_enable` + `junk_hard_threshold 88` | 1.0 | **보류** | 전 종목에서는 모멘텀 알파 역방향, 후보 집합에서는 표본 부족 |
| `watch_penalty_caution` | 0.10 | 미시험 | |
| `vol_close_corr_min` | 0.0 | 꺼짐 | |

## 2. 선택형 게이트 = 신호 (2026-09-10 실측: 평균 −14bps, 4개 중 1개 양수)

| 부품 | 값 | 상태 | 근거 |
|---|---|---|---|
| `v_accel_lim / v_accel_max` | 2.5 / 5.0 | **음(실측)·범위한정** | 진입 조건(v_accel>6.6 & rs>−0.04)이 11.6년 유니버스보다 −2.33%p. L0 값 기준이며 생산은 L6 실효값. H004: v_accel D1 −0.50 t=−3.25 (회피 신호) |
| `near_52w_high_gap_max` | 0.25 | 음(실측) | 선택형 high52 |
| `require_macd_golden` | 0.0 (꺼짐) | 미시험 | 사다리가 완화 안 함. 켜져 있던 시절 실제 차단자(07-27) |
| 백테스트 전용 6게이트 (`v_accel_max 5.0`, `defense_bear_*`, `sector_blacklist`, `min_market_cap`, `require_above_ma200`) | — | **결함** | 생산엔 없음. 백테스트 필터는 논리적 공집합(v_accel>6.6 AND <=5.0) |

## 3. 시장·레짐

| 부품 | 상태 | 근거 |
|---|---|---|
| `mkt_ret20_min / mkt_ret60_min / sector_rs_min` = −1.0 | 꺼짐 | 사실상 미사용 |
| `defense_bear_rs_slope_min / disable_entry` = 0 | 꺼짐 | MA60 곰장 게이트는 **반증**(07-25: 2022 worst fold 악화) |
| `rule_e` (시장 수준, 사다리 미완화) | **양(실측)** | n=133, OOS PF 1.55; 생산 배선 08-14; 후보 발생일 29%→5.6% |
| `regime_entry_policy` (RALLY probe / CRASH 차단 / BEAR) | 미시험 | config.py 435~460. 순위층은 레짐 미반영(08-24 감사) |
| `crash_risk_off` | **결함** | 08-07 폴백 데이터로 판정한 이력 |
| 매크로 신선도 | 수리됨 | 기준일 today 분리(08-21). 절단선은 미결 |

## 4. 점수·순위

| 부품 | 상태 | 근거 |
|---|---|---|
| `w_rs .20 / w_rs_slope .55 / w_v_accel .25` → tech | **음(실측)** | 순위 점수에 기술 지표 spearman −0.446(08-20). ARM_SCORE 10년·N50 매수 알파 없음(H003) |
| `w_tech .75 / w_fund .25` | 미시험 | 재무 IC는 look-ahead 78%로 무효 |
| 7개 명목 가중치 | **사체** | 산술 참조 0건(08-24). 라벨은 실효값으로 정정됨 |
| `execution_lob_adjustment`(가산) / `medium_news_adjustment`(승산) | 미시험 | 검증기준 없는 조정항. 장중 순위를 이것만 움직임 |
| NEWS_ONLY 유령행 (B1) | **결함** | 진입 미도달이지만 부작용으로 막힘. 명시 드롭 없음 |
| `company_analyzer` | 꺼짐(07-24) | 98% 중복 + 결함 3 |
| 순위 자체의 가치 | 무효과(실측) | 순위 상한 +5.35%p 이론, 현실 +0.4%p < 비용(08-22) |

## 5. 완화 사다리 L0~L9

| 부품 | 상태 | 근거 |
|---|---|---|
| L0~L6 | **무효과(실측)** | 이미 열린 게이트를 느슨하게 함(07-27) |
| L7~L9 | 결함이었다가 복구 | 04-24 안전망 유실 06-09~06-25, 07-24 복구. L7이 사실상 정상 |
| 사다리 전체 | 결함 | 후보 83~92%가 폴백 observe_only(09-09). `_relax_ladder` 대신 rule_e 가 실제 게이트 |

## 6. 진입·수량

| 부품 | 상태 | 근거 |
|---|---|---|
| 수량 축소기 곱 (`budget_cap → split_first → normal_qty_reduce → weight_adjust → post_sector_limits → surge_budget_fit`) | **결함·미수리** | 09-12 곱 0.0095. **09-16 재확인: 14→1, 73→4, 33→1 (3~7%)**, 09-11 원장 shrink 0.012~0.2 |
| `split_entry 40%`, `capital_budget gross .55 / surge .15 / split .40` | 미시험 | 개별 축소기는 각각 옳음(09-12) |
| `NORMAL_INTRADAY_MOMENTUM_REDUCE`, `overheat_reduce`, `NORMAL_DYNAMIC_SLIPPAGE` | 미시험 | |
| edge 자기참조 (`edge<0 → scale 0.25` 상수) | **결함** | Kelly·변동성타게팅 계산하고 안 씀(logic_check 8) |
| 진입 시점 | **결함** | 옵티마이저 same_close / 백테스트 next_open / 생산 intraday 54% (logic_check 11) |
| `max_new`, DDM `calc_max_new` | 미시험 | "max_new=4 무력" 주장은 무효(08-19) |
| 후보 신선도 `block_if_stale=false` | 결함 | 4일 된 후보로 진입 가능 |
| `cap_signal_top_n` config 3 / 실효 12 | 결함 | 설정≠실효 |

## 7. 급등(SURGE) 경로

| 부품 | 상태 | 근거 |
|---|---|---|
| 급등 진입 전체 | **HARD_FAIL 4건**(08-19 감사) | dead logic / 결측 fail-open / LIMIT_UP_NEAR 3중모순 / 이력 미보존 |
| probe 허용 | 결함 | 09-16 126340: `source_entry_blocked=True` 인데 `surge_paper_probe_allowed=True` 로 매수 |
| "엣지 없음" | 범위한정 | SURGE 에 대한 진술. NORMAL 은 −0.03% |

## 8. 청산

| 부품 | 값 | 상태 | 근거 |
|---|---|---|---|
| `stop_loss_pct` | NORMAL −7% / RALLY −4% | 미시험 | `exit.py` `max()` 합성으로 음수에선 더 타이트한 손절이 남음(유효) |
| `max_hold_days` | 15 (NORMAL) | 미시험 | 실보유 중앙 1세션, 20일 넘긴 건 0 |
| TP / trail / `STOP_PREEMPTIVE_CLOSE` / `SURGE_INTRADAY_REVERSAL` / `TECHNICAL` | — | **보류** | 손절:익절 5.3:1, PF 0.829(08-21). "청산이 문제다"는 **철회**, 기대값상 1세션 종가보유가 우위(측정결함 대장 참조) |
| `FUNDAMENTAL_CRITICAL` | — | 작동 확인 | 69건 실제 발동. 효과 미시험 |
| `hold_close_drop_guard`, overnight residual guard | — | 미시험 | |
| 청산 경로 실측 | — | **미결** | "한 번도 안 돌아봤다"(등록부). 오늘 SELL 5건이 hold 0~1일 |

## 9. 리스크 게이트·감시

| 부품 | 상태 | 근거 |
|---|---|---|
| kill_switch (`judged_*` 추가) | 수리됨 | 표시값≠판정값 해결(08-21). 검증 25일 GOOD(09-16 아침) |
| DDM (`dd_cap .10 / dd_stop .15`, validation_reduce `max_new 1 ×0.25`) | 미시험 | 축소기 곱의 한 축 |
| `es_gate` (레짐별 limit/hard_block) | 미시험 | |
| `production_risk_playbook` (WATCH/SOFT/HARD, `max_age 30분`) | 미시험 | topn 09-02~07 `PRECHECK_RISK_GATE_HARD_BLOCK` 전면 차단 |
| `macro_news_guard` | 수리됨(신선도) | 절단선 미결 |
| `sigma_outlier / global_outlier / cross_source_integrity / execution_health / backtest_validation_guard` | 미시험 | 존재만 확인 |
| `adaptive_kill_cap` (streak) | 미시험 | |
| `gateway_health` 신선도 2분 | **결함 후보** | 09-16 08:34 age 2.15분 > 2.0 으로 아침 배치 전체 FAIL_CLOSED |
| `exposure_policy` (RootB observer) | 소비처 없음 | 붙이지 않기로(08-21). 자기강화 루프 위험 |
| `calibration_stream` | 소비자 없음 | 09-09 |

## 10. 집행

| 부품 | 상태 | 근거 |
|---|---|---|
| `kis_order_dispatch_from_exec` (지정가·LOB precheck·dedupe·UNKNOWN_PENDING) | **건전** | 08-21 "유일하게 설계=동작" |
| `SKIP_ALREADY_PAPER_FILLED` | **미확인** | 09-16 v41.1 브로커 발주 15/15 스킵 → 실발주 0. 의도인지 결함인지 미규명 |
| 시장가 금지·지정가 | 유효 | 상한가 증거금(07-29) |
| 비용 모델 | 수리됨 | `pricing_engine` `or` 결함(왕복 1.4% 청구) 09-10 수리. 권위 원장 trades_calc 0.358%, 세율 0.2% |
| 진입 스위치 `PAPER_EXIT_ONLY` | 0 (두 런처) | 09-10 한시 해제. 되돌림 결정 미완 |

## 11. 원장·통계·되먹임

| 부품 | 상태 | 근거 |
|---|---|---|
| fills / trades / trades_calc | 유효(권위=trades_calc) | 비용 모델 3종 공존은 잔존 |
| `live_vs_bt` | 결함 | NA·match_rate 0.0(진단행 나눗셈), 실전-백테스트 계기판 비어 있음 |
| `joined_trades` | 결함 | 36행 동결, 생산자 없음 |
| `backtest_stats.json` | 결함 | 전부 None |
| `--auto-optimize` HPO → stable_params 승격 배선 | **결함** | 발동 횟수 셀 수 없음(30일 정리). 다중검정 원장 결손 |
| `stable_params_v41_1.json` | 인용 금지 | 죽은 시뮬레이션 산물, 재현 불가 |
| 워치독 불변식 감시 | 결함 | 4영역 중 1영역 절반 |

## 12. 이 표에서 바로 보이는 것

```
양(실측)      배제형 5 + rule_e           7개    → 남김 후보
음(실측)      선택형 게이트·점수 가중치      4개    → 지움 후보 (지우면 "무엇을 사나"가 빈다)
결함·미수리   수량축소기 곱, 진입시점, edge 상수, 급등 4건, live_vs_bt, HPO 되먹임, gateway 2분  → 수리 또는 지움
미시험        청산 대부분, 리스크 게이트 대부분, 조정항 2개  → 절차의 실측 대상. 가장 많다
```

절차에서 첫 실측 대상은 **미시험 칸 중 매매를 실제로 막거나 줄이는 것**이에요.
축소기 곱과 리스크 게이트가 그 자리예요. 진입이 1%로 줄어든 상태에서는 다른 어떤 실측도 표본이 안 생겨요.
