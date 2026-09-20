# 판정 임계값 전수 분류 (Threshold Inventory)

**2026-08-31 전수 분류 완료. 코드·설정·매매 변경 0건.** 규격 `docs/references/GATE_JUDGMENT_FORM.md` §5
작성 경위 `.agent/PLANS.md` (153)(154)

---

## 1. 대상과 방법

```
대상   paper/paper_engine_config.json  leaf 1,033 중 게이트성 키 460
       12_Risk_Controlled/stable_params_v41_1.json  숫자 파라미터 30
       합계 490 (숫자 327 / 불리언 163), 68개 섹션
게이트성 판정  키 경로에 min_|max_|_min|_max|require|block|enable|threshold|limit|gate|allow|skip|force
```

**분류 축은 "코드가 무엇을 하는가" 가 아니라 "그 값을 무엇으로 정당화해야 하는가" 다.**

---

### 1.1 [2026-09-09 추가] **적용 범위의 한계 — 설정 파일만 셌다**

위 대상은 전부 **설정 파일**이다. `.py` 안에 하드코딩된 문턱은 **한 건도 포함돼 있지 않다.**
```
검색  1_000_000_000_000     0건
검색  junk_hard_threshold   0건
```
따라서 §2 의 490 도, §2.3 의 32 도 **config 한정 수치**다. 전수가 아니다.

개략 스캔 (주요 의사결정 7개 파일, `[<>]=? 숫자리터럴` 패턴, 0/1/2/100 등 관용값 제외)
```
generate_candidates_v41_1.py       46
paper_engine/entry.py              15
optimize_params_v41_1.py           14
paper_engine/exit.py               13
paper_engine/drawdown.py            2
tools/topn_candidates.py            2
paper_engine/risk_orchestration.py  1
합계                               93     <- 이 표에 등록된 것 0건
```
**[2026-09-09 갱신] 손으로 갈랐다 — 83줄 중 실물은 32줄, 문턱 수로는 31개다** (PLANS (276)).
(`:1444`/`:1445` 가 한 문턱을 두 줄에 쓴 것이라 줄 수와 문턱 수가 하나 다르다. 표 §5 는 31행이다.)
```
오탐 51   불리언 강제 `>=0.5` 22 (실제 문턱은 config 키이고 이미 이 표에 있다)
          길이·형상 가드 19 / 주석 2 / 로그 1 / 구조·식별자 3 /
          단위 판별 휴리스틱 3 / 제도적 사실(장시간) 1
실물 32   [라벨·집계 전용]  11   등급 버킷 85/70/50/30, 90/80/65/50/35, 카운터 70/85
          [junk 플래그 성분] 6   liq/pump/young/fund 0.55~0.60, 상한가 근사 29.0·1.24,
                                 market_cap 3,000억  -> 전부 junk_flags 문자열로만 간다
          [실제 판정]        2   market_cap >= 1조 -> hard 문턱 88->95
          [완화·랠리 분기]   9   **후보 개수를 직접 바꾼다. 여기가 중요하다**
          [최적화 구조]      1   n < 5 (최소 폴드)
          [rule_e]           1   sector_rs_min 0.05
          [청산]             2   market_risk_score >= 60 / >= 80
```
**[2026-09-09 정정] [완화·랠리 분기] 9건은 시급하지 않아요** (조사 종결, PLANS (280)).

처음엔 "이 표에서 가장 시급하다" 고 썼는데 조사해 보니 근거가 약했어요.
```
9건이 지배하는 건 섹터 폴백으로 **추가되는** 후보인데, 그 행들은
candidate_origin=SECTOR_PREFILTER_UNION / observe_only=True 로 찍히고
paper_engine/entry.py:6007 에서 **설정 플래그 없이 무조건 제거돼요.**
지금 실제로 매매하는 topn 경로는 이 함수를 아예 쓰지 않고요.
-> "후보 개수를 직접 바꾼다" 는 맞지만 **그 후보는 매매되지 않아요.**
   근거 기록이 없다는 사실은 그대로지만 급하진 않아요.
```
**대신 조사 중에 훨씬 큰 게 나왔어요 — 자연 통과가 하루 1~2건이에요.**
```
날짜        전체행   SECTOR_PREFILTER_UNION   자연 통과
20260902      12            11                  1
20260903      10             9                  1
20260904      12            11                  1
20260907      12            11                  1
20260908      12            10                  2
후보 파일의 83~92% 가 완화 폴백이고 전부 observe_only 예요
```
문턱을 손댈 게 아니라 **왜 자기 게이트가 거의 아무것도 통과시키지 못하는지**를 봐야 해요.

**`broad_rally` 두 정의도 정정해요 — 의도된 것이고 이미 문서화돼 있어요.**
```
generate_candidates_v41_1.py:1749  (max_ret>=20.0 and pct_ge_5>=0.12) or (pct_ge_10>=0.10)
generate_candidates_v41_1.py:1924  (max_ret>=25.0) and (pct_ge_10>=0.08)
optimize_params_v41_1.py:1069      1924 와 동일
```
`_compute_rally_breadth_stats` 독스트링(2026-07-24)에 이유가 있어요 — 과거에 복사-붙여넣기로
갈라진 걸 통계 계산만 공유하고 **문턱은 일부러 분리**한 결과예요.
남는 지적은 하나뿐이에요: 그 설명이 헬퍼 안에 있어서 두 호출부를 읽는 사람은 못 봐요.

확인된 실물 2건 (2026-09-09 추적, PLANS (275))
```
generate_candidates_v41_1.py:1407  market_cap <= 300_000_000_000  (KOSDAQ 3,000억)
    하는 일  junk_flags 에 라벨을 붙인다. **점수에 닿지 않는다**
    근거기록 없음. 게다가 읽는 market 열이 설계상 UNKNOWN 이라 대부분 평가되지 않는다
generate_candidates_v41_1.py:1444  market_cap >= 1_000_000_000_000 -> hard 문턱 88 -> 95
    하는 일  대형주를 hard-exclude 하기 어렵게 만든다
    근거기록 없음. 실측 junk_risk_score 최대 63.7 (>=88 은 0행) -> 현재 도달 불가
    **도달 불가라고 제거하면 안 된다** - 제거는 대형주를 더 쉽게 배제하는 전략 변경이다
```

**이 절은 "고쳐라" 가 아니라 "이 표를 전수로 인용하지 마라" 는 뜻이다.**
`[[feedback_absence_is_not_evidence]]` - 표에 없다는 것이 문턱이 없다는 뜻이 아니다.

---



```
선정/승격  통계적 상대 비교 + 검정력으로 정당화해야 한다
가동      하방 한도(손실·낙폭·파산 회피)로 정당화한다. 검정력은 부차
집행      제도적 사실 또는 집행 제약에서 온다. 통계 판정 대상이 아니다
관측      아무것도 막지 않는다. 근거가 약해도 해가 적다
미분류    위와 별개 축. 근거 기록이 없으면 자동으로 미분류다 (명세 §5)
```

---

## 2. 결과

```
분류          전체   근거기록없음    비율
선정/승격           284        141      50%
가동              119         62      52%
집행               76         43      57%
관측               11          6      55%
합계              490        252      51%
```

**확인필요는 0이다.** 1차에서 38건이 남았으나 4개 섹션의 실효 설정(`load_config()`)과
사용처 코드를 읽어 전부 배정했다(§4).

### 2.1 이 51%는 하한이다

`plans_hits` 는 **언급**이지 **근거**가 아니다. 언급이 있는데 근거가 0이었던 사례가 셋 있다
(worst_fold 0.75 / open_chase 0.05 / 뉴스 축 39개 도구). 따라서 근거 미확인은 최소 252개다.

### 2.2 "코드 미참조 0건" 은 그렇게 강한 말이 아니었다 [정정]

1차에서 `read_by_code` 를 근거로 "죽은 설정은 없다" 고 적었다.
**그 지표는 키명 문자열이 코드에 등장하는가일 뿐, 그 경로의 값이 읽히는가가 아니다.**
반례를 §5 에서 찾았다 — `drawdown_manager.stages[].mdd_threshold` 4개는 등장하지만 **아무도 읽지 않는다.**

### 2.3 [중요] 51%는 과대였다 - 코드 기본값과 대조하면 32개로 줄어든다 [2026-08-31 2차]

`plans_hits==0` 을 미분류로 본 것은 한 축이 빠져 있었다. **값이 코드 기본값과 같으면
그것은 "근거 없이 골랐다" 가 아니라 "결정된 적이 없다" 다.**
config 키 460개를 `paper_engine/config.py::DEFAULT_CONFIG` 와 전수 대조했다.

```
기본값과 동일  232 (50%)  결정된 적이 없다. 코드 기본값 재기술
이탈           63 (14%)  **누군가 값을 정했다. 근거가 있어야 한다**
기본값 없음    165 (36%)  설정에만 존재. DEFAULT_CONFIG 밖의 폴백을 따로 확인해야 한다

이탈 + PLANS 언급 0  =  **32개**   <- 이것이 진짜 작업 목록이다
   가동 15 / 집행 8 / 선정·승격 6 / 관측 3
```
전수 표의 `기본값` 열로 가려낼 수 있고, 표는 **이탈 항목을 각 분류의 맨 앞에** 둔다.

**주의 - 32개도 확정이 아니다.** 키명 검색이 놓치는 근거가 있다. 실례:
`risk_orchestration/dd_stop_validation/require_dd_stop` 는 키명 언급이 0인데
`docs/exec-plans/active/20260424_dd_stop_validation_reduce.md`(37KB)가 그 섹션을 만든 문서다.
**섹션 단위 근거를 함께 봐야 한다.**

### 2.4 DEFAULT_CONFIG 가 유일한 기본값 출처가 아니다 [2026-08-31 2차]

`기본값 없음` 165개 중 일부는 **함수 안에 폴백이 따로 있다.**
```
macro_gate_policy    gate_daily.py:68-72 에 mode=SOFT 폴백. 실제 설정은 HARD
                     -> **오늘 확인한 것 중 유일하게 기본값보다 엄격한 항목이다**
bear_sizing_policy   risk_orchestration.py:261,268 에 0.50 / 0.40 폴백.
                     실제 설정이 그 값과 동일 -> 결정된 적 없음
```
따라서 `기본값 없음` 165개는 **미조사이지 근거 부재가 아니다.**

---

## 3. [가동] 미분류 62개 - 근거 조사 결과

키명이 PLANS 에 없으므로 **섹션 단위로** PLANS·docs(4,509개 파일)를 재조사했다.
본 문서 자기 참조는 제외했다.

```
근거 문서 후보 있음  38개
  risk_orchestration 12  20260424_dd_stop_validation_reduce.md 외
  regime_overrides    7  20260625_regime_overrides.md
  market_ops_policy   5  20260526_intraday_surge_isolation_policy.md
  sigma_outlier_guard 4  PLANS_LEGACY_20260420.md
  crash_risk_off      3  REGIME_ENTRY_POLICY_SSOT_20260305.md
  kill_switch         2  Algorithm_Trading_Platform_SSOT_v1.md
  기타                5

근거 문서 전무      24개  <- **작업 목록**
  drawdown_manager        20   mdd / new_entry_allowed_pct / max_exposure / liquidate_weakest_pct
  entry_gap_risk_guard     2   down_gap_threshold_pct / max_down_gap_count
  macro_gate_policy        1   hard_block_when_risk_on_false
  bear_sizing_policy       1   max_risk_orch_scale
```
**"문서에 섹션명이 등장한다" 는 "그 값의 근거가 적혀 있다" 가 아니다.**
38개도 확정이 아니라 후보다.

---

## 4. 확인필요 38건의 해소 (실효 설정 + 코드 확인)

```
sigma_outlier_guard              5 -> [가동]   enabled=true, zscore_block=5,
                                             fallback_ratio_block=0.2, block_on_param_out_of_range=true
stale_signal_replay              3 -> [집행]   신호 나이·미체결 조건으로 주문 재발행 가부를 정한다
normal_candidate_staleness_check 3 -> [관측]   enabled=true 이나 **block_if_stale=false**
                                             현재 실효 설정에서는 로그만 남긴다.
                                             **스위치 하나로 [가동] 이 된다** (entry.py:10607-10617)
market_ops_policy               27 -> **혼합**  한 섹션에 4개 분류가 섞여 있어 키별로 배정했다
     [선정/승격] auction_* 3 / carryover_* 4
     [가동]     universe_shrink_* 2 / probe_* 2 / max_stage / enabled
     [집행]     close_cutoff_minutes / signal_ttl_minutes / quote_max_age_days /
                max_retry_per_code_per_day / exec_quality_max_slippage_pct /
                fail_closed_* 2 / *_limit_pct 2 / next_open_gap_up_max_pct / stage_gap_up_limits 3
```
**섹션 단위 분류가 성립하지 않는 섹션이 있다** - 이것이 1차 분류의 구조적 한계다.

---

## 5. [발견] drawdown_manager - 리스크 정책 변경이 절반만 적용됐다

### 5.1 아무도 읽지 않는 필드에 값을 썼다

```
stages[].mdd            <- 엔진이 실제로 쓰는 값     0.20 / 0.27 / 0.31 / 0.36
stages[].mdd_threshold  <- **읽는 코드가 없다**      0.25 / 0.35 / 0.40 / 0.45

mdd_threshold 전수 참조처
  paper_engine/drawdown.py:110,201   hard_cfg.get(...)  <- hard_block_conditions 쪽이다
  patch_config.py:12-15              **쓰기만 한다**
  system_health_checker_work.py      자체 max_mdd_threshold (무관)
엔진의 단계 판정은 drawdown.py:182,208 - row 의 mdd 를 읽고 current_mdd_abs >= stg[mdd] 로 비교
```
`patch_config.py`(2026-05-27, V5 Patch)가 단계 문턱을 0.25/0.35/0.40/0.45 로 올리려 했는데
**그 필드를 엔진이 읽지 않아 반영되지 않았다.** 실제 발동은 여전히 0.20/0.27/0.31/0.36 이다.
(안전 방향으로 어긋났다 - 설정이 말하는 것보다 엄격하게 동작한다. 그러나 읽는 사람은 오해한다.)

### 5.2 같은 패치의 다른 절반은 적용됐다

```
patch_config.py:8   production_risk_playbook.drawdown_role = advisory   <- **적용됨**
guards.py:768-792   advisory 이면 drawdown 신호가 action_signals 에서 제외된다
```
즉 **낙폭을 덜 막도록 하려는 변경이 절반만 들어간 채 3개월이 지났다.**

### 5.3 drawdown_manager 는 살아서 진입을 막는다

```
drawdown.py:271      calc_max_new(old_max_new, ddm_action.new_entry_allowed_pct, ddm_stage)
drawdown.py:237      _apply_drawdown_entry_capacity(...)
paper_engine.py:569  호출  -> 신규 진입 수를 실제로 제한한다
```
따라서 [가동] 분류가 맞고, **근거 기록이 0인 20개가 실제로 매매를 제한하고 있다.**

### 5.4 부수 - 설정 직접 편집

`patch_config.py` 는 `paper/paper_engine_config.json` 을 직접 열어 덮어쓴다.
잠금 계약(`*.lock.json`) 경로를 거치지 않는다.

---

## 6. 이 분류의 한계 (반드시 함께 인용)

```
1) 대부분 섹션 기준 1차 분류다. 개별 키의 코드 경로를 추적한 것은 §4·§5 범위뿐이다
2) 근거 유무는 PLANS·docs 언급의 대리 지표다. 언급 != 근거
3) 게이트성 키 선별이 정규식이다. 전체 leaf 1,033 중 460 만 대상 - 나머지 573 미검사
4) 무엇을 막는지는 대부분 확인하지 않았다. 오늘 실제 이벤트를 남긴 게이트는 2종뿐이다
5) read_by_code 는 키명 등장 여부다. 값이 읽히는지가 아니다 (§2.2)
```

---

## 7. 전수 표 (490행)

근거 열: `-` = PLANS 언급 0건 = **미분류**. 숫자 = 언급 횟수(근거 아님, 대리 지표)

`기본값` 열: 동일=코드 DEFAULT_CONFIG 와 같음(결정된 적 없음) / **이탈**=누군가 값을 정함 /
없음=설정에만 존재(함수 내 폴백을 따로 확인해야 한다)

| # | 분류 | 근거 | 기본값 | 섹션 | 키 | 값 | 경로 |
|---|---|---|---|---|---|---|---|
| 1 | 가동 | - | **이탈** | drawdown_manager | `consecutive_loss_days_threshold` | 4 | `/drawdown_manager/consecutive_loss_days_threshold` |
| 2 | 가동 | - | **이탈** | drawdown_manager | `max_exposure` | 0.8 | `/drawdown_manager/stages[0]/max_exposure` |
| 3 | 가동 | - | **이탈** | drawdown_manager | `max_exposure` | 0.6 | `/drawdown_manager/stages[1]/max_exposure` |
| 4 | 가동 | - | **이탈** | drawdown_manager | `max_exposure` | 0 | `/drawdown_manager/stages[3]/max_exposure` |
| 5 | 가동 | - | **이탈** | drawdown_manager | `new_entry_allowed_pct` | 0.75 | `/drawdown_manager/stages[0]/new_entry_allowed_pct` |
| 6 | 가동 | - | **이탈** | drawdown_manager | `new_entry_allowed_pct` | 0.35 | `/drawdown_manager/stages[1]/new_entry_allowed_pct` |
| 7 | 가동 | - | **이탈** | drawdown_manager | `new_entry_allowed_pct` | 0.1 | `/drawdown_manager/stages[2]/new_entry_allowed_pct` |
| 8 | 가동 | - | **이탈** | drawdown_manager | `sector_concentration_limit` | 1 | `/drawdown_manager/sector_concentration_limit` |
| 9 | 가동 | - | **이탈** | entry_gap_risk_guard | `max_down_gap_count` | 1 | `/entry_gap_risk_guard/max_down_gap_count` |
| 10 | 가동 | - | **이탈** | market_ops_policy | `max_stage` | 0 | `/market_ops_policy/entry_fallback_policy/max_stage` |
| 11 | 가동 | - | **이탈** | max_daily_new_exposure_pct | `max_daily_new_exposure_pct` | 0.4 | `/max_daily_new_exposure_pct` |
| 12 | 가동 | - | **이탈** | regime_entry_policy | `rally_gap_up_max_pct` | 0.08 | `/regime_entry_policy/rally_gap_up_max_pct` |
| 13 | 가동 | - | **이탈** | regime_overrides | `max_daily_new_exposure_pct` | 0.05 | `/regime_overrides/CRASH/max_daily_new_exposure_pct` |
| 14 | 가동 | - | **이탈** | regime_overrides | `max_gross_exposure_pct` | 0.1 | `/regime_overrides/CRASH/max_gross_exposure_pct` |
| 15 | 가동 | - | **이탈** | risk_orchestration | `require_dd_stop` | false | `/risk_orchestration/dd_stop_validation/require_dd_stop` |
| 16 | 가동 | 47 | **이탈** | entry_gap_risk_guard | `enabled` | true | `/entry_gap_risk_guard/enabled` |
| 17 | 가동 | 47 | **이탈** | intraday_residual_overnight_guard | `enabled` | true | `/intraday_residual_overnight_guard/enabled` |
| 18 | 가동 | 47 | **이탈** | market_ops_policy | `enabled` | true | `/market_ops_policy/entry_fallback_policy/enabled` |
| 19 | 가동 | 14 | **이탈** | regime_overrides | `max_new_surge` | 1 | `/regime_overrides/BEAR/surge_entry_policy/max_new_surge` |
| 20 | 가동 | 14 | **이탈** | regime_overrides | `max_new_surge` | 3 | `/regime_overrides/CRASH/surge_entry_policy/max_new_surge` |
| 21 | 가동 | 1 | **이탈** | regime_overrides | `max_new_trades_per_day` | 3 | `/regime_overrides/CRASH/max_new_trades_per_day` |
| 22 | 가동 | 47 | **이탈** | risk_orchestration | `enabled` | true | `/risk_orchestration/enabled` |
| 23 | 가동 | 47 | **이탈** | risk_orchestration | `enabled` | true | `/risk_orchestration/dd_taper/account_clear_strategy_dd_override/enabled` |
| 24 | 가동 | 47 | **이탈** | risk_orchestration | `enabled` | false | `/risk_orchestration/dd_stop_validation/enabled` |
| 25 | 가동 | 95 | **이탈** | risk_orchestration | `max_new` | 2 | `/risk_orchestration/dd_stop_validation/max_new` |
| 26 | 가동 | - | 없음 | bear_sizing_policy | `max_risk_orch_scale` | 0.5 | `/bear_sizing_policy/max_risk_orch_scale` |
| 27 | 가동 | - | 없음 | crash_risk_off | `fallback_trigger_max_dd_pct` | 0.35 | `/crash_risk_off/fallback_trigger_max_dd_pct` |
| 28 | 가동 | - | 없음 | crash_risk_off | `min_new_trades_per_day` | 1 | `/crash_risk_off/min_new_trades_per_day` |
| 29 | 가동 | - | 없음 | crash_risk_off | `trigger_max_dd_pct` | 0.12 | `/crash_risk_off/trigger_max_dd_pct` |
| 30 | 가동 | - | 없음 | drawdown_manager | `consecutive_loss_days` | 5 | `/drawdown_manager/hard_block_conditions/consecutive_loss_days` |
| 31 | 가동 | - | 동일 | drawdown_manager | `max_exposure` | 0.4 | `/drawdown_manager/stages[2]/max_exposure` |
| 32 | 가동 | - | 없음 | drawdown_manager | `mdd_threshold` | 0.25 | `/drawdown_manager/stages[0]/mdd_threshold` |
| 33 | 가동 | - | 없음 | drawdown_manager | `mdd_threshold` | 0.35 | `/drawdown_manager/stages[1]/mdd_threshold` |
| 34 | 가동 | - | 없음 | drawdown_manager | `mdd_threshold` | 0.4 | `/drawdown_manager/stages[2]/mdd_threshold` |
| 35 | 가동 | - | 없음 | drawdown_manager | `mdd_threshold` | 0.45 | `/drawdown_manager/stages[3]/mdd_threshold` |
| 36 | 가동 | - | 없음 | drawdown_manager | `mdd_threshold` | 0.36 | `/drawdown_manager/hard_block_conditions/mdd_threshold` |
| 37 | 가동 | - | 없음 | drawdown_manager | `min_positions_for_partial_liquidation` | 4 | `/drawdown_manager/min_positions_for_partial_liquidation` |
| 38 | 가동 | - | 동일 | drawdown_manager | `new_entry_allowed_pct` | 0 | `/drawdown_manager/stages[3]/new_entry_allowed_pct` |
| 39 | 가동 | - | 없음 | drawdown_manager | `require_system_stress` | true | `/drawdown_manager/hard_block_conditions/require_system_stress` |
| 40 | 가동 | - | 동일 | drawdown_manager | `sector_concentration_block` | true | `/drawdown_manager/sector_concentration_block` |
| 41 | 가동 | - | 동일 | drawdown_manager | `vix_proxy_threshold` | 30 | `/drawdown_manager/vix_proxy_threshold` |
| 42 | 가동 | - | 동일 | entry_gap_risk_guard | `down_gap_threshold_pct` | 0.08 | `/entry_gap_risk_guard/down_gap_threshold_pct` |
| 43 | 가동 | - | 동일 | execution_health_guard | `slippage_bps_block` | 20 | `/execution_health_guard/slippage_bps_block` |
| 44 | 가동 | - | 없음 | kill_switch | `max_daily_loss_pct` | 0.08 | `/kill_switch/max_daily_loss_pct` |
| 45 | 가동 | - | 없음 | kill_switch | `min_new_trades_per_day` | 1 | `/kill_switch/min_new_trades_per_day` |
| 46 | 가동 | - | 없음 | macro_gate_policy | `hard_block_when_risk_on_false` | true | `/macro_gate_policy/hard_block_when_risk_on_false` |
| 47 | 가동 | - | 동일 | macro_news_guard | `news_quota_guard_block` | true | `/macro_news_guard/news_quota_guard_block` |
| 48 | 가동 | - | 동일 | market_ops_policy | `probe_crash_min` | 0 | `/market_ops_policy/probe_crash_min` |
| 49 | 가동 | - | 동일 | market_ops_policy | `probe_rally_min` | 1 | `/market_ops_policy/probe_rally_min` |
| 50 | 가동 | - | 동일 | market_ops_policy | `universe_shrink_min_candidates` | 8 | `/market_ops_policy/universe_shrink_min_candidates` |
| 51 | 가동 | - | 동일 | market_ops_policy | `universe_shrink_min_price_codes` | 0 | `/market_ops_policy/universe_shrink_min_price_codes` |
| 52 | 가동 | - | 동일 | max_gross_exposure_pct | `max_gross_exposure_pct` | 1 | `/max_gross_exposure_pct` |
| 53 | 가동 | - | 없음 | regime_overrides | `block_price_vol_divergence` | true | `/regime_overrides/NORMAL/surge_entry_policy/type_policy/block_price_vol_divergence` |
| 54 | 가동 | - | 없음 | regime_overrides | `block_price_vol_divergence` | true | `/regime_overrides/BEAR/surge_entry_policy/type_policy/block_price_vol_divergence` |
| 55 | 가동 | - | 동일 | regime_overrides | `max_daily_new_exposure_pct` | 0.15 | `/regime_overrides/RALLY/max_daily_new_exposure_pct` |
| 56 | 가동 | - | 동일 | regime_overrides | `max_gross_exposure_pct` | 0.6 | `/regime_overrides/RALLY/max_gross_exposure_pct` |
| 57 | 가동 | - | 없음 | regime_overrides | `trend_smoothness_min` | 1 | `/regime_overrides/RALLY/surge_entry_policy/type_policy/trend_smoothness_min` |
| 58 | 가동 | - | 동일 | risk_orchestration | `c_min` | 0.25 | `/risk_orchestration/c_min` |
| 59 | 가동 | - | 동일 | risk_orchestration | `max_scale` | 1 | `/risk_orchestration/max_scale` |
| 60 | 가동 | - | 동일 | risk_orchestration | `min_scale` | 0.25 | `/risk_orchestration/account_clear_min_scale/min_scale` |
| 61 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0.5 | `/risk_orchestration/es_gate/reduction_factor` |
| 62 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0 | `/risk_orchestration/es_gate/by_regime/CRASH/reduction_factor` |
| 63 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0 | `/risk_orchestration/es_gate/by_regime/STAGFLATION/reduction_factor` |
| 64 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0.35 | `/risk_orchestration/es_gate/by_regime/BEAR/reduction_factor` |
| 65 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0.6 | `/risk_orchestration/es_gate/by_regime/RALLY/reduction_factor` |
| 66 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0.5 | `/risk_orchestration/es_gate/by_regime/NORMAL/reduction_factor` |
| 67 | 가동 | - | 동일 | risk_orchestration | `reduction_factor` | 0.4 | `/risk_orchestration/es_gate/by_regime/RATE_HIKE_FEAR/reduction_factor` |
| 68 | 가동 | - | 동일 | risk_orchestration | `require_dd_advisory_only` | true | `/risk_orchestration/account_clear_min_scale/require_dd_advisory_only` |
| 69 | 가동 | - | 없음 | sigma_outlier_guard | `block_on_param_out_of_range` | true | `/sigma_outlier_guard/block_on_param_out_of_range` |
| 70 | 가동 | - | 동일 | sigma_outlier_guard | `fallback_ratio_block` | 0.2 | `/sigma_outlier_guard/fallback_ratio_block` |
| 71 | 가동 | - | 동일 | sigma_outlier_guard | `min_history` | 5 | `/sigma_outlier_guard/min_history` |
| 72 | 가동 | - | 동일 | sigma_outlier_guard | `zscore_block` | 5 | `/sigma_outlier_guard/zscore_block` |
| 73 | 가동 | 47 | 없음 | bear_sizing_policy | `enabled` | true | `/bear_sizing_policy/enabled` |
| 74 | 가동 | 1 | 동일 | capital_budget_policy | `defensive_max_new` | 0 | `/capital_budget_policy/defensive_max_new` |
| 75 | 가동 | 47 | 동일 | capital_budget_policy | `enabled` | true | `/capital_budget_policy/enabled` |
| 76 | 가동 | 1 | 동일 | capital_budget_policy | `recovery_enabled` | false | `/capital_budget_policy/recovery_enabled` |
| 77 | 가동 | 1 | 동일 | capital_budget_policy | `reserve_trade_enabled` | false | `/capital_budget_policy/reserve_trade_enabled` |
| 78 | 가동 | 47 | 없음 | crash_risk_off | `enabled` | true | `/crash_risk_off/enabled` |
| 79 | 가동 | 47 | 동일 | drawdown_manager | `enabled` | true | `/drawdown_manager/enabled` |
| 80 | 가동 | 47 | 동일 | execution_health_guard | `enabled` | true | `/execution_health_guard/enabled` |
| 81 | 가동 | 7 | 없음 | kill_switch | `max_drawdown_pct` | 0.36 | `/kill_switch/max_drawdown_pct` |
| 82 | 가동 | 47 | 동일 | macro_news_guard | `enabled` | true | `/macro_news_guard/enabled` |
| 83 | 가동 | 3 | 동일 | macro_news_guard | `macro_critical_bad_block` | 1 | `/macro_news_guard/macro_critical_bad_block` |
| 84 | 가동 | 1 | 동일 | macro_news_guard | `macro_stale_ratio_block` | 0.8 | `/macro_news_guard/macro_stale_ratio_block` |
| 85 | 가동 | 47 | 동일 | market_ops_policy | `enabled` | true | `/market_ops_policy/enabled` |
| 86 | 가동 | 1 | 동일 | regime_entry_policy | `p0_bear_promote_enabled` | true | `/regime_entry_policy/p0_bear_promote_enabled` |
| 87 | 가동 | 1 | 없음 | regime_entry_policy | `rally_day_ret_min_proxy` | 0.025 | `/regime_entry_policy/rally_day_ret_min_proxy` |
| 88 | 가동 | 47 | 없음 | regime_overrides | `enabled` | false | `/regime_overrides/RALLY/surge_entry_policy/type_policy/afternoon_session_filter/enabled` |
| 89 | 가동 | 47 | 동일 | regime_overrides | `enabled` | true | `/regime_overrides/NORMAL/split_entry/enabled` |
| 90 | 가동 | 47 | 없음 | regime_overrides | `enabled` | true | `/regime_overrides/NORMAL/surge_entry_policy/type_policy/afternoon_session_filter/enabled` |
| 91 | 가동 | 47 | 동일 | regime_overrides | `enabled` | true | `/regime_overrides/BEAR/split_entry/enabled` |
| 92 | 가동 | 47 | 없음 | regime_overrides | `enabled` | true | `/regime_overrides/BEAR/surge_entry_policy/type_policy/afternoon_session_filter/enabled` |
| 93 | 가동 | 3 | 동일 | regime_overrides | `gap_up_max_pct` | 0.08 | `/regime_overrides/RALLY/gap_up_max_pct` |
| 94 | 가동 | 1 | 동일 | regime_overrides | `max_hold_days` | 5 | `/regime_overrides/RALLY/surge_exit_policy/max_hold_days` |
| 95 | 가동 | 1 | 동일 | regime_overrides | `max_hold_days` | 15 | `/regime_overrides/NORMAL/max_hold_days` |
| 96 | 가동 | 1 | 동일 | regime_overrides | `max_hold_days` | 15 | `/regime_overrides/BEAR/max_hold_days` |
| 97 | 가동 | 14 | 동일 | regime_overrides | `max_new_surge` | 6 | `/regime_overrides/RALLY/surge_entry_policy/max_new_surge` |
| 98 | 가동 | 14 | 동일 | regime_overrides | `max_new_surge` | 2 | `/regime_overrides/NORMAL/surge_entry_policy/max_new_surge` |
| 99 | 가동 | 1 | 없음 | regime_overrides | `max_v_accel` | 3.15 | `/regime_overrides/RALLY/split_entry/second_confirmation/max_v_accel` |
| 100 | 가동 | 1 | 동일 | risk_orchestration | `c_max` | 0.5 | `/risk_orchestration/c_max` |
| 101 | 가동 | 47 | 동일 | risk_orchestration | `enabled` | false | `/risk_orchestration/dd_stop_validation/max_positions_full_override/enabled` |
| 102 | 가동 | 47 | 동일 | risk_orchestration | `enabled` | true | `/risk_orchestration/es_gate/enabled` |
| 103 | 가동 | 47 | 동일 | risk_orchestration | `enabled` | true | `/risk_orchestration/account_clear_min_scale/enabled` |
| 104 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | false | `/risk_orchestration/es_gate/hard_block` |
| 105 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | true | `/risk_orchestration/es_gate/by_regime/CRASH/hard_block` |
| 106 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | true | `/risk_orchestration/es_gate/by_regime/STAGFLATION/hard_block` |
| 107 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | false | `/risk_orchestration/es_gate/by_regime/BEAR/hard_block` |
| 108 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | false | `/risk_orchestration/es_gate/by_regime/RALLY/hard_block` |
| 109 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | false | `/risk_orchestration/es_gate/by_regime/NORMAL/hard_block` |
| 110 | 가동 | 42 | 동일 | risk_orchestration | `hard_block` | false | `/risk_orchestration/es_gate/by_regime/RATE_HIKE_FEAR/hard_block` |
| 111 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.05 | `/risk_orchestration/es_gate/limit` |
| 112 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.04 | `/risk_orchestration/es_gate/by_regime/CRASH/limit` |
| 113 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.045 | `/risk_orchestration/es_gate/by_regime/STAGFLATION/limit` |
| 114 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.05 | `/risk_orchestration/es_gate/by_regime/BEAR/limit` |
| 115 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.06 | `/risk_orchestration/es_gate/by_regime/RALLY/limit` |
| 116 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.05 | `/risk_orchestration/es_gate/by_regime/NORMAL/limit` |
| 117 | 가동 | 16 | 동일 | risk_orchestration | `limit` | 0.05 | `/risk_orchestration/es_gate/by_regime/RATE_HIKE_FEAR/limit` |
| 118 | 가동 | 14 | 없음 | risk_orchestration | `max_new_surge` | 1 | `/risk_orchestration/dd_stop_validation/max_new_surge` |
| 119 | 가동 | 47 | 동일 | sigma_outlier_guard | `enabled` | true | `/sigma_outlier_guard/enabled` |
| 120 | 선정/승격 | - | **이탈** | adaptive_entry_control | `kill_switch_override_block` | false | `/adaptive_entry_control/kill_switch_override_block` |
| 121 | 선정/승격 | - | **이탈** | entry_selection_policy | `skip_same_code_day_already_buy` | false | `/entry_selection_policy/skip_same_code_day_already_buy` |
| 122 | 선정/승격 | - | **이탈** | market_ops_policy | `carryover_no_next_day_enabled` | false | `/market_ops_policy/carryover_no_next_day_enabled` |
| 123 | 선정/승격 | - | **이탈** | p1_entry_policy | `macd_golden_min_ratio` | 0 | `/p1_entry_policy/technical_gate/macd_golden_min_ratio` |
| 124 | 선정/승격 | - | **이탈** | surge_exit_policy | `require_high_rejection_for_exit` | true | `/surge_exit_policy/intraday_reversal_exit/require_high_rejection_for_exit` |
| 125 | 선정/승격 | - | **이탈** | union_entry_strength_min | `union_entry_strength_min` | 0.63 | `/union_entry_strength_min` |
| 126 | 선정/승격 | 3 | **이탈** | gap_up_max_pct | `gap_up_max_pct` | 0.03 | `/gap_up_max_pct` |
| 127 | 선정/승격 | 1 | **이탈** | max_hold_days | `max_hold_days` | 8 | `/max_hold_days` |
| 128 | 선정/승격 | 1 | **이탈** | max_new_trades_per_day | `max_new_trades_per_day` | 15 | `/max_new_trades_per_day` |
| 129 | 선정/승격 | 2 | **이탈** | max_per_sector | `max_per_sector` | 2 | `/max_per_sector` |
| 130 | 선정/승격 | 2 | **이탈** | min_hold_protect_stop_loss | `min_hold_protect_stop_loss` | false | `/min_hold_protect_stop_loss` |
| 131 | 선정/승격 | 47 | **이탈** | p1_entry_policy | `enabled` | true | `/p1_entry_policy/intraday/enabled` |
| 132 | 선정/승격 | 1 | **이탈** | p1_entry_policy | `option_expiry_reduce_enabled` | false | `/p1_entry_policy/calendar/option_expiry_reduce_enabled` |
| 133 | 선정/승격 | 10 | **이탈** | positive_entry_criteria | `require_execution_pool_when_present` | false | `/positive_entry_criteria/require_execution_pool_when_present` |
| 134 | 선정/승격 | 1 | **이탈** | sell_rules | `max_hold_days` | 7 | `/sell_rules/asset_type_rules/테마주/max_hold_days` |
| 135 | 선정/승격 | 14 | **이탈** | surge_entry_policy | `max_new_surge` | 6 | `/surge_entry_policy/max_new_surge` |
| 136 | 선정/승격 | - | 동일 | adaptive_entry_control | `probe_min_new` | 1 | `/adaptive_entry_control/probe_min_new` |
| 137 | 선정/승격 | - | 동일 | adaptive_entry_control | `relief_min_new` | 1 | `/adaptive_entry_control/relief_min_new` |
| 138 | 선정/승격 | - | 없음 | adaptive_good_stock_entry | `tier1_gapup_override_max_pct` | 0.16 | `/adaptive_good_stock_entry/tier1_gapup_override_max_pct` |
| 139 | 선정/승격 | - | 없음 | adaptive_good_stock_entry | `tier2_gapup_override_max_pct` | 0.12 | `/adaptive_good_stock_entry/tier2_gapup_override_max_pct` |
| 140 | 선정/승격 | - | 동일 | allow_same_code_reentry | `allow_same_code_reentry` | false | `/allow_same_code_reentry` |
| 141 | 선정/승격 | - | 동일 | backtest_validation_guard | `block_on_missing` | false | `/backtest_validation_guard/block_on_missing` |
| 142 | 선정/승격 | - | 동일 | backtest_validation_guard | `stale_max_age_days` | 3 | `/backtest_validation_guard/stale_max_age_days` |
| 143 | 선정/승격 | - | 없음 | beta_harvest | `max_single_etf_weight` | 0.6 | `/beta_harvest/max_single_etf_weight` |
| 144 | 선정/승격 | - | 없음 | beta_harvest | `min_order_amount_krw` | 100000 | `/beta_harvest/min_order_amount_krw` |
| 145 | 선정/승격 | - | 없음 | dynamic_stop_loss | `max_stop_pct` | -0.001 | `/dynamic_stop_loss/max_stop_pct` |
| 146 | 선정/승격 | - | 없음 | dynamic_stop_loss | `min_stop_pct` | -30 | `/dynamic_stop_loss/min_stop_pct` |
| 147 | 선정/승격 | - | 동일 | entry_overheat_policy | `atr14_pct_threshold` | 0.09907047892735515 | `/entry_overheat_policy/atr14_pct_threshold` |
| 148 | 선정/승격 | - | 동일 | entry_overheat_policy | `ret1_pct_threshold` | 8.849700179324415 | `/entry_overheat_policy/ret1_pct_threshold` |
| 149 | 선정/승격 | - | 동일 | entry_selection_policy | `max_candidates` | 3 | `/entry_selection_policy/fallback_after_block/max_candidates` |
| 150 | 선정/승격 | - | 동일 | entry_selection_policy | `one_pick_when_max_new_le` | 1 | `/entry_selection_policy/one_pick_when_max_new_le` |
| 151 | 선정/승격 | - | 동일 | entry_selection_policy | `when_max_new_le` | 1 | `/entry_selection_policy/fallback_after_block/when_max_new_le` |
| 152 | 선정/승격 | - | 없음 | entry_selection_policy_by_run_label | `one_pick_when_max_new_le` | 1 | `/entry_selection_policy_by_run_label/main/one_pick_when_max_new_le` |
| 153 | 선정/승격 | - | 없음 | entry_selection_policy_by_run_label | `one_pick_when_max_new_le` | 0 | `/entry_selection_policy_by_run_label/shadow/one_pick_when_max_new_le` |
| 154 | 선정/승격 | - | 동일 | entry_signal_date_top_score_cap | `fallback_require_positive_entry` | true | `/entry_signal_date_top_score_cap/fallback_require_positive_entry` |
| 155 | 선정/승격 | - | 동일 | horizon_entry_policy | `max_atr14_pct` | 0.08 | `/horizon_entry_policy/label_rules/LONG/max_atr14_pct` |
| 156 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_atr14_pct` | 0.06 | `/horizon_entry_policy/label_rules/SWING/min_atr14_pct` |
| 157 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_final_score` | 0.03 | `/horizon_entry_policy/label_rules/SHORT/min_final_score` |
| 158 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_final_score` | 0.02 | `/horizon_entry_policy/label_rules/SWING/min_final_score` |
| 159 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_final_score` | 0.02 | `/horizon_entry_policy/label_rules/MID/min_final_score` |
| 160 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_final_score` | 0.015 | `/horizon_entry_policy/label_rules/LONG/min_final_score` |
| 161 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_rs_slope` | 6 | `/horizon_entry_policy/label_rules/SHORT/min_rs_slope` |
| 162 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_stoch_k` | 65 | `/horizon_entry_policy/label_rules/SWING/min_stoch_k` |
| 163 | 선정/승격 | - | 동일 | horizon_entry_policy | `min_v_accel` | 1 | `/horizon_entry_policy/label_rules/SHORT/min_v_accel` |
| 164 | 선정/승격 | - | 동일 | market_ops_policy | `auction_close_position_min` | 0.7 | `/market_ops_policy/entry_fallback_policy/auction_close_position_min` |
| 165 | 선정/승격 | - | 동일 | market_ops_policy | `auction_day_range_max_pct` | 0.07 | `/market_ops_policy/entry_fallback_policy/auction_day_range_max_pct` |
| 166 | 선정/승격 | - | 동일 | market_ops_policy | `carryover_max_age_days` | 2 | `/market_ops_policy/carryover_max_age_days` |
| 167 | 선정/승격 | - | 동일 | market_ops_policy | `carryover_revalidate_min_final_score` | 0 | `/market_ops_policy/carryover_revalidate_min_final_score` |
| 168 | 선정/승격 | - | 동일 | market_ops_policy | `carryover_revalidate_score_gap_max` | 0.05 | `/market_ops_policy/carryover_revalidate_score_gap_max` |
| 169 | 선정/승격 | - | 없음 | min_entry_score_policy | `allow_fallback_top1` | false | `/min_entry_score_policy/allow_fallback_top1` |
| 170 | 선정/승격 | - | 없음 | min_entry_score_policy | `max_cap` | 0.05 | `/min_entry_score_policy/max_cap` |
| 171 | 선정/승격 | - | 없음 | min_entry_score_policy | `min_candidates` | 5 | `/min_entry_score_policy/min_candidates` |
| 172 | 선정/승격 | - | 없음 | min_entry_score_policy | `min_floor` | 0.015 | `/min_entry_score_policy/min_floor` |
| 173 | 선정/승격 | - | 없음 | p1_entry_policy | `boll_mid_min` | 0 | `/p1_entry_policy/technical_gate/boll_mid_min` |
| 174 | 선정/승격 | - | 없음 | p1_entry_policy | `boll_mid_ok_min_ratio` | 0.5 | `/p1_entry_policy/technical_gate/boll_mid_ok_min_ratio` |
| 175 | 선정/승격 | - | 동일 | p1_entry_policy | `min_pool_size` | 8 | `/p1_entry_policy/technical_gate/min_pool_size` |
| 176 | 선정/승격 | - | 동일 | p1_entry_policy | `morning_sector_strength_min` | 0.8 | `/p1_entry_policy/intraday/morning_sector_strength_min` |
| 177 | 선정/승격 | - | 없음 | p1_entry_policy | `obv_ok_min_ratio` | 0.5 | `/p1_entry_policy/technical_gate/obv_ok_min_ratio` |
| 178 | 선정/승격 | - | 없음 | p1_entry_policy | `obv_slope_min` | 0 | `/p1_entry_policy/technical_gate/obv_slope_min` |
| 179 | 선정/승격 | - | 동일 | p1_entry_policy | `rsi_min` | 45 | `/p1_entry_policy/technical_gate/rsi_min` |
| 180 | 선정/승격 | - | 없음 | p1_entry_policy | `rsi_ok_min_ratio` | 0.45 | `/p1_entry_policy/technical_gate/rsi_ok_min_ratio` |
| 181 | 선정/승격 | - | 없음 | p1_entry_policy | `sma_ema_ok_min_ratio` | 0.5 | `/p1_entry_policy/technical_gate/sma_ema_ok_min_ratio` |
| 182 | 선정/승격 | - | 없음 | p1_entry_policy | `stoch_k_max` | 85 | `/p1_entry_policy/technical_gate/stoch_k_max` |
| 183 | 선정/승격 | - | 없음 | p1_entry_policy | `stoch_k_min` | 20 | `/p1_entry_policy/technical_gate/stoch_k_min` |
| 184 | 선정/승격 | - | 없음 | p1_entry_policy | `stoch_ok_min_ratio` | 0.5 | `/p1_entry_policy/technical_gate/stoch_ok_min_ratio` |
| 185 | 선정/승격 | - | 동일 | p1_entry_policy | `volcorr_min` | 0.03 | `/p1_entry_policy/technical_gate/volcorr_min` |
| 186 | 선정/승격 | - | 없음 | p1_entry_policy | `volcorr_ok_min_ratio` | 0.5 | `/p1_entry_policy/technical_gate/volcorr_ok_min_ratio` |
| 187 | 선정/승격 | - | 없음 | paper_validation_sample_policy | `min_scale_floor` | 0 | `/paper_validation_sample_policy/min_scale_floor` |
| 188 | 선정/승격 | - | 없음 | paper_validation_sample_policy | `require_account_risk_clear` | true | `/paper_validation_sample_policy/require_account_risk_clear` |
| 189 | 선정/승격 | - | 없음 | paper_validation_sample_policy | `require_no_dd_stop_triggered` | true | `/paper_validation_sample_policy/require_no_dd_stop_triggered` |
| 190 | 선정/승격 | - | 없음 | paper_validation_sample_policy | `require_no_es_hard_block` | true | `/paper_validation_sample_policy/require_no_es_hard_block` |
| 191 | 선정/승격 | - | 동일 | positive_entry_criteria | `allow_sector_union` | true | `/positive_entry_criteria/allow_sector_union` |
| 192 | 선정/승격 | - | 없음 | positive_entry_criteria | `max_candidates` | 1 | `/positive_entry_criteria/fresh_sector_allowed_fallback/max_candidates` |
| 193 | 선정/승격 | - | 없음 | positive_entry_criteria | `min_final_score` | 0.1 | `/positive_entry_criteria/fresh_sector_allowed_fallback/min_final_score` |
| 194 | 선정/승격 | - | 없음 | positive_entry_criteria | `min_sector_strength` | 0.4 | `/positive_entry_criteria/fresh_sector_allowed_fallback/min_sector_strength` |
| 195 | 선정/승격 | - | 동일 | positive_entry_criteria | `require_sector_entry_when_present` | true | `/positive_entry_criteria/require_sector_entry_when_present` |
| 196 | 선정/승격 | - | 없음 | positive_entry_criteria | `trigger_when_fresh_ok_lte` | 0 | `/positive_entry_criteria/fresh_sector_allowed_fallback/trigger_when_fresh_ok_lte` |
| 197 | 선정/승격 | - | 동일 | sector_correlation_guard | `allow_block` | false | `/sector_correlation_guard/allow_block` |
| 198 | 선정/승격 | - | 동일 | sector_correlation_guard | `hrp_enabled` | true | `/sector_correlation_guard/hrp_enabled` |
| 199 | 선정/승격 | - | 동일 | sector_correlation_guard | `hrp_min_qty_multiplier` | 0.35 | `/sector_correlation_guard/hrp_min_qty_multiplier` |
| 200 | 선정/승격 | - | 동일 | sector_correlation_guard | `reduce_threshold_abs_corr` | 0.85 | `/sector_correlation_guard/reduce_threshold_abs_corr` |
| 201 | 선정/승격 | - | 동일 | sector_rebalance | `limit_pct` | 0.4 | `/sector_rebalance/limit_pct` |
| 202 | 선정/승격 | - | 동일 | sell_rules | `high_score_threshold` | 60 | `/sell_rules/technical/high_score_threshold` |
| 203 | 선정/승격 | - | 동일 | sell_rules | `mid_score_threshold` | 40 | `/sell_rules/technical/mid_score_threshold` |
| 204 | 선정/승격 | - | 동일 | sell_rules | `preemptive_close_enabled` | true | `/sell_rules/stop_loss/preemptive_close_enabled` |
| 205 | 선정/승격 | - | 동일 | sell_rules | `trailing_stop_enabled` | true | `/sell_rules/stop_loss/trailing_stop_enabled` |
| 206 | 선정/승격 | - | - | stable_params | `sector_max_per_day` | 2 | `/sector_max_per_day` |
| 207 | 선정/승격 | - | 없음 | surge_entry_policy | `allow_orderflow_no_history` | true | `/surge_entry_policy/wait_reclaim_paper_probe/allow_orderflow_no_history` |
| 208 | 선정/승격 | - | 없음 | surge_entry_policy | `block_after_hhmm` | 1300 | `/surge_entry_policy/afternoon_session_filter/block_after_hhmm` |
| 209 | 선정/승격 | - | 동일 | surge_entry_policy | `block_on_exclude_reasons` | true | `/surge_entry_policy/market_event_guard/block_on_exclude_reasons` |
| 210 | 선정/승격 | - | 없음 | surge_entry_policy | `block_price_vol_divergence` | true | `/surge_entry_policy/type_policy/block_price_vol_divergence` |
| 211 | 선정/승격 | - | 없음 | surge_entry_policy | `max_down_gap_count` | 2 | `/surge_entry_policy/active_entry_gap_risk_override/max_down_gap_count` |
| 212 | 선정/승격 | - | 없음 | surge_entry_policy | `max_gap_pct` | 0.16 | `/surge_entry_policy/active_entry_gap_up_override/max_gap_pct` |
| 213 | 선정/승격 | - | 없음 | surge_entry_policy | `max_gap_pct` | 0.16 | `/surge_entry_policy/paper_probe/gap_up_override/max_gap_pct` |
| 214 | 선정/승격 | - | 없음 | surge_entry_policy | `max_gap_pct` | 0.3 | `/surge_entry_policy/wait_reclaim_paper_probe/max_gap_pct` |
| 215 | 선정/승격 | - | 없음 | surge_entry_policy | `max_intraday_high_drawdown_pct` | 0.005 | `/surge_entry_policy/wait_reclaim_paper_probe/max_intraday_high_drawdown_pct` |
| 216 | 선정/승격 | - | 없음 | surge_entry_policy | `max_new_per_type` | 3 | `/surge_entry_policy/type_policy/max_new_per_type` |
| 217 | 선정/승격 | - | 없음 | surge_entry_policy | `max_rvol20` | 30 | `/surge_entry_policy/wait_reclaim_paper_probe/max_rvol20` |
| 218 | 선정/승격 | - | 없음 | surge_entry_policy | `max_same_code_per_day` | 1 | `/surge_entry_policy/max_same_code_per_day` |
| 219 | 선정/승격 | - | 없음 | surge_entry_policy | `max_score_gap` | 2 | `/surge_entry_policy/paper_probe/max_score_gap` |
| 220 | 선정/승격 | - | 없음 | surge_entry_policy | `max_selected` | 1 | `/surge_entry_policy/no_lob_probe/max_selected` |
| 221 | 선정/승격 | - | 없음 | surge_entry_policy | `max_selected` | 1 | `/surge_entry_policy/paper_probe/max_selected` |
| 222 | 선정/승격 | - | 없음 | surge_entry_policy | `max_selected` | 1 | `/surge_entry_policy/wait_reclaim_paper_probe/max_selected` |
| 223 | 선정/승격 | - | 없음 | surge_entry_policy | `max_spread_bps` | 25 | `/surge_entry_policy/active_entry_gap_up_override/max_spread_bps` |
| 224 | 선정/승격 | - | 없음 | surge_entry_policy | `max_spread_bps` | 25 | `/surge_entry_policy/paper_probe/gap_up_override/max_spread_bps` |
| 225 | 선정/승격 | - | 없음 | surge_entry_policy | `max_spread_bps` | 25 | `/surge_entry_policy/wait_reclaim_paper_probe/max_spread_bps` |
| 226 | 선정/승격 | - | 없음 | surge_entry_policy | `medium_cap` | 3 | `/surge_entry_policy/dynamic_max_new/medium_cap` |
| 227 | 선정/승격 | - | 없음 | surge_entry_policy | `medium_rvol20_min` | 2 | `/surge_entry_policy/dynamic_max_new/medium_rvol20_min` |
| 228 | 선정/승격 | - | 없음 | surge_entry_policy | `medium_score_final` | 82 | `/surge_entry_policy/dynamic_max_new/medium_score_final` |
| 229 | 선정/승격 | - | 없음 | surge_entry_policy | `medium_spread_bps_max` | 40 | `/surge_entry_policy/dynamic_max_new/medium_spread_bps_max` |
| 230 | 선정/승격 | - | 없음 | surge_entry_policy | `min_bps` | 15 | `/surge_entry_policy/paper_probe/execution_quality_sizing/spread_bands[0]/min_bps` |
| 231 | 선정/승격 | - | 없음 | surge_entry_policy | `min_bps` | 25 | `/surge_entry_policy/paper_probe/execution_quality_sizing/spread_bands[1]/min_bps` |
| 232 | 선정/승격 | - | 없음 | surge_entry_policy | `min_bps` | 40 | `/surge_entry_policy/paper_probe/execution_quality_sizing/spread_bands[2]/min_bps` |
| 233 | 선정/승격 | - | 없음 | surge_entry_policy | `min_change_pct` | 0.05 | `/surge_entry_policy/wait_reclaim_paper_probe/min_change_pct` |
| 234 | 선정/승격 | - | 없음 | surge_entry_policy | `min_current_gap_pct` | 0.05 | `/surge_entry_policy/active_entry_gap_risk_override/min_current_gap_pct` |
| 235 | 선정/승격 | - | 없음 | surge_entry_policy | `min_intraday_range_position_pct` | 0.95 | `/surge_entry_policy/wait_reclaim_paper_probe/min_intraday_range_position_pct` |
| 236 | 선정/승격 | - | 없음 | surge_entry_policy | `min_markout_1step_bps` | 0 | `/surge_entry_policy/active_entry_gap_up_override/min_markout_1step_bps` |
| 237 | 선정/승격 | - | 없음 | surge_entry_policy | `min_markout_1step_bps` | 0 | `/surge_entry_policy/paper_probe/gap_up_override/min_markout_1step_bps` |
| 238 | 선정/승격 | - | 없음 | surge_entry_policy | `min_markout_1step_bps` | 0 | `/surge_entry_policy/wait_reclaim_paper_probe/min_markout_1step_bps` |
| 239 | 선정/승격 | - | 없음 | surge_entry_policy | `min_trading_value_krw` | 1000000000 | `/surge_entry_policy/wait_reclaim_paper_probe/min_trading_value_krw` |
| 240 | 선정/승격 | - | 없음 | surge_entry_policy | `no_lob_cap` | 1 | `/surge_entry_policy/dynamic_max_new/no_lob_cap` |
| 241 | 선정/승격 | - | 없음 | surge_entry_policy | `orderflow_risk_score_max` | 0.6 | `/surge_entry_policy/dynamic_max_new/orderflow_risk_score_max` |
| 242 | 선정/승격 | - | 없음 | surge_entry_policy | `require_active_gap_up_override_pass` | true | `/surge_entry_policy/active_entry_gap_risk_override/require_active_gap_up_override_pass` |
| 243 | 선정/승격 | - | 없음 | surge_entry_policy | `require_lob_ok` | true | `/surge_entry_policy/active_entry_gap_up_override/require_lob_ok` |
| 244 | 선정/승격 | - | 없음 | surge_entry_policy | `require_lob_ok` | true | `/surge_entry_policy/paper_probe/gap_up_override/require_lob_ok` |
| 245 | 선정/승격 | - | 없음 | surge_entry_policy | `require_lob_ok` | true | `/surge_entry_policy/wait_reclaim_paper_probe/require_lob_ok` |
| 246 | 선정/승격 | - | 없음 | surge_entry_policy | `require_orderflow_ok` | false | `/surge_entry_policy/wait_reclaim_paper_probe/require_orderflow_ok` |
| 247 | 선정/승격 | - | 없음 | surge_entry_policy | `require_orderflow_tag_ok` | true | `/surge_entry_policy/active_entry_gap_up_override/require_orderflow_tag_ok` |
| 248 | 선정/승격 | - | 없음 | surge_entry_policy | `require_orderflow_tag_ok` | true | `/surge_entry_policy/paper_probe/gap_up_override/require_orderflow_tag_ok` |
| 249 | 선정/승격 | - | 없음 | surge_entry_policy | `require_paper_order_route` | true | `/surge_entry_policy/active_entry_gap_up_override/require_paper_order_route` |
| 250 | 선정/승격 | - | 없음 | surge_entry_policy | `require_paper_order_route` | true | `/surge_entry_policy/paper_probe/gap_up_override/require_paper_order_route` |
| 251 | 선정/승격 | - | 없음 | surge_entry_policy | `require_paper_ready` | true | `/surge_entry_policy/active_entry_gap_up_override/require_paper_ready` |
| 252 | 선정/승격 | - | 없음 | surge_entry_policy | `require_paper_ready` | true | `/surge_entry_policy/paper_probe/gap_up_override/require_paper_ready` |
| 253 | 선정/승격 | - | 없음 | surge_entry_policy | `strong_cap` | 6 | `/surge_entry_policy/dynamic_max_new/strong_cap` |
| 254 | 선정/승격 | - | 없음 | surge_entry_policy | `strong_rvol20_min` | 3 | `/surge_entry_policy/dynamic_max_new/strong_rvol20_min` |
| 255 | 선정/승격 | - | 없음 | surge_entry_policy | `strong_score_final` | 90 | `/surge_entry_policy/dynamic_max_new/strong_score_final` |
| 256 | 선정/승격 | - | 없음 | surge_entry_policy | `strong_spread_bps_max` | 30 | `/surge_entry_policy/dynamic_max_new/strong_spread_bps_max` |
| 257 | 선정/승격 | - | 없음 | surge_entry_policy | `trend_smoothness_min` | 1.5 | `/surge_entry_policy/type_policy/trend_smoothness_min` |
| 258 | 선정/승격 | - | 없음 | surge_entry_policy | `weak_cap` | 1 | `/surge_entry_policy/dynamic_max_new/weak_cap` |
| 259 | 선정/승격 | - | 동일 | surge_exit_policy | `FORCE` | 100 | `/surge_exit_policy/dynamic_exit_ratio/base_ratios/FORCE` |
| 260 | 선정/승격 | - | 동일 | surge_exit_policy | `early_loss_force_pct` | 0.12 | `/surge_exit_policy/dynamic_exit_ratio/early_loss_force_pct` |
| 261 | 선정/승격 | - | 동일 | surge_exit_policy | `force_gap_loss_pct` | 0.08 | `/surge_exit_policy/dynamic_exit_ratio/force_gap_loss_pct` |
| 262 | 선정/승격 | - | 동일 | surge_exit_policy | `high_rejection_min_drawdown_pct` | 0 | `/surge_exit_policy/reversal_exit/high_rejection_min_drawdown_pct` |
| 263 | 선정/승격 | - | 동일 | surge_exit_policy | `high_rejection_min_drawdown_pct` | 0.025 | `/surge_exit_policy/intraday_reversal_exit/high_rejection_min_drawdown_pct` |
| 264 | 선정/승격 | - | 동일 | surge_exit_policy | `min_points` | 3 | `/surge_exit_policy/intraday_reversal_exit/min_points` |
| 265 | 선정/승격 | - | 동일 | surge_exit_policy | `orderflow_risk_score_max` | 0.6 | `/surge_exit_policy/intraday_reversal_exit/orderflow_risk_score_max` |
| 266 | 선정/승격 | - | 없음 | surge_exit_policy | `repeat_stop_force_exit` | true | `/surge_exit_policy/dynamic_exit_ratio/repeat_stop_force_exit` |
| 267 | 선정/승격 | - | 동일 | surge_exit_policy | `volume_exhaustion_ratio_max` | 0.6 | `/surge_exit_policy/reversal_exit/volume_exhaustion_ratio_max` |
| 268 | 선정/승격 | - | 동일 | surge_exit_policy | `volume_fade_ratio_max` | 0.35 | `/surge_exit_policy/intraday_reversal_exit/volume_fade_ratio_max` |
| 269 | 선정/승격 | - | 동일 | trend_overlay_2026 | `cutting_hawkish_max` | -0.2 | `/trend_overlay_2026/cutting_hawkish_max` |
| 270 | 선정/승격 | - | 동일 | trend_overlay_2026 | `max_open_positions` | 2 | `/trend_overlay_2026/ai_semiconductor_overlay/max_open_positions` |
| 271 | 선정/승격 | 47 | 동일 | adaptive_entry_control | `enabled` | true | `/adaptive_entry_control/enabled` |
| 272 | 선정/승격 | 47 | 없음 | adaptive_good_stock_entry | `enabled` | true | `/adaptive_good_stock_entry/enabled` |
| 273 | 선정/승격 | 47 | 동일 | backtest_validation_guard | `enabled` | true | `/backtest_validation_guard/enabled` |
| 274 | 선정/승격 | 47 | 없음 | beta_harvest | `enabled` | false | `/beta_harvest/enabled` |
| 275 | 선정/승격 | 47 | 없음 | dynamic_stop_loss | `enabled` | true | `/dynamic_stop_loss/enabled` |
| 276 | 선정/승격 | 47 | 동일 | entry_overheat_policy | `enabled` | true | `/entry_overheat_policy/enabled` |
| 277 | 선정/승격 | 1 | 동일 | entry_overheat_policy | `v_accel_threshold` | 3.1520902710607213 | `/entry_overheat_policy/v_accel_threshold` |
| 278 | 선정/승격 | 47 | 동일 | entry_selection_policy | `enabled` | true | `/entry_selection_policy/enabled` |
| 279 | 선정/승격 | 47 | 동일 | entry_selection_policy | `enabled` | true | `/entry_selection_policy/fallback_after_block/enabled` |
| 280 | 선정/승격 | 47 | 동일 | entry_signal_date_top_score_cap | `enabled` | true | `/entry_signal_date_top_score_cap/enabled` |
| 281 | 선정/승격 | 47 | 없음 | hold_close_drop_guard | `enabled` | true | `/hold_close_drop_guard/enabled` |
| 282 | 선정/승격 | 3 | 없음 | hold_close_drop_guard | `min_hold_days` | 1 | `/hold_close_drop_guard/min_hold_days` |
| 283 | 선정/승격 | 47 | 동일 | horizon_entry_policy | `enabled` | true | `/horizon_entry_policy/enabled` |
| 284 | 선정/승격 | 1 | 동일 | horizon_entry_policy | `max_hold_days` | 7 | `/horizon_entry_policy/label_rules/SHORT/max_hold_days` |
| 285 | 선정/승격 | 1 | 동일 | horizon_entry_policy | `max_hold_days` | 12 | `/horizon_entry_policy/label_rules/SWING/max_hold_days` |
| 286 | 선정/승격 | 1 | 동일 | horizon_entry_policy | `max_hold_days` | 20 | `/horizon_entry_policy/label_rules/MID/max_hold_days` |
| 287 | 선정/승격 | 1 | 동일 | horizon_entry_policy | `max_hold_days` | 30 | `/horizon_entry_policy/label_rules/LONG/max_hold_days` |
| 288 | 선정/승격 | 1 | 동일 | market_ops_policy | `auction_v_accel_min` | 1.5 | `/market_ops_policy/entry_fallback_policy/auction_v_accel_min` |
| 289 | 선정/승격 | 2 | 없음 | max_positions | `max_positions` | 18 | `/max_positions` |
| 290 | 선정/승격 | 1 | 없음 | min_entry_score | `min_entry_score` | 0.02 | `/min_entry_score` |
| 291 | 선정/승격 | 47 | 없음 | min_entry_score_policy | `enabled` | true | `/min_entry_score_policy/enabled` |
| 292 | 선정/승격 | 1 | 없음 | min_entry_score_policy | `quantile` | 0.6 | `/min_entry_score_policy/quantile` |
| 293 | 선정/승격 | 3 | 동일 | min_hold_days | `min_hold_days` | 2 | `/min_hold_days` |
| 294 | 선정/승격 | 3 | 동일 | news_implication_entry_policy | `block_observe_only` | false | `/news_implication_entry_policy/block_observe_only` |
| 295 | 선정/승격 | 47 | 동일 | news_implication_entry_policy | `enabled` | true | `/news_implication_entry_policy/enabled` |
| 296 | 선정/승격 | 1 | 동일 | p1_entry_policy | `auto_stub_when_missing` | true | `/p1_entry_policy/event_gate/auto_stub_when_missing` |
| 297 | 선정/승격 | 47 | 동일 | p1_entry_policy | `enabled` | true | `/p1_entry_policy/enabled` |
| 298 | 선정/승격 | 47 | 동일 | p1_entry_policy | `enabled` | true | `/p1_entry_policy/calendar/enabled` |
| 299 | 선정/승격 | 47 | 동일 | p1_entry_policy | `enabled` | true | `/p1_entry_policy/event_gate/enabled` |
| 300 | 선정/승격 | 47 | 동일 | p1_entry_policy | `enabled` | true | `/p1_entry_policy/event_gate/explicit_market_event_guard/enabled` |
| 301 | 선정/승격 | 47 | 동일 | p1_entry_policy | `enabled` | true | `/p1_entry_policy/technical_gate/enabled` |
| 302 | 선정/승격 | 2 | 동일 | p1_entry_policy | `high_risk_max_new_cap` | 1 | `/p1_entry_policy/event_gate/high_risk_max_new_cap` |
| 303 | 선정/승격 | 1 | 동일 | p1_entry_policy | `low_quality_max_new_cap` | 1 | `/p1_entry_policy/technical_gate/low_quality_max_new_cap` |
| 304 | 선정/승격 | 4 | 동일 | p1_entry_policy | `lunch_max_new_cap` | 1 | `/p1_entry_policy/intraday/lunch_max_new_cap` |
| 305 | 선정/승격 | 1 | 동일 | p1_entry_policy | `option_expiry_max_new_cap` | 1 | `/p1_entry_policy/calendar/option_expiry_max_new_cap` |
| 306 | 선정/승격 | 1 | 동일 | p1_entry_policy | `reduce_max_new_cap` | 1 | `/p1_entry_policy/calendar/reduce_max_new_cap` |
| 307 | 선정/승격 | 5 | 동일 | p1_entry_policy | `rsi_max` | 75 | `/p1_entry_policy/technical_gate/rsi_max` |
| 308 | 선정/승격 | 47 | 없음 | paper_validation_sample_policy | `enabled` | true | `/paper_validation_sample_policy/enabled` |
| 309 | 선정/승격 | 47 | 동일 | positive_entry_criteria | `enabled` | true | `/positive_entry_criteria/enabled` |
| 310 | 선정/승격 | 47 | 없음 | positive_entry_criteria | `enabled` | false | `/positive_entry_criteria/fresh_sector_allowed_fallback/enabled` |
| 311 | 선정/승격 | 2 | 동일 | positive_entry_criteria | `min_score` | 0 | `/positive_entry_criteria/min_score` |
| 312 | 선정/승격 | 47 | 동일 | sector_correlation_guard | `enabled` | true | `/sector_correlation_guard/enabled` |
| 313 | 선정/승격 | 47 | 동일 | sector_rebalance | `enabled` | false | `/sector_rebalance/enabled` |
| 314 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | true | `/sell_rules/enabled` |
| 315 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | true | `/sell_rules/take_profit/enabled` |
| 316 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | true | `/sell_rules/fundamental_risk/enabled` |
| 317 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | true | `/sell_rules/market_risk/enabled` |
| 318 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | false | `/sell_rules/candidate_dropout/enabled` |
| 319 | 선정/승격 | 47 | 동일 | sell_rules | `enabled` | true | `/sell_rules/technical/enabled` |
| 320 | 선정/승격 | 1 | 없음 | sell_rules | `max_hold_days` | 10 | `/sell_rules/asset_type_rules/경기민감주/max_hold_days` |
| 321 | 선정/승격 | 1 | 없음 | sell_rules | `max_hold_days` | 20 | `/sell_rules/asset_type_rules/성장주/max_hold_days` |
| 322 | 선정/승격 | 1 | 없음 | sell_rules | `max_hold_days` | 25 | `/sell_rules/asset_type_rules/가치주/max_hold_days` |
| 323 | 선정/승격 | 1 | 없음 | sell_rules | `max_hold_days` | 30 | `/sell_rules/asset_type_rules/배당주/max_hold_days` |
| 324 | 선정/승격 | 1 | 없음 | sell_rules | `max_hold_days` | 20 | `/sell_rules/asset_type_rules/경기방어주/max_hold_days` |
| 325 | 선정/승격 | 3 | 동일 | sell_rules | `min_hold_days` | 2 | `/sell_rules/candidate_dropout/min_hold_days` |
| 326 | 선정/승격 | 13 | - | stable_params | `atr_max` | 0.215 | `/atr_max` |
| 327 | 선정/승격 | 4 | - | stable_params | `best_score` | 3.0288350669570554 | `/best_score` |
| 328 | 선정/승격 | 19 | - | stable_params | `defense_bear_disable_entry` | 1 | `/defense_bear_disable_entry` |
| 329 | 선정/승격 | 3 | - | stable_params | `defense_bear_rs_slope_min` | -0.015 | `/defense_bear_rs_slope_min` |
| 330 | 선정/승격 | 2 | - | stable_params | `entry_gap_down_stop_pct` | 0.03 | `/entry_gap_down_stop_pct` |
| 331 | 선정/승격 | 3 | - | stable_params | `gap_limit` | 0.01 | `/gap_limit` |
| 332 | 선정/승격 | 3 | - | stable_params | `gap_up_max_pct` | 0.03 | `/gap_up_max_pct` |
| 333 | 선정/승격 | 107 | - | stable_params | `hold` | 13 | `/hold` |
| 334 | 선정/승격 | 3 | - | stable_params | `max_pos` | 6 | `/max_pos` |
| 335 | 선정/승격 | 3 | - | stable_params | `min_listing_days` | 126 | `/min_listing_days` |
| 336 | 선정/승격 | 6 | - | stable_params | `min_market_cap` | 100000000000 | `/min_market_cap` |
| 337 | 선정/승격 | 8 | - | stable_params | `mkt_ret20_min` | -1 | `/mkt_ret20_min` |
| 338 | 선정/승격 | 9 | - | stable_params | `mkt_ret60_min` | -1 | `/mkt_ret60_min` |
| 339 | 선정/승격 | 4 | - | stable_params | `near_52w_high_gap_max` | 0.2 | `/near_52w_high_gap_max` |
| 340 | 선정/승격 | 3 | - | stable_params | `require_above_ma200` | 1 | `/require_above_ma200` |
| 341 | 선정/승격 | 41 | - | stable_params | `require_macd_golden` | 0 | `/require_macd_golden` |
| 342 | 선정/승격 | 13 | - | stable_params | `rs_lim` | -0.04 | `/rs_lim` |
| 343 | 선정/승격 | 5 | - | stable_params | `rsi_max` | 70 | `/rsi_max` |
| 344 | 선정/승격 | 7 | - | stable_params | `sector_rs_min` | -1 | `/sector_rs_min` |
| 345 | 선정/승격 | 23 | - | stable_params | `stop_loss` | -0.05 | `/stop_loss` |
| 346 | 선정/승격 | 9 | - | stable_params | `stretch_max` | 1.28 | `/stretch_max` |
| 347 | 선정/승격 | 1 | - | stable_params | `use_relax_ladder` | 1 | `/use_relax_ladder` |
| 348 | 선정/승격 | 33 | - | stable_params | `v_accel_lim` | 6.6 | `/v_accel_lim` |
| 349 | 선정/승격 | 17 | - | stable_params | `v_accel_max` | 5 | `/v_accel_max` |
| 350 | 선정/승격 | 38 | - | stable_params | `value_min` | 155000000000 | `/value_min` |
| 351 | 선정/승격 | 4 | - | stable_params | `vol_close_corr_min` | 0 | `/vol_close_corr_min` |
| 352 | 선정/승격 | 8 | - | stable_params | `w_rs` | 0.29 | `/w_rs` |
| 353 | 선정/승격 | 3 | - | stable_params | `w_rs_slope` | 0.52 | `/w_rs_slope` |
| 354 | 선정/승격 | 5 | - | stable_params | `w_v_accel` | 0.19 | `/w_v_accel` |
| 355 | 선정/승격 | 8 | 없음 | stable_params_quality_gate | `min_mean_pf` | 1 | `/stable_params_quality_gate/min_mean_pf` |
| 356 | 선정/승격 | 7 | 없음 | stable_params_quality_gate | `min_oos_pf` | 1 | `/stable_params_quality_gate/min_oos_pf` |
| 357 | 선정/승격 | 2 | 없음 | stable_params_quality_gate | `min_oos_trades` | 20 | `/stable_params_quality_gate/min_oos_trades` |
| 358 | 선정/승격 | 4 | 없음 | stable_params_quality_gate | `min_stable_score` | -20 | `/stable_params_quality_gate/min_stable_score` |
| 359 | 선정/승격 | 3 | 없음 | stable_params_quality_gate | `require_promoted` | true | `/stable_params_quality_gate/require_promoted` |
| 360 | 선정/승격 | 10 | 없음 | surge_entry_policy | `LIMIT_UP_NEAR` | 0.25 | `/surge_entry_policy/type_policy/type_qty_multiplier/LIMIT_UP_NEAR` |
| 361 | 선정/승격 | 14 | 없음 | surge_entry_policy | `alloc_pct` | 0.003 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/alloc_pct` |
| 362 | 선정/승격 | 2 | 없음 | surge_entry_policy | `bypass_type_block` | true | `/surge_entry_policy/wait_reclaim_paper_probe/bypass_type_block` |
| 363 | 선정/승격 | 47 | 동일 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/enabled` |
| 364 | 선정/승격 | 47 | 동일 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/type_policy/enabled` |
| 365 | 선정/승격 | 47 | 동일 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/market_event_guard/enabled` |
| 366 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/dynamic_max_new/enabled` |
| 367 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/no_lob_probe/enabled` |
| 368 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/active_entry_gap_up_override/enabled` |
| 369 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/active_entry_gap_risk_override/enabled` |
| 370 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/paper_probe/enabled` |
| 371 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/paper_probe/gap_up_override/enabled` |
| 372 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/paper_probe/execution_quality_sizing/enabled` |
| 373 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/afternoon_session_filter/enabled` |
| 374 | 선정/승격 | 47 | 없음 | surge_entry_policy | `enabled` | true | `/surge_entry_policy/wait_reclaim_paper_probe/enabled` |
| 375 | 선정/승격 | 3 | 없음 | surge_entry_policy | `first_ratio` | 0.25 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/first_ratio` |
| 376 | 선정/승격 | 4 | 없음 | surge_entry_policy | `hard_cap` | 6 | `/surge_entry_policy/dynamic_max_new/hard_cap` |
| 377 | 선정/승격 | 1 | 동일 | surge_entry_policy | `max_hold_days` | 5 | `/surge_entry_policy/type_policy/type_overrides/PRICE_VOL_BREAKOUT/max_hold_days` |
| 378 | 선정/승격 | 1 | 동일 | surge_entry_policy | `max_hold_days` | 3 | `/surge_entry_policy/type_policy/type_overrides/PRICE_RANGE_BREAKOUT/max_hold_days` |
| 379 | 선정/승격 | 1 | 동일 | surge_entry_policy | `max_hold_days` | 5 | `/surge_entry_policy/type_policy/type_overrides/REG_SHORT_5D60/max_hold_days` |
| 380 | 선정/승격 | 1 | 동일 | surge_entry_policy | `max_hold_days` | 3 | `/surge_entry_policy/type_policy/type_overrides/REG_MID_15D100/max_hold_days` |
| 381 | 선정/승격 | 1 | 없음 | surge_entry_policy | `max_hold_days` | 1 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/max_hold_days` |
| 382 | 선정/승격 | 1 | 없음 | surge_entry_policy | `max_hold_days` | 2 | `/surge_entry_policy/type_policy/type_overrides/OVERSOLD_REVERSAL/max_hold_days` |
| 383 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_qty` | 1 | `/surge_entry_policy/min_qty` |
| 384 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_score_final` | 75 | `/surge_entry_policy/min_score_final` |
| 385 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_score_final` | 75 | `/surge_entry_policy/type_policy/type_overrides/PRICE_VOL_BREAKOUT/min_score_final` |
| 386 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_score_final` | 80 | `/surge_entry_policy/type_policy/type_overrides/PRICE_RANGE_BREAKOUT/min_score_final` |
| 387 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_score_final` | 70 | `/surge_entry_policy/type_policy/type_overrides/REG_SHORT_5D60/min_score_final` |
| 388 | 선정/승격 | 2 | 동일 | surge_entry_policy | `min_score_final` | 85 | `/surge_entry_policy/type_policy/type_overrides/REG_MID_15D100/min_score_final` |
| 389 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 80 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/min_score_final` |
| 390 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 60 | `/surge_entry_policy/type_policy/type_overrides/OVERSOLD_REVERSAL/min_score_final` |
| 391 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 75 | `/surge_entry_policy/dynamic_max_new/min_score_final` |
| 392 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 78 | `/surge_entry_policy/active_entry_gap_up_override/min_score_final` |
| 393 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 78 | `/surge_entry_policy/paper_probe/min_score_final` |
| 394 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 78 | `/surge_entry_policy/paper_probe/gap_up_override/min_score_final` |
| 395 | 선정/승격 | 2 | 없음 | surge_entry_policy | `min_score_final` | 78 | `/surge_entry_policy/wait_reclaim_paper_probe/min_score_final` |
| 396 | 선정/승격 | 7 | 없음 | surge_entry_policy | `stop_loss_pct` | -0.04 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/stop_loss_pct` |
| 397 | 선정/승격 | 8 | 없음 | surge_entry_policy | `take_profit_pct` | 0.04 | `/surge_entry_policy/type_policy/type_overrides/LIMIT_UP_NEAR/take_profit_pct` |
| 398 | 선정/승격 | 47 | 동일 | surge_exit_policy | `enabled` | true | `/surge_exit_policy/enabled` |
| 399 | 선정/승격 | 47 | 동일 | surge_exit_policy | `enabled` | true | `/surge_exit_policy/dynamic_exit_ratio/enabled` |
| 400 | 선정/승격 | 47 | 동일 | surge_exit_policy | `enabled` | true | `/surge_exit_policy/reversal_exit/enabled` |
| 401 | 선정/승격 | 47 | 동일 | surge_exit_policy | `enabled` | true | `/surge_exit_policy/intraday_reversal_exit/enabled` |
| 402 | 선정/승격 | 1 | 동일 | surge_exit_policy | `max_hold_days` | 5 | `/surge_exit_policy/max_hold_days` |
| 403 | 선정/승격 | 47 | 동일 | trend_overlay_2026 | `enabled` | true | `/trend_overlay_2026/enabled` |
| 404 | 집행 | - | **이탈** | entry_gap_up_reduce | `threshold_pct` | 0.02 | `/entry_gap_up_reduce/threshold_pct` |
| 405 | 집행 | - | **이탈** | fx_entry_policy | `daily_abs_change_block_level` | 23 | `/fx_entry_policy/daily_abs_change_block_level` |
| 406 | 집행 | - | **이탈** | fx_entry_policy | `hard_block_requires_crisis_level` | true | `/fx_entry_policy/hard_block_requires_crisis_level` |
| 407 | 집행 | - | **이탈** | limit_price_tolerance | `max_deviation_pct` | 0.08 | `/limit_price_tolerance/max_deviation_pct` |
| 408 | 집행 | - | **이탈** | market_ops_policy | `exec_quality_max_slippage_pct` | 0.05 | `/market_ops_policy/exec_quality_max_slippage_pct` |
| 409 | 집행 | - | **이탈** | market_ops_policy | `max_retry_per_code_per_day` | 2 | `/market_ops_policy/max_retry_per_code_per_day` |
| 410 | 집행 | - | **이탈** | market_ops_policy | `signal_ttl_minutes` | 60 | `/market_ops_policy/signal_ttl_minutes` |
| 411 | 집행 | - | **이탈** | split_entry | `second_entry_max_days` | 2 | `/split_entry/second_entry_max_days` |
| 412 | 집행 | 47 | **이탈** | entry_gap_up_reduce | `enabled` | true | `/entry_gap_up_reduce/enabled` |
| 413 | 집행 | 47 | **이탈** | entry_liquidity_check | `enabled` | true | `/entry_liquidity_check/enabled` |
| 414 | 집행 | 47 | **이탈** | limit_price_tolerance | `enabled` | true | `/limit_price_tolerance/enabled` |
| 415 | 집행 | 21 | **이탈** | normal_realtime_gap_policy | `block_v_accel_min` | 0 | `/normal_realtime_gap_policy/intraday_momentum_recheck/block_v_accel_min` |
| 416 | 집행 | 4 | **이탈** | normal_realtime_gap_policy | `max_day_range_pct` | 0.3 | `/normal_realtime_gap_policy/close_auction/max_day_range_pct` |
| 417 | 집행 | 47 | **이탈** | split_entry | `enabled` | true | `/split_entry/enabled` |
| 418 | 집행 | 1 | **이탈** | split_entry | `max_v_accel` | 0.8 | `/split_entry/second_confirmation/max_v_accel` |
| 419 | 집행 | 1 | **이탈** | split_entry | `second_dip_max_pct` | 0.04 | `/split_entry/second_dip_max_pct` |
| 420 | 집행 | 1 | **이탈** | split_entry | `second_dip_min_pct` | 0.015 | `/split_entry/second_dip_min_pct` |
| 421 | 집행 | 47 | **이탈** | stale_signal_replay | `enabled` | true | `/stale_signal_replay/enabled` |
| 422 | 집행 | 47 | **이탈** | tiered_slippage | `enabled` | true | `/tiered_slippage/enabled` |
| 423 | 집행 | - | 동일 | entry_liquidity_check | `min_trading_value_krw` | 1000000000 | `/entry_liquidity_check/min_trading_value_krw` |
| 424 | 집행 | - | 없음 | fx_entry_policy | `caution_avg_abs_change_threshold` | 11 | `/fx_entry_policy/caution_avg_abs_change_threshold` |
| 425 | 집행 | - | 없음 | fx_entry_policy | `soft_avg_abs_change_threshold` | 11 | `/fx_entry_policy/soft_avg_abs_change_threshold` |
| 426 | 집행 | - | 없음 | fx_entry_policy | `soft_daily_abs_change_threshold` | 23 | `/fx_entry_policy/soft_daily_abs_change_threshold` |
| 427 | 집행 | - | 동일 | fx_entry_policy | `three_day_extreme_force_defensive` | true | `/fx_entry_policy/three_day_extreme_force_defensive` |
| 428 | 집행 | - | 동일 | market_ops_policy | `close_cutoff_minutes` | 10 | `/market_ops_policy/close_cutoff_minutes` |
| 429 | 집행 | - | 동일 | market_ops_policy | `fail_closed_propagate_block` | true | `/market_ops_policy/fail_closed_propagate_block` |
| 430 | 집행 | - | 없음 | market_ops_policy | `fail_closed_unfilled_ratio_threshold` | 0.4 | `/market_ops_policy/fail_closed_unfilled_ratio_threshold` |
| 431 | 집행 | - | 동일 | market_ops_policy | `intraday_limit_pct` | 0.01 | `/market_ops_policy/entry_fallback_policy/intraday_limit_pct` |
| 432 | 집행 | - | 동일 | market_ops_policy | `next_open_gap_up_max_pct` | 0.03 | `/market_ops_policy/entry_fallback_policy/next_open_gap_up_max_pct` |
| 433 | 집행 | - | 동일 | market_ops_policy | `next_open_limit_pct` | 0.01 | `/market_ops_policy/entry_fallback_policy/next_open_limit_pct` |
| 434 | 집행 | - | 동일 | market_ops_policy | `quote_max_age_days` | 1 | `/market_ops_policy/quote_max_age_days` |
| 435 | 집행 | - | 동일 | market_ops_policy | `stage_gap_up_limits` | 0.03 | `/market_ops_policy/entry_fallback_policy/stage_gap_up_limits[0]` |
| 436 | 집행 | - | 동일 | market_ops_policy | `stage_gap_up_limits` | 0.02 | `/market_ops_policy/entry_fallback_policy/stage_gap_up_limits[1]` |
| 437 | 집행 | - | 동일 | market_ops_policy | `stage_gap_up_limits` | 0.01 | `/market_ops_policy/entry_fallback_policy/stage_gap_up_limits[2]` |
| 438 | 집행 | - | 동일 | normal_entry_execution_quality | `block_on_missing_markout` | true | `/normal_entry_execution_quality/block_on_missing_markout` |
| 439 | 집행 | - | 동일 | normal_entry_execution_quality | `max_bps` | 5 | `/normal_entry_execution_quality/qty_reduction/markout_bands[0]/max_bps` |
| 440 | 집행 | - | 동일 | normal_entry_execution_quality | `max_depth_levels` | 10 | `/normal_entry_execution_quality/max_depth_levels` |
| 441 | 집행 | - | 동일 | normal_entry_execution_quality | `max_spread_bps` | 30 | `/normal_entry_execution_quality/max_spread_bps` |
| 442 | 집행 | - | 동일 | normal_entry_execution_quality | `min_bps` | 20 | `/normal_entry_execution_quality/qty_reduction/spread_bands[0]/min_bps` |
| 443 | 집행 | - | 동일 | normal_entry_execution_quality | `min_bps` | 25 | `/normal_entry_execution_quality/qty_reduction/spread_bands[1]/min_bps` |
| 444 | 집행 | - | 동일 | normal_entry_execution_quality | `min_executable_qty_ratio` | 1 | `/normal_entry_execution_quality/min_executable_qty_ratio` |
| 445 | 집행 | - | 동일 | normal_entry_execution_quality | `min_markout_1step_bps` | 0 | `/normal_entry_execution_quality/min_markout_1step_bps` |
| 446 | 집행 | - | 동일 | normal_entry_execution_quality | `require_executable_qty` | true | `/normal_entry_execution_quality/require_executable_qty` |
| 447 | 집행 | - | 동일 | normal_intraday_realtime_policy | `allow_dd_stop_validation_reduce` | false | `/normal_intraday_realtime_policy/allow_dd_stop_validation_reduce` |
| 448 | 집행 | - | 동일 | normal_intraday_realtime_policy | `block_when_entry_gate_not_allow` | true | `/normal_intraday_realtime_policy/block_when_entry_gate_not_allow` |
| 449 | 집행 | - | 동일 | normal_intraday_realtime_policy | `block_when_p0_rolling_dd_ge_threshold` | false | `/normal_intraday_realtime_policy/block_when_p0_rolling_dd_ge_threshold` |
| 450 | 집행 | - | 동일 | normal_intraday_realtime_policy | `p0_rolling_dd_block_pct` | 0.1 | `/normal_intraday_realtime_policy/p0_rolling_dd_block_pct` |
| 451 | 집행 | - | 동일 | normal_realtime_gap_policy | `max_slippage_pct` | 0.03 | `/normal_realtime_gap_policy/dynamic_slippage/max_slippage_pct` |
| 452 | 집행 | - | 동일 | split_entry | `allow_split_second_carryover` | true | `/split_entry/allow_split_second_carryover` |
| 453 | 집행 | - | 동일 | split_entry | `max_atr14_pct` | 0.09907047892735515 | `/split_entry/second_confirmation/max_atr14_pct` |
| 454 | 집행 | - | 동일 | split_entry | `max_ret1_pct` | 8.849700179324415 | `/split_entry/second_confirmation/max_ret1_pct` |
| 455 | 집행 | - | 없음 | split_entry | `require_ma60_support_bounce` | true | `/split_entry/second_confirmation/require_ma60_support_bounce` |
| 456 | 집행 | - | 동일 | stale_signal_replay | `min_signal_age_days` | 2 | `/stale_signal_replay/min_signal_age_days` |
| 457 | 집행 | - | 동일 | stale_signal_replay | `require_no_open_positions` | true | `/stale_signal_replay/require_no_open_positions` |
| 458 | 집행 | 2 | 동일 | entry_gap_up_reduce | `min_qty` | 1 | `/entry_gap_up_reduce/min_qty` |
| 459 | 집행 | 47 | 동일 | fx_entry_policy | `enabled` | true | `/fx_entry_policy/enabled` |
| 460 | 집행 | 2 | 없음 | min_qty | `min_qty` | 1 | `/min_qty` |
| 461 | 집행 | 47 | 없음 | news_topic_execution_policy | `enabled` | false | `/news_topic_execution_policy/enabled` |
| 462 | 집행 | 47 | 동일 | normal_entry_execution_quality | `enabled` | true | `/normal_entry_execution_quality/enabled` |
| 463 | 집행 | 47 | 동일 | normal_entry_execution_quality | `enabled` | false | `/normal_entry_execution_quality/qty_reduction/enabled` |
| 464 | 집행 | 2 | 동일 | normal_entry_execution_quality | `require_lob` | true | `/normal_entry_execution_quality/require_lob` |
| 465 | 집행 | 47 | 동일 | normal_intraday_realtime_policy | `enabled` | true | `/normal_intraday_realtime_policy/enabled` |
| 466 | 집행 | 2 | 동일 | normal_realtime_gap_policy | `block_close_pos_min` | 0.5 | `/normal_realtime_gap_policy/close_auction/block_close_pos_min` |
| 467 | 집행 | 21 | 동일 | normal_realtime_gap_policy | `block_v_accel_min` | 1 | `/normal_realtime_gap_policy/close_auction/block_v_accel_min` |
| 468 | 집행 | 47 | 동일 | normal_realtime_gap_policy | `enabled` | true | `/normal_realtime_gap_policy/enabled` |
| 469 | 집행 | 47 | 동일 | normal_realtime_gap_policy | `enabled` | true | `/normal_realtime_gap_policy/close_auction/enabled` |
| 470 | 집행 | 47 | 동일 | normal_realtime_gap_policy | `enabled` | true | `/normal_realtime_gap_policy/intraday_momentum_recheck/enabled` |
| 471 | 집행 | 47 | 동일 | normal_realtime_gap_policy | `enabled` | true | `/normal_realtime_gap_policy/overnight_gap_exit/enabled` |
| 472 | 집행 | 47 | 동일 | normal_realtime_gap_policy | `enabled` | true | `/normal_realtime_gap_policy/dynamic_slippage/enabled` |
| 473 | 집행 | 6 | 동일 | normal_realtime_gap_policy | `min_value_ratio` | 0.7 | `/normal_realtime_gap_policy/intraday_momentum_recheck/min_value_ratio` |
| 474 | 집행 | 2 | 동일 | normal_realtime_gap_policy | `reduce_close_pos_min` | 0.7 | `/normal_realtime_gap_policy/close_auction/reduce_close_pos_min` |
| 475 | 집행 | 7 | 동일 | normal_realtime_gap_policy | `reduce_v_accel_min` | 1.5 | `/normal_realtime_gap_policy/close_auction/reduce_v_accel_min` |
| 476 | 집행 | 7 | 동일 | normal_realtime_gap_policy | `reduce_v_accel_min` | 1.5 | `/normal_realtime_gap_policy/intraday_momentum_recheck/reduce_v_accel_min` |
| 477 | 집행 | 2 | 동일 | normal_realtime_gap_policy | `require_intraday` | true | `/normal_realtime_gap_policy/intraday_momentum_recheck/require_intraday` |
| 478 | 집행 | 47 | 동일 | split_entry | `enabled` | true | `/split_entry/second_confirmation/enabled` |
| 479 | 집행 | 12 | 없음 | split_entry | `max_open_to_entry_chase_pct` | 0.28 | `/split_entry/max_open_to_entry_chase_pct` |
| 480 | 관측 | - | **이탈** | cross_source_integrity | `block_on_missing_required` | true | `/cross_source_integrity/block_on_missing_required` |
| 481 | 관측 | - | **이탈** | global_outlier_watcher | `block_on_snapshot_missing` | true | `/global_outlier_watcher/block_on_snapshot_missing` |
| 482 | 관측 | - | **이탈** | parquet_max_open_files | `parquet_max_open_files` | 5 | `/parquet_max_open_files` |
| 483 | 관측 | - | 동일 | cross_source_integrity | `max_skew_days` | 1 | `/cross_source_integrity/max_skew_days` |
| 484 | 관측 | - | 동일 | global_outlier_watcher | `stale_max_age_days` | 2 | `/global_outlier_watcher/stale_max_age_days` |
| 485 | 관측 | - | 없음 | parquet_search | `max_open_files` | 20 | `/parquet_search/max_open_files` |
| 486 | 관측 | 47 | 동일 | cross_source_integrity | `enabled` | true | `/cross_source_integrity/enabled` |
| 487 | 관측 | 47 | 동일 | global_outlier_watcher | `enabled` | true | `/global_outlier_watcher/enabled` |
| 488 | 관측 | 1 | 동일 | normal_candidate_staleness_check | `block_if_stale` | false | `/normal_candidate_staleness_check/block_if_stale` |
| 489 | 관측 | 47 | 동일 | normal_candidate_staleness_check | `enabled` | true | `/normal_candidate_staleness_check/enabled` |
| 490 | 관측 | 1 | 동일 | normal_candidate_staleness_check | `max_stale_days` | 3 | `/normal_candidate_staleness_check/max_stale_days` |

---

## 5. [2026-09-09 신설] 하드코딩 문턱 전수 표 (코드 안, config 밖)

§1.1 의 분류 결과를 **표로 옮긴다.** 각주에만 두면 이 문서를 여는 사람은 여전히 보지 못한다.
번호는 config 표(1~490)와 겹치지 않게 **H** 접두를 쓴다.

`근거` 열이 전부 `-` 인 것은 실수가 아니다 - **31건 전부 PLANS·docs 근거 기록이 0건**이다.
`성격` 열: 라벨/집계 = 점수·선정에 닿지 않음 / **후보수** = 후보 개수를 직접 바꿈 / **판정** = 진입·청산 판정에 직접 관여.

| # | 분류 | 근거 | 성격 | 위치 | 식 | 값 |
|---|---|---|---|---|---|---|
| H01 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1283` | junk 등급 버킷 `s >= 85` | 85 |
| H02 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1285` | junk 등급 버킷 `s >= 70` | 70 |
| H03 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1287` | junk 등급 버킷 `s >= 50` | 50 |
| H04 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1289` | junk 등급 버킷 `s >= 30` | 30 |
| H05 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1385` | junk 플래그 `liq_r >= 0.55` | 0.55 |
| H06 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1387` | junk 플래그 `pump_r >= 0.55` | 0.55 |
| H07 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1389` | junk 플래그 `young_r >= 0.60` | 0.60 |
| H08 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1391` | junk 플래그 `fund_r >= 0.55` | 0.55 |
| H09 | 집행 | - | 라벨 | `generate_candidates_v41_1.py:1401` | 상한가 근사 `ret1 >= 29.0 or stretch >= 1.24` | 29.0 / 1.24 |
| H10 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1407` | `market_cap <= 3,000억` (KOSDAQ) -> junk_flags | 3.0e11 |
| H11 | 관측 | - | 집계 | `generate_candidates_v41_1.py:1417` | high_risk 카운터 `>= 70` | 70 |
| H12 | 관측 | - | 집계 | `generate_candidates_v41_1.py:1418` | extreme_risk 카운터 `>= 85` | 85 |
| H13 | 선정/승격 | - | **판정** | `generate_candidates_v41_1.py:1444` | `market_cap >= 1조` -> hard 문턱 88 -> 95 | 1.0e12 / 95.0 |
| H14 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1538` | fundamental 등급 `s >= 90` | 90 |
| H15 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1540` | fundamental 등급 `s >= 80` | 80 |
| H16 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1542` | fundamental 등급 `s >= 65` | 65 |
| H17 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1544` | fundamental 등급 `s >= 50` | 50 |
| H18 | 관측 | - | 라벨 | `generate_candidates_v41_1.py:1546` | fundamental 등급 `s >= 35` | 35 |
| H19 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1737` | 랠리 폭 통계 `ret >= 5.0` / `>= 10.0` | 5.0 / 10.0 |
| H20 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1749` | broad_rally 정의 A `(max>=20.0 and p5>=0.12) or p10>=0.10` | 20.0/0.12/0.10 |
| H21 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1756` | `level_num >= 5` and broad_rally -> min_total 16 | 5 / 16 |
| H22 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1758` | `level_num >= 5` -> min_total 12 | 5 / 12 |
| H23 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1770` | top_per_sector 3 vs 2 (`level_num >= 5`) | 3 / 2 |
| H24 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1914` | `level_num < 5` 분기 | 5 |
| H25 | 선정/승격 | - | **후보수** | `generate_candidates_v41_1.py:1924` | broad_rally 정의 B `max>=25.0 and p10>=0.08` | 25.0 / 0.08 |
| H26 | 선정/승격 | - | **후보수** | `optimize_params_v41_1.py:1067` | `lvnum >= 5 and len(chosen) < 5` | 5 / 5 |
| H27 | 선정/승격 | - | **후보수** | `optimize_params_v41_1.py:1069` | broad_rally 정의 B (시뮬) `_mx>=25.0 and _p10>=0.08` | 25.0 / 0.08 |
| H28 | 선정/승격 | - | 구조 | `optimize_params_v41_1.py:1476` | 최소 폴드 `n < 5` | 5 |
| H29 | 선정/승격 | - | **판정** | `optimize_params_v41_1.py:1751` | rule_e `sector_rs_min` | 0.05 |
| H30 | 가동 | - | **판정** | `paper_engine/exit.py:687` | `market_risk_score >= 60` -> 시장위험 청산 | 60 |
| H31 | 가동 | - | **판정** | `paper_engine/exit.py:688` | `market_risk_score >= 80` (이익중일 때) | 80 |

**우선순위: `성격=후보수` 9건.** 진입 기아 경로 위에서 후보 개수를 직접 바꾸는데 근거가 0이다.
`broad_rally` 두 정의(H20 / H25·H27)가 서로 함의하지 않는 문제도 여기 들어 있다 (PLANS (276)).

이 표는 **수동 분류분이다.** 정규식 스캔 83줄에서 오탐 51을 손으로 걷어낸 결과이며,
스캔 자체가 `[<>]=? 숫자리터럴` 패턴만 잡으므로 `in`/`between`/함수 인자 기본값 형태의
문턱은 **여전히 빠져 있다.** 전수라고 인용하지 말 것.

---

## 변경 이력

| 일자 | 변경 | 사유 |
|---|---|---|
| 2026-09-09 | **§5 하드코딩 문턱 표 31건 신설.** 스캔 83줄 -> 오탐 51 제거 -> 실물 31건 등재 | 이 표가 config 만 세고 있었다. 각주로 두면 표를 여는 사람이 못 본다 |
| 2026-08-31 | **DEFAULT_CONFIG 전수 대조 추가. 진짜 미분류 252 -> 32.** 전수 표에 `기본값` 열 추가, 이탈 항목을 앞으로 | 값이 코드 기본값과 같으면 "결정된 적이 없다" 이지 "근거 없이 골랐다" 가 아니다 |
| 2026-08-31 | 확인필요 38 -> 0 해소, market_ops_policy 키별 배정, drawdown_manager 발견 2건, §2.2 자체 정정 | 1차 분류의 미해결분을 코드·실효설정으로 닫았다 |
| 2026-08-31 | 1차 전수 분류 490건 | 명세 §5 의 분류 틀을 실제로 적용 |
