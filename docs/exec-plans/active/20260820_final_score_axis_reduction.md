# ExecPlan — final_score 축 축소 (8축 가중합 -> 검증된 2축 공식)

- 작성: 2026-08-20
- 상태: **적용 완료 (2026-08-20).** (상태줄 정정 2026-08-24 — "적용 진행"에서 완료로)
- 상태줄 정정 근거: 2026-08-24 실측으로 생산 코드에서 확인됨 —
  `tools/final_score_merge_daily.py` 의 `final_score_axis_mode = "TECH_FUND_2AXIS_20260820"`,
  `final_score_base = 기술×0.75 + 재무×0.25`, 그리고 격리된 7개 가중치는
  2191행 이후 **산술 참조 0건**(전수 조회). `.agent/PLANS.md` 2026-08-24 (65).
- 분류: **매매 정책 변경** (AGENTS.md 5). 9 ExecPlan / 11 E2E / 14 백업
- 근거 기록: `.agent/PLANS.md` 2026-08-20 (74)(78)(79)(80)

## 1. 문제 — 후보 선정과 순위가 다른 축을 쓴다

`tools/final_score_merge_daily.py:2209`:
```
final_score_base = sector*w + regime*w + news*w + fx*w
                 + fundamental_quality*w + fundamental_prereflection*w
                 + policy*w + forecast*w
```
**8축 어디에도 기술 점수(`score`)가 없다.** 파일 전체에서 `score`/`tech_score`/`w_tech`
사용처 **0건**. `score` 컬럼은 사이드카에 존재하나(중앙 0.5288) **계산에 쓰이지 않는다.**

반면 후보 **선정**은 기술 지표로 한다 — `v_accel > 6.6`, `rs`, `high_52w_gap`, `stretch`, `atr`.

-> **선정은 기술, 순위는 비기술.** `entry.py:6047 cap_signal_top_n` 이 순위로 자르므로
**기술적으로 가장 강한 종목이 잘려나갈 수 있다.**

실측(2026-08-19, 22행) 축별 기여:

| 축 | 중앙값 | 비영 행 |
|---|---:|---:|
| `regime_score` | 0.2000 | **22/22** |
| `news_score` | 0.7500 | 12 |
| `forecast_score` | 0.1395 | **22/22** |
| `sector` / `fx` / `fundamental*` / `policy` | 0.0000 | 0~10 |

실질적으로 **regime · news · forecast 셋**이 순위를 만든다. 셋 다 검정된 적이 없다.

## 2. 왜 축을 줄이는가 — 근거

**(c) 전략 전제**에서 유도된다(11절 원칙 적용).
전략 전제는 기술적 모멘텀 선별이다(후보 생성층 10개 게이트 전부 기술 지표).
**순위가 전제와 다른 축을 쓰면 선정과 실행이 어긋난다.**

그리고 **분해 가능성**: 8축 가중합은 어느 축이 결과를 만들었는지 사후 분해가 불가능하다.
2026-08-19 사례에서 `medium_news` +0.05 가 순위를 뒤집었는데 총점만 보면 보이지 않았다.
Q3(수익률)을 물으려면 무엇이 결과를 만들었는지 분해할 수 있어야 한다.

## 3. 변경

`final_score_base` 를 **base CSV 가 이미 쓰는 공식**으로 교체한다:
```
final_score_base = clip(score,0,1) * w_tech + clip(fundamental_score/100,0,1) * w_fund
                   (w 정규화, stable_params 기준 0.75 / 0.25)
```

**이 공식은 새로 만드는 것이 아니다.** `generate_candidates_v41_1.py:1594-1606` 에 이미 있고,
**2026-08-20 에 10행 전부 차이 0.0 으로 재현 검증됐다**(PLANS (74) B항).

나머지 6축(`sector` `regime` `fx` `news` `policy` `forecast` 및 `fundamental_prereflection`)은
**컬럼으로 계속 산출하되 `final_score_base` 에 반영하지 않는다** — `observe_only` 격리.
제거가 아니라 격리인 이유: 제거하면 나중에 검정할 데이터도 사라진다.
2026-07-24 에 표본 부족으로 유예된 뉴스/펀더멘털 검정을 언젠가 하려면 데이터가 필요하다.

`execution_lob_adjustment`(가산)와 `medium_news_adjustment`(승산, 2026-08-20 변경)는 **유지**한다.
전자는 Q1 6번으로 별도, 후자는 오늘 이미 스케일 정합을 맞췄다.

## 4. 하지 않는 것

- 후보 생성층 게이트 변경 없음
- 진입 게이트 변경 없음
- 6축의 산출 중단 없음 — 컬럼은 계속 생성된다
- `w_news` 등 축별 가중치 삭제 없음 — 값은 남기되 `final_score_base` 에 안 쓴다
- **레짐별 가중치 테이블(`_ASOF_WEIGHTS`) 삭제 없음** — 관측용으로 보존

## 5. 검증 (AGENTS.md 6)

| 항목 | 방법 | 즉시 |
|---|---|---|
| 기능 | 구문 검사, 08-19 22행 재계산으로 새 공식 산출 확인 | O |
| 정합성 | 새 `final_score` 가 base CSV 의 `final_score` 와 정합하는지 | O |
| 운영 반영 / E2E(11) | `orders -> fills -> ledger -> stats` | **후보 발생일 대기** |
| 정책 | 6축 컬럼이 계속 생성되는지, 진입/후보 게이트 불변 | O |
| FAIL-CLOSED | `score` 결측 시 0.0 -> `fundamental` 결측 시 50.0 폴백 유지 | O |
| 회귀 | 순위 변화 규모 측정 (전면 재편이 예상되므로 기록) | O |

## 6. 위험

- **순위가 전면 재편된다.** 오늘 시작하려던 관측의 기준이 바뀐다.
  다만 **후보가 0행이라 지금이 리셋 비용이 가장 싼 시점**이다
- 새 공식도 **검정된 것이 아니다.** 근거는 "더 좋다"가 아니라
  **"전제와 정합하고 분해 가능하다"**이다. 이 구분을 흐리지 말 것
- 6축을 격리하면 그 축들이 담고 있던 정보가 순위에서 빠진다.
  **정보량 감소는 의도된 대가**이며, 축은 검정을 통과할 때만 다시 편입한다
