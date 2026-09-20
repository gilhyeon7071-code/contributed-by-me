# ExecPlan — 산출물의 자기 기술 정정 (B3)

- 작성: 2026-08-24
- 상태: **적용 완료 (2026-08-24 09:56).** 검증은 6절.
- 분류: **기록·메타데이터만 변경.** `final_score` 값과 진입 판단은 건드리지 않는다
- 근거 기록: `.agent/PLANS.md` 2026-08-24 (65)(67)(68)

## 1. 문제 — 세 곳이 사실과 다르게 적는다

### (가) 재현검증식이 08-20 변경을 안 따라갔다

`tools/archive_final_score_snapshot.py:119`
```
err = (base + lob - fs).abs()          <- 가산만
```
그러나 생산 공식은 2026-08-20 부터 승산이다 (`final_score_merge_daily.py:2264`)
```
final_score = (base + lob) * (1 + medium_news_adjustment)
```
실측(2026-08-24, 22행):
```
가산식  20/22 일치  최대오차 0.00667
승산식  22/22 일치  최대오차 0.0
불일치 2행 = medium_news_adjustment 비영 2행 (267250, 282330)
```
**아카이버 자신의 무결성 검사가 286개 스냅샷 내내 실패하고 있었고 아무도 읽지 않았다.**
검사가 있는데 신호로 못 쓰이는 상태 = 없는 것보다 나쁘다.

### (나) axis_mode 와 axis_source 가 같은 행에서 모순된다

매니페스트 286행 전부:
```
axis_mode    TECH_FUND_2AXIS_20260820                                  <- 사실
axis_source  ASOF_BLEND_CAUTION_..._NEWS_ON_POLICY_OFF_FORECAST_ON_PREREF_ON   <- 거짓
```
`NEWS_ON` / `FORECAST_ON` / `PREREF_ON` 은 (80) 격리 이후 전부 미적용이다.

### (다) 가중치 컬럼이 안 쓰인 값을 적는다

`final_score_merge_daily.py:2269-2274` 가 `final_score_w_news` 등에 레짐 테이블의
**명목 가중치**를 적는데, 그 값은 `final_score` 산술에 들어가지 않는다((65) 전수 조회: 산술 참조 0건).
`archive_final_score_snapshot.py:51-53` 이 이 컬럼들을 이력으로 보존한다.
-> 훗날 "그날 뉴스 가중치가 얼마였나"를 물으면 0 이 아닌 값이 나온다.

## 2. 하지 않는 것

- `final_score` / `final_score_base` 값 변경 없음
- 축 점수 컬럼 산출 중단 없음 — (80) 의 격리 방침(제거 아닌 보존) 유지
- 진입·후보 게이트 변경 없음
- 기존 스냅샷 286개 소급 수정 없음 (이력은 이력대로 둔다)
- 컬럼 **이름** 변경 없음 — 아카이브 스키마와 소비처 2곳을 깨지 않기 위해

## 3. 변경

### (가) 재현식을 생산 공식과 일치시킨다
`archive_final_score_snapshot.py`
```
mn   = num("medium_news_adjustment").fillna(0.0).clip(-0.95, 0.95)
recon = ((base + lob) * (1.0 + mn)).clip(0.0, 1.0)
err   = (recon - fs).abs()
```
출력 문구도 `base + exec_lob` -> `(base + exec_lob) * (1 + medium_news)` 로.

### (나) 라벨이 적용된 것만 말하게 한다
`final_score_merge_daily.py` 의 `blend_policy`:
```
전 ASOF_BLEND_{레짐}_{장중}_NEWS_ON_POLICY_OFF_FORECAST_ON_PREREF_ON
후 ASOF_BLEND_{레짐}_{장중}_AXIS2_TECHFUND_NEWSADJ_{ON|OFF}_LOBADJ_{ON|OFF}
```
`AXIS2_TECHFUND` 는 `final_score_axis_mode` 와 같은 사실을 말한다.
뒤의 둘은 **실제로 곱해지고 더해지는** 두 조정항의 적용 여부다.

### (다) 가중치 컬럼은 실효값을 적는다
```
final_score_w_news / policy / forecast / fundamental_quality / fundamental_prereflection
  -> 0.0 (실효값. 이 축들은 final_score 산술에 들어가지 않는다)
final_score_w_execution_lob_adjustment -> 0.03 유지 (실제로 적용되는 항이다)
```
명목값은 버리지 않고 **상태 JSON 에 이름을 붙여 남긴다**:
```
weights            -> 실효 (적용된 것)
weights_nominal_unapplied -> 레짐 테이블이 줬을 값 (관측용)
```
축을 다시 편입할 때 필요한 데이터라 버리지 않는다((80) 방침과 같다).

## 4. 검증

| 항목 | 방법 |
|---|---|
| 구문 | `py_compile` 두 파일 |
| 기능 | merge 재실행 후 `final_score` 가 **변하지 않는지** 이전 값과 전 행 대조 |
| 재현검증 | 아카이버 재실행 -> `reconstruct_match_rows` 가 22/22, `max_err` 0.0 |
| 라벨 | `axis_source` 에 `NEWS_ON` 이 더 이상 없는지 |
| 소비처 | `build_future_signal_candidate_shadow_join.py` 가 읽는 `final_score_w_forecast` 존재 유지 |
| 회귀 | 컬럼 개수·이름 불변 확인 |

## 5. 위험

- `final_score_source` 문자열 모양이 바뀐다. 문자열을 **파싱**하는 소비처가 있으면 깨진다.
  조회 결과 소비처는 2곳이고 둘 다 기록·미리보기 용도라 파싱하지 않는다. 그래도 적용 후 확인한다
- 매니페스트의 `axis_source` 가 286행째부터 값이 달라진다. **의도된 것이다** —
  이력에서 "언제부터 사실을 적기 시작했는지"가 보이는 편이 낫다

---

## 6. 적용 결과 (2026-08-24 09:52~09:56)

**상태: 적용 완료.**

백업 `backup/20260824_b3_self_description/20260824_095200/` (두 파일 원본).

### 검증

| 항목 | 결과 |
|---|---|
| 구문 | `py_compile` 두 파일 PASS |
| 컬럼 회귀 | 200 -> 200, 이름 순서까지 동일 |
| `final_score_base` | 전 행 변화 0.0 |
| 재현검증 | `20/22 (err 6.2e-3)` -> **`22/22 (err 2e-7)`** |
| 라벨 | `ASOF_BLEND_CAUTION_NEUTRAL_INTRADAY_AXIS2_TECHFUND_NEWSADJ_ON_LOBADJ_ON` |
| 가중치 컬럼 | news/policy/forecast/fund_quality/prereflection = 0.0, lob = 0.03 유지 |
| 상태 JSON | `weights`(적용분) / `weights_nominal_unapplied`(명목) 분리, `axis_mode` 추가 |
| 소비처 | `final_score_w_forecast` 컬럼 존재 유지 (값만 0.0) |

### final_score 가 움직인 것에 대한 귀속

재실행 후 `final_score` 가 최대 0.0305 움직였다. **내 변경 때문이 아니다.**
```
공식 재현 (base+lob)x(1+mn)   22/22 일치, 최대오차 0.0   -> 산식 불변
execution_lob_adjustment      21행 중 20행이 변함 (최대 0.0277)  -> 장중 실시간 입력
lob 이 안 변한 행의 final_score 변화   정확히 0.0
```
**lob 이 고정된 행에서 점수 변화가 0 이라는 것이 귀속의 증거다.**
`execution_lob_adjustment` 는 장중 갱신되는 입력이고 기준 스냅샷은 08:50, 재실행은 09:54 였다.

### 매니페스트에 전후가 남았다

```
08:56  ...NEWS_ON_POLICY_OFF_FORECAST_ON_PREREF_ON   20/22  err 6.2e-3  w_news 0.08
09:55  ...AXIS2_TECHFUND_NEWSADJ_ON_LOBADJ_ON        22/22  err 2e-7   w_news 0.00
```
소급 수정은 하지 않았다. 이력에서 **언제부터 사실을 적기 시작했는지**가 보인다.

### 부수 효과

아카이버의 `[NOTE] source 는 ... 이라 하지만 산술상 반영되지 않는다` 경고가 더 이상 찍히지 않는다.
`claimed_on` 이 비었기 때문이며, 경고할 것이 없어진 것이 맞다.
