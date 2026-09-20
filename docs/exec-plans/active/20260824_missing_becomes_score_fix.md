# ExecPlan — 결측이 점수가 되는 것을 멈춘다 (B1)

- 작성: 2026-08-24
- 상태: **적용 완료 (2026-08-24 13:02).** 검증 결과는 PLANS (82).
- 분류: **점수 산출 변경.** 진입 판단에 닿을 수 있으므로 매매 영향 검토 포함
- 근거 기록: `.agent/PLANS.md` 2026-08-24 (65) B1 · (67) 등급 확정 · (79)

## 1. 문제

`tools/final_score_merge_daily.py:2247-2249`
```python
_tech = pd.to_numeric(df.get("score")).fillna(0.0)             # 결측 -> 0.0
_fund = (pd.to_numeric(df.get("fundamental_score")).fillna(50.0) / 100.0)   # 결측 -> 50점
df["final_score_base"] = (_tech * 0.75 + _fund * 0.25)
```
둘 다 결측이면 `0*0.75 + 0.5*0.25 = 0.125` 가 **창작된다.**
아는 것이 하나도 없는 종목이 "중립 재무를 가진 종목"으로 번역된다.

실측(2026-08-24 산출물 22행):
```
date 결측 12행  ==  candidate_origin_hybrid=NEWS_ONLY 12행   (정확히 일치)
그 12행은 score / fundamental_score 가 **둘 다 결측**이고 base 가 전부 0.125
나머지 10행은 TECH 9 + TECH+NEWS 1
```

이 행들은 `merge:1644` 에서 **전 컬럼 NA 템플릿**으로 append 된 것이다.

### 지금 진입을 막는 것은 설계가 아니라 우연이다
`(67)` 에서 확인했다. `date` NaN -> `astype(str)` -> `'nan'` -> 숫자만 남기면 `''` 가 되어
날짜 필터에 걸린다. **"날짜 없는 행을 버린다"는 의도가 코드 어디에도 없다.**
merge 템플릿이 `date` 를 채우는 순간 12행이 그대로 통과한다.

## 2. 변경 — 두 겹

### (가) 창작 금지 — 모름을 모름으로
tech 와 fund 가 **둘 다 결측**인 행은 `final_score_base` 를 만들지 않는다(NaN).
한쪽만 결측이면 기존 폴백을 유지한다(그 경우는 정보가 하나는 있다).
```python
_tech_raw = pd.to_numeric(df.get("score"), errors="coerce")
_fund_raw = pd.to_numeric(df.get("fundamental_score"), errors="coerce")
_both_missing = _tech_raw.isna() & _fund_raw.isna()
... 기존 계산 ...
df.loc[_both_missing, "final_score_base"] = pd.NA
df["score_inputs_missing"] = _both_missing        # 새 컬럼. 사실을 기록한다
```

### (나) 명시적 분리 — NEWS_ONLY 를 별도 산출물로
**이것은 이미 기록된 계획이다** — `.agent/PLANS.md` 2026-08-22 (57) 남은 것 ④
"병합 단계의 행 추가 분리 (①-1) - NEWS_ONLY 를 별도 산출물로".
```
메인   candidates_latest_data.with_final_score.csv       <- 순위·진입용. NEWS_ONLY 제외
분리   candidates_latest_data.news_only.csv              <- 신설. 전 컬럼 보존
```
**데이터를 버리지 않는다.** (58)의 "(나) 격리 유지, 산출 계속" 방침과 같다 —
축 데이터는 나중에 검정하려면 필요하다.

## 3. 하지 않는 것

- 뉴스 수집·점수 산출을 멈추지 않는다
- 한쪽만 결측인 경우의 폴백을 바꾸지 않는다
- 날짜 필터(`entry.py`)를 건드리지 않는다 — 거기는 정상 동작 중이다
- 6축 컬럼 산출을 중단하지 않는다

## 4. 매매 영향 검토

```
entry.py pick_candidates  사이드카를 읽고 date 로 필터한다 -> NEWS_ONLY 12행은 **이미 탈락한다**
                          메인에서 빼도 결과가 같다. **동작 불변**
archive_final_score_snapshot  rows_news_only 가 0 이 된다(스키마 유지).
                          AXIS_COLS 통계가 10행 기준이 된다 - 더 정확해진다
build_future_signal_candidate_shadow_join  미리보기 도구. NEWS_ONLY 는 새 산출물에서 읽으면 된다
```
**즉 이 변경은 진입 결과를 바꾸지 않는다.** 우연히 막히던 것을 설계로 막는 것이다.

## 5. 검증

| 항목 | 방법 |
|---|---|
| 구문 | `py_compile` |
| 기능 | 재실행 후 메인 10행 / news_only 12행으로 분리되는지 |
| 창작 금지 | 메인에 `final_score_base == 0.125` 인 행이 0인지 |
| 회귀 | **10행의 `final_score` 가 변하지 않는지** (lob 실시간 변동분 귀속 후) |
| 컬럼 | 메인 컬럼 이름·순서 불변 + `score_inputs_missing` 추가 |
| 소비처 | 아카이버 재실행 -> 재현검증 통과 유지 |
| 매매 | `pick_candidates` 의 `[CAND_D_FILTER]` 결과가 이전과 동일한 10행인지 |

---

## 6. 적용 결과 (2026-08-24 13:01~13:02)

| 항목 | 결과 |
|---|---|
| 구문 | `py_compile` PASS |
| 분리 | 메인 22행 → **10행** / `news_only.csv` **12행** / 합계 22 보존 |
| 창작 금지 | 메인에 `base == 0.125` 인 행 **0** |
| 모름 보존 | `news_only` 12행 전부 `final_score_base` 결측 (12/12) |
| 회귀 | 공통 10종목 `final_score` 최대차이 **0.00000000** |
| 컬럼 | 200 → 201 (`score_inputs_missing` 추가만, 사라진 것 없음) |
| 소비처 · 아카이버 | 10행 201컬럼, 재현검증 **10/10 err 0.0**, `NEWS_ONLY=0` |
| 소비처 · entry | 날짜필터 fallback → **10행. 이전과 동일** |

**진입 결과 불변.** 우연히 막히던 것을 설계로 막았다.

부수: 매니페스트의 `rows_news_only` 가 앞으로 0 으로 기록된다. 스키마는 유지되고
값의 의미가 바뀐 것이며, 이력에서 언제부터 분리했는지가 보인다. 소급 수정은 하지 않는다.
