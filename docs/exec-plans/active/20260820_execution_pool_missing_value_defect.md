# ExecPlan — execution_pool 결측값이 차단 결정으로 둔갑하는 결함

> ## ⛔ 이 계획서는 무효다 (2026-08-20)
>
> 전제였던 "`execution_pool` 게이트가 정상 후보를 차단한다"가 **사실이 아니다.**
> 게이트를 켜는 설정 `positive_entry_criteria.require_execution_pool_when_present`가
> 원본과 런타임 스냅샷 모두에서 **이미 `False`**이며(최소 2026-07-13부터),
> `entry.py:5910`과 `:5549` 두 게이트 모두 **실행되지 않는다.**
>
> 작성자가 게이트 코드만 읽고 그 코드를 켜는 설정값을 확인하지 않았다.
> 아래 본문은 **오류 기록으로만 보존**한다. 실행 근거로 인용하지 말 것.
> 철회 상세: `.agent/PLANS.md` 2026-08-20 (42).
>
> 본문 중 유일하게 유지되는 사실: `final_score_merge_daily.py:1459`의 `.fillna("False")`가
> 없는 값을 날조하는 것은 결함이 맞다. 다만 **소비 게이트가 꺼져 있어 차단으로 이어지지 않는다.**

- 작성: 2026-08-20
- 상태: **무효 (2026-08-20 철회). 전제가 사실이 아니었다. 어떤 변경도 적용되지 않았다.**
- 분류: **매매 정책 변경** (AGENTS.md 5). 9 ExecPlan 필수 / 11 E2E 증거 필수 / 14 백업 필수
- 근거 기록: `.agent/PLANS.md` 2026-08-19 (37)(39)(40), 2026-08-20 본 계획

## 1. 확정된 사실

`execution_pool`이 True가 될 경로가 정상 후보에는 존재하지 않는다. 3단 결정론적 체인:

1. **`generate_candidates_v41_1.py`** — `execution_pool` 등장 **0회**. `keep_cols`(2212행)에도 없다.
2. **`tools/final_score_merge_daily.py:1407 _restore_lineage_columns()`** — 컬럼이 `df`/`base`
   양쪽에 없으면 `""`로 만들고, 1459-1467행
   `.str.lower().map({"true":"True","false":"False"}).fillna("False")`로 **`"False"` 확정**.
3. **`paper_engine/entry.py:5910-5932`** — 1차 필터. 여기서 **행을 잘라낸다**:
   ```
   if "execution_pool" in candidate_df.columns and _req_ep:
       entry_mask = execution_pool.isin(["TRUE","1","Y","YES"])   # 전부 False
       ... union_mask 병합 ...
       candidate_df = candidate_df[entry_mask].copy()             # 나머지 전량 삭제
   ```
   (`entry.py:5549`의 행 단위 게이트는 2차이며, 여기서 이미 잘린 뒤다.)

**결과**: `SECTOR_PREFILTER_UNION` + `sector_action=BUY` + `sector_entry_allowed` +
`sector_strength >= 0.65`(union_mask)를 만족하지 않는 **정상 후보는 진입 평가에 도달하지 못한다.**

**검증**: 2026-08-14 `candidates_latest_data.with_final_score.csv` 23행 전부 `execution_pool=False`.
실체결 측: 관측 30거래일 v41.1 정상 경로 진입 **0건**, 체결 38건 전부 우회 경로 (PLANS (38)).

**부작용 하나 더**: 로그가 `[ENTRY_POOL] execution_pool=True applied: before->after`로 찍힌다.
생존자가 union뿐일 때도 같은 문구라 **컬럼이 조작된 값이라는 사실이 로그에서 보이지 않는다.**

## 2. 이것은 기술 결함인가 정책 공백인가

`.fillna("False")` 한 줄을 고치는 문제로 보이지만, AGENTS.md 5("FAIL-CLOSED 원칙 유지",
"Gate 의미 변경 금지")를 대면 **정책 선택이 먼저다.**

`execution_pool`이 무엇이어야 하는지 두 가지 해석이 가능하고, 코드는 지금 **어느 쪽도 아니다.**

| | 정책 A: 필수 조건 | 정책 B: 조건부 보조 |
|---|---|---|
| 의미 | 실행 풀에 든 후보만 진입 | 값이 있으면 쓰고 없으면 건너뜀 |
| 코드 근거 | `entry.py:5910` 필터, `require_execution_pool_when_present` 기본 True | `"execution_pool" in columns` 조건부 발동 = 결측 시 skip 설계 |
| 성립 조건 | **산출 주체가 있어야 한다** | 결측이 결측으로 남아야 한다 |
| 현재 | 산출 주체 없음 -> 전량 차단 | `.fillna("False")`가 결측을 없앰 -> skip 불가 |

**현재 상태는 A를 표방하면서 A의 전제(산출 주체)가 없는 상태다.**
그래서 "모든 정상 후보 차단"이라는, 어느 정책도 의도하지 않은 결과가 나온다.

FAIL-CLOSED 관점의 구분이 중요하다:
- 정당한 fail-closed = **"데이터가 없으니 막는다"**
- 현재 = **"데이터가 없는데 있다고 착각하고 막는다"**
후자가 더 나쁘다. 로그·산출물 어디에도 컬럼이 조작됐다는 흔적이 없어 **관측으로 발견되지 않는다.**
실제로 이 상태가 최소 2026-04부터 지속됐고 (PLANS (38) 경로별 월별표: 04~05월 PURE_NORMAL 0건)
8월 19일까지 아무도 몰랐다.

## 3. 선택지

| 안 | 내용 | 5 정합성 | 비용 | 결과 |
|---|---|---|---|---|
| **A1** | 산출 주체 신설 — `generate_candidates_v41_1.py`가 `execution_pool`을 실제 계산 | 게이트 의미 유지, fail-closed 유지 | **높음. "실행 풀" 정의를 새로 만들어야 함** | 게이트가 처음으로 실제 데이터로 동작 |
| **B1** | `.fillna("False")` -> 빈 값 보존. 결측 시 `entry.py:5910` 조건이 자연히 skip | 게이트 의미 유지, **fail-open 발생** | 낮음. 1줄 | 정상 후보가 진입 평가에 도달 |
| **B2** | `require_execution_pool_when_present`를 False로 | **게이트 의미 변경 = 5 위반 소지** | 낮음. 설정 1개 | B1과 동일 결과, 근거는 약함 |
| **C** | 유지 | - | 0 | v41.1은 계속 매매하지 않음. Q3 영구 불가 |

**권고: B1을 임시 조치로, A1을 본 조치로.**
- B1은 **결함 발생 지점에서 고친다**. `.fillna("False")`는 lineage **복원** 함수의 코드이며,
  복원할 원본이 없는 값을 만들어내는 것은 함수 이름과 모순된다. 이것은 정책 변경이 아니라 **버그 수정**이다.
- 다만 B1 적용 후 게이트는 **무의미해진다**(항상 skip). 그것이 정직한 현재 상태의 표현이다.
  게이트가 일하게 하려면 A1이 필요하고, A1은 별도 정책 결정이다.
- **B1은 fail-open을 만든다. 이것이 AGENTS.md 5와 충돌하는지는 사용자 판단이 필요하다.**
  본 계획은 "결측을 결정값으로 바꾸는 것"이 fail-closed가 아니라고 보지만, 이 해석 자체가 승인 대상이다.

## 4. B1 적용 시 절차 (승인 후)

1. **백업** (14): `backup/20260820_execution_pool_fillna_fix/YYYYMMDD_HHMMSS/`
   - `tools/final_score_merge_daily.py`
2. **수정**: 1459-1467행. `execution_pool`/`natural_pass`의 `.fillna("False")` 제거,
   빈 문자열을 빈 문자열로 보존. `"true"/"false"` 명시값의 정규화는 유지.
3. **로그 개선(동반)**: `entry.py:5932` 문구를 생존 경로별로 분리
   (`execution_pool=True` n건 / `union_conditional` n건) — 같은 실수의 재발 방지
4. **검증 (6개 항목)**
   - 기능: 수정 후 `final_score_merge_daily.py` 재실행, `execution_pool` 컬럼이 빈 값으로 남는지
   - 정합성: `entry.py:5910` 조건이 skip으로 가는지 로그 확인
   - **운영 반영 / E2E (11)**: `orders(D) -> fills(D) -> ledger -> stats`.
     **현재 후보가 0행이라 즉시 E2E 불가.** 후보가 발생하는 날까지 대기해야 완료 판정 가능
   - 정책: 게이트 의미 불변 확인
   - FAIL-CLOSED: skip 경로가 다른 안전장치(`final_score>0`, `min_entry_score`)로 여전히 막히는지
   - 회귀: union 경로 후보가 종전과 동일하게 통과하는지
5. **롤백**: 백업 복원 1파일

## 5. 적용하지 않을 것

- `natural_pass`의 의미 변경 — 같은 `.fillna`를 타지만 별건이며 영향 미조사
- `entry.py:5549` 2차 게이트 — 1차에서 이미 잘리므로 B1만으로 충분한지 미확인
- `union_entry_strength_min=0.65` 등 union 경로 파라미터
- `stable_params_v41_1.json` (sha256 `4ed8011346787d3c` 불변)

## 6. 이 변경이 하지 않는 것

**수익성 개선이 아니다.** B1은 v41.1이 **처음으로 진입 표본을 만들게** 할 뿐이다.
후보 생성층에 별도 병목이 남아 있고(완전 완화에도 무후보일 33.1%, PLANS 07-27),
적용 후 Q2 합격 기준(PLANS (38) 4축)을 통과한다는 보장은 없다.
**목적은 Q3을 가능하게 하는 것이지 Q3의 답을 좋게 만드는 것이 아니다.**

---

## 7. [개정 2026-08-20] B1안 재작성 - 원안은 효과가 없다

원안 B1(`.fillna("False")` 제거)은 **차단을 풀지 못한다.**

- `final_score_merge_daily.py:1418-1420` — `if col not in df.columns: df[col] = ""`.
  **컬럼을 무조건 생성한다.** `.fillna`를 제거해도 값이 `""`로 남는다.
- `entry.py:5910` — `if "execution_pool" in candidate_df.columns` — **존재 여부만 본다.**
- `entry.py:5913` — `entry_mask = ...isin(["TRUE","1","Y","YES"])` — **`""`도 탈락한다.**

즉 `.fillna("False")`는 증상이고, 차단을 성립시키는 것은 **컬럼 생성 자체**다.

### 개정 선택지

| 안 | 변경 지점 | 변경 규모 | 롤백 | 5 쟁점 |
|---|---|---|---|---|
| **B2** | `require_execution_pool_when_present` -> False | **설정 1개, 코드 0** | 값 하나 | 설계된 스위치를 쓰는 것. 명시 승인 시 5 저촉 아님 |
| **B1'** | `final_score_merge_daily.py:1418-1420` 컬럼 생성을 조건부화 | 코드 3행 | 백업 1파일 | fail-open 발생. 원 설계 복원 |
| **A1** | `generate_candidates_v41_1.py`가 실제 산출 | 신규 정의 + 코드 | 큼 | 정합. 단 "실행 풀" 정의를 새로 만들어야 함 |
| **C** | 유지 | 0 | - | v41.1 영구 미매매 |

### 권고 개정: B2 -> B1' -> A1 순서

1. **B2를 먼저.** 코드를 안 바꾸고 관측을 시작할 수 있고 롤백이 값 하나다.
   이 시스템은 코드 변경마다 새 놀라움이 나왔으므로(본 계획서 자체가 두 번 개정됐다)
   **가장 되돌리기 쉬운 수단으로 먼저 관측을 여는 것**이 맞다.
2. **B1'로 결함 제거.** B2는 `.fillna` 결함을 남긴다. 다른 소비자(`entry.py:456`,
   `positions.py`, audit 도구 다수)가 여전히 조작된 `False`를 본다.
3. **A1은 정책 결정으로 분리.** 게이트가 일하게 하려면 필요하지만 별건이다.

### B2 적용 전 확인 필요 (미검증)

- `entry.py:5910`의 `_req_ep`는 `cfg["positive_entry_criteria"]["require_execution_pool_when_present"]`,
  `entry.py:5549`는 `pol.get(...)`에서 읽는다. **두 곳이 같은 설정 섹션인지 미확인.**
  다르면 B2를 두 곳에 적용해야 한다.
- B1' 적용 시 컬럼 부재를 전제하지 않는 소비자에서 KeyError 가능성. 39개 파일 미조사.
