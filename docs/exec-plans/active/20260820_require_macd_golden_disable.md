# ExecPlan — require_macd_golden 비활성화 (검증 가능성 확보)

- 작성: 2026-08-20
- 상태: **B안 철회 (2026-08-20). 1절~3절의 진단은 유효, 5절 B안은 무효.**

> **[철회 2026-08-20]** 5절 B안(`promoted=false` + 연구 모드 환경변수)은 **게이트 무력화**다.
> `CANDIDATE_RESEARCH_ALLOW_UNAPPROVED=1`을 생산 배치에 상시 세팅하면
> `generate_candidates_v41_1.py:2070`의 `or` 우변이 항상 참이 되어
> **이번 파라미터뿐 아니라 앞으로 어떤 stable_params에 대해서도 게이트가 판정력을 잃는다.**
> 계획서에 "1회성 한정"이라 써놓고 수단은 영구적이었다.
>
> 그리고 이는 **원인 해결이 아니다.** 원인은 `promoted` 하나가 "인증"과 "가동"을
> 동시에 의미해 **"검증 안 됨 + 그래도 가동"을 표현할 수단이 없는 것**이다.
> 원인 해결은 `docs/exec-plans/active/20260820_certified_operational_split.md`로 옮겼다.
>
> 1~3절(순환 구조 진단, `macd_golden`이 이벤트 조건이라는 발견, 정량 효과)은 **유효하며
> 그대로 유지한다.** `require_macd_golden` 변경 자체는 원인 해결 방향이 맞으나,
> **분리 설계가 선행되어야 정직하게 적용할 수 있다.**
- 분류: **매매 정책 변경** (AGENTS.md 5). 9 ExecPlan 필수 / 11 E2E 증거 필수 / 14 백업 필수
- 근거 기록: `.agent/PLANS.md` 2026-08-20 (61)(62)(63)
- **목적: 성과 개선이 아니라 검증 가능성 확보다.** 현재 표본으로는 이 전략이 되는지 물을 수 없다.

## 1. 문제 — 순환 구조

```
후보 게이트의 곱  ->  표본 부족  ->  HPO 채점 불가  ->  승격 불가
                                                        -> 파라미터 교체 불가 -> 게이트가 계속 좁다
```

게이트별 통과율 (270,630 종목-일, 2025-10-31~2026-08-18):
`value_min` **0.70%** x `v_accel_lim` **1.65%** x `macd_golden` **4.14%** -> 전체 AND **0.0015% (4건)**

승격 차단의 1차 사유는 PF가 아니라 **`insufficient_folds`**다
(selection fold 2개 < `HPO_MIN_FOLDS=3`, PLANS 2026-08-15 (5)).

## 2. 왜 `macd_golden`인가 — 근거 다섯

1. **정의가 이벤트다.** `generate_candidates_v41_1.py:501`
   `(macd_line > macd_signal) & (prev_macd <= prev_sig)` -> **교차 발생 당일만 True.**
   실측: 종목당 관측일 중앙 134일 중 True **6일**(4.10%).
   **나머지 9개 게이트는 전부 상태 조건이다.** 이벤트 하나를 상태 아홉과 AND로 묶으면
   "교차가 일어난 그날에 동시에 나머지 전부 만족"이 되어 교집합이 산술적으로 붕괴한다
2. **코드 기본값이 비활성**: `DEFAULT_PARAMS["require_macd_golden"] = 0.0` (`:111`)
3. **근거 기록 0건**: PLANS 전체에 왜 켰는지가 없다
4. **옵티마이저 증거가 반대**: worst_fold 0.4929(n=73) vs 0.4038(n=87), **p<0.00001** (PLANS (33))
5. **래더에서 완화되지 않음**: L0~L9 어느 레벨에서도 안 풀려 상한을 4.1%로 고정

**다섯 조건을 모두 만족하는 게이트는 이것 하나다.**

## 3. 정량 효과 (195거래일 실측)

| 시나리오 | 후보 | 현행 대비 |
|---|---:|---:|
| 현행 | 4 | 1x |
| **macd off** | **16** | **4x** |
| macd off + v3.24 + value 620억 | 109 | 27x |
| macd off + v3.24 + value 100억 | 395 | 99x |
| macd **ON** + v3.24 + value 100억 | 104 | 26x |

**최대 완화 상태에서도 macd 하나가 395 -> 104로 74%를 죽인다. 단일 최대 레버.**

## 4. [중대] 변경 경로가 막혀 있다 — 네 번째 겹

`require_macd_golden`은 `12_Risk_Controlled/stable_params_v41_1.json`에 있고
**환경변수 오버라이드가 없다.** 정상 변경 경로는 HPO 승격뿐인데 **그 승격이 막혀 있다**(1절).

그리고 `generate_candidates_v41_1.py:2070`:
```
if (stable_gate_status["ok"] or (research_mode and research_allow_unapproved)) and not cand.empty:
    chosen_level = level; break
```
**게이트 FAIL이면 후보가 선택되지 않는다.** 즉 `promoted=False`로 정직하게 표시하면
**생산이 멈춘다.**

이것이 PLANS (58)(59)(60)의 "세 겹 보호"에 이은 **네 번째 겹**이다:
정직한 표시가 시스템 정지를 부른다.

## 5. 선택지

| 안 | 내용 | 정직성 | 운영 |
|---|---|---|---|
| **A** | `require_macd_golden`만 0.0으로. `promoted=True` 유지 | **낮음** — 인증 표식이 더 거짓이 된다. (59)의 재채점 결함 덕에 통과한다 = **결함을 이용** | 무중단 |
| **B** | 값 변경 + `promoted=False` + 연구 모드 환경변수(`CANDIDATE_RESEARCH_MODE=1`, `CANDIDATE_RESEARCH_ALLOW_UNAPPROVED=1`)로 운영 | **높음** — 산출물에 `official_use_allowed=False`가 남는다 | **배치 수정 필요** |
| **C** | 유지 | - | 현상 유지, 검증 영구 불가 |

**권고: B.** 이유 —
- 현재 상태는 이미 "인증되지 않은 파라미터가 인증된 척하는" 것이다((58)~(60)).
  **A는 그 거짓을 한 겹 더한다.**
- B는 거짓을 없애고 "이것은 연구 모드다"를 **모든 산출물에 새긴다.**
  사용자 원칙(구현 신뢰가 데이터 판단에 선행, PLANS (48))과 정합한다 —
  연구 모드로 표시된 데이터는 **자격이 명확하다**
- 단 B는 운영 배치 수정을 요구하므로 범위가 커진다. **이 트레이드오프는 사용자 판단이다**

## 6. 적용 절차 (B 기준, 승인 후)

1. **백업**(14): `backup/20260820_macd_golden_disable/YYYYMMDD_HHMMSS/`
   - `12_Risk_Controlled/stable_params_v41_1.json` (현 sha256 `4ed8011346787d3c`)
   - 수정 대상 배치 파일
2. `stable_params_v41_1.json`: `require_macd_golden` **1.0 -> 0.0**, `promoted` **true -> false**
3. 배치에 `CANDIDATE_RESEARCH_MODE=1`, `CANDIDATE_RESEARCH_ALLOW_UNAPPROVED=1` 주입
4. **stable_params 직접 편집은 승격 경로 우회다.** 변경 이력을 PLANS에 명시하고
   `meta`에 수동 편집 표식을 남긴다

## 7. 검증 (AGENTS.md 6)

| 항목 | 방법 | 즉시 가능 |
|---|---|---|
| 기능 | `[PARAM] require_macd_golden` 반영, `[PARAM_GATE] ok=false` 출력 | O |
| 정합성 | 후보 수가 4배 이상 증가하는지 (3절 예측 대조) | **후보 발생일** |
| 운영 반영/E2E(11) | `orders -> fills -> ledger -> stats` | **후보 발생일** |
| 정책 | 나머지 9개 게이트 불변 확인 | O |
| FAIL-CLOSED | 연구 모드 우회가 **의도된 경로**인지, 산출물에 `official_use_allowed=false`가 실리는지 | O |
| 회귀 | 진입층 3게이트 동작 불변 | O |

## 8. 합격 기준 (손익 배제)

PLANS (38) Q2 4축을 그대로 쓴다. 추가로:
- **후보 발생일 비율**이 측정 가능한 수준(현행 무후보일 90.4%)으로 개선되는지
- **창당 후보 15건 상당**에 접근하는지 (3절 기준 `macd off` 단독은 부족할 수 있음 -> 재측정 후 2단계 판단)

## 9. 주장하지 않는 것

- **수익성 개선을 주장하지 않는다.** `macd_golden`을 끈 뒤의 성과는 판정하지 않았다 —
  자격 있는 표본이 0이기 때문((48))
- `macd off` 단독으로 HPO 요건(창당 15건)을 충족한다고 주장하지 않는다.
  3절에서 충족은 `+v3.24 +value 100억`까지 갔을 때다. **본 계획은 1단계만 다룬다**
- 후보 -> 거래 전환율 10%는 **가정**이며 실측이 아니다

## 10. 위험

- **stable_params 직접 편집은 전례를 만든다.** 승격 경로를 우회하는 것이 일상화되면
  이번 감사가 밝힌 문제(근거 없는 값이 눌러앉음)를 재생산한다.
  **본 변경은 "순환을 끊기 위한 1회성"으로 한정하고, 승격 경로 복구를 별도 과제로 남긴다**
- `promoted=false`가 다른 소비자에 미치는 영향 미조사 (`official_use_allowed` 외)
- 연구 모드 환경변수가 배치에서 누락되면 **후보 생성이 전면 중단**된다. 롤백 절차 필수

---

## 11. [개정 2026-08-20] D안 — certified/operational 분리 후의 정직한 적용

B안 철회 사유(게이트 무력화)는 `docs/exec-plans/active/20260820_certified_operational_split.md`가
해소했다. **이제 무력화 없이 정직하게 적용할 수 있다.**

### 왜 이제 가능한가

`stable_params_v41_1.json`은 **이미 `certified=false`로 표시돼 있다**
(PLANS 2026-08-20 (67)). 즉 **"이 아티팩트는 정식 승격 산물이 아니다"가 이미 선언돼 있으므로**,
파라미터를 수동 편집해도 **새로운 거짓이 생기지 않는다.** 기존 라벨이 그 사실을 이미 담고 있다.

이전에는 `promoted=true`(인증 참칭) 위에 수동 편집을 얹는 것이라
거짓을 한 겹 더하는 문제였다. 그 전제가 사라졌다.

### 변경

`12_Risk_Controlled/stable_params_v41_1.json`
```
require_macd_golden : 1.0 -> 0.0
cert_reason         : 수동 파라미터 편집 사실을 추가 기재
meta.manual_edit    : fields 에 require_macd_golden 추가
```
`certified=false` / `operational=true`는 **그대로 유지**한다.

### 하지 않는 것

- `promoted` / `certified` / `operational` 값 변경 없음
- 다른 9개 게이트 파라미터 변경 없음 (`value_min`, `v_accel_lim` 등)
- 게이트 코드·설정 변경 없음
- **승격 절차 우회를 정상화하지 않는다.** 이것은 순환을 끊기 위한 1회성이며,
  승격 경로 복구는 별도 과제로 남는다

### 예상 효과 (3절 실측)

후보 4건 -> **16건** (195거래일, 4x). **HPO 요건(창당 15건)에는 여전히 미달**이며,
추가로 `v_accel_lim` / `value_min` 완화가 필요하다(3절). **본 변경은 1단계다.**

### 검증

- 기능: `[PARAM] ...` 출력과 파일 값 확인
- 정합성: 두 게이트 판정 불변(`ok=True`) 확인
- 정책: 나머지 9개 파라미터 및 자격 3필드 불변 확인
- FAIL-CLOSED / 회귀: 게이트 경로 불변
- **운영 반영/E2E: 후보 발생일 대기.** 후보 수가 예측(4x) 방향으로 움직이는지 관측
