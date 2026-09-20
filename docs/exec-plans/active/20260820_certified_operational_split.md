# ExecPlan — 인증(certified)과 가동(operational) 분리

- 작성: 2026-08-20
- 상태: **적용됨 (2026-08-20).** (상태줄 정정 2026-08-24 — 아래 근거)
- 상태줄 정정 근거: `12_Risk_Controlled/stable_params_v41_1.json` 에 certified/operational 키가 존재하고,
  `.agent/PLANS.md` 2026-08-20 (66)(67) 이 **[운영 변경] certified / operational 분리 구현**으로 기록돼 있다.
  파라미터 값은 불변인 채 sha256 이 `4ed8011346787d3c` → `c5822fc7bae2f0b0` 로 바뀌었고
  게이트 차단 기준이 promoted → operational 로 이동했다(구현 2곳).
  "승인 전이며 어떤 변경도 적용하지 않았다"는 서술은 **사실과 달랐다**.
- 분류: **SSOT / 게이트 의미 변경** (AGENTS.md 5, 9, 11, 14)
- 근거 기록: `.agent/PLANS.md` 2026-08-20 (58)(59)(60)(61)(63)(64)
- **이것은 원인 해결이다.** 앞선 `20260820_require_macd_golden_disable.md`의 B안(게이트 무력화)은
  증상 처리였으므로 철회하고, 그 원인을 여기서 다룬다.

## 1. 원인

**`promoted` 하나가 두 가지를 동시에 의미한다.**

| 의미 | 질문 |
|---|---|
| 인증 | 이 파라미터가 **정식 절차로 검증됐는가** |
| 가동 | 이 파라미터로 **매매해도 되는가** |

둘은 다른 질문인데 한 값이다. 그래서 **"검증 안 됨 + 그래도 가동"이라는 실제 상태를
표현할 수단이 없다.** 선택지가 셋뿐이다:

1. 거짓말한다 (`promoted=true` 유지 — 현재 상태)
2. 멈춘다 (`promoted=false` -> 게이트 FAIL -> 후보 0)
3. 판정 장치를 부순다 (연구 모드 상시 세팅 = **무력화**)

**셋 다 나쁘다. 원인은 표현 수단의 부재다.**

## 2. 현재 상태가 그 부재의 산물이다

- `stable_params_v41_1.json`(as_of 2026-08-14)은 **재현되지 않는다.**
  같은 파라미터를 다시 재면 `avg_pf 0.7993`, `worst 0.4714`, 게이트 재채점 FAIL (PLANS 2026-08-15 (3))
- 그런데 `promoted=true`이고 게이트는 **저장된 windows를 재채점만 하므로** 통과시킨다 (PLANS (59))
- 즉 **인증되지 않은 파라미터가 인증된 척하며 가동 중이다.**
  이 상태를 정직하게 적을 칸이 없어서 그렇게 됐다

## 3. 전수 조사 결과

### 3.1 `promoted` 소비 지점 (6곳)

| 위치 | 역할 |
|---|---|
| `optimize_params_v41_1.py:1874` | **쓰기** — 승격 결정 |
| `optimize_params_v41_1.py:327 _persist_promoted_stable()` | **쓰기 차단** — 승격 아니면 파일 미기록 |
| `utils/stable_params_gate.py:235` | **차단** — `require_promoted and not promoted` -> `not_promoted` |
| `paper_engine/state.py:2588` | **차단** — 동일 로직 |
| `live_vs_bt_paper_daily.py:893` | 보고 (`stable_promoted`) |
| `report_backtest_v41_1.py:440` | 보고 (`stable_promoted`) |

### 3.2 [신규 발견] 게이트 구현이 둘이고 설정 키가 다르다

| 구현 | 읽는 설정 키 | 기본 `min_oos_pf` |
|---|---|---|
| `utils/stable_params_gate.py:147` | **`stable_quality_gate`** | 0.75 |
| `paper_engine/state.py:2578` | **`stable_params_quality_gate`** | 0.75 |

**같은 판정을 두 코드가 서로 다른 설정 키로 수행한다.**
한쪽 설정만 바꾸면 두 게이트가 다른 문턱으로 동작할 수 있다. **본 계획에서 함께 다룬다.**

## 4. 설계

### 4.1 필드

| 필드 | 의미 | 누가 정하나 |
|---|---|---|
| **`certified`** | 정식 승격 절차를 통과했는가 | **기계** (`optimize_params`가 승격 시 true) |
| **`operational`** | 이 파라미터로 가동해도 되는가 | **사람** (명시적 승인) |
| `promoted` | 하위호환 별칭 | `certified`와 동일값 유지 |

### 4.2 게이트 판정 변경

```
현재:  가동 허용 = gate_ok  (gate_ok는 promoted를 포함)
변경:  가동 허용 = operational
       certified 는 차단 조건이 아니라 라벨
```

- `certified=false`여도 `operational=true`면 **가동한다. 대신 모든 산출물에 라벨이 실린다**
- `operational=false`면 가동하지 않는다 (현재의 `promoted=false`와 동일 효과)
- **연구 모드 환경변수를 상시 켤 이유가 사라진다**

### 4.3 산출물 라벨

`generate_candidates_v41_1.py:2114`의 `official_use_allowed`를 다음으로 대체·확장:
```
"param_certified":   bool   # 검증 여부
"param_operational": bool   # 가동 승인 여부
"param_cert_reason": str    # certified=false 인 이유
```
대시보드(RootB)와 보고서가 이 값을 표시하도록 한다. **RootB 작업은 별도 PLANS에 기록한다.**

### 4.4 하위호환

기존 stable에 신규 필드가 없을 때:
```
certified   = stable.get("certified",   stable.get("promoted", False))
operational = stable.get("operational", stable.get("promoted", False))
```
-> **기존 파일은 동작이 바뀌지 않는다.** 신규 필드를 넣은 파일만 새 의미로 동작한다.

## 5. 이 변경이 지금 상태에 적용되면

`stable_params_v41_1.json`:
```
certified   = false     # 재현 불가, 게이트 재채점 FAIL (PLANS 2026-08-15 (3))
operational = true      # 사용자가 명시적으로 가동을 승인
cert_reason = "unreproducible: sector-union KeyError world (as_of 2026-08-14)"
```
**현재 상태가 거짓 없이 그대로 표현된다.** 그리고 산출물마다 `param_certified=false`가 실리므로,
그 데이터로 내린 판단의 **자격이 명확해진다** — PLANS (48)의 사용자 원칙과 직접 연결된다.

## 6. 적용 절차 (승인 후)

1. **백업**(14): `backup/20260820_certified_operational_split/YYYYMMDD_HHMMSS/`
   - `utils/stable_params_gate.py`, `paper_engine/state.py`,
     `optimize_params_v41_1.py`, `generate_candidates_v41_1.py`,
     `12_Risk_Controlled/stable_params_v41_1.json`
2. 4.4 하위호환 헬퍼를 **두 게이트 구현 모두**에 추가
3. 차단 조건을 `operational` 기준으로 변경 (두 곳)
4. `optimize_params`가 승격 시 `certified=true, operational=true` 기록
5. 산출물 라벨(4.3) 추가
6. `stable_params_v41_1.json`에 5절 값을 **사용자 승인하에** 기록
7. 설정 키 이중화(3.2) 통합 여부 판단 — **본 계획 범위에 포함하되 별도 커밋**

## 7. 검증 (AGENTS.md 6)

| 항목 | 방법 | 즉시 가능 |
|---|---|---|
| 기능 | 두 게이트가 `operational` 기준으로 판정하는지, 하위호환 경로가 기존 동작을 보존하는지 | O |
| 정합성 | 두 게이트 구현이 같은 결론을 내는지 (설정 키 이중화 확인 포함) | O |
| 운영 반영/E2E(11) | `orders -> fills -> ledger -> stats`, 산출물에 라벨이 실리는지 | **후보 발생일** |
| 정책 | `certified=false`가 **차단하지 않는지**, `operational=false`가 **차단하는지** 양방향 확인 | O |
| FAIL-CLOSED | 신규 필드 결측 시 기존 `promoted` 의미로 폴백하는지. **결측이 가동 허용으로 둔갑하지 않는지** | O |
| 회귀 | 기존 stable 파일로 판정 결과 불변 확인 | O |

## 8. 이 설계가 해결하는 것 / 하지 않는 것

**해결한다**
- 표현 수단의 부재 -> 거짓말/정지/무력화 삼자택일이 사라진다
- 게이트 무력화(연구 모드 상시)의 필요가 사라진다
- 산출물 소비자가 파라미터 자격을 **기계적으로** 알 수 있다

**해결하지 않는다**
- **`stable_params`가 재현되지 않는다는 사실 자체.** 라벨을 붙일 뿐 고치지 않는다
- 승격이 막혀 있다는 것 (PLANS (61) 순환). **표본 부족은 별도 과제**
- 게이트가 저장 windows를 재채점만 한다는 것 (PLANS (59)). **재계산 도입은 별건**
- 세 겹 보호 중 1·2·3번. 본 계획은 **4번(정직한 표시가 생산을 멈춤)만** 제거한다

## 9. 위험

- **`operational=true`는 사람의 명시적 책임이 된다.** 지금은 게이트 뒤에 숨어 있는데 드러난다.
  이것이 이 설계의 핵심 가치이자 부담이다
- 게이트 구현이 둘이므로 **한쪽만 고치면 판정이 갈린다.** 반드시 동시 적용
- `official_use_allowed` 소비자(RootB 대시보드 등) 미조사. 라벨 변경이 표시를 깨뜨릴 수 있다
- 하위호환 폴백을 잘못 만들면 **결측이 가동 허용으로 둔갑**한다. 7절 FAIL-CLOSED 항목의 핵심
