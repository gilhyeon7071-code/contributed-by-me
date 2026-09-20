# ExecPlan — 설정 잠금 계약 강제 (LOCK-B가 장중에 돌지 않는다)

- 작성: 2026-08-20
- 상태: **적용됨.** (상태줄 정정 2026-08-24 — 아래 근거)
- 상태줄 정정 근거: `paper/paper_engine_config.lock.json` 이 존재하고
  `tasks/task_00_config_lock.bat` 이 `run_paper_daily.bat` [0/14] 단계에서 이를 강제한다.
  실제로 이 잠금이 **2026-08-23·08-24 배치를 이틀 연속 rc=1 로 중단시켰다**
  (`.agent/PLANS.md` 2026-08-24 (64)). 즉 강제까지 살아 있다.
  "승인 전이며 어떤 변경도 적용하지 않았다"는 서술은 **사실과 달랐다**.
- 분류: **배치 엔트리포인트 / FAIL-CLOSED 동작 변경** (AGENTS.md 5, 9, 11, 14)
- 근거 기록: `.agent/PLANS.md` 2026-08-20 (88) 부수 소득 2
- 관련: `20260820_certified_operational_split.md` (같은 형태의 결함 — 감지 장치가 차단에 연결되지 않음)

## 1. 원인

**계약은 이미 존재하는데, 실행 대부분이 그 계약을 거치지 않는다.**

`paper_engine/config.py:1073`:

```
# Config file changes must go through tools/paper_engine_config_lock.py.
```

이 계약을 강제하는 유일한 장치가 LOCK-B다. `tasks/task_00_config_lock.bat`이
config의 sha256과 `paper/paper_engine_config.lock.json`의 `approved_sha256`을 대조하고
불일치 시 `exit /b 1` 한다.

**그런데 이 검사는 `run_paper_daily.bat` [0/14]에서만 돈다.**

## 2. 사실 (3종 증거)

### 2.1 코드

| 지점 | 내용 |
|---|---|
| `run_paper_daily.bat:164` | `call "%ROOT%tasks\task_00_config_lock.bat"` — 유일한 호출처 |
| `tasks/task_00_config_lock.bat:52` | sha 불일치 시 `exit /b 1` (FAIL-CLOSED) |
| `intraday_paper_loop.py:1843` | `engine_cmd = [py, str(ROOT / "paper_engine.py")]` — 잠금 검사 없음 |
| `paper_engine.py:201` | `cfg = load_config()` — 검사 없이 파일을 읽는다 |
| `paper_engine/config.py:1048` | `load_config()`에 잠금 검증 코드 없음 |

`task_00_config_lock`을 호출하는 파일은 `run_paper_daily.bat` **하나**다
(검색 범위: 루트 `*.bat`, `tasks/*.bat`, `scripts/*.bat`).

### 2.2 런타임 설정값

`paper/paper_engine_config.lock.json` 현재 상태:

```
ts               20260820_125855
approved_sha256  40fc14bd18baf65d9f96fd99e5643c89a2d9f39357d9ff8c48161c1516b26d4e
last_change_log  2_Logs\paper_engine_config.change_20260820_125855.json
```

현재 config sha256과 일치한다. 즉 **오늘 변경 4건은 계약을 지켰다.**
문제는 지켰는지 여부가 아니라 **지키지 않아도 대부분의 실행이 돌아간다**는 것이다.

### 2.3 실행 흔적

| 경로 | 오늘 실행 수 | 잠금 검사 |
|---|---|---|
| `run_paper_daily.bat` (배치) | 4 | **받음** |
| `intraday_paper_loop.py` -> `paper_engine.py` | **25** | **안 받음** |

> 인용 조건 — 장중 25회: 출처 `2_Logs/run_intraday_paper_last.txt`,
> 창 2026-08-20 09:04~13:55, 집계 단위는 `[paper_engine]` 단계 실행 1회.
> 배치 4회: 출처 `2_Logs/paper_engine_config.used_20260820_*.json` 파일 수
> (스냅샷은 배치 1회당 정확히 1개 — (88)).

**29회 중 25회(86%)가 미검사 상태로 매매 판정을 냈다.**

## 3. 두 번째 구멍 — `init`이 감사 흔적 없이 재승인한다

`tools/paper_engine_config_lock.py:98` `cmd_init`:

```
cur_sha = sha256_file(CFG_PATH)
lock = {..., "approved_sha256": cur_sha, "last_change_log": None, ...}
atomic_write_json(LOCK_PATH, lock)
```

- 기존 lock이 있어도 **묻지 않고 덮어쓴다**
- **change 로그를 쓰지 않는다** (`cmd_set`은 before/after config 전문을 남긴다)
- 즉 **config를 손으로 고친 뒤 `init`을 돌리면 무엇이 바뀌었는지 기록이 남지 않는다**

이것이 `split_entry` 5개 키가 변경 로그 없이 바뀐 이력의 설명 가능한 경로다
(경로가 그것뿐이라고 단정하지는 않는다 — 미확인).

## 4. 왜 지금 고치는가 — 근거의 출처

임계값 결정 원칙(`20260820_entry_gate_premise_alignment.md` 11절) 적용:

- **제도적 사실**: 해당 없음
- **집행 제약**: 해당 없음
- **전략 전제**: 해당 없음
- **새 정책인가**: **아니다.** 계약은 `config.py:1073`에 이미 있다

따라서 이것은 **새 임계값을 정하는 일이 아니라, 기존 계약을 실효화하는 일**이다.
알파 판단이 아니므로 "근거 없음 -> 제거" 대상이 아니라 "근거 있음 -> 강제" 대상이다.

## 5. 선택지

### A. `load_config()`에 검증을 넣는다 (단일 초크포인트)

- 장점: config를 읽는 **모든** 경로가 자동으로 검사받는다. 누락 불가
- 단점: `load_config()`를 쓰는 도구/감사 스크립트까지 전부 막힌다.
  잠금이 깨진 상태에서 **진단조차 못 하게 된다** — 자기 발등 찍기
- FAIL-CLOSED 영향: 매우 큼

### B. `paper_engine.py:main()` 진입부에서 검증한다

- 장점: **매매 판정을 내리는 경로만** 정확히 덮는다. 25회가 전부 커버된다
- 장점: 도구/감사 스크립트는 계속 돈다
- 단점: `beta_harvest_engine.py` 등 다른 엔진은 별도 처리 필요
- FAIL-CLOSED 영향: 엔진이 안 뜬다. 배치의 기존 동작(`exit 1`)과 **동일한 의미**

### C. `intraday_paper_loop.py`가 사이클마다 검사한다

- 장점: 배치와 같은 위치(오케스트레이터)에 두어 대칭이 맞는다
- 단점: 엔진을 직접 호출하는 다른 경로는 여전히 무방비.
  **지금 고치려는 결함과 같은 형태를 하나 더 만드는 것**
- FAIL-CLOSED 영향: 중간

### D. 기록만 한다 (advisory)

- 단점: **이것이 지금 상태다.** provenance advisory와 같은 실패 형태 —
  감지 장치가 차단에 연결되지 않음. (58)에서 이미 문제로 지목했다

## 6. 권고 — **B**

이유:
1. 덮어야 할 대상이 정확히 "매매 판정을 내리는 실행"이고 B가 그것과 일치한다
2. A는 진단 경로까지 막아, 잠금이 깨졌을 때 원인 규명을 불가능하게 만든다.
   **FAIL-CLOSED는 매매를 막는 것이지 관측을 막는 것이 아니다**
3. C는 오케스트레이터 의존을 하나 더 만든다 — 오늘 고치려는 결함의 재생산
4. D는 현상 유지

**B에 더해 `cmd_init`을 수리한다** (3절 구멍):
- 기존 lock이 있으면 `--force` 없이는 거부
- `--force` 사용 시 change 로그를 강제로 남긴다 (before=이전 approved_sha256, after=현재)
- `--reason` 필수

## 7. 적용 범위 (B안 상세)

| 파일 | 변경 |
|---|---|
| `paper_engine/config.py` | `verify_config_lock() -> (ok, reason, cur_sha, approved_sha)` 신설. **읽기 전용, 예외 없음** |
| `paper_engine.py` | `main()` 진입부, `load_config()` **직후** 호출. 불일치 시 사유 출력 후 `return 1` |
| `tools/paper_engine_config_lock.py` | `cmd_init`에 `--force` / `--reason` 강제, change 로그 기록 |

**우회 스위치**: 환경변수 `PAPER_CONFIG_LOCK_ENFORCE=0`으로 끌 수 있게 한다.
단 끈 경우 **표준출력과 `pending_entry_status`에 그 사실을 남긴다.**
(끄는 수단이 없으면 잠금 파일 손상 시 복구가 불가능해진다.
끄는 수단이 흔적 없이 있으면 계약이 다시 무의미해진다. 둘 다 피한다.)

## 8. FAIL-CLOSED 영향 (AGENTS.md 5)

- **의미 변경 없음.** 배치는 이미 불일치 시 `exit 1` 한다. 장중에 같은 규칙을 적용할 뿐이다
- 새로 막히는 상황: config가 승인 해시와 다른 채로 장중 엔진이 도는 경우.
  **이 상황은 지금 매일 발생 가능하며 아무도 모른다**
- 잠금 파일 부재 시: **차단**한다 (배치와 동일 — `task_00_config_lock.bat:12`)

## 9. 검증 계획 (AGENTS.md 11)

1. **정상 경로**: 현재 config(승인 해시 일치)로 엔진 1회 실행 -> 통과, 기존과 동일 동작
2. **차단 경로**: config 사본을 1바이트 바꿔 `PAPER_CONFIG_PATH`로 주입 -> `return 1`,
   사유 출력 확인. **생산 config는 건드리지 않는다**
3. **우회 스위치**: `PAPER_CONFIG_LOCK_ENFORCE=0`으로 2번 재실행 -> 통과하되 경고 기록 확인
4. **`init` 수리**: 기존 lock 존재 상태에서 `init` -> 거부. `--force --reason`으로 -> change 로그 생성 확인
5. **회귀**: 장중 사이클 1회 완주 확인 (`[paper_engine] OK`)
6. **E2E**: 후보 발생일에 orders -> fills -> ledger 연결 확인

## 10. 롤백

- `backup/20260820_config_lock_enforcement/<ts>/`에 3개 파일 원본 보존
- 되돌리기: 파일 3개 복원. 설정/데이터 변경이 없으므로 상태 되감기 불필요

## 11. 미적용 명시

**이 문서의 어떤 것도 아직 적용하지 않았다.** 승인 후 7절 순서대로 진행한다.

## 12. 이 결함의 형태 (종합 문서 연결)

오늘 발견한 다섯 겹 중 다섯 번째 — **감지 장치가 차단에 연결되지 않음**의 두 번째 사례다.

| 사례 | 감지 | 차단 |
|---|---|---|
| provenance | 계산함 | **안 함** (`stable_params_gate.py:222`) |
| 설정 잠금 | 계산함 | **배치에서만** |

공통 형태: 값을 재고, 기록하고, **그 결과로 아무것도 하지 않는다.**
