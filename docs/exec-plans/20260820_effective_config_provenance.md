# ExecPlan — 유효 설정 기록 (무엇이 실제로 쓰였는가)

- 작성: 2026-08-20
- 상태: **설계 초안. 사용자 승인 전이며 어떤 변경도 적용하지 않았다.**
- 분류: **배치 엔트리포인트 / FAIL-CLOSED 동작 변경** (AGENTS.md 5, 9, 11, 14)
- 근거 기록: `.agent/PLANS.md` 2026-08-20 (88)(92) 및 beta_harvest 조사
- 선행: `20260820_config_lock_enforcement.md` (적용 완료) — 그 구멍을 여기서 막는다

## 1. 원인

**`2_Logs/paper_engine_config.used_*.json`은 이름과 달리 "쓰인 설정"이 아니다. 파일 복사본이다.**

`tasks/task_00_config_lock.bat:28`:

```
copy /y "%CFG%" "%CFG_SNAP%"
```

그런데 엔진이 실제로 쓰는 값은 `load_config()`의 결과이고, 그것은 세 단계를 더 거친다.

| 단계 | 코드 | 파일에 없는 것을 만들 수 있나 |
|---|---|---|
| DEFAULT_CONFIG 백필 | `config.py:1075` | **예** |
| `PAPER_CFG_OVERRIDES_JSON` deep merge | `config.py:103` | **예 (임의 깊이)** |
| `PAPER_CFG_OVERRIDES` dotted-key | `config.py:83` | **예 (임의 깊이)** |

즉 **파일 해시는 유효 설정을 증명하지 않는다.** 어제 넣은 잠금 검사(`verify_config_lock()`)도 파일만 본다.

## 2. 이것이 실제로 사고를 냈다 (2026-07-28)

`paper/fills.csv`에 `beta_harvest` 체결 12건이 있다. 전부 20260728T15:20:00.

```
beta_harvest;market=KOSPI;regime=CRASH;target_exposure=0.1;
target_notional=555555.56;current_notional=0.0;signal_date=20260728
```

`beta_harvest_engine.py:306`의 출력 형식과 정확히 일치한다. 그런데 그날 이 엔진은 **꺼져 있어야 했다.**

| 증거 | 값 |
|---|---|
| 코드 게이트 | `beta_harvest_engine.py:336` `if not beta.get("enabled"): return 0` |
| 07-28 08:30 스냅샷 | `beta_harvest` 키 **부재** |
| 07-28 21:30 스냅샷 | `beta_harvest` 키 **부재** |
| `DEFAULT_CONFIG` 폴백 | `beta_harvest` **없음** -> `{}` -> falsy -> 차단 |
| 설정 변경 로그 | 07-28 전후 **0건** (전체 9건) |
| 키가 처음 나타난 시점 | **20260730_133121**, 그때 값이 `enabled: false` |

> 인용 조건: 스냅샷 209개 전수(`used_2026*.json`, 창 20260713~20260820).
> 배치 1회당 1개이므로 **장중 상태는 구조적으로 포착되지 않는다.**

**모든 기록이 "꺼짐"인데 실체결이 났다.** 남은 설명은 셋뿐이고 셋 다 흔적을 남기지 않는다:
`--config` 다른 파일 / `PAPER_CONFIG_PATH` 재지정 / `PAPER_CFG_OVERRIDES(_JSON)` 주입.

## 3. 같은 형태가 오늘만 셋

| 사례 | 형태 |
|---|---|
| `news_topic_execution_policy` | 설정 키 부재 -> 코드 기본값 True -> 아무도 켠 적 없이 후보 제거 |
| `shadow_collect`의 `PAPER_CFG_OVERRIDES` | 파일 안 건드리고 유효 설정 5개 변경 |
| `beta_harvest` 07-28 | 모든 설정 기록이 "꺼짐"인데 실체결 12건 |

공통 원인 하나: **"무엇이 실제로 쓰였는가"가 어디에도 기록되지 않는다.**

## 4. 근거의 출처 (임계값 결정 원칙)

- 제도적 사실 / 집행 제약 / 전략 전제: **해당 없음**
- 새 정책인가: **아니다.** `config.py:1073`의 계약("설정 변경은 lock 도구를 거칠 것")을 실효화하는 일이다

새 임계값을 정하는 것이 아니므로 알파 판단이 아니다.

## 5. 설계

### 5.1 `paper_engine/config.py` — 유효 설정 지문

`load_config()` 안에서 계산하고 모듈 전역에 보관한다. **계산은 순수 함수, 쓰기는 별도.**

```
_EFFECTIVE_FINGERPRINT: Dict[str, Any] = {}

fingerprint = {
  "effective_sha256":  <최종 cfg의 정규화 JSON sha256>,
  "file_sha256":       <CONFIG_PATH 바이트 sha256>,
  "config_path":       str(CONFIG_PATH),
  "defaults_backfilled": [<파일에 없어 DEFAULT_CONFIG에서 채운 top-level 키>],
  "override_json_keys":  [<PAPER_CFG_OVERRIDES_JSON의 top-level 키>],
  "override_kv_keys":    [<PAPER_CFG_OVERRIDES의 dotted 키 전체>],
  "override_label":      os.environ.get("PAPER_CFG_OVERRIDES_LABEL", ""),
  "argv0":               <실행 스크립트>,
  "pid": ..., "generated_at": ...,
}
```

정규화: `json.dumps(cfg, sort_keys=True, ensure_ascii=False, separators=(",",":"))`.

### 5.2 기록 — 해시 중복 제거로 상한을 건다

`2_Logs/paper_engine_config.effective_<YMD>_<eff_sha10>.json`

- **이미 있으면 쓰지 않는다.** 하루에 생기는 파일 수 = 그날의 서로 다른 유효 설정 가짓수
- 정상 운영이면 하루 1~2개. 그림자 레인이 켜지면 2~3개
- 전체 try/except. 실패해도 판정에 영향 없음

`load_config()` 안에서 호출하므로 **`beta_harvest_engine.py`를 포함한 모든 진입점이 자동으로 남긴다.**
이것이 07-28의 사각을 덮는 지점이다.

### 5.3 라벨 없는 덮어쓰기는 거부한다 (FAIL-CLOSED)

`verify_config_lock()`에 조건을 추가한다.

| 상황 | 판정 |
|---|---|
| 덮어쓰기 없음 + 파일 해시 일치 | `ok` |
| 덮어쓰기 있음 + `PAPER_CFG_OVERRIDES_LABEL` 있음 | `ok`, 라벨과 키 목록을 기록 |
| **덮어쓰기 있음 + 라벨 없음** | **`unlabeled_override` -> 차단** |
| 파일 해시 불일치 | `lock_mismatch` -> 차단 (현행 유지) |

**덮어쓰기를 금지하지 않는다. 이름을 요구할 뿐이다.**
07-28 사례는 이 규칙 하에서 차단되거나, 최소한 라벨과 함께 기록됐을 것이다.

기존 우회 스위치 `PAPER_CONFIG_LOCK_ENFORCE=0`은 그대로 둔다(경고 기록 후 진행).

### 5.4 `run_paper_daily.bat` — 그림자 레인에 라벨 부여

`shadow_collect` 블록에 한 줄 추가한다.

```
set "PAPER_CFG_OVERRIDES_LABEL=shadow_collect"
```

지금은 비활성이지만, **되켤 때 5.3에 걸려 배치가 죽는 것을 막기 위해 미리 넣는다.**
해제 목록에도 추가한다.

### 5.5 `used_*.json`의 이름 정정

파일 자체는 그대로 두되 `task_00_config_lock.bat`에 주석 3줄을 넣는다 —
**"이것은 파일 복사본이며 유효 설정이 아니다. 유효 설정은 `effective_*.json`이다."**
이름을 바꾸면 이것을 읽는 도구를 전수 확인해야 하므로 범위를 넓히지 않는다.

## 6. 적용 범위

| 파일 | 변경 |
|---|---|
| `paper_engine/config.py` | 지문 계산 + 기록 + 전역 보관 (신규 함수 2개) |
| `paper_engine/config.py` | `verify_config_lock()`에 `unlabeled_override` 판정 추가 |
| `run_paper_daily.bat` | 그림자 블록에 라벨 1줄 + 해제 1줄 |
| `tasks/task_00_config_lock.bat` | 주석 3줄 |

`paper_engine.py`는 **변경 없음** — 이미 `verify_config_lock()`을 부른다.

## 7. FAIL-CLOSED 영향 (AGENTS.md 5)

- **새로 막히는 상황은 하나뿐**: 라벨 없는 env 덮어쓰기
- 현재 운영에서 env 덮어쓰기를 쓰는 곳은 `shadow_collect` 하나이고 지금 비활성이며 5.4로 라벨을 받는다
- 즉 **정상 운영 경로에서 새로 막히는 것은 없다**
- 기존 `lock_mismatch` 의미는 변경하지 않는다

## 8. 검증 계획 (AGENTS.md 11)

1. **정상**: 덮어쓰기 없이 엔진 1회 -> `ok`, `effective_*.json` 1개 생성, `override_*_keys` 빈 배열
2. **중복 제거**: 같은 조건으로 1회 더 -> 파일 수 증가 **0**
3. **라벨 있는 덮어쓰기**: `PAPER_CFG_OVERRIDES=max_new_trades_per_day=6` +
   `PAPER_CFG_OVERRIDES_LABEL=test` -> `ok`, 새 `effective_*.json`에 키와 라벨 기록
4. **라벨 없는 덮어쓰기**: 라벨만 빼고 재실행 -> **exit 1**, 사유 `unlabeled_override`
5. **07-28 재현**: `PAPER_CFG_OVERRIDES=beta_harvest.enabled=true`를 라벨 없이 ->
   **차단되는지 확인.** 이 플랜의 존재 이유를 직접 시험한다
6. **회귀**: 장중 사이클 1회 완주 (`[paper_engine] OK`)
7. **파일 수 상한**: 하루치 `effective_*.json` 개수가 유효 설정 가짓수와 일치하는지

**5번은 `--dry-run`으로 한다. 실체결을 만들지 않는다.**

## 9. 롤백

- `backup/20260820_effective_config_provenance/<ts>/`에 3개 파일 원본 보존
- 되돌리기: 파일 복원. 생성된 `effective_*.json`은 읽기 전용 산출물이라 남아도 무해

## 10. 범위 밖 (별도 과제)

- **체결에 생산 엔진 기록** (`note` 문자열 매칭 대신 `engine` 컬럼) — 원장 스키마 변경이라 분리
- **`beta_harvest_engine.py`의 실행 경로 정리** — 스케줄러에도 배치에도 없는데 원장에 쓸 수 있는 상태
- `paper_sync.py`의 자체 `load_config()` — 별도 구현이라 이 지문을 남기지 않는다

## 11. 미적용 명시

**이 문서의 어떤 것도 아직 적용하지 않았다.** 승인 후 6절 순서대로 진행한다.
