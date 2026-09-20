# ExecPlan — 하드 블록 시 청산·포지션 관리는 계속 실행

- 작성: 2026-08-24
- **개정: 2026-08-24 (v2). 새 스위치를 만들려던 설계를 폐기하고, 이미 있는 `PAPER_EXIT_ONLY` 를 쓴다.**
- 상태: **초안. 사용자 승인 전이며 어떤 변경도 적용하지 않았다.**
- 분류: **매매 동작 변경** (AGENTS.md 5). ExecPlan / E2E / 백업 필요
- 근거 기록: `.agent/PLANS.md` 2026-08-24 (80)C (81)

## 1. 문제 — 블록이 사는 것과 파는 것을 함께 막는다

`intraday_paper_loop.py:2828-2837`
```python
while True:
    flag_path = LOG_DIR / "paper_intraday_hard_blocked.flag"
    if flag_path.exists():
        ... 자동 해제 시도 ...
        else:
            logger.error("...Sleeping to prevent crash loop.")
            time.sleep(60)
            continue          # <- 사이클 전체를 건너뛴다
```
`continue` 가 사이클 본문 전체를 건너뛰므로 **가격 스냅샷도, `paper_engine` 실행도 일어나지 않는다.**
`paper_engine` 안에 진입과 청산이 같이 있으므로 **손절·트레일·시간청산·부분익절이 전부 정지**한다.

### 실제 발생 (2026-08-24)
```
09:47  하드 블록 (사유: surge_freshness_gate, cand 20260820 vs expected 20260821)
       이 시점 open_positions = 005690 1주 (entry 10,970, stop -5% = 10,421, atr14_pct 8.3%)
12:32  2시간 45분간 청산 관리 정지. 손절선 미접촉으로 손실은 없었다
       (오늘 저가 10,800 > 손절선 10,421). **보호가 없었을 뿐이다**
```

## 2. 왜 고쳐야 하는가

```
신호 게이트의 근거   "데이터가 낡았으니 **새 판단**을 하지 마라"
  진입 차단  타당하다
  청산 차단  근거가 없다. 보유 포지션의 손절선은 **진입 시점에 확정된 값**이고
             후보 신선도와 무관하다. 데이터가 낡을수록 관리는 더 필요하다
```
> **"살 수 없다"는 안전하지만 "팔 수 없다"는 위험하다.**

## 3. [개정] 진입 억제 스위치는 이미 있다 — 만들지 않는다

**v1 초안은 `PAPER_ENGINE_NO_NEW_ENTRY` 를 새로 만들어 `max_new=0` 을 강제하려 했다.**
그 설계의 최대 위험은 "`max_new` 변형 지점이 많아 하류에서 되살아날 수 있다"였다.

**확인 결과 그 기능이 이미 정식으로 구현돼 있다.** `paper_engine.py`
```
:440   exit_only_mode = os.getenv("PAPER_EXIT_ONLY") or os.getenv("PAPER_NO_ENTRY")
:448   if exit_only_mode: max_new = 0; max_new_surge = 0             초기 설정
:1255  if exit_only_mode: ...                                        중간 재확인
:1282  (not exit_only_mode) 를 다른 조건에 결합
:1388  if exit_only_mode: _capture_max_new_zero("exit_only_mode_final")   최종 재확인
:1684  exit_only_mode=bool(exit_only_mode) 를 entry.py 로 전달  ->  entry.py:8083
```
**세 겹으로 걸려 있어 하류에서 되살아나지 않는다.** v1 이 걱정한 위험이 이미 해소돼 있다.

→ **새 스위치를 만들지 않는다. 축소 사이클에서 `PAPER_EXIT_ONLY=1` 로 엔진을 실행한다.**

## 4. 변경 — 축소 사이클 (loop 한 곳)

블록 상태에서 사이클 전체를 건너뛰는 대신, **보유 포지션이 있을 때만** 최소 단계를 실행한다.

```
블록 상태 진입
  ├ 블록 사유가 가격 계열이면      -> 현행과 동일 (가격을 못 믿으므로 청산 판단도 하지 않는다)
  ├ open_positions 가 비어 있으면  -> 현행과 동일 (지킬 것이 없다)
  └ 그 외                         -> 축소 사이클
        1) intraday_price_snapshot   청산 판단에 필요한 현재가
        2) paper_engine  with PAPER_EXIT_ONLY=1
        3) 나머지 관측·연구·뉴스·LOB 스텝은 전부 건너뛴다
```

읽는 곳: `paper/paper_state.json` 의 `open_positions`
**주의: `paper/positions.csv` 가 아니다.** 2026-08-24 에 그것을 보고 "포지션 0" 이라 오판했다.

## 5. 하지 않는 것

- 하드 블록 자체를 없애지 않는다. 진입 차단은 유지한다
- 자동 해제 한도·냉각 시간을 바꾸지 않는다
- 신선도 게이트의 판정 기준을 바꾸지 않는다 (게이트는 옳게 작동했다)
- 포지션이 없을 때의 동작을 바꾸지 않는다
- **`max_new` 관련 코드를 건드리지 않는다** (v1 대비 최대 변경점)

## 6. 검증

| 항목 | 방법 |
|---|---|
| 구문 | `py_compile` |
| 기능 A | 포지션 0 + 플래그 존재 -> 현행과 동일하게 sleep (동작 불변) |
| 기능 B | 포지션 1 + 플래그 존재 -> 스냅샷 + 엔진 실행되고 `[ENTRY_EXIT_ONLY]` 가 찍히는지 |
| 기능 C | 블록 사유가 가격 계열이면 축소 사이클을 돌지 않는지 |
| 정합성 | `PAPER_EXIT_ONLY` 경로가 기존 구현 그대로인지 (엔진 코드 미변경 확인) |
| FAIL-CLOSED | `paper_state.json` 읽기 실패 시 **현행 동작(전체 스킵)으로 폴백.** 열지 않는다 |
| 회귀 | 플래그 없는 정상 사이클 불변 |

## 7. 위험

- 축소 사이클이 60초마다 `paper_engine` 을 호출한다. 부하와 로그량을 확인해야 한다
- `PAPER_EXIT_ONLY` 는 진입을 막을 뿐 **청산 로직 자체의 결함은 막지 못한다**
- 블록 사유 분기를 라벨 문자열로 판단한다. 라벨이 바뀌면 분기가 어긋난다
  → 가격 계열 라벨을 상수로 두고 주석에 근거를 남긴다

## 8. v1 대비 달라진 점

```
v1   PAPER_ENGINE_NO_NEW_ENTRY 신설 + max_new=0 강제    엔진 + 루프 2곳 변경, 샐 위험 있음
v2   이미 있는 PAPER_EXIT_ONLY 사용                     **루프 1곳만 변경, 엔진 무변경**
```

**이 개정 자체가 오늘 네 번 나온 형태의 반복이다** — 장치는 이미 있었고 우리가 몰랐다.
새로 만들기 전에 있는지부터 확인한다.
