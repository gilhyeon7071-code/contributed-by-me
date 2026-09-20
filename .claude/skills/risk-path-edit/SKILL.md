---
name: risk-path-edit
description: >
  E:\1_Data 에서 진입·청산·위험 경로를 건드리기 전 체크리스트. paper_engine/,
  p0_daily_check.py, drawdown/kill_switch/DDM, 게이트, 발주 경로, PAPER_EXIT_ONLY
  같은 스위치를 고치거나 켜고 끌 때 쓴다. 실제 돈이 움직이는 경로다.
---

# 위험 경로 편집 전

이 경로의 변경은 **매매 행동을 바꾼다.** 되돌리기 어렵고, 틀리면 조용히 틀린다.

## 1. 지금 무엇이 돌고 있나  <- 먼저 본다

```bash
Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='cmd.exe'" |
  Where-Object {$_.CommandLine -match 'paper_daily|intraday_paper|paper_engine'}
```

```
배치가 도는 중이면 고치지 않는다
  이유: 뒤에 오는 스텝이 새 파이썬 프로세스를 띄운다 -> 실행 중인 검증의
        대상이 검증 도중에 바뀐다
장중이면 영향을 먼저 계산한다
  진입 한도·청산 규칙 변경은 그 순간부터 발주를 바꾼다
```

## 2. 실효 설정값을 읽는다. 파일이 아니다

```python
from paper_engine.config import load_config
cfg = load_config()   # DEFAULT_CONFIG 병합 결과
```
스위치는 런처의 env 도 본다 (`run_intraday_paper.bat`, `run_paper_daily.bat`).
**이미 뜬 프로세스는 나중 수정을 모른다** — 재기동이 필요한지 판단한다.

## 3. 입력이 공유되는지 확인한다

하나를 연결하면 딸려오는 것이 있다.
```
예: paper_engine/entry.py:6357  open_pos = paper_state.open_positions
    손절·트레일·TP·보유만기·DDM 이 **전부 이 하나**를 입력으로 쓴다
    -> "DDM 만 연결" 은 불가능하다
```
건드릴 값의 소비처를 전수로 찾고, 의도하지 않은 소비처가 있으면 설계를 바꾼다.

## 4. 변경이 안전 방향을 뒤집는가

```
진입을 넓히나 좁히나
청산을 늦추나 앞당기나
차단을 완화하나 강화하나
```
완화 방향이면 **그렇다고 명시적으로 적는다.** 근거도 같이 적는다.
틀린 비관치로 얻는 안전은 안전이 아니지만, 방향을 바꾼 사실은 기록돼야 한다.

## 5. 오늘 이 변경으로 무엇이 팔리고 무엇이 사지나 — 숫자로 낸다

추상적으로 "영향 없음" 이라 하지 않는다. 현재 보유·현재 가격으로 계산한다.
```
2026-09-11 실측 예: topn 보유를 paper 청산 규칙에 붙이면
  012210 +25.92% -> TP10%30% + TP20%40% = 즉시 70% (1,136주) 매도
  나머지 5종목 -> 발동 없음
이 숫자가 없었으면 "DDM 만 붙는다" 고 오판했을 것이다
```

## 6. 자동 매도·자동 발주를 무장하는가

```
무장은 사용자 승인 사항이다. 관측 -> 그림자 -> 무장 순서로 간다
관측 단계에서 며칠치 값을 쌓고, 그 값이 맞는 걸 본 뒤에 무장한다
```

## 7. 검증 지점을 **먼저** 정한다

고치기 전에 "무엇이 어떻게 나오면 성공인가" 를 적는다.
```
파일 / 키 / 기대값 / 언제 나오나
예: p1_entry_gate_status_latest.json 의 exit_only_mode=false, 09:00 이후
```
그리고 **트리거 요일·시각을 실측으로 대조한다** (DaysOfWeek / NextRunTime).
"내일 확인" 이 휴일·주말에 걸리면 검증은 영원히 안 온다.
