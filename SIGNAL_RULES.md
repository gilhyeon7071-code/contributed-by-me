# SIGNAL_RULES.md

## 범위
- 기준 코드: `E:\1_Data\paper_engine\entry.py`  (2026-08-24 정정: 모듈 분리로 `paper_engine.py` 에서 옮겨졌다. `[CAND_D_FILTER]`/`pick_candidates` 는 entry.py 에만 있다)
- 기준 설정: `E:\1_Data\paper\paper_engine_config.json`
- 본 문서는 코드 구현 기준 동작을 그대로 기록한다.

## 날짜 기준(D)
- D 산출 규칙:
  - `fills.csv`에서 `side=BUY` 행의 timestamp 앞 8자리(YYYYMMDD) 중 최신값
  - BUY가 없으면 `datetime/ts/date` 앞 8자리 최신값
- 후보 진입 신호는 `signal_date == D`만 사용한다.
- 구현 위치:
  - `_derive_d_from_fills_path(...)`
  - `pick_candidates(...)`의 `[CAND_D_FILTER]`

## 진입(Entry) 조건
1. 후보/점수 준비
- 후보 입력: `candidates_latest_data.csv` (+ sidecar 우선)
- 랭크 키: `final_score` 우선, 없으면 `score`

2. 사전 게이트
- ENTRY_GATE 결과(`ALLOW/CAUTION/REDUCE/BLOCK`)에 따라 `max_new` 조정
- `BLOCK`이면 신규 진입 0

3. 진입 스킵 우선순위(대표)
- `MAX_NEW_REACHED`
- `MAX_POSITIONS_BLOCK`
- `MARKET_GATE_FAILED`(carryover)
- `SECTOR_CONCENTRATION_BLOCK`
- `FALLBACK_SIGNAL_EXPIRED`
- `PROCESSED_DUPLICATE` / `SIGNAL_ALREADY_COMMITTED`
- `CODE_ALREADY_OPEN`
- `NO_NEXT_TRADING_DAY`
- `FALLBACK_EXHAUSTED`
- `IDEMPOTENT_BUY`

4. 진입 실행
- 위 조건 미해당 시 BUY 실행
- 결과 reason: `BUY_EXECUTED`

## 청산(Exit) 조건 및 우선순위
우선순위는 `paper_engine/entry.py` 의 if/elif 순서를 따른다.

1. STOP/TRAIL
- trail 우선 적용 조건일 때 TRAIL 먼저 검사
- 아니면 STOP 먼저 검사

2. STOP_PREEMPTIVE_CLOSE

3. Fundamental Risk
- `_check_fundamental_risk(...)` 결과 우선

4. SURGE_TP

5. Staged Take-Profit
- `TP_Lxx` 규칙

6. Technical Exit

7. Market Risk Exit

8. Candidate Dropout

9. TIME Exit
- `max_hold_days` 또는 horizon 기반 보유일 만료

## signal / reason / rank 산출물
- `pending_entry_signals_latest.csv`:
  - carryover queue 파일
  - 컬럼 스키마 고정(빈 경우도 파일 유지)
- `entry_signal_snapshot_latest.csv`:
  - 진입 평가/실행 결과 스냅샷
  - 주요 컬럼: `code, signal_date, signal, reason, rank_score`
