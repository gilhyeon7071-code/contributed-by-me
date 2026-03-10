# Design: signal-integration (SSOT Synced)

작성일: 2026-03-04  
최종수정: 2026-03-05  
상태: Active (Implemented)  
참조:
- `E:\1_Data\docs\02-design\system-architecture.md`
- `E:\1_Data\docs\02-design\pipeline-contracts.md`
- `E:\1_Data\docs\01-plan\features\signal-integration.plan.md`

---

## 1. 문서 목적
이 문서는 Signal Integration의 설계 기준을 **현재 코드/배치 구현 상태**와 일치시키기 위한 SSOT 설계 문서다.

핵심 목표:
1. Phase 1/2/3 신호의 입력/출력 계약을 명확히 정의
2. fail-soft/fail-closed 경계를 명확히 정의
3. `run_paper_daily.bat` 실제 실행 순서와 설계 일치

---

## 2. 범위와 비범위
### 범위
- Macro gate (Phase 1)
- Sector score (Phase 2)
- News collect + score (Phase 3)
- Final score merge (Phase 2+3)
- Paper engine 점수 사용 우선순위
- Trades join 통합 산출물 상태 모델

### 비범위
- 전략 가중치 최적화 로직 설계
- 뉴스 NLP 모델 고도화 세부 알고리즘
- 백테스트 엔진 내부 로직 변경

---

## 3. 구현 기준 경로
### 오케스트레이션
- `E:\1_Data\run_paper_daily.bat`

### 실행 스크립트
- Phase 1: `E:\1_Data\tools\macro_signal_daily.py`
- Phase 2: `E:\1_Data\tools\sector_score_daily.py`
- Phase 3 collect: `E:\1_Data\tools\news_collect_naver_daily.py`
- Phase 3 score: `E:\1_Data\tools\news_score_daily.py`
- Final merge: `E:\1_Data\tools\final_score_merge_daily.py`
- Integration: `E:\1_Data\tools\signal_integration_daily.py`

### 실행 엔진
- `E:\1_Data\paper_engine.py`

---

## 4. 표준 실행 순서 (배치 기준)
`run_paper_daily.bat`에서 신호 단계는 아래 순서를 따른다.

1. `macro_signal_daily.py` (PRE)
2. `p0_daily_check.py`
3. `gate_daily.py`
4. `survivorship_policy_daily.py`
5. `liquidity_filter_daily.py`
6. `tools\sector_score_daily.py` (6.5)
7. `tools\news_collect_naver_daily.py` (6.55)
8. `tools\news_score_daily.py` (6.6)
9. `tools\final_score_merge_daily.py` (6.7)
10. `paper_engine.py` (7/9)
11. `tools\signal_integration_daily.py` (14/16)

설계 원칙:
- 후보 체인(score sidecar)은 항상 `liquidity_filter` 이후 실행.
- optional 신호 단계 실패 시 파이프라인 전체는 중단하지 않고 fail-soft.

---

## 5. 데이터 계약 요약
상세 계약은 `pipeline-contracts.md`를 우선한다. 본 문서는 요약만 유지한다.

### 후보 체인 (Candidate chain)
- Base: `candidates_latest_data.csv`
- Filtered: `candidates_latest_data.filtered.csv`
- Sector: `candidates_latest_data.with_sector_score.csv`
- News: `candidates_latest_data.with_news_score.csv`
- Final: `candidates_latest_data.with_final_score.csv`

규칙:
- same-run 기준으로 filtered/sector/news/final의 date 정렬 유지.
- 원본 후보 파일을 강제 덮어쓰기하지 않고 sidecar 방식 우선.

### 통합 체인 (Trades integration)
- `joined_trades_latest.csv`
- `joined_trades_final_latest.csv`
- `signal_integration_status_YYYYMMDD.json`

규칙:
- trades 구간은 과거 기간일 수 있음.
- candidate date와 trades date가 다르다고 즉시 오류로 간주하지 않음.

---

## 6. Phase별 설계
### Phase 1: Macro
입력:
- 외부 거시/시장 지표 (가능 시 실데이터, 실패 시 제한적 fallback)

출력:
- `2_Logs\macro_signal_latest.json`

게이트 연동:
- `gate_daily.py`의 `gate_macro`에서 참조
- macro 신호 부재 시 fail-soft PASS 정책 허용 가능(운영 정책에 따름)

### Phase 2: Sector
입력:
- `candidates_latest_data.filtered.csv`
- sector mapping/cache 데이터

출력:
- `candidates_latest_data.with_sector_score.csv`

### Phase 3: News
1) Collect 입력:
- 최신 as-of 후보 산출물(우선순위 파일 중 **최신 날짜 파일 선택**)

1) Collect 출력:
- DB upsert (`news_trading\data\trading.db`)
- `news_collect_status_latest.json`

2) Score 입력:
- `candidates_latest_data.with_sector_score.csv`
- news DB

2) Score 출력:
- `candidates_latest_data.with_news_score.csv`

### Phase 2+3: Final merge
입력:
- `candidates_latest_data.with_news_score.csv`
- macro 상태(필요 시 regime 점수 반영)

출력:
- `candidates_latest_data.with_final_score.csv`

---

## 7. 엔진 점수 사용 규칙
`paper_engine.py`는 후보 입력 시 sidecar 우선순위를 적용한다.

후보 입력 우선순위:
1. `.with_final_score.csv`
2. `.with_news_score.csv`
3. `.with_sector_score.csv`
4. `.filtered.csv`
5. base `candidates_latest_data.csv`

랭킹 컬럼 우선순위:
- `final_score` 존재 시 `final_score`
- 없으면 `score`

---

## 8. 안정성 정책
### Fail-soft (계속 진행)
- sector/news/final score 단계 실패
- 일부 데이터 소스 누락

동작:
- 점수 0 또는 source=`FAIL_SOFT`로 채움
- 상태 JSON에 원인 기록

### Fail-closed (진입 차단/감축)
- kill switch
- crash risk off
- relax ladder 상위 레벨 안전 캡

동작:
- `BLOCK` 또는 `max_new` 감축

---

## 9. 현재 상태 (as-of 2026-03-05)
구현 반영 완료:
- 배치 연결: Macro/Sector/News/Final/Integration 포함
- 후보 체인: `2026-03-05` 정렬 확인
- 엔진: `final_score` 우선 랭킹 지원
- 뉴스 수집기: 최신 as-of 입력 선택 로직 반영

운영 리스크(잔여):
- 외부 API 품질 변동으로 fallback 빈도 증가 가능
- integration에서 trades 구간 news 영향은 데이터 창 차이로 0일 수 있음

---

## 10. Deprecated 설계 정리
아래는 구버전 문서 표현이며, 본 문서 기준으로 사용 중단한다.

1. `sector_signal_daily.py` 표기
- 정식 구현명: `tools\sector_score_daily.py`

2. `news_signal_daily.py` 단일 단계 표기
- 정식 구현: `news_collect_naver_daily.py` + `news_score_daily.py` 2단계

3. 원본 후보 CSV에 직접 append 전제
- 현재는 sidecar 산출물 체인 방식이 기준

4. 고정 가중치 문구를 단일 진실로 서술
- 가중치/정책은 `final_score_merge_daily.py`와 상태 파일을 기준으로 관리

---

## 11. 변경 관리 규칙
Signal Integration 변경 시 필수 업데이트:
1. 본 문서 (`signal-integration.design.md`)
2. 계약 문서 (`pipeline-contracts.md`)
3. 운영 문서 (`03-operations/daily-runbook.md`)
4. 증거 산출물 (`2_Logs` status JSON)

이 4개가 동시에 갱신되지 않으면 변경 완료로 간주하지 않는다.
