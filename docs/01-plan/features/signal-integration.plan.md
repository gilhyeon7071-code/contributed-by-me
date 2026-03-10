# Plan: signal-integration (Execution Roadmap)

작성일: 2026-03-04  
최종수정: 2026-03-05  
상태: Active (Phase 1~3 wiring implemented)
대상 시스템: `E:\1_Data`

참조:
- `E:\1_Data\docs\02-design\features\signal-integration.design.md`
- `E:\1_Data\docs\02-design\pipeline-contracts.md`
- `E:\1_Data\docs\03-operations\daily-runbook.md`

---

## 1. 목적
Signal Integration을 메인 파이프라인에서 안정적으로 사용하고,
후보 체인(candidate)과 트레이드 통합(integration)의 정합성을 운영 SSOT로 고정한다.

핵심 목적:
1. Macro/Sector/News/Final 신호를 배치에 일관 연결
2. fail-soft/fail-closed 경계 명확화
3. 일별 증거 산출물(`2_Logs`) 기반 검증 체계 유지

---

## 2. 현재 상태 요약 (2026-03-05)
### Done
- `run_paper_daily.bat`에 신호 단계 연결 완료
  - `macro_signal_daily.py`
  - `sector_score_daily.py`
  - `news_collect_naver_daily.py`
  - `news_score_daily.py`
  - `final_score_merge_daily.py`
  - `signal_integration_daily.py`
- 후보 sidecar 체인 생성 확인
  - `with_sector_score` -> `with_news_score` -> `with_final_score`
- `paper_engine.py` 점수 우선순위 반영
  - `final_score` 우선, 없으면 `score`
- relax ladder 안전장치 + 리스크 캡(설정) 반영

### In-progress 관찰 항목
- 외부 API 불안정 구간에서 fallback 빈도 모니터링
- trades 통합에서 news 영향(`news_nonzero`)이 0인 날짜창 지속 여부 모니터링

---

## 3. 범위 정의
### 포함
- Phase 1 Macro gate
- Phase 2 Sector score
- Phase 3 News collect/score
- Final score merge
- Signal integration status

### 제외
- 전략 가중치 자동 최적화 설계 변경
- 뉴스 모델 고도화 알고리즘
- 백테스트 코어 로직 리팩터

---

## 4. 단계별 로드맵
## Phase 1 (Macro)
목표:
- `macro_signal_latest.json` 생성
- `gate_daily`의 macro 게이트 연동 유지

완료 기준:
- 당일 산출물 존재
- gate 결과에 macro 상태 반영

상태: Implemented

## Phase 2 (Sector)
목표:
- filtered candidates 기준 sector score 부착

완료 기준:
- `with_sector_score.csv` 생성
- filtered 대비 행수 정합

상태: Implemented

## Phase 3 (News)
목표:
- candidate universe 뉴스 수집/점수화

완료 기준:
- `news_collect_status_latest.json` 정상
- `with_news_score.csv` 생성

상태: Implemented (운영 데이터 품질 모니터링 필요)

## Phase 2+3 (Final)
목표:
- 후보용 최종 점수 산출

완료 기준:
- `with_final_score.csv` 생성
- 비정상 시 fail-soft 상태 명시

상태: Implemented

## Integration (Trades)
목표:
- trades 구간 조인 산출물/상태 JSON 안정 생산

완료 기준:
- `joined_trades_final_latest.csv` 생성
- `signal_integration_status_YYYYMMDD.json` 생성

상태: Implemented (date-window 해석 주의)

---

## 5. 운영 검증 체크리스트
일별 최소 검증:
1. 후보 체인 날짜 정렬 확인
2. p0/gate risk_off reason 확인
3. integration status의 phase별 score_fill 확인
4. 실패 시 fail-soft/fail-closed 동작이 설계와 일치하는지 확인

판정 기준:
- PASS: 산출물 존재 + 계약 충족
- FAIL: 산출물 누락 또는 계약 위반
- NA: 해당 날짜/구간에서 평가 불가(명시 필요)

---

## 6. 남은 단기 과제 (Next)
1. `signal_integration_daily.py`의 candidate/trades 기간 불일치 진단 리포트 자동화
2. crash_risk_off 실데이터 경로 안정화(현재 fallback 의존도 완화)
3. 로그 보존 정책 적용(대용량 JSON 누적 제어)

---

## 7. 변경 관리 규칙
Signal Integration 관련 변경은 아래 4종을 동시에 갱신한다.
1. 본 Plan 문서
2. Design 문서
3. Contract 문서
4. 운영 runbook

추가로, 해당 변경의 실행 증거를 `2_Logs`에 남긴다.
