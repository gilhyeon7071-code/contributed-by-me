# 2026-04-21 Risk Alignment P1-P4

## 목적
- 현재 일반매매 로직 기준 우선순위 1~4 항목을 최소 범위로 수정한다.

## 범위
- 대상 파일:
  - `E:\1_Data\generate_candidates_v41_1.py`
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\tools\sector_score_daily.py`
  - `E:\1_Data\paper\paper_engine_config.json`
  - `E:\1_Data\PLANS.md`
- 필요 시 테스트 파일 추가
- 배치 엔트리포인트 의미, SSOT 체인, D 규칙, FAIL-CLOSED 의미는 변경하지 않는다.

## 현재 문제
1. 일부 경로에서 macro/p0 입력 장애 시 친위험 bullish fallback이 남아 있다.
2. DDM의 MDD 선택 로직이 rolling/lifetime debug를 혼합할 수 있다.
3. lookahead 점검이 감사 중심이고 사전 차단이 없다.
4. AI 반도체 코드 기본값이 코드 내부 기본 설정에 남아 있다.

## 적용
1. macro/p0 입력 장애 fallback을 보수적으로 조정한다.
2. DDM이 rolling metric 우선, lifetime은 명시적 호환 fallback로만 쓰도록 정렬한다.
3. factor guard에서 negative shift 탐지 시 생성기 자체를 fail-closed 처리한다.
4. 코드 내부 기본 설정의 AI 종목 하드코딩을 제거하고 config 의존으로 정렬한다.

## 검증
1. 문법 검증
2. 단위/스모크 테스트 검증
3. 생성기 실행 검증
4. 배치 실행 검증
5. 산출물/lock/config snapshot 검증
