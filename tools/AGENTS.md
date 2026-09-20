# AGENTS.md

## 적용 범위
- 이 파일은 `E:\1_Data\tools\` 아래 스크립트 작업에 적용한다.

## tools 작업 원칙
- 상태/로그 생성 스크립트는 `latest` 파일과 dated 파일의 역할을 구분한다.
- `latest`만 갱신하고 dated 산출물을 빼먹지 않는다.
- 읽기 전용 해석 스크립트와 원인 해결 스크립트를 섞지 않는다.
- 결과물 JSON은 실제 파싱 가능한 유효 JSON인지 실행으로 확인한다.
- 상태를 좋게 보이게 만드는 하드코딩은 금지한다.
- `news_score`에서 `candidate_article_overlap=0`이면 `candidate_article_coverage_zero` 원인으로 분리 표기하고 PASS/WARN으로 승격하지 않는다.
- `DB_SIGNAL_NO_ARTICLE_COVERAGE`는 운영 차단 완화 상태값이 아니라 원인 추적 라벨로만 사용한다.
- `news_collect` 수정 반영 검증은 `outside_time_window` 스킵 실행만으로 완료 처리하지 않고, 장중 또는 강제 윈도우 1회 수집 실행 증거까지 확인한다.

## 검증
- Python 파일 수정 후 `py_compile` 검증을 먼저 한다.
- 가능하면 스크립트를 실제 1회 실행해 최신 산출물까지 확인한다.
- 산출물 구조가 바뀌면 `latest` 파일 기준으로 실제 키 존재 여부를 확인한다.
