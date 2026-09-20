# Dashboard Safe Change Protocol (SSOT)

Last updated: 2026-03-17
Scope: `E:\vibe\buffett\dashboard.py`, `E:\vibe\buffett\dashboard_stock_v2.py`

## 1) 목적
- 대시보드 수정 중 파일 손상/복구 반복을 방지한다.
- 변경 1건마다 복구 가능 지점을 남긴다.
- 사용자 화면 기준으로만 완료 판정한다.

## 2) 고정 규칙
1. 대량 정규식 치환 금지 (파일 전체 Replace 금지).
2. 변경 단위는 기능 블록(10~40줄)으로 제한한다.
3. 변경 1회마다 즉시 검증한다.
4. 검증 실패 시 다음 변경 금지, 즉시 롤백한다.
5. 숫자/표시 규칙은 아래 SSOT를 따른다.
   - `ui_numeric_display_rules.md`
   - `ui_typography_hierarchy_rules.md`
   - `dashboard_ui_operating_rules_ssot_latest.md`
6. 수정 전 체크는 아래 문서를 먼저 확인한다.
   - `dashboard_ui_preflight_checklist_latest.md`

## 3) 표준 절차 (필수)
1. 백업 생성
   - `dashboard.py.bak_codex_YYYYMMDD_HHMMSS`
2. 소규모 패치 적용
   - 한 번에 1개 기능만 변경
3. 문법 검사
   - `python -m py_compile E:\vibe\buffett\dashboard.py`
4. 스모크 체크
   - 대시보드 실행 후 대상 화면 1개 동작 확인
   - `dashboard_ui_preflight_checklist_latest.md` 기준 체크
5. 결과 기록
   - 적용/미적용/보류를 작업 로그에 남김

## 4) 롤백 기준
- 아래 중 하나라도 발생하면 즉시 롤백:
1. `NameError`, `SyntaxError`
2. 버튼/토글 상태 역전
3. 화면 미표시 또는 주요 카드 누락
4. 파일 크기 비정상(예: 수 바이트)

## 5) 롤백 명령 예시
```powershell
Copy-Item E:\vibe\buffett\dashboard.py.bak_codex_ui_YYYYMMDD_HHMMSS `
  E:\vibe\buffett\dashboard.py -Force
python -m py_compile E:\vibe\buffett\dashboard.py
```

## 6) 완료 판정
- 코드 완료가 아니라 화면 기준 완료로 판정:
1. 요청한 텍스트/버튼 상태가 즉시 반영됨
2. 숫자 표기(콤마/퍼센트/결측) 규칙 일치
3. 한글 용어/레이아웃 규칙 일치
4. 오류 없이 재실행 가능

## 7) 런타임 검증 필수 규칙
- `py_compile` 통과 후 실제 실행 확인을 필수로 한다.
- 실행 확인 전에는 완료 판정을 하지 않는다.
- 런타임 에러가 하나라도 남으면 다음 수정으로 넘어가지 않는다.

## 8) 묶음 수정 우선 규칙
- 반복되는 레이아웃/라벨 수정은 `화면 단위 묶음`으로 처리한다.
- 같은 블록을 3회 이상 연속 수정하는 방식은 금지한다.
- 사용자가 화면 기준으로 확인하기 어려운 경우, 다음 묶음으로 넘기지 않는다.
