# 20260420_instruction_chain_realign

## 2026-04-24 Verification
- Backup:
  - `E:\1_Data\backup\20260424_instruction_chain_realign\20260424_1130_instruction_chain_verify`
- Checked:
  - `E:\1_Data\AGENTS.md`
  - `E:\1_Data\.agent\PLANS.md`
  - `E:\1_Data\docs\references\WORK_PROMPT_MANDATORY.md`
  - `E:\1_Data\docs\references\FINAL_REPORT_TEMPLATE.md`
  - `E:\1_Data\docs\exec-plans\active`
  - `E:\1_Data\docs\exec-plans\completed`
- Result:
  - AGENTS document links exist.
  - Required reference docs exist.
  - active/completed directories exist.
  - Active plan filename rule `YYYYMMDD_<topic>.md` matched current active plan files.
- Scope:
  - No trading logic change.
  - No dashboard UI change.
  - No RootA/RootB path policy change.

## Goal
- AGENTS/PLANS 운영 구조를 권장 체계로 유지하고, 이후 대규모 작업 시 계획-실행-종료 흐름을 표준화한다.

## In-Scope
- `E:\1_Data\AGENTS.md` 맵형 유지
- `E:\1_Data\.agent\PLANS.md` 정책 기준 준수
- `E:\1_Data\docs\exec-plans\active\`에서 작업별 계획 문서 생성/갱신
- 완료된 계획을 `E:\1_Data\docs\exec-plans\completed\`로 이동

## Out-of-Scope
- 전략/주문/리스크 로직 변경
- 대시보드 UI 구조 변경
- RootA/RootB 외 경로 작업

## Risks And Fail-Closed Checks
- 경로 이탈 위험: RootA/RootB 외 경로 변경 금지
- 무검증 완료 위험: 문법/실행/산출물 확인 전 완료 선언 금지
- 범위 확장 위험: 요청 범위 외 개선/확장 금지

## Steps
1. 작업 범위를 1문장으로 확정한다.
2. 수정 대상 파일 백업 경로를 먼저 기록한다.
3. 최소 범위로 수정한다.
4. 문법 검증 -> 실행 검증 -> 산출물 검증 순으로 확인한다.
5. 결과를 `현재 완료된 것 / 남은 실제 문제 / 다음 진행사항` 순서로 보고한다.

## Validation
- 문서 링크 유효성 확인
- plan 파일 위치/이름 규칙(`YYYYMMDD_<topic>.md`) 확인
- active/completed 디렉터리 존재 확인

## Rollback Point
- 문서 구조 롤백 필요 시:
  - `E:\1_Data\_bak\agents_plans_restructure_20260420_085152\AGENTS.md.bak`
  - `E:\1_Data\_bak\agents_plans_restructure_20260420_085152\PLANS.md.bak`
