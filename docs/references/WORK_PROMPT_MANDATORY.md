# 작업 프롬프트 고정 문구

## A) 일반 작업 시작 문구

AGENTS.md와 관련 PLANS.md를 먼저 읽고, 이번 작업 범위만 진행한다.
최종 보고는 `E:\1_Data\docs\references\FINAL_REPORT_TEMPLATE.md`를 사용하되, 충돌 시 `E:\1_Data\AGENTS.md` 6~8번과 20~22번을 우선한다.

이번 작업에서 아래 조건이 모두 충족되기 전까지
`완료`, `수정 완료`, `문제없음`, `운영 가능`, `done`, `fixed`, `ready`
라고 결론 내리지 않는다.

필수 검증 항목:
1. 기능 검증
2. 정합성 검증
3. 운영 반영 검증
4. 정책 검증
5. FAIL-CLOSED 검증
6. 회귀 검증

최종 판정 전에는 반드시 아래를 명시한다.
- 무엇을 수정했는지
- 무엇을 테스트했는지
- 무엇을 테스트하지 않았는지
- 어떤 증거가 있는지
- 각 검증 항목의 PASS / FAIL / NA

코드 확인만으로는 충분하지 않다.
런타임 증거가 필요하다.
사실과 해석을 분리해서 작성한다.

## B) 분석 전용 문구

AGENTS.md, 관련 계획 문서, 대상 파일을 읽는다.
코드는 수정하지 않는다.

분석만 수행한다.

아래 6개 관점에서 평가한다.
1. 기능 검증 범위
2. 정합성 검증 범위
3. 운영 반영 검증 범위
4. 정책 검증 범위
5. FAIL-CLOSED 검증 범위
6. 회귀 검증 범위

아래 형식으로 반환한다.
- 사실
- 해석
- 빠진 검증 영역
- 모호한 지점
- 최소 수정 포인트

`완료`, `문제없음`이라고 쓰지 않는다.

## C) 서브에이전트 병렬 검증 문구

현재 작업은 주식로직 운영 검증이다.
코드 수정 금지.
서브에이전트는 read-only 검증만 수행한다.

공통 금지:
- 파일 수정 금지
- apply / DOIT 금지
- 스케줄 등록 금지
- 원장 보정 금지
- config lock 변경 금지
- Gate / STOP / LOCK 의미 변경 금지

허용:
- 조회 명령
- 로그 분석
- JSON / CSV / Markdown 읽기
- 코드 위치 확인
- 테스트 결과 요약

각 에이전트는 다음 형식으로만 보고한다.
- 발견 사실
- 근거 파일/라인
- 위험도: HARD_FAIL / WARN / INFO
- 권장 조치
- 수정 필요 여부

모든 에이전트 결과를 기다린 뒤 메인 요약만 출력한다.

## D) 원인 해결 문구

검증 결과 중 지정된 문제 1개만 원인 해결한다.
수정 전 백업을 만들고 백업 경로를 먼저 보고한다.
최소 범위만 수정한다.

수정 후 아래를 수행한다.
1. 문법 검증
2. 실행 검증
3. 결과물 검증
4. 정책 검증
5. FAIL-CLOSED 검증
6. 회귀 검증

정책 변경인지 버그 수정인지 분리해서 보고한다.
표시 수정과 원인 해결을 섞지 않는다.

## E) 종료 및 기록 문구

지금까지 한 것, 남은 것, 결정 사항을 PLANS.md에 업데이트한다.
AGENTS.md 업데이트 필요 여부를 확인한다.

보고는 아래 순서로 한다.
1. 지시사항
2. 진행된 것
3. 남은 것
4. 다음 진행
5. 다음 보고 기준

전체가 아니면 전체 완료라고 말하지 않는다.
검증하지 않은 항목은 PASS로 쓰지 않는다.
표시 수정, 원인 해결, 전체로직 적용 여부를 분리해서 보고한다.

## F) 주식로직 서브에이전트 검증 문구

현재 작업은 주식로직 운영 검증이다.
서브에이전트 5개를 생성해 병렬 검토한다.

공통 기준:
- `E:\1_Data\AGENTS.md`와 `E:\1_Data\.agent\PLANS.md`를 기준으로 한다.
- RootA는 `E:\1_Data`이며 매매 로직, 배치, 데이터, SSOT, 주문-체결-원장 작업을 담당한다.
- RootB는 `E:\vibe\buffett`이며 대시보드, 화면, 상태 JSON, React 작업을 담당한다.
- RootB 항목을 검토할 때는 `E:\vibe\buffett\AGENTS.md`와 `E:\vibe\buffett\PLANS.md`도 확인한다.
- 모든 서브에이전트는 read-only 검증만 수행한다.

공통 금지:
- 파일 수정 금지
- apply / DOIT 금지
- 스케줄 등록 금지
- 원장 보정 금지
- config lock 변경 금지
- Gate / STOP / LOCK 의미 변경 금지
- 운영 정책 변경 금지
- 실제 주문 실행 금지

Agent-1 Data/Date Auditor:
- `orders(D) -> fills(D) -> ledger -> stats` 흐름을 검토한다.
- D, exec_date, as_of, run_id, paper/broker 날짜 혼용 가능성을 찾는다.
- `orders_exec` 없음, `exec_date != D`, as_of/run_id 불일치 여부를 확인한다.

Agent-2 Risk Gate Auditor:
- risk_off, kill_switch, max_drawdown, daily_loss, FAIL-CLOSED 조건을 검토한다.
- 리스크 게이트가 우회될 가능성을 찾는다.
- Gate / STOP / LOCK 의미가 유지되는지 확인한다.

Agent-3 Ops Auditor:
- bat/ps1 실행 체인, exit code 전파, 로그 생성, 재시작 안전성을 검토한다.
- 자동 실행 작업이 숨김 실행, DRY/APPLY 정책, lock 정책을 지키는지 확인한다.
- 공식 배치와 단독 실행 경로를 구분한다.

Agent-4 Backtest Auditor:
- look-ahead bias, survivorship bias, data snooping, paper/live 혼용 가능성을 찾는다.
- 검증 기간이 운영 기준과 맞는지 확인한다.
- 비용, 세금, 슬리피지, 표본 수, 과최적화 위험을 점검한다.

Agent-5 UI/SSOT Auditor:
- 대시보드 표시값과 SSOT 파일의 불일치 가능성을 찾는다.
- stale 값, NA 처리, latest JSON 갱신 시각, 화면 표시 기준을 확인한다.
- RootB 표시 수정과 RootA 원인 해결을 분리한다.

각 에이전트 보고 형식:
- 발견 사실
- 근거 파일/라인
- 위험도: HARD_FAIL / WARN / INFO
- 권장 조치
- 수정 필요 여부

메인 Codex 처리 기준:
- 모든 에이전트 결과를 기다린다.
- 중복 이슈를 병합한다.
- HARD_FAIL을 먼저 정렬한다.
- 실제 수정은 메인 Codex 또는 단일 Writer Agent만 수행한다.
- 여러 서브에이전트가 동시에 코드를 수정하게 하지 않는다.
- 최종 판단은 메인 Codex가 한다.

## G) LLM Wiki Context Autoload

At the start of RootA work in `E:\1_Data`, read this file first unless the user explicitly requests a self-contained task:

`E:\1_Data\docs\llm_wiki\wiki_index_latest.md`

Use it only as a retrieval entry point. Follow links to the smallest needed current-state, source, policy, or artifact files.

Do not treat source notes as policy changes. Do not change Gate, STOP, LOCK, risk, score, order, fill, ledger, stats, scheduler, credentials, or external API behavior without explicit approval.

If `orders_exec_exists=false` or `asof_matches_D=false`, treat order/fill/ledger/stat interpretation as STOP concern until canonical RootA artifacts are inspected.
