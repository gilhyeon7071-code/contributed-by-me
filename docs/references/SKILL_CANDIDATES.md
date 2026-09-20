# 스킬·플러그인 후보 대장

2026-09-11 조사. 이 파일의 목적은 **지금 안 깐 것을 나중에 왜·언제 깔지** 남기는 것이다.
목록만 남기면 다음 사람이 다시 조사한다. 그래서 후보마다 **설치 조건**을 적는다.

조사 범위: easier-life-skills 13개 / claude-plugins-official 294개 / bkit-marketplace.
온라인 검색은 GSD·design-taste 두 건에만 했다.

---

## 이미 설치·사용 가능 (다시 깔지 말 것)

```
docs:changelog / docs:document-project   2026-09-11 설치. git 이력 -> CHANGELOG, /docs 링크 README
find-skills:find-skills                  2026-09-11 설치. 저장소 분석 -> 스킬 추천 (읽기 전용)
bkit:*                                   PDCA 방법론 일체. GSD 와 역할이 겹친다
dataviz                                  차트·대시보드 설계, 색 체계, 접근성 (내장)
web-design-guidelines                    UI 접근성·UX 검토 (사용자 스킬)
vercel-react-best-practices              React 성능 (사용자 스킬)
프로젝트 스킬 7개                          cite-check / tool-change / risk-path-edit /
                                         diagnose-outage / claim-standing / measure-claim / capture-lesson
```

---

## 보류 — 조건이 오면 깐다

### serena  ·  claude-plugins-official  ·  우선순위 1
의미 기반 코드 탐색 MCP 서버 (language server 통합).
```
왜 필요한가   2026-09-11 하루에 grep -r 이 120초 타임아웃으로 4번 죽었다.
              paper_engine/ 은 14모듈 패키지, 루트 .py 수백 개. 텍스트 grep 이 규모에 안 맞는다
막는 것       uvx 필요. 실측 결과 uv/uvx 미설치
              .mcp.json -> uvx --from git+https://github.com/oraios/serena
              community-managed 태그. 매 실행 시 서드파티 저장소를 가져온다
설치 조건     (1) uv 설치  (2) 위 git 의존을 받아들일지 판단
설치          /plugin install serena@claude-plugins-official
```

### code-audit  ·  easier-life-skills
미사용 코드 탐지 + 로깅 품질 감사 (REST 부분은 해당 없음).
```
왜 필요한가   backup/ 하위 3,862개, 루트+tools .bak 100개.
              그리고 이 시스템의 문서화된 실패 양식이 "조용한 실패" 라 로깅 감사가 본질에 닿는다
설치 조건     **backup/ 를 제외 범위로 정한 뒤.** 안 그러면 아무도 못 읽는 보고서가 나온다
설치          /plugin install code-audit@easier-life-skills
```

### dependency-audit  ·  easier-life-skills
pip-audit 기반 취약점·노후 버전 스캔.
```
왜 필요한가   requirements.txt 30개가 전부 핀 고정인데 감사 흔적이 없다
              (numpy 1.26.4 / pandas 2.3.3 / streamlit 1.52.2 / requests 2.32.5 ...)
              핀 고정은 재현성엔 좋지만 취약점도 같이 굳는다
설치 조건     한 번 돌리고 끝나는 일. 조용한 시간에
설치          /plugin install dependency-audit@easier-life-skills
```

### claude-code-setup  ·  claude-plugins-official
코드베이스 분석 -> 훅·스킬·MCP·서브에이전트 추천.
```
왜 필요한가   훅이 0개다 (사용자·프로젝트 설정 양쪽 실측 2026-09-11)
              "문서는 내가 읽어야 작동하고 훅은 내가 잊어도 작동한다" -> capture-lesson 의 코드 우선 원칙
설치 조건     훅 도입을 실제로 결정할 때. 2026-09-11 에 스킬 7개는 손으로 만들었다
```

### mcp-builder  ·  anthropics/skills  (Anthropic 공식)
MCP 서버를 Python/TypeScript 로 만드는 4단계 워크플로 (설계 -> 구현 -> 테스트 -> 문서화).
```
자리는 있다   *_latest.json 상태 산출물을 매번 일회성 python 으로 읽고 있다.
              "실효 설정값 / 이 키의 생산자 / 현재 DDM 단계" 를 타입 있는 도구로 노출하면 반복이 사라진다
왜 보류       새로 만드는 소프트웨어다. 이 프로젝트 진단은 "검증 없이 구현된 아이디어가 과잉"
              그리고 만들까말까 기준 중 **죽으면 드러나는가** 를 통과 못 한다 —
              상태 조회 MCP 가 조용히 낡은 값을 돌려주면 나는 그걸 확신을 갖고 인용한다.
              [[feedback_check_artifact_age_first]] 가 기계화된 형태로 깨진다
설치 조건     (1) 상태 조회 MCP 를 만들기로 결정  AND
              (2) **모든 응답에 generated_at + 나이를 강제로 싣고, 임계 초과 시 값 대신 STALE 을 던지는**
                  설계를 먼저 확정
설치          /plugin marketplace add anthropics/skills
              /plugin install <마켓이 선언한 이름>   <- add 후 known_marketplaces.json 에서 실제 키 확인할 것
                                                      (저장소 경로와 마켓 선언명이 다를 수 있다)
미확인        document-skills / example-skills 두 플러그인에 17개 스킬이 중복 포함된다는 버그
              (anthropics/skills#189) — 사용자 제보, 대조하지 않았다
```

### GSD (get-shit-done)  ·  gsd-build/get-shit-done  (서드파티, TÂCHES)
질문 -> 리서치 -> 요구사항 -> 로드맵 -> 계획 -> 실행 -> 검증. 스펙 기반 개발 워크플로.
```
왜 보류       이 저장소엔 그 층이 이미 더 많다 (AGENTS.md 23KB / docs 3계층 / PLANS 2.3MB /
              OBJECTIVE_LEDGER / PREREG / STAGE_PLAN / GATE_JUDGMENT_FORM / 결론·고장구간 등록부)
              오늘 확인된 실패는 "계획이 없다" 가 아니라 **있는 문서가 그 순간에 안 열린다** 였다
              그리고 bkit(PDCA) 과 정면으로 겹친다 -> 설명이 겹치면 둘 다 안 열린다
              파일 배치 규약이 하나 더 생긴다. 경로 계약을 이미 두 번 어겼다
있는 장점      맨 앞의 "질문" 단계. cite-check 과 같은 발상이되 입자가 프로젝트 단위다
설치 조건     **TRADABILITY PIVOT 착수 시** — 백지에서 새 매매 로직을 만들 때.
              그때는 요구사항도 로드맵도 실제로 없으므로 맞는 도구다
설치          /plugin marketplace add gsd-build/get-shit-done
미확인        저장소 본문 미열람. 사용자 설명 + 검색 결과 기준 평가다
```

### design-taste  ·  h3nryprod01 (서드파티)
emilkowalski/skill + pbakaus/impeccable + leonxlnx/taste-skill 병합·중복제거. 시각 완성도.
```
대상          E:\vibe\control_center_v2 — React + TypeScript + Vite, .tsx 15개,
              recharts / lightweight-charts / lucide-react.  **streamlit 아니다** (2026-09-11 정정)
왜 보류       dataviz + web-design-guidelines + vercel-react-best-practices 셋이 이미 덮는다.
              차트 대시보드라 순수 시각 취향보다 dataviz 입자가 더 맞다
              관객이 한 명이고 ①매매가능·②수익률 어느 쪽도 못 민다
              09-04 UI 개선(개수 -> 종목 목록)은 **정보 설계** 문제였고 이 스킬로는 안 풀린다
설치 조건     위 셋으로 먼저 돌려보고 부족할 때. 그리고 설치 위치는
              E:\vibe\.claude\skills\ 또는 사용자 전역 (대시보드가 다른 루트에 있다)
설치 방법     **zip 업로드는 claude.ai 웹/Cowork 경로다.** Claude Code 는 폴더를 그대로 놓으면 된다
```

### 그 외 — 조건이 붙지 않는다 (기각에 가까움)
```
security-review    .secrets/ 는 .gitignore:164 로 제외, 추적 446개에 민감 후보 0건 (실측)
brainstorm         병목이 아이디어가 아니라 검증이다. 방향은 OBJECTIVE_LEDGER 에 정해져 있다
skill-creator      "스킬 성능 측정" 만 없는 조각이고 나머지는 capture-lesson 과 겹친다
pyright-lsp        mypy 가 pyproject.toml 에 이미 설정돼 있다
PR 리뷰 계열        원격은 있으나 60커밋·워크플로 1개. PR 기반 흐름이 관측되지 않는다
클라우드/SaaS 약 240개  이 프로젝트에 클라우드 구성요소가 없다
```

---

## 설치·로딩에 관해 이날 확인한 사실

```
프로젝트 스킬은 **세션의 프로젝트 루트** .claude/skills/ 에서만 읽힌다
  -> 세션을 C:\Windows\System32 에서 열면 E:\1_Data\.claude\skills\ 는 영원히 안 잡힌다
  -> E:\1_Data\start_claude.bat 로 열 것
메모도 프로젝트 경로별로 갈린다 (.claude/projects/<경로키>/memory)
  -> 2026-09-11 에 C--Windows-System32 의 134개를 E--1-Data 로 병합했다
저장소 분석형 스킬(find-skills, docs:*)도 활성 저장소를 본다. 루트가 틀리면 엉뚱한 데를 분석한다
플러그인 설치는 슬래시 명령으로만 된다 (에이전트가 실행할 수 없다)
/plugin 에 인자를 주면 대화형 UI 로 넘어간다. **한 줄씩 따로 제출할 것**
스킬 개수가 늘면 트리거가 흐려진다 -> capture-lesson 의 위생 규칙 참조
```


---

## [2026-09-11] 스킬이 **두 벌**이다 — 권위와 동기화

```
원본(권위)   E:\1_Data\.claude\skills\          <- 저장소 안. 여기를 고친다
배포 사본     C:\Users\jjtop\.claude\skills\   <- 지금 **실제로 읽히는** 곳
```
세션 루트가 `C:\Windows\System32` 라 프로젝트 스킬이 안 잡혀서 사용자 폴더로 복사했다.
**한쪽만 고치면 갈라진다.** 원본을 고친 뒤 아래로 다시 배포한다:

```
cp -r E:/1_Data/.claude/skills/<이름> "C:/Users/jjtop/.claude/skills/<이름>"
```

근본 해결은 `E:\1_Data\start_claude.bat` 으로 세션을 여는 것이다.
그렇게 되면 **사용자 폴더 사본 7개를 지워야 한다** (이름이 겹쳐 둘 다 뜬다).
대상: capture-lesson / cite-check / claim-standing / diagnose-outage /
measure-claim / risk-path-edit / tool-change


---

## [2026-09-12] 추가 설치 — `grilling` / `grill-me` (mattpocock/skills)

```
npx -y skills add mattpocock/skills --skill grilling --agent claude-code
npx -y skills add mattpocock/skills --skill grill-me --agent claude-code
-> E:\1_Data\.claude\skills\  (저장소, 권위)  + 사용자 폴더로 복사 배포
```

**함정**: `grill-me` 는 껍데기다 — 본문이 *"Call the Skill tool with 'grilling'"* 한 줄뿐이다.
`--skill grill-me` 만 설치하면 **알맹이가 없어 호출이 실패한다.** 둘 다 받아야 한다.

### 무엇을 하나

일을 시작하기 전에 결정을 뽑아내는 절차다. 결정을 **설계 트리**로 매핑하고,
선행 결정이 정해진 질문들(=프론티어)을 **한 라운드에 번호 붙여 권고안과 함께** 묻는다.
답이 오면 트리가 재구성되고 프론티어가 밀린다. 빌 때까지 반복.

핵심 두 줄:
```
사실을 찾는 것은 에이전트의 일이다. 환경에서 알아낼 수 있는 것을 사용자에게 묻지 않는다
결정은 사용자의 것이다. 각각을 제시하고 기다린다
```

### 왜 이 저장소에 맞나 — 겹치지 않고 **앞단이 비어 있었다**

```
grilling         일을 **시작하기 전** 결정을 뽑는다      <- 비어 있던 층
cite-check       과거 결론을 **인용하기 전** 대조
claim-standing   주장을 **말하기 전** 자격 검사
```
그리고 반복된 지적([[feedback_dont_turn_work_into_questions]] / 2026-09-11
*"궁금한 질문만 하는거야"*)의 교정본이다 — "더 묻자"가 아니라
**"사실은 내가 찾고, 결정만, 한 번에 몰아서"** 다.

2026-09-11 에 계좌 분리·상한 금액·block_gate_names·되돌리기 방식을
메시지 끝마다 하나씩 흩뿌려 물었다. 그것이 한 라운드가 됐어야 했다.

### 폭주 위험은 구조적으로 막혀 있다

`grill-me` 에 `disable-model-invocation: true` 가 있어 **에이전트가 임의로 발동할 수 없다.**
사용자가 부를 때만 뜬다.

### 공식 마켓에도 같은 저장소가 있다

`mattpocock-skills@claude-plugins-official` (SHA 3cca18b3 고정, 묶음 전체).
npx 는 master 최신이라 고정되지 않는다. 하나만 원해서 npx 를 썼다.

---

## 직접 만든 것

### screen-claim  ·  2026-09-12
```
왜            대시보드에서 걸려온 문제가 한 번도 '보기 안 좋다' 가 아니었다.
              전부 **화면이 사실이 아닌 것을 주장한** 문제다. 시장 스킬로는 안 덮인다
근거          Open Orders 51일 낡은 원천을 PASS 로 읽고 '미체결 없습니다' 단정 /
              진행 단계에 창 개념이 없어 15:20 이후 정상 종료를 '멈췄다' 로 오독 /
              종목 목록이 개수만 (2026-09-04). **같은 형태 세 번**
설치 위치     사용자 전역만 (C:\Users\jjtop\.claude\skills\screen-claim)
              **의도적으로 프로젝트 사본을 안 만들었다** - 대시보드는 E:\vibe\control_center_v2 에
              있고 다른 프로젝트 스킬은 E:\1_Data 에 있다. 두 루트에 다 걸리려면 전역이어야 한다
              -> **B2(사용자 폴더 사본 11개 정리) 에서 이건 사본이 아니다. 지우지 말 것**
```

### frontend-design  ·  claude-plugins-official  ·  2026-09-12 조사
```
무엇          Anthropic 공식. 설치 122만. 'production-grade frontend interfaces,
              avoids generic AI aesthetics'
카탈로그      292 플러그인 중 대시보드에 닿는 것 24건, 쓸 만한 건 사실상 이것 하나
              superdesign(2,918)은 마케팅 그래픽 쪽이라 운영 대시보드엔 과하다
설치          /plugin install frontend-design@claude-plugins-official
판단          무해하고 공식이라 깔아도 된다. 다만 **위의 세 사례를 이걸로 못 고친다**
```

### [정정] dataviz 는 '내장' 이라고 단정하지 말 것
2026-09-11 에 '내장' 으로 적었는데 2026-09-12 재확인에서 **근거를 못 찾았다.**
플러그인 카탈로그 292개에 없고, CLI 가 단일 실행파일이라 파일로 확인이 안 된다.
있다고 인용하지 말 것.

---

## [2026-09-13 추가] superpowers · karpathy-skills — **수동 설치함**

09-11 스캔 307개에 **둘 다 안 걸렸다.** 사용자가 저장소 주소를 직접 줘서 알았다.
-> 이 대장의 조사 범위가 마켓 3곳뿐이라는 한계가 드러난 것이다. GitHub 직접 배포는 안 잡힌다.

### 설치 경로가 달랐다 — Remote Control 에서는 `/plugin` 이 안 된다

```
/plugin marketplace add ...   ->  "isn't available over Remote Control"
```

ECC 때(세션 초반)와 같은 벽이다. 그래서 **저장소를 clone 해서 내용을 보고
`~/.claude/skills/` 에 디렉터리째 복사**했다. 15개가 들어갔다.

```
superpowers      obra/superpowers        v6.3.0  MIT   스킬 14개
karpathy-skills  forrestchang/andrej-karpathy-skills   스킬 1개 (karpathy-guidelines)

플러그인명 (정식 설치가 가능한 환경에서)
  /plugin install superpowers@superpowers-dev
  /plugin install andrej-karpathy-skills@karpathy-skills
```

### 훅은 **일부러 안 깔았다**

superpowers 에는 SessionStart 훅이 있다.

```
sup/hooks/hooks.json   matcher "startup|clear|compact"
sup/hooks/session-start  ->  using-superpowers SKILL.md 전문을
                            <EXTREMELY_IMPORTANT> 로 감싸 매 세션 컨텍스트에 주입
```

스킬 디렉터리만 복사했으므로 **이 훅은 등록되지 않았다.**
스킬은 `Skill` 도구로 부를 수 있고, 자동 주입만 없다.
정식 `/plugin install` 을 하면 훅이 같이 붙는다 — 그때는 매 세션 주입이 생긴다는 뜻이다.

### 설치된 15개

```
superpowers
  brainstorming                    창작 작업 전 의도·요구사항 탐색
  writing-plans / executing-plans  계획 작성 / 별도 세션에서 체크포인트 두고 실행
  test-driven-development          구현 전 시험부터
  systematic-debugging             버그·시험실패 시 수정 제안 전에
  verification-before-completion   완료 주장 전 검증 명령 실행
  requesting-code-review / receiving-code-review
  subagent-driven-development / dispatching-parallel-agents
  using-git-worktrees / finishing-a-development-branch
  using-superpowers / writing-skills
karpathy-skills
  karpathy-guidelines              가정 금지 / 단순함 우선 / 외과적 변경 / 검증가능한 성공기준
```

### 주의 세 가지

**1. 두 개는 현재 세션 규칙과 충돌한다**
```
dispatching-parallel-agents / subagent-driven-development
  -> 서브에이전트를 띄우라고 지시한다
  -> 현재 지시는 "사용자가 요청하지 않으면 Agent 도구·워크플로 금지"
  -> 요청이 있을 때만 쓴다
```

**2. 프로젝트 스킬과 역할이 겹친다. 겹치면 프로젝트 스킬이 우선이다**
```
verification-before-completion  <->  claim-standing
systematic-debugging            <->  diagnose-outage
receiving-code-review           <->  grill-me / grilling
```
프로젝트 스킬은 **이 저장소에서 실제로 난 사고**에서 나왔다 (완료 오보, 경보 미도달,
낡은 산출물). 일반 스킬보다 구체적이다. 일반 스킬은 프로젝트 스킬이 안 덮는 자리에만 쓴다.

**3. karpathy-guidelines 1번이 2026-09-13 의 실패와 같은 지점이다**
```
"가정하지 마라. 혼란을 숨기지 마라. 트레이드오프를 드러내라"
  -> 이날 차단 신호를 유리 신호의 잣대로 재고 그대로 결론을 냈다 (46절)
  -> 측정규약 14번이 같은 사안의 기계적 대응이다
```

### 다음에 이 대장을 쓸 사람에게

```
조사 범위를 마켓 3곳으로 잡으면 GitHub 직접 배포를 놓친다.
사용자가 주소를 주면 그건 대개 이 대장에 없는 것이다. 재조사 말고 여기에 추가할 것.
Remote Control 세션이면 /plugin 은 처음부터 못 쓴다. clone -> 내용 확인 -> 복사 순으로 간다.
**설치 전에 hooks/ scripts/ 를 먼저 본다.** 스킬은 내가 따르게 될 지시다
```
