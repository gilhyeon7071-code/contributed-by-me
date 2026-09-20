# Obsidian Daily Experiment YAML Template (RootA 운영계약 반영)

## 1) 고정 YAML 템플릿
```yaml
---
date: 2026-05-26
project: trading-lab
context:
  D: 20260526
  regime: BULL
  risk_off: false

outcome_one_line: ""
failing_signal: ""
action_tomorrow: ""

evidence:
  logs: []
  outputs: []

# 일일 실험 범위 판정(요약용)
scope_verdict: PASS   # PASS / FAIL / NA

# 운영 판정(6개 검증 + 런타임 증거 기반)
ops_verdict: NA       # PASS / FAIL / NA

# STOP 조건 점검(혼용 금지)
stop_conditions:
  orders_exec_present: NA      # PASS / FAIL / NA
  exec_date_matches_D: NA      # PASS / FAIL / NA
  asof_runid_match: NA         # PASS / FAIL / NA
  paper_broker_date_mixed: NA  # PASS / FAIL / NA (혼용이면 FAIL)

# 필수 6개 검증 항목
checks:
  functional: NA      # PASS / FAIL / NA
  consistency: NA     # PASS / FAIL / NA
  ops_reflect: NA     # PASS / FAIL / NA
  policy: NA          # PASS / FAIL / NA
  fail_closed: NA     # PASS / FAIL / NA
  regression: NA      # PASS / FAIL / NA

# 사실/해석 분리
fact: ""
interpretation: ""

# 어떤 테스트를 했고/안 했는지 명시
tested:
  - ""
not_tested:
  - ""

tags: [daily, experiment, trading, obsidian, llm-wiki]
---
```

## 2) Obsidian Templater용
```yaml
---
date: <% tp.date.now("YYYY-MM-DD") %>
project: trading-lab
context:
  D: <% tp.system.prompt("D(YYYYMMDD)") %>
  regime: <% tp.system.prompt("regime", "UNKNOWN") %>
  risk_off: <% tp.system.prompt("risk_off(true/false)", "false") %>

outcome_one_line: "<% tp.system.prompt("오늘 한 줄 결과") %>"
failing_signal: "<% tp.system.prompt("가장 큰 실패 신호") %>"
action_tomorrow: "<% tp.system.prompt("내일 액션 1개") %>"

evidence:
  logs: ["E:/1_Data/2_Logs/...", "E:/vibe/buffett/..."]
  outputs: ["..."]

scope_verdict: <% tp.system.prompt("scope_verdict(PASS/FAIL/NA)", "PASS") %>
ops_verdict: <% tp.system.prompt("ops_verdict(PASS/FAIL/NA)", "NA") %>

stop_conditions:
  orders_exec_present: <% tp.system.prompt("orders_exec_present(PASS/FAIL/NA)", "NA") %>
  exec_date_matches_D: <% tp.system.prompt("exec_date_matches_D(PASS/FAIL/NA)", "NA") %>
  asof_runid_match: <% tp.system.prompt("asof_runid_match(PASS/FAIL/NA)", "NA") %>
  paper_broker_date_mixed: <% tp.system.prompt("paper_broker_date_mixed(PASS/FAIL/NA)", "NA") %>

checks:
  functional: <% tp.system.prompt("functional(PASS/FAIL/NA)", "NA") %>
  consistency: <% tp.system.prompt("consistency(PASS/FAIL/NA)", "NA") %>
  ops_reflect: <% tp.system.prompt("ops_reflect(PASS/FAIL/NA)", "NA") %>
  policy: <% tp.system.prompt("policy(PASS/FAIL/NA)", "NA") %>
  fail_closed: <% tp.system.prompt("fail_closed(PASS/FAIL/NA)", "NA") %>
  regression: <% tp.system.prompt("regression(PASS/FAIL/NA)", "NA") %>

fact: "<% tp.system.prompt("사실(증거 기반)") %>"
interpretation: "<% tp.system.prompt("해석") %>"

tested:
  - "<% tp.system.prompt("테스트한 것 1") %>"
not_tested:
  - "<% tp.system.prompt("테스트하지 않은 것 1") %>"

tags: [daily, experiment, trading, llm-wiki]
---
```
