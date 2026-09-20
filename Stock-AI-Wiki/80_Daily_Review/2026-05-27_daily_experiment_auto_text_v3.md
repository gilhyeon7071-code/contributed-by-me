---
date: 2026-05-27
project: trading-lab
context:
  D: 20260527
  regime: RISK
  risk_off: true

outcome_one_line: "RootB 상태 FAIL 유지, RootA 신선도는 HARD_FAIL (cand lag=4일)"
failing_signal: "[RootA] cand: HARD_FAIL (max_date=20260522 expected=20260526 lag=4) cand behind by 4 day(s)"
action_tomorrow: "cand/krx_clean/prices 기준일을 D(20260526)에 맞춰 갱신 후 freshness_source 재실행으로 HARD_FAIL 해소 여부 검증"

evidence:
  logs:
    - E:/1_Data/2_Logs/freshness_source_20260527_091019.json
    - E:/vibe/buffett/runs/observer_state_last.json
  outputs: []

scope_verdict: PASS   # PASS / FAIL / NA
ops_verdict: NA       # PASS / FAIL / NA

stop_conditions:
  orders_exec_present: NA
  exec_date_matches_D: NA
  asof_runid_match: NA
  paper_broker_date_mixed: NA

checks:
  functional: NA
  consistency: NA
  ops_reflect: NA
  policy: NA
  fail_closed: NA
  regression: NA

fact: "context auto: regime=RISK, risk_off=true, reason=roota_verdict=HARD_FAIL; rootb_status=FAIL"
interpretation: ""

tested:
  - ""
not_tested:
  - ""

tags: [daily, experiment, trading, obsidian, llm-wiki]
---

# Daily Experiment

## Outcome (One Line)

## Failing Signal

## Action Tomorrow
