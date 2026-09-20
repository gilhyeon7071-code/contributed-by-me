---
date: 2026-05-27
project: trading-lab
context:
  D: 20260527
  regime: RISK
  risk_off: true

outcome_one_line: "RootA 신선도 PASS, RootB 상태 FAIL"
failing_signal: "[RootB] DECISION_APPLY_FAIL: decision_apply.status=FAIL"
action_tomorrow: "observer_state_last 기준 reasons 우선순위 원인 제거 후 상태 재검증"

evidence:
  logs:
    - E:/1_Data/2_Logs/freshness_source_20260527_092425.json
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

fact: "context auto: regime=RISK, risk_off=true, reason=rootb_status=FAIL; expected_date=20260526, cand_max_date=20260526, cand_lag_days=0, krx_clean_lag_days=0, prices_lag_days=0, rootb_D=20260526, D_mismatch=false"
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
