# Current Logic Structural Audit — 2026-07-20

## Scope and fixed question

- Scope: read-only audit of the current normal and surge paths.
- Fixed question: where does the current logic lose candidates, and is the observed no-entry state caused by source generation, a risk policy, an execution rule, or a runtime artifact?
- Not in scope: strategy addition, threshold relaxation, Gate/LOCK/order/broker changes, or judging existing paper trade records as the answer.

## Evidence read

- `2_Logs/candidates_latest_meta.json` (generated `2026-07-20 09:55:59`)
- `2_Logs/candidates_latest_data.csv`, `candidates_latest_data.with_final_score.csv`, `candidates_latest_data.with_news_score.csv`
- `2_Logs/p1_entry_gate_status_latest.json`, `entry_decision_layers_runtime_latest.csv`, `pending_entry_status_latest.json`, `paper_order_validation_report_latest.json`
- `2_Logs/normal_entry_path_bottleneck_audit_latest.json` and `.csv`
- `2_Logs/surge_path_validation_outcome_latest.csv`, `surge_outcome_promotion_report_latest.csv`, `surge_ev_conditional_verdict_latest.csv`
- `generate_candidates_v41_1.py` and `paper_engine.py`

## Facts

### 1. Normal candidate generation

- The current candidate meta says `latest_date=2026-07-16`, `market_regime=BEAR`, and `stable_param_gate.ok=false` (`not_promoted`, `stable_score_low`). `official_use_allowed=false`.
- The base generator evaluated 2,607 rows. Its L0 through L6 attempts each produced `all_pass=0`.
- The base selector is an AND filter over relative strength, volume acceleration, stretch, value, ATR, RSI, volume/price correlation, 52-week-high distance, and listing age (`generate_candidates_v41_1.py`, `_select_candidates`).
- When generation is empty, the generator deliberately does not overwrite `candidates_latest_data.csv`. The currently retained file has 10 rows dated 2026-07-16. The later final-score file has 18 rows, including 8 rows without a date value.

### 2. Normal entry path

- P1 saw 6 candidates before and after its controls. It was `CAUTION` because of `BEAR macro caution`; the risk size multiplier was `0.25`, not zero.
- The entry-decision runtime snapshot classifies all 6 as `ALPHA_ELIGIBLE`.
- None became entry-ready. Four were held by `NORMAL_INTRADAY_MOMENTUM_BLOCK(v_accel<1.0000)` and two by `GAP_RISK_HISTORY_BLOCK`.
- `pending_entry_status_latest.json` records `entry_ready=0`, `candidates_after_caps=6`, and `pending_queue_len=0`.
- The current normal bottleneck report therefore identifies `NORMAL_MOMENTUM_RECHECK_REQUIRED`; no order was sent.

### 3. Surge path

- The current intraday path recorded 23 read-only path-observation rows; all have `paper_order_route=false` and `broker_order_route=false`.
- The aggregate surge promotion report has 33 regular-primary observations with mean primary return `-2.323182%`, median `-1.597015%`, and stop rate `78.7879%`. Its current verdict is `REJECT_POLICY_CHANGE`, not promotion.
- This is evidence against relaxing the present surge entry policy. It is not evidence that no future surge strategy can work.

### 4. Candidate-contract finding

- The generator records an unapproved stable-parameter gate in candidate meta, but inspected `paper_engine.py` code uses stable-parameter usability only for stop/hold/max-position overrides.
- At the candidate input boundary, inspected meta handling checks parameter ranges but does not check `stable_param_gate.ok` or `research_mode.official_use_allowed`.
- Runtime evidence is consistent with that gap: P1 processed six retained candidate rows despite `official_use_allowed=false`. No order occurred, but it was blocked later by separate execution rules.
- Therefore the absence of orders does **not** prove that the stable-parameter gate is enforced end-to-end at the paper-engine candidate-input boundary.

## Structural map

```text
price/OHLCV -> base AND selector -> retained candidate file when empty
             -> sector/news/final-score rows -> P1 CAUTION/size reduction
             -> alpha eligible rows -> gap/momentum execution checks -> HOLD
             -> pending entry ready=0 -> no order

intraday surge detector -> read-only path observation -> promotion evidence
                        -> currently not promotable -> no order route
```

## Interpretation

1. The normal path currently has two separate problems, which must not be blended:
   - fresh base generation produced zero rows under every relaxation level; and
   - retained candidates that reached the entry layer were all rejected by later execution checks.
2. The current no-entry state is therefore not explained by the BEAR risk state alone. Risk was reduced, not hard-blocked.
3. The present normal output cannot establish that the base strategy is bad. It establishes that the current selection-plus-execution chain does not currently produce an entry-ready candidate.
4. The surge result is negative for the tested current entry policy, so that policy should not be relaxed on the basis of the observed path-positive cases.
5. The stable-parameter boundary needs a separate contract audit before any claim that unapproved parameters are blocked throughout the paper-engine input path.

## Audit limits

- `entry_layer_input_trace_audit_latest.json` is dated 2026-06-25, so it was not used as same-cycle evidence for the 2026-07-20 conclusion.
- No historical filter-ablation replay was run in this audit. Consequently, this report does not yet determine whether the normal momentum or gap-history execution checks improve or worsen expected candidate quality.
- No operating code, configuration, policy, order, fill, ledger, or Gate was changed.

## Validation status for this audit

| Item | Status | Evidence |
|---|---|---|
| Functional | PASS | Latest normal/surge diagnostic artifacts parsed and current code paths were inspected. |
| Consistency | FAIL | A stale 2026-06-25 entry-source trace is still consumed by the normal bottleneck audit; the stable-gate boundary is not proven end-to-end. |
| Operational reflection | NA | Read-only audit; no operational change requested or made. |
| Policy | PASS | No threshold, Gate, LOCK, or route policy was changed. |
| FAIL-CLOSED | FAIL | No order was sent, but later execution blocks—not the unapproved stable gate—caused that result; input-boundary enforcement is not proven. |
| Regression | NA | No code changed and no regression run was required for this audit. |

## Exact next analysis, if continued

Run a same-cycle, read-only candidate-input contract trace: fresh base generation status -> retained/final-score row lineage -> P1 -> execution decision. Then test the momentum and gap-history filters as separate historical ablations against the same candidate population. No policy value is changed in either step.