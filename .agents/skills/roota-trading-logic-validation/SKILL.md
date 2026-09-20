---
name: roota-trading-logic-validation
description: Audit RootA stock-trading logic for requirement-to-code fit, edge cases, runtime orders-to-stats integrity, and dashboard evidence. Use for implementation suitability, logic validation, E2E verification, signal-stage audits, or PASS/FAIL readiness judgments in E:\1_Data; default to read-only unless the user explicitly requests fixes.
---

# RootA Trading Logic Validation

Validate the trading system without treating existing code, Gate results, or dashboard labels as the answer.

## Start

1. Read `E:\1_Data\AGENTS.md` and obey its scope, STOP, backup, and completion rules.
2. Read `.agent\PLANS.md`, `docs\references\SYSTEM_DEFINITION.md`, and `docs\references\OBJECTIVE_LEDGER.md` before deciding what the current target is.
3. Read `docs\references\STRATEGY_VALIDATION_GUARD.md` before strategy, candidate, regime, signal, or execution-hypothesis validation.
4. For any research measurement or comparison, also read `docs\references\NEW_SIGNAL_VALIDATION_STANDARD.md` and `docs\references\METRIC_CITATION_PROTOCOL.md` before measuring.
5. If the user invokes a validation alias, use the path mapping in `AGENTS.md`; revised aliases also require `E:\TMP\검증프롬프트 세부\공통_운영감사_기준.txt`.

Start reports with:

```text
가드 읽음: 예/해당 없음
현재 고정 목표: ...
탐색/확증 라운드: EXPLORATION / CONFIRMATION / NA
방향 변경 여부: ...
이번 검증 범위: ...
운영 변경: 없음 / 있음(사용자 승인 범위)
```

## Select the validation lanes

- For policy completeness, target ambiguity, stage causality, or definition conflicts, read [contract and scope](references/contract-and-scope.md).
- For implementation suitability, source tracing, edge cases, complexity, or tests, read [code logic audit](references/code-logic-audit.md).
- For orders, fills, ledger, stats, dates, runtime, batches, or operational readiness, read [runtime E2E audit](references/runtime-e2e-audit.md).
- For verification UI, dashboard JSON, evidence buttons, or displayed PASS/FAIL, read [dashboard evidence audit](references/dashboard-evidence-audit.md).

Use every lane needed by the request, but do not expand into unrelated strategy research or repairs.

## Non-negotiable distinctions

- Separate facts from interpretation.
- Separate retired or experimental selection logic, research candidates, preserved operating plumbing, and the explicitly approved operational target.
- A document labeled `final` is not operational approval by itself. Reconcile it with the objective ledger, PLANS, current runtime, and the user's explicit decision. If they conflict, report `TARGET_CONTRACT_CONFLICT` and do not assume a replacement.
- Code presence, unit-test success, batch `rc=0`, JSON creation, HTTP 200, or dashboard PASS each proves only one layer.
- Dashboard output never overrides RootA source evidence.
- Candidate exclusion and buy-candidate discovery are separate judgments. For surge candidates, classify `매수 가능`, `탐색 후보`, and `진짜 제외` separately.
- Do not convert insufficient evidence into PASS or FAIL. Use `NA` or the registered deferred state.

## Worst-case requirement

Every audit must exercise or explicitly reason through at least one concrete worst case. Prefer the failure most capable of producing an unintended order, duplicate fill, stale decision, silent data loss, or false PASS. Record its input, expected fail-closed behavior, observed evidence, and verdict.

## Modification boundary

Default to read-only. A request to validate does not authorize fixes.

If the user explicitly requests a fix:

1. Identify the actual writer and affected consumers.
2. Back up every existing file before editing under the backup path required by `AGENTS.md`; new files with no predecessor may be `backup: NA`.
3. Classify the change as defect repair, policy change, display change, or research-only work.
4. Make the smallest approved change. Never relax Gate, LOCK, risk controls, scores, thresholds, or FAIL-CLOSED behavior without explicit approval.
5. Re-read the changed source, then perform syntax, execution, and artifact validation. Add runtime E2E and rendered UI checks when those layers are affected.
6. Revert and verify any temporary test values.

## Definition of done

Report each finding in this order:

```text
발견된 문제점 -> 발생 원인 -> 구체적인 해결책 -> 검증 증거 -> 판정
```

Use `docs\references\FINAL_REPORT_TEMPLATE.md`. Always report functional, consistency, operational reflection, policy, FAIL-CLOSED, and regression as `PASS / FAIL / NA` with evidence paths. If any item is FAIL or NA, do not say the system is complete, fixed, ready, or operational.

End in this order:

```text
지시사항 / 진행된 것 / 남은 것 / 다음 진행 / 다음 보고 기준
```
