# Dashboard Evidence Audit

Use this lane when the dashboard, verification page, forensic actions, or displayed status is in scope.

## Active path

Treat RootB as one dashboard system:

```text
E:\1_Data source artifacts
  -> E:\vibe\buffett\tools\build_dashboard_state_v2.py
  -> E:\vibe\buffett\runs\dashboard_state_latest.json
  -> E:\vibe\control_center_v2 public/dist data
  -> E:\vibe\control_center_v2 React UI
```

The active verification UI includes:

- `src\views\VerifyView.tsx`, which reads `trading_stage_validation_latest.json`
- `src\views\ForensicView.tsx`, which exposes evidence actions for order integrity, fills-ledger reconciliation, and chain replay
- the Vite verification endpoint and its evidence-id allowlist

Confirm paths and code again because UI wiring can change.

## Verification layers

Check all applicable layers separately:

1. RootA validator actually ran.
2. RootA artifact exists and contains the expected D, `as_of`, `run_id`, checks, and provenance.
3. RootB state builder reads that exact artifact and preserves FAIL/NA semantics.
4. copied `runs`, `public`, and `dist` JSON files are current and equivalent where required.
5. the React view reads the intended field and does not convert missing, empty, zero-row, stale, or parsing errors into PASS.
6. evidence buttons invoke only allowlisted actions and display actual results.
7. the rendered screen matches the source JSON.

## Required distinctions

- A visible card proves rendering, not RootA execution.
- HTTP 200 proves serving, not data correctness.
- JSON freshness by mtime does not prove its internal D is current.
- Zero missing rows can mean success or an empty/misaligned comparison; verify denominators and source windows.
- A UI wording correction is not an underlying logic repair.

Include at least one worst case, such as a stale prior PASS remaining visible after the current RootA validator fails or produces no artifact.

When RootB code or state generation is changed, record the work in `E:\vibe\buffett\PLANS.md` even if the UI file lives in `control_center_v2`. Rendered verification is required before claiming the screen is correct.
