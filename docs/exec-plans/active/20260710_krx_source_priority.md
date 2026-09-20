# 2026-07-10 KRX Source Priority Correction

## Scope

- Make `_krx_manual` corrections win over archive/root rows for the same `(code, date)`.
- Keep archive rows as coverage fallback where manual rows do not exist.
- Apply identical source priority to candidate, report, and HPO loaders.

## Evidence

- Recent 120-day overlap: 103,911 keys; price differences above 25%: 307; above 2x: 226.
- Example `2026-05-20/000150`: archive close 7,140 vs manual close 1,478,000.
- Mtime-only selection chose archive when archive sync ran later, creating +22,309% false forward returns.

## Backup

- `E:\1_Data\backup\20260710_krx_source_priority\20260710_100000\`

## Validation

1. Syntax compile all three loaders.
2. Confirm loader row/key counts remain aligned.
3. Recompute adjacent-session outlier counts.
4. Regenerate candidate and h1/h2/h5 diagnostics.
5. Confirm parameter gate and observe-only behavior remain unchanged.
## Result
- Explicit manual/archive/root priority is applied consistently in candidate, report, and HPO loaders.
- The source-switch distortion decreased but did not disappear.
- Remaining price-scale and forward-horizon defects block policy-grade signal validation.
- Regime/strategy/signal policy changes were not applied.