# Codex LLM Review Advisory Runbook

## 1. Boundary

This runbook is for manual advisory review only.

It does not run trading batches, does not call an LLM API directly, does not write operational outputs, and does not create a GitHub Actions workflow.

## 2. Inputs

Required files:

- `tools/ci/build_codex_review_context.py`
- `tools/ci/codex_review_prompt.md`
- `tools/ci/parse_codex_review_result.py`
- `docs/03-operations/codex_llm_code_review_ci_checklist.md`
- `docs/03-operations/codex_llm_code_review_ci_design.md`

Required state:

- Run from `E:\1_Data`
- Review only the current diff or an explicitly selected git diff ref
- Do not include secrets, API keys, credentials, or private tokens in pasted review context

## 3. Manual Advisory Flow

1. Build review context.

```powershell
& 'C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe' tools\ci\build_codex_review_context.py --max-diff-chars 60000 --max-context-file-chars 40000
```

2. Combine the prompt and context.

Use:

- Prompt: `tools/ci/codex_review_prompt.md`
- Context: output from step 1

3. Ask the LLM for advisory review.

The review output must include:

- Findings
- Facts
- Interpretation
- Validation matrix
- Untested areas
- Machine result JSON

4. Parse the review result in advisory mode.

```powershell
Get-Content review_output.md -Raw | & 'C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe' tools\ci\parse_codex_review_result.py
```

5. Parse the review result in phase-2 hard-fail mode only after advisory mode is stable.

```powershell
Get-Content review_output.md -Raw | & 'C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe' tools\ci\parse_codex_review_result.py --hard-fail-blocks
```

## 4. Expected Exit Codes

| Case | Advisory mode | Hard-fail mode |
| --- | --- | --- |
| `PASS` | `0` | `0` |
| `WARN` | `0` | `0` |
| `HARD_FAIL` | `0` | `2` |
| Parse error | `3` | `3` |

## 5. Evidence To Record

Record these items in the work report:

- Context command used
- Diff ref used, or note that the working tree diff was used
- LLM review output path, if saved
- Parser command used
- Parser JSON output
- Parser exit code
- Untested areas listed by the review

## 6. Stop Conditions

Stop and do not treat the review as usable if:

- `Machine result` JSON is missing
- Parser returns exit code `3`
- Review output omits the validation matrix
- Review claims runtime validation without evidence paths
- Review says complete, ready, fixed, problem-free, or operationally safe without all six validation items having PASS evidence

## 7. Current Non-Goals

Do not add these in the manual advisory phase:

- GitHub Actions workflow
- direct LLM API call
- PR comment writer
- auto-fix
- batch execution
- live order path
- operational JSON/log/CSV rewrite
