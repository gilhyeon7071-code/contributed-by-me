# LLM Wiki Manual Runbook

## Scope

This runbook is for manual RootA LLM Wiki refresh and source-note ingestion.

It does not change trading policy, scheduler actions, Gate, STOP, LOCK, risk,
score, order, fill, ledger, or stats behavior.

## Daily Manual Refresh

Preferred one-command local refresh:

```powershell
E:\1_Data\run_llm_wiki_pipeline.bat
```

This runs local export collection, source contract validation, source ingest,
and current-state snapshot generation in order.

## Automatic Refresh

Scheduler dry-run:

```powershell
E:\1_Data\run_llm_wiki_register_scheduler_dryrun.bat
```

Scheduler apply:

```powershell
E:\1_Data\run_llm_wiki_register_scheduler_apply.bat
```

The task name is `VIBE_LLM_Wiki_Pipeline`. It runs the local pipeline hourly.
It does not call Slack, create credentials, or change trading runtime behavior.

Manual step-by-step refresh:

1. Run the current-state snapshot:

```powershell
E:\1_Data\run_llm_wiki_snapshot.bat
```

2. Open:

```text
E:\1_Data\docs\llm_wiki\wiki_index_latest.md
```

3. Confirm the key fields:

- `D`
- `orders_exec_exists`
- `asof_matches_D`
- `intraday_steps`

4. Treat `orders_exec_exists=false` or `asof_matches_D=false` as a STOP concern
   for any order/fill/ledger/stat interpretation.

## Manual Source Ingest

Optional local export collection:

```powershell
E:\1_Data\run_llm_wiki_collect.bat
```

1. Create a note from one of:

- `00_Inbox\templates\slack_note.md`
- `00_Inbox\templates\meeting_note.md`
- `00_Inbox\templates\doc_note.md`

   Follow `SOURCE_FORMAT_CONTRACT.md`.

2. Save the note into one of:

- `00_Inbox\raw_slack`
- `00_Inbox\raw_meetings`
- `00_Inbox\raw_docs`

3. Dry-run:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\ingest_llm_wiki_inbox.py
```

4. Validate source contract:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\validate_llm_wiki_source_contract.py
```

5. Apply:

```powershell
E:\1_Data\run_llm_wiki_ingest.bat
```

6. Check:

```text
E:\1_Data\docs\llm_wiki\02_Sources\source_index_latest.md
```

## LLM Retrieval Use

Provide only the relevant files to the LLM:

- `wiki_index_latest.md`
- `00_Current_State\system_state_latest.md`
- `02_Sources\source_index_latest.md`
- selected files under `02_Sources`
- `01_Policies\roota_operating_contract.md` when policy context matters

Do not provide all logs by default.

## Stop Conditions

Stop and inspect canonical RootA artifacts before interpretation when:

- `orders_exec_exists=false`
- `asof_matches_D=false`
- source notes imply a policy change
- source notes imply Gate, STOP, LOCK, risk, score, order, fill, ledger, or stats changes
