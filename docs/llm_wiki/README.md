# RootA LLM Wiki

This folder is a read-only second-brain surface for `E:\1_Data`.

It is not a trading policy source and it must not be used to relax gates, locks,
STOP conditions, risk controls, scores, thresholds, orders, fills, ledger, or
stats.

## Structure

- `OBSIDIAN_START.md`: vault setup and start-page guide.
- `RUNBOOK.md`: manual refresh and source-ingest procedure.
- `SOURCE_FORMAT_CONTRACT.md`: accepted Markdown input contract.
- `AUTO_INGEST_PLAN.md`: future automation design and approval boundary.
- `wiki_index_latest.md`: generated top-level retrieval index.
- `00_Current_State/system_state_latest.md`: generated current-state snapshot.
- `00_Inbox`: manual Markdown drop zone for Slack, meetings, and docs.
- `01_Policies/roota_operating_contract.md`: links to canonical policy sources.
- `02_Sources`: dated source notes copied from `00_Inbox` by the inbox ingester.
- `05_Logs/latest_artifact_index.md`: generated index of source artifacts used
  by the snapshot.

## Refresh

Run:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\build_llm_wiki_snapshot.py
```

Or:

```powershell
E:\1_Data\run_llm_wiki_snapshot.bat
```

The generator reads RootA artifacts and writes Markdown only under
`E:\1_Data\docs\llm_wiki`.

## Inbox Ingest

Local export collect:

```powershell
E:\1_Data\run_llm_wiki_collect.bat
```

Dry-run:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\ingest_llm_wiki_inbox.py
```

Apply:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\ingest_llm_wiki_inbox.py --apply
```

Or:

```powershell
E:\1_Data\run_llm_wiki_ingest.bat
```

The ingester only copies Markdown files from `00_Inbox` to `02_Sources` and
writes a source index. It does not modify trading runtime artifacts.

## Obsidian

Open `E:\1_Data\docs\llm_wiki` as the vault folder and start from
`wiki_index_latest.md`. See `OBSIDIAN_START.md`.

## Runbook

Use `RUNBOOK.md` for the manual refresh and source-ingest procedure.

## Source Format

Use `SOURCE_FORMAT_CONTRACT.md` before adding Slack, meeting, or document notes.
Validate candidate notes with `tools\validate_llm_wiki_source_contract.py`.

## Auto Ingest

Use `AUTO_INGEST_PLAN.md` only as the design boundary for future automation.
The implemented collector is local-file only and does not use API/Webhook access.
