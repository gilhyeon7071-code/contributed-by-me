# Auto Ingest Plan

## Scope

This is a design plan for future automatic ingestion into the LLM Wiki.

No API, webhook, scheduler, token, credential, trading runtime, Gate, STOP,
LOCK, risk, score, order, fill, ledger, or stats path is changed by this plan.

## Current Manual Baseline

- Inbox: `00_Inbox`
- Source contract: `SOURCE_FORMAT_CONTRACT.md`
- Local export collector: `E:\1_Data\tools\collect_llm_wiki_exports.py`
- Local collector runner: `E:\1_Data\run_llm_wiki_collect.bat`
- Manual ingester: `E:\1_Data\tools\ingest_llm_wiki_inbox.py`
- Manual runner: `E:\1_Data\run_llm_wiki_ingest.bat`
- Source index: `02_Sources\source_index_latest.md`

## Proposed Flow

```text
Slack export or meeting transcript
  -> local collector writes Markdown under 00_Inbox\raw_*
  -> ingest_llm_wiki_inbox.py --apply
  -> 02_Sources dated copy
  -> source_index_latest.md
  -> build_llm_wiki_snapshot.py
  -> wiki_index_latest.md
```

## Input Contract

Every automatic collector must write Markdown that follows
`SOURCE_FORMAT_CONTRACT.md`.

Minimum metadata:

- title: first `# Heading`
- date
- channel
- tags

## Safety Boundary

Automatic collection may write only under:

- `E:\1_Data\docs\llm_wiki\00_Inbox`

Automatic ingestion may write only under:

- `E:\1_Data\docs\llm_wiki\02_Sources`
- `E:\1_Data\docs\llm_wiki\02_Sources\source_index_latest.md`

Snapshot refresh may write only under:

- `E:\1_Data\docs\llm_wiki`

It must not write to:

- `paper`
- `2_Logs`, except if a future explicit audit artifact is separately approved
- `state`
- `live`
- `config`
- scheduler definitions
- trading code paths

## Approval Gates

Separate explicit approval is required before any of these:

- Slack API or webhook connection
- meeting transcription automation
- credential storage
- scheduler registration
- background service
- network access
- automatic deletion or movement of source files

No approval is required for local dry-run collection from
`00_Inbox\import_exports`, because it does not use network access or
credentials.

## Validation Required Before Automation

1. Sample export validation with at least one real Slack or meeting Markdown.
2. Dry-run ingestion result with expected metadata.
3. Apply ingestion result with copied file and source index.
4. Cleanup or retention rule confirmation.
5. Policy boundary check: no Gate, STOP, LOCK, risk, score, order, fill,
   ledger, or stats changes.

## Readiness Probe

Before requesting external API or scheduler approval, run:

```powershell
E:\1_Data\run_llm_wiki_readiness.bat
```

This writes:

```text
E:\1_Data\docs\llm_wiki\05_Logs\external_readiness_latest.json
```

The probe is read-only. It checks local paths, expected environment-variable
presence, and whether the scheduler task name is already registered. It does
not create credentials, call Slack, call meeting APIs, or register a scheduler.
