# Source Format Contract

## Scope

This contract defines Markdown input accepted by the LLM Wiki inbox.

It is for source-note retrieval only. It does not change trading policy, Gate,
STOP, LOCK, risk, score, order, fill, ledger, or stats behavior.

## Accepted Folders

| folder | source_type |
|---|---|
| `00_Inbox\raw_slack` | `slack` |
| `00_Inbox\raw_meetings` | `meetings` |
| `00_Inbox\raw_docs` | `docs` |

## Required Shape

Each source note must be a Markdown file.

Use this header shape:

```markdown
# Note Title

date: 2026-05-18
channel: trading-system
tags: #policy #fills
```

## Recognized Metadata

| field | required | note |
|---|---|---|
| `title` | yes | taken from the first `# Heading` |
| `date` | recommended | source date, `YYYY-MM-DD` preferred |
| `channel` | recommended | Slack channel, meeting label, or document source |
| `tags` | optional | plain text tags |

Unknown fields are preserved in the note body but are not indexed.

## Output Contract

The ingester copies each source note to:

```text
02_Sources\<source_type>\<YYYY-MM-DD>\<source_stem>.<sha12>.md
```

It writes the latest index to:

```text
02_Sources\source_index_latest.md
```

Indexed fields:

- `source_type`
- `date`
- `channel`
- `title`
- `tags`
- `source_path`
- `ingested_path`
- `sha256`
- `bytes`

## Validation Command

Validate one file:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\validate_llm_wiki_source_contract.py E:\1_Data\docs\llm_wiki\00_Inbox\examples\slack_contract_valid.md
```

Validate current raw inbox files:

```powershell
C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe E:\1_Data\tools\validate_llm_wiki_source_contract.py
```

## Stop Conditions

Do not treat a source note as a policy change when:

- it requests Gate, STOP, LOCK, risk, score, order, fill, ledger, or stats changes
- it conflicts with `AGENTS.md`
- it conflicts with `01_Policies\roota_operating_contract.md`
- it lacks evidence paths for an operational claim

Those cases require separate review before any implementation.
