# Sample Inbox To Source Flow

## Purpose

This note shows the Stage 0 manual flow.

```text
00_Inbox/SAMPLE_INBOX_TO_SOURCE.md
-> 01_Sources/SAMPLE_SOURCE.md
-> 30_News/SAMPLE_NEWS.md
-> 10_Companies/SAMPLE_COMPANY.md
```

## Inbox Capture

Paste unprocessed material here first.

```text
unknown
```

## Move To Source

Create or update a source note under:

```text
01_Sources/
```

Use:

```text
99_Prompts/templates/source.md
```

## Then Link Analysis Notes

Allowed Stage 0 links:

```text
[[SAMPLE_SOURCE]] -> [[SAMPLE_NEWS]]
[[SAMPLE_NEWS]] -> [[SAMPLE_COMPANY]]
```

## Trading Boundary

This flow does not create:

```text
buy decision
sell decision
candidate
signal
order
fill
ledger row
stats update
gate pass
```

