# Obsidian Start

Vault folder:

```text
E:\1_Data\docs\llm_wiki
```

Start page:

```text
wiki_index_latest.md
```

## Use

1. Open `E:\1_Data\docs\llm_wiki` as an Obsidian vault.
2. Open `wiki_index_latest.md` first.
3. Use `00_Inbox\templates` for manually exported Slack, meeting, or document notes.
4. Put exported notes into `00_Inbox\raw_slack`, `00_Inbox\raw_meetings`, or `00_Inbox\raw_docs`.
5. Run `E:\1_Data\run_llm_wiki_ingest.bat` to copy inbox notes into `02_Sources`.
6. Run `E:\1_Data\run_llm_wiki_snapshot.bat` to refresh current-state and artifact indexes.

## Boundary

- This vault is for LLM retrieval and human review.
- It is not a trading policy source.
- It does not approve trading.
- It must not be used to relax Gate, STOP, LOCK, risk, score, order, fill,
  ledger, or stats behavior.

