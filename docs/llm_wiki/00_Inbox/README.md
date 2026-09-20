# LLM Wiki Inbox

Put manually exported Markdown notes here before ingestion.

Accepted source folders:

- `raw_slack`
- `raw_meetings`
- `raw_docs`

Use the templates under `templates` for consistent metadata.

Recognized metadata fields:

- `title`: taken from the first `# Heading` if omitted.
- `date`: source date, for example `2026-05-18`.
- `channel`: Slack channel, meeting label, or document source.
- `tags`: plain text tags such as `#policy #fills`.

The ingestion script copies Markdown files into dated read-only collections
under `docs\llm_wiki\02_Sources`.

This inbox is not a trading policy source and must not be used to change Gate,
STOP, LOCK, risk, score, order, fill, ledger, or stats behavior.
