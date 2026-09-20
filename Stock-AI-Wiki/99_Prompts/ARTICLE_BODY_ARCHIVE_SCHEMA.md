# Article Body Archive Schema

## Purpose

This schema lets the note generator attach locally stored article text to source, news, and verification notes.

It does not approve source verification by itself.

## File Format

UTF-8 or UTF-8-SIG JSON.

Accepted top-level shapes:

```json
{
  "articles": []
}
```

or:

```json
[]
```

or:

```json
{
  "005930": {}
}
```

## Article Fields

```json
{
  "code": "005930",
  "title": "article title",
  "source": "publisher name",
  "published_at": "2026-05-18T09:00:00+09:00",
  "url": "https://example.com/article",
  "evidence_path": "E:\\1_Data\\Stock-AI-Wiki\\01_Sources\\archive\\005930.txt",
  "body": "full article text copied from the locally archived source"
}
```

## Required Fields

```text
code
body
```

Rows without both fields are ignored.

## Safety Rules

```yaml
verification:
  verified: false
  verification_status: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

The generator may set `original_text_available: true` when article body text exists, but it must not set `verified: true`.
