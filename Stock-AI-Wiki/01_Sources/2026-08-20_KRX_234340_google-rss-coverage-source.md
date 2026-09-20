---
id: source-2026-08-20-KRX-234340-google-rss-coverage
type: source
title: KRX 234340 Google RSS Coverage Source
created: 2026-08-20
updated: 2026-08-20
status: raw
stage: 0

market: KRX
ticker: "234340"
company: 헥토파이낸셜
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 234340.
  key_facts:
    - code=234340
    - name=헥토파이낸셜
    - naver_article_count=3
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 234340
    - 헥토파이낸셜
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated coverage source note
---

# KRX 234340 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=234340`
- `name=헥토파이낸셜`
- `naver_article_count=3`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 234340.

## RSS Item Metadata
- Title: `간편결제 '쑥쑥'…헥토파이낸셜 수익성 '껑충' - 비즈워치`
- Source: `비즈워치`
- Published at: `2026-08-20T07:40:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE95WkUtWGJkaWxwc3pBM0V2MEpTYXIyM3dmX2Vma1VPX1Q4c2s4XzJxWjFIdDlob0xFNElzT21vSWFaNERIdlBmTEdWU0V6N2FKYkh0LXNQbEJQNDlOSnNSNFhJLUVISUhWb0E?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:37:31+09:00`
- Company: [[KRX_234340_헥토파이낸셜]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-20.json`
- Latest observation title: `간편결제 '쑥쑥'…헥토파이낸셜 수익성 '껑충' - 비즈워치`
- Latest observation source: `비즈워치`
- Latest observation published_at: `2026-08-20T07:40:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE95WkUtWGJkaWxwc3pBM0V2MEpTYXIyM3dmX2Vma1VPX1Q4c2s4XzJxWjFIdDlob0xFNElzT21vSWFaNERIdlBmTEdWU0V6N2FKYkh0LXNQbEJQNDlOSnNSNFhJLUVISUhWb0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
