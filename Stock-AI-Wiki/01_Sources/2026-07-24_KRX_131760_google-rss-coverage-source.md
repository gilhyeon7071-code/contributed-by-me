---
id: source-2026-07-24-KRX-131760-google-rss-coverage
type: source
title: KRX 131760 Google RSS Coverage Source
created: 2026-07-24
updated: 2026-07-24
status: raw
stage: 0

market: KRX
ticker: "131760"
company: 파인텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-24

analysis:
  summary: Local coverage report row shows news coverage for KRX 131760.
  key_facts:
    - code=131760
    - name=파인텍
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 131760
    - 파인텍
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

# KRX 131760 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=131760`
- `name=파인텍`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 131760.

## RSS Item Metadata
- Title: `파인텍, 195억 규모 OLED 제조장비 공급 계약 체결 - 마켓인`
- Source: `마켓인`
- Published at: `2026-07-24T14:24:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBUR0pKV0pfX0F2Z2tyY2FTeVE5eWE0dG0wcjl0eEExNzNTd1BCZkljQ2JMZTlxZms4VlJDNWVZM19aTk44Qm43Zm1tOHgzYWtFS0xNTDJ6bG9JcTQ5MzgxOUFWQ2lNZlRwZDdXTTlaV2p1WE0?oc=5`

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
- Updated at: `2026-08-21T19:31:16+09:00`
- Company: [[KRX_131760_파인텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-24.json`
- Latest observation title: `파인텍, 195억 규모 OLED 제조장비 공급 계약 체결 - 마켓인`
- Latest observation source: `마켓인`
- Latest observation published_at: `2026-07-24T14:24:58+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBUR0pKV0pfX0F2Z2tyY2FTeVE5eWE0dG0wcjl0eEExNzNTd1BCZkljQ2JMZTlxZms4VlJDNWVZM19aTk44Qm43Zm1tOHgzYWtFS0xNTDJ6bG9JcTQ5MzgxOUFWQ2lNZlRwZDdXTTlaV2p1WE0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
