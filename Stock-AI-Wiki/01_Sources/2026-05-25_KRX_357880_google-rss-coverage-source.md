---
id: source-2026-05-25-KRX-357880-google-rss-coverage
type: source
title: KRX 357880 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "357880"
company: SKAI
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 357880.
  key_facts:
    - code=357880
    - name=SKAI
    - naver_article_count=0
    - google_rss_article_count=4
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 357880
    - SKAI
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

# KRX 357880 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=357880`
- `name=SKAI`
- `naver_article_count=0`
- `google_rss_article_count=4`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 357880.

## RSS Item Metadata
- Title: `빚으로 버티는 SKAI, 주가 회복에 450억 CB '승부수' - 딜사이트`
- Source: `딜사이트`
- Published at: `2026-04-21T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1GM1dPSmc2TVNGN3FtaTRFbzVqd3diSUJVMHBSOVZOLXpNYVNQTkNPRXkwODU0MmhKb0JIVk5ENkRRc0I5emY3N2NTM3c1Z2s?oc=5`

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
- Updated at: `2026-05-25T21:05:04+09:00`
- Company: [[KRX_357880_SKAI]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `빚으로 버티는 SKAI, 주가 회복에 450억 CB '승부수' - 딜사이트`
- Latest observation source: `딜사이트`
- Latest observation published_at: `2026-04-21T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1GM1dPSmc2TVNGN3FtaTRFbzVqd3diSUJVMHBSOVZOLXpNYVNQTkNPRXkwODU0MmhKb0JIVk5ENkRRc0I5emY3N2NTM3c1Z2s?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
