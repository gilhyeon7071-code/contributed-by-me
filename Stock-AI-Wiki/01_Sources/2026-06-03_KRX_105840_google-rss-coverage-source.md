---
id: source-2026-06-03-KRX-105840-google-rss-coverage
type: source
title: KRX 105840 Google RSS Coverage Source
created: 2026-06-03
updated: 2026-06-03
status: raw
stage: 0

market: KRX
ticker: "105840"
company: 우진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-03

analysis:
  summary: Local coverage report row shows news coverage for KRX 105840.
  key_facts:
    - code=105840
    - name=우진
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 105840
    - 우진
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

# KRX 105840 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=105840`
- `name=우진`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 105840.

## RSS Item Metadata
- Title: `LNGSHOT 커뮤니티 포스트 - LA to NYC SHAWTY to B - Weverse`
- Source: `Weverse`
- Published at: `2026-05-29T21:54:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBLdWJjNUgtMWdfX1NWemtOZXNfTFlIbnE5bWJIb2s2Qkh6Z3dRYU5Xb1RTd3ZGMGEzcEZ0ckZUaWdua09VT1N1NFdIZlJ6WWNOMzhiaUpnNnFnaHlOVmw3Wg?oc=5`

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
- Updated at: `2026-08-21T19:17:43+09:00`
- Company: [[KRX_105840_우진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-03.json`
- Latest observation title: `우진문화재단 ‘젊은춤판’, 강동혁-정승준-한대교 무대로 - 전북도민일보`
- Latest observation source: `전북도민일보`
- Latest observation published_at: `2026-06-03T15:10:08+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5iUkpNYTJJZmRSaGRLbWdMX3V3TXBFWkZvTUIwM2MtMnJ5dU9RaHdNaEctMHFveFRWSFNTNWtVZ2JRVVpiTWV3SXZTSGllWW9kZENXWkpJdzA5eFNnVW1naktlYkdZcHYyUnJN?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
