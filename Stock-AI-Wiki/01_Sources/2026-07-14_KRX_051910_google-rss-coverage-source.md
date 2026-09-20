---
id: source-2026-07-14-KRX-051910-google-rss-coverage
type: source
title: KRX 051910 Google RSS Coverage Source
created: 2026-07-14
updated: 2026-07-14
status: raw
stage: 0

market: KRX
ticker: "051910"
company: LG화학
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-14

analysis:
  summary: Local coverage report row shows news coverage for KRX 051910.
  key_facts:
    - code=051910
    - name=LG화학
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 051910
    - LG화학
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

# KRX 051910 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=051910`
- `name=LG화학`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 051910.

## RSS Item Metadata
- Title: `7월 엔지니어상에 김세현 LG화학 연구위원·최양일 이엠텍 대표 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-13T12:02:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5uUHpGckNVZmttVGxPWG5La1RJY0oyMVhhYjBjY2F4Vk5kRkMxZGw3dGMxY0tyckdRcFRrc3dzakswb0hOYmFCajQzV1czVHM?oc=5`

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
- Updated at: `2026-08-21T19:29:21+09:00`
- Company: [[KRX_051910_LG화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-14.json`
- Latest observation title: `7월 엔지니어상에 김세현 LG화학 연구위원·최양일 이엠텍 대표 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-13T12:02:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE9nQW0yT3lfdWcybUZpUWhkUzhzR2lra0JVSF9ndGNuNmM4SXREc0pOSUtqNVpKQUhBN000a0VLN25qcmQxQ2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
