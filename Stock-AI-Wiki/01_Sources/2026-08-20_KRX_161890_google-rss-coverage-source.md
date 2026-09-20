---
id: source-2026-08-20-KRX-161890-google-rss-coverage
type: source
title: KRX 161890 Google RSS Coverage Source
created: 2026-08-20
updated: 2026-08-20
status: raw
stage: 0

market: KRX
ticker: "161890"
company: 한국콜마
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 161890.
  key_facts:
    - code=161890
    - name=한국콜마
    - naver_article_count=5
    - google_rss_article_count=3
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 161890
    - 한국콜마
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

# KRX 161890 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=161890`
- `name=한국콜마`
- `naver_article_count=5`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 161890.

## RSS Item Metadata
- Title: `[K-뷰티를 이끄는 주역] ⑤한국콜마, 인디브랜드 글로벌 진출에 날개…영업익 1000억 돌파 - 뉴스포스트`
- Source: `뉴스포스트`
- Published at: `2026-08-20T09:35:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9IZS1sNlkyRDFIbENaVlBQaF9ZUTI2UElZdmdFOUZrc1RKSWlUOUdvZXNKMXM0Y0Z5Z0g4c1pUWWRmQWViNG1zakhTUEJzTHdoS0c1VWFWLVdmalpXbUticDNqN0RBZktUcWc?oc=5`

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
- Company: [[KRX_161890_한국콜마]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-20.json`
- Latest observation title: `[K-뷰티를 이끄는 주역] ⑤한국콜마, 인디브랜드 글로벌 진출에 날개…영업익 1000억 돌파 - 뉴스포스트`
- Latest observation source: `뉴스포스트`
- Latest observation published_at: `2026-08-20T09:35:36+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9IZS1sNlkyRDFIbENaVlBQaF9ZUTI2UElZdmdFOUZrc1RKSWlUOUdvZXNKMXM0Y0Z5Z0g4c1pUWWRmQWViNG1zakhTUEJzTHdoS0c1VWFWLVdmalpXbUticDNqN0RBZktUcWc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
