---
id: source-2026-08-09-KRX-125490-google-rss-coverage
type: source
title: KRX 125490 Google RSS Coverage Source
created: 2026-08-09
updated: 2026-08-09
status: raw
stage: 0

market: KRX
ticker: "125490"
company: 한라캐스트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 125490.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 125490
    - 한라캐스트
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

# KRX 125490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 125490.

## RSS Item Metadata
- Title: `[리포트 브리핑]한라캐스트, '점점 더 커지는 존재감' Not Rated - SK증권 - newspim.com`
- Source: `newspim.com`
- Published at: `2026-08-06T10:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5BSVVPUGh0NFo0eWh0bDVIZ1VrV1dHMGJJMU9MMVFYTk1IZmJSYmtla1pQRWg0b3gyUzQ1OEx5LTRlYktKQmVmU2pLRmJkYUhRZGZiRnJ1TUd0aDRu?oc=5`

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
- Updated at: `2026-08-21T19:34:30+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-09.json`
- Latest observation title: `[리포트 브리핑]한라캐스트, '점점 더 커지는 존재감' Not Rated - SK증권 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-08-06T10:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5BSVVPUGh0NFo0eWh0bDVIZ1VrV1dHMGJJMU9MMVFYTk1IZmJSYmtla1pQRWg0b3gyUzQ1OEx5LTRlYktKQmVmU2pLRmJkYUhRZGZiRnJ1TUd0aDRu?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
