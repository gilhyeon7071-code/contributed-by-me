---
id: source-2026-07-16-KRX-179530-google-rss-coverage
type: source
title: KRX 179530 Google RSS Coverage Source
created: 2026-07-16
updated: 2026-07-16
status: raw
stage: 0

market: KRX
ticker: "179530"
company: 애드바이오텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-16

analysis:
  summary: Local coverage report row shows news coverage for KRX 179530.
  key_facts:
    - code=179530
    - name=애드바이오텍
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 179530
    - 애드바이오텍
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

# KRX 179530 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=179530`
- `name=애드바이오텍`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 179530.

## RSS Item Metadata
- Title: `[IB토마토]애드바이오텍, 적자 커지는데…R&D 줄이고 100억 지분투자 - 뉴스토마토`
- Source: `뉴스토마토`
- Published at: `2026-07-13T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBLU28yZHMyNlMtMDJnTVNXajJZeERabGZfMm9HNFFCY085Wk1wTFNEQVlvYWpjWDZoYmhmR3p4eDJtcE11dm9nUF9LQWZONEZwaS1pVDRoUEoybmVPTjhTQw?oc=5`

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
- Updated at: `2026-08-21T19:29:40+09:00`
- Company: [[KRX_179530_애드바이오텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-16.json`
- Latest observation title: `[IB토마토]애드바이오텍, 적자 커지는데…R&D 줄이고 100억 지분투자 - 뉴스토마토`
- Latest observation source: `뉴스토마토`
- Latest observation published_at: `2026-07-13T06:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBLU28yZHMyNlMtMDJnTVNXajJZeERabGZfMm9HNFFCY085Wk1wTFNEQVlvYWpjWDZoYmhmR3p4eDJtcE11dm9nUF9LQWZONEZwaS1pVDRoUEoybmVPTjhTQw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
