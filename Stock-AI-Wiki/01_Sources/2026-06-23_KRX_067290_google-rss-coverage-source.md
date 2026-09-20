---
id: source-2026-06-23-KRX-067290-google-rss-coverage
type: source
title: KRX 067290 Google RSS Coverage Source
created: 2026-06-23
updated: 2026-06-23
status: raw
stage: 0

market: KRX
ticker: "067290"
company: JW신약
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-23

analysis:
  summary: Local coverage report row shows news coverage for KRX 067290.
  key_facts:
    - code=067290
    - name=JW신약
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 067290
    - JW신약
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

# KRX 067290 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=067290`
- `name=JW신약`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 067290.

## RSS Item Metadata
- Title: `탈모 급여 언급되자 관련주 '고공행진'…JW신약·현대약품·위더스 급등 - 메디팜스투데이`
- Source: `메디팜스투데이`
- Published at: `2026-06-23T12:19:12+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9FaUlCODlFaE5NbkpMbS1CcENYcFJMRkJYQXV0MGxnSHc0VDZ5bEpTdmh3WEVqdWlubVBoV0VhWm5qaHc1VFZXZmtSS0IwdUJRZ2ZPS3hRZ2VwYnRkYnZwUUZ3cUE3WjRY?oc=5`

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
- Updated at: `2026-08-21T19:23:39+09:00`
- Company: [[KRX_067290_JW신약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-23.json`
- Latest observation title: `탈모 급여 언급되자 관련주 '고공행진'…JW신약·현대약품·위더스 급등 - 메디팜스투데이`
- Latest observation source: `메디팜스투데이`
- Latest observation published_at: `2026-06-23T12:19:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9FaUlCODlFaE5NbkpMbS1CcENYcFJMRkJYQXV0MGxnSHc0VDZ5bEpTdmh3WEVqdWlubVBoV0VhWm5qaHc1VFZXZmtSS0IwdUJRZ2ZPS3hRZ2VwYnRkYnZwUUZ3cUE3WjRY?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
