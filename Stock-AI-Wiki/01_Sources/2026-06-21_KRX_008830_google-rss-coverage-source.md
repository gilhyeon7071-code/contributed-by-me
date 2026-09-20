---
id: source-2026-06-21-KRX-008830-google-rss-coverage
type: source
title: KRX 008830 Google RSS Coverage Source
created: 2026-06-21
updated: 2026-06-21
status: raw
stage: 0

market: KRX
ticker: "008830"
company: 대동기어
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 008830.
  key_facts:
    - code=008830
    - name=대동기어
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 008830
    - 대동기어
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

# KRX 008830 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=008830`
- `name=대동기어`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 008830.

## RSS Item Metadata
- Title: `대동, 대동기어 유증 명분 세우고 돈 아꼈다 : 네이버 블로그 - Naver Blog`
- Source: `Naver Blog`
- Published at: `2026-06-20T18:45:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMijwFBVV95cUxOSXNoc0JtcjVOdVVIRjBZbzFfN2lUU1JVekdiTUthd0JUNnRJUkNieFNDcEJzNWxLbUdHcEc4bHB2b3o3VzdWNnNaeTM3S1luU0UzelBObkY2NnBZdjd4WFBVTmNHcXhEb21MdTY3NHhmZ3BsMXdkYWVDSHprM3RIV2RDbER5VW54MkJRajh5dw?oc=5`

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
- Updated at: `2026-06-21T13:12:44+09:00`
- Company: [[KRX_008830_대동기어]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-21.json`
- Latest observation title: `대동, 대동기어 유증 명분 세우고 돈 아꼈다 : 네이버 블로그 - Naver Blog`
- Latest observation source: `Naver Blog`
- Latest observation published_at: `2026-06-21T00:45:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMijwFBVV95cUxOSXNoc0JtcjVOdVVIRjBZbzFfN2lUU1JVekdiTUthd0JUNnRJUkNieFNDcEJzNWxLbUdHcEc4bHB2b3o3VzdWNnNaeTM3S1luU0UzelBObkY2NnBZdjd4WFBVTmNHcXhEb21MdTY3NHhmZ3BsMXdkYWVDSHprM3RIV2RDbER5VW54MkJRajh5dw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
