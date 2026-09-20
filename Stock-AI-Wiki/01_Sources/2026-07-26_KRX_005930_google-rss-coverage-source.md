---
id: source-2026-07-26-KRX-005930-google-rss-coverage
type: source
title: KRX 005930 Google RSS Coverage Source
created: 2026-07-26
updated: 2026-07-26
status: raw
stage: 0

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 005930.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=5
    - google_rss_article_count=148
    - kis_title_count=81
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005930`
- `name=삼성전자`
- `naver_article_count=5`
- `google_rss_article_count=148`
- `kis_title_count=81`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 005930.

## RSS Item Metadata
- Title: `삼성전자, 브로드컴과 AI 반도체 동맹…5년간 2천억달러 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-07-25T14:28:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBCcjZKRTB2djJNd2FXYXhkbndvQ1dtQ3M0NVBzb2hMM1lPVi00NUNhQ0tIWjdOOG53RTczOTBLQUoyUkpOb21BenJfZVNOWFhwRzVxVUJMR1VocEXSAWBBVV95cUxPYmswT3JTYUhjWllOTFJvb2xDM3dSTVJpY2ViamtQQ3Z6UTZkN2doRTU4dmlFQUZ2Ylo0d0dtMXZxcjRxTmRKZVVCZU1NVnFVSzNWZFpabkFOWExId1IxWk4?oc=5`

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
- Updated at: `2026-08-21T19:31:54+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-26.json`
- Latest observation title: `다음주 SK하이닉스·삼성전자 실적 발표…코스피 향방은 - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-07-26T07:20:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFA4VUJCWm9GY1pKUnRJUDRpeDNYSXUxdWs3YjRPQWIxcDlfZV9nZ0h5WVFzVjVuLU9TZ3haSWZRVlRDNTBjOFhkTUJ5T1U4VUVTUHk1aTdoZnlWcFJWZUxvRWdnbHAtV05qemMtUHhVU1jSAXRBVV95cUxOdHpfRlhDQklRNzVPNlF0NWxfVXV0WEZHNVVPTno3RlgzZkJIT3AyNGl0aUtweHFfNkJmNDdnRlFqYnY0V0dvX0gtTW93Zkh6VWpSR2FVNHZKSWxLNl9zV0dkX2dzeDlVdTRjUFFOWWlIaGxiOQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
