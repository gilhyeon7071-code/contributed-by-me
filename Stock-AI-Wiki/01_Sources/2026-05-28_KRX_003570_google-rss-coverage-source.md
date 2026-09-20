---
id: source-2026-05-28-KRX-003570-google-rss-coverage
type: source
title: KRX 003570 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "003570"
company: SNT다이내믹스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 003570.
  key_facts:
    - code=003570
    - name=SNT다이내믹스
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003570
    - SNT다이내믹스
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

# KRX 003570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003570`
- `name=SNT다이내믹스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003570.

## RSS Item Metadata
- Title: `"SNT다이내믹스, 목표가 7.7만…무기·부품 수출 성장축"-SK - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-05-26T08:12:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9QUThfaGRZc1RiaUFHRWViZDN1cG5JUnoyblJ2SDJ4b0NZZzMwU3pyVlZzTk1aei1iWXNUU2w5ZG1iTEJCam0wbkZuU1VaeGJhNW9JMG83ai0xM0JzMU1hOUJWdlVNYkQxUkEtWHdB0gFuQVVfeXFMT1BROF9oZFlzVGJpQUdFZWJkM3VwbklSejJuUnZIMnhvQ1lnMzBTenJWVnNOTVp6LWJZc1RTbDlkbWJMQkJqbTBuRm5TVVp4YmE1b0kwbzdqLTEzQnMxTWE5QlZ2VU1iRDFSQS1Yd0E?oc=5`

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
- Updated at: `2026-05-28T21:05:04+09:00`
- Company: [[KRX_003570_SNT다이내믹스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `SK증권 “SNT다이내믹스, 수출 증가하고 수익성 높아지고⋯사상 최고 연간 영업익 기대” - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-26T07:52:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE5xdnJWRHdxRl9wT2p0RUo3UnlRRkxNSGJZM283WXlDWWg5bTZzYXM1QkRhLS1adHU5OGpmMnBRQVh5Wkpza3c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
