---
id: source-2026-07-13-KRX-179530-google-rss-coverage
type: source
title: KRX 179530 Google RSS Coverage Source
created: 2026-07-13
updated: 2026-07-13
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
  collected_at: 2026-07-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 179530.
  key_facts:
    - code=179530
    - name=애드바이오텍
    - naver_article_count=1
    - google_rss_article_count=3
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
- `google_rss_article_count=3`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 179530.

## RSS Item Metadata
- Title: `애드바이오텍 김도형 대표 “바이오·헬스케어 기업으로 체질 전환…주주가치 높일 것” - 메디코파마`
- Source: `메디코파마`
- Published at: `2026-07-08T10:15:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBqVFJqZjlsVEJKYllNMzY4ZklJWFBzNG5uRkJwYlRpRkphNjZZeklNc3pmYmZLSkJGZzR4RENnYkZPRHNjWVk4ejBCQklIS0NRSjV1S3FyN0NoY1NrRjRNaFVSeFR0Vl9zMTNuaWNJQy1WQQ?oc=5`

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
- Updated at: `2026-07-13T09:05:56+09:00`
- Company: [[KRX_179530_애드바이오텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-13.json`
- Latest observation title: `애드바이오텍 김도형 대표 “바이오·헬스케어 기업으로 체질 전환…주주가치 높일 것” - 메디코파마`
- Latest observation source: `메디코파마`
- Latest observation published_at: `2026-07-08T10:15:36+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFBqVFJqZjlsVEJKYllNMzY4ZklJWFBzNG5uRkJwYlRpRkphNjZZeklNc3pmYmZLSkJGZzR4RENnYkZPRHNjWVk4ejBCQklIS0NRSjV1S3FyN0NoY1NrRjRNaFVSeFR0Vl9zMTNuaWNJQy1WQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
