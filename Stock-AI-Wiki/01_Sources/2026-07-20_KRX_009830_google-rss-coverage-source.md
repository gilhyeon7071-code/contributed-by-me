---
id: source-2026-07-20-KRX-009830-google-rss-coverage
type: source
title: KRX 009830 Google RSS Coverage Source
created: 2026-07-20
updated: 2026-07-20
status: raw
stage: 0

market: KRX
ticker: "009830"
company: 한화솔루션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 009830.
  key_facts:
    - code=009830
    - name=한화솔루션
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009830
    - 한화솔루션
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

# KRX 009830 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=009830`
- `name=한화솔루션`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 009830.

## RSS Item Metadata
- Title: `[리포트 브리핑]한화솔루션, '태양광 모듈 판가 상승세 지속' 목표가 60,000원 - 하나증권 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-07-20T09:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA1anFkcVJNb3hRRDhhQWI1c3RzOEQ3MVl1a1Q4UWZWLVRCUWZBRFNDVjNjV0JxS1BkWGNKajY0eUV3WXVWRFFFWFJPQW9XMmxQeTZEdjZYWC12Tk9u?oc=5`

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
- Updated at: `2026-07-20T21:05:06+09:00`
- Company: [[KRX_009830_한화솔루션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `한화솔루션 유상증자 1.2조 확정…당초 계획 절반 수준 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-07-20T18:30:48+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE1KYmMxYU9PamxwcWZ3dzFoeEdvWU9wV2lRcDdmWVJSLXdSV0hGOThMVEF3WldFZWVHbllOVV9BZm5IMzlWdmhaVVpVNGdjd0dCeW5GMUFZVGlURWPSAWBBVV95cUxPZEZSY1JOWVk0MGpMenpPSS1tT2tQUTcwLWdGMmNjUEtpWUpycTZfdTZ6MXB4YjJQZEtWVEdVZTE3djI5ZW15cDhKQ0ZabkdncEJDdnkxTXdsU1kwdlN5dFQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
