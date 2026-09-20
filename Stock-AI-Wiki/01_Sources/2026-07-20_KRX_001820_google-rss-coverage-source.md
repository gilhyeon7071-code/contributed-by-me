---
id: source-2026-07-20-KRX-001820-google-rss-coverage
type: source
title: KRX 001820 Google RSS Coverage Source
created: 2026-07-20
updated: 2026-07-20
status: raw
stage: 0

market: KRX
ticker: "001820"
company: 삼화콘덴서공업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 001820.
  key_facts:
    - code=001820
    - name=삼화콘덴서공업
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 001820
    - 삼화콘덴서공업
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

# KRX 001820 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=001820`
- `name=삼화콘덴서공업`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 001820.

## RSS Item Metadata
- Title: `[7월 15일 고용24 채용정보] 삼화콘덴서공업·중소기업기술정보진흥원·신용보증기금·효성화학·한화에어로스페이스 - 뉴스투데이`
- Source: `뉴스투데이`
- Published at: `2026-07-15T13:03:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE1jTXBFa1FFZW1lWGtXZ3BiQWZTYUEzbDFFaHRDc0xsa2lEQjE4a2JlQ1huZXhoMUFJMVR4bEJoQ2RUT24wNkhfbUhYbUJ5V3VOaGtLNjJLNWxKSGVkZlE?oc=5`

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
- Updated at: `2026-07-20T08:33:21+09:00`
- Company: [[KRX_001820_삼화콘덴서공업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `[7월 15일 고용24 채용정보] 삼화콘덴서공업·중소기업기술정보진흥원·신용보증기금·효성화학·한화에어로스페이스 - 뉴스투데이`
- Latest observation source: `뉴스투데이`
- Latest observation published_at: `2026-07-15T13:03:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE1jTXBFa1FFZW1lWGtXZ3BiQWZTYUEzbDFFaHRDc0xsa2lEQjE4a2JlQ1huZXhoMUFJMVR4bEJoQ2RUT24wNkhfbUhYbUJ5V3VOaGtLNjJLNWxKSGVkZlE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
