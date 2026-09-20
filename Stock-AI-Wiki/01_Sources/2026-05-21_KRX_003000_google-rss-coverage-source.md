---
id: source-2026-05-21-KRX-003000-google-rss-coverage
type: source
title: KRX 003000 Google RSS Coverage Source
created: 2026-05-21
updated: 2026-05-21
status: raw
stage: 0

market: KRX
ticker: "003000"
company: 부광약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 003000.
  key_facts:
    - code=003000
    - name=부광약품
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003000
    - 부광약품
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

# KRX 003000 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003000`
- `name=부광약품`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003000.

## RSS Item Metadata
- Title: `[경력채용] 휴온스바이오파마, 익수제약, 부광약품, 넥스아이, 앱클론 - 히트뉴스`
- Source: `히트뉴스`
- Published at: `2026-05-21T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5VSWpqcldON0JwajJuVEg4U3VqTWNBTjc4MmZoV0FuR2hORjRZN2RuZjM4VUFLbWFmOWw5RjJyV3BaWVIxMkRKM3E3eE8wQ2V5eUU4NzV1VnVIeTdhcWNoQ3R0aXR2ZnZ6dWFz?oc=5`

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
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_003000_부광약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `[경력채용] 휴온스바이오파마, 익수제약, 부광약품, 넥스아이, 앱클론 - 히트뉴스`
- Latest observation source: `히트뉴스`
- Latest observation published_at: `2026-05-21T06:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5VSWpqcldON0JwajJuVEg4U3VqTWNBTjc4MmZoV0FuR2hORjRZN2RuZjM4VUFLbWFmOWw5RjJyV3BaWVIxMkRKM3E3eE8wQ2V5eUU4NzV1VnVIeTdhcWNoQ3R0aXR2ZnZ6dWFz?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
