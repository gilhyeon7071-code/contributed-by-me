---
id: source-2026-05-25-KRX-000090-google-rss-coverage
type: source
title: KRX 000090 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "000090"
company: 에임드바이오
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 000090.
  key_facts:
    - code=000090
    - name=에임드바이오
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 000090
    - 에임드바이오
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

# KRX 000090 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000090`
- `name=에임드바이오`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000090.

## RSS Item Metadata
- Title: `에임드바이오, 베링거서 'ADC' 연구개발비 수령 - 바이오스펙테이터`
- Source: `바이오스펙테이터`
- Published at: `2026-05-06T18:37:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiV0FVX3lxTE9qTUhzYUZsQVY1VHBhdy0wcWVJb2JDUEoxYTBaU1U5SUVnbGRsYVRZWGN6MlVVN0hIRjA3RVdQdUlyMHRrYVdOa1k3Sk9mZnBmQThiQ1ZXYw?oc=5`

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
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_000090_에임드바이오]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `에임드바이오, 베링거서 'ADC' 연구개발비 수령 - 바이오스펙테이터`
- Latest observation source: `바이오스펙테이터`
- Latest observation published_at: `2026-05-06T18:37:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiV0FVX3lxTE9qTUhzYUZsQVY1VHBhdy0wcWVJb2JDUEoxYTBaU1U5SUVnbGRsYVRZWGN6MlVVN0hIRjA3RVdQdUlyMHRrYVdOa1k3Sk9mZnBmQThiQ1ZXYw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
