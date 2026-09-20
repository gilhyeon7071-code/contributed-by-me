---
id: verification-2026-05-21-KRX-081180-google-rss-coverage
type: verification
title: KRX 081180 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "081180"
company: 쎄크
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=081180
    - name=쎄크
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 081180
    - 쎄크
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 081180 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_081180_google-rss-coverage-source]]

## Facts Checked
- `code=081180`
- `name=쎄크`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `쎄크, HBM 인라인 검사 잰걸음…"TSV·하이브리드 본딩 대응 장비 2~3분기 진입 목표" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-21T14:22:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1KckFrd0ZSVUZzSW1zVHM3bGZWRzBVLXpBT2plb2xraXFOZUxVUHV3U0huNmpPUVUzQVdJeGpacnZ2NVhfS3NnRHRnV1RDb3M?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-21T15:05:23+09:00`
- Company: [[KRX_081180_쎄크]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `쎄크, HBM 인라인 검사 잰걸음…"TSV·하이브리드 본딩 대응 장비 2~3분기 진입 목표" - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-21T14:22:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1KckFrd0ZSVUZzSW1zVHM3bGZWRzBVLXpBT2plb2xraXFOZUxVUHV3U0huNmpPUVUzQVdJeGpacnZ2NVhfS3NnRHRnV1RDb3M?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
