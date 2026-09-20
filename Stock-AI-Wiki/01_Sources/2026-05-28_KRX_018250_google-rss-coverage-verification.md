---
id: verification-2026-05-28-KRX-018250-google-rss-coverage
type: verification
title: KRX 018250 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "018250"
company: 애경산업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018250
    - name=애경산업
    - naver_article_count=2
    - google_rss_article_count=20
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018250
    - 애경산업
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

# KRX 018250 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_018250_google-rss-coverage-source]]

## Facts Checked
- `code=018250`
- `name=애경산업`
- `naver_article_count=2`
- `google_rss_article_count=20`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 애경산업, 탈모완화 신소재 발견 소식에 5%↑(종합) - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-05-28T15:53:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE1UQ2llcjYtNkpxQ3h1Y2ZUN25zbFBYbmt0SnM0WHhUQkhYQlg2SHh3ck0xa214alYyaHJUTmVzUjRjV2VlbHRBQlo3OE5OeTNnLTljVlhaMnNiUUXSAWBBVV95cUxPMjhHaDlWMEhBX0taODBPMHZnb2R4UWJNa3dLMjhOMUxISWJIaTU3a3RfMG9famN1SEVJYl9VdU9la0RvaHhQbXR6V1JjS2h4QkhaUHFFODFhUWtFX3lCcTg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:01+09:00`
- Company: [[KRX_018250_애경산업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `[특징주] 애경산업, 탈모완화 신소재 발견 소식에 5%↑(종합) - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-28T15:53:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE1UQ2llcjYtNkpxQ3h1Y2ZUN25zbFBYbmt0SnM0WHhUQkhYQlg2SHh3ck0xa214alYyaHJUTmVzUjRjV2VlbHRBQlo3OE5OeTNnLTljVlhaMnNiUUXSAWBBVV95cUxPMjhHaDlWMEhBX0taODBPMHZnb2R4UWJNa3dLMjhOMUxISWJIaTU3a3RfMG9famN1SEVJYl9VdU9la0RvaHhQbXR6V1JjS2h4QkhaUHFFODFhUWtFX3lCcTg?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
