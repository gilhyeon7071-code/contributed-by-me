---
id: verification-2026-05-25-KRX-128940-google-rss-coverage
type: verification
title: KRX 128940 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "128940"
company: 한미약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=128940
    - name=한미약품
    - naver_article_count=2
    - google_rss_article_count=10
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 128940
    - 한미약품
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

# KRX 128940 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_128940_google-rss-coverage-source]]

## Facts Checked
- `code=128940`
- `name=한미약품`
- `naver_article_count=2`
- `google_rss_article_count=10`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한미약품, 조직개편..미래성장부문장에 최인영 전무 - 바이오스펙테이터`
- Source: `바이오스펙테이터`
- Published at: `2026-05-08T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiV0FVX3lxTE1Mbm5RdEx6TjhkQ0ZRQTczRlZ3eWJNUzBjWXZKdTZRT0wxZW5NcUlCY0I5S3hkOVpjMmVSZmZLeW0xNnpJZzBHeWRIWUFoWm1pV09zZDkyNA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_128940_한미약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `한미약품, 조직개편..미래성장부문장에 최인영 전무 - 바이오스펙테이터`
- Latest observation source: `바이오스펙테이터`
- Latest observation published_at: `2026-05-08T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiV0FVX3lxTE1Mbm5RdEx6TjhkQ0ZRQTczRlZ3eWJNUzBjWXZKdTZRT0wxZW5NcUlCY0I5S3hkOVpjMmVSZmZLeW0xNnpJZzBHeWRIWUFoWm1pV09zZDkyNA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
