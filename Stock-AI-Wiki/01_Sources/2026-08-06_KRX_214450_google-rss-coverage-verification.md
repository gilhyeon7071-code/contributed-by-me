---
id: verification-2026-08-06-KRX-214450-google-rss-coverage
type: verification
title: KRX 214450 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

market: KRX
ticker: "214450"
company: 파마리서치
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=214450
    - name=파마리서치
    - naver_article_count=1
    - google_rss_article_count=28
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 214450
    - 파마리서치
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

# KRX 214450 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_214450_google-rss-coverage-source]]

## Facts Checked
- `code=214450`
- `name=파마리서치`
- `naver_article_count=1`
- `google_rss_article_count=28`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `파마리서치 '신약 플랫폼' 만든다 - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-08-04T15:52:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE1lVkl4cnpEOUNtQUMwbmpyd2dwN3h1SjUyNVJ3NG1KOGJqUE5helF5aWZzT0x4NHd1bDR3LUdlaU5oakFPeG1kaTlVZUVfQjVTLVh1SGo3dEg0QkkwT2dSbnpicmk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-06T16:05:15+09:00`
- Company: [[KRX_214450_파마리서치]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `파마리서치 '신약 플랫폼' 만든다 - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-08-04T15:52:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE1lVkl4cnpEOUNtQUMwbmpyd2dwN3h1SjUyNVJ3NG1KOGJqUE5helF5aWZzT0x4NHd1bDR3LUdlaU5oakFPeG1kaTlVZUVfQjVTLVh1SGo3dEg0QkkwT2dSbnpicmk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
