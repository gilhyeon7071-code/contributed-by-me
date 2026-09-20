---
id: verification-2026-06-05-KRX-424870-google-rss-coverage
type: verification
title: KRX 424870 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "424870"
company: 이뮨온시아
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=424870
    - name=이뮨온시아
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 424870
    - 이뮨온시아
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

# KRX 424870 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_424870_google-rss-coverage-source]]

## Facts Checked
- `code=424870`
- `name=이뮨온시아`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `이뮨온시아, 'CD47 병용' TNBC 1b상 "ASCO 공개" - 바이오스펙테이터`
- Source: `바이오스펙테이터`
- Published at: `2026-06-01T09:39:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiV0FVX3lxTFBWWmlPUEZrM2UxRE5wdnluN3psVkdJem92ZjFtd09pZ1BXbVVUVzhsSkw3WnJtak54T3FnX3pVQm1rV1lhWl9KZVMyR2dReHhrcHJabnZEVQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_424870_이뮨온시아]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `[메자닌 투자파일] 이뮨온시아, 불확실 LO 의존…CB 카드 꺼내나 - 블로터`
- Latest observation source: `블로터`
- Latest observation published_at: `2026-06-05T16:22:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBLcGs1N2pyVFdtTlhhZ3AyaXU0cm9hU2hGZEN4TWN0amlmdERSQ1A4U08wY241Rk42WjU1aUNYd2RaT2txQ1Q4QURZUVNhbGlsUG9RVWE1YzN1T0lpcms1eU16NmctUE4wSDlhcNIBbEFVX3lxTFBLcGs1N2pyVFdtTlhhZ3AyaXU0cm9hU2hGZEN4TWN0amlmdERSQ1A4U08wY241Rk42WjU1aUNYd2RaT2txQ1Q4QURZUVNhbGlsUG9RVWE1YzN1T0lpcms1eU16NmctUE4wSDlhcA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
