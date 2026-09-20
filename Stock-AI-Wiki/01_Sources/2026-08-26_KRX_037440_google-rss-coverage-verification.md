---
id: verification-2026-08-26-KRX-037440-google-rss-coverage
type: verification
title: KRX 037440 Google RSS Coverage Verification
created: 2026-08-26
updated: 2026-08-26
status: verification
stage: 1

market: KRX
ticker: "037440"
company: 희림
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=037440
    - name=희림
    - naver_article_count=0
    - google_rss_article_count=4
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 037440
    - 희림
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 037440 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-26_KRX_037440_google-rss-coverage-source]]

## Facts Checked
- `code=037440`
- `name=희림`
- `naver_article_count=0`
- `google_rss_article_count=4`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, 희림-전후 재건(우크라/중동 전쟁 등) 테마 상승세에 14.04% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-08-26T10:23:31+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBWVjVqSWRjZ3VXZ0k5Um1SQno4QWI4RHJya2Y3dU4wUlpjT2NlUHJEWGFMYVh2VGFfaFBTSlpYOThTb0FremhtY3dmNlhEckpSSWc?oc=5`

## Article Body Archive Checked
- Title: `특징주, 희림-전후 재건(우크라/중동 전쟁 등) 테마 상승세에 14.04% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-08-26T10:23:31+09:00`
- URL: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBWVjVqSWRjZ3VXZ0k5Um1SQno4QWI4RHJya2Y3dU4wUlpjT2NlUHJEWGFMYVh2VGFfaFBTSlpYOThTb0FremhtY3dmNlhEckpSSWc?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBWVjVqSWRjZ3VXZ0k5Um1SQno4QWI4RHJya2Y3dU4wUlpjT2NlUHJEWGFMYVh2VGFfaFBTSlpYOThTb0FremhtY3dmNlhEckpSSWc?oc=5`
- Body excerpt: 특징주, 희림-전후 재건(우크라/중동 전쟁 등) 테마 상승세에 14.04% ↑ 매일경제 마켓

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-26T23:05:22+09:00`
- Company: [[KRX_037440_희림]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-26.json`
- Latest observation title: `특징주, 희림-전후 재건(우크라/중동 전쟁 등) 테마 상승세에 14.04% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-08-26T10:23:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBWVjVqSWRjZ3VXZ0k5Um1SQno4QWI4RHJya2Y3dU4wUlpjT2NlUHJEWGFMYVh2VGFfaFBTSlpYOThTb0FremhtY3dmNlhEckpSSWc?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
