---
id: verification-2026-07-23-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=35
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-23_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=35`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독]시공사 선정 끝난 성수4, 대우건설 입찰보증금 500억 또 '미반환' - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-07-22T16:14:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBHOUF4LWJCbU9tMGpMV1NOOUYxeWtaOHhxZjdtcU1Yb1FLcEVZSThEa3pVdF91YzNYRWxud3pJWVQwdnhBbU1ENDFjRjFKcVBLSkhXVFJodHVqUF9Dc0paMFpFbk0?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `[단독]시공사 선정 끝난 성수4, 대우건설 입찰보증금 500억 또 '미반환' - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-07-22T16:14:58+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBHOUF4LWJCbU9tMGpMV1NOOUYxeWtaOHhxZjdtcU1Yb1FLcEVZSThEa3pVdF91YzNYRWxud3pJWVQwdnhBbU1ENDFjRjFKcVBLSkhXVFJodHVqUF9Dc0paMFpFbk0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
