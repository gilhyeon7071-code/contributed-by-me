---
id: verification-2026-06-22-KRX-005680-google-rss-coverage
type: verification
title: KRX 005680 Google RSS Coverage Verification
created: 2026-06-22
updated: 2026-06-22
status: verification
stage: 1

market: KRX
ticker: "005680"
company: 삼영전자공업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-22

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005680
    - name=삼영전자공업
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005680
    - 삼영전자공업
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

# KRX 005680 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-22_KRX_005680_google-rss-coverage-source]]

## Facts Checked
- `code=005680`
- `name=삼영전자공업`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼영전자공업, 아비코전자·NCC와 협력…전자부품 사업 포트폴리오 확대 - 한국경제`
- Source: `한국경제`
- Published at: `2026-03-18T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1PNV9Xb3ZpZ1ZuYS1lZ0U0a2RfMDlBUEl6N2FSRkNsWFRRaDdkaS1zMFhqNGpJVWJGdTJDWVdSMkdKaDJzMXN1aXZLb1o2eWF0UmRwclZXcDNmUdIBVEFVX3lxTFBqLUx0ZGhmaGRFSTFJWl9xZnpkdldjam53TW9HYTJMNkpOMWJIU2ZwdXJZTVFIWl9SQjJpS2ExMXpIb21vWER4OWp6aDAzSlBBN2ZmQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:21+09:00`
- Company: [[KRX_005680_삼영전자공업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-22.json`
- Latest observation title: `삼영전자공업, 아비코전자·NCC와 협력…전자부품 사업 포트폴리오 확대 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-03-18T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1PNV9Xb3ZpZ1ZuYS1lZ0U0a2RfMDlBUEl6N2FSRkNsWFRRaDdkaS1zMFhqNGpJVWJGdTJDWVdSMkdKaDJzMXN1aXZLb1o2eWF0UmRwclZXcDNmUdIBVEFVX3lxTFBqLUx0ZGhmaGRFSTFJWl9xZnpkdldjam53TW9HYTJMNkpOMWJIU2ZwdXJZTVFIWl9SQjJpS2ExMXpIb21vWER4OWp6aDAzSlBBN2ZmQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
