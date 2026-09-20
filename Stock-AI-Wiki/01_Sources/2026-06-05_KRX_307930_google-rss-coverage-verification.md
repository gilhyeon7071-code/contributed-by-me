---
id: verification-2026-06-05-KRX-307930-google-rss-coverage
type: verification
title: KRX 307930 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "307930"
company: 컴퍼니케이
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
    - code=307930
    - name=컴퍼니케이
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 307930
    - 컴퍼니케이
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

# KRX 307930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_307930_google-rss-coverage-source]]

## Facts Checked
- `code=307930`
- `name=컴퍼니케이`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `컴퍼니케이, +9.88% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-02T09:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNVmhZZWR2QVZZR3dYNkViSlRtOFpQamU1MUtydlhZV1dac2FaSThWUW0wRkRzRTBoWHRDcE9jeWFncDNDZ3lsTjQtNUM4ejdaTVFvRkdsb1JMTzctXzBMSnM3UGVmRm5MVThZNHM4WmxnLW12Skszci1jWlM4Q1YtNVJfa9IBlwFBVV95cUxONVZVbFBKaGtrUnBONXE3MWstMGliSHd6OWx4SzBCRVJTYVhZTXN1ck1VNlc5TWQxekhDSFpXRlVoeE1INHYtM2FFQm5xV0VaeVNQTGQySVFYSUkzV0Z1TnB6MV9XeWpFZUwtV2N1VDV3VVpMNVFpNFgyWkVwZ3k3MFJDaHBKNnFOTlVtcXRHTUM5cTBHTy04?oc=5`

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
- Company: [[KRX_307930_컴퍼니케이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `컴퍼니케이, +1.05% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-05T14:16:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxQcTBMVzRwZ01RbWZXY1J4dGwzdlJHMDUzTVBZam9VLVhSUjd3aVVjckh0bkVHRjBUQWliVml5ZFJ1M3BtdktpUmdSczhVdkt2ekpmbDRSdFR3RkhLb0NFSVhRN05SQ1c0bjZPQl82ZjdWQjQ2SGFLV09iVC1oWkt6NnNSSTVxX3FHX2xCZUVicUVuQlRXdFFv0gGXAUFVX3lxTFBxMExXNHBnTVFtZldjUnh0bDN2UkcwNTNNUFlqb1UtWFJSN3dpVWNySHRuRUdGMFRBaWJWaXlkUnUzcG12S2lSZ1JzOFV2S3Z6SmZsNFJ0VHdGSEtvQ0VJWFE3TlJDVzRuNk9CXzZmN1ZCNDZIYUtXT2JULWhaS3o2c1JJNXFfcUdfbEJlRWJxRW5CVFd0UW8?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
