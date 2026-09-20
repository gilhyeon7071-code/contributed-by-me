---
id: verification-2026-07-28-KRX-036930-google-rss-coverage
type: verification
title: KRX 036930 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "036930"
company: 주성엔지니어링
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=036930
    - name=주성엔지니어링
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 036930
    - 주성엔지니어링
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

# KRX 036930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_036930_google-rss-coverage-source]]

## Facts Checked
- `code=036930`
- `name=주성엔지니어링`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[고래사냥] '한화솔루션·주성엔지니어링·대한항공! 내일장 고래 종목은?! - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-27T21:38:41+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9Gc3ZPSnR4Z1c0a0pLRHZzczlCSUczMlA5S3VNV2MwV2Q4OGxWNmhjb0lybTJEbGMwdG5lVVZoTU1EVzMyY2dpWWxUaGZVR3I4RF9yTm91TGd3MkV5TFNnTllOR1ZSVVBiOWt6aVp30gFuQVVfeXFMT0Zzdk9KdHhnVzRrSktEdnNzOUJJRzMyUDlLdU1XYzBXZDg4bFY2aGNvSXJtMkRsYzB0bmVVVmhNTURXMzJjZ2lZbFRoZlVHcjhEX3JOb3VMZ3cyRXlMU2dOWU5HVlJVUGI5a3ppWnc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T08:05:50+09:00`
- Company: [[KRX_036930_주성엔지니어링]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `[고래사냥] '한화솔루션·주성엔지니어링·대한항공! 내일장 고래 종목은?! - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-27T21:38:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9Gc3ZPSnR4Z1c0a0pLRHZzczlCSUczMlA5S3VNV2MwV2Q4OGxWNmhjb0lybTJEbGMwdG5lVVZoTU1EVzMyY2dpWWxUaGZVR3I4RF9yTm91TGd3MkV5TFNnTllOR1ZSVVBiOWt6aVp30gFuQVVfeXFMT0Zzdk9KdHhnVzRrSktEdnNzOUJJRzMyUDlLdU1XYzBXZDg4bFY2aGNvSXJtMkRsYzB0bmVVVmhNTURXMzJjZ2lZbFRoZlVHcjhEX3JOb3VMZ3cyRXlMU2dOWU5HVlJVUGI5a3ppWnc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
