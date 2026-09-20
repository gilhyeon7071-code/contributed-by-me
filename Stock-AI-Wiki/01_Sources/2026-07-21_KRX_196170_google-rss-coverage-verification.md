---
id: verification-2026-07-21-KRX-196170-google-rss-coverage
type: verification
title: KRX 196170 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=7
    - google_rss_article_count=11
    - kis_title_count=27
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_196170_google-rss-coverage-source]]

## Facts Checked
- `code=196170`
- `name=알테오젠`
- `naver_article_count=7`
- `google_rss_article_count=11`
- `kis_title_count=27`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"키트루다 큐렉스는 시작일 뿐"…알테오젠, SC 전환 본게임 예열 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-20T16:56:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9CeDlMQjRXNFBJRTMyWFVSN0xSVGYtU2V2S1pDNUVySU15dUVGNTJMZmRGbXZnM3cxWEphRGJQVDRjY0RxVGdlMUxxdF9nakluU01fcVFXdGx4TWVkWVI1d0FlTmwtUGRhOXfSAW9BVV95cUxOZ0xjRmx0VDNVOUo1Q0hQNHhPS0g3cG5HdG9ZckxYakNwdU9YSHRfeDZCUmQwSmRGYVdQUG9QRlFSWUZjQ3RKRXBVa1p3ZDUybW1INjhTQmVVSVgzakppZXdoUlpkd0VFWi1zeDRCeDQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-21T21:05:15+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `"키트루다 큐렉스는 시작일 뿐"…알테오젠, SC 전환 본게임 예열 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-20T16:56:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9CeDlMQjRXNFBJRTMyWFVSN0xSVGYtU2V2S1pDNUVySU15dUVGNTJMZmRGbXZnM3cxWEphRGJQVDRjY0RxVGdlMUxxdF9nakluU01fcVFXdGx4TWVkWVI1d0FlTmwtUGRhOXfSAW9BVV95cUxOZ0xjRmx0VDNVOUo1Q0hQNHhPS0g3cG5HdG9ZckxYakNwdU9YSHRfeDZCUmQwSmRGYVdQUG9QRlFSWUZjQ3RKRXBVa1p3ZDUybW1INjhTQmVVSVgzakppZXdoUlpkd0VFWi1zeDRCeDQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
