---
id: verification-2026-07-15-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "000720"
company: KB제33호스팩
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=KB제33호스팩
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - KB제33호스팩
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=KB제33호스팩`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `사실상 지배주주 KB증권, KB제33호스팩 주식 매도 - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-07-09T14:53:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE44Vmk0ZXpKQmh5aWQwdDdEaVk5ZDc3Y05HSV9YMElsX3FWeUlIcW4wNi1oaG9TOUE3LXlVdThucU5Za3VHWUZ2cFdoTk4yZkx0LUZiWlVmeW12UmU5aXNuTUxhN1ZTQTdVQkFOTE1xd3BSYTQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T08:05:13+09:00`
- Company: [[KRX_000720_KB제33호스팩]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `사실상 지배주주 KB증권, KB제33호스팩 주식 매도 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-07-09T14:53:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE44Vmk0ZXpKQmh5aWQwdDdEaVk5ZDc3Y05HSV9YMElsX3FWeUlIcW4wNi1oaG9TOUE3LXlVdThucU5Za3VHWUZ2cFdoTk4yZkx0LUZiWlVmeW12UmU5aXNuTUxhN1ZTQTdVQkFOTE1xd3BSYTQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
