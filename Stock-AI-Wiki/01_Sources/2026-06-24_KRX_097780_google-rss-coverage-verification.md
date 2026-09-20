---
id: verification-2026-06-24-KRX-097780-google-rss-coverage
type: verification
title: KRX 097780 Google RSS Coverage Verification
created: 2026-06-24
updated: 2026-06-24
status: verification
stage: 1

market: KRX
ticker: "097780"
company: 에코볼트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=097780
    - name=에코볼트
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 097780
    - 에코볼트
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

# KRX 097780 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-24_KRX_097780_google-rss-coverage-source]]

## Facts Checked
- `code=097780`
- `name=에코볼트`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[속보] 알에프텍, 에코볼트 흡수합병 결정...합병비율 1:0.4053487 - Investing.com 한국어`
- Source: `Investing.com 한국어`
- Published at: `2026-04-14T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBWa3IxcEdWTDc4eTctclNTdnhKclhFSnl6VC1nRzFtTXNsMmhOdXNVV1VXRWtjRy13T3RMVmh1TXhMSFkxVWpfVlJJZ3BRQ1N4c2x5VUhIdlY3eFRGdTdBbnZvb1Bwc25yZEw5WGFXRFZabEU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-24T09:05:17+09:00`
- Company: [[KRX_097780_에코볼트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-24.json`
- Latest observation title: `[속보] 알에프텍, 에코볼트 흡수합병 결정...합병비율 1:0.4053487 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-04-14T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBWa3IxcEdWTDc4eTctclNTdnhKclhFSnl6VC1nRzFtTXNsMmhOdXNVV1VXRWtjRy13T3RMVmh1TXhMSFkxVWpfVlJJZ3BRQ1N4c2x5VUhIdlY3eFRGdTdBbnZvb1Bwc25yZEw5WGFXRFZabEU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
