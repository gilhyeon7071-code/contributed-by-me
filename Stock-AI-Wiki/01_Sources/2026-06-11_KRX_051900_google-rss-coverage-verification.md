---
id: verification-2026-06-11-KRX-051900-google-rss-coverage
type: verification
title: KRX 051900 Google RSS Coverage Verification
created: 2026-06-11
updated: 2026-06-11
status: verification
stage: 1

market: KRX
ticker: "051900"
company: LG생활건강
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=051900
    - name=LG생활건강
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 051900
    - LG생활건강
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

# KRX 051900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-11_KRX_051900_google-rss-coverage-source]]

## Facts Checked
- `code=051900`
- `name=LG생활건강`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG생활건강株 15년래 최저 수준?… 시장은 '성장성'을 더 따진다 - 디지털포스트(PC사랑)`
- Source: `디지털포스트(PC사랑)`
- Published at: `2026-06-11T10:45:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE1lT05iNTFWVkNzcmh3TTEzV3BTMXdxckhMN0I3QU94YXVydnBGVkJBZGh3N2hqV0t2NzMtcmVscmd2dUl4amcwOTFLWjhlNzNEblhPVHBDc25sMjJ3YXJZQXRGY2pPd0ZOQllZ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-11T17:05:49+09:00`
- Company: [[KRX_051900_LG생활건강]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `LG생활건강株 15년래 최저 수준?… 시장은 '성장성'을 더 따진다 - 디지털포스트(PC사랑)`
- Latest observation source: `디지털포스트(PC사랑)`
- Latest observation published_at: `2026-06-11T10:45:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE1lT05iNTFWVkNzcmh3TTEzV3BTMXdxckhMN0I3QU94YXVydnBGVkJBZGh3N2hqV0t2NzMtcmVscmd2dUl4amcwOTFLWjhlNzNEblhPVHBDc25sMjJ3YXJZQXRGY2pPd0ZOQllZ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
