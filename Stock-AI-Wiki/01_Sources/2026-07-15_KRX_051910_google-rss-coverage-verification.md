---
id: verification-2026-07-15-KRX-051910-google-rss-coverage
type: verification
title: KRX 051910 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "051910"
company: LG화학
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
    - code=051910
    - name=LG화학
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 051910
    - LG화학
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

# KRX 051910 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_051910_google-rss-coverage-source]]

## Facts Checked
- `code=051910`
- `name=LG화학`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `7월 엔지니어상에 김세현 LG화학 연구위원·최양일 이엠텍 대표 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-13T12:02:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE9nQW0yT3lfdWcybUZpUWhkUzhzR2lra0JVSF9ndGNuNmM4SXREc0pOSUtqNVpKQUhBN000a0VLN25qcmQxQ2c?oc=5`

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
- Company: [[KRX_051910_LG화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `7월 엔지니어상에 김세현 LG화학 연구위원·최양일 이엠텍 대표 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-13T12:02:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE9nQW0yT3lfdWcybUZpUWhkUzhzR2lra0JVSF9ndGNuNmM4SXREc0pOSUtqNVpKQUhBN000a0VLN25qcmQxQ2c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
