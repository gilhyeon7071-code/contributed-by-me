---
id: verification-2026-07-26-KRX-009830-google-rss-coverage
type: verification
title: KRX 009830 Google RSS Coverage Verification
created: 2026-07-26
updated: 2026-07-26
status: verification
stage: 1

market: KRX
ticker: "009830"
company: 한화솔루션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009830
    - name=한화솔루션
    - naver_article_count=1
    - google_rss_article_count=10
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009830
    - 한화솔루션
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

# KRX 009830 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-26_KRX_009830_google-rss-coverage-source]]

## Facts Checked
- `code=009830`
- `name=한화솔루션`
- `naver_article_count=1`
- `google_rss_article_count=10`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한화솔루션, 1.2조 유상증자 구주주 청약률 102.7% - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-07-24T10:22:12+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9PMWFPb2prVkUtWktveHBqR0JIWHV1c2xNT01mb042UEtqbU13cVN4a0dvd1M1UTIxblAxSlhOMG5LQ2ZDVVlOTk5YZGxjYThOVWZJQ19xN0tEamJNZDUyUzVPWkptSEdTbDRtX0tYbGg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:54+09:00`
- Company: [[KRX_009830_한화솔루션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-26.json`
- Latest observation title: `한화솔루션, 1.2조 유상증자 구주주 청약률 102.7% - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-07-24T10:22:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9PMWFPb2prVkUtWktveHBqR0JIWHV1c2xNT01mb042UEtqbU13cVN4a0dvd1M1UTIxblAxSlhOMG5LQ2ZDVVlOTk5YZGxjYThOVWZJQ19xN0tEamJNZDUyUzVPWkptSEdTbDRtX0tYbGg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
