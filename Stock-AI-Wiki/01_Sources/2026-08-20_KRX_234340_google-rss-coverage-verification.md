---
id: verification-2026-08-20-KRX-234340-google-rss-coverage
type: verification
title: KRX 234340 Google RSS Coverage Verification
created: 2026-08-20
updated: 2026-08-20
status: verification
stage: 1

market: KRX
ticker: "234340"
company: 헥토파이낸셜
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=234340
    - name=헥토파이낸셜
    - naver_article_count=3
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 234340
    - 헥토파이낸셜
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

# KRX 234340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-20_KRX_234340_google-rss-coverage-source]]

## Facts Checked
- `code=234340`
- `name=헥토파이낸셜`
- `naver_article_count=3`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `간편결제 '쑥쑥'…헥토파이낸셜 수익성 '껑충' - 비즈워치`
- Source: `비즈워치`
- Published at: `2026-08-20T07:40:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE95WkUtWGJkaWxwc3pBM0V2MEpTYXIyM3dmX2Vma1VPX1Q4c2s4XzJxWjFIdDlob0xFNElzT21vSWFaNERIdlBmTEdWU0V6N2FKYkh0LXNQbEJQNDlOSnNSNFhJLUVISUhWb0E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:37:31+09:00`
- Company: [[KRX_234340_헥토파이낸셜]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-20.json`
- Latest observation title: `간편결제 '쑥쑥'…헥토파이낸셜 수익성 '껑충' - 비즈워치`
- Latest observation source: `비즈워치`
- Latest observation published_at: `2026-08-20T07:40:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE95WkUtWGJkaWxwc3pBM0V2MEpTYXIyM3dmX2Vma1VPX1Q4c2s4XzJxWjFIdDlob0xFNElzT21vSWFaNERIdlBmTEdWU0V6N2FKYkh0LXNQbEJQNDlOSnNSNFhJLUVISUhWb0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
