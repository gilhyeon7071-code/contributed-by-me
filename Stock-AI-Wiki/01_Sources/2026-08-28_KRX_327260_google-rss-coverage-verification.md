---
id: verification-2026-08-28-KRX-327260-google-rss-coverage
type: verification
title: KRX 327260 Google RSS Coverage Verification
created: 2026-08-28
updated: 2026-08-28
status: verification
stage: 1

market: KRX
ticker: "327260"
company: RF머트리얼즈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=327260
    - name=RF머트리얼즈
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 327260
    - RF머트리얼즈
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 327260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-28_KRX_327260_google-rss-coverage-source]]

## Facts Checked
- `code=327260`
- `name=RF머트리얼즈`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독] RF머트리얼즈 "내년 美 주문 2.5배↑…공장 주야간 풀 가동" - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-26T07:30:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9lZ0tQYmxXY2VxM0kya0dVVlhUSjhXc1BTMUNNTmhVTFBSaUNYMGhFSm1qN3kzanFfYXV0UmFTNnZqS3JhaHJPcF9Jb0ZRMDZhbFBZbV9CeTRrdw?oc=5`

## Article Body Archive Checked
- Title: `[단독] RF머트리얼즈 "내년 美 주문 2.5배↑…공장 주야간 풀 가동" - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-26T07:30:02+09:00`
- URL: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9lZ0tQYmxXY2VxM0kya0dVVlhUSjhXc1BTMUNNTmhVTFBSaUNYMGhFSm1qN3kzanFfYXV0UmFTNnZqS3JhaHJPcF9Jb0ZRMDZhbFBZbV9CeTRrdw?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9lZ0tQYmxXY2VxM0kya0dVVlhUSjhXc1BTMUNNTmhVTFBSaUNYMGhFSm1qN3kzanFfYXV0UmFTNnZqS3JhaHJPcF9Jb0ZRMDZhbFBZbV9CeTRrdw?oc=5`
- Body excerpt: [단독] RF머트리얼즈 "내년 美 주문 2.5배↑…공장 주야간 풀 가동" 한국경제

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-28T14:15:06+09:00`
- Company: [[KRX_327260_RF머트리얼즈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-28.json`
- Latest observation title: `[단독] RF머트리얼즈 "내년 美 주문 2.5배↑…공장 주야간 풀 가동" - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-08-26T07:30:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9lZ0tQYmxXY2VxM0kya0dVVlhUSjhXc1BTMUNNTmhVTFBSaUNYMGhFSm1qN3kzanFfYXV0UmFTNnZqS3JhaHJPcF9Jb0ZRMDZhbFBZbV9CeTRrdw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
