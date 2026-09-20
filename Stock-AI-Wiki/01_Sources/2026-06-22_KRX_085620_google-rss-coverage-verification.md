---
id: verification-2026-06-22-KRX-085620-google-rss-coverage
type: verification
title: KRX 085620 Google RSS Coverage Verification
created: 2026-06-22
updated: 2026-06-22
status: verification
stage: 1

market: KRX
ticker: "085620"
company: 미래에셋생명
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-22

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=085620
    - name=미래에셋생명
    - naver_article_count=0
    - google_rss_article_count=20
    - kis_title_count=14
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 085620
    - 미래에셋생명
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

# KRX 085620 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-22_KRX_085620_google-rss-coverage-source]]

## Facts Checked
- `code=085620`
- `name=미래에셋생명`
- `naver_article_count=0`
- `google_rss_article_count=20`
- `kis_title_count=14`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `미래에셋생명, 수익·건전성 두마리 토끼…CSM 확대에 K-ICS 167.7% 안정세 - 전자신문`
- Source: `전자신문`
- Published at: `2026-06-21T09:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiTkFVX3lxTFAxMlk3NGl5ZFZwbVpocHBCUXlQdm5KenVqTFpvZ1BhQUU2aFgxV0sxSTJWTV9hMGNVeGpRd25XRnRYb0Z3TUdaeXZIdzdsZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:21+09:00`
- Company: [[KRX_085620_미래에셋생명]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-22.json`
- Latest observation title: `거래소, 미래에셋생명·강동씨앤엘 투자경고종목 지정 - 포인트데일리`
- Latest observation source: `포인트데일리`
- Latest observation published_at: `2026-06-22T09:07:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBWQWsxSC16bFVyd05yb3FaLXRuTnZQNVBPUS1SQWR6Q3NLSThjTjV6ellHVlFjUE9wcHAxYnRqNi1HZnNiSi1oTVd3TkxSQVNPYXRUOXJIbWhoZnRVVjYwTTFoTkhJMWp6bDBLZ1Z6Q0rSAXRBVV95cUxOTFlRazRKeDA1ZkRaWlpoYWZGdXowYXdiYTJBa0NMbXhsTFNNNTdkeFhTZUh1RG5KOXlaR1JDZTRGaGx3RXpKQk1LbFdTbHZ3cUZMT21pQ21jSHNOZmNfRE9WUUl5MkNqRnk0SDRwbTNQcGJsXw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
