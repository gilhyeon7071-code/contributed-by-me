---
id: verification-2026-06-19-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-19
updated: 2026-06-19
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=1
    - google_rss_article_count=153
    - kis_title_count=26
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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
- [[2026-06-19_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=1`
- `google_rss_article_count=153`
- `kis_title_count=26`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대건설, 웨스팅하우스와 네덜란드서 원전 건설 심포지엄 개최 - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-06-18T16:32:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE43OXVRaUNBTXZERktRdzgwb3BZQWFELXFNNkVmeVB3OHJZcU9XSHBpb0lFZFk2Ny02NlhfV1U0QVkyVlJtMkRVZnFURUdtdVZ5eVFGczlseEMwSjNGOXpPQ04xSlpDT2E2TW04cHp0WFJXTDg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:23+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `현대건설·남동발전, 석탄화력발전소 SMR 전환기술 공동개발 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-06-19T14:18:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9KUEdEajduRGNOQjdNbUVVaFNnbE5McnNXNGRaZ01Ka2VmdFRuVzdkZjA1Q2RwbXRjc0hPTElXNHBKWTZUdTZiMHYzYzY5XzVCcFF0V05aRjZxVzDSAWBBVV95cUxNYjlfODZQZlZWVXd1Zl82RFJXeGk1d1IyMjA0WWtrSTJkdW1NTWpqTnYwckw5WEV2aW5SemdQUnRyc21SaTJCVWtTUzVfaGs5dllFeHBLU3FqdGstZUhjY2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
