---
id: verification-2026-06-26-KRX-006800-google-rss-coverage
type: verification
title: KRX 006800 Google RSS Coverage Verification
created: 2026-06-26
updated: 2026-06-26
status: verification
stage: 1

market: KRX
ticker: "006800"
company: 미래에셋증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=006800
    - name=미래에셋증권
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 006800
    - 미래에셋증권
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

# KRX 006800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-26_KRX_006800_google-rss-coverage-source]]

## Facts Checked
- `code=006800`
- `name=미래에셋증권`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[게시판] 미래에셋증권, 투자센터여의도WM 이전 오픈 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-06-25T14:35:31+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9FczZ3WWVfbG1Ga0tuQjR4ZkktaXJfRy13c0ZYME10OF9ZNVlVamZuT2JqSTk4emRpLV9KTU9fQnFYRy1jdzNWY3p2dm14WDJWS0Y4clRyYWdKRk3SAWBBVV95cUxNYnFFUVJVenQtdkNVNV9hZncxQUdYc29wQmRTY3U4ZHVsdkJnOGRQbm1UdnlteUoybG5MV0VEQXJLNE54Vm1HNEpwUXZ3U0ZzSEpOWVhsLUQwVDRxaW85ZGU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_006800_미래에셋증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `'총수 일가 골프장만 이용 원칙' 미래에셋 계열사들 무죄 확정(종합) - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-06-25T18:13:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE1HT25rTnZaVlFjRWdXNVoxdXEyUktXQXR2cnZoT1RvRVRzNzlDaHlFaUdqN3hRRU5IWXByaDZTYlJiSHc0RHFqVXRydWZ3UFJZR18talFwSGMyUU3SAWBBVV95cUxOZWRNYldKTXNjdEdnYllQQ25NWkhWYnA5V0kyRF81MTRodjRqSFlRZDZMNXRvcnhyeGdRdkJNSzRyQ2xtUjFRUGZvYjN4R0hheWVzeFJtdXAyME5uWGlRM3M?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
