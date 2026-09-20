---
id: verification-2026-06-19-KRX-003570-google-rss-coverage
type: verification
title: KRX 003570 Google RSS Coverage Verification
created: 2026-06-19
updated: 2026-06-19
status: verification
stage: 1

market: KRX
ticker: "003570"
company: SNT다이내믹스
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
    - code=003570
    - name=SNT다이내믹스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003570
    - SNT다이내믹스
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

# KRX 003570 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-19_KRX_003570_google-rss-coverage-source]]

## Facts Checked
- `code=003570`
- `name=SNT다이내믹스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SNT다이내믹스, 특허·논문 확대로 ‘K-방산’ 기술 선점 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-16T09:55:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1ZMEhFTDFKeW5vYjRLRE4tSlh2MkFpbjhNX1lTb3p6VmJubUFqQU4tWHE3TmZhMXZTblVPZGVZUS1FaDVFdEJPVTJMTlR0b1k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-19T09:07:46+09:00`
- Company: [[KRX_003570_SNT다이내믹스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `SNT다이내믹스, 특허·논문 확대로 ‘K-방산’ 기술 선점 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-16T09:55:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1ZMEhFTDFKeW5vYjRLRE4tSlh2MkFpbjhNX1lTb3p6VmJubUFqQU4tWHE3TmZhMXZTblVPZGVZUS1FaDVFdEJPVTJMTlR0b1k?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
