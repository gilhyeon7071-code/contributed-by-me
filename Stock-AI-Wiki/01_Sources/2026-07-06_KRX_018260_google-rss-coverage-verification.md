---
id: verification-2026-07-06-KRX-018260-google-rss-coverage
type: verification
title: KRX 018260 Google RSS Coverage Verification
created: 2026-07-06
updated: 2026-07-06
status: verification
stage: 1

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=9
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-06_KRX_018260_google-rss-coverage-source]]

## Facts Checked
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=1`
- `google_rss_article_count=9`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독]삼성, 반도체 공정에 양자컴 심는다 - 서울경제신문`
- Source: `서울경제신문`
- Published at: `2026-07-01T17:40:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE05SDdiMlA1NWxwcHVOS09objVqSnBVeWhPZ1QxNmd6cHgwRngzTUppYXJGRVZ2WE95SFlYdFA1NmlEa1hSRFFIMzhESURuQVRxcHfSAVNBVV95cUxNMWNLd3Flb3BjWUg3Q25YWkdiRUt5ZzRJR0l5b05xeFExSnZrdDNPVUNabUFLeEFkMU1MM3lsQ0xKSVpSZW5uWWdWUGtBVVE3Z2djUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-06T08:05:14+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `삼성SDS, AI 데이터센터 성장 가속…목표가↑-iM증권 - 이데일리`
- Latest observation source: `이데일리`
- Latest observation published_at: `2026-07-06T07:52:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigAFBVV95cUxQNjl3VlpCeXlhYmo5VUtsOGZmZ2s5cldBb3ZzbHZGS3pFcVVJazNfVGlaaktxbzRMSFBvY0FZS2ZoOVpUYnFoV0tSb01mMkdLb3NKZ2Q1T0JHZTZmbWFSSWhDQm03dmQ3TlM3WEdsQWtKVm5lQmRidnNtcHdKUkg4Uw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
