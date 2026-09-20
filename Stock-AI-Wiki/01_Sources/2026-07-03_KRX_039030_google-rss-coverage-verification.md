---
id: verification-2026-07-03-KRX-039030-google-rss-coverage
type: verification
title: KRX 039030 Google RSS Coverage Verification
created: 2026-07-03
updated: 2026-07-03
status: verification
stage: 1

market: KRX
ticker: "039030"
company: 이오테크닉스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=039030
    - name=이오테크닉스
    - naver_article_count=3
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 039030
    - 이오테크닉스
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

# KRX 039030 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-03_KRX_039030_google-rss-coverage-source]]

## Facts Checked
- `code=039030`
- `name=이오테크닉스`
- `naver_article_count=3`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `김보라 시장, 첨단 반도체 소부장 기업 ㈜이오테크닉스 방문 - 자치안성신문`
- Source: `자치안성신문`
- Published at: `2026-07-01T15:56:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5OWVBzSy02Y0FSd3dBQUZuemVFRm1zanhhVnU5UXZWb3VaWFUySnQ3THF3d1Ezb05yNEJhLWU2em01djhWWS1zYmlNZFgwcHl4emZnZmxFWkZRemFFdlhiMkMyU3Z2VUtRalBYbmhhdlY5RjIwSy12aEhGMFE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:32+09:00`
- Company: [[KRX_039030_이오테크닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-03.json`
- Latest observation title: `김보라 시장, 첨단 반도체 소부장 기업 ㈜이오테크닉스 방문 - 자치안성신문`
- Latest observation source: `자치안성신문`
- Latest observation published_at: `2026-07-01T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5OWVBzSy02Y0FSd3dBQUZuemVFRm1zanhhVnU5UXZWb3VaWFUySnQ3THF3d1Ezb05yNEJhLWU2em01djhWWS1zYmlNZFgwcHl4emZnZmxFWkZRemFFdlhiMkMyU3Z2VUtRalBYbmhhdlY5RjIwSy12aEhGMFE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
