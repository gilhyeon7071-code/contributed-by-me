---
id: source-2026-06-24-KRX-039030-google-rss-coverage
type: source
title: KRX 039030 Google RSS Coverage Source
created: 2026-06-24
updated: 2026-06-24
status: raw
stage: 0

market: KRX
ticker: "039030"
company: 이오테크닉스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-24

analysis:
  summary: Local coverage report row shows news coverage for KRX 039030.
  key_facts:
    - code=039030
    - name=이오테크닉스
    - naver_article_count=1
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
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 039030 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=039030`
- `name=이오테크닉스`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 039030.

## RSS Item Metadata
- Title: `김보라 시장, 첨단 반도체 소부장 기업 ㈜이오테크닉스 방문 - 자치안성신문`
- Source: `자치안성신문`
- Published at: `2026-06-21T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5OWVBzSy02Y0FSd3dBQUZuemVFRm1zanhhVnU5UXZWb3VaWFUySnQ3THF3d1Ezb05yNEJhLWU2em01djhWWS1zYmlNZFgwcHl4emZnZmxFWkZRemFFdlhiMkMyU3Z2VUtRalBYbmhhdlY5RjIwSy12aEhGMFE?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-24T13:05:29+09:00`
- Company: [[KRX_039030_이오테크닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-24.json`
- Latest observation title: `김보라 시장, 첨단 반도체 소부장 기업 ㈜이오테크닉스 방문 - 자치안성신문`
- Latest observation source: `자치안성신문`
- Latest observation published_at: `2026-06-21T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5OWVBzSy02Y0FSd3dBQUZuemVFRm1zanhhVnU5UXZWb3VaWFUySnQ3THF3d1Ezb05yNEJhLWU2em01djhWWS1zYmlNZFgwcHl4emZnZmxFWkZRemFFdlhiMkMyU3Z2VUtRalBYbmhhdlY5RjIwSy12aEhGMFE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
