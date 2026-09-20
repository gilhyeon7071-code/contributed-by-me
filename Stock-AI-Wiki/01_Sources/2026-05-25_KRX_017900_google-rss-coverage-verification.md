---
id: verification-2026-05-25-KRX-017900-google-rss-coverage
type: verification
title: KRX 017900 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "017900"
company: 광전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=017900
    - name=광전자
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017900
    - 광전자
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

# KRX 017900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_017900_google-rss-coverage-source]]

## Facts Checked
- `code=017900`
- `name=광전자`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `광전자 주가, 급등세... 무슨 기업이길래? - 금강일보`
- Source: `금강일보`
- Published at: `2026-05-20T10:20:54+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE02MGhxcGl6b0Yya0p6a2ZHc2tWdVdFR1ZIbkJRYlJSQy1NWlhQOThEaHRzYW1IQzZFQk5HZXJncG5LaVhzbXJudWU4YmVoMTZia214VkNzLTBZbnNHRzlISXpMYzhFSm5yRnc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_017900_광전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `광전자 주가, 급등세... 무슨 기업이길래? - 금강일보`
- Latest observation source: `금강일보`
- Latest observation published_at: `2026-05-20T10:20:54+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE02MGhxcGl6b0Yya0p6a2ZHc2tWdVdFR1ZIbkJRYlJSQy1NWlhQOThEaHRzYW1IQzZFQk5HZXJncG5LaVhzbXJudWU4YmVoMTZia214VkNzLTBZbnNHRzlISXpMYzhFSm5yRnc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
