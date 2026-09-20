---
id: source-2026-06-19-KRX-028670-google-rss-coverage
type: source
title: KRX 028670 Google RSS Coverage Source
created: 2026-06-19
updated: 2026-06-19
status: raw
stage: 0

market: KRX
ticker: "028670"
company: 팬오션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Local coverage report row shows news coverage for KRX 028670.
  key_facts:
    - code=028670
    - name=팬오션
    - naver_article_count=0
    - google_rss_article_count=5
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028670
    - 팬오션
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

# KRX 028670 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=028670`
- `name=팬오션`
- `naver_article_count=0`
- `google_rss_article_count=5`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 028670.

## RSS Item Metadata
- Title: `팬오션, SK에너지와 2조4711억원 원유 운송 계약 - 리드경제`
- Source: `리드경제`
- Published at: `2026-06-19T14:26:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1GOWhKZkxpVWMtQnQ1RTV1RHhHMDZhWlJqZFpOdW9BOTQ5WVpEODBsR1hGSnlwRmpvTW5LSTdJSzAyQzBQX21sZjNMQWdYaXZKVDZTcmxDUFlWRzZMMDlBVVpkbG8tMVR2dzNWOFRYcw?oc=5`

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
- Updated at: `2026-08-21T19:22:23+09:00`
- Company: [[KRX_028670_팬오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `팬오션, SK에너지와 2조4711억원 원유 운송 계약 - 리드경제`
- Latest observation source: `리드경제`
- Latest observation published_at: `2026-06-19T14:26:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1GOWhKZkxpVWMtQnQ1RTV1RHhHMDZhWlJqZFpOdW9BOTQ5WVpEODBsR1hGSnlwRmpvTW5LSTdJSzAyQzBQX21sZjNMQWdYaXZKVDZTcmxDUFlWRzZMMDlBVVpkbG8tMVR2dzNWOFRYcw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
