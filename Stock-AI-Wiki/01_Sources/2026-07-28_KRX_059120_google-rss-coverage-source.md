---
id: source-2026-07-28-KRX-059120-google-rss-coverage
type: source
title: KRX 059120 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "059120"
company: 아진엑스텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 059120.
  key_facts:
    - code=059120
    - name=아진엑스텍
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 059120
    - 아진엑스텍
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

# KRX 059120 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=059120`
- `name=아진엑스텍`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 059120.

## RSS Item Metadata
- Title: `아진엑스텍(059120)저점을 줄때마다 물량 모아둘 기회로 보이며 이후 전망 및 대응전략. - ThinkPool`
- Source: `ThinkPool`
- Published at: `2026-07-27T13:51:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE4yU05NQWJ0NDJZUDlnaWhObHpiUENNaTh4TEFQbU5ZTXZTNHNlNzE2Sll6U2NzUjJIcHhvQ1JiQThKLWtwakQ5Uk5fVjJGRGRpcWZDbk1iVjF5cG95?oc=5`

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
- Updated at: `2026-07-28T21:05:05+09:00`
- Company: [[KRX_059120_아진엑스텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `아진엑스텍(059120)저점을 줄때마다 물량 모아둘 기회로 보이며 이후 전망 및 대응전략. - ThinkPool`
- Latest observation source: `ThinkPool`
- Latest observation published_at: `2026-07-27T13:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE4yU05NQWJ0NDJZUDlnaWhObHpiUENNaTh4TEFQbU5ZTXZTNHNlNzE2Sll6U2NzUjJIcHhvQ1JiQThKLWtwakQ5Uk5fVjJGRGRpcWZDbk1iVjF5cG95?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
