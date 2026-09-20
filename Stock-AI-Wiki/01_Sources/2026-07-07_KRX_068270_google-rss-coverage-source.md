---
id: source-2026-07-07-KRX-068270-google-rss-coverage
type: source
title: KRX 068270 Google RSS Coverage Source
created: 2026-07-07
updated: 2026-07-07
status: raw
stage: 0

market: KRX
ticker: "068270"
company: 셀트리온
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 068270.
  key_facts:
    - code=068270
    - name=셀트리온
    - naver_article_count=1
    - google_rss_article_count=42
    - kis_title_count=23
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 068270
    - 셀트리온
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

# KRX 068270 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=068270`
- `name=셀트리온`
- `naver_article_count=1`
- `google_rss_article_count=42`
- `kis_title_count=23`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 068270.

## RSS Item Metadata
- Title: `한투증권, 셀트리온 목표가↓…"펀더멘털 개선에도 섹터 약세" - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-07-06T08:24:44+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9EOGduRlpaQWlFdUFWMlVzMXpfTlAwbFZRbFFBLW5oRkhwMFhrekhvdngycXRfVWJwTE56b3BwYXdfQVhCQVFlekJBNXhHaXFMd3ZNMXRrbzBvWm_SAWBBVV95cUxPRTB6azNYU2lTLV9PaWtuZzJsa0VHaS1sTC1xUU9kNnNwSzQwb0o2SUJiZGxqck9qYi15d0NOOUFtU3lOS2FmT0Q2MnNkQ29XTm1Udm85Y0J0MHhZTHM2V0U?oc=5`

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
- Updated at: `2026-07-07T08:05:22+09:00`
- Company: [[KRX_068270_셀트리온]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `한투증권, 셀트리온 목표가↓…"펀더멘털 개선에도 섹터 약세" - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-07-06T08:24:44+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9EOGduRlpaQWlFdUFWMlVzMXpfTlAwbFZRbFFBLW5oRkhwMFhrekhvdngycXRfVWJwTE56b3BwYXdfQVhCQVFlekJBNXhHaXFMd3ZNMXRrbzBvWm_SAWBBVV95cUxPRTB6azNYU2lTLV9PaWtuZzJsa0VHaS1sTC1xUU9kNnNwSzQwb0o2SUJiZGxqck9qYi15d0NOOUFtU3lOS2FmT0Q2MnNkQ29XTm1Udm85Y0J0MHhZTHM2V0U?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
