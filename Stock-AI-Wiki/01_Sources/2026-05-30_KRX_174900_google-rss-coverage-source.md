---
id: source-2026-05-30-KRX-174900-google-rss-coverage
type: source
title: KRX 174900 Google RSS Coverage Source
created: 2026-05-30
updated: 2026-05-30
status: raw
stage: 0

market: KRX
ticker: "174900"
company: 앱클론
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 174900.
  key_facts:
    - code=174900
    - name=앱클론
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 174900
    - 앱클론
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

# KRX 174900 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=174900`
- `name=앱클론`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 174900.

## RSS Item Metadata
- Title: `앱클론, 관리종목 족쇄 풀었다…CAR-T·AC101 글로벌 임상 확대 ‘가속’ - 약사공론`
- Source: `약사공론`
- Published at: `2026-03-16T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE0tWXQ0MXhsNmF1UGlyN1Y3anFzdUhQTUFpTXNsQ3dFNmRkZmxLdHVIUWp5N1JZazBBVHprMHNXcUhBMzdxS1lZa255VDg2ZTNkYWJaY2ZtOW5kcnlJd1Ntc19NazRoYU5TQ0hTbg?oc=5`

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
- Updated at: `2026-08-21T19:16:37+09:00`
- Company: [[KRX_174900_앱클론]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-30.json`
- Latest observation title: `앱클론, 관리종목 족쇄 풀었다…CAR-T·AC101 글로벌 임상 확대 ‘가속’ - 약사공론`
- Latest observation source: `약사공론`
- Latest observation published_at: `2026-03-16T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE0tWXQ0MXhsNmF1UGlyN1Y3anFzdUhQTUFpTXNsQ3dFNmRkZmxLdHVIUWp5N1JZazBBVHprMHNXcUhBMzdxS1lZa255VDg2ZTNkYWJaY2ZtOW5kcnlJd1Ntc19NazRoYU5TQ0hTbg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
