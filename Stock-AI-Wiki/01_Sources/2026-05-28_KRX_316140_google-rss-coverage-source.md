---
id: source-2026-05-28-KRX-316140-google-rss-coverage
type: source
title: KRX 316140 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "316140"
company: 우리금융지주
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 316140.
  key_facts:
    - code=316140
    - name=우리금융지주
    - naver_article_count=3
    - google_rss_article_count=26
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 316140
    - 우리금융지주
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

# KRX 316140 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=316140`
- `name=우리금융지주`
- `naver_article_count=3`
- `google_rss_article_count=26`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 316140.

## RSS Item Metadata
- Title: `금감원 제동에 우리금융·동양생명 완전자회사 작업 차질빚나…"계획대로 진행" - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-05-28T09:02:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBEYjZmYi1Tb0h6a3EwSzcwd1BRZE01M3NfSGNnX2NjemJjbUJmRmRGd2IySlRtakJPRGZyQzFNcHNQU2hTVmp0QnNBMlBQT2hOY3VsYXFXSUxUdUpmRHhQdw?oc=5`

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
- Updated at: `2026-05-28T10:05:07+09:00`
- Company: [[KRX_316140_우리금융지주]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `금감원 제동에 우리금융·동양생명 완전자회사 작업 차질빚나…"계획대로 진행" - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-05-28T09:02:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBEYjZmYi1Tb0h6a3EwSzcwd1BRZE01M3NfSGNnX2NjemJjbUJmRmRGd2IySlRtakJPRGZyQzFNcHNQU2hTVmp0QnNBMlBQT2hOY3VsYXFXSUxUdUpmRHhQdw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
