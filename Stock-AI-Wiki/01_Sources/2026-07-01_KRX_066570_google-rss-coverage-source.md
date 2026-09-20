---
id: source-2026-07-01-KRX-066570-google-rss-coverage
type: source
title: KRX 066570 Google RSS Coverage Source
created: 2026-07-01
updated: 2026-07-01
status: raw
stage: 0

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 066570.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=2
    - google_rss_article_count=10
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=066570`
- `name=LG전자`
- `naver_article_count=2`
- `google_rss_article_count=10`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066570.

## RSS Item Metadata
- Title: `LG전자, TSMC 공정 기반 ASIC 디자인 서비스 본격화 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-06-30T17:30:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE9jWXN2UHRrdnR2QWM5TEkxdHdlVWpCNzdGdDQ4ajI2U0lySlZNdGVsV1k3RE11cWoyMVpqcl8wc2JNMG1odzBsX2NXVnpoZ1lfeUM2ajh3?oc=5`

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
- Updated at: `2026-08-21T19:25:55+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `LG전자, TSMC 공정 기반 ASIC 디자인 서비스 본격화 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-06-30T17:30:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE9jWXN2UHRrdnR2QWM5TEkxdHdlVWpCNzdGdDQ4ajI2U0lySlZNdGVsV1k3RE11cWoyMVpqcl8wc2JNMG1odzBsX2NXVnpoZ1lfeUM2ajh3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
