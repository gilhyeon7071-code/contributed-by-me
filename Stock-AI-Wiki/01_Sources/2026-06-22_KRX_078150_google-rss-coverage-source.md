---
id: source-2026-06-22-KRX-078150-google-rss-coverage
type: source
title: KRX 078150 Google RSS Coverage Source
created: 2026-06-22
updated: 2026-06-22
status: raw
stage: 0

market: KRX
ticker: "078150"
company: HB테크놀러지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-22

analysis:
  summary: Local coverage report row shows news coverage for KRX 078150.
  key_facts:
    - code=078150
    - name=HB테크놀러지
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 078150
    - HB테크놀러지
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

# KRX 078150 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=078150`
- `name=HB테크놀러지`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 078150.

## RSS Item Metadata
- Title: `특징주, HB테크놀러지-유리 기판 테마 상승세에 7.11% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-19T09:03:30+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1nY2pNUzZlblFGaVNYeGtCWEtXaGFkTktINjU1Tm5jZWFraFhOSXV6MEdtQjhKQXJfMVZUSnVrX3BDMzZENjd2WE1JMkN0ZmRBMHc?oc=5`

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
- Updated at: `2026-06-22T17:05:39+09:00`
- Company: [[KRX_078150_HB테크놀러지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-22.json`
- Latest observation title: `김윤혁 상무이사, HB테크놀러지 주식 2만주 매도 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-06-22T16:41:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBteXBtcXRuMk90Mi11dTdRRGZvTmVPekVwOU5RVUU5d1l2eTgzcG9ta3ZTZU9lOXNDQmJhTDMzX09WQ09BNEFJd19JTXlpaS1GVnN6WTBYb2Z2QUNrWUdXZU1oVV81aTdZRmpFcUJOMTl0UlE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
