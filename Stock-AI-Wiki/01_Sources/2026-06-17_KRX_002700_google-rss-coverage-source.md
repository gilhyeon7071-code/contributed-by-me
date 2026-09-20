---
id: source-2026-06-17-KRX-002700-google-rss-coverage
type: source
title: KRX 002700 Google RSS Coverage Source
created: 2026-06-17
updated: 2026-06-17
status: raw
stage: 0

market: KRX
ticker: "002700"
company: 신일전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Local coverage report row shows news coverage for KRX 002700.
  key_facts:
    - code=002700
    - name=신일전자
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 002700
    - 신일전자
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

# KRX 002700 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=002700`
- `name=신일전자`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 002700.

## RSS Item Metadata
- Title: `신일전자, 자기주식 165만주 취득 결정...주가 안정 및 주주가치 제고 - Investing.com 한국어`
- Source: `Investing.com 한국어`
- Published at: `2026-06-17T14:31:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE53ci1Wb2VRV3JncG1ObWFHRkpocTJUUnQ3M18xcWxqaFEwZ3A4NkFTQWNfelRQakM4M3lXdlA5RzRDUC1ZT0pSaXlkaWRnZEtac2J1Zmtucmw5YlBJTjdqMnU0NzJRT2hscGFXX013cTVvNTA?oc=5`

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
- Updated at: `2026-08-21T19:21:46+09:00`
- Company: [[KRX_002700_신일전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `신일전자, 자기주식 165만주 취득 결정...주가 안정 및 주주가치 제고 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-06-17T14:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE53ci1Wb2VRV3JncG1ObWFHRkpocTJUUnQ3M18xcWxqaFEwZ3A4NkFTQWNfelRQakM4M3lXdlA5RzRDUC1ZT0pSaXlkaWRnZEtac2J1Zmtucmw5YlBJTjdqMnU0NzJRT2hscGFXX013cTVvNTA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
