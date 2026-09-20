---
id: source-2026-05-28-KRX-323280-google-rss-coverage
type: source
title: KRX 323280 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "323280"
company: 태성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 323280.
  key_facts:
    - code=323280
    - name=태성
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 323280
    - 태성
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

# KRX 323280 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=323280`
- `name=태성`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 323280.

## RSS Item Metadata
- Title: `태성, 5월 신규 수주 약 268억원 달성… 中 시장 공략 가속 - 파이낸스스코프`
- Source: `파이낸스스코프`
- Published at: `2026-05-27T08:58:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5haWRlNVdoaXFPR3ktcFNhay1WaXM4WFVnamZFcUlQMS1kNkZEMG51UTJHMlBMVkR2ekVUZm05NHlvQndVbTIxTy1iUjg3OWNCVDdleVl3eWpWWnh2LWxfOUltYTlFSUcxcUE?oc=5`

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
- Updated at: `2026-08-21T19:16:01+09:00`
- Company: [[KRX_323280_태성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `태성, 5월 신규 수주 약 268억원 달성… 中 시장 공략 가속 - 파이낸스스코프`
- Latest observation source: `파이낸스스코프`
- Latest observation published_at: `2026-05-27T08:58:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5haWRlNVdoaXFPR3ktcFNhay1WaXM4WFVnamZFcUlQMS1kNkZEMG51UTJHMlBMVkR2ekVUZm05NHlvQndVbTIxTy1iUjg3OWNCVDdleVl3eWpWWnh2LWxfOUltYTlFSUcxcUE?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
