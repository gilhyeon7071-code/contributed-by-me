---
id: source-2026-07-01-KRX-007660-google-rss-coverage
type: source
title: KRX 007660 Google RSS Coverage Source
created: 2026-07-01
updated: 2026-07-01
status: raw
stage: 0

market: KRX
ticker: "007660"
company: 이수페타시스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 007660.
  key_facts:
    - code=007660
    - name=이수페타시스
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 007660
    - 이수페타시스
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

# KRX 007660 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=007660`
- `name=이수페타시스`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 007660.

## RSS Item Metadata
- Title: `AI 서버 떠받친 초고다층기판, 이수페타시스 1조 올라섰다 - jabon.co.kr`
- Source: `jabon.co.kr`
- Published at: `2026-06-30T13:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTFBwTXo1RnhISkhlTkRINmMwM3RubW14UFIweTVKNEZzb1k1czNVZHNYbUtoc001YjdjV21NV2RCeFBLejF3TzdOQWRIRzlqdG4tdmZEdEZCTGlGeGhFX1BIWkdJdTV0Z2c?oc=5`

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
- Updated at: `2026-07-01T11:05:15+09:00`
- Company: [[KRX_007660_이수페타시스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `특징주, 이수페타시스-5G 테마 상승세에 5.81% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-07-01T10:12:52+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE05U0VVQkY3RHMzOVZwa25LRk9YNktYM0RJR21GMXJYdmVtLUt6TWdGak9xb05IOVZ3NjJkLVJ5MjN5TkNsSnppU2hKS2s4bnlUUUE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
