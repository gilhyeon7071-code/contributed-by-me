---
id: source-2026-07-28-KRX-018260-google-rss-coverage
type: source
title: KRX 018260 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 018260.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 018260.

## RSS Item Metadata
- Title: `네이버·삼성SDS, 빅테크와 빅사이즈 협력 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-27T04:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE5NNndhQ0hwOVhoN21FOXA1WlUxcFlwSnhueFFYRnBJcVFFcnZ4UExGMGp1THNKOHZ0YldudThLQVpfdFRieTJ5ZnMtdGI0TVh1VjNpU2x1SlRfYzNYZEVmUjloYzlWcFHSAWxBVV95cUxOTWptbzFPOEwzVTVLRWZOd0RqMXpVQkx3UHZ5STJqOTFvWmEtUE5JNmxuSnpHYVJZcEFsblloaWpwb2VJS1BOMFZVWG1RZDlHYXhfd0cyVWU0ZHRPOWxCb3lYanFUT3VkekpzMkc?oc=5`

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
- Updated at: `2026-07-28T13:06:41+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `네이버·삼성SDS, 빅테크와 빅사이즈 협력 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-27T04:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE5NNndhQ0hwOVhoN21FOXA1WlUxcFlwSnhueFFYRnBJcVFFcnZ4UExGMGp1THNKOHZ0YldudThLQVpfdFRieTJ5ZnMtdGI0TVh1VjNpU2x1SlRfYzNYZEVmUjloYzlWcFHSAWxBVV95cUxOTWptbzFPOEwzVTVLRWZOd0RqMXpVQkx3UHZ5STJqOTFvWmEtUE5JNmxuSnpHYVJZcEFsblloaWpwb2VJS1BOMFZVWG1RZDlHYXhfd0cyVWU0ZHRPOWxCb3lYanFUT3VkekpzMkc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
