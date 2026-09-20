---
id: source-2026-06-01-KRX-064400-google-rss-coverage
type: source
title: KRX 064400 Google RSS Coverage Source
created: 2026-06-01
updated: 2026-06-01
status: raw
stage: 0

market: KRX
ticker: "064400"
company: LG씨엔에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 064400.
  key_facts:
    - code=064400
    - name=LG씨엔에스
    - naver_article_count=1
    - google_rss_article_count=65
    - kis_title_count=49
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 064400
    - LG씨엔에스
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

# KRX 064400 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=064400`
- `name=LG씨엔에스`
- `naver_article_count=1`
- `google_rss_article_count=65`
- `kis_title_count=49`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 064400.

## RSS Item Metadata
- Title: `“LG CNS, AI 클라우드 고성장·에이전틱웍스 플랫폼 매출 확대” 목표가↑ - 매일경제`
- Source: `매일경제`
- Published at: `2026-05-29T08:08:47+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE55WU9rbzBnZm5PSk9IVVMzYVp2dDNueGdWUS03anBGcEhEMWR6azRBZmJzNm1uR3RQRi1pcEIwWVk5SnpBeHdjajdycFI0dWdDcEE?oc=5`

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
- Updated at: `2026-06-01T08:42:44+09:00`
- Company: [[KRX_064400_LG씨엔에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `“LG CNS, AI 클라우드 고성장·에이전틱웍스 플랫폼 매출 확대” 목표가↑ - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-05-29T08:08:47+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE55WU9rbzBnZm5PSk9IVVMzYVp2dDNueGdWUS03anBGcEhEMWR6azRBZmJzNm1uR3RQRi1pcEIwWVk5SnpBeHdjajdycFI0dWdDcEE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
