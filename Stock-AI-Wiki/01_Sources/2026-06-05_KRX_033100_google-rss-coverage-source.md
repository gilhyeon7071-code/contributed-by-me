---
id: source-2026-06-05-KRX-033100-google-rss-coverage
type: source
title: KRX 033100 Google RSS Coverage Source
created: 2026-06-05
updated: 2026-06-05
status: raw
stage: 0

market: KRX
ticker: "033100"
company: 제룡전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Local coverage report row shows news coverage for KRX 033100.
  key_facts:
    - code=033100
    - name=제룡전기
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 033100
    - 제룡전기
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

# KRX 033100 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=033100`
- `name=제룡전기`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 033100.

## RSS Item Metadata
- Title: `제룡전기, 애프터마켓서 10%대 급등 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-05-20T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE42dElVTWRrbi16Y1V1UFMtSllLX1dJVTFvLVlBUnAtTTBYaExCR1JuRlJ5eEp0QzBlcGFwZWFYRlo3ZmRZOE82blQtTnNBSDFSblBuZVFCNThFZ1HSAWBBVV95cUxNeGhnRjAwZ19GZGtqNlBUNkYyVnhlRC1xRkdMUE1DbE01R281MXBrbE5BR3E5bFE2UVZCMlA0Wll1ZXZ2RTdJZnRNUEs3R0tPR2p0bHBWaU05XzRLekQwV0g?oc=5`

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
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_033100_제룡전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `제룡전기, 애프터마켓서 10%대 급등 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-20T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE42dElVTWRrbi16Y1V1UFMtSllLX1dJVTFvLVlBUnAtTTBYaExCR1JuRlJ5eEp0QzBlcGFwZWFYRlo3ZmRZOE82blQtTnNBSDFSblBuZVFCNThFZ1HSAWBBVV95cUxNeGhnRjAwZ19GZGtqNlBUNkYyVnhlRC1xRkdMUE1DbE01R281MXBrbE5BR3E5bFE2UVZCMlA0Wll1ZXZ2RTdJZnRNUEs3R0tPR2p0bHBWaU05XzRLekQwV0g?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
