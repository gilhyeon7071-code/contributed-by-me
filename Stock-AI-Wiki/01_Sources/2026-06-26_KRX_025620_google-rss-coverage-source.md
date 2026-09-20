---
id: source-2026-06-26-KRX-025620-google-rss-coverage
type: source
title: KRX 025620 Google RSS Coverage Source
created: 2026-06-26
updated: 2026-06-26
status: raw
stage: 0

market: KRX
ticker: "025620"
company: 차AI헬스케어
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 025620.
  key_facts:
    - code=025620
    - name=차AI헬스케어
    - naver_article_count=5
    - google_rss_article_count=8
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 025620
    - 차AI헬스케어
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

# KRX 025620 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=025620`
- `name=차AI헬스케어`
- `naver_article_count=5`
- `google_rss_article_count=8`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 025620.

## RSS Item Metadata
- Title: `[특징주] 차AI헬스케어, 대규모 장기 공급 계약에 상한가 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-23T09:37:30+09:00`
- Link: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE52Unh3a0xHTVd4QW5sOXhGUjRhYm5jRFFnczVobDQtS0EtLXNTU0FTclB4dUpBX0FWelQ5OGdWYUZsRUlPZHc?oc=5`

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
- Updated at: `2026-06-26T02:15:06+09:00`
- Company: [[KRX_025620_차AI헬스케어]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `[특징주] 차AI헬스케어, 대규모 장기 공급 계약에 상한가 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-23T09:37:30+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE52Unh3a0xHTVd4QW5sOXhGUjRhYm5jRFFnczVobDQtS0EtLXNTU0FTclB4dUpBX0FWelQ5OGdWYUZsRUlPZHc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
