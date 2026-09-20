---
id: source-2026-07-29-KRX-214450-google-rss-coverage
type: source
title: KRX 214450 Google RSS Coverage Source
created: 2026-07-29
updated: 2026-07-29
status: raw
stage: 0

market: KRX
ticker: "214450"
company: 파마리서치
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 214450.
  key_facts:
    - code=214450
    - name=파마리서치
    - naver_article_count=1
    - google_rss_article_count=20
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 214450
    - 파마리서치
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

# KRX 214450 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=214450`
- `name=파마리서치`
- `naver_article_count=1`
- `google_rss_article_count=20`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 214450.

## RSS Item Metadata
- Title: `파마리서치메디케어, '리본 심포지엄'…팜뷰티 성장 전략 공유 - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-28T09:17:41+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFB2TEJJdGMyQkgzdnR3OGotMjhQX2x4N296SkJzQW1wa2dBUEhndXA2V1RFUUFsR04yNTloNmhqT3dRbGYxenJsRWdpYVNzUjBycWpqZVQ0eHg3a3BENmlfNg?oc=5`

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
- Updated at: `2026-08-21T19:32:53+09:00`
- Company: [[KRX_214450_파마리서치]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `파마리서치메디케어, '리본 심포지엄'…팜뷰티 성장 전략 공유 - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-07-28T09:17:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFB2TEJJdGMyQkgzdnR3OGotMjhQX2x4N296SkJzQW1wa2dBUEhndXA2V1RFUUFsR04yNTloNmhqT3dRbGYxenJsRWdpYVNzUjBycWpqZVQ0eHg3a3BENmlfNg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
