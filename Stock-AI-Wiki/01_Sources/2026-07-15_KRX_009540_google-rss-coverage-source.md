---
id: source-2026-07-15-KRX-009540-google-rss-coverage
type: source
title: KRX 009540 Google RSS Coverage Source
created: 2026-07-15
updated: 2026-07-15
status: raw
stage: 0

market: KRX
ticker: "009540"
company: HD한국조선해양
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 009540.
  key_facts:
    - code=009540
    - name=HD한국조선해양
    - naver_article_count=2
    - google_rss_article_count=5
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009540
    - HD한국조선해양
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

# KRX 009540 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=009540`
- `name=HD한국조선해양`
- `naver_article_count=2`
- `google_rss_article_count=5`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 009540.

## RSS Item Metadata
- Title: `HD한국조선해양, 5456억원 규모 '초대형 암모니아 운반선' 3척 수주 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-07-13T17:40:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFBIVzZsU3RVeVVpVVdOWm41a1Z6UlliQS1JejFkT3JySzdzT2lzS20xeEpLWmRHaXJrSU1jYloyZ0Y2SkxEUEFrR2xVenB6bzlUc3NHZUhxbjBBSmFi?oc=5`

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
- Updated at: `2026-07-15T21:05:34+09:00`
- Company: [[KRX_009540_HD한국조선해양]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `HD한국조선해양, 2Q 최대 실적 전망…"조선업 초호황·통합 시너지 효과" - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-07-15T16:08:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE00YUFRSmdBYWR3SnpXakV2dlhYZHAtVHh3OG9FY2dhVXlleTBxVjNCZEd1NGJkNGNEYkR0aWdmOUFoQjhLSmNRaEwxUDFLb1VYWTV6bS1nVmhPT3hzUHdxTg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
