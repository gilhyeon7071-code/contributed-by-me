---
id: source-2026-05-18-KRX-008490-google-rss-coverage
type: source
title: KRX 008490 Google RSS Coverage Source
created: 2026-05-18
updated: 2026-05-18
status: raw
stage: 0

market: KRX
ticker: "008490"
company: 서흥
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Local coverage report row shows news coverage for KRX 008490.
  key_facts:
    - code=008490
    - name=서흥
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 008490
    - 서흥
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

# KRX 008490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=008490`
- `name=서흥`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 008490.

## RSS Item Metadata
- Title: `서흥, 1분기 영업레버리지 효과 본격화…영업익 84% 급증 - 메디코파마`
- Source: `메디코파마`
- Published at: `2026-05-15T11:02:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE0za3gwaGJKa2tYZGtrQW5Cei1HX2s5Z0p3b0FKeGVjempZQzJxQTJJNmdkNGZfQm9RQVJ0bVZPZnNOZm9fTUNLa1JoMWprQ1BJamJXdlljVG40VWhKckJnUmxZak9lTFNBS1VrUnhNYmtRZw?oc=5`

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
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_008490_서흥]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `서흥, 1분기 영업레버리지 효과 본격화…영업익 84% 급증 - 메디코파마`
- Latest observation source: `메디코파마`
- Latest observation published_at: `2026-05-15T11:02:17+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE0za3gwaGJKa2tYZGtrQW5Cei1HX2s5Z0p3b0FKeGVjempZQzJxQTJJNmdkNGZfQm9RQVJ0bVZPZnNOZm9fTUNLa1JoMWprQ1BJamJXdlljVG40VWhKckJnUmxZak9lTFNBS1VrUnhNYmtRZw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
