---
id: source-2026-08-18-KRX-024840-google-rss-coverage
type: source
title: KRX 024840 Google RSS Coverage Source
created: 2026-08-18
updated: 2026-08-18
status: raw
stage: 0

market: KRX
ticker: "024840"
company: KBI메탈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Local coverage report row shows news coverage for KRX 024840.
  key_facts:
    - code=024840
    - name=KBI메탈
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024840
    - KBI메탈
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

# KRX 024840 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=024840`
- `name=KBI메탈`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 024840.

## RSS Item Metadata
- Title: `상반기 최대 실적 KBI메탈…영업익 전년比 813% 증가 - 철강금속신문`
- Source: `철강금속신문`
- Published at: `2026-08-13T16:24:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9CS3hhbHRQQ3pwaWI5UnNrSnZ4QmdSWHd6QWpqbDNtQXdrLWZrY1RNckdweTNxZk5IYTh5ZHAyWjF1ckJoMlB3aVhpLU5Rb19hblJmOGVRank0XzRBVzk4UG5HRG5yUFJNUnc?oc=5`

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
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_024840_KBI메탈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `상반기 최대 실적 KBI메탈…영업익 전년比 813% 증가 - 철강금속신문`
- Latest observation source: `철강금속신문`
- Latest observation published_at: `2026-08-13T16:24:58+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9CS3hhbHRQQ3pwaWI5UnNrSnZ4QmdSWHd6QWpqbDNtQXdrLWZrY1RNckdweTNxZk5IYTh5ZHAyWjF1ckJoMlB3aVhpLU5Rb19hblJmOGVRank0XzRBVzk4UG5HRG5yUFJNUnc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
