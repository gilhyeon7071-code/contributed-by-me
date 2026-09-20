---
id: source-2026-06-19-KRX-039200-google-rss-coverage
type: source
title: KRX 039200 Google RSS Coverage Source
created: 2026-06-19
updated: 2026-06-19
status: raw
stage: 0

market: KRX
ticker: "039200"
company: 오스코텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Local coverage report row shows news coverage for KRX 039200.
  key_facts:
    - code=039200
    - name=오스코텍
    - naver_article_count=1
    - google_rss_article_count=12
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039200
    - 오스코텍
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

# KRX 039200 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=039200`
- `name=오스코텍`
- `naver_article_count=1`
- `google_rss_article_count=12`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 039200.

## RSS Item Metadata
- Title: `[종목+] 오스코텍 주가 왜 빠졌나…1조 계약 뒤에 남은 임상 변수 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-06-19T08:05:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE9sUlJGNlJUVGpxVmNRSUhISDlkcWNaU0R5Q3UtdW5xZzdCMk16MTF2cy1KclA3bjhockREVThya1VXcmdIekpFbGJyNEJjZWFKS01LeHczQlZOR0xP?oc=5`

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
- Updated at: `2026-06-19T09:07:46+09:00`
- Company: [[KRX_039200_오스코텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `[종목+] 오스코텍 주가 왜 빠졌나…1조 계약 뒤에 남은 임상 변수 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-06-19T08:05:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE9sUlJGNlJUVGpxVmNRSUhISDlkcWNaU0R5Q3UtdW5xZzdCMk16MTF2cy1KclA3bjhockREVThya1VXcmdIekpFbGJyNEJjZWFKS01LeHczQlZOR0xP?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
