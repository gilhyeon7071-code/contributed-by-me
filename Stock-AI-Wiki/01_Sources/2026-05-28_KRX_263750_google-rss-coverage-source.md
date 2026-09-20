---
id: source-2026-05-28-KRX-263750-google-rss-coverage
type: source
title: KRX 263750 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "263750"
company: 펄어비스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 263750.
  key_facts:
    - code=263750
    - name=펄어비스
    - naver_article_count=0
    - google_rss_article_count=23
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 263750
    - 펄어비스
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

# KRX 263750 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=263750`
- `name=펄어비스`
- `naver_article_count=0`
- `google_rss_article_count=23`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 263750.

## RSS Item Metadata
- Title: `펄어비스 주주연대 결집…'단기 주가' vs '장기 IP 가치' 충돌 [이슈 더보기] - 뉴스저널리즘`
- Source: `뉴스저널리즘`
- Published at: `2026-05-27T09:10:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE9vMklrN2V5c2tKZFFHR1dyVk5fMnNzN00tRGswbG9XZTVVY2ZLNTdyMTBzQjR2Uk9rakdwQ0o2bHdoNS1FNWZPRHp1YnRMTVAxMWJZWkZtUjg1Nm5yU2xkek4wb1gzVEUtVlJN?oc=5`

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
- Updated at: `2026-05-28T10:05:07+09:00`
- Company: [[KRX_263750_펄어비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `펄어비스 주주연대 결집…'단기 주가' vs '장기 IP 가치' 충돌 [이슈 더보기] - 뉴스저널리즘`
- Latest observation source: `뉴스저널리즘`
- Latest observation published_at: `2026-05-27T09:10:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE9vMklrN2V5c2tKZFFHR1dyVk5fMnNzN00tRGswbG9XZTVVY2ZLNTdyMTBzQjR2Uk9rakdwQ0o2bHdoNS1FNWZPRHp1YnRMTVAxMWJZWkZtUjg1Nm5yU2xkek4wb1gzVEUtVlJN?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
