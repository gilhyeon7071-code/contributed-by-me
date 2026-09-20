---
id: source-2026-06-11-KRX-035250-google-rss-coverage
type: source
title: KRX 035250 Google RSS Coverage Source
created: 2026-06-11
updated: 2026-06-11
status: raw
stage: 0

market: KRX
ticker: "035250"
company: 강원랜드
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Local coverage report row shows news coverage for KRX 035250.
  key_facts:
    - code=035250
    - name=강원랜드
    - naver_article_count=1
    - google_rss_article_count=8
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 035250
    - 강원랜드
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

# KRX 035250 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=035250`
- `name=강원랜드`
- `naver_article_count=1`
- `google_rss_article_count=8`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 035250.

## RSS Item Metadata
- Title: `강원랜드 사장 선임 앞두고 도계 주민 반발 확산 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-11T11:13:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9hRDJ3UVhKT1k5TmhJdlIzalhUNjhPNmh3WU1MYXJ4QXlDVmlXN3ZVWTM1SUlLUGhJUXk1NDB0bTJtcnVuejZVU2k4VVpjbjA?oc=5`

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
- Updated at: `2026-08-21T19:20:10+09:00`
- Company: [[KRX_035250_강원랜드]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `강원랜드 사장 선임 앞두고 도계 주민 반발 확산 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-11T11:13:35+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9hRDJ3UVhKT1k5TmhJdlIzalhUNjhPNmh3WU1MYXJ4QXlDVmlXN3ZVWTM1SUlLUGhJUXk1NDB0bTJtcnVuejZVU2k4VVpjbjA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
