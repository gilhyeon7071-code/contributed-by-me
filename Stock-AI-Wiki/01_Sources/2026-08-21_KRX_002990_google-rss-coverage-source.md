---
id: source-2026-08-21-KRX-002990-google-rss-coverage
type: source
title: KRX 002990 Google RSS Coverage Source
created: 2026-08-21
updated: 2026-08-21
status: raw
stage: 0

market: KRX
ticker: "002990"
company: 금호건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 002990.
  key_facts:
    - code=002990
    - name=금호건설
    - naver_article_count=1
    - google_rss_article_count=11
    - kis_title_count=21
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 002990
    - 금호건설
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

# KRX 002990 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=002990`
- `name=금호건설`
- `naver_article_count=1`
- `google_rss_article_count=11`
- `kis_title_count=21`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 002990.

## RSS Item Metadata
- Title: `원가 부담 던 금호건설…상반기 영업이익 '증가' - 신아일보`
- Source: `신아일보`
- Published at: `2026-08-20T09:43:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9QTndSNk1PemFMZWJsSXFzLWVPNVFEUG8zLVlWQ3FiLTRHdHowVHR2dUdOVnFZSTB4UG00VDR6QWlXRG13RTZwSHk2OWhyRmhwZ2J3eGZQV21YZFBZcUZobmtERE12YTloNl8zQ0ZuTk8?oc=5`

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
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_002990_금호건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `금호건설, 삼성전자 ‘플랙트 한국공장’ 수주…광주서 2028년 준공 - 데일리안`
- Latest observation source: `데일리안`
- Latest observation published_at: `2026-08-21T18:24:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMirgJBVV95cUxNTTQ5S3JYczFRTG11WGhrUGprakh2U3cybERnT0xlR3QtVjBtY0ZqaTRWQWtYWkUtSWMwSGFrR3FsOENGSzVqZVJqSTBhdUhDYl9fcGF5UjRfa0pLZUpzYlVGV3h1Z3psaEljVWpDWGVxaEczakJDbGI0UUtUQ3JaTUUxM0VvY2tQWWFzSGxNTUVlYWRKTVAyeXBVN3FlbnJWNEU4ZkdjNWN5ck84RDQ5enRQOEVmQzVVaG1JWi1iZks0UUd5U01QSXBXLXRiTEcwVnFIemdOLTNUYkJQQ3JhLUllalFnbk5vTGFSU25HYkx2ZkJSNklWRjI3WW1PNmQ3eXJxR2hxNXU5dUc1bExQQURUaEphUnJGZVJURmZMaTlxNEVEQTJocDZSVUdhZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
