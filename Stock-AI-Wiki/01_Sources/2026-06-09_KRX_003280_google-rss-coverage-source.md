---
id: source-2026-06-09-KRX-003280-google-rss-coverage
type: source
title: KRX 003280 Google RSS Coverage Source
created: 2026-06-09
updated: 2026-06-09
status: raw
stage: 0

market: KRX
ticker: "003280"
company: 흥아해운
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 003280.
  key_facts:
    - code=003280
    - name=흥아해운
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003280
    - 흥아해운
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

# KRX 003280 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003280`
- `name=흥아해운`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003280.

## RSS Item Metadata
- Title: `흥아해운, 中 우창조선에 탱커 최대 6척 발주…10년 만에 신규 발주 - ebn.co.kr`
- Source: `ebn.co.kr`
- Published at: `2026-06-08T08:29:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBHR3F5SHVVRE0zQnVTbUdLQjI3Tm4tclFHRmFNempxbnQ4alNwUTQ5YVNtdHRZWmV3RUYyTGxyaDY2ZVExek4zRm1IdHlES3BWeFAyX3lucVpPQndYSTZzY3RwTWpfUENs?oc=5`

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
- Updated at: `2026-08-21T19:19:32+09:00`
- Company: [[KRX_003280_흥아해운]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `흥아해운, 26K 케미컬 탱커 3척 발주 - 한국해운신문`
- Latest observation source: `한국해운신문`
- Latest observation published_at: `2026-06-09T17:26:26+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1WOHB6eURIVjREWkZlbGM0Rm8wTUtQb0ZwYXg2OUVoa3ZPNjVHNXB0MzlyM0VVcC1EM1FWWGNlUWVKbFFlcFFQalNpVms5SjJ1R3VhQjh1ZXluN0Z1N0wyd1B4ZzRvZzYxT05INGJROTNWVzA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
