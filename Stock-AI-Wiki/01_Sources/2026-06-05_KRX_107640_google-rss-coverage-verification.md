---
id: verification-2026-06-05-KRX-107640-google-rss-coverage
type: verification
title: KRX 107640 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "107640"
company: 한중엔시에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=107640
    - name=한중엔시에스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 107640
    - 한중엔시에스
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 107640 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_107640_google-rss-coverage-source]]

## Facts Checked
- `code=107640`
- `name=한중엔시에스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `한중엔시에스, 북미 공장 착공...ESS 수냉 냉각으로 시장 선점 - 디일렉`
- Source: `디일렉`
- Published at: `2026-04-24T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9tNUFDSl91aWVQck0xejhZbkNKaE5iVXVZQTZpTGVjYUVZWHdwaUlnYzQ1bFZsRlhfUHNyaDUzYlllR21leHc1NzQtQkp0ZGNnVEJicS16Y2g2bGN1aHhFcWh2eUs0QQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:19+09:00`
- Company: [[KRX_107640_한중엔시에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `한중엔시에스, 북미 공장 착공...ESS 수냉 냉각으로 시장 선점 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-04-24T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9tNUFDSl91aWVQck0xejhZbkNKaE5iVXVZQTZpTGVjYUVZWHdwaUlnYzQ1bFZsRlhfUHNyaDUzYlllR21leHc1NzQtQkp0ZGNnVEJicS16Y2g2bGN1aHhFcWh2eUs0QQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
