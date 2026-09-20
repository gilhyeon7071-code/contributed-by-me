---
id: verification-2026-07-21-KRX-034020-google-rss-coverage
type: verification
title: KRX 034020 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=2
    - google_rss_article_count=14
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_034020_google-rss-coverage-source]]

## Facts Checked
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=2`
- `google_rss_article_count=14`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `두산에너빌, '에너지 빅사이클'에 터빈·원전 '쌍끌이' 기대↑ - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-07-20T09:05:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9lX0l5bGY4X2NVN29kMHVVOG1xNWxVcEx4S0VEQTBTZWZtMDhfOE9CclBOWXBJeGZIYy1nOTg1VV9IbEFBdXVrQjBkRFUtT1JiNzU1aU56OWtvNXZaY1FGYm1LYWwtRnBPZ1BqNXljSFg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-21T21:05:15+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `두산에너빌, '에너지 빅사이클'에 터빈·원전 '쌍끌이' 기대↑ - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-07-20T09:05:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9lX0l5bGY4X2NVN29kMHVVOG1xNWxVcEx4S0VEQTBTZWZtMDhfOE9CclBOWXBJeGZIYy1nOTg1VV9IbEFBdXVrQjBkRFUtT1JiNzU1aU56OWtvNXZaY1FGYm1LYWwtRnBPZ1BqNXljSFg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
