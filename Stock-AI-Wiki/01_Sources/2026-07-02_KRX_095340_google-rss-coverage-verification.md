---
id: verification-2026-07-02-KRX-095340-google-rss-coverage
type: verification
title: KRX 095340 Google RSS Coverage Verification
created: 2026-07-02
updated: 2026-07-02
status: verification
stage: 1

market: KRX
ticker: "095340"
company: ISC
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=095340
    - name=ISC
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 095340
    - ISC
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

# KRX 095340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-02_KRX_095340_google-rss-coverage-source]]

## Facts Checked
- `code=095340`
- `name=ISC`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `ISC(095340)저점을 줄때마다 물량 모아둘 기회로 보이며 이후 전망 및 대응전략. - ThinkPool`
- Source: `ThinkPool`
- Published at: `2026-07-01T14:24:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5FVGZrZEZWTXFEaWh0RkxGeUFjejMtRVJrTmc3eklyQjI1aEpMZHZfbGx3X1J2RmVWdUJVaTc0WXRIY0puaXhuOGtBc0I4RVpTQ2VqSVZvamx2NGx6?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:14+09:00`
- Company: [[KRX_095340_ISC]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `01일, 외국인 코스닥에서 리노공업(-2.74%), ISC(-5.95%) 등 순매수 - 씽크풀 AI`
- Latest observation source: `씽크풀 AI`
- Latest observation published_at: `2026-07-02T05:38:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBxZlltSFdSbjgta1dqMHUyMHFKWHRlX2dQU1ZwcHJMUEgzUDg3aG4wWXJUY056eUZCbElMaDlISmN6WENhUmcybF94Y0g4M0g3V1lXLTJIYXRHQzNnYkpYN1c1YnhhVjVj?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
