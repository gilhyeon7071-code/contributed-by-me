---
id: verification-2026-05-27-KRX-018000-google-rss-coverage
type: verification
title: KRX 018000 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "018000"
company: 유니슨
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018000
    - name=유니슨
    - naver_article_count=5
    - google_rss_article_count=15
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018000
    - 유니슨
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

# KRX 018000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_018000_google-rss-coverage-source]]

## Facts Checked
- `code=018000`
- `name=유니슨`
- `naver_article_count=5`
- `google_rss_article_count=15`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `유니슨, 10MW급 국산 해상풍력터빈 실증기 설치 - 에너지경제신문`
- Source: `에너지경제신문`
- Published at: `2026-05-27T15:00:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE5aSzVsdkhUSmhMUEp5UDRVdWhFNmdueEhXUld1bEZ3UTB4ZG5TSG82MnEzWFl1clpfRjJPc0s3bk40VWdjWVVDbkc5OWllS1FkQXZvTVpnU2tsdDJZVm5mUTZNOA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:44+09:00`
- Company: [[KRX_018000_유니슨]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `유니슨, 10MW급 국산 해상풍력터빈 실증기 설치 - 에너지경제신문`
- Latest observation source: `에너지경제신문`
- Latest observation published_at: `2026-05-27T15:00:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE5aSzVsdkhUSmhMUEp5UDRVdWhFNmdueEhXUld1bEZ3UTB4ZG5TSG82MnEzWFl1clpfRjJPc0s3bk40VWdjWVVDbkc5OWllS1FkQXZvTVpnU2tsdDJZVm5mUTZNOA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
