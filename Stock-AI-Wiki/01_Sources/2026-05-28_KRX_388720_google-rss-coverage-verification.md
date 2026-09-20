---
id: verification-2026-05-28-KRX-388720-google-rss-coverage
type: verification
title: KRX 388720 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "388720"
company: 유일로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=388720
    - name=유일로보틱스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 388720
    - 유일로보틱스
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

# KRX 388720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_388720_google-rss-coverage-source]]

## Facts Checked
- `code=388720`
- `name=유일로보틱스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `유일로보틱스, 250kg급 수직 다관절로봇 개발 완료 - 뉴스타운`
- Source: `뉴스타운`
- Published at: `2026-05-15T08:51:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1VNURIbnB5NTFpYXdXNjNMekE3Q0NWdTdKTkplSmpQbXlTeDNkdHVTQkRoTEN3UmZVMzlhdHQ1RGRFM1NJY3pMZ3UtOEFqZ2tjSlktS2xfMDNVVXBvY2h5NXlHZmlNMkNhN3doRzBn?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-28T09:05:09+09:00`
- Company: [[KRX_388720_유일로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `유일로보틱스, 250kg급 수직 다관절로봇 개발 완료 - 뉴스타운`
- Latest observation source: `뉴스타운`
- Latest observation published_at: `2026-05-15T08:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1VNURIbnB5NTFpYXdXNjNMekE3Q0NWdTdKTkplSmpQbXlTeDNkdHVTQkRoTEN3UmZVMzlhdHQ1RGRFM1NJY3pMZ3UtOEFqZ2tjSlktS2xfMDNVVXBvY2h5NXlHZmlNMkNhN3doRzBn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
