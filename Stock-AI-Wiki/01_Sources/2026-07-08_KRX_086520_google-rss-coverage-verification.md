---
id: verification-2026-07-08-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-07-08
updated: 2026-07-08
status: verification
stage: 1

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=4
    - google_rss_article_count=7
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-08_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=4`
- `google_rss_article_count=7`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에코프로비엠, 끝내 주주에 손 벌린 이유 - 비즈워치`
- Source: `비즈워치`
- Published at: `2026-07-07T06:40:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5fV3B4V0pzQzNFYThCcDJ0VmszMmkybmtpOUxsZ3lxcDAxYnFvdlNpemlBdFZrQ3JmWHI5UkliNU11Q0RBWG5BUnIyQzNzdVN6dGgzaUJQTENONU01QjV5THhKTnVzMDBLNkZ4NA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:46+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `에코프로비엠, 끝내 주주에 손 벌린 이유 - 비즈워치`
- Latest observation source: `비즈워치`
- Latest observation published_at: `2026-07-07T06:40:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5fV3B4V0pzQzNFYThCcDJ0VmszMmkybmtpOUxsZ3lxcDAxYnFvdlNpemlBdFZrQ3JmWHI5UkliNU11Q0RBWG5BUnIyQzNzdVN6dGgzaUJQTENONU01QjV5THhKTnVzMDBLNkZ4NA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
