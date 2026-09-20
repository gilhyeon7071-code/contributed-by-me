---
id: verification-2026-05-27-KRX-056080-google-rss-coverage
type: verification
title: KRX 056080 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "056080"
company: 유진로봇
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
    - code=056080
    - name=유진로봇
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 056080
    - 유진로봇
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

# KRX 056080 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_056080_google-rss-coverage-source]]

## Facts Checked
- `code=056080`
- `name=유진로봇`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `유진로봇, ‘밀레 그룹’서 173억 규모 투자 유치...플랫폼 사업 확장 가속화한다 - 헬로티`
- Source: `헬로티`
- Published at: `2026-05-18T13:26:06+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTFA0dDVZaGdaYzNVWWRxdEhTWGJ6WGxtSXFrbDU5Yy04OTVrNEVWc2FhN0F4aG5Vbm5uWlRIWXY0UUQ5WTdfSUUyano5elBzZFJsLXg3R3FOcUxBb0Vjem8w?oc=5`

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
- Company: [[KRX_056080_유진로봇]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `유진로봇, ‘밀레 그룹’서 173억 규모 투자 유치...플랫폼 사업 확장 가속화한다 - 헬로티`
- Latest observation source: `헬로티`
- Latest observation published_at: `2026-05-18T13:26:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTFA0dDVZaGdaYzNVWWRxdEhTWGJ6WGxtSXFrbDU5Yy04OTVrNEVWc2FhN0F4aG5Vbm5uWlRIWXY0UUQ5WTdfSUUyano5elBzZFJsLXg3R3FOcUxBb0Vjem8w?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
