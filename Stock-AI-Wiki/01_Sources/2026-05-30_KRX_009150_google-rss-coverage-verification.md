---
id: verification-2026-05-30-KRX-009150-google-rss-coverage
type: verification
title: KRX 009150 Google RSS Coverage Verification
created: 2026-05-30
updated: 2026-05-30
status: verification
stage: 1

market: KRX
ticker: "009150"
company: 삼성전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009150
    - name=삼성전기
    - naver_article_count=81
    - google_rss_article_count=157
    - kis_title_count=96
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009150
    - 삼성전기
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

# KRX 009150 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-30_KRX_009150_google-rss-coverage-source]]

## Facts Checked
- `code=009150`
- `name=삼성전기`
- `naver_article_count=81`
- `google_rss_article_count=157`
- `kis_title_count=96`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 삼성전기, 7% 급등 역대 최고가…코스피 시총 4위로 '쑥' - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-05-29T09:45:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE8wZHFtOUMzeUJZOG9ETU0xZHVPRWVscURudlVQZ19McllZX2NlUFNpQUoxVTNkNlFWeUVTcHBiSUhDem9TNXN3UjNYX2I0aVl5T2xKVFFlOGV2cHPSAWBBVV95cUxQRTdWLVVTaG9haVMzWE9GTDNaRWJCTTJTZnNQMlR1LUhKM3hMU2x3b3NpZmNpalRKS21NT09zZ3NNRDFoaFNrY1djTDh6SWlwUWJ2alRGSmZjOHVZUzZ5OVM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:37+09:00`
- Company: [[KRX_009150_삼성전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-30.json`
- Latest observation title: `[특징주] 삼성전기, 7% 급등 역대 최고가…코스피 시총 4위로 '쑥' - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-29T09:45:58+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE8wZHFtOUMzeUJZOG9ETU0xZHVPRWVscURudlVQZ19McllZX2NlUFNpQUoxVTNkNlFWeUVTcHBiSUhDem9TNXN3UjNYX2I0aVl5T2xKVFFlOGV2cHPSAWBBVV95cUxQRTdWLVVTaG9haVMzWE9GTDNaRWJCTTJTZnNQMlR1LUhKM3hMU2x3b3NpZmNpalRKS21NT09zZ3NNRDFoaFNrY1djTDh6SWlwUWJ2alRGSmZjOHVZUzZ5OVM?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
