---
id: verification-2026-08-21-KRX-064550-google-rss-coverage
type: verification
title: KRX 064550 Google RSS Coverage Verification
created: 2026-08-21
updated: 2026-08-21
status: verification
stage: 1

market: KRX
ticker: "064550"
company: 바이오니아
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=064550
    - name=바이오니아
    - naver_article_count=3
    - google_rss_article_count=6
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 064550
    - 바이오니아
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

# KRX 064550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-21_KRX_064550_google-rss-coverage-source]]

## Facts Checked
- `code=064550`
- `name=바이오니아`
- `naver_article_count=3`
- `google_rss_article_count=6`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, 바이오니아-유전자 치료제/분석 테마 상승세에 15.84% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-08-20T09:27:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE8zUDl5LXR6NXZxNF9zZDhVVHlyMF9DdHE4TzF0OW1fbGhRd2N3RFZyeFY4alNaSHk5NGZ6cEFpV3Z5QnBQdWs2Vm9vVkZNcUttS2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_064550_바이오니아]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `[바이오e종목] 탈모 연구에 상한가…바이오벤처 1호 바이오니아, 부침 딛고 반등할까 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-08-21T06:24:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQNkNaZmhjeFh1MEtWV3FTZXBpWkVLNV8tbGoyTXBTOEJNNEFwaWkyODhNSlVJZ3lyRE1ib2pEMUlyQTZMaWZxX0pZVkVDdWRkd0s3NVUyUTFsazl6NGhuS2Z4VkRWWGI0bklERHNUVjZraGxVaEpyYTFnM19Mc2RDS3BqaG1oMknSAZsBQVVfeXFMUEZtZHRtTVZZa2lxVElPMW9CNk5obUcyVEd3RG5wS0EtRENNWm5fVkQ4ckZpU2MtaXZ3RlF1ME9UTEVJdnBYU2pBTTZ5ekxLa2hoZmFvNTJINTFzYW56WHdvbkg4cUNoZlBQcEx4Ni1VelNkQlVOaVh2SFg0dnhoR25jZGRXSlA3bFNZcjV6eUNZNXJZMVlnaUN2YWM?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
