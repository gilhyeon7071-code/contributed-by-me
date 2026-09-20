---
id: verification-2026-06-10-KRX-032830-google-rss-coverage
type: verification
title: KRX 032830 Google RSS Coverage Verification
created: 2026-06-10
updated: 2026-06-10
status: verification
stage: 1

market: KRX
ticker: "032830"
company: 삼성생명
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=032830
    - name=삼성생명
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=5
    - google_rss_covered=False
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 032830
    - 삼성생명
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

# KRX 032830 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-10_KRX_032830_google-rss-coverage-source]]

## Facts Checked
- `code=032830`
- `name=삼성생명`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=5`
- `google_rss_covered=False`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성생명, 퇴직연금으로 ETF 모은다 - 위클리서울`
- Source: `위클리서울`
- Published at: `2026-06-09T16:56:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9XZ0ViRGdBVHNlc0ZQS3NobS1LVVM2R2Q0NkFXVXotaDBtbWQ2UlV4blpGTW1BUlNWSk5BVzQ2c0ROUmlTMloyVERTQkdPaXczSDQzbks5MG0yYkNqQzg4YmQwREpsSnQ3SjJMWldB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:51+09:00`
- Company: [[KRX_032830_삼성생명]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-10.json`
- Latest observation title: `보유 삼전지분 가치보다 시총 작은 삼성생명…리레이팅 찾아올까 - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-06-10T10:07:54+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFA5UHJIV182WUJjaTFTYmtvUS1ka0FLTVRGTkpudmltS2RMblI5c1JaOGVMcE1NWjRHblBJTTZKSUVhclpxVy1JNVNVZVNCUFhLNUE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
