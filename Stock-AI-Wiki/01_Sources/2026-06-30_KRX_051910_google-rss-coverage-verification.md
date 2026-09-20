---
id: verification-2026-06-30-KRX-051910-google-rss-coverage
type: verification
title: KRX 051910 Google RSS Coverage Verification
created: 2026-06-30
updated: 2026-06-30
status: verification
stage: 1

market: KRX
ticker: "051910"
company: LG화학
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=051910
    - name=LG화학
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 051910
    - LG화학
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

# KRX 051910 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-30_KRX_051910_google-rss-coverage-source]]

## Facts Checked
- `code=051910`
- `name=LG화학`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `자금 조달 필요한 LG화학·에코프로… “딜 잡아라” 증권사 IB 물밑 경쟁 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-28T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQRzVaYlc4Y2VUcGNyN1F3TTZ4ZmFvTC1SQS1PZVl1eEN1cnNYbVRqUDV4dnFfaGZWWmthLV8yMFVlWlc4ZjVRMWxoVkRCOF9tTE9iSmExOG5YdDlsU2RlLUNrcXppSHIzQjFDZXdPUVM2MzBtYkowR3BjZzdqdVlGUXFIbVlPWDjSAZsBQVVfeXFMTUNhb3Mwck16Y3NvWE0zdTMwMFIxWUpIQ1hHU3BWazZ5dE12cy1lbmdsRk45dVVVNy1aXzdvNUxxWS1GWExQUUlEc3NiRW1aRDFHOEpxRjhvaktGc2JVX0ZnVnp1Q1QxWWwxV3NGR0sycl9zWkNkNVJuSExvLVhyWG9jUXJBS0I2SEtiMDRuWWJ5TktNbEltbGR1RFU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:36+09:00`
- Company: [[KRX_051910_LG화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `자금 조달 필요한 LG화학·에코프로… “딜 잡아라” 증권사 IB 물밑 경쟁 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-28T06:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQRzVaYlc4Y2VUcGNyN1F3TTZ4ZmFvTC1SQS1PZVl1eEN1cnNYbVRqUDV4dnFfaGZWWmthLV8yMFVlWlc4ZjVRMWxoVkRCOF9tTE9iSmExOG5YdDlsU2RlLUNrcXppSHIzQjFDZXdPUVM2MzBtYkowR3BjZzdqdVlGUXFIbVlPWDjSAZsBQVVfeXFMTUNhb3Mwck16Y3NvWE0zdTMwMFIxWUpIQ1hHU3BWazZ5dE12cy1lbmdsRk45dVVVNy1aXzdvNUxxWS1GWExQUUlEc3NiRW1aRDFHOEpxRjhvaktGc2JVX0ZnVnp1Q1QxWWwxV3NGR0sycl9zWkNkNVJuSExvLVhyWG9jUXJBS0I2SEtiMDRuWWJ5TktNbEltbGR1RFU?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
