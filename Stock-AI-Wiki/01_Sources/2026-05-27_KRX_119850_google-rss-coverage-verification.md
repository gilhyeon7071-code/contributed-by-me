---
id: verification-2026-05-27-KRX-119850-google-rss-coverage
type: verification
title: KRX 119850 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "119850"
company: 지엔씨에너지
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
    - code=119850
    - name=지엔씨에너지
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 119850
    - 지엔씨에너지
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

# KRX 119850 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_119850_google-rss-coverage-source]]

## Facts Checked
- `code=119850`
- `name=지엔씨에너지`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `지엔씨에너지, -9.51% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-27T13:31:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNYVJlS19feENjdDFkM3U2cFY3RHFGRXBRLVoxQnpiMGt2Um5UT3hETmxtNFlSRGMyQU1yT0gyaFNXZmYzWnJPMFFtZWQ3N3drUWliTHFhZmkzQVAwYVdKUDF1X3V6bzA5UEV3Z2hjZ3E5eFREQ0VvblpLellsbEdZbWNqOS1pb3lwOVV0YUJocTI4NDJfVXFn0gGXAUFVX3lxTE1hUmVLX194Q2N0MWQzdTZwVjdEcUZFcFEtWjFCemIwa3ZSblRPeERObG00WVJEYzJBTXJPSDJoU1dmZjNack8wUW1lZDc3d2tRaWJMcWFmaTNBUDBhV0pQMXVfdXpvMDlQRXdnaGNncTl4VERDRW9uWkt6WWxsR1ltY2o5LWlveXA5VXRhQmhxMjg0Ml9VcWc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T17:05:04+09:00`
- Company: [[KRX_119850_지엔씨에너지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `지엔씨에너지, -9.51% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T13:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNYVJlS19feENjdDFkM3U2cFY3RHFGRXBRLVoxQnpiMGt2Um5UT3hETmxtNFlSRGMyQU1yT0gyaFNXZmYzWnJPMFFtZWQ3N3drUWliTHFhZmkzQVAwYVdKUDF1X3V6bzA5UEV3Z2hjZ3E5eFREQ0VvblpLellsbEdZbWNqOS1pb3lwOVV0YUJocTI4NDJfVXFn0gGXAUFVX3lxTE1hUmVLX194Q2N0MWQzdTZwVjdEcUZFcFEtWjFCemIwa3ZSblRPeERObG00WVJEYzJBTXJPSDJoU1dmZjNack8wUW1lZDc3d2tRaWJMcWFmaTNBUDBhV0pQMXVfdXpvMDlQRXdnaGNncTl4VERDRW9uWkt6WWxsR1ltY2o5LWlveXA5VXRhQmhxMjg0Ml9VcWc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
