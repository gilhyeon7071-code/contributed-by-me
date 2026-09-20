---
id: verification-2026-06-26-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-06-26
updated: 2026-06-26
status: verification
stage: 1

market: KRX
ticker: "012330"
company: 현대모비스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=1
    - google_rss_article_count=47
    - kis_title_count=51
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 012330
    - 현대모비스
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

# KRX 012330 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-26_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=1`
- `google_rss_article_count=47`
- `kis_title_count=51`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'220조 큰손' 노르웨이 스토어브랜드 "삼성화재·KCC·현대모비스, 숨겨진 가치주" - 더구루`
- Source: `더구루`
- Published at: `2026-06-25T12:49:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE9qYVBIeXFqM1JUOVBRZnVSaFNkSERuaW9PY2xQY2t1dng4blh6aWJ0ZTFSS2l4VWM3cXhqTzZXSjZNc0Q1eGRCNVM1Tk1BZG1TTlZEUTBmcG44S1ZRTGJPZHYyQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `현대모비스, 협력사 소프트웨어 인재 육성… 상생 확대 - 서울신문`
- Latest observation source: `서울신문`
- Latest observation published_at: `2026-06-26T08:30:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiggFBVV95cUxPb1VhVWJvTkR1TzlXZFhGb0VCWDdCVnBLOHJTeDZJSllNNDctamJHcHNqNDEtdVpQZ045YnF1R2czQk0tbXFNWjNXcEdwQmVUb2trZ2JjZDFWOGlMTTFROGdyanh6VlVNRjRnR0h6OU4xTGZqdDhMRGc1eURJaWw3SXJR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
