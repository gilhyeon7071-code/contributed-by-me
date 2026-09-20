---
id: source-2026-07-23-KRX-009150-google-rss-coverage
type: source
title: KRX 009150 Google RSS Coverage Source
created: 2026-07-23
updated: 2026-07-23
status: raw
stage: 0

market: KRX
ticker: "009150"
company: 삼성전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Local coverage report row shows news coverage for KRX 009150.
  key_facts:
    - code=009150
    - name=삼성전기
    - naver_article_count=1
    - google_rss_article_count=22
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009150
    - 삼성전기
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 009150 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=009150`
- `name=삼성전기`
- `naver_article_count=1`
- `google_rss_article_count=22`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 009150.

## RSS Item Metadata
- Title: `“241만원 찍더니 127만원”…47% 빠진 삼성전기, 목표가는 300만원 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-21T06:19:08+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBkOWdzNVRLM2tIZU53QU1NR2RrZzFaQWppRDdvbjc4ZjFtajRhMm9nWUZLSUFJMHRGdVA4UkNIVy1xX0lGVXh5LW1kcjIwaDA?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_009150_삼성전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `삼성전기, AI 서버용 MLCC 3000억원 수주… 내년부터 공급 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-23T11:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiggFBVV95cUxPMmVWZGNVWGU2QzVGb3hkZl9LVHh1OVZfX0NITEoxOWRLS09NNFU4MlBJeWJYVzhiNjFiV1o2dENYREVJbUVVQnlZdVFwVmdxS3V2TTB5NXVXY2tkblZjZnBFSlhKQ2duTlZ3WXlCUGc4dUZfNE1nNXNhejlBWDFyVzlR0gGWAUFVX3lxTE44dXpzSjd0eUluMGtzejhkLV9SVmRFbEtpNUV4dFVXNUN3VmgxNVdSTTZ4aVkyVi11QzVxMFJvQS11alJkQnpXbklrN3R6R25EOS1iM2RGX1A5bzBCRHRTTGc2clIxNzYwQll6S2dUTXNuTFdmZFROSmZWSUdsTWxiMW1tWndLV2xPeUdJUWpPNzlBZ2xDZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
