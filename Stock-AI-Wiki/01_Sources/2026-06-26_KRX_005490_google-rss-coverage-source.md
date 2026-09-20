---
id: source-2026-06-26-KRX-005490-google-rss-coverage
type: source
title: KRX 005490 Google RSS Coverage Source
created: 2026-06-26
updated: 2026-06-26
status: raw
stage: 0

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 005490.
  key_facts:
    - code=005490
    - name=POSCO홀딩스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 005490
    - POSCO홀딩스
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

# KRX 005490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 005490.

## RSS Item Metadata
- Title: `POSCO홀딩스, 하반기 철강 스프레드 개선 기대 - 알파경제`
- Source: `알파경제`
- Published at: `2026-06-25T11:18:33+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE42SEUtQVBYREhUaEtvWmFzckl4cTBzUTJDMXVpMHh1V2tMTk1fNXVMbmRZN2hQQ2lnOVRwWFNuNU1TMVExNHZlMkNzcnNtNXRrN2VhRUJRb3J2TDlDLUw3ZnRfeE1VdElmb2NhVVN3?oc=5`

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
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `포스코홀딩스, 그룹 통합 ESG 공시 체계 도입…지속가능경영보고서 발간 - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-06-26T14:04:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBfblRsMmRCaVBzYzQ4VXFrSjRDN190clBELVdOVjByb1MzNlRKUmZMVU10UVBsR0FfQWxYbmlmUGtjTmlVb01BSXJxakV5UHU4cGJHcG53?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
