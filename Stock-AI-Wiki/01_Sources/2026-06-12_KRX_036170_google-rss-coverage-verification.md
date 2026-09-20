---
id: verification-2026-06-12-KRX-036170-google-rss-coverage
type: verification
title: KRX 036170 Google RSS Coverage Verification
created: 2026-06-12
updated: 2026-06-12
status: verification
stage: 1

market: KRX
ticker: "036170"
company: 에이치엠넥스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-12

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=036170
    - name=에이치엠넥스
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 036170
    - 에이치엠넥스
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

# KRX 036170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-12_KRX_036170_google-rss-coverage-source]]

## Facts Checked
- `code=036170`
- `name=에이치엠넥스`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에이치엠넥스, -8.84% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-10T09:51:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNVmFQX2hta25FOTZUNlRSNjhRSmlmaHRHc1Z3d3BSNnhlVnl1V0FEOVRkYllHaEtYLXhNSWpvX3hSU1BXaGV3bDNiYVBHZnNocG9vZU9IUXZQc1VYeHFGaHpWemtMQ1h4dTdHUXU3U1c2bml1Z1hXSURCMnU5YjQybEtuZFlPLVBZaEczTm96dVpvYXhJNnRj0gGXAUFVX3lxTE1WYVBfaG1rbkU5NlQ2VFI2OFFKaWZodEdzVnd3cFI2eGVWeXVXQUQ5VGRiWUdoS1gteE1Jam9feFJTUFdoZXdsM2JhUEdmc2hwb29lT0hRdlBzVVh4cUZoelZ6a0xDWHh1N0dRdTdTVzZuaXVnWFdJREIydTliNDJsS25kWU8tUFloRzNOb3p1Wm9heEk2dGM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-12T13:06:14+09:00`
- Company: [[KRX_036170_에이치엠넥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-12.json`
- Latest observation title: `에이치엠넥스, -8.84% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-10T09:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNVmFQX2hta25FOTZUNlRSNjhRSmlmaHRHc1Z3d3BSNnhlVnl1V0FEOVRkYllHaEtYLXhNSWpvX3hSU1BXaGV3bDNiYVBHZnNocG9vZU9IUXZQc1VYeHFGaHpWemtMQ1h4dTdHUXU3U1c2bml1Z1hXSURCMnU5YjQybEtuZFlPLVBZaEczTm96dVpvYXhJNnRj0gGXAUFVX3lxTE1WYVBfaG1rbkU5NlQ2VFI2OFFKaWZodEdzVnd3cFI2eGVWeXVXQUQ5VGRiWUdoS1gteE1Jam9feFJTUFdoZXdsM2JhUEdmc2hwb29lT0hRdlBzVVh4cUZoelZ6a0xDWHh1N0dRdTdTVzZuaXVnWFdJREIydTliNDJsS25kWU8tUFloRzNOb3p1Wm9heEk2dGM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
