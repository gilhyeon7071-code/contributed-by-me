---
id: verification-2026-07-27-KRX-000660-google-rss-coverage
type: verification
title: KRX 000660 Google RSS Coverage Verification
created: 2026-07-27
updated: 2026-07-27
status: verification
stage: 1

market: KRX
ticker: "000660"
company: SK하이닉스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000660
    - name=SK하이닉스
    - naver_article_count=4
    - google_rss_article_count=87
    - kis_title_count=41
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000660
    - SK하이닉스
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

# KRX 000660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-27_KRX_000660_google-rss-coverage-source]]

## Facts Checked
- `code=000660`
- `name=SK하이닉스`
- `naver_article_count=4`
- `google_rss_article_count=87`
- `kis_title_count=41`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `다음주 SK하이닉스·삼성전자 실적 발표…코스피 향방은 - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-07-26T07:20:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTFA4VUJCWm9GY1pKUnRJUDRpeDNYSXUxdWs3YjRPQWIxcDlfZV9nZ0h5WVFzVjVuLU9TZ3haSWZRVlRDNTBjOFhkTUJ5T1U4VUVTUHk1aTdoZnlWcFJWZUxvRWdnbHAtV05qemMtUHhVU1jSAXRBVV95cUxOdHpfRlhDQklRNzVPNlF0NWxfVXV0WEZHNVVPTno3RlgzZkJIT3AyNGl0aUtweHFfNkJmNDdnRlFqYnY0V0dvX0gtTW93Zkh6VWpSR2FVNHZKSWxLNl9zV0dkX2dzeDlVdTRjUFFOWWlIaGxiOQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-27T21:05:13+09:00`
- Company: [[KRX_000660_SK하이닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `[단독]서울 한복판에 거점 세운다... SK하이닉스, 강남 르메르디앙 호텔 자리에 사옥 건립 추진 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-07-27T17:07:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE14RU9BX051M3l5aU9BWGZZMlJtbm5MbHdvX2psa21jbE12V3FSNlZOdFFTWnNNQTdlWDdQdU1RcHk1RUViOEdmTm9reExrVjJUVWd3dnhHSXNIRkVVdlY3bA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
