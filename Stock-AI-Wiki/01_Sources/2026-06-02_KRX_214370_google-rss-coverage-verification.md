---
id: verification-2026-06-02-KRX-214370-google-rss-coverage
type: verification
title: KRX 214370 Google RSS Coverage Verification
created: 2026-06-02
updated: 2026-06-02
status: verification
stage: 1

market: KRX
ticker: "214370"
company: 케어젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=214370
    - name=케어젠
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 214370
    - 케어젠
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

# KRX 214370 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-02_KRX_214370_google-rss-coverage-source]]

## Facts Checked
- `code=214370`
- `name=케어젠`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `케어젠 “과거 계약 정리 절차일 뿐”…성장 전략 이상 無 - 한스경제`
- Source: `한스경제`
- Published at: `2026-06-01T15:40:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTFByVDZWdGdoSlVOZUdOdy0yOVExbzNNVlk3U25WaGJITFR3WnBqaktQblh1QnZFRlhhdXgzSkRtcXdsQ0FCNUlxVzBLRUloZ19Md0tZOFVDMHhwU0REOWRaUTZSNXpmX1pwU1pRRNIBb0FVX3lxTE9BaUtpZEJsUGJ1RlV5REZmNkpoa2w0eFNoRWV3UG4tVzRfN202ZTlYTHdNZmVMMHdSVnd1WEVRMm9ySzg5OUxqcEFkeHd0R09tUVRodnM3Y1FITVZfYWlmWFFLR2NEbFJXRkxwTWlycw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:26+09:00`
- Company: [[KRX_214370_케어젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-02.json`
- Latest observation title: `케어젠,탈모방지·발모촉진용 'DR. CYJ Hair Filler' 공급계약 해지 - 약업신문`
- Latest observation source: `약업신문`
- Latest observation published_at: `2026-05-29T20:08:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFBtNklJQmtSZUJEMDE1Tmp4eDFrRkhmelJvSDFJMlpXa0J1bms5V0JrUDZROTlsNk5tNGNRZ1ZPeEdBM1ZDd1VxaEFYVUctRE9UNEVtdEd6UFAwRUdMWjRGVTJvX1YzZw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
