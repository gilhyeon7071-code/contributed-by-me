---
id: verification-2026-06-05-KRX-082640-google-rss-coverage
type: verification
title: KRX 082640 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "082640"
company: 동양생명
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=082640
    - name=동양생명
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 082640
    - 동양생명
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

# KRX 082640 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_082640_google-rss-coverage-source]]

## Facts Checked
- `code=082640`
- `name=동양생명`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `동양생명, '우리WON하는7배더행복한플러스종신보험’ 출시 - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-06-01T14:06:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9zclBlWVpGWTJ1OUVDa3lPQ243VUZrWmctYkZxN1FXWkxSc2w2ZFA3cUNTT2dZbmtCWjZIdk9SSGpiU0dMbmwzcmgtc0hQWVhhbzM5Wi1pdWZUUmNwQUxDNw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:19+09:00`
- Company: [[KRX_082640_동양생명]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `실적 늘어도 지주로 배당 어렵다···동양생명, 준비금 ‘급증’ - 시사저널e`
- Latest observation source: `시사저널e`
- Latest observation published_at: `2026-06-02T10:55:45+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFB2cDBEUVpQZjZTODdKeVVSZ19DODlxZEp6eG8tXzJrd0JsT0R2WEE3VUMteWZSdjNKUzk4dlhWVnNKWmZPelctR2F4WlAxU2VKMGp6UTVhMWdPLVVQMjJpV1BWMm0zS0N0N0VlTEIxeFB3Z9IBdkFVX3lxTE94Vjl6LUItcndfMnpXVUtINjcyeGRYakh2ZGtYU2VudGZpVkFNQWJLTFNHbmdfTmhtbW9ac3l2MVp0UGtYdDRoTmgyMmVlV3A1NU0wUFROaVNBaTdid2phYkJNYzBPRWtjWFBsTm5OaTU1bkItVVE?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
