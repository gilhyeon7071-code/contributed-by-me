---
id: verification-2026-06-02-KRX-347860-google-rss-coverage
type: verification
title: KRX 347860 Google RSS Coverage Verification
created: 2026-06-02
updated: 2026-06-02
status: verification
stage: 1

market: KRX
ticker: "347860"
company: 알체라
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
    - code=347860
    - name=알체라
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 347860
    - 알체라
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

# KRX 347860 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-02_KRX_347860_google-rss-coverage-source]]

## Facts Checked
- `code=347860`
- `name=알체라`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `사진·동영상 대도 완벽 차단... 알체라, 독보적 생체인증 기술에 매수 溫氣 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-29T15:06:21+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1fNm9uT295TDNkeW5LTXg4ZEM0WXFMdi1YT2tCWjAyN192TXBiX0pPZURyWWJqTEZxUlA1QlV5WDZLbFBEcTZzTnowV3dIZklOSzRlalBiZV9KdTE5d2xrTUhvOUZaNTROZi1ZTVZRNEZ4OFnSAXdBVV95cUxQLWlxNXpwc1FoWWlWRGlzSnFxZVZPWS1aMG1LTnU1RFp2S3F2WTJSd0ZFclhhcVZlTGZXSU9jcjJxN1Q5RWpTYm4zalFYbTlaQURaZDVZUGViTjJWUHgzVlB5eXZGVEpyU04xZUgtX0wtaDRMaUx2UQ?oc=5`

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
- Company: [[KRX_347860_알체라]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-02.json`
- Latest observation title: `사진·동영상 대도 완벽 차단... 알체라, 독보적 생체인증 기술에 매수 溫氣 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-05-29T15:06:21+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1fNm9uT295TDNkeW5LTXg4ZEM0WXFMdi1YT2tCWjAyN192TXBiX0pPZURyWWJqTEZxUlA1QlV5WDZLbFBEcTZzTnowV3dIZklOSzRlalBiZV9KdTE5d2xrTUhvOUZaNTROZi1ZTVZRNEZ4OFnSAXdBVV95cUxQLWlxNXpwc1FoWWlWRGlzSnFxZVZPWS1aMG1LTnU1RFp2S3F2WTJSd0ZFclhhcVZlTGZXSU9jcjJxN1Q5RWpTYm4zalFYbTlaQURaZDVZUGViTjJWUHgzVlB5eXZGVEpyU04xZUgtX0wtaDRMaUx2UQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
