---
id: verification-2026-06-25-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-25
updated: 2026-06-25
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=2
    - google_rss_article_count=89
    - kis_title_count=21
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-25_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=2`
- `google_rss_article_count=89`
- `kis_title_count=21`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독] 현대건설 본사에 차량 돌진, 한남3구역 조합원 '집행유예' - 비즈한국`
- Source: `비즈한국`
- Published at: `2026-06-23T16:38:43+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE12OWNubXdIMDBIa3ZfQ1EwN0d2bXpEZG1aUkI3RExnaTVGUTlqeV8tRnJndk1qMmkyX0l0bGl4TTJDX3VfeTE2N1Vad2JhcE9aOGJoSUNB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:18+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-25.json`
- Latest observation title: `[단독] 현대건설 '압구정 현대' 상표 12건 모두 등록 거절 - 비즈한국`
- Latest observation source: `비즈한국`
- Latest observation published_at: `2026-06-25T11:25:58+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBNRlFEYWd1d2htZHVDa2RPbXZIRWNBd3VHNDd4WVFaeTRPNzljeEJjd0RRNUJoajhxTzNBWVRzamV4YVhVVE5jWXR6dEpZLWltY0NCS0ZR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
