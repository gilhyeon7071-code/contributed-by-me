---
id: verification-2026-06-16-KRX-003000-google-rss-coverage
type: verification
title: KRX 003000 Google RSS Coverage Verification
created: 2026-06-16
updated: 2026-06-16
status: verification
stage: 1

market: KRX
ticker: "003000"
company: 부광약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=003000
    - name=부광약품
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003000
    - 부광약품
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

# KRX 003000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-16_KRX_003000_google-rss-coverage-source]]

## Facts Checked
- `code=003000`
- `name=부광약품`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[핀셋+][부광약품]아픈 손가락 '콘테라·재규어'…오픈이노베이션 재정비 : 네이버 블로그 - Naver Blog`
- Source: `Naver Blog`
- Published at: `2026-06-15T22:26:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMijwFBVV95cUxQTkoyQnBTRElQWTF0cm9mTHNYQjhwNEJyNVVBcFYzVDBpRVB3d1hXRHB1QW1xN05fTWRoSWh0VFFQNTdpb3I3MlJVUk1OMGp5NnphS2s5dWUyZFJyaUpRdTY2MmwtbWNaRmJMMkRPVWZvV00yOGR2WTFra0I2S2VtUkZpb3RJYzZPT1B6OGFIYw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:21:26+09:00`
- Company: [[KRX_003000_부광약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-16.json`
- Latest observation title: `부광, 유니온 경영 정상화 시동…'300억 투자' 시너지 기대 - 데일리팜`
- Latest observation source: `데일리팜`
- Latest observation published_at: `2026-06-09T12:03:08+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE9nSlUwU0R1RHlFNktqUklhdF9xSjMyTEVzVWFYajk3UmVxbmwyaUJQRTZwQXNEM0dINFJMQlAzcFRscUZVRUt6Ylp3dHdZSW9t?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
