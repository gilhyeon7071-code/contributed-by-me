---
id: verification-2026-06-05-KRX-214370-google-rss-coverage
type: verification
title: KRX 214370 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
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
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=214370
    - name=케어젠
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=1
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
- [[2026-06-05_KRX_214370_google-rss-coverage-source]]

## Facts Checked
- `code=214370`
- `name=케어젠`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `케어젠, +4.41% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-04T13:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPcVZaQmVuY2lLZjNHc09CeGFfdGNTQUtPWTZwSThaVG9wM1g2UTFtcVJDcGUzSWs1aXlHOHpBRlJuWGlvakFla1NfZDdTNnBGa21feFF6QTNZckxfcnA5SVg3ek1mTFF5a1BLSWJqbEs3SW1NTXZzbEVnMlNIZW1fQW5Id9IBlwFBVV95cUxNdFoxZFJ2a1daTXJ4eHZWX0VTenFKTFcwS1YxOVFERDF5LUhnY3VsakJ1QXFiRHNzQnRVbVkybE43aTBDZ25UcXBXeE1hOUNyS0U3eF9tY2EweFRQUjM3RXFvVFdDbnZKSzd2cHFKUzhvUkFlb3VtcXF1TEJFb2p2bFFlREVpN0FZT3lIa1p6WkxiRWo2dTE0?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T08:11:49+09:00`
- Company: [[KRX_214370_케어젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `케어젠, +4.41% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-04T13:06:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPcVZaQmVuY2lLZjNHc09CeGFfdGNTQUtPWTZwSThaVG9wM1g2UTFtcVJDcGUzSWs1aXlHOHpBRlJuWGlvakFla1NfZDdTNnBGa21feFF6QTNZckxfcnA5SVg3ek1mTFF5a1BLSWJqbEs3SW1NTXZzbEVnMlNIZW1fQW5Id9IBlwFBVV95cUxNdFoxZFJ2a1daTXJ4eHZWX0VTenFKTFcwS1YxOVFERDF5LUhnY3VsakJ1QXFiRHNzQnRVbVkybE43aTBDZ25UcXBXeE1hOUNyS0U3eF9tY2EweFRQUjM3RXFvVFdDbnZKSzd2cHFKUzhvUkFlb3VtcXF1TEJFb2p2bFFlREVpN0FZT3lIa1p6WkxiRWo2dTE0?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
