---
id: verification-2026-08-18-KRX-064400-google-rss-coverage
type: verification
title: KRX 064400 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "064400"
company: LG씨엔에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=064400
    - name=LG씨엔에스
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 064400
    - LG씨엔에스
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

# KRX 064400 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_064400_google-rss-coverage-source]]

## Facts Checked
- `code=064400`
- `name=LG씨엔에스`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `그룹 밖에서 더 커진다…LG씨엔에스, AX 시장 공략 본격화 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-08-17T14:42:46+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE11WXRmaVc2Ujh6WW1BYkpfZHh1RVB4Q0x5UUo2VGJHNFhzNF9SNG5fRDU0eE5MNWVPaEhFa2dwUHJsUnIxTk52em1xVHItcVZrdk5MSl8tNzAwTzkxQjVIbVJxcnJRSl9mMDZXTnlZMmpGbWFRbDBN0gF3QVVfeXFMTXVZdGZpVzZSOHpZbUFiSl9keHVFUHhDTHlRSjZUYkc0WHM0X1I0bl9ENTR4Tkw1ZU9oSEVrZ3BQcmxScjFOTnZ6bXFUci1xVmt2TkxKXy03MDBPOTFCNUhtUnFyclFKX2YwNldOeVkyakZtYVFsME0?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_064400_LG씨엔에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `증권사 리포트 쏟아지자 '반전'…외국인·기관 동시에 사들이는 종목 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-08-18T10:00:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9BQm45QjFpekdsWWR5RHpLTW4tVmZpcjZBUVhWWkxudjR1UDdxQy1yT2dwbnptZnBocWNxRFVIT3MzZlhaZ0hTNlRNaDhVZFJ1ZDNXTm9UT09vQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
