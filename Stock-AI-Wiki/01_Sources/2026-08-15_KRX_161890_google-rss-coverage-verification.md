---
id: verification-2026-08-15-KRX-161890-google-rss-coverage
type: verification
title: KRX 161890 Google RSS Coverage Verification
created: 2026-08-15
updated: 2026-08-15
status: verification
stage: 1

market: KRX
ticker: "161890"
company: 한국콜마
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=161890
    - name=한국콜마
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 161890
    - 한국콜마
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

# KRX 161890 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-15_KRX_161890_google-rss-coverage-source]]

## Facts Checked
- `code=161890`
- `name=한국콜마`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[Why] 같은 K뷰티 호황인데… 한국콜마·코스맥스 美 성적표 갈린 이유 - 조선비즈 - biz.chosun.com`
- Source: `biz.chosun.com`
- Published at: `2026-08-14T15:04:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMikwFBVV95cUxPMEVDUlFqLWtycnlHM3k2Z0hYczYyS01qYkkzU0lnNWpCdEM4ZW5WRXRRell2SkFGWlF6Q0Q1WEJmd0R6emNWUHhHRk4zZFg0ck1KWkMzQUZVZFRHOEhMMFJMWlM1SkgybURtdzlVTWRoVEFUdkNKTDRmU2JQblNULTF6SHRRb2pCajY5N3V6QVJZcknSAacBQVVfeXFMTVV5TmhHS1Z2LTI3eldwYnI3TjN6Yl94OHR4UmYycmdPY2pMb1dqZGNDeHlLZFE0Zlc3VTY4Qi1hYXRFc1FYSVJ5UGRzR1FPanpLY3E0VjRwYlJ4c0J0bTFBR0Q0WmNxYi1YN2hmNDR4RTZSckgwZGdyU0pIZ0pOSmN4UDZvX1RkZkpocEJUcm5BWFVfQm4wbU81dm9fM1dCb3MyVzhKRjQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:30+09:00`
- Company: [[KRX_161890_한국콜마]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-15.json`
- Latest observation title: `[Why] 같은 K뷰티 호황인데… 한국콜마·코스맥스 美 성적표 갈린 이유 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-08-14T15:04:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMikwFBVV95cUxPMEVDUlFqLWtycnlHM3k2Z0hYczYyS01qYkkzU0lnNWpCdEM4ZW5WRXRRell2SkFGWlF6Q0Q1WEJmd0R6emNWUHhHRk4zZFg0ck1KWkMzQUZVZFRHOEhMMFJMWlM1SkgybURtdzlVTWRoVEFUdkNKTDRmU2JQblNULTF6SHRRb2pCajY5N3V6QVJZcknSAacBQVVfeXFMTVV5TmhHS1Z2LTI3eldwYnI3TjN6Yl94OHR4UmYycmdPY2pMb1dqZGNDeHlLZFE0Zlc3VTY4Qi1hYXRFc1FYSVJ5UGRzR1FPanpLY3E0VjRwYlJ4c0J0bTFBR0Q0WmNxYi1YN2hmNDR4RTZSckgwZGdyU0pIZ0pOSmN4UDZvX1RkZkpocEJUcm5BWFVfQm4wbU81dm9fM1dCb3MyVzhKRjQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
