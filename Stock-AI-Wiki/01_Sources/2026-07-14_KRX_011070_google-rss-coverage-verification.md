---
id: verification-2026-07-14-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-07-14
updated: 2026-07-14
status: verification
stage: 1

market: KRX
ticker: "011070"
company: LG이노텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-14_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG전자, 유리기판 상용화 난제 ‘TGV 기술’ 고도화… ‘후발주자’ LG이노텍 지원사격 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-13T16:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiggFBVV95cUxQYktvQzJLWk11c3hPUkFKbkdpSk1OT1FmM0hzc2dSWlh4Q2l4RWs4clUtbkhmXzFvclRESWRfbkZ5RVktNG5lZGNpU1JNaTRLRXlyYkQtemJuNW5TMkpqWUp4bGRQeHVlV3VyWC13N1FjV2lBLW5zRHpGd2l4LXpaU3VR0gGWAUFVX3lxTFB4aXpLN0JrSXVoQjZVaWJNTTV0RkxiWGdpb01hVWc2RGsxMVZmWHY2cjVIYUt1M1I0dTJsR09kbHVYbU1FQVJMaUdnQUlCSjlIVTQ5a0RxVC0wSW50OXBYcmUyNHRHYnNWcXNDQ0VoZXRlX1BjamFCTW41MldQRXRieVAxLXhTel9RVk14OWlaOFlIYlh6dw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:21+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-14.json`
- Latest observation title: `애플 의존도 낮추려는 LG이노텍, 기판 사업 체질 개선 - 뉴스톱`
- Latest observation source: `뉴스톱`
- Latest observation published_at: `2026-07-14T15:31:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1YMWRNUFRRRHE1U25uZnVpalRmZlo0YVJ1YmRCM2toWjZZYWdocEVnckpmSzlDSjRJZ1cwQV9saFJid01PRU9PSnNIbmFmVXI3NlVzNnd6SjBqblUwR21oRWNIQ2ZvZ1RYWWYzQWc4UQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
