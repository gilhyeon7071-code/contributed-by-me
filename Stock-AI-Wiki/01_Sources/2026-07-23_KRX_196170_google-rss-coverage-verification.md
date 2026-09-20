---
id: verification-2026-07-23-KRX-196170-google-rss-coverage
type: verification
title: KRX 196170 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
status: verification
stage: 1

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=3
    - google_rss_article_count=4
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-23_KRX_196170_google-rss-coverage-source]]

## Facts Checked
- `code=196170`
- `name=알테오젠`
- `naver_article_count=3`
- `google_rss_article_count=4`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"코스닥 1등이 좋아" 알테오젠, 코스피 안 가는 까닭 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-07-21T15:50:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE5jMHlqYUE2eVU1eXNqQVVLRnpSRlNmSnhEVEx6Y3ZwSjNWemZ6alJBbnpPLVJ4NzVobWhxZ1pkWUFfLTBpWnM0OW1LUXM3MGRMUy1wR0l5ZVFLa0V4LXEtOGxRekJ1eXVtVHZnVDRYQlQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-23T21:05:07+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `‘주주가치 보호’ 앞세운 중복상장 규제…휴온스·알테오젠 셈법 복잡해졌다 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-23T06:03:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQZHhtOENLNTU0RUpLM0JwZkxvUnRpcS1QWlFPWVl1cmNlbkNIandFc2x5cXE1WTFKal9sWkJTQjUxTnY2QTY1Ty1LWkhyb3o5aWZfT3F6TmhWS1NldHpyNXU1ZmpqVXo0LTloTTVBb2ktT0hiNUFsYzhGQjE5RVJIUGgzRjVNY1XSAZsBQVVfeXFMTmlEbTBzaGhGYW0xZzZib2tmM1ZWQXBFOVRUVGh0Y1BOcXFyY2ZpdTZZZGJzTzk3UTRwbC1MNUluV290aU42QU96R3ZXOU52dU0wZWdZenFwVHRTTm1ZWmJ6NUNicmo4S3M5bnEzNXIxVVU1Vm4yWlhkREdrb2t1T1d5MUJCdXhjcGZ3SWRSS2xGWFhWMk8ySWZFQVE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
