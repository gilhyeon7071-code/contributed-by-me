---
id: verification-2026-08-08-KRX-096770-google-rss-coverage
type: verification
title: KRX 096770 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "096770"
company: SK이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=096770
    - name=SK이노베이션
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 096770
    - SK이노베이션
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

# KRX 096770 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_096770_google-rss-coverage-source]]

## Facts Checked
- `code=096770`
- `name=SK이노베이션`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[주식민원처리반 3부] '포스코인터내셔널, SK이노베이션' 월요일에 선택할 이 종목은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-08T11:04:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTFB1M3BoQ1A1aXMtUUNfal9DTE5fNmZyTU1vUnpwNWpyazFjaWRYbDlRLUtnZ1NYMVhpaklqOHZnNEFKRmlta19LcWFnblNNS2N2T2syUDJKc1BKQWR0LVdXVGVLOGhoeU9kNF9V0gFuQVVfeXFMTzN6TVp1RWZCRlk4TU16bG5ta1VjSExkNG1IRkwyMnZRLTFJcGZYRFJrc1hzUk9PRGhNblZRbWx0SGtRU0wxSlhZM19ta0lsampFbHQ1VVZiOElFZFlqUnZ5MU1qYmtEQ3FrSW5WVFE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:34:10+09:00`
- Company: [[KRX_096770_SK이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `[주식민원처리반 3부] '포스코인터내셔널, SK이노베이션' 월요일에 선택할 이 종목은? - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-08-08T11:04:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE8zek1adUVmQkZZOE1NemxubWtVY0hMZDRtSEZMMjJ2US0xSXBmWERSa3NYc1JPT0RoTW5WUW1sdEhrUVNMMUpYWTNfbWtJbGpqRWx0NVVWYjhJRWRZalJ2eTFNamJrRENxa0luVlRR0gFuQVVfeXFMTzN6TVp1RWZCRlk4TU16bG5ta1VjSExkNG1IRkwyMnZRLTFJcGZYRFJrc1hzUk9PRGhNblZRbWx0SGtRU0wxSlhZM19ta0lsampFbHQ1VVZiOElFZFlqUnZ5MU1qYmtEQ3FrSW5WVFE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
