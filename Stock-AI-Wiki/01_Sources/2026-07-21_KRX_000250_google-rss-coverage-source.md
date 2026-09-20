---
id: source-2026-07-21-KRX-000250-google-rss-coverage
type: source
title: KRX 000250 Google RSS Coverage Source
created: 2026-07-21
updated: 2026-07-21
status: raw
stage: 0

market: KRX
ticker: "000250"
company: 삼천당제약
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 000250.
  key_facts:
    - code=000250
    - name=삼천당제약
    - naver_article_count=7
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 000250
    - 삼천당제약
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 000250 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000250`
- `name=삼천당제약`
- `naver_article_count=7`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000250.

## RSS Item Metadata
- Title: `어제는 상한가, 오늘은 하한가…롤러코스터에 개미들 '비명' [종목+] - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-21T12:06:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE42SHRKcmZxekUtN01vZzh4T2ZZS2ozR3BhRmpROXltNnpPci1rcnB5bFh0TUJoRFNUdE43YUdWYWxkMGx6MHFzQXpuOHRMQTVvM1ZXZENZQ09BZw?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:18+09:00`
- Company: [[KRX_000250_삼천당제약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `“상한가 올라탔는데 곧바로 하한가 털썩”…천당·지옥 오간 삼천당제약, 무슨 일? - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-07-21T17:05:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBTMjJHU1pJYzNNMmNMVmRSRXo4R3l1YnY1ZEZmVVF0dE9ldURvRXlpTDFTMUx6dDQ0dHZySHl4aHlhMmR5Sm1GLTR1TzBTNFV6RXc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
