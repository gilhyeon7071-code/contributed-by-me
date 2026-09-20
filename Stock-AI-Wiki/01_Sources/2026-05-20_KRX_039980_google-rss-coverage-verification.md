---
id: verification-2026-05-20-KRX-039980-google-rss-coverage
type: verification
title: KRX 039980 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "039980"
company: 폴라리스AI
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=039980
    - name=폴라리스AI
    - naver_article_count=2
    - google_rss_article_count=3
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039980
    - 폴라리스AI
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

# KRX 039980 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_039980_google-rss-coverage-source]]

## Facts Checked
- `code=039980`
- `name=폴라리스AI`
- `naver_article_count=2`
- `google_rss_article_count=3`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `폴라리스AI파마 투자분석 2026. 05. 19 - 주달`
- Source: `주달`
- Published at: `2026-05-19T16:54:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBYM1pDODF3MlFYeEJ1YW42MDNvLTBwYURzS3VPY2F0Qjk4c3hFTkM0U2o1anpfTGpPLTg4NlBJZXR3OXZqRmNETXhBUkdmSGhrdWowS1pCSVlYYVludHlGaE5hcDc5X0w4dmNTZWRCbzZOS00?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_039980_폴라리스AI]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `핸디소프트, 보훈공단 AI 그룹웨어 사업 수주…폴라리스오피스와 공공 AX 시장 공략 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-05-20T10:33:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9rYUNsVkpEUW5GSVdZNU9LOU1palpjc2pYTE9PMnRCYmtsTEIxcjV5SldnY1F3aTZmMEZ5VklQUERUMS1uSVp0RXhNaFFRaVdqeXJvSGFaZTE1Q05RaWNGRg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
