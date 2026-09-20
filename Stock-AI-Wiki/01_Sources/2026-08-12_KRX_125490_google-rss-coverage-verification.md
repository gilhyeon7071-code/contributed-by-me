---
id: verification-2026-08-12-KRX-125490-google-rss-coverage
type: verification
title: KRX 125490 Google RSS Coverage Verification
created: 2026-08-12
updated: 2026-08-12
status: verification
stage: 1

market: KRX
ticker: "125490"
company: 한라캐스트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-12

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 125490
    - 한라캐스트
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

# KRX 125490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-12_KRX_125490_google-rss-coverage-source]]

## Facts Checked
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `신공장 켰다, 로봇 양산 시작한다…한라캐스트 하반기 '이중 폭발' 조건 - 리드경제`
- Source: `리드경제`
- Published at: `2026-04-20T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE10TjJrTVBHb1BMUC1NbFZnS2ZjdlNmMENIRW1qQzJTRWxGNDdNVm42SlhzbGNpbXo4Zy04SjljTUxEU1ZGRE5CYTJCQld3dUc4S1J3YXYxb1pTSWlUSlFpbmJpUDhfRzdJcFpmalhfWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:30+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-12.json`
- Latest observation title: `한라캐스트 투자분석 2026. 08. 11 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-08-11T18:23:30+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9FaFVmbVhEcE9MZElhMk5Vczh2MGtCajF6RWJBU3phcmphT05aRUVWZFotUW85MEcxMEpsdEhZZUY5UzZ6eTl6TGZrd0lOT3hIQk9fT0FqbjY2NzlpREM2SWJBZndFVmEtamhXV1dEZVRwTm8?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
