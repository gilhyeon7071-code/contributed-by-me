---
id: source-2026-06-07-KRX-125490-google-rss-coverage
type: source
title: KRX 125490 Google RSS Coverage Source
created: 2026-06-07
updated: 2026-06-07
status: raw
stage: 0

market: KRX
ticker: "125490"
company: 한라캐스트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 125490.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 125490
    - 한라캐스트
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

# KRX 125490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 125490.

## RSS Item Metadata
- Title: `한라캐스트, 글로벌 고객사 휴머노이드 로봇 '핵심 기업' 등극…"발열·무게 잡은 기술 주목" - 프라임경제`
- Source: `프라임경제`
- Published at: `2026-05-07T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1wVU1rUHFyckptQWtySk1hYlhlSjVJUXJ5b3VIbFZuMHZRcG8zQkNGZFczY0dacmFEMEtjX2VWWnRHTmlKTXpsYWJfRml2cDFrQktkLXZqT1BYdzRTaEFzWFloWThOZzA0OXY2cw?oc=5`

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
- Updated at: `2026-08-21T19:18:54+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-07.json`
- Latest observation title: `신공장 켰다, 로봇 양산 시작한다…한라캐스트 하반기 '이중 폭발' 조건 - 리드경제`
- Latest observation source: `리드경제`
- Latest observation published_at: `2026-04-20T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE10TjJrTVBHb1BMUC1NbFZnS2ZjdlNmMENIRW1qQzJTRWxGNDdNVm42SlhzbGNpbXo4Zy04SjljTUxEU1ZGRE5CYTJCQld3dUc4S1J3YXYxb1pTSWlUSlFpbmJpUDhfRzdJcFpmalhfWQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
