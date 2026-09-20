---
id: source-2026-06-05-KRX-128940-google-rss-coverage
type: source
title: KRX 128940 Google RSS Coverage Source
created: 2026-06-05
updated: 2026-06-05
status: raw
stage: 0

market: KRX
ticker: "128940"
company: 한미약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Local coverage report row shows news coverage for KRX 128940.
  key_facts:
    - code=128940
    - name=한미약품
    - naver_article_count=0
    - google_rss_article_count=23
    - kis_title_count=30
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 128940
    - 한미약품
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

# KRX 128940 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=128940`
- `name=한미약품`
- `naver_article_count=0`
- `google_rss_article_count=23`
- `kis_title_count=30`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 128940.

## RSS Item Metadata
- Title: `[HIT알공] 한미약품·오스코텍, 합쳐서 2.9조원 기술 수출 - 히트뉴스`
- Source: `히트뉴스`
- Published at: `2026-06-01T18:13:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE90enVqWng2RklQT0RVZXlMdFZnWDh5ZUhJY185cjFYWG1Jc2VIQjdoNWxiSGpFWEtUX2FIcHJzSzZWNHZSS0NJSHowYmJnZ1RfUmE4a2tIcUVFcWk0S0hqTFROaGZkMVlLajVN?oc=5`

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
- Updated at: `2026-06-05T08:11:49+09:00`
- Company: [[KRX_128940_한미약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `한미약품, 릴리와 '깜짝' 기술이전 계약…"추가 L/O 가능성" - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-06-04T08:32:35+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9sSmtuYmU0WDN6LUVTUnJ4SXJoSGtULUxzTGI1YWRDR1E2MzJXa3pBR1hJRXBnMjM0amRqMzIyYjF3Sm13WVh2ekdSZnVaQi1HRU1WUTRGMmhTb1BiUE45Zy1oa1dwdGNjUGtXNjdJM0U?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
