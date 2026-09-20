---
id: source-2026-08-21-KRX-222800-google-rss-coverage
type: source
title: KRX 222800 Google RSS Coverage Source
created: 2026-08-21
updated: 2026-08-21
status: raw
stage: 0

market: KRX
ticker: "222800"
company: 심텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 222800.
  key_facts:
    - code=222800
    - name=심텍
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 222800
    - 심텍
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

# KRX 222800 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=222800`
- `name=심텍`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 222800.

## RSS Item Metadata
- Title: `심텍, 3000억 조달 추진…반도체 소부장社 자본확충 봇물 [시그널] - 서울경제`
- Source: `서울경제`
- Published at: `2026-08-20T16:25:31+09:00`
- Link: `https://news.google.com/rss/articles/CBMiV0FVX3lxTE1uZ3RSUG1nNmd4dUxTMUhSM2pkLUk5OV9SbEhXMW0xUXRPSXkzZXBCRWI1YVpPLTMxcHZ6MDlmSE1Xd2EwYkF1MEtEdG02UEFpZmVLMHFTdw?oc=5`

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
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_222800_심텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `심텍, 3000억 조달 추진…반도체 소부장社 자본확충 봇물 [시그널] - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-08-20T16:25:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1kWDYxTFVsaEFJNkJzbnNTMVFLQzhKX3RVTWxDZU15d2lIUGRFeHhCd25ESmhsaFpMdzJwMVczUUJvUXdDdmc2bjFESWh6cGJFbGExek13?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
