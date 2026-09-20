---
id: source-2026-05-26-KRX-185490-google-rss-coverage
type: source
title: KRX 185490 Google RSS Coverage Source
created: 2026-05-26
updated: 2026-05-26
status: raw
stage: 0

market: KRX
ticker: "185490"
company: 아이진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 185490.
  key_facts:
    - code=185490
    - name=아이진
    - naver_article_count=1
    - google_rss_article_count=9
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 185490
    - 아이진
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

# KRX 185490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=185490`
- `name=아이진`
- `naver_article_count=1`
- `google_rss_article_count=9`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 185490.

## RSS Item Metadata
- Title: `아이진, 한타바이러스 백신 과제 선정에 '上'…녹십자MS·리브스메드 '들썩'[바이... - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-05-21T08:00:06+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1TSlBrQkctM3Z4bTdkckVlejlPWGtOMUQ5N0VFS2hQY1JUblFuUjVvdUdMeHk3VWhiU3BSTG1XLUNNVjQweE1HZGtzUGk5ODR4NzFCVEhROGFTVC1QbkxhamIxSk1kbEJNX24wblJB?oc=5`

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
- Updated at: `2026-05-26T07:05:05+09:00`
- Company: [[KRX_185490_아이진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `아이진, 한타바이러스 백신 과제 선정에 '上'…녹십자MS·리브스메드 '들썩'[바이... - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-05-21T08:00:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1TSlBrQkctM3Z4bTdkckVlejlPWGtOMUQ5N0VFS2hQY1JUblFuUjVvdUdMeHk3VWhiU3BSTG1XLUNNVjQweE1HZGtzUGk5ODR4NzFCVEhROGFTVC1QbkxhamIxSk1kbEJNX24wblJB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
