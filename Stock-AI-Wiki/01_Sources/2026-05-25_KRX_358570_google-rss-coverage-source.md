---
id: source-2026-05-25-KRX-358570-google-rss-coverage
type: source
title: KRX 358570 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "358570"
company: 지아이이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 358570.
  key_facts:
    - code=358570
    - name=지아이이노베이션
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 358570
    - 지아이이노베이션
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

# KRX 358570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=358570`
- `name=지아이이노베이션`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 358570.

## RSS Item Metadata
- Title: `지아이이노베이션 "GI-101A, 모든 표준치료 실패한 신장암서 객관적반응률 40% 확인" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-22T08:39:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9mWW9iUnUwaVprcXBEdnBTVW5iTTcwcW5lUVJfU2k0Wm8xcFltbmNiRFc1NFl3WWdRWlNIWXZxTUJCQ29FMk4zNGU0Qm1fU0k?oc=5`

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
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_358570_지아이이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `지아이이노베이션 "GI-101A, 모든 표준치료 실패한 신장암서 객관적반응률 40% 확인" - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-22T08:39:16+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9mWW9iUnUwaVprcXBEdnBTVW5iTTcwcW5lUVJfU2k0Wm8xcFltbmNiRFc1NFl3WWdRWlNIWXZxTUJCQ29FMk4zNGU0Qm1fU0k?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
