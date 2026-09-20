---
id: source-2026-05-28-KRX-052710-google-rss-coverage
type: source
title: KRX 052710 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "052710"
company: 아모텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 052710.
  key_facts:
    - code=052710
    - name=아모텍
    - naver_article_count=3
    - google_rss_article_count=21
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 052710
    - 아모텍
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

# KRX 052710 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=052710`
- `name=아모텍`
- `naver_article_count=3`
- `google_rss_article_count=21`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 052710.

## RSS Item Metadata
- Title: `[유상증자 모니터] 아모텍, 주주업고 MLCC 증설…수익 개선 시험대 - 블로터`
- Source: `블로터`
- Published at: `2026-05-27T15:46:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBxa0E3bUJyVFN4RTRmOVJ4d0tEUGVRU2R0c0tXWlNPRHBRd2lkeHpXZzc2eU1Vc0hWQ3FYbGV1cXFiUi1tR0dtbVJpa20ya0FsaW9qTW14VkpuTm85VTh6U3AxM2w3TGI10gFsQVVfeXFMT2ptdi1oYnN2bks3OERqSEMyZE5aaG9MazRqTmZzUC0tSmcxbWZLUXEzeWN3LUZTM0E2aG5nNkpSWXJ4dHA1ZWRCeW5aR2ZVbmFqd1ZwU0dGeHp3bVFiUTNmMEhiVDJCMXprQmpU?oc=5`

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
- Updated at: `2026-05-28T12:05:06+09:00`
- Company: [[KRX_052710_아모텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `[유상증자 모니터] 아모텍, 주주업고 MLCC 증설…수익 개선 시험대 - 블로터`
- Latest observation source: `블로터`
- Latest observation published_at: `2026-05-27T15:46:14+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBxa0E3bUJyVFN4RTRmOVJ4d0tEUGVRU2R0c0tXWlNPRHBRd2lkeHpXZzc2eU1Vc0hWQ3FYbGV1cXFiUi1tR0dtbVJpa20ya0FsaW9qTW14VkpuTm85VTh6U3AxM2w3TGI10gFsQVVfeXFMT2ptdi1oYnN2bks3OERqSEMyZE5aaG9MazRqTmZzUC0tSmcxbWZLUXEzeWN3LUZTM0E2aG5nNkpSWXJ4dHA1ZWRCeW5aR2ZVbmFqd1ZwU0dGeHp3bVFiUTNmMEhiVDJCMXprQmpU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
