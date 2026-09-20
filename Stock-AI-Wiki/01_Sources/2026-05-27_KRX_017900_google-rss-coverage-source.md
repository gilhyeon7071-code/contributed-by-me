---
id: source-2026-05-27-KRX-017900-google-rss-coverage
type: source
title: KRX 017900 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "017900"
company: 광전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 017900.
  key_facts:
    - code=017900
    - name=광전자
    - naver_article_count=0
    - google_rss_article_count=6
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017900
    - 광전자
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

# KRX 017900 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=017900`
- `name=광전자`
- `naver_article_count=0`
- `google_rss_article_count=6`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 017900.

## RSS Item Metadata
- Title: `광전자·빛과전자·대한광통신·한국첨단소재, 중동 긴장 재확산 속 광통신주 동반 강세…호르무즈 변수 촉각 - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-05-27T00:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5uTjVURmdiN3JmQ0syWXctOTEzalE3ZG1DQmV3NDR3aU8tZnhvYTFVaHNVTllSOVVabXpYbnBVc2FVRGRfLXVKeDhrOWZZNjVrdTlFb0lDblNYR095SWZUU3pCN0M0OGkw?oc=5`

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
- Updated at: `2026-05-27T09:05:50+09:00`
- Company: [[KRX_017900_광전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `광전자·빛과전자·대한광통신·한국첨단소재, 중동 긴장 재확산 속 광통신주 동반 강세…호르무즈 변수 촉각 - CBC뉴스`
- Latest observation source: `CBC뉴스`
- Latest observation published_at: `2026-05-27T00:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5uTjVURmdiN3JmQ0syWXctOTEzalE3ZG1DQmV3NDR3aU8tZnhvYTFVaHNVTllSOVVabXpYbnBVc2FVRGRfLXVKeDhrOWZZNjVrdTlFb0lDblNYR095SWZUU3pCN0M0OGkw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
