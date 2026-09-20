---
id: source-2026-07-13-KRX-001820-google-rss-coverage
type: source
title: KRX 001820 Google RSS Coverage Source
created: 2026-07-13
updated: 2026-07-13
status: raw
stage: 0

market: KRX
ticker: "001820"
company: 삼화콘덴서공업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 001820.
  key_facts:
    - code=001820
    - name=삼화콘덴서공업
    - naver_article_count=0
    - google_rss_article_count=29
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 001820
    - 삼화콘덴서공업
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

# KRX 001820 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=001820`
- `name=삼화콘덴서공업`
- `naver_article_count=0`
- `google_rss_article_count=29`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 001820.

## RSS Item Metadata
- Title: `삼화콘덴서공업, 2026년 기업가치 제고 계획 발표 - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-03-27T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5oTER3dXA3RHVWMzRaWW1DMVA1RDN1U0pVYjdjUEN6RC1uNEJVS2JobnZPMkdQMW5JamZVTHJfRE5Hdzhjb09wREU5UG0yVHRNaC13bEpEZUpyR1QzNGltRzRWNVNtX3BYNUFvelBZLURUdGM?oc=5`

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
- Updated at: `2026-08-21T19:29:02+09:00`
- Company: [[KRX_001820_삼화콘덴서공업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-13.json`
- Latest observation title: `삼화콘덴서공업, 2026년 기업가치 제고 계획 발표 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-03-27T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5oTER3dXA3RHVWMzRaWW1DMVA1RDN1U0pVYjdjUEN6RC1uNEJVS2JobnZPMkdQMW5JamZVTHJfRE5Hdzhjb09wREU5UG0yVHRNaC13bEpEZUpyR1QzNGltRzRWNVNtX3BYNUFvelBZLURUdGM?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
