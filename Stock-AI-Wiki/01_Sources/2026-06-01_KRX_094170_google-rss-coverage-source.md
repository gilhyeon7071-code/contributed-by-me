---
id: source-2026-06-01-KRX-094170-google-rss-coverage
type: source
title: KRX 094170 Google RSS Coverage Source
created: 2026-06-01
updated: 2026-06-01
status: raw
stage: 0

market: KRX
ticker: "094170"
company: 동운아나텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 094170.
  key_facts:
    - code=094170
    - name=동운아나텍
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 094170
    - 동운아나텍
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

# KRX 094170 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=094170`
- `name=동운아나텍`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 094170.

## RSS Item Metadata
- Title: `동운아나텍, 올해 타액 기반 혈당측정기 'D-SaLife' 판매 - 디일렉`
- Source: `디일렉`
- Published at: `2026-03-27T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE1BR1pPa1lxVThaS1BHcnRvTXJnOGpKMFF3dXRmaFhSSHhHRXFZUTkxRnVqWC1obnV0Y2tWLTc0eHNROUNKOUFncFRtUUF2N0tQX0ZHdG00VTcwRjFEZWdGTjNYWlBwQQ?oc=5`

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
- Updated at: `2026-08-21T19:17:10+09:00`
- Company: [[KRX_094170_동운아나텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `동운아나텍, 올해 타액 기반 혈당측정기 'D-SaLife' 판매 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-03-27T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE1BR1pPa1lxVThaS1BHcnRvTXJnOGpKMFF3dXRmaFhSSHhHRXFZUTkxRnVqWC1obnV0Y2tWLTc0eHNROUNKOUFncFRtUUF2N0tQX0ZHdG00VTcwRjFEZWdGTjNYWlBwQQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
