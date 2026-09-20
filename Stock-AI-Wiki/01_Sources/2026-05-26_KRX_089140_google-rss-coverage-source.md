---
id: source-2026-05-26-KRX-089140-google-rss-coverage
type: source
title: KRX 089140 Google RSS Coverage Source
created: 2026-05-26
updated: 2026-05-26
status: raw
stage: 0

market: KRX
ticker: "089140"
company: 넥스턴앤롤코리아
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 089140.
  key_facts:
    - code=089140
    - name=넥스턴앤롤코리아
    - naver_article_count=4
    - google_rss_article_count=9
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 089140
    - 넥스턴앤롤코리아
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

# KRX 089140 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=089140`
- `name=넥스턴앤롤코리아`
- `naver_article_count=4`
- `google_rss_article_count=9`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 089140.

## RSS Item Metadata
- Title: `넥스턴앤롤코리아, 미래산업 지분 40.4% 부각…151억 평가차익 ‘톡톡’ - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-22T10:33:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE5jYkhEWlhOVEpHSTFod1NWYVpmMWdoeEFXd0pDLTBFaFlJNjZQU2wzalhBQWNzQ3RGcFZhQ0ZVb2NfOC1WaEM0VTVLdnpHQUwxd1BIMQ?oc=5`

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
- Updated at: `2026-05-26T09:05:07+09:00`
- Company: [[KRX_089140_넥스턴앤롤코리아]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `넥스턴앤롤코리아, 미래산업 지분 40.4% 부각…151억 평가차익 ‘톡톡’ - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-22T10:33:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE5jYkhEWlhOVEpHSTFod1NWYVpmMWdoeEFXd0pDLTBFaFlJNjZQU2wzalhBQWNzQ3RGcFZhQ0ZVb2NfOC1WaEM0VTVLdnpHQUwxd1BIMQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
