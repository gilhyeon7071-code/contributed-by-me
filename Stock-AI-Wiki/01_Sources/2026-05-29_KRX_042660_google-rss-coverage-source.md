---
id: source-2026-05-29-KRX-042660-google-rss-coverage
type: source
title: KRX 042660 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "042660"
company: 한화오션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 042660.
  key_facts:
    - code=042660
    - name=한화오션
    - naver_article_count=1
    - google_rss_article_count=63
    - kis_title_count=24
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042660
    - 한화오션
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

# KRX 042660 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=042660`
- `name=한화오션`
- `naver_article_count=1`
- `google_rss_article_count=63`
- `kis_title_count=24`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 042660.

## RSS Item Metadata
- Title: `[단독]한화오션, 핵잠 설계 올해 마무리 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-05-27T08:58:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYkFVX3lxTE0zV1ZMYXM0eUdQTDJCVmVFYm9OeV9lSVJaT3Z2cG03WEpldXN1UWJWUUF5Z1BUaUJ6SHN5cmdmSFBOYjRjRnl2RGVpVXJtOEMxc01VQkpUS0E4UXF6RlotU3Zn?oc=5`

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
- Updated at: `2026-05-29T08:05:03+09:00`
- Company: [[KRX_042660_한화오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `[단독]한화오션, 핵잠 설계 올해 마무리 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-05-27T08:58:26+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYkFVX3lxTE0zV1ZMYXM0eUdQTDJCVmVFYm9OeV9lSVJaT3Z2cG03WEpldXN1UWJWUUF5Z1BUaUJ6SHN5cmdmSFBOYjRjRnl2RGVpVXJtOEMxc01VQkpUS0E4UXF6RlotU3Zn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
