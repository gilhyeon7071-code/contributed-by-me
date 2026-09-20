---
id: source-2026-06-23-KRX-014680-google-rss-coverage
type: source
title: KRX 014680 Google RSS Coverage Source
created: 2026-06-23
updated: 2026-06-23
status: raw
stage: 0

market: KRX
ticker: "014680"
company: 한솔케미칼
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-23

analysis:
  summary: Local coverage report row shows news coverage for KRX 014680.
  key_facts:
    - code=014680
    - name=한솔케미칼
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 014680
    - 한솔케미칼
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

# KRX 014680 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=014680`
- `name=한솔케미칼`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 014680.

## RSS Item Metadata
- Title: `한솔케미칼, 600억 규모 자사주 소각..."주주가치 제고" - thecommoditiesnews.com`
- Source: `thecommoditiesnews.com`
- Published at: `2026-06-19T15:54:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE9ySUczTTZfMVB2d0JxLW03bGRfbGVGTXVXTDYtZlZyVTZKeVBUd2ZSd19FMjNYRUZPSm56SWplSDk5QmFhcXp6U3d6blpoalRhZUFGRVFMZlEwS0pHNUJoMnBKa0p5RWVLZXJfcGJCcXNHbkZ2THQ4?oc=5`

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
- Updated at: `2026-08-21T19:23:39+09:00`
- Company: [[KRX_014680_한솔케미칼]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-23.json`
- Latest observation title: `한솔케미칼, 600억 규모 자사주 소각..."주주가치 제고" - thecommoditiesnews.com`
- Latest observation source: `thecommoditiesnews.com`
- Latest observation published_at: `2026-06-19T15:54:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMid0FVX3lxTE9ySUczTTZfMVB2d0JxLW03bGRfbGVGTXVXTDYtZlZyVTZKeVBUd2ZSd19FMjNYRUZPSm56SWplSDk5QmFhcXp6U3d6blpoalRhZUFGRVFMZlEwS0pHNUJoMnBKa0p5RWVLZXJfcGJCcXNHbkZ2THQ4?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
