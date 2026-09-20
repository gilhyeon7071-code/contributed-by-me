---
id: source-2026-05-29-KRX-048530-google-rss-coverage
type: source
title: KRX 048530 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "048530"
company: 인트론바이오
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 048530.
  key_facts:
    - code=048530
    - name=인트론바이오
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 048530
    - 인트론바이오
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

# KRX 048530 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=048530`
- `name=인트론바이오`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 048530.

## RSS Item Metadata
- Title: `인트론바이오, 과수화상병 예방·치료 신약후보 ‘EAL2200’ 항균 효능 검증 - 약업신문`
- Source: `약업신문`
- Published at: `2026-05-28T11:28:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiekFVX3lxTE5wSFI2a3NKeURsdF9JNThzOGp1TWZ6dDAtMnpGOHVWZkJzN0g1SjdYc256T2VzVUxJN3Z3ZFRrNDA0NjE5TGtEdUNNWGV2M0JvWXNMRzdYdkgwMS0zSGMtSjhYbWUtZFRueFRsLXBoQktvbS1ocUVXMDRn?oc=5`

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
- Company: [[KRX_048530_인트론바이오]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `인트론바이오, 과수화상병 예방·치료 신약후보 ‘EAL2200’ 항균 효능 검증 - 약업신문`
- Latest observation source: `약업신문`
- Latest observation published_at: `2026-05-28T11:28:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiekFVX3lxTE5wSFI2a3NKeURsdF9JNThzOGp1TWZ6dDAtMnpGOHVWZkJzN0g1SjdYc256T2VzVUxJN3Z3ZFRrNDA0NjE5TGtEdUNNWGV2M0JvWXNMRzdYdkgwMS0zSGMtSjhYbWUtZFRueFRsLXBoQktvbS1ocUVXMDRn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
