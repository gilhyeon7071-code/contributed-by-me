---
id: source-2026-07-22-KRX-196170-google-rss-coverage
type: source
title: KRX 196170 Google RSS Coverage Source
created: 2026-07-22
updated: 2026-07-22
status: raw
stage: 0

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-22

analysis:
  summary: Local coverage report row shows news coverage for KRX 196170.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=3
    - google_rss_article_count=4
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=196170`
- `name=알테오젠`
- `naver_article_count=3`
- `google_rss_article_count=4`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 196170.

## RSS Item Metadata
- Title: `"코스닥 1등이 좋아" 알테오젠, 코스피 안 가는 까닭 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-07-21T15:50:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE5jMHlqYUE2eVU1eXNqQVVLRnpSRlNmSnhEVEx6Y3ZwSjNWemZ6alJBbnpPLVJ4NzVobWhxZ1pkWUFfLTBpWnM0OW1LUXM3MGRMUy1wR0l5ZVFLa0V4LXEtOGxRekJ1eXVtVHZnVDRYQlQ?oc=5`

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
- Updated at: `2026-08-21T19:30:38+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-22.json`
- Latest observation title: `"코스닥 1등이 좋아" 알테오젠, 코스피 안 가는 까닭 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-07-21T15:50:05+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE5jMHlqYUE2eVU1eXNqQVVLRnpSRlNmSnhEVEx6Y3ZwSjNWemZ6alJBbnpPLVJ4NzVobWhxZ1pkWUFfLTBpWnM0OW1LUXM3MGRMUy1wR0l5ZVFLa0V4LXEtOGxRekJ1eXVtVHZnVDRYQlQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
