---
id: source-2026-07-13-KRX-087010-google-rss-coverage
type: source
title: KRX 087010 Google RSS Coverage Source
created: 2026-07-13
updated: 2026-07-13
status: raw
stage: 0

market: KRX
ticker: "087010"
company: 펩트론
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 087010.
  key_facts:
    - code=087010
    - name=펩트론
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 087010
    - 펩트론
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

# KRX 087010 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=087010`
- `name=펩트론`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 087010.

## RSS Item Metadata
- Title: `'바이오 대형주' HLB·펩트론, 돌발 악재에 동반 하한가 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-10T18:23:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5Sd2RJN1pzRzV0Q0hzZzVscTh3UHNpelJPUzVUVVFfZkNiYTRqdmxsNGhnT2JwdUNncUU3MVJfUFZybnFVYTgzamM0TWEtTUtBRFZaamNnXy0zZ9IBVEFVX3lxTE5NcVluWWg4UEJOQ3N4bXhKVzdENEp6ZGplR3ZVS2U0Vy1RZ1Zzd056TjRwNU9qbnJzUUhVSmlDYU9xbGY3S2NCZnVXT0FBbU5WeVZnMQ?oc=5`

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
- Company: [[KRX_087010_펩트론]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-13.json`
- Latest observation title: `펩트론 주가 반등 성공...릴리 비만약 논란 해명 - gukjenews.com`
- Latest observation source: `gukjenews.com`
- Latest observation published_at: `2026-07-13T09:32:08+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE5SeE9OODJEV0VaeEhlQjJfbVVSdjJwanVnaklPNVM4NTZ1Uk1RaXVvUzVMSi13NXdhN3RNUDRKX1YtWnZ0aThwdXNTT050Uy1fWjBmXzJkMk5icXFiMFBXdTFJQngyNUxoRVhVdGlR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
