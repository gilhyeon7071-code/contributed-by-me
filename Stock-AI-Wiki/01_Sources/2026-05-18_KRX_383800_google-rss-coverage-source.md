---
id: source-2026-05-18-KRX-383800-google-rss-coverage
type: source
title: KRX 383800 Google RSS Coverage Source
created: 2026-05-18
updated: 2026-05-18
status: raw
stage: 0

market: KRX
ticker: "383800"
company: LX홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Local coverage report row shows news coverage for KRX 383800.
  key_facts:
    - code=383800
    - name=LX홀딩스
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 383800
    - LX홀딩스
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

# KRX 383800 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=383800`
- `name=LX홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 383800.

## RSS Item Metadata
- Title: `LX홀딩스, 1Q 영업익 416억 '흑자 전환'…전년比 41.7% 감소 - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-05-14T18:32:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE5fUERMZndjVVNQUms0bm5EZ0hsemZ4Sl9BSGhLeVBzYnQ4RnRGZzdZTGVkckkzSy1FdjdpRnRpQ2VLVzYtcW1kR0kwbGp5eS1SMGFkLTZ5c0laZVltLUFiOFJSdmE?oc=5`

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
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_383800_LX홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `LX홀딩스, 1Q 영업익 416억 '흑자 전환'…전년比 41.7% 감소 - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-05-14T18:32:16+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE5fUERMZndjVVNQUms0bm5EZ0hsemZ4Sl9BSGhLeVBzYnQ4RnRGZzdZTGVkckkzSy1FdjdpRnRpQ2VLVzYtcW1kR0kwbGp5eS1SMGFkLTZ5c0laZVltLUFiOFJSdmE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
