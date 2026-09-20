---
id: source-2026-06-21-KRX-000720-google-rss-coverage
type: source
title: KRX 000720 Google RSS Coverage Source
created: 2026-06-21
updated: 2026-06-21
status: raw
stage: 0

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 000720.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=2
    - google_rss_article_count=117
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000720`
- `name=현대건설`
- `naver_article_count=2`
- `google_rss_article_count=117`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000720.

## RSS Item Metadata
- Title: `현대건설·남동발전, 석탄화력발전소 SMR 전환기술 공동개발 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-06-19T14:18:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9KUEdEajduRGNOQjdNbUVVaFNnbE5McnNXNGRaZ01Ka2VmdFRuVzdkZjA1Q2RwbXRjc0hPTElXNHBKWTZUdTZiMHYzYzY5XzVCcFF0V05aRjZxVzDSAWBBVV95cUxNYjlfODZQZlZWVXd1Zl82RFJXeGk1d1IyMjA0WWtrSTJkdW1NTWpqTnYwckw5WEV2aW5SemdQUnRyc21SaTJCVWtTUzVfaGs5dllFeHBLU3FqdGstZUhjY2c?oc=5`

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
- Updated at: `2026-08-21T19:23:01+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-21.json`
- Latest observation title: `양효진 떠난 수원 현대건설, 더 빨라진다…강성형 감독의 ‘새 우승 공식’ - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-21T13:04:45+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE8ya21WdVhTRVlJeTRRTmhaLVJnclVndVhZb0dUWkpTVG5XNE52V2xDSktuaVoxNzd4TVJWX0daT2pxa0x0enRUd3d5VktBSGs?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
