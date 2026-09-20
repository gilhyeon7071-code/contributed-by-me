---
id: source-2026-08-06-KRX-079550-google-rss-coverage
type: source
title: KRX 079550 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "079550"
company: LIG넥스원
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 079550.
  key_facts:
    - code=079550
    - name=LIG넥스원
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 079550
    - LIG넥스원
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

# KRX 079550 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=079550`
- `name=LIG넥스원`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 079550.

## RSS Item Metadata
- Title: `LIG넥스원 주가, 8월 4일 장중 789,000원 12.88% 상승 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-04T11:02:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBUS0NUMmlnNGlqZmgtS0loM1ZXZWNsRkh6b28tYXJTUkxvZlhuR3pRcm1ReFZJVng3LVB1OE9QRHltRGxyakpiQ1lHYmx5RlZtSmpoRUphcDh0RUtkdDJRR3ZmRzFLcFhEanVzMlVScEgydw?oc=5`

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
- Updated at: `2026-08-06T16:05:15+09:00`
- Company: [[KRX_079550_LIG넥스원]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `방사청 직원에 뇌물 혐의…LIG D&A 임직원 구속 - 뉴스저널리즘`
- Latest observation source: `뉴스저널리즘`
- Latest observation published_at: `2026-08-06T14:36:33+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTFBRQnhKR0sycFVMb3hkX25fOHA3VnZ2bHNTcTZPcHRMcGExVGFid01ycnZSeGZYUGU0SmYweHBiNG9FMkoxNGk2WnBoSzRoOHJmVlpzYkMtS0F2TkMyX0RpM3BIcFJxT3VNQkhJ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
