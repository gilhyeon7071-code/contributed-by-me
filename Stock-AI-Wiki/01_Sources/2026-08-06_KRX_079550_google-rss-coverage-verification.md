---
id: verification-2026-08-06-KRX-079550-google-rss-coverage
type: verification
title: KRX 079550 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

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
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 079550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_079550_google-rss-coverage-source]]

## Facts Checked
- `code=079550`
- `name=LIG넥스원`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LIG넥스원 주가, 8월 4일 장중 789,000원 12.88% 상승 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-04T11:02:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBUS0NUMmlnNGlqZmgtS0loM1ZXZWNsRkh6b28tYXJTUkxvZlhuR3pRcm1ReFZJVng3LVB1OE9QRHltRGxyakpiQ1lHYmx5RlZtSmpoRUphcDh0RUtkdDJRR3ZmRzFLcFhEanVzMlVScEgydw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

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
