---
id: verification-2026-08-21-KRX-138610-google-rss-coverage
type: verification
title: KRX 138610 Google RSS Coverage Verification
created: 2026-08-21
updated: 2026-08-21
status: verification
stage: 1

market: KRX
ticker: "138610"
company: 나이벡
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=138610
    - name=나이벡
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=25
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 138610
    - 나이벡
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

# KRX 138610 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-21_KRX_138610_google-rss-coverage-source]]

## Facts Checked
- `code=138610`
- `name=나이벡`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 모더나 3상 성공에 mRNA주 강세…나이벡·소마젠 상한가 - 자본시장뉴스`
- Source: `자본시장뉴스`
- Published at: `2026-08-20T09:41:57+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE5aQ2dYeWc5VDk5UTlLLTNnbWtReC0wbDNHOFJvb1VOTkhWaTJrUlpMQkd4aE9HUnkzNXRaM1pUeUdVc2JOY0VWT3BCMjMwc0ttVWdFaERYTXZzZEwtSDl5WHlIRWE1UHc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_138610_나이벡]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `나이벡, 공매도 과열종목 지정기간 연장에 따른 공매도 거래 금지 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-21T20:24:55+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE5GOEZMZkg5dTVPZy1uLXZKcWxTOFFlNkY3YW9YeUNWcjlOWVRJaWtZTUNTM3pjeHZGZ2VnMV9kWDFwQ19wSi1rNzVRbkh6MFlHSDYxUGc1RXZfenpuV2VyNF8wVUdpSl94UVBHTDRheG13dw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
