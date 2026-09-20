---
id: verification-2026-07-27-KRX-079550-google-rss-coverage
type: verification
title: KRX 079550 Google RSS Coverage Verification
created: 2026-07-27
updated: 2026-07-27
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
  collected_at: 2026-07-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=079550
    - name=LIG넥스원
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=16
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
- [[2026-07-27_KRX_079550_google-rss-coverage-source]]

## Facts Checked
- `code=079550`
- `name=LIG넥스원`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“베트남선 19년간 전쟁” 트럼프 외침에도…LIG넥스원 목표가 하향 왜 [오늘 나온 보고서] - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-22T08:45:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTFBjeEhkQUJmcUlUWjQwSmpDNVVEMWVic0lHX3NLZjVYak5fQ3RUcHVoM0lhLVF6NFdNTDEwYkhBNFBRVVJ6Q2dpRGh3Yw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:13+09:00`
- Company: [[KRX_079550_LIG넥스원]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `LIG D&A, 소방관 위한 무인수상정 개발 착수 - 한국재난안전뉴스`
- Latest observation source: `한국재난안전뉴스`
- Latest observation published_at: `2026-07-27T13:07:11+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE9oMzlyYVhBcVNPZWJ2NWtxSlY1Q24xa2lEdFNKaVFka21lek1qZ2JMN01WdDA4UFpjXzJEQUZpUXo3WXprU3VnNWtkVENKMkNhRlY0WmJjbjVLNmJzT2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
