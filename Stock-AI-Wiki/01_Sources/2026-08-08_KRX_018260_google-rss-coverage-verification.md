---
id: verification-2026-08-08-KRX-018260-google-rss-coverage
type: verification
title: KRX 018260 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_018260_google-rss-coverage-source]]

## Facts Checked
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성에스디에스 주가, 8월 7일 221,500원 4.73% 하락 마감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-07T15:52:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE55c2JaVnVqSklZWklwcHIycHdoMGZ1Njd4MTR2cllnVUl5dzVRTVdNRGN2bm9YMkE3RWVVVl9CVWlvVFpYSGdGUkh3RTF3MTMzVU00bW9yRGxzbDVoYWZUSTV5d05xaFdqMzVzdlZkdlgtQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-08T12:07:14+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `삼성에스디에스 주가, 8월 7일 221,500원 4.73% 하락 마감 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-07T15:52:17+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE55c2JaVnVqSklZWklwcHIycHdoMGZ1Njd4MTR2cllnVUl5dzVRTVdNRGN2bm9YMkE3RWVVVl9CVWlvVFpYSGdGUkh3RTF3MTMzVU00bW9yRGxzbDVoYWZUSTV5d05xaFdqMzVzdlZkdlgtQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
