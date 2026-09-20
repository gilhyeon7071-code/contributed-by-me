---
id: verification-2026-06-27-KRX-007660-google-rss-coverage
type: verification
title: KRX 007660 Google RSS Coverage Verification
created: 2026-06-27
updated: 2026-06-27
status: verification
stage: 1

market: KRX
ticker: "007660"
company: 이수페타시스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=007660
    - name=이수페타시스
    - naver_article_count=3
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 007660
    - 이수페타시스
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

# KRX 007660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-27_KRX_007660_google-rss-coverage-source]]

## Facts Checked
- `code=007660`
- `name=이수페타시스`
- `naver_article_count=3`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"이수페타시스, 구글 TPU 기판 점유율 하락 우려 과도"-NH - 한국경제`
- Source: `한국경제`
- Published at: `2026-06-25T07:50:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE0wdDl6Q1o0N1ctSnk2cU9aQ1BIc0hPdWJwVTVhdG04cG5aS3Y1a2ZGcGVDTDlDeXYtRk00NDN4b2xqY0E5My1tSXhiVkRNTERISXkyQlo5S0xsQdIBVEFVX3lxTE95a3RGWTE1enVQMjduamcyd3hGUEo0ZmNSdFVjSmVTSjVMa1BfSUlvVVRCSndWeG15QWdpNHR4bnhMX09yS2dhbHlBTmF5RC10ZFA3cg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:56+09:00`
- Company: [[KRX_007660_이수페타시스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-27.json`
- Latest observation title: `"이수페타시스, 구글 TPU 기판 점유율 하락 우려 과도"-NH - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-06-25T07:50:56+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE0wdDl6Q1o0N1ctSnk2cU9aQ1BIc0hPdWJwVTVhdG04cG5aS3Y1a2ZGcGVDTDlDeXYtRk00NDN4b2xqY0E5My1tSXhiVkRNTERISXkyQlo5S0xsQdIBVEFVX3lxTE95a3RGWTE1enVQMjduamcyd3hGUEo0ZmNSdFVjSmVTSjVMa1BfSUlvVVRCSndWeG15QWdpNHR4bnhMX09yS2dhbHlBTmF5RC10ZFA3cg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
