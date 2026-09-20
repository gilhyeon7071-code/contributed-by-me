---
id: verification-2026-05-20-KRX-177350-google-rss-coverage
type: verification
title: KRX 177350 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "177350"
company: 베셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=177350
    - name=베셀
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 177350
    - 베셀
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

# KRX 177350 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_177350_google-rss-coverage-source]]

## Facts Checked
- `code=177350`
- `name=베셀`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `베셀, 임직원 대상 보통주 3만9542주 자기주식 처분 - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-05-20T11:32:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBCVmFWTjNJZ0tZZlZmMUZHTHJCTFVjUjVjcWR0ZDlSWUh2QzJnX0dwbGNTSDhrWmNLNTE0VzVITm5uZnBUSkpUZHpYVzVFdEt5Z2drWGpGOHZ4MHV0T0gweDMta3o5RHZFYTN5MThXZjRPQkU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T18:05:04+09:00`
- Company: [[KRX_177350_베셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `베셀, 임직원 대상 보통주 3만9542주 자기주식 처분 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-05-20T11:32:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBCVmFWTjNJZ0tZZlZmMUZHTHJCTFVjUjVjcWR0ZDlSWUh2QzJnX0dwbGNTSDhrWmNLNTE0VzVITm5uZnBUSkpUZHpYVzVFdEt5Z2drWGpGOHZ4MHV0T0gweDMta3o5RHZFYTN5MThXZjRPQkU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
