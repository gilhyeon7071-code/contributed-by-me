---
id: verification-2026-05-20-KRX-347700-google-rss-coverage
type: verification
title: KRX 347700 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "347700"
company: 스피어
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
    - code=347700
    - name=스피어
    - naver_article_count=2
    - google_rss_article_count=11
    - kis_title_count=25
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 347700
    - 스피어
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

# KRX 347700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_347700_google-rss-coverage-source]]

## Facts Checked
- `code=347700`
- `name=스피어`
- `naver_article_count=2`
- `google_rss_article_count=11`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `스피어, 4회차 CB 재매각으로 900억 확보…우주 인프라ㆍGSCM 영토 넓힌다 - 이투데이`
- Source: `이투데이`
- Published at: `2026-05-19T17:47:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFB5ZUhCQTFQTm5VVl9jQTQ2eV9wUy1TbDJkTjBrUTEyQXAtLVBBeG84SHkxVzJFSm5iMC15emVDQUhCV093azJlY3EtdjFCRDR1bjNCOQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_347700_스피어]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `스피어, 4회차 CB 재매각…900억 현금유동성 확보 - 블로터`
- Latest observation source: `블로터`
- Latest observation published_at: `2026-05-19T20:14:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9ObHQ0WDlhenRKY2tIdlg1d1p3QmhLTXE2NGgtZEoyMHAtQnhnWW9oZnpwb0NmWGwxTnNPVkpZRW9aYThKc2x2VWxURDRUQjNtQkdtTERGMzNSa3IwNkNSOF9hSHZZcThk0gFsQVVfeXFMT2RQQmN0Q3V1bWE4NEN6bkNlOVVMaDhud1VtSDR3LTBFb2lfenFYakxNS0pXZ3F5b25SZXFrbkZDVEsxNjZQYVg2YThpWm5oaGhvQU4xUk9pc1R1Rm5OakM5U243aTBwZjlRbks1?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
