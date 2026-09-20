---
id: verification-2026-06-04-KRX-373220-google-rss-coverage
type: verification
title: KRX 373220 Google RSS Coverage Verification
created: 2026-06-04
updated: 2026-06-04
status: verification
stage: 1

market: KRX
ticker: "373220"
company: LG에너지솔루션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-04

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=373220
    - name=LG에너지솔루션
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 373220
    - LG에너지솔루션
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

# KRX 373220 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-04_KRX_373220_google-rss-coverage-source]]

## Facts Checked
- `code=373220`
- `name=LG에너지솔루션`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `이차전지株, 하반기 쏟아질 수주…주가 반등 기대 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-06-02T15:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYkFVX3lxTE5HM1dDS2dvSERwMXdNU3lTTmZnQWRpLVA4VTd1aGY2a3ZDWHU1TmVOTmNvcnktajcwZk9zcWZTOWNTWnZnbHp3YU5IUVR0LXRIdW1KSndsWHlrNzhNZ3NVMHZB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-04T09:05:04+09:00`
- Company: [[KRX_373220_LG에너지솔루션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-04.json`
- Latest observation title: `이차전지株, 하반기 쏟아질 수주…주가 반등 기대 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-06-02T15:30:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYkFVX3lxTE5HM1dDS2dvSERwMXdNU3lTTmZnQWRpLVA4VTd1aGY2a3ZDWHU1TmVOTmNvcnktajcwZk9zcWZTOWNTWnZnbHp3YU5IUVR0LXRIdW1KSndsWHlrNzhNZ3NVMHZB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
