---
id: verification-2026-05-25-KRX-382800-google-rss-coverage
type: verification
title: KRX 382800 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "382800"
company: 지앤비에스 에코
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=382800
    - name=지앤비에스 에코
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 382800
    - 지앤비에스 에코
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

# KRX 382800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_382800_google-rss-coverage-source]]

## Facts Checked
- `code=382800`
- `name=지앤비에스 에코`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[이슈] 지앤비에스에코, 반도체·태양광 장비 앞세워 해외 매출 확대 - 팍스경제TV`
- Source: `팍스경제TV`
- Published at: `2026-05-04T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFAxSGxNTVZyOHUtQmVvQVpQSE80eHQ0UVUtb2xaalk1UjRhNzYtbVFMRUhSWGlJMmRKaVE5QVgwc1M0bVBBRDRuNXdMdU1EOVNUbWxWVnpBbU9ESDhSRC1zTml2WU5vXzk3?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_382800_지앤비에스-에코]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `[이슈] 지앤비에스에코, 반도체·태양광 장비 앞세워 해외 매출 확대 - 팍스경제TV`
- Latest observation source: `팍스경제TV`
- Latest observation published_at: `2026-05-04T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFAxSGxNTVZyOHUtQmVvQVpQSE80eHQ0UVUtb2xaalk1UjRhNzYtbVFMRUhSWGlJMmRKaVE5QVgwc1M0bVBBRDRuNXdMdU1EOVNUbWxWVnpBbU9ESDhSRC1zTml2WU5vXzk3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_exports_수출]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
