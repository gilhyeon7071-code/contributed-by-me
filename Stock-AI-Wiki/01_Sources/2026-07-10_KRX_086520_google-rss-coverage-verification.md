---
id: verification-2026-07-10-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-07-10
updated: 2026-07-10
status: verification
stage: 1

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-10_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에코프로비엠, 1.2조 유증 앞두고 주가 하락…핵심 과제로 떠오른 ‘수익성 비전’ - 아시아투데이`
- Source: `아시아투데이`
- Published at: `2026-07-09T18:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFB0enlQQ005SUhTRFpoOWo0MkNGTnNHaUM3VEhCc3dxc2NXb0QxZWl6dkxNbnlkeFpnenhYRHpYazg5dUNjSFlIOEpSWEVHN3dvMjk4alV4Y2U5a2lVMklXTUkwOGI1c1NfT3BHQWlB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:24+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-10.json`
- Latest observation title: `에코프로비엠, 1.2조 유증 앞두고 주가 하락…핵심 과제로 떠오른 ‘수익성 비전’ - 아시아투데이`
- Latest observation source: `아시아투데이`
- Latest observation published_at: `2026-07-09T18:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTFB0enlQQ005SUhTRFpoOWo0MkNGTnNHaUM3VEhCc3dxc2NXb0QxZWl6dkxNbnlkeFpnenhYRHpYazg5dUNjSFlIOEpSWEVHN3dvMjk4alV4Y2U5a2lVMklXTUkwOGI1c1NfT3BHQWlB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
