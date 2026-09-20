---
id: verification-2026-07-30-KRX-240810-google-rss-coverage
type: verification
title: KRX 240810 Google RSS Coverage Verification
created: 2026-07-30
updated: 2026-07-30
status: verification
stage: 1

market: KRX
ticker: "240810"
company: 원익IPS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=240810
    - name=원익IPS
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 240810
    - 원익IPS
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

# KRX 240810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-30_KRX_240810_google-rss-coverage-source]]

## Facts Checked
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `하나증권 "AI 투자 확대에 반도체장비주 부각, 관련주 원익IPS 주성엔지니어링" - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-07-30T09:17:39+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9DMXhJQmQzbzJqOEhEblBocmFuT0dXX1l5YVVQRTZ1SG9qcDBtcGpqR3ZZUmotLXVobTVJdFZibzZkU1lsaGRSNmVjalpZc1B3YzFjY1o3V3M4N2pqaDIwa2xqSkdQZ3hMTmt5WDZGZ3B0Y1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:12+09:00`
- Company: [[KRX_240810_원익IPS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-30.json`
- Latest observation title: `하나증권 "AI 투자 확대에 반도체장비주 부각, 관련주 원익IPS 주성엔지니어링" - 비즈니스포스트`
- Latest observation source: `비즈니스포스트`
- Latest observation published_at: `2026-07-30T09:17:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9DMXhJQmQzbzJqOEhEblBocmFuT0dXX1l5YVVQRTZ1SG9qcDBtcGpqR3ZZUmotLXVobTVJdFZibzZkU1lsaGRSNmVjalpZc1B3YzFjY1o3V3M4N2pqaDIwa2xqSkdQZ3hMTmt5WDZGZ3B0Y1E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
