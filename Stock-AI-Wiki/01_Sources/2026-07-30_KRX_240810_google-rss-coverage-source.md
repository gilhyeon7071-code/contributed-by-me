---
id: source-2026-07-30-KRX-240810-google-rss-coverage
type: source
title: KRX 240810 Google RSS Coverage Source
created: 2026-07-30
updated: 2026-07-30
status: raw
stage: 0

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
  summary: Local coverage report row shows news coverage for KRX 240810.
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
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 240810 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 240810.

## RSS Item Metadata
- Title: `하나증권 "AI 투자 확대에 반도체장비주 부각, 관련주 원익IPS 주성엔지니어링" - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-07-30T09:17:39+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9DMXhJQmQzbzJqOEhEblBocmFuT0dXX1l5YVVQRTZ1SG9qcDBtcGpqR3ZZUmotLXVobTVJdFZibzZkU1lsaGRSNmVjalpZc1B3YzFjY1o3V3M4N2pqaDIwa2xqSkdQZ3hMTmt5WDZGZ3B0Y1E?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

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
