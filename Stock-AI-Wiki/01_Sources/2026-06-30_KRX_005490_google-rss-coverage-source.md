---
id: source-2026-06-30-KRX-005490-google-rss-coverage
type: source
title: KRX 005490 Google RSS Coverage Source
created: 2026-06-30
updated: 2026-06-30
status: raw
stage: 0

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 005490.
  key_facts:
    - code=005490
    - name=POSCO홀딩스
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005490
    - POSCO홀딩스
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

# KRX 005490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 005490.

## RSS Item Metadata
- Title: `다들 종전 축포 와중에…POSCO홀딩스 목표주가 54만 → 48만원 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-15T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5QX0M4NGpmeTUyb0ZEVVBOZHJTbG5kNWxCTHQwSnRBNUgwOG0yS3U5cVppV1AwZ3F2UXgta0pkbFR5Uk5HMDYwMzlpQmRIU2c?oc=5`

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
- Updated at: `2026-08-21T19:25:36+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `POSCO홀딩스 주가, 6월 29일 333,000원 9.18% 상승 마감 - TopStarNews`
- Latest observation source: `TopStarNews`
- Latest observation published_at: `2026-06-29T15:52:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE1DM1pCMGQ5bE5qMjNZMkF0Q1M5U0JMOVZUc1hwaUtTWUozeVNsOEhHczh2TlFWQm1fcFp1VVFzZTJmaS1RSTFCeU5fenpBejNiX3dhWDZHU1FNdVQ3dkZCWDZQOTFBTGgwVTZlV3VrbENTZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
