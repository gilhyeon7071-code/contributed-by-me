---
id: verification-2026-06-29-KRX-005490-google-rss-coverage
type: verification
title: KRX 005490 Google RSS Coverage Verification
created: 2026-06-29
updated: 2026-06-29
status: verification
stage: 1

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 005490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-29_KRX_005490_google-rss-coverage-source]]

## Facts Checked
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `다들 종전 축포 와중에…POSCO홀딩스 목표주가 54만 → 48만원 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-15T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5QX0M4NGpmeTUyb0ZEVVBOZHJTbG5kNWxCTHQwSnRBNUgwOG0yS3U5cVppV1AwZ3F2UXgta0pkbFR5Uk5HMDYwMzlpQmRIU2c?oc=5`

## Article Body Archive Checked
- Title: `다들 종전 축포 와중에…POSCO홀딩스 목표주가 54만 → 48만원 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-15T16:00:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5QX0M4NGpmeTUyb0ZEVVBOZHJTbG5kNWxCTHQwSnRBNUgwOG0yS3U5cVppV1AwZ3F2UXgta0pkbFR5Uk5HMDYwMzlpQmRIU2c?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5QX0M4NGpmeTUyb0ZEVVBOZHJTbG5kNWxCTHQwSnRBNUgwOG0yS3U5cVppV1AwZ3F2UXgta0pkbFR5Uk5HMDYwMzlpQmRIU2c?oc=5`
- Body excerpt: 다들 종전 축포 와중에…POSCO홀딩스 목표주가 54만 → 48만원 v.daum.net

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:16+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-29.json`
- Latest observation title: `다들 종전 축포 와중에…POSCO홀딩스 목표주가 54만 → 48만원 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-15T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5QX0M4NGpmeTUyb0ZEVVBOZHJTbG5kNWxCTHQwSnRBNUgwOG0yS3U5cVppV1AwZ3F2UXgta0pkbFR5Uk5HMDYwMzlpQmRIU2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
