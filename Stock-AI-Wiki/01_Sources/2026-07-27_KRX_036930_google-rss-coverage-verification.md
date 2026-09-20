---
id: verification-2026-07-27-KRX-036930-google-rss-coverage
type: verification
title: KRX 036930 Google RSS Coverage Verification
created: 2026-07-27
updated: 2026-07-27
status: verification
stage: 1

market: KRX
ticker: "036930"
company: 주성엔지니어링
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=036930
    - name=주성엔지니어링
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 036930
    - 주성엔지니어링
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

# KRX 036930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-27_KRX_036930_google-rss-coverage-source]]

## Facts Checked
- `code=036930`
- `name=주성엔지니어링`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, 주성엔지니어링-반도체 장비 테마 상승세에 7.8% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-07-27T09:47:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBBalRsekZURzNaaUdjY1Z5V3lBcXlMS3RfcFVmSF9IdnVJTDNoaGliUU04SEk2c3ZCN0FSSXI5RFVackhkM0Q5TGo3WFMyaUE5a1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:13+09:00`
- Company: [[KRX_036930_주성엔지니어링]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `[고래사냥] '한화솔루션·주성엔지니어링·대한항공! 내일장 고래 종목은?! - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-27T21:38:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBFTlFSZmtzaHo2QnBHZ0hhWXFnU1lMQUpPSHdUWGsxaEJTSUJabm1lQ0lidmtNY0JPZDJJcTNLR2tRaTM0eVpzSjF5dHJKS3BiYnd2MFFPVFQtQWlvOG9PSHBGckNGS3pP0gFuQVVfeXFMT0Zzdk9KdHhnVzRrSktEdnNzOUJJRzMyUDlLdU1XYzBXZDg4bFY2aGNvSXJtMkRsYzB0bmVVVmhNTURXMzJjZ2lZbFRoZlVHcjhEX3JOb3VMZ3cyRXlMU2dOWU5HVlJVUGI5a3ppWnc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
