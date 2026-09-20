---
id: source-2026-07-07-KRX-121440-google-rss-coverage
type: source
title: KRX 121440 Google RSS Coverage Source
created: 2026-07-07
updated: 2026-07-07
status: raw
stage: 0

market: KRX
ticker: "121440"
company: 골프존홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 121440.
  key_facts:
    - code=121440
    - name=골프존홀딩스
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 121440
    - 골프존홀딩스
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

# KRX 121440 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=121440`
- `name=골프존홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 121440.

## RSS Item Metadata
- Title: `골프존홀딩스 공개매수에 거래 집중…기관 매수세 유입 - cctoday.co.kr`
- Source: `cctoday.co.kr`
- Published at: `2026-07-06T15:24:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBsOEJkMFd1M2dUYjlwM1pzel9ZOGc4THd1MFdKLUV3WEdsVjBPMlNJTm5ZS3NZeWQwdjFDNlp4OC1FemVaOUpqRXFVcWdFSGt1OF9mLTFBYlVKS3ZheDhJTFRiTDd4aG9KNE8wOXRR?oc=5`

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
- Updated at: `2026-08-21T19:27:28+09:00`
- Company: [[KRX_121440_골프존홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `골프존홀딩스 공개매수 순항…목표 물량 54% 거래 - 딜사이트`
- Latest observation source: `딜사이트`
- Latest observation published_at: `2026-07-06T10:05:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE85dkQ1My03VUllR1hlcmdWZm1RYmdKU1JsVjFuWTJfY0pBRWxnTGtwdEk3MU9KUTZjeEZ5OEdnVS1KQlBMN0FYNGR3NXJOdE0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
