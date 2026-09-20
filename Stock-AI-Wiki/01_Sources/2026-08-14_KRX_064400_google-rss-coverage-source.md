---
id: source-2026-08-14-KRX-064400-google-rss-coverage
type: source
title: KRX 064400 Google RSS Coverage Source
created: 2026-08-14
updated: 2026-08-14
status: raw
stage: 0

market: KRX
ticker: "064400"
company: LG씨엔에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-14

analysis:
  summary: Local coverage report row shows news coverage for KRX 064400.
  key_facts:
    - code=064400
    - name=LG씨엔에스
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 064400
    - LG씨엔에스
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

# KRX 064400 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=064400`
- `name=LG씨엔에스`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 064400.

## RSS Item Metadata
- Title: `[장중수급포착] LG씨엔에스, 기관 8일 연속 순매수행진... 주가 +1.68% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-13T10:20:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE82b0E5bHNfRFExclc5TlVncEtBUUN4dUM0c1JPQmdzWVpuTVo1bV9SbnQyaHVEMWlFZEt2Z003ZmxacnI5RzQwR2lBcnRYMzZnQ3BfUFdoSnFIVWxG?oc=5`

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
- Updated at: `2026-08-21T19:36:10+09:00`
- Company: [[KRX_064400_LG씨엔에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-14.json`
- Latest observation title: `[특징주] LG, 엔비디아와 휴머노이드 로봇 협력 소식에 상승(종합) - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-08-14T15:59:26+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBIOGttYWQ3T2JaSHU3V2V4a1RWVV9fNEFxNjJNc1ZWQ0RHZTh2cVNyX3NQVlpGYzl4ZFNrdVFOdFJLZXZScEVOZUlPbjdRVW55Nk9uV3dteXJtT1JqX0pPc9IBYEFVX3lxTFBIOGttYWQ3T2JaSHU3V2V4a1RWVV9fNEFxNjJNc1ZWQ0RHZTh2cVNyX3NQVlpGYzl4ZFNrdVFOdFJLZXZScEVOZUlPbjdRVW55Nk9uV3dteXJtT1JqX0pPcw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
