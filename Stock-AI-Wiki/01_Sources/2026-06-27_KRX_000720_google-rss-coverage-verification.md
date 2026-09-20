---
id: verification-2026-06-27-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-27
updated: 2026-06-27
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=2
    - google_rss_article_count=62
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-27_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=2`
- `google_rss_article_count=62`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대건설, 울산 샤힌 프로젝트 현장서 중대사고 발생…근로자 1명 사망 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-26T17:24:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMimAFBVV95cUxQN05yekpBV0VEYVZLWTI1eXBCRzFNbmQxaVN3S29BWWxiUlFHSGNHTnVTbFpDRDQ1MDhCUGotSDFVb01KN3F3c282eGdSU0dNVHliSXFvcUU5QnpPMzlxXzhybVRJdTNvNUxPQ0h6ZFpEWnROWVdMU0ktRjdMOUVKNDhyR2M4cFhLSTlEcEduaF9sSDJwMThaddIBrAFBVV95cUxPMDFYaFhWY2NsSmtqMlZCQ1VYSHo5a1ZfclE2cTRpUEVTNWwwck9pZnZHQ2kzR2JjbnEtcGMxbkx2eDBRUXNQdDFacGgtN0xySkJRdUxjVk15NHJ6YTR0dFNZWUFvWlB4dm1rY2hmLXBhcXZCZDcxd0VqbjlyTnpyb2l2MlZGcW8yWVY5MUFEcDI4ZXUtN0NOSXhIMHJGTmlOYjZqUnlFanQ3VjNo?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:56+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-27.json`
- Latest observation title: `현대건설, 울산 샤힌 프로젝트 현장서 중대사고 발생…근로자 1명 사망 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-26T17:24:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMimAFBVV95cUxQN05yekpBV0VEYVZLWTI1eXBCRzFNbmQxaVN3S29BWWxiUlFHSGNHTnVTbFpDRDQ1MDhCUGotSDFVb01KN3F3c282eGdSU0dNVHliSXFvcUU5QnpPMzlxXzhybVRJdTNvNUxPQ0h6ZFpEWnROWVdMU0ktRjdMOUVKNDhyR2M4cFhLSTlEcEduaF9sSDJwMThaddIBrAFBVV95cUxPMDFYaFhWY2NsSmtqMlZCQ1VYSHo5a1ZfclE2cTRpUEVTNWwwck9pZnZHQ2kzR2JjbnEtcGMxbkx2eDBRUXNQdDFacGgtN0xySkJRdUxjVk15NHJ6YTR0dFNZWUFvWlB4dm1rY2hmLXBhcXZCZDcxd0VqbjlyTnpyb2l2MlZGcW8yWVY5MUFEcDI4ZXUtN0NOSXhIMHJGTmlOYjZqUnlFanQ3VjNo?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
