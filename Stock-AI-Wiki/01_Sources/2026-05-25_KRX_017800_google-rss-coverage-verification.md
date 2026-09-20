---
id: verification-2026-05-25-KRX-017800-google-rss-coverage
type: verification
title: KRX 017800 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "017800"
company: 현대엘리베이터
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
    - code=017800
    - name=현대엘리베이터
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017800
    - 현대엘리베이터
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

# KRX 017800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_017800_google-rss-coverage-source]]

## Facts Checked
- `code=017800`
- `name=현대엘리베이터`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대엘리베이터, 스페이스X 투자로 2500억 회수 기대… 배당 대폭 확대할 듯 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-22T10:07:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihwFBVV95cUxPem1yQUJYbnh3eHlkU0U2STdKamhTRTg4cFJLYV9jTHk1WmhsMkEtTGFfbGtVNW5pRlltR1hVb0UyelV1ZEVSVHlUbnVSRmlIR3h4SlRJVGtuMjBnQUExMElrWVFoU2ZHRjdmMFJtRFBfLThmMDJhZ1E4dGtiUkFNdkR2aWJDM0XSAZsBQVVfeXFMTjNIMUdpWElBaG1UbDEyTTlkU2ZETzRIY1FKVFNTcEdyU01EWDh1dkE1WF9JM184ZlMxZGZranlXLTBaMXBFcUZSbG1uTmI5NFczNkRzbkFvVkMtLUhKS3daZFhOa0d3SXFiajdheG9HOVkyX1VrdUp0eWlHT1NEZXRndEtqWDd2YmFtanpyclVUaFIwbGVPRUpfREE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_017800_현대엘리베이터]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `현대엘리베이터, 스페이스X 투자로 2500억 회수 기대… 배당 대폭 확대할 듯 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-22T10:07:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxPem1yQUJYbnh3eHlkU0U2STdKamhTRTg4cFJLYV9jTHk1WmhsMkEtTGFfbGtVNW5pRlltR1hVb0UyelV1ZEVSVHlUbnVSRmlIR3h4SlRJVGtuMjBnQUExMElrWVFoU2ZHRjdmMFJtRFBfLThmMDJhZ1E4dGtiUkFNdkR2aWJDM0XSAZsBQVVfeXFMTjNIMUdpWElBaG1UbDEyTTlkU2ZETzRIY1FKVFNTcEdyU01EWDh1dkE1WF9JM184ZlMxZGZranlXLTBaMXBFcUZSbG1uTmI5NFczNkRzbkFvVkMtLUhKS3daZFhOa0d3SXFiajdheG9HOVkyX1VrdUp0eWlHT1NEZXRndEtqWDd2YmFtanpyclVUaFIwbGVPRUpfREE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
