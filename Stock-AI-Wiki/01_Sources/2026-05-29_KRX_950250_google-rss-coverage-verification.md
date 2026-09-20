---
id: verification-2026-05-29-KRX-950250-google-rss-coverage
type: verification
title: KRX 950250 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "950250"
company: 테라뷰
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=950250
    - name=테라뷰
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 950250
    - 테라뷰
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

# KRX 950250 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_950250_google-rss-coverage-source]]

## Facts Checked
- `code=950250`
- `name=테라뷰`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `테라뷰, +4.32% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-28T14:35:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxPMTBMNjRod3ZWN0Q0cHdZa2d2bFdxbzBaeDJsVWtReGpzSy1BLWhjYXJpZU1HM3ZocElCMFZPc0MtY0FQQjA3UG1CVmdMbHVHekVQR1k3ZHBzS21PbC1GN1NnZ2hQaW5Gb0tMbWlHUjg2aDJHb2stMWdfLXVoc1F6TG0xWGFBQ1Fsbi1lOW9wbEFHMEV0Ymdv0gGXAUFVX3lxTE8xMEw2NGh3dlY3RDRwd1lrZ3ZsV3FvMFp4MmxVa1F4anNLLUEtaGNhcmllTUczdmhwSUIwVk9zQy1jQVBCMDdQbUJWZ0xsdUd6RVBHWTdkcHNLbU9sLUY3U2dnaFBpbkZvS0xtaUdSODZoMkdvay0xZ18tdWhzUXpMbTFYYUFDUWxuLWU5b3BsQUcwRXRiZ28?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-29T10:05:04+09:00`
- Company: [[KRX_950250_테라뷰]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `테라뷰, +4.32% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-28T14:35:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxPMTBMNjRod3ZWN0Q0cHdZa2d2bFdxbzBaeDJsVWtReGpzSy1BLWhjYXJpZU1HM3ZocElCMFZPc0MtY0FQQjA3UG1CVmdMbHVHekVQR1k3ZHBzS21PbC1GN1NnZ2hQaW5Gb0tMbWlHUjg2aDJHb2stMWdfLXVoc1F6TG0xWGFBQ1Fsbi1lOW9wbEFHMEV0Ymdv0gGXAUFVX3lxTE8xMEw2NGh3dlY3RDRwd1lrZ3ZsV3FvMFp4MmxVa1F4anNLLUEtaGNhcmllTUczdmhwSUIwVk9zQy1jQVBCMDdQbUJWZ0xsdUd6RVBHWTdkcHNLbU9sLUY3U2dnaFBpbkZvS0xtaUdSODZoMkdvay0xZ18tdWhzUXpMbTFYYUFDUWxuLWU5b3BsQUcwRXRiZ28?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
