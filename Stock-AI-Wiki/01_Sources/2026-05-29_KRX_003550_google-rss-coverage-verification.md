---
id: verification-2026-05-29-KRX-003550-google-rss-coverage
type: verification
title: KRX 003550 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "003550"
company: LG
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
    - code=003550
    - name=LG
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003550
    - LG
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

# KRX 003550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_003550_google-rss-coverage-source]]

## Facts Checked
- `code=003550`
- `name=LG`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG전자 사무실서 임직원 2명 흉기로 찌른 협력업체 직원 체포 [지금뉴스] - KBS 뉴스`
- Source: `KBS 뉴스`
- Published at: `2026-05-27T14:03:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE5QdzhlUjhxZWJWOXowbXRMQWdldmlnNEhLTDFla3cyM0t3TUFLQTFlLS1YY3RadEk5UlBOZjlabDB0ZWlmbFprWTBRdllqZlJIVkFfakgxSUhIMjA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:19+09:00`
- Company: [[KRX_003550_LG]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `젠슨 황, 한국 찾아 LG 등 회동···‘제2의 깐부회동’ 되나 - 경향신문`
- Latest observation source: `경향신문`
- Latest observation published_at: `2026-05-28T19:21:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBIVUliYm5zSTlQMDM1QVNhZjB1cUFJSjAwaTdLZ0JJTmlna3hHa2xSajVrYndNbkFpQjdqZFZreXNZUlJDVDg4cXQyeHkzMTZNejhnZFVHT1d6Z9IBX0FVX3lxTE9wUGdGbW9xR1JabnEwZUppRjlTWkZvelFzTmU2Nm51WXY5WXYyOUdoWFM3M29YaWotZFBqRlNMcjRzTElaT1U4eVBzMm5RUVVsNnFOMTlHME5fWVROR19N?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
