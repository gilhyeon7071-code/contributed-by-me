---
id: verification-2026-07-29-KRX-240810-google-rss-coverage
type: verification
title: KRX 240810 Google RSS Coverage Verification
created: 2026-07-29
updated: 2026-07-29
status: verification
stage: 1

market: KRX
ticker: "240810"
company: 원익IPS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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

# KRX 240810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-29_KRX_240810_google-rss-coverage-source]]

## Facts Checked
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `코스닥 '털썩'...에코프로·주성엔지니어링·원익IPS 등 '와르르' - 초이스경제`
- Source: `초이스경제`
- Published at: `2026-07-28T16:29:52+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE44b3l3YmZ2SXBSVDk0NjhobWl0TnVDSGVyNlF0ck5zUE9Zalg0a3RIUUlEVVBEaXo2RjJkc3A5QU9mZXUzdmhOTzdsaDkwZF9jT2Y3VDBjTlJGME1qUVJUS0EzS3otclViN29PN1FTZ9IBc0FVX3lxTE5qZEgwNXRPN01PR3dCa09iUHJzYjhrZVpKQTNRS0RPd2tSaGdhZFZOWk9tQllTclNzZHJMaFFjSVFPbkRvN0VwYVFqUWJLWkd3aEs0a3BOTzJzLVBNbjhWNjRiOUVKQmtxc29NbGpLZTZJSEE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:53+09:00`
- Company: [[KRX_240810_원익IPS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `[코스닥 외국인] 반도체로 쏠린 자금…주성엔지니어링·원익IPS '러브콜' - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-07-29T15:53:44+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE55TUtOOEl2UGZ3WUhKYWo3V05SUkMyTDVYLU5ReU1DbFRVOEc1ZEVuZUxCY09ScjNzY1JubFlIenRBQ0t4N2RqWU9nQUFBYlQwWDN1NjJ4cmNRLW8zWHlGSFRmRkFVeVlCTzVPX0xjSzVpaDTSAXdBVV95cUxQOGhpVmdIUHhhTFpyOTFxazJmRVJ3cEt2bkF4a25udGZfNVhRd3JnQm1YeXgwQ3ltY2xTZGpLTkVqUlROTmtOQzFhNHF4aXl5d3JJZDNBM09MM0I2bHJ2dmhFQkJXTVpZS04yVW5nZW5iM05DcEdSVQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
