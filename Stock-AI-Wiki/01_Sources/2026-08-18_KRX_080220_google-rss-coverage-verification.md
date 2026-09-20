---
id: verification-2026-08-18-KRX-080220-google-rss-coverage
type: verification
title: KRX 080220 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "080220"
company: 제주반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=080220
    - name=제주반도체
    - naver_article_count=2
    - google_rss_article_count=22
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 080220
    - 제주반도체
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

# KRX 080220 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_080220_google-rss-coverage-source]]

## Facts Checked
- `code=080220`
- `name=제주반도체`
- `naver_article_count=2`
- `google_rss_article_count=22`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `제주반도체, 2분기 영업익 1200억원…전년비 2700%↑ - 디일렉`
- Source: `디일렉`
- Published at: `2026-08-14T16:38:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFAyTHVVWlA1QTZWcVMtZ3lyQzAzSkY2NnZGR1RtYS05UjRKMzBRRkI1OEI1clAtU0paNGZxSkw0U1Y4RzB0cmc5a0d3LTVXWUVfVHNNRmwtdUNPZklxTG1meFVVX3FyQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_080220_제주반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `[P전송금지]"범용 D램도 품귀 지속" 반등하는 제주반도체 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-08-18T17:36:52+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9VOE8tbHdZanJRV0gxTWtCN0dXZ1lwUHZMSEdPbHNJS2JGTWZMUXltN0FhWG0xekppSXdzQkxFM3JiZG45bzB3bGdIWFFCZDRpSklvQzJSdWVuZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
