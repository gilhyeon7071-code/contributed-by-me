---
id: verification-2026-07-29-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-07-29
updated: 2026-07-29
status: verification
stage: 1

market: KRX
ticker: "012330"
company: 현대모비스
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
    - code=012330
    - name=현대모비스
    - naver_article_count=1
    - google_rss_article_count=11
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 012330
    - 현대모비스
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

# KRX 012330 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-29_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=1`
- `google_rss_article_count=11`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대모비스, 부품사 밸류에이션 하락…목표가 75만원으로 하향-LS - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-27T08:58:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE1ic2RPY0ZPQ01ReXpYTzFJbGFrRUpmTFYwb1VMUDhRa0g3VkZ4ZjNKQXhMT3d6YlQ3MkhDWG1fQjN5cnRnbFkwQUM0RFg0VW53c0V0UnFLWFJfdVN0NzNGdHg0ZHhiN08w0gFuQVVfeXFMT1Aza1dLUUpkVURtcVlybjBUWng3VWk5cDlxN3lYMDRFLXRkQUFac0djZFhJenNJM1MtbDBYUFhFZFlacXEyblZPNUJlNDYzWGNwdjJVajgyUW9XY0gtRG5vZHlhbWVVOEVDWXZoNHc?oc=5`

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
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `현대모비스·현대차, ‘아틀라스 양산’ 공장 실사 앞두고 강세 - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-07-29T09:52:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE4yTXBTUV9kamoyOVhhckRTNGlmZFJZTkZwQzhzQzJjTDBuelJ0bFl3bkNXWEV0MFEtWjFpc0otTm1IMWl0cHdsVURLU3AzcS1qYkE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
