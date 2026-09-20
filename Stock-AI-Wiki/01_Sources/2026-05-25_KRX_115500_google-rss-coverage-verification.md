---
id: verification-2026-05-25-KRX-115500-google-rss-coverage
type: verification
title: KRX 115500 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "115500"
company: 케이씨에스
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
    - code=115500
    - name=케이씨에스
    - naver_article_count=8
    - google_rss_article_count=6
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 115500
    - 케이씨에스
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

# KRX 115500 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_115500_google-rss-coverage-source]]

## Facts Checked
- `code=115500`
- `name=케이씨에스`
- `naver_article_count=8`
- `google_rss_article_count=6`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'美 3조 투자 훈풍' 양자컴퓨팅주 급등…케이씨에스 상한가 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-05-22T09:28:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE52bjB6MjBiUnUxQ0ZIMVVQbm1PSzZ4NURUeDdCUnRsdHpCZHA4d0E1NFNIVVZiNnRLLXBiUGxSQ3kzaFhzazhKYVllVGhacGFSSEFkc05BTnNfZFU2cjdqZ9IBeEFVX3lxTFB6VGdHT0YwSm1aOTV6bTJfUW9JT2QwOXBOQzgyd0VkLWhxb0trODZJMzZzQ3dFNEQ5REd3UklZbU9hUE5rS3F6REdhNVhSa0M1WnQzeVIyM1U3NWtYajJEWXp4Yzk1NnVwdzkwZXBtWFMxX1JsWnZfNg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_115500_케이씨에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `'美 3조 투자 훈풍' 양자컴퓨팅주 급등…케이씨에스 상한가 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-05-22T09:28:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE52bjB6MjBiUnUxQ0ZIMVVQbm1PSzZ4NURUeDdCUnRsdHpCZHA4d0E1NFNIVVZiNnRLLXBiUGxSQ3kzaFhzazhKYVllVGhacGFSSEFkc05BTnNfZFU2cjdqZ9IBeEFVX3lxTFB6VGdHT0YwSm1aOTV6bTJfUW9JT2QwOXBOQzgyd0VkLWhxb0trODZJMzZzQ3dFNEQ5REd3UklZbU9hUE5rS3F6REdhNVhSa0M1WnQzeVIyM1U3NWtYajJEWXp4Yzk1NnVwdzkwZXBtWFMxX1JsWnZfNg?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
