---
id: verification-2026-05-27-KRX-103140-google-rss-coverage
type: verification
title: KRX 103140 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "103140"
company: 풍산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=103140
    - name=풍산
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 103140
    - 풍산
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

# KRX 103140 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_103140_google-rss-coverage-source]]

## Facts Checked
- `code=103140`
- `name=풍산`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `하남풍산초 학생들 조선시대 선비로 변신 인의예지신(仁義禮智信 )체험 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-27T15:40:12+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1mWExpdXJXSS1tbXkwVkp3X3ROMWRoRGtyOEJMOHVNOXdQT1djeDQ4TzlnWGxMQnkwMy1sTFhGSjkyWW00aWpzM2ptM2lCaGM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:44+09:00`
- Company: [[KRX_103140_풍산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `하남풍산초 학생들 조선시대 선비로 변신 인의예지신(仁義禮智信 )체험 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-27T15:40:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1mWExpdXJXSS1tbXkwVkp3X3ROMWRoRGtyOEJMOHVNOXdQT1djeDQ4TzlnWGxMQnkwMy1sTFhGSjkyWW00aWpzM2ptM2lCaGM?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
