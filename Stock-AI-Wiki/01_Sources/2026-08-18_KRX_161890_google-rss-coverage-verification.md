---
id: verification-2026-08-18-KRX-161890-google-rss-coverage
type: verification
title: KRX 161890 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "161890"
company: 한국콜마
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
    - code=161890
    - name=한국콜마
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 161890
    - 한국콜마
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

# KRX 161890 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_161890_google-rss-coverage-source]]

## Facts Checked
- `code=161890`
- `name=한국콜마`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[창간 27주년 특집] 인디 뷰티 호황 ‘한국콜마’ 웃는다… ‘북미 법인’ 적자 탈출 - 이코노미톡뉴스`
- Source: `이코노미톡뉴스`
- Published at: `2026-08-18T07:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE80R3BKMVY4X21tdUtadGVYekVtbjdNVjc3VEdiNG9HYlRVeTVOUUZlSmtXOHJqelA3SFhndjRtZllSRjkzaHVaTWktSmVOMDB0QlJDdmZhNW51ZlpmaVB3WWNLR1FzSmJyWlM5bQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-18T13:11:52+09:00`
- Company: [[KRX_161890_한국콜마]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `[창간 27주년 특집] 인디 뷰티 호황 ‘한국콜마’ 웃는다… ‘북미 법인’ 적자 탈출 - 이코노미톡뉴스`
- Latest observation source: `이코노미톡뉴스`
- Latest observation published_at: `2026-08-18T07:30:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE80R3BKMVY4X21tdUtadGVYekVtbjdNVjc3VEdiNG9HYlRVeTVOUUZlSmtXOHJqelA3SFhndjRtZllSRjkzaHVaTWktSmVOMDB0QlJDdmZhNW51ZlpmaVB3WWNLR1FzSmJyWlM5bQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
