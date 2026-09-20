---
id: verification-2026-05-26-KRX-040350-google-rss-coverage
type: verification
title: KRX 040350 Google RSS Coverage Verification
created: 2026-05-26
updated: 2026-05-26
status: verification
stage: 1

market: KRX
ticker: "040350"
company: 크레오에스지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=040350
    - name=크레오에스지
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 040350
    - 크레오에스지
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

# KRX 040350 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-26_KRX_040350_google-rss-coverage-source]]

## Facts Checked
- `code=040350`
- `name=크레오에스지`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `지엔코 최대주주 크레오에스지, 지엔코 주식등의 수 153만140주 증가…총 지분율 71.45% - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-05-22T16:02:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9yY3dBRTBndUYwUnZqWnNueVFtNGJLTnJ1MUxJYVpTWWY4UFhXY250ZUVGc05WR2hhVm9kcTlueUpYY2x4VldTeWlISXV6T1RMNm51X3hMNmxYbXlVZXRkNDR6RV93YVNqV0t2cGN1SnVOVEU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-26T08:05:05+09:00`
- Company: [[KRX_040350_크레오에스지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `지엔코 최대주주 크레오에스지, 지엔코 주식등의 수 153만140주 증가…총 지분율 71.45% - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-05-22T16:02:05+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9yY3dBRTBndUYwUnZqWnNueVFtNGJLTnJ1MUxJYVpTWWY4UFhXY250ZUVGc05WR2hhVm9kcTlueUpYY2x4VldTeWlISXV6T1RMNm51X3hMNmxYbXlVZXRkNDR6RV93YVNqV0t2cGN1SnVOVEU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
