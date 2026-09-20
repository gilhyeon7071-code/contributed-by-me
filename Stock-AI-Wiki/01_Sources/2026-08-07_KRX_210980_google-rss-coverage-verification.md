---
id: verification-2026-08-07-KRX-210980-google-rss-coverage
type: verification
title: KRX 210980 Google RSS Coverage Verification
created: 2026-08-07
updated: 2026-08-07
status: verification
stage: 1

market: KRX
ticker: "210980"
company: SK디앤디
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=210980
    - name=SK디앤디
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 210980
    - SK디앤디
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

# KRX 210980 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-07_KRX_210980_google-rss-coverage-source]]

## Facts Checked
- `code=210980`
- `name=SK디앤디`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"금감원 유상증자 제동에"…SK디앤디 '상한가' 직행 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-08-06T09:51:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBWVEQwdjAtQkNtTFIwOFB4WmpNcm8tMEF4SmhmLXQ1cnFUblM0b0E1TTdUeEx1QUhSVWNseXhnaTFtR1ZySmFjWVp6ZHdEdDRRWTI0REpsVFk3NHpMUk5OOdIBeEFVX3lxTFBDUHY1SEVDOTRpZTZNRHJfWFJPSThxbk5FSFZjQktpQTlNOUpLT3RsdEVPVTdtTUkyYVRLWUtNTVNzTVowdWtGVW5scnZvRTJZS1ZDNDBTb3RpamthS2U4MGd4eWtwdWtkMFhjbXlHZmwyYjNyWWI1MQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-07T09:09:24+09:00`
- Company: [[KRX_210980_SK디앤디]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-07.json`
- Latest observation title: `"금감원 유상증자 제동에"…SK디앤디 '상한가' 직행 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-08-06T09:51:14+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBWVEQwdjAtQkNtTFIwOFB4WmpNcm8tMEF4SmhmLXQ1cnFUblM0b0E1TTdUeEx1QUhSVWNseXhnaTFtR1ZySmFjWVp6ZHdEdDRRWTI0REpsVFk3NHpMUk5OOdIBeEFVX3lxTFBDUHY1SEVDOTRpZTZNRHJfWFJPSThxbk5FSFZjQktpQTlNOUpLT3RsdEVPVTdtTUkyYVRLWUtNTVNzTVowdWtGVW5scnZvRTJZS1ZDNDBTb3RpamthS2U4MGd4eWtwdWtkMFhjbXlHZmwyYjNyWWI1MQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
