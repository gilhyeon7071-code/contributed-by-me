---
id: verification-2026-05-29-KRX-086900-google-rss-coverage
type: verification
title: KRX 086900 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "086900"
company: 메디톡스
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
    - code=086900
    - name=메디톡스
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086900
    - 메디톡스
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

# KRX 086900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_086900_google-rss-coverage-source]]

## Facts Checked
- `code=086900`
- `name=메디톡스`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[퍼스트클래스] 메디톡스 ‘이노톡스’, 복합시술 확산으로 재조명 - fetv.co.kr`
- Source: `fetv.co.kr`
- Published at: `2026-05-29T07:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBiX3lKOER4elpYc2xBNDlvc3hxUnlBX3liZjMzdlIteFZtTUpibXQxMjlPbHBmSS0wNUYxeEpWLWhEeWFoczRCOURtSzRpb0FKWmtaZllLNm9WT1IzejN3T0E3LXMzQW1S?oc=5`

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
- Company: [[KRX_086900_메디톡스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `[퍼스트클래스] 메디톡스 ‘이노톡스’, 복합시술 확산으로 재조명 - fetv.co.kr`
- Latest observation source: `fetv.co.kr`
- Latest observation published_at: `2026-05-29T07:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBiX3lKOER4elpYc2xBNDlvc3hxUnlBX3liZjMzdlIteFZtTUpibXQxMjlPbHBmSS0wNUYxeEpWLWhEeWFoczRCOURtSzRpb0FKWmtaZllLNm9WT1IzejN3T0E3LXMzQW1S?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
