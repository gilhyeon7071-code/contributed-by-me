---
id: verification-2026-06-09-KRX-007390-google-rss-coverage
type: verification
title: KRX 007390 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
status: verification
stage: 1

market: KRX
ticker: "007390"
company: 네이처셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-09

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=007390
    - name=네이처셀
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 007390
    - 네이처셀
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

# KRX 007390 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-09_KRX_007390_google-rss-coverage-source]]

## Facts Checked
- `code=007390`
- `name=네이처셀`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `FDA, 조인트스템 한국 3상만으로 BLA 허가 신청 유례 없는 승인 - 글로벌경제신문`
- Source: `글로벌경제신문`
- Published at: `2026-06-09T09:24:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE55SkV4aFdtbmVWWGVGNmFjOGFZaU1vSnYtamh3cVphUm9fZmxBWHhHTTdYT3V2QnJKeXZoa0dXNmRod1BienZKbEdfQTVzLVhuOE1IRGdaNDNnT3EzLXM5cjEyMmN3ZUlUMEkwdNIBcEFVX3lxTE8tTV9vS0hnb2RaR2ZKY3BHb3FSbE1BeGpHNTJ5aDN4UEJNQ2tsUmdBUkxlNU51NFZuX3Q5aWFMakJSS1pIOWw4UUx5VjdJUXYxWWVpektZQ1NZTUZxX003UkpJZVNscDF5dV83WHpwa2Y?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:32+09:00`
- Company: [[KRX_007390_네이처셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `네이처셀 주주대표 “조인트스템 BLA 신청 경로 구체화…한국 3상 기반 FDA 논의 진전” - 약업신문`
- Latest observation source: `약업신문`
- Latest observation published_at: `2026-06-09T13:45:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiekFVX3lxTE90QjZFMXUxYzFFNndTODM3Ykg5TEpBV2JKNF9kUV9tSWROZWhETC1veGdBTVhUbUo4cE5TMFNSMFh0ZFlPZHdyX1AtdTExcWIxWkVzWF9uZ1YyWUpwd2ZyVjBOcTVfbF9Wd3RNOXlGcHpzdEVkZ1JwUmRn?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
