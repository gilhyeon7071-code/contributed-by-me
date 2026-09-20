---
id: verification-2026-05-28-KRX-476060-google-rss-coverage
type: verification
title: KRX 476060 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "476060"
company: 온코닉테라퓨틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=476060
    - name=온코닉테라퓨틱스
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 476060
    - 온코닉테라퓨틱스
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

# KRX 476060 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_476060_google-rss-coverage-source]]

## Facts Checked
- `code=476060`
- `name=온코닉테라퓨틱스`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `온코닉테라퓨틱스 네수파립, 췌장암서 완전관해·3년 생존 확인…ASCO 첫 공개 - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-05-22T09:17:25+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE5GZnlaR19TOXdXazVwSF9CY1pUTE5PaDE2WWhQTVpndmVCNGt4SWFnb2JodTVsblJ4Mk1XQnMwYm5qV2hsbGIwNFFZUHBpWW9ocEc5LVhjYWVRRm80SGpPQ1ZVZ0FTV3ltd2JnVmNR?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-28T14:05:13+09:00`
- Company: [[KRX_476060_온코닉테라퓨틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `온코닉테라퓨틱스, 기존 PARP 저해제 내성 극복 가능성 제시하며 국제 학술지 게재 - 메디컬투데이`
- Latest observation source: `메디컬투데이`
- Latest observation published_at: `2026-05-12T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE02enh3X3ljSUFrblJtQmJ3QlFMbGZFbENMYmJfTDZuMWlYVndOTTIxeTZKSE5pdVZpRDY2NGpCcGgwNXpYalpkUEl5WGNCUkd1Z0RuV2FCenUxQ0Rr0gFsQVVfeXFMT0RENmFvOWFxMmtGS181RkwxUlJ1Y2duNVNjMUkzSzV1OTlNTWxTZlpyZDZCa0Z3MU1yZlNBdVR3MUhyaDhILXcwTDNZdDJQVXhQcGhnMC1hZ1JTd0VFaE5peU5vWHRjeGZfOVFB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
