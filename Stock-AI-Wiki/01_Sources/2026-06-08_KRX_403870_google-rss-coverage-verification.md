---
id: verification-2026-06-08-KRX-403870-google-rss-coverage
type: verification
title: KRX 403870 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "403870"
company: HPSP
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=403870
    - name=HPSP
    - naver_article_count=0
    - google_rss_article_count=4
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 403870
    - HPSP
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

# KRX 403870 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_403870_google-rss-coverage-source]]

## Facts Checked
- `code=403870`
- `name=HPSP`
- `naver_article_count=0`
- `google_rss_article_count=4`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `05일 코스닥 시장 공매도 수량 상위 종목. HPSP, 우리기술 등 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-08T08:01:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOeWZLVmpfNWdnRXhmZzJXblVaaG5QUHdyemQ1aEpmaUNGTFNIR3VrVWFwUGJGeWtRcHlXVXZnMzVPNXM1WUIybW1nbW5ha1dodE1GdDM1Uk5rc1dpNlk1M19ZSmszYXZESnd2VTBrSm9VN3lNTmZIN2NKc3dlSllfLTg4SHVtbGx3V0pucFpjNVlHbnhSQ2Rn0gGXAUFVX3lxTE55ZktWal81Z2dFeGZnMlduVVpoblBQd3J6ZDVoSmZpQ0ZMU0hHdWtVYXBQYkZ5a1FweVdVdmczNU81czVZQjJtbWdtbmFrV2h0TUZ0MzVSTmtzV2k2WTUzX1lKazNhdkRKd3ZVMGtKb1U3eU1OZkg3Y0pzd2VKWV8tODhIdW1sbHdXSm5wWmM1WUdueFJDZGc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-08T18:15:43+09:00`
- Company: [[KRX_403870_HPSP]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `05일 코스닥 시장 공매도 수량 상위 종목. HPSP, 우리기술 등 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-08T08:01:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOeWZLVmpfNWdnRXhmZzJXblVaaG5QUHdyemQ1aEpmaUNGTFNIR3VrVWFwUGJGeWtRcHlXVXZnMzVPNXM1WUIybW1nbW5ha1dodE1GdDM1Uk5rc1dpNlk1M19ZSmszYXZESnd2VTBrSm9VN3lNTmZIN2NKc3dlSllfLTg4SHVtbGx3V0pucFpjNVlHbnhSQ2Rn0gGXAUFVX3lxTE55ZktWal81Z2dFeGZnMlduVVpoblBQd3J6ZDVoSmZpQ0ZMU0hHdWtVYXBQYkZ5a1FweVdVdmczNU81czVZQjJtbWdtbmFrV2h0TUZ0MzVSTmtzV2k2WTUzX1lKazNhdkRKd3ZVMGtKb1U3eU1OZkg3Y0pzd2VKWV8tODhIdW1sbHdXSm5wWmM1WUdueFJDZGc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
