---
id: verification-2026-06-15-KRX-003570-google-rss-coverage
type: verification
title: KRX 003570 Google RSS Coverage Verification
created: 2026-06-15
updated: 2026-06-15
status: verification
stage: 1

market: KRX
ticker: "003570"
company: SNT다이내믹스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=003570
    - name=SNT다이내믹스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003570
    - SNT다이내믹스
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

# KRX 003570 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-15_KRX_003570_google-rss-coverage-source]]

## Facts Checked
- `code=003570`
- `name=SNT다이내믹스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SNT다이내믹스, 경남 품질분임조 ‘최우수상’ - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-11T09:21:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTE9TZ3RXNXhlaUF1aGI5aU1xd09Jb0tibDk5cU5KTHRLVEFYdm5RRFJlS0ZhX0xUbFBpSW9UeG9OUmlzdi1CMkRBZmdIWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:21:08+09:00`
- Company: [[KRX_003570_SNT다이내믹스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-15.json`
- Latest observation title: `SNT다이내믹스 연구논문·특허출원 확대 R&D 경쟁력 강화 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-15T13:06:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTFBHMGI5eklQSHJ6S1lJbUt6ekxEMUlObVVubTVESkM0Vkw2b0ZsQVM1anZreXphSkFGWVl5ZzBScXlxLW9CekI4X3dZVGZ1VXd1QUd5WVI0T0NFZjR1NGtsUXJVU1NXM2ZrcVHSAW9BVV95cUxPM21Gd3dkb2F1VXhuS0p5TUVmSXp3ZnZDbzBQeS1senV4Nk5zTW9zamJDNnczY3c2dWxFT2Y1a1l4SWhpcVJJcGQ0Vy1IN0hPeEk3LW93MHg4bWx4R29HSE9sYU0ybDhKMDV0YzI0UzQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
