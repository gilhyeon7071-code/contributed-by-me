---
id: verification-2026-08-31-KRX-126730-google-rss-coverage
type: verification
title: KRX 126730 Google RSS Coverage Verification
created: 2026-08-31
updated: 2026-08-31
status: verification
stage: 1

market: KRX
ticker: "126730"
company: 코칩
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-31

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=126730
    - name=코칩
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 126730
    - 코칩
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 126730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-31_KRX_126730_google-rss-coverage-source]]

## Facts Checked
- `code=126730`
- `name=코칩`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"AI 서버 MLCC 없어서 못 판다"…코칩, 슈퍼커패시터로 데이터센터·로봇·자율주행 공급 타진 - 파이낸셜포스트`
- Source: `파이낸셜포스트`
- Published at: `2026-08-28T13:21:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5nRGRFY0xzaHZoN1pHVXd5dkZKQ0t0UTlDMHBoTFY5OW5nZmFaNTRfZ0dJQjR1SWduQ2c5QWVkVUk1cXdBU2N4MndPWW5veVZnRTBnMTg3eGpPbnc2dng2bHNIXzYzb3RZQnJLSnlGa044Qk1l?oc=5`

## Article Body Archive Checked
- Title: `"AI 서버 MLCC 없어서 못 판다"…코칩, 슈퍼커패시터로 데이터센터·로봇·자율주행 공급 타진 - 파이낸셜포스트`
- Source: `파이낸셜포스트`
- Published at: `2026-08-28T13:21:18+09:00`
- URL: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5nRGRFY0xzaHZoN1pHVXd5dkZKQ0t0UTlDMHBoTFY5OW5nZmFaNTRfZ0dJQjR1SWduQ2c5QWVkVUk1cXdBU2N4MndPWW5veVZnRTBnMTg3eGpPbnc2dng2bHNIXzYzb3RZQnJLSnlGa044Qk1l?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5nRGRFY0xzaHZoN1pHVXd5dkZKQ0t0UTlDMHBoTFY5OW5nZmFaNTRfZ0dJQjR1SWduQ2c5QWVkVUk1cXdBU2N4MndPWW5veVZnRTBnMTg3eGpPbnc2dng2bHNIXzYzb3RZQnJLSnlGa044Qk1l?oc=5`
- Body excerpt: "AI 서버 MLCC 없어서 못 판다"…코칩, 슈퍼커패시터로 데이터센터·로봇·자율주행 공급 타진 파이낸셜포스트

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-31T10:12:20+09:00`
- Company: [[KRX_126730_코칩]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-31.json`
- Latest observation title: `"AI 서버 MLCC 없어서 못 판다"…코칩, 슈퍼커패시터로 데이터센터·로봇·자율주행 공급 타진 - 파이낸셜포스트`
- Latest observation source: `파이낸셜포스트`
- Latest observation published_at: `2026-08-28T13:21:18+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5nRGRFY0xzaHZoN1pHVXd5dkZKQ0t0UTlDMHBoTFY5OW5nZmFaNTRfZ0dJQjR1SWduQ2c5QWVkVUk1cXdBU2N4MndPWW5veVZnRTBnMTg3eGpPbnc2dng2bHNIXzYzb3RZQnJLSnlGa044Qk1l?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
