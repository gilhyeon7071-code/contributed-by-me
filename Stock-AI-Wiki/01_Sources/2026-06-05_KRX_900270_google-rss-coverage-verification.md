---
id: verification-2026-06-05-KRX-900270-google-rss-coverage
type: verification
title: KRX 900270 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "900270"
company: 헝셩그룹
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=900270
    - name=헝셩그룹
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 900270
    - 헝셩그룹
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

# KRX 900270 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_900270_google-rss-coverage-source]]

## Facts Checked
- `code=900270`
- `name=헝셩그룹`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `헝셩그룹, 中 문화관광 시장 공략…고궁박물관 등에 관련 제품 납품 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-06-01T08:18:15+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFB2d3hwYnhkVkhYaVphb0c4Q01wU2h3TjBCZXdZclRUSjQ1WGRJemRrOGV6Z0FLbzhRYXluQXBJby1QSVEyNUhueTA3Vlp5VmhxRXg0RHFmcVRPNUJoRGpzUg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_900270_헝셩그룹]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `헝셩그룹, 中 문화관광 시장 공략…고궁박물관 등에 관련 제품 납품 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-06-01T08:18:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFB2d3hwYnhkVkhYaVphb0c4Q01wU2h3TjBCZXdZclRUSjQ1WGRJemRrOGV6Z0FLbzhRYXluQXBJby1QSVEyNUhueTA3Vlp5VmhxRXg0RHFmcVRPNUJoRGpzUg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
