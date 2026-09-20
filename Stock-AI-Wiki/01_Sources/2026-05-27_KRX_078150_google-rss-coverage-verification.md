---
id: verification-2026-05-27-KRX-078150-google-rss-coverage
type: verification
title: KRX 078150 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "078150"
company: HB테크놀러지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=078150
    - name=HB테크놀러지
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 078150
    - HB테크놀러지
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

# KRX 078150 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_078150_google-rss-coverage-source]]

## Facts Checked
- `code=078150`
- `name=HB테크놀러지`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HB테크놀러지, 차세대 디스플레이 및 AI 기술로 글로벌 시장 공략 박차 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-06T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBxTnFNU1hyN05xbnRCVVBXc2Qzc1hiUHlxMHlhZDFZcWtUQ1QxT0FEcmVTTElONUMyZnloRmtuQ2QwcUZsQ05aX01vOUlIOVg4ZmpOWVRKSFE3enBKbjRwT280R2tKVGdocFpVZnlmd1pSZlHSAXdBVV95cUxPbkliRkxzTGphUjIwWGVHUUhLZkl0eDlUSndGWHhSOXJETUNHYjVhUlhYbXdwU2Q2WC16cmJaeGctZjRNcmFUWmpqWUFPRF9IZkFoNkJvakZDdUpRZ195d0FYUXFjWjE0UkUwTlBMRXFlbnpVTkRNWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T16:05:05+09:00`
- Company: [[KRX_078150_HB테크놀러지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `HB테크놀러지, -7.91% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T09:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxOZjYyUU03Q0IyM0xoNEkzNG0xd214Ull1VnZ6aEIzdWJmNEh3ZThfLVBnMnRmdDU4ckRDQXRvWUhFblhHSllReS12ekN2NDRBSEtBSkFEc0VQbWRXRVpWQUdsY0RkLVJYaHI2UnBXNjhhT3JrcEtRTkpweC1Yejk3SUYyZ9IBlwFBVV95cUxPS1BJelhOVUIyb25LTWM2Y050c3haeHdnYnJ2ak51b2Yza3ZXVW5fV1lUOVJPMjhjMFNYS1FXQ195RS04M1lHTmxVNzE4TUN2SThzeXhVRXZnUVd4TFJ4NmdvaTFRWmMwTVk1MGhUSUI4X3d5cmF5bnE5SlQ2V3NoLWNGckFwRUo4TFZaalJDR3oyTHZidUUw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
