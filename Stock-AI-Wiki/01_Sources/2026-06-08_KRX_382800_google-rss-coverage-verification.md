---
id: verification-2026-06-08-KRX-382800-google-rss-coverage
type: verification
title: KRX 382800 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "382800"
company: 지앤비에스 에코
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
    - code=382800
    - name=지앤비에스 에코
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 382800
    - 지앤비에스 에코
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

# KRX 382800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_382800_google-rss-coverage-source]]

## Facts Checked
- `code=382800`
- `name=지앤비에스 에코`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `지앤비에스 에코, +2.82% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-08T13:24:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxQVXo4N05OakludVRDRzVPTkhFcG93TmJ4OHJVeHpXUEJNdUVUOFRzQjRtNkNIcy1VQkZ6VjBTeFI2RS10U3ZfRkJ5U3d2dGtmdjFiM2RSZXRrU3hUbzJjTFBESGxOZVJzUGdFVWNQOV9FTTBvdVR2bExoUzV6YURjVVQxd2hyQ0EteE1palA4ckdKYkpaVGNr0gGXAUFVX3lxTFBVejg3Tk5qSW51VENHNU9OSEVwb3dOYng4clV4eldQQk11RVQ4VHNCNG02Q0hzLVVCRnpWMFN4UjZFLXRTdl9GQnlTd3Z0a2Z2MWIzZFJldGtTeFRvMmNMUERIbE5lUnNQZ0VVY1A5X0VNMG91VHZsTGhTNXphRGNVVDF3aHJDQS14TWlqUDhyR0piSlpUY2s?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:12+09:00`
- Company: [[KRX_382800_지앤비에스-에코]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `지앤비에스 에코, +2.82% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-08T13:24:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxQVXo4N05OakludVRDRzVPTkhFcG93TmJ4OHJVeHpXUEJNdUVUOFRzQjRtNkNIcy1VQkZ6VjBTeFI2RS10U3ZfRkJ5U3d2dGtmdjFiM2RSZXRrU3hUbzJjTFBESGxOZVJzUGdFVWNQOV9FTTBvdVR2bExoUzV6YURjVVQxd2hyQ0EteE1palA4ckdKYkpaVGNr0gGXAUFVX3lxTFBVejg3Tk5qSW51VENHNU9OSEVwb3dOYng4clV4eldQQk11RVQ4VHNCNG02Q0hzLVVCRnpWMFN4UjZFLXRTdl9GQnlTd3Z0a2Z2MWIzZFJldGtTeFRvMmNMUERIbE5lUnNQZ0VVY1A5X0VNMG91VHZsTGhTNXphRGNVVDF3aHJDQS14TWlqUDhyR0piSlpUY2s?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
