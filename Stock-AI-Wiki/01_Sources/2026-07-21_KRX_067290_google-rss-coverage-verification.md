---
id: verification-2026-07-21-KRX-067290-google-rss-coverage
type: verification
title: KRX 067290 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "067290"
company: JW신약
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=067290
    - name=JW신약
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 067290
    - JW신약
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

# KRX 067290 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_067290_google-rss-coverage-source]]

## Facts Checked
- `code=067290`
- `name=JW신약`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `JW신약, '듀크레이 케르티올 크림' 인플루언서 설문 결과 발표 - 팜뉴스`
- Source: `팜뉴스`
- Published at: `2026-07-21T09:41:15+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1zN3N5RmJDS25lXzQySUtWdEgtSkhKNWg1N0RVSlFGRTI2WlVhWWdFeFNNSkhuR1pqNGJuaEJmNUJMUmxOR0FCVFcwYjltZ0ttemp2VXllNWZTUWcxN05vS2doeWgzMXIyV29PTQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-21T21:05:15+09:00`
- Company: [[KRX_067290_JW신약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `JW신약 "듀크레이 케르티올, 인플루언서 설문서 각질 개선 만족도 확인" - 파이낸셜뉴스`
- Latest observation source: `파이낸셜뉴스`
- Latest observation published_at: `2026-07-21T09:30:13+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBreVRHQ0c1VmcyN3IyVzFtM19wM3A4SkQ2VGRVZ1QwOTJqcFBTZ3lzcllubjRQOFRtSW5zUEVqd1ZkUlA5UThURldXdEwtY3pSbXlCYlppX28wQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
