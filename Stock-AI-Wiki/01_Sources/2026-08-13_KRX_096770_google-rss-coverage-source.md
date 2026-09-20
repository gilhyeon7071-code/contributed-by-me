---
id: source-2026-08-13-KRX-096770-google-rss-coverage
type: source
title: KRX 096770 Google RSS Coverage Source
created: 2026-08-13
updated: 2026-08-13
status: raw
stage: 0

market: KRX
ticker: "096770"
company: SK이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 096770.
  key_facts:
    - code=096770
    - name=SK이노베이션
    - naver_article_count=1
    - google_rss_article_count=20
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 096770
    - SK이노베이션
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 096770 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=096770`
- `name=SK이노베이션`
- `naver_article_count=1`
- `google_rss_article_count=20`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 096770.

## RSS Item Metadata
- Title: `SK이노베이션, 에너지 실적 개선 속 배터리에 쏠린 시선… ESS 3차 입찰 전망은? - 리버티코리아포스트`
- Source: `리버티코리아포스트`
- Published at: `2026-08-12T15:52:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTFBndGFHRFJXek5uc3ozZ0I5Q2tTRWRFcDF2NC1MZkdHSEUtZGNOZklxNy1GcEdieTRBblFhdkhsdWx2R0kteVVzVDZ1UVFlU2lTRkFnWE04d0tfYkRQVFZZY2owONIBZ0FVX3lxTE1tZWpVTWI3NEw5T09JUXhhZlRIb0RtcTRqemFLbFNoZVdLVGViSF9HTkpLcjlId3dadU12M1NsY3NjVTNyTHZpc3NZcnAxbHJwUW9XTUhwa09Ha01mdVA5ZUtPOGVWcm8?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-13T10:05:56+09:00`
- Company: [[KRX_096770_SK이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `SK이노베이션 주가 상승 중 - 아시아뉴스통신`
- Latest observation source: `아시아뉴스통신`
- Latest observation published_at: `2026-08-13T09:11:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE9lck5Bay1pM1BmVjJYZzBJTnc3SENtelZlajg4NDhHZ2N4dDNxcGQxS0pkT3BSOFAwSUk3SGsxZDF1aUFocHhoMElBMjZ6TFNIZEQ5Y1ZjRkFBeEpp?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
