---
id: verification-2026-05-27-KRX-195870-google-rss-coverage
type: verification
title: KRX 195870 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "195870"
company: 해성디에스
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
    - code=195870
    - name=해성디에스
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=14
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 195870
    - 해성디에스
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

# KRX 195870 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_195870_google-rss-coverage-source]]

## Facts Checked
- `code=195870`
- `name=해성디에스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=14`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[장중수급포착] 해성디에스, 외국인/기관 동시 순매수… 주가 +21.68% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-05-26T13:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFBkVTEzSTdidGhaQWdfdVhGWEhaeWRzZ205cXB0ZE5KTjRseGFjQjhIaTY2cXh2SFIybjBSamNveXpHb3pYTWVyM1R2ckFiWHY3YUNCUHo0ZVA5Y2ZQ?oc=5`

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
- Company: [[KRX_195870_해성디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `해성디에스, -10.45% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T11:56:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNVFRaZFdtaFBydWhCM2xiZXBfSm5qbDExd2F2MVBKYkUybHp2ZENlS01ZMEt0T0dmRXRYb3FEaHVyVnNVVDIwWVV1ZTc1Ylkwc05qMTZZX1g5RE9jODJLaW5vV0JVWFdPUnhVQmFkcDlSVDBxRWRZbzQtUDc0S2xPR09FZ3hMOVdqcU5QVENLdDByOW51Qm930gGXAUFVX3lxTE1UVFpkV21oUHJ1aEIzbGJlcF9KbmpsMTF3YXYxUEpiRTJsenZkQ2VLTVkwS3RPR2ZFdFhvcURodXJWc1VUMjBZVXVlNzViWTBzTmoxNllfWDlET2M4Mktpbm9XQlVYV09SeFVCYWRwOVJUMHFFZFlvNC1QNzRLbE9HT0VneEw5V2pxTlBUQ0t0MHI5bnVCb3c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
