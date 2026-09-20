---
id: verification-2026-06-11-KRX-004000-google-rss-coverage
type: verification
title: KRX 004000 Google RSS Coverage Verification
created: 2026-06-11
updated: 2026-06-11
status: verification
stage: 1

market: KRX
ticker: "004000"
company: 롯데정밀화학
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=004000
    - name=롯데정밀화학
    - naver_article_count=0
    - google_rss_article_count=8
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 004000
    - 롯데정밀화학
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

# KRX 004000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-11_KRX_004000_google-rss-coverage-source]]

## Facts Checked
- `code=004000`
- `name=롯데정밀화학`
- `naver_article_count=0`
- `google_rss_article_count=8`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `롯데정밀화학, +3.86% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-09T12:44:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNbUFBTWxFRzVqY1NSM3RweXh2Y1ZpRDVNOWw0QmVnUEtMdDUtTFhEYTh5Wmo2ajFHWE1yMW1hNWNiSVR3QU9RTTNjNmNuUHFzU21qcnEtVDdMcHNfdFVWWXczNUxkdGtmeG0wbUJDYkJNWjk5dEpQSEc0QlpHT2oyVWVsdzdVa2ZOOXQwcnNZelQ1eGh2TzZJ0gGXAUFVX3lxTE1tQUFNbEVHNWpjU1IzdHB5eHZjVmlENU05bDRCZWdQS0x0NS1MWERhOHlaajZqMUdYTXIxbWE1Y2JJVHdBT1FNM2M2Y25QcXNTbWpycS1UN0xwc190VVZZdzM1TGR0a2Z4bTBtQkNiQk1aOTl0SlBIRzRCWkdPajJVZWx3N1VrZk45dDByc1l6VDV4aHZPNkk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-11T09:05:15+09:00`
- Company: [[KRX_004000_롯데정밀화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `롯데정밀화학 투자분석 2026. 06. 10 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-06-11T05:48:17+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5XbXFnMFlBQ0JoNGR2bl9sdjY5eTlyR1ktenVCcEdyWGU3Ym01SGRVa3JSSE1UTWVHSFFNY1F4bFFGcUR4QVByNThfdVQ1VnlPekJpSUQyeXBhaWs4UFZReHlSLXVwY3ZEQVZ0azZ5UzlQZXM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
