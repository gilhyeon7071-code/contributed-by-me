---
id: source-2026-08-13-KRX-068270-google-rss-coverage
type: source
title: KRX 068270 Google RSS Coverage Source
created: 2026-08-13
updated: 2026-08-13
status: raw
stage: 0

market: KRX
ticker: "068270"
company: 셀트리온
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 068270.
  key_facts:
    - code=068270
    - name=셀트리온
    - naver_article_count=2
    - google_rss_article_count=82
    - kis_title_count=25
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 068270
    - 셀트리온
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

# KRX 068270 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=068270`
- `name=셀트리온`
- `naver_article_count=2`
- `google_rss_article_count=82`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 068270.

## RSS Item Metadata
- Title: `[단독] 셀트리온, 일본 스텔라라 '독점시장' 깬다…첫 IV 바이오시밀러 출시 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-11T10:48:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE51MDJPOHl4SGZFOVF4RXZYU2dZeVlTNDBnQ1Y4b09XZkgzamloUGg4ZFh3anRZeVZyTV9FMTVqUVY4aGNsbGhoYWhEUXA4UU5sYmpja1Z6THd6RDJa?oc=5`

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
- Updated at: `2026-08-21T19:35:49+09:00`
- Company: [[KRX_068270_셀트리온]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `[단독] 셀트리온, 일본 스텔라라 '독점시장' 깬다…첫 IV 바이오시밀러 출시 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-08-11T10:48:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE51MDJPOHl4SGZFOVF4RXZYU2dZeVlTNDBnQ1Y4b09XZkgzamloUGg4ZFh3anRZeVZyTV9FMTVqUVY4aGNsbGhoYWhEUXA4UU5sYmpja1Z6THd6RDJa?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
