---
id: verification-2026-06-23-KRX-042700-google-rss-coverage
type: verification
title: KRX 042700 Google RSS Coverage Verification
created: 2026-06-23
updated: 2026-06-23
status: verification
stage: 1

market: KRX
ticker: "042700"
company: 한미반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=2
    - google_rss_article_count=8
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042700
    - 한미반도체
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

# KRX 042700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-23_KRX_042700_google-rss-coverage-source]]

## Facts Checked
- `code=042700`
- `name=한미반도체`
- `naver_article_count=2`
- `google_rss_article_count=8`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[히든밸류 소부장]①한미반도체, TC본더 패권 더 굳힌다 - 블로터`
- Source: `블로터`
- Published at: `2026-06-22T11:30:25+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5tWUZBdkRGa3NqQVNhell0RlRuWWFqVU5EOG5lZXFxbks5cktEajRoOXo3SXBqeGtqMkJhR0hST01NMDhIVXpScElaTHRHRzViLUtQa3o1TDVZTlpFMWNRZC1XV204d3lMR29EaNIBbEFVX3lxTE5tWUZBdkRGa3NqQVNhell0RlRuWWFqVU5EOG5lZXFxbks5cktEajRoOXo3SXBqeGtqMkJhR0hST01NMDhIVXpScElaTHRHRzViLUtQa3o1TDVZTlpFMWNRZC1XV204d3lMR29EaA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:39+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-23.json`
- Latest observation title: `[히든밸류 소부장]①한미반도체, TC본더 패권 더 굳힌다 - 블로터`
- Latest observation source: `블로터`
- Latest observation published_at: `2026-06-22T11:24:47+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9EUkRBSElGQXY4Y25hQWIwQjJhcnFqblA5bTViSnhMbmFSNDVBODNvSFFFeV94d0c4WXpoLUVkM1llcWxOd0tlU0hnZENXV3VVejlPSVBEb1RxbGE3TkhmTWJLOElrWUJz0gFsQVVfeXFMTm1ZRkF2REZrc2pBU2F6WXRGVG5ZYWpVTkQ4bmVlcXFuSzlyS0RqNGg5ejdJcGp4a2oyQmFHSFJPTU0wOEhVelJwSVpMdEdHNWItS1BrejVMNVlOWkUxY1FkLVdXbTh3eUxHb0Ro?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
