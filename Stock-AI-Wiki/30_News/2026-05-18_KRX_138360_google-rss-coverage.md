---
id: news-2026-05-18-KRX-138360-google-rss-coverage
type: news
title: KRX 138360 Google RSS Coverage
created: 2026-05-18
updated: 2026-05-18
status: interpreted
stage: 0

market: KRX
ticker: "138360"
company: Hyupjin
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Local coverage report indicates at least one Google RSS item and seven KIS title items for KRX 138360.
  key_facts:
    - google_rss_article_count=1
    - kis_title_count=7
    - naver_article_count=0
    - any_covered=True
  related_entities:
    - KRX 138360
    - Hyupjin
  possible_impact: unknown
  uncertainty:
    - Coverage exists, but original article text is not verified.
    - This note cannot infer sentiment or trading implication.

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
  change_reason: real local coverage sample
---

# KRX 138360 Google RSS Coverage

## Source
- [[2026-05-18_KRX_138360_google-rss-coverage-source]]

## Facts
- Local CSV evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- KRX code: `138360`
- Company label: `Hyupjin`
- Google RSS article count: `1`
- KIS title count: `7`
- Naver article count: `0`
- Any covered: `True`

## Interpretation
- The local pipeline observed news coverage for KRX 138360.
- This note does not know whether the coverage is positive, negative, relevant, or duplicate.

## Uncertainty
- Original article text is unavailable in the CSV row.
- RSS probe text contains mojibake in the current local artifact.
- No source conflict check was completed.

## Related Notes
- [[2026-05-18_KRX_138360_google-rss-coverage-verification]]
- [[2026-05-18_KRX_138360_google-rss-thesis-blocked]]

## Questions
- What is the original Google RSS article title and publisher?
- Is the coverage item actually about the listed company?
- Is there a primary source or only secondary media coverage?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_138360_협진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `농어촌 취약지 응급환자 '원격협진' 시스템 확대 - 메디칼타임즈`
- Latest observation source: `메디칼타임즈`
- Latest observation published_at: `2026-05-17T09:30:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE94SEdjR2hoZEhZTU95TjZNRHFZQVFCbjByTmdyR3hTV1prV082eGI0RThPM1FvV3BJcWE2NEx1YW05RnRtaHpaYXZOamNrc2pHN2QyRjVzejZIZ2p4MGJUZTJUSU5iSVF3WXdxVnh3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
