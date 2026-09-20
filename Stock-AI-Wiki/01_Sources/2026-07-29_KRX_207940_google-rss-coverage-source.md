---
id: source-2026-07-29-KRX-207940-google-rss-coverage
type: source
title: KRX 207940 Google RSS Coverage Source
created: 2026-07-29
updated: 2026-07-29
status: raw
stage: 0

market: KRX
ticker: "207940"
company: 삼성바이오로직스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 207940.
  key_facts:
    - code=207940
    - name=삼성바이오로직스
    - naver_article_count=1
    - google_rss_article_count=14
    - kis_title_count=22
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 207940
    - 삼성바이오로직스
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

# KRX 207940 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=207940`
- `name=삼성바이오로직스`
- `naver_article_count=1`
- `google_rss_article_count=14`
- `kis_title_count=22`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 207940.

## RSS Item Metadata
- Title: `삼성바이오 ‘CDMO 초격차’ vs 셀트리온 ‘시밀러 질적성장’…거침없는 ‘최대 실적’ - 헤럴드경제`
- Source: `헤럴드경제`
- Published at: `2026-07-28T09:20:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE15bXdYZEdvOHozcnFxQ1Bxb2hZaWg4dHNaZDlabXRZN2h2cnFHWTV3clJscWlCZS1GTDJTRldPYUU3Tk5kRmxzWGxqcmJFaDlXNVk2akRB?oc=5`

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
- Updated at: `2026-08-21T19:32:53+09:00`
- Company: [[KRX_207940_삼성바이오로직스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `‘영업이익률 44% vs 32%’⋯ 삼성바이오·셀트리온, 단순 비교 불가 이유 - 브릿지경제`
- Latest observation source: `브릿지경제`
- Latest observation published_at: `2026-07-29T06:06:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9JNGVLTWZEX2pTRTJxdm9PcEljSm9XTGh5MGNjSHFZQ2RxcFJUd2Y3MFI5S3VTLVlaS2RLT0Y1cFk3dEZWekVKTDJhZG5WcTBwbC1DUi05cnp2UQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
