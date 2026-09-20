---
id: verification-2026-08-15-KRX-034020-google-rss-coverage
type: verification
title: KRX 034020 Google RSS Coverage Verification
created: 2026-08-15
updated: 2026-08-15
status: verification
stage: 1

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=3
    - google_rss_article_count=7
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-15_KRX_034020_google-rss-coverage-source]]

## Facts Checked
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=3`
- `google_rss_article_count=7`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `두산에너빌리티, 미국 테라파워 SMR 핵심 기자재 수주 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-14T08:48:33+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9XNzM5NUFoTENZa3VTRDltUVJtZDJ3eDJ3MGlWZHJNeC1HNmtQNlE4N0ZCZEpzOGljZWs0d0VaLVN0SjhtMmhqV3JlSmtQWGVNR0FZdVpMckdmMVnSAWBBVV95cUxNVVctUnlGVHpfakdqb1pwR0xkbzFKR016NmF5QU16YUloaHF6VTNtaUNOcWxBZmRRYnlYek43c3JvaGRfMnlKRWFyNElua1oyX2FudUhDOENyLW92YkFWUms?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:30+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-15.json`
- Latest observation title: `두산에너빌, 빌 게이츠의 테라파워에 'SMR 핵심 기자재' 공급 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-08-14T08:43:46+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBOdmFYZUJUb01sQVR3RXZqSFNHT0hld3c4cFFMb0hkUzhiLTJsNENVUHZlUV9TV0kya2JUb2hFX1VhamR5YlVxaXdQRTRrNmRPQ1RDQkFiYXFKYmVrWkRlOW8wNlRYUmo3bFJCbtIBckFVX3lxTFBYZDM5QmtzamJPb1REZlVYSGh3eVJBXzBhenJCYnAwNmpYZ0hIcmZyS055NlpMdi05Yi16YU9IdlB1VHRrZHB5Z1VMaGZJaVRTMGdRaThIRDVGQzE4YVVmbE1UTS1GcTlFR0xLeER4dkhLdw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
