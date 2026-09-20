---
id: verification-2026-08-26-KRX-052690-google-rss-coverage
type: verification
title: KRX 052690 Google RSS Coverage Verification
created: 2026-08-26
updated: 2026-08-26
status: verification
stage: 1

market: KRX
ticker: "052690"
company: 한전기술
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=052690
    - name=한전기술
    - naver_article_count=3
    - google_rss_article_count=19
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 052690
    - 한전기술
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 052690 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-26_KRX_052690_google-rss-coverage-source]]

## Facts Checked
- `code=052690`
- `name=한전기술`
- `naver_article_count=3`
- `google_rss_article_count=19`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `원전·건설주, 수주 기대감 연이틀 강세…한전기술 13% 급등[핫종목] - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-26T16:58:47+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9MMTJvcmFZNEJMMDhoZXRJWU5XZ1gwaG45LUtFRWkwdmliZExHMUZydVlDTW43YVRiVGV2OFY0V2ZBeG9JbVlSTUMyN2hHa0U?oc=5`

## Article Body Archive Checked
- Title: `원전·건설주, 수주 기대감 연이틀 강세…한전기술 13% 급등[핫종목] - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-26T16:58:47+09:00`
- URL: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9MMTJvcmFZNEJMMDhoZXRJWU5XZ1gwaG45LUtFRWkwdmliZExHMUZydVlDTW43YVRiVGV2OFY0V2ZBeG9JbVlSTUMyN2hHa0U?oc=5`
- Evidence path: `https://n.news.naver.com/mnews/article/009/0005725732?sid=101`
- Body excerpt: 미국 신규 대형원전 사업에서 국내 기업의 수주가 늘어날 것이란 기대에 원전주가 일제히 급등했다. 현대건설이 미국 텍사스 원전 기본설계를 완료했다고 공개한 데 이어 증권가에서 미국 원전 정책에 따른 발주 조기화 가능성을 잇달아 제기하면서 설계·시공·기자재 업체 전반으로 매수세가 확산됐다. 25일 한전기술 주가는 전 거래일보다 20.27% 급등한 10만8600원에 거래를 마쳤다. 같은 날 현대건설은 14.87% 상승한 12만1300원을 기록했다. 비에이치아이와 대우건설도 각각 13.80%, 11.80% 올랐고 두산에너빌리티는 10.55% 상승했다. 김주연 미래에셋증권 연구원은 이날 원전주 흐름을 "대형 프로젝트 수주 가시화 기대감에 섹터 전반이 강세"라고 짚었다. 현대건설의 미 원전사업 진척이 직접적인 재료로 작용했다. 현대건설은 전날 공시를 통해 2025년 10월 페르미 아메리카와 체결한 미국 대형 원전 건설 기본설계(FEED) 역무를 완료했다고 밝혔다. 회사는 설계·조달·시공(EPC) 계약 체결을 목표로 사업성·안전·공사기간 등 구체적인 내용을 검토하고 있다. 해당 사업은 미국 텍사스주 애머릴로 인근에 조성되는 '프로젝트 매터도어'다. 페르미 아메리카는 이곳에 웨스팅하우스의 AP1000 원전 4기를 건설할 계획이다. [신윤재 기자]

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-26T23:05:22+09:00`
- Company: [[KRX_052690_한전기술]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-26.json`
- Latest observation title: `원전·건설주, 수주 기대감 연이틀 강세…한전기술 13% 급등[핫종목] - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-26T16:58:47+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9MMTJvcmFZNEJMMDhoZXRJWU5XZ1gwaG45LUtFRWkwdmliZExHMUZydVlDTW43YVRiVGV2OFY0V2ZBeG9JbVlSTUMyN2hHa0U?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
