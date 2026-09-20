---
id: verification-2026-08-24-KRX-028260-google-rss-coverage
type: verification
title: KRX 028260 Google RSS Coverage Verification
created: 2026-08-24
updated: 2026-08-24
status: verification
stage: 1

market: KRX
ticker: "028260"
company: 삼성물산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=2
    - google_rss_article_count=3
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028260
    - 삼성물산
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

# KRX 028260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-24_KRX_028260_google-rss-coverage-source]]

## Facts Checked
- `code=028260`
- `name=삼성물산`
- `naver_article_count=2`
- `google_rss_article_count=3`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성 덕에 '특별배당' 돈벼락…주주들 함박웃음 짓는 회사 [종목+] - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-23T16:30:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE42RHdQTHREVXA5T29HemhQT2pDU3V4NWlCTlJQd2dPTUFFSGdMTVMzZkhYSmY4ZUxMQVFIajVFUnAtUGE5ZkNVLU04OHRDd2RKMExhVkZzbTV0QQ?oc=5`

## Article Body Archive Checked
- Title: `삼성 덕에 '특별배당' 돈벼락…주주들 함박웃음 짓는 회사 [종목+] - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-23T16:30:14+09:00`
- URL: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE42RHdQTHREVXA5T29HemhQT2pDU3V4NWlCTlJQd2dPTUFFSGdMTVMzZkhYSmY4ZUxMQVFIajVFUnAtUGE5ZkNVLU04OHRDd2RKMExhVkZzbTV0QQ?oc=5`
- Evidence path: `https://finance.naver.com/research/industry_read.naver?nid=45801`
- Body excerpt: 삼성전자 90~110조원 주주환원 발표 ⇒ 자사주 소각 여력은 10~20조원으로 추정 삼성전자는 26년 주주환원 재원 90~110조원을 결정했고 이 중 30조원을 3분기말 현금 배당으로 지급하기로 했다. 잔여 재원 60~80조원은 내년 초 집행한다. 다만 삼성생명 + 삼성화재의 삼성전자 보통주 지분율이 10%로 맞춰져있어 보통주를 추가 소각할 경우 양사 지분율이 금산법 한도인 10%를 초과하게 된다. 참고로 올해 3월에도 양사는 삼성전자 자사주 소각에 앞서 약 1.5조원 규모의 삼성전자 지분을 블록딜 처리한 바 있다. 이를 감안하면 내년 1월 확정될 잔여 주주환원 약 70조원 중 상당 부분은 배당으로 집행될 전망이다. 당사는 내년 초 집행되는 주주환원에서 자사주 매입/소각 금액은 약 10~20조원으로 추정하며 배당 규모는 약 50~60조원으로 추정한다.

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-24T18:05:32+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-24.json`
- Latest observation title: `삼성 덕에 '특별배당' 돈벼락…주주들 함박웃음 짓는 회사 [종목+] - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-08-23T16:30:14+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE42RHdQTHREVXA5T29HemhQT2pDU3V4NWlCTlJQd2dPTUFFSGdMTVMzZkhYSmY4ZUxMQVFIajVFUnAtUGE5ZkNVLU04OHRDd2RKMExhVkZzbTV0QQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
