---
id: verification-2026-08-24-KRX-001510-google-rss-coverage
type: verification
title: KRX 001510 Google RSS Coverage Verification
created: 2026-08-24
updated: 2026-08-24
status: verification
stage: 1

market: KRX
ticker: "001510"
company: SK증권
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
    - code=001510
    - name=SK증권
    - naver_article_count=10
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 001510
    - SK증권
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

# KRX 001510 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-24_KRX_001510_google-rss-coverage-source]]

## Facts Checked
- `code=001510`
- `name=SK증권`
- `naver_article_count=10`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK증권, 해외 기업 '비지오'의 국내 로드쇼 주관 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-24T10:30:10+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE45OXA4X0J3OVhjVFBWSTVjMExzWVNPb1Zzb2pKelNUS2lfTXdEdlVNZlAxb0NnV3dVa0dmTmlyUnhXVkxWNmRCMnlyZzR1bkk?oc=5`

## Article Body Archive Checked
- Title: `SK증권, 해외 기업 '비지오'의 국내 로드쇼 주관 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-24T10:30:10+09:00`
- URL: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE45OXA4X0J3OVhjVFBWSTVjMExzWVNPb1Zzb2pKelNUS2lfTXdEdlVNZlAxb0NnV3dVa0dmTmlyUnhXVkxWNmRCMnlyZzR1bkk?oc=5`
- Evidence path: `https://n.news.naver.com/mnews/article/011/0004653849?sid=101`
- Body excerpt: 삼성전자·삼성전기·LS 순매수 상위 삼성전기 5%대 하락에 저가매수 유입 SK하이닉스·삼양바이오팜 순매도 1·2위 미래에셋증권에 따르면 주식 거래 고객 중 최근 1개월간 투자 수익률 상위 1%에 해당하는 ‘주식 초고수’들이 11시까지 가장 많이 사들인 종목은 삼성전자다. 삼성전자는 같은 시간 전 거래일보다 2.77% 오른 27만 8500원에 거래됐다. 삼성전자가 이르면 이달 중 대규모 주주환원 정책을 공개할 수 있다는 전망이 매수세를 자극한 것으로 풀이된다. 블룸버그는 삼성전자의 주주환원 규모가 90조~110조 원에 이를 수 있다고 보도했다. DS투자증권은 올해 잉여현금흐름(FCF)을 263조 2000억 원으로 추산해 최근 3년 누적 FCF의 50%에서 정기배당을 제외하면 131조 8000억 원을 추가 환원할 여력이 있다고 분석했다. 삼성전자 역시 2분기 실적발표 당시 특별배당을 포함한 주주환원 정책과 차기 정책을 논의 중이라고 밝힌 바 있다. 삼성전자는 2024~2026년 3년간 누적 잉여현금흐름(FCF)의 50%를 주주환원에 활용하는 정책을 운용하고 있다. KB증권은 이날 보고서를 통해 해당 정책을 적용할 경우 잔여 재원을 활용한 연내 특별배당 규모가 최소 100조 원을 넘어설 것으로 추정했다. 김동원 KB증권 리서치본부장은 “특별배당의 현금배당 비중이 확대될 경우 높은 배당수익률이 부각되면서 삼성전자 주가는 단기적으로 30만 원대 안착이 가능할 것”이라고 말했다. 순매수 2위는 삼성전기다. 삼성전기는 5.80% 내린 131만 5000원에 거래되면서 주가 하락을 매수 기회로 본 자금이 유입된 것으로 보인다. 인공지능(AI) 서버용 적층세라믹콘덴서(MLCC)와 기판 수요 확대에 따른 중장기 실적 기대도 이어지고 있다. 유안타증권은 삼성전기가 고성능 AI 서버용 초고압·고용량 M...

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-24T18:05:32+09:00`
- Company: [[KRX_001510_SK증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-24.json`
- Latest observation title: `SK증권, 해외 기업 '비지오'의 국내 로드쇼 주관 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-24T10:30:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE45OXA4X0J3OVhjVFBWSTVjMExzWVNPb1Zzb2pKelNUS2lfTXdEdlVNZlAxb0NnV3dVa0dmTmlyUnhXVkxWNmRCMnlyZzR1bkk?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
