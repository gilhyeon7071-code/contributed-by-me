---
id: verification-2026-08-25-KRX-010130-google-rss-coverage
type: verification
title: KRX 010130 Google RSS Coverage Verification
created: 2026-08-25
updated: 2026-08-25
status: verification
stage: 1

market: KRX
ticker: "010130"
company: 고려아연
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010130
    - name=고려아연
    - naver_article_count=1
    - google_rss_article_count=16
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010130
    - 고려아연
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

# KRX 010130 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-25_KRX_010130_google-rss-coverage-source]]

## Facts Checked
- `code=010130`
- `name=고려아연`
- `naver_article_count=1`
- `google_rss_article_count=16`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `원자재 랠리에 AI인프라 수요…고려아연·삼아알미늄 뜬다 - 매일경제TV`
- Source: `매일경제TV`
- Published at: `2026-08-24T17:50:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5DUF9LWDlkQkp6ZDFaT0RjZTlfbTA4ZVFGUUxSdHhwX3k0bFdYQUNscXJuSEZSUTJnQURhR1VFQmlPWUJZZEIwd1Y0MWc5YmkwWFBpWXlsUnhNWnNRNDZPSWdrbW5TZXli?oc=5`

## Article Body Archive Checked
- Title: `원자재 랠리에 AI인프라 수요…고려아연·삼아알미늄 뜬다 - 매일경제TV`
- Source: `매일경제TV`
- Published at: `2026-08-24T17:50:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5DUF9LWDlkQkp6ZDFaT0RjZTlfbTA4ZVFGUUxSdHhwX3k0bFdYQUNscXJuSEZSUTJnQURhR1VFQmlPWUJZZEIwd1Y0MWc5YmkwWFBpWXlsUnhNWnNRNDZPSWdrbW5TZXli?oc=5`
- Evidence path: `https://n.news.naver.com/mnews/article/011/0004653450?sid=101`
- Body excerpt: 중국, 엔비디아 H200 본토 반입 허용 삼성전자 7.82%·SK하이닉스 9.75% 급락 SK하이닉스, D램 사용량 16분의 1로 [주요 이슈 브리핑] ■ 중국 H200 반입: 중국이 엔비디아의 최신 인공지능(AI) 칩 H200의 본토 반입을 허용하면서 바이트댄스와 텐센트가 최근 수주 동안 각각 1만 개를 들여온 것으로 파이낸셜타임스(FT)가 전했다. 극자외선(EUV) 노광장비 수출 제한으로 자국 첨단 제조 역량이 부족해지자 미국산 칩 반입으로 방향을 튼 것으로 풀이되는 모습이다. ■ 메모리주 동반 급락: 뉴욕증시에서 AI 메모리 반도체주가 18일(현지 시간) 일제히 급락하며 필라델피아반도체지수(SOX)가 4.98% 밀렸고, 그 여파로 삼성전자(005930)와 SK하이닉스(000660)도 19일 종가 기준 각각 7.82%, 9.75% 떨어졌다. 미국 30년물 국채금리가 장중 5.33%를 넘어선 것이 직접적 배경으로 꼽히지만 시장 일각에서는 AI 메모리 슈퍼사이클에 균열이 생긴 것 아니냐는 분석이 나온다. ■ 하이브리드 메모리: SK하이닉스가 낸드플래시 기반 솔리드스테이트드라이브(SSD)를 AI용 메모리로 활용하는 하이브리드 메모리를 실증하며 AI 가속기의 메모리 구조 자체를 바꾸는 해법을 제시했다. 같은 D램 예산을 투입했을 때 데이터 처리 능력이 기존 모델 대비 최대 3배 높았고, 1TB 분산 D램 시스템과 비교하면 성능은 약 30% 낮았지만 D램 사용량은 16분의 1 수준에 그친 것으로 확인됐다. [기업 CEO 관심 뉴스] 1. 중국, 바이트댄스·텐센트에 엔비디아 H200 반입 허용 - 핵심 요약: 중국이 미·중 AI 경쟁 속 기술 격차를 좁히기 위해 엔비디아 최신 AI 칩의 본토 반입을 허용했다. FT는 소식통을 인용해 바이트댄스와 텐센트가 최근 수주 동안 각각 1만 개의...

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-25T23:05:22+09:00`
- Company: [[KRX_010130_고려아연]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-25.json`
- Latest observation title: `의결권 자문 ISS, 고려아연 추천 백인규 감사위원 후보 찬성 권고 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-25T13:48:42+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBCUkZfOEtBZWQ0bmdWNjR5eGoyTFNLTldkRDV0M3FNTFpLNGo2RmlmNVFZWVdWNEotSkhXYUJjQ01QbmR5OWRKTElMckx5dzQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
