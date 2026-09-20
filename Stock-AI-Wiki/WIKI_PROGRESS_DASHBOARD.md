---
id: stock-ai-wiki-progress-dashboard
type: dashboard
title: Stock AI Wiki Progress Dashboard
created: 2026-08-31
updated: 2026-08-31
status: active
stage: dashboard

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated progress dashboard
---

# Stock AI Wiki Progress Dashboard

Generated at: `2026-08-31T10:12:30+09:00`

## Stage Progress

| Stage | Status | Evidence |
|---|---|---|
| 1. 문서 갱신 | DONE | 자동 링크 블록 5462개 |
| 2. 새 개념 문서 | DONE | 개념 후보 9개 |
| 3. 관련 링크 연결 | DONE | 회사/개념/뉴스/검증/소스 연결 |
| 4. 본문 기반 검증 | PARTIAL | 본문 있음 860 / 없음 7 |
| 5. Shadow Review | BLOCKED | 검증 완료 소스 필요 |
| 6. Paper Review | BLOCKED | Shadow 결과 필요 |

## Current Counts

| Item | Count |
|---|---:|
| Company notes | 318 |
| Concept candidate notes | 9 |
| Source notes | 1291 |
| Verification notes | 1291 |
| News notes | 1291 |
| Blocked thesis notes | 1291 |
| Auto-linked notes | 5462 |
| Article archive files | 88 |
| Archived article metadata rows | 867 |
| Article bodies available | 860 |
| Article bodies missing | 7 |
| Verification unknown notes | 1291 |
| Verification passed notes | 0 |
| Trading approved notes | 0 |
| Execution allowed notes | 0 |

## Automation Status

| Item | Value |
|---|---|
| Scheduler task | `VIBE_Stock_AI_Wiki_Update` |
| Scheduler info status | `available` |
| Last run time | `2026-08-31T10:05:01` |
| Last task result | `267009` |
| Next run time | `2026-08-31T11:05:00` |
| Missed runs | `0` |

## Data Freshness

| Item | Value |
|---|---|
| Dashboard generated at | `2026-08-31T10:12:30+09:00` |
| Latest RSS probe generated at | `2026-08-31T10:05:08+09:00` |
| Latest coverage generated at | `2026-08-31T10:05:25+09:00` |
| Freshness status | `CURRENT` |
| RSS probe quality | `PASS` |
| Coverage quality | `PASS` |

## Latest Observation

| Item | Value |
|---|---|
| Latest probe codes | `001820`, `002990`, `003350`, `006110`, `018000`, `024840`, `086450`, `126730`, `204620`, `222800` |
| RSS symbol count | 10 |
| RSS item count total | 956 |
| Coverage reference ymd | `20260828` |
| Coverage rows | 11 |
| Google RSS covered | 10 |
| KIS title covered | 10 |
| Naver covered | 8 |

## Evidence Boundary

| Evidence type | Count | Meaning |
|---|---:|---|
| RSS metadata rows | 956 | Headlines, links, timestamps, and sources only |
| Article archive metadata rows | 867 | Local archive seed rows |
| Article bodies available | 860 | Body-backed source verification input |
| Article bodies missing | 7 | Verification remains blocked |

## Main Blockers

- Article body text is not archived yet.
- Source verification remains `unknown`.
- Stage 5 and Stage 6 remain blocked.
- Trading approval and execution approval remain `false`.

## Stage 4 Waiting Reason

- `verification_status: verified` notes: `0`
- `original_text_available: true` notes: `163`
- Trading approved notes: `0`
- Execution allowed notes: `0`
- Shadow Decision remains blocked until body-backed verification and review queue records exist.

## Company Hubs

- [[KRX_000090_에임드바이오]]
- [[KRX_000104_CJ4우-전환]]
- [[KRX_000250_삼천당제약]]
- [[KRX_000660_nan]]
- [[KRX_000660_SK하이닉스]]
- [[KRX_000720_KB제33호스팩]]
- [[KRX_000720_현대건설]]
- [[KRX_000810_nan]]
- [[KRX_000810_삼성화재]]
- [[KRX_000990_미래에셋비전스팩11호]]
- [[KRX_001510_SK증권]]
- [[KRX_001740_SK네트웍스]]
- [[KRX_001820_삼화콘덴서]]
- [[KRX_001820_삼화콘덴서공업]]
- [[KRX_002630_nan]]
- [[KRX_002700_신일전자]]
- [[KRX_002990_금호건설]]
- [[KRX_003000_부광약품]]
- [[KRX_003230_삼양식품]]
- [[KRX_003280_흥아해운]]
- [[KRX_003350_한국화장품제조]]
- [[KRX_003550_LG]]
- [[KRX_003570_SNT다이내믹스]]
- [[KRX_004000_롯데정밀화학]]
- [[KRX_004310_현대약품]]
- [[KRX_004560_현대비앤지스틸]]
- [[KRX_005490_POSCO홀딩스]]
- [[KRX_005680_삼영전자공업]]
- [[KRX_005690_파미셀]]
- [[KRX_005930_nan]]
- [[KRX_005930_삼성전자]]
- [[KRX_006110_삼아알미늄]]
- [[KRX_006360_GS건설]]
- [[KRX_006660_삼성공조]]
- [[KRX_006800_nan]]
- [[KRX_006800_미래에셋증권]]
- [[KRX_007390_네이처셀]]
- [[KRX_007610_선도전기]]
- [[KRX_007660_이수페타시스]]
- [[KRX_008260_NI스틸]]
- [[KRX_008490_서흥]]
- [[KRX_008830_대동기어]]
- [[KRX_008930_한미사이언스]]
- [[KRX_009150_삼성전기]]
- [[KRX_009420_한올바이오파마]]
- [[KRX_009540_HD한국조선해양]]
- [[KRX_009830_한화솔루션]]
- [[KRX_010060_OCI홀딩스]]
- [[KRX_010120_엘에스일렉트릭]]
- [[KRX_010130_고려아연]]
- [[KRX_010770_평화홀딩스]]
- [[KRX_011070_LG이노텍]]
- [[KRX_011200_HMM]]
- [[KRX_011210_현대위아]]
- [[KRX_011700_한신기계공업]]
- [[KRX_011930_신성이엔지]]
- [[KRX_012210_삼미금속]]
- [[KRX_012330_현대모비스]]
- [[KRX_012340_뉴인텍]]
- [[KRX_014470_부방]]
- [[KRX_014620_성광벤드]]
- [[KRX_014680_한솔케미칼]]
- [[KRX_014950_삼익제약]]
- [[KRX_016360_삼성증권]]
- [[KRX_016380_KG스틸]]
- [[KRX_016610_DB증권]]
- [[KRX_017670_nan]]
- [[KRX_017800_현대엘리베이터]]
- [[KRX_017900_광전자]]
- [[KRX_018000_유니슨]]
- [[KRX_018250_애경산업]]
- [[KRX_018260_삼성에스디에스]]
- [[KRX_018310_삼목에스폼]]
- [[KRX_018620_nan]]
- [[KRX_018670_SK가스]]
- [[KRX_024060_흥구석유]]
- [[KRX_024740_한일단조]]
- [[KRX_024840_KBI메탈]]
- [[KRX_025620_차AI헬스케어]]
- [[KRX_026940_부국철강]]
- [[KRX_026960_동서]]
- [[KRX_027360_아주IB투자]]
- [[KRX_028260_삼성물산]]
- [[KRX_028300_HLB]]
- [[KRX_028670_팬오션]]
- [[KRX_032640_LG유플러스]]
- [[KRX_032820_우리기술]]
- [[KRX_032830_nan]]
- [[KRX_032830_삼성생명]]
- [[KRX_033100_제룡전기]]
- [[KRX_034020_두산에너빌리티]]
- [[KRX_034220_LG디스플레이]]
- [[KRX_034730_SK]]
- [[KRX_035250_강원랜드]]
- [[KRX_036170_에이치엠넥스]]
- [[KRX_036620_감성코퍼레이션]]
- [[KRX_036930_주성엔지니어링]]
- [[KRX_037030_파워넷]]
- [[KRX_037440_희림]]
- [[KRX_039030_이오테크닉스]]
- [[KRX_039200_오스코텍]]
- [[KRX_039490_키움증권]]
- [[KRX_039860_나노엔텍]]
- [[KRX_039980_폴라리스AI]]
- [[KRX_040350_크레오에스지]]
- [[KRX_041190_우리기술투자]]
- [[KRX_041830_인바디]]
- [[KRX_041920_메디아나]]
- [[KRX_042660_한화오션]]
- [[KRX_042700_한미반도체]]
- [[KRX_043260_성호전자]]
- [[KRX_045340_토탈소프트]]
- [[KRX_046390_삼화네트웍스]]
- [[KRX_047040_대우건설]]
- [[KRX_047770_코데즈컴바인]]
- [[KRX_048410_현대바이오]]
- [[KRX_048530_인트론바이오]]
- [[KRX_049960_쎌바이오텍]]
- [[KRX_051900_LG생활건강]]
- [[KRX_051910_LG화학]]
- [[KRX_052690_한전기술]]
- [[KRX_052710_아모텍]]
- [[KRX_052900_KX하이텍]]
- [[KRX_053260_금강철강]]
- [[KRX_053280_예스24]]
- [[KRX_053690_한미글로벌]]
- [[KRX_056080_유진로봇]]
- [[KRX_056360_코위버]]
- [[KRX_058430_포스코스틸리온]]
- [[KRX_058470_리노공업]]
- [[KRX_058610_에스피지]]
- [[KRX_058650_세아홀딩스]]
- [[KRX_058970_엠로]]
- [[KRX_059100_아이컴포넌트]]
- [[KRX_059120_아진엑스텍]]
- [[KRX_060980_HL홀딩스]]
- [[KRX_062040_산일전기]]
- [[KRX_062970_한국첨단소재]]
- [[KRX_063160_종근당바이오]]
- [[KRX_064260_다날]]
- [[KRX_064290_인텍플러스]]
- [[KRX_064400_LG씨엔에스]]
- [[KRX_064550_바이오니아]]
- [[KRX_065510_nan]]
- [[KRX_066310_큐에스아이]]
- [[KRX_066430_아이로보틱스]]
- [[KRX_066570_LG전자]]
- [[KRX_066980_한성크린텍]]
- [[KRX_067160_SOOP]]
- [[KRX_067290_JW신약]]
- [[KRX_068270_셀트리온]]
- [[KRX_069260_티케이지휴켐스]]
- [[KRX_069540_빛과전자]]
- [[KRX_069960_현대백화점]]
- [[KRX_071050_한국금융지주]]
- [[KRX_073490_이노와이어리스]]
- [[KRX_078150_HB테크놀러지]]
- [[KRX_078340_컴투스]]
- [[KRX_078350_한양디지텍]]
- [[KRX_078930_GS]]
- [[KRX_079190_케스피온]]
- [[KRX_079370_제우스]]
- [[KRX_079550_LIG넥스원]]
- [[KRX_079900_전진건설로봇]]
- [[KRX_080220_제주반도체]]
- [[KRX_081180_쎄크]]
- [[KRX_081660_미스토홀딩스]]
- [[KRX_082640_동양생명]]
- [[KRX_083650_비에이치아이]]
- [[KRX_084370_유진테크]]
- [[KRX_085620_미래에셋생명]]
- [[KRX_085660_차바이오텍]]
- [[KRX_086450_동국제약]]
- [[KRX_086520_에코프로]]
- [[KRX_086900_메디톡스]]
- [[KRX_087010_펩트론]]
- [[KRX_089010_켐트로닉스]]
- [[KRX_089140_넥스턴앤롤코리아]]
- [[KRX_089890_코세스]]
- [[KRX_089970_nan]]
- [[KRX_090710_휴림로봇]]
- [[KRX_091590_남화토건]]
- [[KRX_091970_나노캠텍]]
- [[KRX_093370_후성]]
- [[KRX_094170_동운아나텍]]
- [[KRX_094480_갤럭시아머니트리]]
- [[KRX_095340_ISC]]
- [[KRX_095610_테스]]
- [[KRX_096250_와이즈넛]]
- [[KRX_096530_씨젠]]
- [[KRX_096770_SK이노베이션]]
- [[KRX_097780_에코볼트]]
- [[KRX_097950_CJ제일제당]]
- [[KRX_099430_바이오플러스]]
- [[KRX_100840_SNT에너지]]
- [[KRX_101730_위메이드맥스]]
- [[KRX_101970_nan]]
- [[KRX_102710_이엔에프테크놀로지]]
- [[KRX_103140_풍산]]
- [[KRX_105560_KB금융]]
- [[KRX_105840_우진]]
- [[KRX_107640_한중엔시에스]]
- [[KRX_108230_톱텍]]
- [[KRX_108490_로보티즈]]
- [[KRX_108860_셀바스AI]]
- [[KRX_112290_와이씨켐]]
- [[KRX_115500_케이씨에스]]
- [[KRX_117730_티로보틱스]]
- [[KRX_119850_지엔씨에너지]]
- [[KRX_121440_골프존홀딩스]]
- [[KRX_122640_예스티]]
- [[KRX_123410_코리아에프티]]
- [[KRX_123420_위메이드플레이]]
- [[KRX_124500_아이티센글로벌]]
- [[KRX_125490_한라캐스트]]
- [[KRX_126730_코칩]]
- [[KRX_128820_대성산업]]
- [[KRX_128940_한미약품]]
- [[KRX_130660_한전산업]]
- [[KRX_131290_티에스이]]
- [[KRX_131760_파인텍]]
- [[KRX_137400_피엔티]]
- [[KRX_138360_협진]]
- [[KRX_138610_나이벡]]
- [[KRX_140860_파크시스템스]]
- [[KRX_142280_녹십자엠에스]]
- [[KRX_144960_뉴파워프라즈마]]
- [[KRX_147760_피엠티]]
- [[KRX_149950_아바텍]]
- [[KRX_161890_한국콜마]]
- [[KRX_168330_내츄럴엔도텍]]
- [[KRX_174900_앱클론]]
- [[KRX_177350_베셀]]
- [[KRX_179530_애드바이오텍]]
- [[KRX_183300_코미코]]
- [[KRX_185490_아이진]]
- [[KRX_187790_나노]]
- [[KRX_192820_코스맥스]]
- [[KRX_195870_해성디에스]]
- [[KRX_196170_알테오젠]]
- [[KRX_204620_글로벌텍스프리]]
- [[KRX_205500_넥써쓰]]
- [[KRX_207940_삼성바이오로직스]]
- [[KRX_208640_썸에이지]]
- [[KRX_210980_SK디앤디]]
- [[KRX_212710_아이에스티이]]
- [[KRX_214330_금호에이치티]]
- [[KRX_214370_케어젠]]
- [[KRX_214450_파마리서치]]
- [[KRX_215600_신라젠]]
- [[KRX_222040_코스맥스엔비티]]
- [[KRX_222800_심텍]]
- [[KRX_226340_본느]]
- [[KRX_229000_젠큐릭스]]
- [[KRX_232140_와이씨]]
- [[KRX_234340_헥토파이낸셜]]
- [[KRX_240810_원익IPS]]
- [[KRX_244920_에이플러스에셋]]
- [[KRX_251270_넷마블]]
- [[KRX_251970_펌텍코리아]]
- [[KRX_252990_샘씨엔에스]]
- [[KRX_263750_펄어비스]]
- [[KRX_263860_지니언스]]
- [[KRX_264850_이랜시스]]
- [[KRX_267260_HD현대일렉트릭]]
- [[KRX_272210_한화시스템]]
- [[KRX_277810_레인보우로보틱스]]
- [[KRX_278470_에이피알]]
- [[KRX_289080_SV인베스트먼트]]
- [[KRX_293580_나우IB]]
- [[KRX_298830_슈어소프트테크]]
- [[KRX_299660_셀리드]]
- [[KRX_304100_솔트룩스]]
- [[KRX_306040_에스제이그룹]]
- [[KRX_307180_아이엘]]
- [[KRX_307930_컴퍼니케이]]
- [[KRX_309930_조이웍스앤코]]
- [[KRX_316140_우리금융지주]]
- [[KRX_319660_피에스케이]]
- [[KRX_321260_프로이천]]
- [[KRX_322000_HD현대에너지솔루션]]
- [[KRX_323280_태성]]
- [[KRX_327260_RF머트리얼즈]]
- [[KRX_336260_두산퓨얼셀]]
- [[KRX_336370_솔루스첨단소재]]
- [[KRX_347700_스피어]]
- [[KRX_347850_디앤디파마텍]]
- [[KRX_347860_알체라]]
- [[KRX_356860_티엘비]]
- [[KRX_357880_SKAI]]
- [[KRX_358570_지아이이노베이션]]
- [[KRX_373220_LG에너지솔루션]]
- [[KRX_377300_카카오페이]]
- [[KRX_382800_지앤비에스-에코]]
- [[KRX_383800_LX홀딩스]]
- [[KRX_388720_유일로보틱스]]
- [[KRX_389470_인벤티지랩]]
- [[KRX_403870_HPSP]]
- [[KRX_417860_오브젠]]
- [[KRX_424870_이뮨온시아]]
- [[KRX_432470_케이엔에스]]
- [[KRX_437730_삼현]]
- [[KRX_445180_퓨릿]]
- [[KRX_452190_한빛레이저]]
- [[KRX_453340_현대그린푸드]]
- [[KRX_454910_두산로보틱스]]
- [[KRX_457370_한켐]]
- [[KRX_462860_더즌]]
- [[KRX_466100_클로봇]]
- [[KRX_474650_링크솔루션]]
- [[KRX_475830_오름테라퓨틱]]
- [[KRX_476060_온코닉테라퓨틱스]]
- [[KRX_476830_알지노믹스]]
- [[KRX_900250_크리스탈신소재]]
- [[KRX_900270_헝셩그룹]]
- [[KRX_900290_GRT]]
- [[KRX_900300_오가닉티코스메틱]]
- [[KRX_950250_테라뷰]]

## Concept Candidates

- [[concept_bio_바이오]]
- [[concept_earnings_실적]]
- [[concept_eco-packaging_친환경-패키징]]
- [[concept_exports_수출]]
- [[concept_gas-energy_가스-에너지]]
- [[concept_holding-company_지주회사]]
- [[concept_medical-cooperation_의료-협진]]
- [[concept_robotics_로봇]]
- [[concept_unclassified-news_미분류-뉴스]]

## Visual Navigation

- Use Obsidian Graph View from this note to see company, concept, news, source, verification, and thesis links.
- Use the counts above to check whether the wiki is still metadata-only or has moved into body-backed verification.

## Safety Boundary

- This dashboard is a read-only progress view.
- It does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
