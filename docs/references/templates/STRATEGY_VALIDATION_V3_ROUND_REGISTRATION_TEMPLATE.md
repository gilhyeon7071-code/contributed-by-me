# V3 연구 검증 라운드 등록표

> 상태: **TEMPLATE — 작성·해시 고정 후에만 탐색 또는 확증을 실행한다.**
>
> 이 문서는 현재 V2 가드 또는 운영 Gate를 대체하지 않는다.

## A. 식별·질문

- `round_id`:
- 작성 시각 (KST / UTC):
- 작성자:
- 라운드 종류: `EXPLORATION` / `CONFIRMATION`
- 검증 층: `SIGNAL_VALIDITY` / `CANDIDATE_SELECTION` / `STRATEGY_EXECUTION`
- 한 문장 질문:
- 현재 V3 `protocol_version`:
- 등록표 SHA-256 (동결 후 기록):

## B. 독립성 선언과 신호 후보 사전등록

- 기존 RS·v_accel·final score·Gate 성과를 보고 후보·방향·임계값을 정했는가: `NO` / `YES — LEGACY_CONTROL_ONLY로 격리`
- `feature_dictionary_id`:
- 신호 후보 목록 파일 경로:
- 신호 후보 목록 SHA-256:
- 동결 시각:

| signal_id | 원천 입력 | 계산식·방향 가설 | 경제적 근거·최초 제안 출처 | LEGACY_ADJACENCY | 독립성 근거 | 자동 중복 확인 |
|---|---|---|---|---|---|---|
| | | | | `NONE` / `SIMILAR_BUT_INDEPENDENT` / `LEGACY_REDUNDANT_OR_REVIEW` / `LEGACY_DERIVED` | | |

규칙: `LEGACY_DERIVED`는 발견 입력으로 사용 금지. `LEGACY_REDUNDANT_OR_REVIEW`는 독립 추가 기여 또는 독립 후보군 승격 주장 금지. 상관이 높다는 사실만으로 `LEGACY_DERIVED`로 분류하지 않는다.

### B.1 자동 레거시 상관 보강 검사

- `legacy_corroboration_id`:
- 계산 시점: 등록표 동결 직후, 미래수익 열람 전
- 비교 대상 `LEGACY_CONTROL_ONLY` 목록 및 스냅샷 ID:
- 같은 날짜·같은 적격 모집단 확인: `YES` / `NO`
- 사전등록 임계값: 일별 횡단면 Pearson 중앙값 | |, Spearman 중앙값 | |, Top-K 분위 중복률 | |
- 검사 코드 해시:
- 결과물 경로·SHA-256:

| signal_id | legacy_key | Pearson 중앙값 | Spearman 중앙값 | Top-K 중복률 | 임계 초과 | 자동 상태 |
|---|---|---:|---:|---:|---|---|
| | | | | | `YES` / `NO` | `NONE` / `LEGACY_REDUNDANT_OR_REVIEW` |

규칙: 임계값이 비어 있으면 라운드 등록은 유효하지 않다. 임계 초과는 `LEGACY_REDUNDANT_OR_REVIEW`를 자동 부여하며, 출처 증거가 있을 때만 별도로 `LEGACY_DERIVED`를 부여한다.

## C. RESEARCH_DERIVED 파라미터 등록

- `parameter_registry_id`:

| parameter_id | 값 | 적용 위치 | 허용 출처 | 출처 설명·문헌·규칙 참조 | legacy-derived 여부 |
|---|---:|---|---|---|---|
| | | | `MARKET_STRUCTURE` / `ACADEMIC_OR_PUBLIC_METHOD` / `PRE_REGISTERED_DESIGN` / `RAW_DATA_CONSTRAINT` | | `NO` / `YES` |

규칙: `YES`는 독립 발견용 파라미터로 사용 금지. Train 결과 뒤의 값 변경은 새 round_id로 분리.

## D. 모집단·실행 계약

- 시장·종목유형:
- 데이터 스냅샷 ID·as-of:
- 상장/상폐/거래정지 처리:
- 유동성·가격·거래대금 제외 규칙:
- `PRICE_HISTORY_INTEGRITY_V1` 적용 확인: `YES` / `NO`
- 신호 시점 / 진입 / 보유 / 청산:
- 비용·슬리피지·세금:
- 후보 수 상한·동점·후보 없음·후보 과다 처리:
- 동일가중 바스켓 규칙:
- 코드 해시:

## E. 기준선·가설·조합

- 주 기준선: `SAME_DATE_SAME_ELIGIBLE_UNIVERSE_EQUAL_WEIGHT_NET_RETURN`
- 보조 기준선: `RANDOM_SAME_SIZE_RESAMPLE` / `CAP_WEIGHTED_MARKET_EXPOSURE_CHECK` / 해당 없음
- 주 지표와 단위:
- 보조 지표:
- 사전등록 조합 목록:
- 다중검정 통제(CPCV/PBO/DSR/Plateau/비용 민감도):
- 최소 고유 신호일·달력 기간·비연속 에피소드:
- `DEFERRED_INSUFFICIENT_SAMPLE` 규칙:

## F. 시간 분할·봉인 OOS·관측 이력

- `batch_id`:
- 개발 Walk-Forward: `DEV-WF-1` / `DEV-WF-2` / `DEV-WF-3` / 새 protocol_version 사전등록
- 사전등록한 적용 대상 DEV-WF와 각 Train / Validation 실제 거래일 경계:
- `SEALED-OOS` 구간 및 실제 거래일 경계 (관측 이력 감사 적격 판정 후에만 기입):
- 관측 이력 감사 결과물 경로·SHA-256:
- SEALED-OOS 적격 판정: `UNSEEN_ELIGIBLE` / `NOT_ELIGIBLE_QUARANTINED` / `UNVERIFIED_NOT_ELIGIBLE`
- SEALED-OOS 사전 관측 여부: `UNSEEN` / `BATCH_SEALED_NOT_VIEWED` / `OBSERVED` / `QUARANTINED_OBSERVED` / `QUARANTINED_PRIOR_RESEARCH_ARTIFACT` / `UNVERIFIED_NO_SCANNED_EVIDENCE`
- purge / embargo:
- 레짐 또는 시계열 파생값 truncate-replay 대상·판정:
- 2026년 부분 기간 사용 여부: 최근성 관찰만 / 해당 없음

| 달력 구간 | round_id | batch_id | 역할 | 최초 계산 시각 | 최초 결과 열람 시각 | 결과물 SHA-256 | 관측 상태 |
|---|---|---|---|---|---|---|---|
| | | | `TRAIN` / `VALIDATION` / `SEALED_OOS` / `EXPLORATION_ONLY` | | | | `UNSEEN` / `BATCH_SEALED_NOT_VIEWED` / `OBSERVED` / `QUARANTINED_OBSERVED` / `QUARANTINED_PRIOR_RESEARCH_ARTIFACT` / `UNVERIFIED_NO_SCANNED_EVIDENCE` |

규칙: `OBSERVED` 또는 `QUARANTINED_*` 구간은 새 가설의 blind Validation·SEALED OOS·확증 근거로 재사용할 수 없다. `UNVERIFIED_NO_SCANNED_EVIDENCE`도 blind 구간으로 선언할 수 없다. 하나의 가설의 DEV-WF는 결과 공개 전 하나의 batch로 동시 실행한다.

## G. 상태 전이 증거

| 목표 상태 | 필요한 증거 | 실제 산출물 경로·SHA-256 | 판정 |
|---|---|---|---|
| SCREENED | 등록표·신호 유효성 결과·코드/데이터 해시 일치 | | |
| CANDIDATE | SCREENED + 후보 선별 Train 결과 | | |
| REPRODUCIBLE | CANDIDATE + 모든 적용 대상 DEV-WF + 관측 이력 감사에서 UNSEEN_ELIGIBLE인 SEALED OOS + 다중검정 증거 | | |
| PAPER_READY | REPRODUCIBLE + 별도 승인 기록 + 사용자 명시 승인 | | |

- DEV-WF batch 전체 결과 공개 전 봉인 해시:
- 실행 로그 경로·SHA-256:
- 결과물 경로·SHA-256:
- SEALED OOS 관측 이력 증거 경로·SHA-256:
- 이전 round_id와의 관계:
- 상태: `SCREENED` / `CANDIDATE` / `REPRODUCIBLE` / `PAPER_READY` / `DEFERRED_INSUFFICIENT_SAMPLE` / `NOT_SUPPORTED`
- PAPER_READY 승인 기록 및 사용자 승인: 해당 없음 / 경로·시각

## H. 운영 분리 선언

이 라운드는 Gate, LOCK, 주문, 체결, 원장, 브로커, 운영 paper runtime을 변경하지 않는다: `YES` / `NO — 별도 승인 필요`

## I. 동결 확인

- 동결 전 누락 항목 확인:
- 동결 시각:
- 동결 후 문서 SHA-256:
- 이후 수정 발생 시 새 round_id 사용 확인: `YES`
