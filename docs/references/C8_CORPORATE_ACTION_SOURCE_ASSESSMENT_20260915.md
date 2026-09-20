# C8 corporate action official-source assessment

검증일: 2026-09-15 KST

## 1. 고정 범위

- 전략: `KOSPI_MCAP_QUARTERLY_V1`
- 대상 역할: `corporate_actions_as_of`
- 필요한 canonical 필드: `merger_status`, `event_type`, `event_effective_date`
- 검증 성격: `NA_SOURCE_CONTRACT_RESEARCH`
- 방향 변경: 없음
- 실제 API 수집, raw 파일 생성, adapter 요청, canonical 발행, M2, 후보, 주문: 수행하지 않음
- 정책 및 계약 JSON 변경: 없음

## 2. 현재 계약 요구사항

현재 상태 재구성 계약은 다음을 동시에 요구한다.

1. 모든 base-universe code를 포괄할 것
2. point-in-time 예외 또는 interval history가 누락 없이 공식 원천으로 증명될 것
3. interval history는 `query_start <= earliest listed_date`, `query_end == selection_as_of`일 것
4. sparse row 부재를 `NONE`으로 바꾸려면 exhaustive coverage와 provenance가 모두 PASS일 것
5. `merger_status`는 `NONE`, `PENDING`, `EFFECTIVE`만 허용할 것

현재 공식 source route 계약의 차단 사유는 다음과 같다.

`no exhaustive revision-aware pending-to-effective merger lifecycle route has been confirmed`

## 3. 확인한 공식 원천

| source | official endpoint or page | 확인된 기능 | 계약상 한계 |
|---|---|---|---|
| OpenDART 공시검색 | `https://opendart.fss.or.kr/api/list.json` | 접수일 범위, 법인구분, 상세유형, 6자리 종목코드, 보고서명, 접수번호, 접수일, 정정·철회 비고를 조회한다. `last_reprt_at=N`은 정정보고서를 포함한 제출 보고서 전체를 반환한다. | 회사 고유번호 없이 한 번에 조회할 수 있는 기간은 3개월이다. 각 접수번호가 같은 합병 사건의 어느 revision인지 직접 나타내는 event key는 없다. |
| OpenDART 회사합병 결정 | `https://opendart.fss.or.kr/api/cmpMgDecsn.json` | 회사별 회사합병 결정과 합병계약일, 주주총회예정일, 합병기일, 종료보고 총회일, 합병등기예정일, 신주상장예정일을 구조화해 제공한다. | 검색일은 최초접수일 기준이고 2015년 이후만 제공한다. 구조화 자료는 제출 공시 일부의 추출값이므로 정정·취소·실제 완료를 단독으로 확정할 수 없다. |
| OpenDART 공시서류 원본 | `https://opendart.fss.or.kr/api/document.xml` | 접수번호별 원문 ZIP을 제공한다. 정정 연혁, 취소 문구, 일정 삭제, 종료 문구를 원문에서 확인할 수 있다. | 원문 parsing과 동일 사건 revision 연결 규칙이 별도로 필요하다. |
| OpenDART 공시상세유형 | 공시검색 문서의 `E003` | `합병등종료보고서`를 별도 상세유형으로 찾을 수 있다. | 이번 조사에서 E003의 실제 효력일을 직접 제공하는 전용 구조화 endpoint는 확인하지 못했다. 원문 확인이 필요하다. |
| KRX KIND 정정 공시 | KRX 공식 공시 원문 | 최초 제출 뒤 여러 차례 정정되는 실제 연혁과 예정 합병기일 변경 가능성을 확인했다. | 한 사례의 존재 증거이며 exhaustive collection route는 아니다. |
| KRX KIND 취소 공시 | KRX 공식 공시 원문 | 합병결정 취소 시 이전 합병 일정이 삭제되고 계약·이사회 결의가 취소되는 실제 상태 전이를 확인했다. | 취소를 자동 분류하는 안정된 구조화 필드는 확인하지 못했다. |

OpenDART 공식 안내는 주요사항보고서 주요정보가 제출 공시의 일부정보를 추출한 것이며 정확성과 완전성을 보장하지 않는다고 명시한다. 따라서 `cmpMgDecsn`만으로 `NONE` 기본값을 공급하거나 실제 완료를 확정할 수 없다.

## 4. 계약 적합성 판정

| requirement | active evidence | conflict or omission | worst case | verdict |
|---|---|---|---|---|
| 공식 권한 | OpenDART와 KIND는 금융감독원·한국거래소 공식 공시 원천이다. | 없음 | 비공식 재가공 자료를 공식 원천으로 오인 | PASS |
| 합병결정 발견 | `list.json`과 `cmpMgDecsn`으로 결정 공시를 발견할 수 있다. | 전자는 상세 event key가 없고 후자는 회사별·최초접수일 기준이다. | 조회 시작 전 최초 제출된 진행 중 합병을 누락 | PARTIAL |
| revision 발견 | `last_reprt_at=N`, `report_nm`, `rm`으로 정정·철회 존재를 알 수 있다. | 동일 사건의 revision chain을 결정적으로 연결하는 공식 event key가 확인되지 않았다. | 다른 합병 사건의 정정을 잘못 덮어씀 | FAIL |
| 예정 효력일 | `mgsc_mgdt` 등 일정 필드가 있다. | 일정은 정정될 수 있는 예정값이다. | 과거 예정일을 실제 효력일로 오인 | PARTIAL |
| 취소 전이 | KIND 원문에서 취소와 일정 삭제가 확인된다. | 구조화 endpoint만으로 취소를 확정하는 규칙이 없다. | 취소된 합병을 계속 `PENDING`으로 유지 | PARTIAL |
| 실제 완료 전이 | 공시검색 상세유형 `E003`이 존재하고 원문 다운로드가 가능하다. | E003와 최초 결정의 사건 연결, 실제 효력일의 권위 필드, `EFFECTIVE` 종료 시점이 미정이다. | 완료 전 예정일에 `EFFECTIVE`로 선반영하거나 영구 차단 | FAIL |
| 전 기간 포괄 | 공시검색은 접수일 범위 조회가 가능하다. | 구조화 회사합병 자료는 2015년 이후이고 현재 계약은 earliest listed date 이전부터를 요구한다. | 2015년 이전 시작 사건 또는 장기 미종결 사건 누락 | FAIL |
| 전체 universe의 NONE 증명 | 6자리 `stock_code`와 `corp_code`를 얻을 수 있다. | 모든 base-universe code에 대해 전 기간·전 revision·종료를 포괄했다는 수집 증거가 없다. | 미수집 종목을 사건 없음으로 간주 | FAIL |

## 5. 확인된 상태 전이와 미결정 사항

공식 자료가 보여 준 최소 상태 전이는 다음과 같다.

1. 최초 합병결정 공시가 제출된다.
2. 여러 정정 공시가 합병비율과 예정 일정을 바꿀 수 있다.
3. 취소 정정은 이전 일정을 삭제하고 합병결정을 종료한다.
4. 합병등종료보고서는 완료 확인 후보지만, 현재 계약의 실제 `EFFECTIVE` 시작·종료 의미는 정해지지 않았다.

다음 네 항목은 데이터 mapping이 아니라 정책·계약 결정이 필요하다.

1. `event_type` 범위를 회사합병만으로 제한할지, 분할합병·주식교환까지 포함할지
2. 서로 다른 접수번호를 같은 합병 사건으로 묶는 공식 identity 규칙
3. `EFFECTIVE` 시작일의 권한을 합병기일, 종료보고 접수일, 등기일 중 무엇으로 둘지
4. 2015년 이전 공백을 별도 공식 history로 채울지, 현재 earliest-listing 요구를 변경할지

## 6. 최종 판단

- 공식 source 후보: 확인됨
- 단일 source로 계약 충족: 불가
- 복합 source 체인 구현 가능성: 있음
- 현재 계약에 즉시 반영 가능: 불가
- `CORPORATE_ACTION_SOURCE_UNRESOLVED`: 유지
- manual collection readiness: 계속 BLOCKED
- M2·후보·주문 권한: 계속 false

정확한 표현은 `공식 원천 부재`가 아니라 `공식 복합 원천 후보는 확인됐지만 revision-aware lifecycle 계약이 미완성`이다.

## 7. 공식 근거 URL

- 공시검색 개발가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
- 회사합병 결정 개발가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS005&apiId=2020050
- 공시서류 원본파일 개발가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003
- 주요사항보고서 API 목록: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005
- KRX KIND 다중 정정 사례: https://kind.krx.co.kr/external/2025/06/04/000081/20250604000206/11344.htm
- KRX KIND 합병 취소 사례: https://kind.krx.co.kr/external/2024/05/17/000455/20240517001013/11344.htm

