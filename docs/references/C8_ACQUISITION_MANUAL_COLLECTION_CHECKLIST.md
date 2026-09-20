# C8 Acquisition Manual Collection Checklist

## Current Verdict

- Status: `BLOCKED_C8_MANUAL_COLLECTION`
- This checklist does not authorize network collection, descriptor finalization, canonical publication, M2,
  candidate selection, or orders.
- Selection-date retrieval sources may only be collected on `selection_as_of` at or after 15:40 KST.

## Required Inputs

| Order | Role | Source | Mode | Required input | Current state |
|---:|---|---|---|---|---|
| 1 | Listing | `KRX_MDCSTAT019_CURRENT_BASIC_INFO` | Same-day retrieval | KOSPI current basic information | Conditional: exact official transform profile required |
| 2 | Trade status | `KRX_MDCSTAT213_SUSPENSION_HISTORY` | Exhaustive interval | Every base-universe code from earliest listing date through selection date | Conditional: exhaustive traversal required |
| 3 | Management status | `KRX_MDCSTAT215_MANAGEMENT_HISTORY` | Exhaustive interval | Every base-universe code from earliest listing date through selection date | Conditional: exhaustive traversal required |
| 4 | Liquidation status | `KRX_MDCSTAT237_LIQUIDATION_CURRENT` | Same-day retrieval | KOSPI current liquidation exceptions | Conditional: exact official transform profile required; date authority stays in acquisition evidence |
| 5 | Corporate actions | Unresolved | Unresolved | Revision-aware pending-to-effective merger lifecycle | Blocked: official source not confirmed |
| 6 | Close and market cap | `KRX_OPENAPI_STK_BYDD_TRD` | Full query-date snapshot | `basDd=selection_as_of` response | Conditional: authenticated OpenAPI preflight; documentation host `openapi.krx.co.kr`, download host `data-dbg.krx.co.kr` |

## Descriptor Fields

Every source entry needs `role`, `source_id`, `temporal_mode`, `source_path`, `method`,
`official_source_id`, `source_page_url`, `download_url`, `query_parameters`, `date_parameter`, and
`as_of_date`.

Retrieval sources additionally need `covered_fields`, `authenticated_session_verified=true`, `http_status=200`,
`http_response_timestamp` with timezone, and `historical_replay=false`.

Interval sources additionally need `covered_fields`, `query_start_parameter`, and `query_end_parameter`.

The status role coverage must use:

- mode: `MIXED_APPROVED_TEMPORAL_COMPONENTS`
- basis: `EXACT_APPROVED_MIXED_TEMPORAL_COMPONENTS`
- scope: `ALL_BASE_UNIVERSE_CODES`
- fields: `trade_status`, `management_status`, `delisting_procedure_status`, each owned exactly once

## Exact Transform Profiles

- `MDCSTAT019_ACTIVE_LISTING_MEMBERSHIP_V1`: map `ISU_SRT_CD`, `ISU_NM`, `MKT_TP_NM`,
  `KIND_STKCERT_TP_NM`, and `LIST_DD`; only mapped `KOSPI` and `보통주` values are accepted. Each captured
  row becomes `listing_status=ACTIVE`, while absence is never synthesized by the adapter.
- `MDCSTAT237_LIQUIDATION_MEMBERSHIP_V1`: map `ISU_CD`; each captured exception row becomes
  `delisting_procedure_status=PROCEDURE`. `NONE` is supplied only by complete mixed-state reconstruction.
- A profile ID, column map, constant, or value map mismatch must block component and build-request publication.

## Pre-Finalize Checks

1. Start the acquisition session with the exact `selection_as_of` before creating source files.
2. Confirm every source file was created after session start and before finalize.
3. Confirm every source `as_of_date` equals the session and descriptor selection date.
4. For same-day retrieval, confirm KST date, time at or after 15:40, authenticated session, HTTP 200, and response timestamp.
5. For interval history, confirm query start is not after the earliest listing date and query end equals selection date.
6. Confirm suspension and management traversal covers every exact base-universe code.
7. Confirm each source page and download URL uses the field-specific host allowed for the declared method. For `KRX_OPEN_API`, source pages use only `openapi.krx.co.kr` and downloads use only `data-dbg.krx.co.kr`.
8. Keep credentials, cookies, OTP values, authorization headers, and tokens out of the descriptor.
9. Do not manually enter raw hashes; the acquisition session must calculate them from exact bytes.
10. Do not finalize while any blocker below remains.

## Blocking Issues

- `CORPORATE_ACTION_SOURCE_UNRESOLVED`

Read-only readiness command:

```powershell
.\_runtime\python312-embed\python.exe tools\check_kospi_mcap_quarterly_c8_manual_collection.py --expect-verdict BLOCKED_C8_MANUAL_COLLECTION
```
