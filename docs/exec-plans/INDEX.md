# ExecPlan 상태 인덱스

- 생성: 2026-09-08 17:08 · `tools/build_execplan_index.py`
- active **77건** / completed **17건**
- 파일은 옮기지 않는다. 옮기면 PLANS 의 경로 참조가 깨지고, 한 번 옮긴 결과는 다시 낡는다

## 판정 분포

| 판정 | 건수 | 뜻 |
|---|---:|---|
| 초안(승인 전) | 4 | **진짜 대기 목록.** 여기부터 본다 |
| 적용완료 | 7 | 상태줄에 적용 완료가 적혀 있다 |
| 철회 | 2 | 상태줄에 철회/무효가 적혀 있다 |
| PLANS 참조됨 | 3 | 파일명이 PLANS 본문에 있다 |
| PLANS 유사언급 | 51 | 제목 키워드가 PLANS 에 있다. **적용됐다는 뜻이 아니다** |
| 미판정 | 10 | 파일에도 PLANS 에도 근거가 없다 |

월별 분포: {'202604': 22, '202605': 22, '202606': 5, '202607': 15, '202608': 13}

> **`PLANS 유사언급`을 완료로 읽지 말 것.** 논의 흔적이 있다는 뜻이고,
> 적용 여부는 코드를 봐야 안다. 이 인덱스는 그 구분을 흐리지 않는다.

## 초안(승인 전) — 4건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260819 | `20260819_surge_entry_reduction.md` | 초안. 사용자 승인 전이며 어떤 변경도 적용하지 않았다. |
| 20260820 | `20260820_entry_gate_premise_alignment.md` | 초안. 사용자 승인 전이며 어떤 변경도 적용하지 않았다. |
| 20260824 | `20260824_hard_block_exit_management.md` | 초안. 사용자 승인 전이며 어떤 변경도 적용하지 않았다. |
| 20260825 | `20260825_measurement_live_trades.md` | 초안. 사용자 승인 전이며 어떤 주문도 넣지 않았다. |

## 적용완료 — 7건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260820 | `20260820_certified_operational_split.md` | 적용됨 (2026-08-20). (상태줄 정정 2026-08-24 — 아래 근거) |
| 20260820 | `20260820_config_lock_enforcement.md` | 적용됨. (상태줄 정정 2026-08-24 — 아래 근거) |
| 20260820 | `20260820_final_score_axis_reduction.md` | 적용 완료 (2026-08-20). (상태줄 정정 2026-08-24 — "적용 진행"에서 완료로) |
| 20260820 | `20260820_rule_e_open_for_observation.md` | 적용 완료 (2026-08-20 16:38). 사전 측정 통과 후 반영. 상세 `.agent/PLANS.md` (98) |
| 20260824 | `20260824_archive_dedup_time_derived.md` | 적용 완료 (2026-08-24 13:08). 검증 결과는 6절. |
| 20260824 | `20260824_missing_becomes_score_fix.md` | 적용 완료 (2026-08-24 13:02). 검증 결과는 PLANS (82). |
| 20260824 | `20260824_output_self_description_fix.md` | 적용 완료 (2026-08-24 09:56). 검증은 6절. |

## 철회 — 2건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260820 | `20260820_execution_pool_missing_value_defect.md` | 무효 (2026-08-20 철회). 전제가 사실이 아니었다. 어떤 변경도 적용되지 않았다. |
| 20260820 | `20260820_require_macd_golden_disable.md` | B안 철회 (2026-08-20). 1절~3절의 진단은 유효, 5절 B안은 무효. |

## PLANS 참조됨 — 3건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260424 | `20260424_dd_stop_validation_reduce.md` | - Keep active because production/live use of `validation_reduce` is policy-sensi |
| 20260514 | `20260514_entry_decision_layer_separation.md` | — |
| 20260526 | `20260526_normal_entry_baseline_policy_separation.md` | — |

## PLANS 유사언급 — 51건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260423 | `20260423_order_stage_paper_v1.md` | — |
| 20260423 | `20260423_sell_logic_upgrade_v1.md` | — |
| 20260427 | `20260427_future_signal_preview_spec.md` | — |
| 20260429 | `20260429_reconcile_lineage_preserve.md` | — |
| 20260430 | `20260430_blocked_entry_leak_fix.md` | — |
| 20260430 | `20260430_candidate_disparity_l8_apply.md` | — |
| 20260430 | `20260430_candidate_pool_policy_review.md` | — |
| 20260430 | `20260430_general_signal_date_only.md` | — |
| 20260430 | `20260430_low_turnover_validation_profile.md` | — |
| 20260430 | `20260430_news_alignment_llm_status_fix.md` | — |
| 20260430 | `20260430_news_source_signal_llm.md` | — |
| 20260430 | `20260430_sell_trade_chain_fix.md` | — |
| 20260430 | `20260430_stop_gap_policy_review.md` | — |
| 20260430 | `20260430_surge_news_fallback_weight.md` | — |
| 20260504 | `20260504_crash_trigger_explicit.md` | — |
| 20260504 | `20260504_dividend_tp_config_align.md` | — |
| 20260504 | `20260504_min_qty_verification_mode.md` | — |
| 20260504 | `20260504_safe_exploration_advisory.md` | — |
| 20260514 | `20260514_backtest_stable_param_gate_consistency.md` | — |
| 20260517 | `20260517_paper_initial_min_qty_verification_execplan.md` | — |
| 20260518 | `20260518_agentic_trading_observer.md` | — |
| 20260521 | `20260521_bt_aligned_sample_status_policy.md` | — |
| 20260521 | `20260521_ddm_stage_cap_shadow_eval.md` | — |
| 20260521 | `20260521_existing_second_chance_data_audit.md` | — |
| 20260521 | `20260521_news_recheck_shadow_policy_eval.md` | — |
| 20260521 | `20260521_news_recheck_shadow_promotion.md` | — |
| 20260521 | `20260521_promoted_recheck_tradability_guard.md` | — |
| 20260521 | `20260521_second_chance_sample_collection_design.md` | — |
| 20260521 | `20260521_second_chance_shadow_eval.md` | — |
| 20260521 | `20260521_session_only_intraday_gate_exception_execplan.md` | — |
| 20260526 | `20260526_gradual_reduction_axis_review.md` | — |
| 20260526 | `20260526_intraday_surge_isolation_policy.md` | PLAN ONLY |
| 20260603 | `20260603_news_signal_shadow_stage.md` | — |
| 20260604 | `20260604_logic_tier_registry_procedure.md` | — |
| 20260619 | `20260619_promotion_gate_manifest_draft.md` | — |
| 20260619 | `20260619_promotion_gate_readonly_validator_design.md` | — |
| 20260625 | `20260625_regime_overrides.md` | — |
| 20260710 | `20260710_backtest_operational_param_alignment.md` | — |
| 20260710 | `20260710_candidate_loader_alignment.md` | — |
| 20260710 | `20260710_krx_source_priority.md` | — |
| 20260710 | `20260710_stable_param_approval_contract.md` | — |
| 20260720 | `20260720_general_stock_s10_execution_diagnostic.md` | — |
| 20260720 | `20260720_general_stock_s10_pullback_entry.md` | — |
| 20260720 | `20260720_general_stock_strength_within_structure.md` | — |
| 20260720 | `20260720_general_stock_structure_outcome_map.md` | — |
| 20260720 | `20260720_general_stock_trend_liquidity_interaction.md` | — |
| 20260720 | `20260720_strategy_methodology_v3_review_controls.md` | — |
| 20260720 | `20260720_strategy_methodology_v3_temporal_independence.md` | — |
| 20260720 | `20260720_strategy_validation_round1.md` | — |
| 20260720 | `20260720_strategy_validation_round2_regime_strategy.md` | — |
| 20260720 | `20260720_v3_observation_ledger_bootstrap.md` | — |

## 미판정 — 10건

| 날짜 | 파일 | 상태줄 |
|---|---|---|
| 20260422 | `20260422_news_gate_block_fix.md` | — |
| 20260422 | `20260422_surge_change_pct_fix.md` | — |
| 20260423 | `20260423_dashboard_ssot_chain_fix.md` | — |
| 20260423 | `20260423_forecast_score_v1.md` | — |
| 20260429 | `20260429_hpo_time_safe_selection.md` | — |
| 20260429 | `20260429_news_press_tier4.md` | — |
| 20260429 | `20260429_surge_news_guard.md` | — |
| 20260517 | `20260517_roota_ops_sanity_incident_artifacts.md` | — |
| 20260528 | `20260528_surge_entry_change_exception_policy.md` | PLAN ONLY |
| 20260720 | `20260720_liquidity_leader_continuation_exploration.md` | — |

