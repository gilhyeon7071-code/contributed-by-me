# paper_engine_config.json → paper_engine code mapping

Generated: 2026-07-30T14:09:25.998728
Config path: `E:\1_Data\paper\paper_engine_config.json`
Code paths: `paper_engine.py`, `paper_engine/__init__.py`, `paper_engine/common.py`, `paper_engine/config.py`, `paper_engine/drawdown.py`, `paper_engine/guards.py`, `paper_engine/io.py`, `paper_engine/regime.py`, `paper_engine/risk_orchestration.py`, `paper_engine/settlement.py`
Total config keys: 890
Unused (no full-key or qualified last-segment match): 759

| config_key | value_summary | usage_count | locations |
|---|---|---:|---|
| `max_new_trades_per_day` | number(15) | 3 | paper_engine.py:18549, paper_engine/config.py:110, paper_engine/config.py:508 |
| `fixed_qty` | number(1) | 3 | paper_engine.py:15962, paper_engine.py:18861, paper_engine/config.py:111 |
| `max_hold_days` | number(8) | 70 | paper_engine.py:1813, paper_engine.py:4235, paper_engine.py:4238, paper_engine.py:4240, paper_engine.py:5654, paper_engine.py:5655, paper_engine.py:5656, paper_engine.py:5658, paper_engine.py:5662, paper_engine.py:5664 ... |
| `min_hold_days` | number(2) | 12 | paper_engine.py:3307, paper_engine.py:3323, paper_engine.py:3324, paper_engine.py:3325, paper_engine.py:16995, paper_engine.py:16996, paper_engine.py:17319, paper_engine.py:17321, paper_engine.py:17525, paper_engine.py:17531 ... |
| `min_hold_protect_stop_loss` | bool(False) | 4 | paper_engine.py:16997, paper_engine.py:17210, paper_engine.py:17322, paper_engine/config.py:114 |
| `atr_stop_multiplier` | number(2) | 2 | paper_engine.py:16985, paper_engine/config.py:115 |
| `allow_same_code_reentry` | bool(False) | 3 | paper_engine.py:13586, paper_engine.py:18862, paper_engine/config.py:116 |
| `entry_timing_mode` | str("same_close") | 4 | paper_engine.py:18863, paper_engine.py:18864, paper_engine.py:18868, paper_engine/config.py:117 |
| `fee_pct` | number(4e-05) | 44 | paper_engine.py:992, paper_engine.py:1124, paper_engine.py:1145, paper_engine.py:1156, paper_engine.py:1167, paper_engine.py:8156, paper_engine.py:8255, paper_engine.py:11021, paper_engine.py:11027, paper_engine.py:13010 ... |
| `slippage_pct` | number(0.001) | 32 | paper_engine.py:29, paper_engine.py:1341, paper_engine.py:1820, paper_engine.py:2158, paper_engine.py:2161, paper_engine.py:2538, paper_engine.py:2574, paper_engine.py:5081, paper_engine.py:5094, paper_engine.py:5101 ... |
| `tiered_slippage.enabled` | bool(True) | 1 | paper_engine.py:2157 |
| `tiered_slippage.large_cap_krw` | number(1000000000000) | 0 | - |
| `tiered_slippage.mid_cap_krw` | number(100000000000) | 0 | - |
| `tiered_slippage.large_slip_pct` | number(0.003) | 0 | - |
| `tiered_slippage.mid_slip_pct` | number(0.005) | 0 | - |
| `tiered_slippage.small_slip_pct` | number(0.01) | 0 | - |
| `entry_liquidity_check.enabled` | bool(True) | 0 | - |
| `entry_liquidity_check.min_trading_value_krw` | number(1000000000) | 0 | - |
| `limit_price_tolerance.enabled` | bool(True) | 0 | - |
| `limit_price_tolerance.max_deviation_pct` | number(0.08) | 0 | - |
| `split_entry.enabled` | bool(True) | 2 | paper_engine.py:15467, paper_engine.py:16035 |
| `split_entry.first_ratio` | number(0.3) | 0 | - |
| `split_entry.second_dip_min_pct` | number(0.015) | 0 | - |
| `split_entry.second_dip_max_pct` | number(0.04) | 0 | - |
| `split_entry.second_entry_max_days` | number(2) | 0 | - |
| `split_entry.budget_alloc_pct` | number(0.4) | 0 | - |
| `split_entry.allow_split_second_carryover` | bool(True) | 0 | - |
| `split_entry.surge_first_ratio` | number(0.3) | 1 | paper_engine.py:15471 |
| `split_entry.max_open_to_entry_chase_pct` | number(0.05) | 0 | - |
| `split_entry.second_confirmation.enabled` | bool(True) | 0 | - |
| `split_entry.second_confirmation.max_v_accel` | number(0.8) | 0 | - |
| `split_entry.second_confirmation.max_ret1_pct` | number(8.849700179324415) | 0 | - |
| `split_entry.second_confirmation.max_atr14_pct` | number(0.09907047892735515) | 0 | - |
| `split_entry.second_confirmation.missing_feature_action` | str("BLOCK") | 0 | - |
| `split_entry.second_confirmation.require_ma60_support_bounce` | bool(True) | 0 | - |
| `sell_tax_pct` | number(0.0015) | 31 | paper_engine.py:994, paper_engine.py:1124, paper_engine.py:1167, paper_engine.py:8158, paper_engine.py:8255, paper_engine.py:11021, paper_engine.py:11029, paper_engine.py:13012, paper_engine.py:13099, paper_engine.py:16696 ... |
| `candidates_latest_data` | str("E:\1_Data\2_Logs\candidates_latest_data....") | 3 | paper_engine.py:10389, paper_engine.py:10405, paper_engine/config.py:264 |
| `parquet_root` | str("E:\1_Data\paper\prices") | 3 | paper_engine.py:10685, paper_engine.py:10735, paper_engine/config.py:267 |
| `parquet_top_n_recent` | number(10) | 2 | paper_engine.py:10686, paper_engine/config.py:268 |
| `parquet_max_open_files` | number(5) | 2 | paper_engine.py:10737, paper_engine/config.py:269 |
| `parquet_search.root` | str(".") | 0 | - |
| `parquet_search.top_n_recent` | number(80) | 0 | - |
| `parquet_search.max_open_files` | number(20) | 0 | - |
| `engine_log_dir` | str("E:\1_Data\2_Logs") | 0 | - |
| `sizing_mode` | str("capital_slots") | 4 | paper_engine.py:15427, paper_engine.py:15434, paper_engine.py:15962, paper_engine.py:18861 |
| `capital_total` | number(100000000) | 52 | paper_engine.py:3520, paper_engine.py:3541, paper_engine.py:5794, paper_engine.py:5875, paper_engine.py:5876, paper_engine.py:5877, paper_engine.py:8298, paper_engine.py:8382, paper_engine.py:13265, paper_engine.py:13282 ... |
| `max_positions` | number(18) | 74 | paper_engine.py:7403, paper_engine.py:7494, paper_engine.py:8302, paper_engine.py:8386, paper_engine.py:13404, paper_engine.py:13517, paper_engine.py:13556, paper_engine.py:13580, paper_engine.py:14021, paper_engine.py:14022 ... |
| `cash_per_trade` | number(2000000) | 0 | - |
| `min_qty` | number(1) | 44 | paper_engine.py:3998, paper_engine.py:4011, paper_engine.py:4155, paper_engine.py:4181, paper_engine.py:5157, paper_engine.py:5212, paper_engine.py:5421, paper_engine.py:5424, paper_engine.py:5428, paper_engine.py:5429 ... |
| `kill_switch.max_drawdown_pct` | number(0.36) | 2 | paper_engine.py:4779, paper_engine.py:18617 |
| `kill_switch.max_daily_loss_pct` | number(0.08) | 1 | paper_engine.py:18618 |
| `kill_switch.mode` | str("BLOCK") | 5 | paper_engine.py:569, paper_engine.py:572, paper_engine.py:18574, paper_engine.py:18614, paper_engine/config.py:331 |
| `kill_switch.reduce_factor` | number(0.3) | 1 | paper_engine.py:18615 |
| `kill_switch.min_new_trades_per_day` | number(1) | 0 | - |
| `crash_risk_off.enabled` | bool(True) | 0 | - |
| `crash_risk_off.index_market` | str("KOSPI") | 0 | - |
| `crash_risk_off.index_name_contains` | str("KOSPI") | 0 | - |
| `crash_risk_off.lookback_days` | number(120) | 0 | - |
| `crash_risk_off.rv20_floor` | number(0.0001) | 0 | - |
| `crash_risk_off.rv20_spike_ratio` | number(0.1) | 0 | - |
| `crash_risk_off.day_drop_pct` | number(-0.0001) | 0 | - |
| `crash_risk_off.gap_down_pct` | number(-0.0001) | 0 | - |
| `crash_risk_off.mode` | str("REDUCE") | 1 | paper_engine.py:18591 |
| `crash_risk_off.reduce_factor` | number(0.5) | 0 | - |
| `crash_risk_off.min_new_trades_per_day` | number(1) | 0 | - |
| `crash_risk_off.index_code` | str("1001") | 0 | - |
| `crash_risk_off.fallback_trigger_max_dd_pct` | number(0.35) | 0 | - |
| `crash_risk_off.fallback_trigger_day_ret_pct` | number(0.15) | 0 | - |
| `crash_risk_off.trigger_max_dd_pct` | number(0.12) | 0 | - |
| `crash_risk_off.trigger_day_ret_pct` | number(0.05) | 0 | - |
| `cap_signal_top_n` | number(3) | 10 | paper_engine.py:12962, paper_engine.py:12965, paper_engine.py:12977, paper_engine.py:12979, paper_engine.py:12981, paper_engine.py:13438, paper_engine.py:13446, paper_engine.py:13447, paper_engine.py:13448, paper_engine.py:13505 ... |
| `gap_up_max_pct` | number(0.03) | 26 | paper_engine.py:13523, paper_engine.py:14498, paper_engine.py:14726, paper_engine.py:14728, paper_engine.py:14729, paper_engine.py:14730, paper_engine.py:14731, paper_engine.py:14854, paper_engine.py:14860, paper_engine.py:14866 ... |
| `entry_gap_risk_guard.enabled` | bool(True) | 0 | - |
| `entry_gap_risk_guard.lookback_sessions` | number(20) | 0 | - |
| `entry_gap_risk_guard.down_gap_threshold_pct` | number(0.08) | 0 | - |
| `entry_gap_risk_guard.max_down_gap_count` | number(1) | 0 | - |
| `hold_close_drop_guard.enabled` | bool(True) | 0 | - |
| `hold_close_drop_guard.drop_pct` | number(0.05) | 0 | - |
| `hold_close_drop_guard.sell_ratio_pct` | number(100) | 0 | - |
| `hold_close_drop_guard.min_hold_days` | number(1) | 0 | - |
| `max_per_sector` | number(2) | 20 | paper_engine.py:12963, paper_engine.py:12983, paper_engine.py:12986, paper_engine.py:12997, paper_engine.py:12999, paper_engine.py:13435, paper_engine.py:13439, paper_engine.py:13449, paper_engine.py:13450, paper_engine.py:13451 ... |
| `union_entry_strength_min` | number(0.63) | 4 | paper_engine.py:423, paper_engine.py:11867, paper_engine.py:12255, paper_engine/config.py:309 |
| `max_gross_exposure_pct` | number(1) | 44 | paper_engine.py:13266, paper_engine.py:13283, paper_engine.py:18919, paper_engine.py:18928, paper_engine.py:18929, paper_engine.py:18931, paper_engine.py:18941, paper_engine.py:18942, paper_engine.py:18943, paper_engine.py:18948 ... |
| `max_daily_new_exposure_pct` | number(0.4) | 13 | paper_engine.py:13267, paper_engine.py:13284, paper_engine.py:18920, paper_engine.py:19463, paper_engine.py:19464, paper_engine.py:19465, paper_engine.py:19495, paper_engine.py:19969, paper_engine/config.py:271, paper_engine/config.py:438 ... |
| `entry_gap_down_stop_pct` | number(0.03) | 13 | paper_engine.py:13524, paper_engine.py:14988, paper_engine.py:14989, paper_engine.py:14990, paper_engine.py:17159, paper_engine.py:19008, paper_engine.py:19488, paper_engine.py:19490, paper_engine.py:19497, paper_engine.py:20075 ... |
| `regime_entry_policy.rally_day_ret_min_proxy` | number(0.025) | 1 | paper_engine/guards.py:245 |
| `regime_entry_policy.rate_hike_fear_reduce_day_ret_floor` | number(-0.015) | 0 | - |
| `regime_entry_policy.rally_gap_up_max_pct` | number(0.08) | 0 | - |
| `regime_entry_policy.p0_bear_promote_enabled` | bool(True) | 0 | - |
| `regime_entry_policy.p0_bear_allowed_macro_regimes` | list[4] | 0 | - |
| `beta_harvest.enabled` | bool(False) | 0 | - |
| `beta_harvest.mode` | str("largecap") | 0 | - |
| `beta_harvest.paper_only` | bool(True) | 0 | - |
| `beta_harvest.order_type` | str("limit") | 0 | - |
| `beta_harvest.etfs.KOSPI` | str("069500") | 0 | - |
| `beta_harvest.etfs.KOSDAQ` | str("229200") | 0 | - |
| `beta_harvest.largecaps.KOSPI` | list[9] | 0 | - |
| `beta_harvest.largecaps.KOSDAQ` | list[5] | 0 | - |
| `beta_harvest.allocation.KOSPI` | number(0.5) | 0 | - |
| `beta_harvest.allocation.KOSDAQ` | number(0.5) | 0 | - |
| `beta_harvest.rebalance_freq` | str("monthly") | 0 | - |
| `beta_harvest.use_regime_scaling` | bool(True) | 0 | - |
| `beta_harvest.regime_fallback` | str("NORMAL") | 0 | - |
| `beta_harvest.min_order_amount_krw` | number(100000) | 0 | - |
| `beta_harvest.max_single_etf_weight` | number(0.6) | 0 | - |
| `beta_harvest.order_output_dir` | str("E:\1_Data\paper") | 0 | - |
| `beta_harvest.position_ledger_path` | str("E:\1_Data\paper\beta_harvest_positions.j...") | 0 | - |
| `regime_overrides.RALLY.max_gross_exposure_pct` | number(0.6) | 0 | - |
| `regime_overrides.RALLY.max_daily_new_exposure_pct` | number(0.15) | 0 | - |
| `regime_overrides.RALLY.gap_up_max_pct` | number(0.08) | 0 | - |
| `regime_overrides.RALLY.stop_loss_pct` | number(-0.04) | 0 | - |
| `regime_overrides.RALLY.capital_budget_policy.gross_exposure_pct` | number(0.6) | 1 | paper_engine.py:18926 |
| `regime_overrides.RALLY.capital_budget_policy.surge_alloc_pct` | number(0.45) | 1 | paper_engine.py:18933 |
| `regime_overrides.RALLY.capital_budget_policy.split_alloc_pct` | number(0.2) | 1 | paper_engine.py:18934 |
| `regime_overrides.RALLY.surge_entry_policy.total_alloc_pct` | number(0.45) | 0 | - |
| `regime_overrides.RALLY.surge_entry_policy.max_new_surge` | number(6) | 0 | - |
| `regime_overrides.RALLY.surge_entry_policy.type_policy.afternoon_session_filter.enabled` | bool(False) | 0 | - |
| `regime_overrides.RALLY.surge_entry_policy.type_policy.trend_smoothness_min` | number(1) | 1 | paper_engine.py:5588 |
| `regime_overrides.RALLY.surge_exit_policy.stop_loss_pct` | number(-0.04) | 0 | - |
| `regime_overrides.RALLY.surge_exit_policy.max_hold_days` | number(5) | 0 | - |
| `regime_overrides.RALLY.sell_rules.stop_loss.trailing_stop_activation_profit_pct` | number(7) | 0 | - |
| `regime_overrides.RALLY.sell_rules.stop_loss.trailing_stop_pct` | number(-4) | 0 | - |
| `regime_overrides.RALLY.sell_rules.take_profit.levels` | list[3] | 1 | paper_engine.py:4230 |
| `regime_overrides.RALLY.sell_rules.take_profit.ratios` | list[3] | 1 | paper_engine.py:4231 |
| `regime_overrides.RALLY.split_entry.second_confirmation.max_v_accel` | number(3.15) | 0 | - |
| `regime_overrides.NORMAL.max_hold_days` | number(15) | 0 | - |
| `regime_overrides.NORMAL.stop_loss_pct` | number(-0.05) | 0 | - |
| `regime_overrides.NORMAL.capital_budget_policy.gross_exposure_pct` | number(0.55) | 1 | paper_engine.py:18926 |
| `regime_overrides.NORMAL.capital_budget_policy.surge_alloc_pct` | number(0.15) | 1 | paper_engine.py:18933 |
| `regime_overrides.NORMAL.capital_budget_policy.split_alloc_pct` | number(0.4) | 1 | paper_engine.py:18934 |
| `regime_overrides.NORMAL.split_entry.enabled` | bool(True) | 2 | paper_engine.py:15467, paper_engine.py:16035 |
| `regime_overrides.NORMAL.split_entry.budget_alloc_pct` | number(0.4) | 0 | - |
| `regime_overrides.NORMAL.surge_entry_policy.total_alloc_pct` | number(0.15) | 0 | - |
| `regime_overrides.NORMAL.surge_entry_policy.max_new_surge` | number(2) | 0 | - |
| `regime_overrides.NORMAL.surge_entry_policy.type_policy.afternoon_session_filter.enabled` | bool(True) | 0 | - |
| `regime_overrides.NORMAL.surge_entry_policy.type_policy.block_price_vol_divergence` | bool(True) | 1 | paper_engine.py:5580 |
| `regime_overrides.NORMAL.sell_rules.stop_loss.trailing_stop_activation_profit_pct` | number(12) | 0 | - |
| `regime_overrides.NORMAL.sell_rules.stop_loss.trailing_stop_pct` | number(-5) | 0 | - |
| `regime_overrides.NORMAL.sell_rules.take_profit.levels` | list[3] | 1 | paper_engine.py:4230 |
| `regime_overrides.NORMAL.sell_rules.take_profit.ratios` | list[3] | 1 | paper_engine.py:4231 |
| `regime_overrides.BEAR.max_hold_days` | number(15) | 0 | - |
| `regime_overrides.BEAR.stop_loss_pct` | number(-0.05) | 0 | - |
| `regime_overrides.BEAR.capital_budget_policy.gross_exposure_pct` | number(0.35) | 1 | paper_engine.py:18926 |
| `regime_overrides.BEAR.capital_budget_policy.surge_alloc_pct` | number(0.08) | 1 | paper_engine.py:18933 |
| `regime_overrides.BEAR.capital_budget_policy.split_alloc_pct` | number(0.25) | 1 | paper_engine.py:18934 |
| `regime_overrides.BEAR.split_entry.enabled` | bool(True) | 2 | paper_engine.py:15467, paper_engine.py:16035 |
| `regime_overrides.BEAR.split_entry.budget_alloc_pct` | number(0.25) | 0 | - |
| `regime_overrides.BEAR.surge_entry_policy.total_alloc_pct` | number(0.08) | 0 | - |
| `regime_overrides.BEAR.surge_entry_policy.max_new_surge` | number(1) | 0 | - |
| `regime_overrides.BEAR.surge_entry_policy.type_policy.afternoon_session_filter.enabled` | bool(True) | 0 | - |
| `regime_overrides.BEAR.surge_entry_policy.type_policy.block_price_vol_divergence` | bool(True) | 1 | paper_engine.py:5580 |
| `regime_overrides.BEAR.sell_rules.stop_loss.trailing_stop_activation_profit_pct` | number(12) | 0 | - |
| `regime_overrides.BEAR.sell_rules.stop_loss.trailing_stop_pct` | number(-5) | 0 | - |
| `regime_overrides.BEAR.sell_rules.take_profit.levels` | list[3] | 1 | paper_engine.py:4230 |
| `regime_overrides.BEAR.sell_rules.take_profit.ratios` | list[3] | 1 | paper_engine.py:4231 |
| `regime_overrides.CRASH.max_new_trades_per_day` | number(3) | 0 | - |
| `regime_overrides.CRASH.max_gross_exposure_pct` | number(0.1) | 0 | - |
| `regime_overrides.CRASH.max_daily_new_exposure_pct` | number(0.05) | 0 | - |
| `regime_overrides.CRASH.capital_budget_policy.gross_exposure_pct` | number(0.1) | 1 | paper_engine.py:18926 |
| `regime_overrides.CRASH.capital_budget_policy.basic_alloc_pct` | number(0) | 1 | paper_engine.py:18932 |
| `regime_overrides.CRASH.capital_budget_policy.surge_alloc_pct` | number(0.05) | 1 | paper_engine.py:18933 |
| `regime_overrides.CRASH.capital_budget_policy.split_alloc_pct` | number(0) | 1 | paper_engine.py:18934 |
| `regime_overrides.CRASH.capital_budget_policy.recovery_alloc_pct` | number(0) | 1 | paper_engine.py:18935 |
| `regime_overrides.CRASH.split_entry.budget_alloc_pct` | number(0) | 0 | - |
| `regime_overrides.CRASH.surge_entry_policy.total_alloc_pct` | number(0.05) | 0 | - |
| `regime_overrides.CRASH.surge_entry_policy.max_new_surge` | number(3) | 0 | - |
| `regime_overrides.CRASH.surge_entry_policy.type_policy.allowed_types` | list[1] | 1 | paper_engine.py:5573 |
| `trend_overlay_2026.enabled` | bool(True) | 0 | - |
| `trend_overlay_2026.cutting_hawkish_max` | number(-0.2) | 0 | - |
| `trend_overlay_2026.seasonal_risk_multiplier.Q1` | number(1) | 0 | - |
| `trend_overlay_2026.seasonal_risk_multiplier.Q2` | number(1) | 0 | - |
| `trend_overlay_2026.seasonal_risk_multiplier.Q3` | number(0.85) | 0 | - |
| `trend_overlay_2026.seasonal_risk_multiplier.Q4` | number(0.75) | 0 | - |
| `trend_overlay_2026.cutting_overlay.growth_entry_weight` | number(1.15) | 0 | - |
| `trend_overlay_2026.cutting_overlay.defensive_entry_weight` | number(0.9) | 0 | - |
| `trend_overlay_2026.cutting_overlay.growth_codes` | list[0] | 0 | - |
| `trend_overlay_2026.cutting_overlay.defensive_codes` | list[0] | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.daily_top_n` | number(2) | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.max_open_positions` | number(2) | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.single_name_cap_pct` | number(0.15) | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.semiconductor_codes` | list[3] | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.ai_software_codes` | list[2] | 0 | - |
| `trend_overlay_2026.ai_semiconductor_overlay.it_hardware_codes` | list[2] | 0 | - |
| `adaptive_entry_control.enabled` | bool(True) | 0 | - |
| `adaptive_entry_control.kill_switch_override_block` | bool(False) | 0 | - |
| `adaptive_entry_control.dd_ratio_soft` | number(1) | 0 | - |
| `adaptive_entry_control.dd_ratio_mid` | number(1.1) | 0 | - |
| `adaptive_entry_control.dd_ratio_hard` | number(1.25) | 0 | - |
| `adaptive_entry_control.reduce_soft` | number(0.8) | 0 | - |
| `adaptive_entry_control.reduce_mid` | number(0.6) | 0 | - |
| `adaptive_entry_control.reduce_hard` | number(0.4) | 0 | - |
| `adaptive_entry_control.probe_min_new` | number(1) | 0 | - |
| `adaptive_entry_control.relief_after_streak_days` | number(3) | 0 | - |
| `adaptive_entry_control.relief_min_new` | number(1) | 0 | - |
| `adaptive_entry_control.dynamic_relax_l5_factor` | number(0.1) | 0 | - |
| `adaptive_good_stock_entry.enabled` | bool(True) | 0 | - |
| `adaptive_good_stock_entry.policy_design_path` | str("E:/1_Data/2_Logs/adaptive_entry_conditio...") | 0 | - |
| `adaptive_good_stock_entry.open_chase_probe_routes` | list[2] | 0 | - |
| `adaptive_good_stock_entry.intraday_momentum_probe_routes` | list[1] | 0 | - |
| `adaptive_good_stock_entry.gapup_wait_route` | str("WAIT_PULLBACK_RECLAIM") | 0 | - |
| `adaptive_good_stock_entry.tier1_post_split_qty_multiplier` | number(1) | 0 | - |
| `adaptive_good_stock_entry.tier2_post_split_qty_multiplier` | number(0.5) | 0 | - |
| `adaptive_good_stock_entry.pullback_post_split_qty_multiplier` | number(0.67) | 0 | - |
| `adaptive_good_stock_entry.tier1_gapup_override_max_pct` | number(0.16) | 0 | - |
| `adaptive_good_stock_entry.tier2_gapup_override_max_pct` | number(0.12) | 0 | - |
| `risk_orchestration.enabled` | bool(True) | 1 | paper_engine.py:19394 |
| `risk_orchestration.lookback_trades` | number(60) | 0 | - |
| `risk_orchestration.target_vol` | number(0.03) | 0 | - |
| `risk_orchestration.f_kelly_cap` | number(1.5) | 0 | - |
| `risk_orchestration.c_default` | number(0.5) | 0 | - |
| `risk_orchestration.c_min` | number(0.25) | 0 | - |
| `risk_orchestration.c_max` | number(0.5) | 0 | - |
| `risk_orchestration.vol_ratio_cap` | number(3) | 0 | - |
| `risk_orchestration.max_scale` | number(1) | 0 | - |
| `risk_orchestration.edge_zero_floor_scale` | number(0.25) | 0 | - |
| `risk_orchestration.regime_confidence.RALLY` | number(0.45) | 0 | - |
| `risk_orchestration.regime_confidence.NORMAL` | number(0.4) | 0 | - |
| `risk_orchestration.regime_confidence.BEAR` | number(0.3) | 0 | - |
| `risk_orchestration.regime_confidence.CRASH` | number(0.25) | 0 | - |
| `risk_orchestration.regime_confidence.STAGFLATION` | number(0.25) | 0 | - |
| `risk_orchestration.regime_confidence.RATE_HIKE_FEAR` | number(0.3) | 0 | - |
| `risk_orchestration.dd_taper.dd_cap` | number(0.1) | 0 | - |
| `risk_orchestration.dd_taper.dd_stop` | number(0.25) | 0 | - |
| `risk_orchestration.dd_taper.mode` | str("advisory") | 1 | paper_engine/risk_orchestration.py:107 |
| `risk_orchestration.dd_taper.account_clear_strategy_dd_override.enabled` | bool(True) | 0 | - |
| `risk_orchestration.dd_taper.account_clear_strategy_dd_override.scope` | str("paper_virtual_only") | 0 | - |
| `risk_orchestration.dd_taper.account_clear_strategy_dd_override.reason` | str("When account risk is clear, keep strateg...") | 0 | - |
| `risk_orchestration.dd_stop_validation.enabled` | bool(False) | 1 | paper_engine.py:19394 |
| `risk_orchestration.dd_stop_validation.mode` | str("validation_reduce") | 0 | - |
| `risk_orchestration.dd_stop_validation.max_new` | number(2) | 0 | - |
| `risk_orchestration.dd_stop_validation.max_new_surge` | number(1) | 0 | - |
| `risk_orchestration.dd_stop_validation.position_size_multiplier` | number(0.25) | 0 | - |
| `risk_orchestration.dd_stop_validation.allow_reasons` | list[1] | 0 | - |
| `risk_orchestration.dd_stop_validation.allowed_run_labels` | list[3] | 0 | - |
| `risk_orchestration.dd_stop_validation.max_positions_full_override.enabled` | bool(False) | 0 | - |
| `risk_orchestration.dd_stop_validation.max_positions_full_override.mode` | str("gross_exposure_cap") | 0 | - |
| `risk_orchestration.dd_stop_validation.require_dd_stop` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.enabled` | bool(True) | 0 | - |
| `risk_orchestration.es_gate.limit` | number(0.05) | 0 | - |
| `risk_orchestration.es_gate.hard_block` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.reduction_factor` | number(0.5) | 0 | - |
| `risk_orchestration.es_gate.by_regime.CRASH.limit` | number(0.04) | 0 | - |
| `risk_orchestration.es_gate.by_regime.CRASH.hard_block` | bool(True) | 0 | - |
| `risk_orchestration.es_gate.by_regime.CRASH.reduction_factor` | number(0) | 0 | - |
| `risk_orchestration.es_gate.by_regime.STAGFLATION.limit` | number(0.045) | 0 | - |
| `risk_orchestration.es_gate.by_regime.STAGFLATION.hard_block` | bool(True) | 0 | - |
| `risk_orchestration.es_gate.by_regime.STAGFLATION.reduction_factor` | number(0) | 0 | - |
| `risk_orchestration.es_gate.by_regime.BEAR.limit` | number(0.05) | 0 | - |
| `risk_orchestration.es_gate.by_regime.BEAR.hard_block` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.by_regime.BEAR.reduction_factor` | number(0.35) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RALLY.limit` | number(0.06) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RALLY.hard_block` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RALLY.reduction_factor` | number(0.6) | 0 | - |
| `risk_orchestration.es_gate.by_regime.NORMAL.limit` | number(0.05) | 0 | - |
| `risk_orchestration.es_gate.by_regime.NORMAL.hard_block` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.by_regime.NORMAL.reduction_factor` | number(0.5) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.limit` | number(0.05) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.hard_block` | bool(False) | 0 | - |
| `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.reduction_factor` | number(0.4) | 0 | - |
| `risk_orchestration.kelly_role` | str("confirm_only") | 0 | - |
| `risk_orchestration.account_clear_min_scale.enabled` | bool(True) | 0 | - |
| `risk_orchestration.account_clear_min_scale.min_scale` | number(0.25) | 0 | - |
| `risk_orchestration.account_clear_min_scale.require_dd_advisory_only` | bool(True) | 0 | - |
| `bear_sizing_policy.enabled` | bool(True) | 1 | paper_engine.py:19362 |
| `bear_sizing_policy.market_regimes` | list[1] | 1 | paper_engine.py:19365 |
| `bear_sizing_policy.entry_gate_decisions` | list[1] | 1 | paper_engine.py:19370 |
| `bear_sizing_policy.max_risk_orch_scale` | number(0.5) | 1 | paper_engine.py:19374 |
| `bear_sizing_policy.gross_exposure_cap_pct` | number(0.4) | 1 | paper_engine.py:19381 |
| `production_risk_playbook.hard_entry_decision` | str("REDUCE") | 0 | - |
| `production_risk_playbook.hard_size_multiplier` | number(0.25) | 0 | - |
| `production_risk_playbook.drawdown_role` | str("advisory") | 0 | - |
| `production_risk_playbook.drawdown_owner` | str("DDM") | 0 | - |
| `production_risk_playbook.dd_pct.watch` | number(0.25) | 0 | - |
| `production_risk_playbook.dd_pct.soft` | number(0.5) | 0 | - |
| `production_risk_playbook.dd_pct.hard` | number(1) | 0 | - |
| `market_ops_policy.enabled` | bool(True) | 0 | - |
| `market_ops_policy.slo_normal` | number(0.1) | 0 | - |
| `market_ops_policy.slo_rally_base` | number(0.2) | 0 | - |
| `market_ops_policy.slo_rally_per_ret` | number(2) | 0 | - |
| `market_ops_policy.slo_rally_cap` | number(0.5) | 0 | - |
| `market_ops_policy.slo_crash_base` | number(0.05) | 0 | - |
| `market_ops_policy.slo_crash_per_ret` | number(0.8) | 0 | - |
| `market_ops_policy.slo_crash_cap` | number(0.2) | 0 | - |
| `market_ops_policy.probe_rally_min` | number(1) | 0 | - |
| `market_ops_policy.probe_rally_ratio_base` | number(0.1) | 0 | - |
| `market_ops_policy.probe_rally_ratio_per_ret` | number(2) | 0 | - |
| `market_ops_policy.probe_rally_ratio_cap` | number(0.35) | 0 | - |
| `market_ops_policy.probe_crash_min` | number(0) | 0 | - |
| `market_ops_policy.probe_crash_ratio_base` | number(0.03) | 0 | - |
| `market_ops_policy.probe_crash_ratio_per_ret` | number(0.8) | 0 | - |
| `market_ops_policy.probe_crash_ratio_cap` | number(0.1) | 0 | - |
| `market_ops_policy.universe_shrink_min_candidates` | number(8) | 0 | - |
| `market_ops_policy.universe_shrink_min_price_codes` | number(0) | 0 | - |
| `market_ops_policy.universe_shrink_disable_signal_cap` | bool(True) | 0 | - |
| `market_ops_policy.universe_shrink_disable_sector_cap` | bool(True) | 0 | - |
| `market_ops_policy.universe_shrink_probe_uplift` | number(0.05) | 0 | - |
| `market_ops_policy.carryover_no_next_day_enabled` | bool(False) | 0 | - |
| `market_ops_policy.carryover_max_age_days` | number(2) | 0 | - |
| `market_ops_policy.carryover_revalidate_score_gap_max` | number(0.05) | 0 | - |
| `market_ops_policy.carryover_revalidate_min_final_score` | number(0) | 0 | - |
| `market_ops_policy.entry_fallback_policy.enabled` | bool(True) | 0 | - |
| `market_ops_policy.entry_fallback_policy.max_stage` | number(0) | 0 | - |
| `market_ops_policy.entry_fallback_policy.stage_gap_up_limits` | list[3] | 0 | - |
| `market_ops_policy.entry_fallback_policy.signal_valid_days` | number(1) | 0 | - |
| `market_ops_policy.entry_fallback_policy.auction_close_position_min` | number(0.7) | 0 | - |
| `market_ops_policy.entry_fallback_policy.auction_day_range_max_pct` | number(0.07) | 0 | - |
| `market_ops_policy.entry_fallback_policy.auction_v_accel_min` | number(1.5) | 0 | - |
| `market_ops_policy.entry_fallback_policy.next_open_limit_pct` | number(0.01) | 0 | - |
| `market_ops_policy.entry_fallback_policy.intraday_limit_pct` | number(0.01) | 0 | - |
| `market_ops_policy.entry_fallback_policy.next_open_gap_up_max_pct` | number(0.03) | 0 | - |
| `market_ops_policy.strict_same_day_only` | bool(True) | 0 | - |
| `market_ops_policy.signal_ttl_minutes` | number(60) | 0 | - |
| `market_ops_policy.max_retry_per_code_per_day` | number(2) | 0 | - |
| `market_ops_policy.partial_fill_same_day_only` | bool(True) | 0 | - |
| `market_ops_policy.exec_quality_max_slippage_pct` | number(0.05) | 0 | - |
| `market_ops_policy.close_cutoff_minutes` | number(10) | 0 | - |
| `market_ops_policy.quote_max_age_days` | number(1) | 0 | - |
| `market_ops_policy.fail_closed_propagate_block` | bool(True) | 0 | - |
| `market_ops_policy.fail_closed_unfilled_ratio_threshold` | number(0.4) | 0 | - |
| `surge_entry_policy.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.realtime_only` | bool(True) | 0 | - |
| `surge_entry_policy.top_n` | number(8) | 0 | - |
| `surge_entry_policy.min_score_final` | number(75) | 0 | - |
| `surge_entry_policy.max_new_surge` | number(6) | 0 | - |
| `surge_entry_policy.max_same_code_per_day` | number(1) | 0 | - |
| `surge_entry_policy.per_symbol_alloc_pct` | number(0.03) | 0 | - |
| `surge_entry_policy.total_alloc_pct` | number(0.45) | 0 | - |
| `surge_entry_policy.min_qty` | number(1) | 2 | paper_engine.py:15476, paper_engine.py:15737 |
| `surge_entry_policy.type_policy.enabled` | bool(True) | 1 | paper_engine.py:5569 |
| `surge_entry_policy.type_policy.max_new_per_type` | number(3) | 1 | paper_engine.py:13977 |
| `surge_entry_policy.type_policy.allowed_types` | list[5] | 1 | paper_engine.py:5573 |
| `surge_entry_policy.type_policy.blocked_types` | list[1] | 1 | paper_engine.py:5572 |
| `surge_entry_policy.type_policy.type_qty_multiplier.PRICE_VOL_BREAKOUT` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_qty_multiplier.REG_SHORT_5D60` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_qty_multiplier.REG_MID_15D100` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_qty_multiplier.PRICE_RANGE_BREAKOUT` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_qty_multiplier.LIMIT_UP_NEAR` | number(0.25) | 0 | - |
| `surge_entry_policy.type_policy.type_qty_multiplier.OVERSOLD_REVERSAL` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.min_score_final` | number(75) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.alloc_pct` | number(0.01) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.first_ratio` | number(0.3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.stop_loss_pct` | number(-0.08) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.take_profit_pct` | number(0.1) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.max_hold_days` | number(5) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.entry_timing` | str("realtime") | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.min_score_final` | number(80) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.alloc_pct` | number(0.0075) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.first_ratio` | number(0.3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.stop_loss_pct` | number(-0.07) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.take_profit_pct` | number(0.08) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.max_hold_days` | number(3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.entry_timing` | str("realtime") | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.min_score_final` | number(70) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.alloc_pct` | number(0.02) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.first_ratio` | number(0.3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.stop_loss_pct` | number(-0.1) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.take_profit_pct` | number(0.15) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.max_hold_days` | number(5) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.entry_timing` | str("next_open") | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.min_score_final` | number(85) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.alloc_pct` | number(0.015) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.first_ratio` | number(0.3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.stop_loss_pct` | number(-0.08) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.take_profit_pct` | number(0.08) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.max_hold_days` | number(3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.entry_timing` | str("next_open") | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.min_score_final` | number(80) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.alloc_pct` | number(0.003) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.first_ratio` | number(0.25) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.stop_loss_pct` | number(-0.04) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.take_profit_pct` | number(0.04) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.max_hold_days` | number(1) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.entry_timing` | str("realtime") | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.min_score_final` | number(60) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.alloc_pct` | number(0.005) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.first_ratio` | number(0.3) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.stop_loss_pct` | number(-0.05) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.take_profit_pct` | number(0.05) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.max_hold_days` | number(2) | 0 | - |
| `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.entry_timing` | str("realtime") | 0 | - |
| `surge_entry_policy.type_policy.trend_smoothness_min` | number(1.5) | 1 | paper_engine.py:5588 |
| `surge_entry_policy.type_policy.block_price_vol_divergence` | bool(True) | 1 | paper_engine.py:5580 |
| `surge_entry_policy.market_event_guard.enabled` | bool(True) | 2 | paper_engine.py:4468, paper_engine.py:4544 |
| `surge_entry_policy.market_event_guard.block_on_exclude_reasons` | bool(True) | 0 | - |
| `surge_entry_policy.market_event_guard.blocked_terms` | list[9] | 0 | - |
| `surge_entry_policy.dynamic_max_new.enabled` | bool(True) | 1 | paper_engine.py:19879 |
| `surge_entry_policy.dynamic_max_new.hard_cap` | number(6) | 0 | - |
| `surge_entry_policy.dynamic_max_new.weak_cap` | number(1) | 0 | - |
| `surge_entry_policy.dynamic_max_new.medium_cap` | number(3) | 0 | - |
| `surge_entry_policy.dynamic_max_new.strong_cap` | number(6) | 0 | - |
| `surge_entry_policy.dynamic_max_new.min_score_final` | number(75) | 0 | - |
| `surge_entry_policy.dynamic_max_new.medium_score_final` | number(82) | 0 | - |
| `surge_entry_policy.dynamic_max_new.strong_score_final` | number(90) | 0 | - |
| `surge_entry_policy.dynamic_max_new.medium_rvol20_min` | number(2) | 0 | - |
| `surge_entry_policy.dynamic_max_new.strong_rvol20_min` | number(3) | 0 | - |
| `surge_entry_policy.dynamic_max_new.medium_spread_bps_max` | number(40) | 0 | - |
| `surge_entry_policy.dynamic_max_new.strong_spread_bps_max` | number(30) | 0 | - |
| `surge_entry_policy.dynamic_max_new.orderflow_risk_score_max` | number(0.6) | 0 | - |
| `surge_entry_policy.dynamic_max_new.no_lob_cap` | number(1) | 0 | - |
| `surge_entry_policy.no_lob_probe.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.no_lob_probe.max_selected` | number(1) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.scope` | str("paper_mock_surge_active_entry_only") | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.max_gap_pct` | number(0.16) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.max_spread_bps` | number(25) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.min_markout_1step_bps` | number(0) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.min_score_final` | number(78) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.require_lob_ok` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.require_orderflow_tag_ok` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.require_paper_ready` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_up_override.require_paper_order_route` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_risk_override.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_risk_override.scope` | str("paper_mock_surge_active_entry_only") | 0 | - |
| `surge_entry_policy.active_entry_gap_risk_override.require_active_gap_up_override_pass` | bool(True) | 0 | - |
| `surge_entry_policy.active_entry_gap_risk_override.max_down_gap_count` | number(2) | 0 | - |
| `surge_entry_policy.active_entry_gap_risk_override.min_current_gap_pct` | number(0.05) | 0 | - |
| `surge_entry_policy.paper_probe.enabled` | bool(True) | 1 | paper_engine.py:9901 |
| `surge_entry_policy.paper_probe.max_selected` | number(1) | 0 | - |
| `surge_entry_policy.paper_probe.min_score_final` | number(78) | 1 | paper_engine.py:9902 |
| `surge_entry_policy.paper_probe.max_score_gap` | number(2) | 1 | paper_engine.py:9903 |
| `surge_entry_policy.paper_probe.gap_up_override.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.scope` | str("paper_mock_surge_probe_only") | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.max_gap_pct` | number(0.16) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.max_spread_bps` | number(25) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.min_markout_1step_bps` | number(0) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.min_score_final` | number(78) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.require_lob_ok` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.require_orderflow_tag_ok` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.require_paper_ready` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.gap_up_override.require_paper_order_route` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.scope` | str("all_surge_order_routes") | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.lob_sweep_multiplier` | number(0.5) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.no_history_multiplier` | number(0.5) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.no_lob_fallback_multiplier` | number(0.5) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.score_reduce_below` | number(85) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.score_low_multiplier` | number(0.5) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.rvol_bands` | list[3] | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.spread_bands` | list[3] | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.multi_condition.two_or_more_multiplier` | number(0.3) | 0 | - |
| `surge_entry_policy.paper_probe.execution_quality_sizing.multi_condition.three_or_more_multiplier` | number(0.2) | 0 | - |
| `surge_entry_policy.afternoon_session_filter.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.afternoon_session_filter.block_after_hhmm` | number(1300) | 0 | - |
| `surge_entry_policy.afternoon_session_filter.blocked_types` | list[2] | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.enabled` | bool(True) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.scope` | str("paper_mock_surge_wait_reclaim_probe_only") | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.max_selected` | number(1) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.min_change_pct` | number(0.05) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.max_gap_pct` | number(0.3) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.min_trading_value_krw` | number(1000000000) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.max_rvol20` | number(30) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.max_spread_bps` | number(25) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.min_markout_1step_bps` | number(0) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.min_score_final` | number(78) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.min_intraday_range_position_pct` | number(0.95) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.max_intraday_high_drawdown_pct` | number(0.005) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.require_lob_ok` | bool(True) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.require_orderflow_ok` | bool(False) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.allow_orderflow_no_history` | bool(True) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.broker_order_route` | bool(False) | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.allowed_types` | list[1] | 0 | - |
| `surge_entry_policy.wait_reclaim_paper_probe.bypass_type_block` | bool(True) | 0 | - |
| `entry_selection_policy.enabled` | bool(True) | 0 | - |
| `entry_selection_policy.mode` | str("AUTO") | 0 | - |
| `entry_selection_policy.prefer` | str("HIGHER_SCORE") | 0 | - |
| `entry_selection_policy.surge_priority_first` | bool(False) | 0 | - |
| `entry_selection_policy.one_pick_when_max_new_le` | number(1) | 0 | - |
| `entry_selection_policy.skip_same_code_day_already_buy` | bool(False) | 0 | - |
| `entry_selection_policy.fallback_after_block.enabled` | bool(True) | 0 | - |
| `entry_selection_policy.fallback_after_block.max_candidates` | number(3) | 0 | - |
| `entry_selection_policy.fallback_after_block.when_max_new_le` | number(1) | 0 | - |
| `entry_selection_policy_by_run_label.main.mode` | str("AUTO") | 0 | - |
| `entry_selection_policy_by_run_label.main.one_pick_when_max_new_le` | number(1) | 0 | - |
| `entry_selection_policy_by_run_label.shadow.mode` | str("AUTO") | 0 | - |
| `entry_selection_policy_by_run_label.shadow.one_pick_when_max_new_le` | number(0) | 0 | - |
| `surge_exit_policy.enabled` | bool(True) | 0 | - |
| `surge_exit_policy.stop_loss_pct` | number(-0.03) | 0 | - |
| `surge_exit_policy.take_profit_pct` | number(0.1) | 0 | - |
| `surge_exit_policy.max_hold_days` | number(5) | 0 | - |
| `surge_exit_policy.stop_sell_ratio_pct` | number(40) | 0 | - |
| `surge_exit_policy.preemptive_sell_ratio_pct` | number(30) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.enabled` | bool(True) | 1 | paper_engine.py:17062 |
| `surge_exit_policy.dynamic_exit_ratio.base_ratios.LIGHT` | number(25) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.base_ratios.NORMAL` | number(50) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.base_ratios.HARD` | number(75) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.base_ratios.FORCE` | number(100) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.t1_surge_relax_one_level` | bool(True) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.risk_off_raise_one_level` | bool(True) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.reversal_raise_one_level` | bool(True) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.repeat_stop_raise_one_level` | bool(True) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.force_gap_loss_pct` | number(0.08) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.hard_gap_loss_pct` | number(0.05) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.hard_atr14_pct` | number(0.06) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.early_loss_hold_days` | number(1) | 1 | paper_engine.py:17312 |
| `surge_exit_policy.dynamic_exit_ratio.early_loss_hard_pct` | number(0.08) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.early_loss_force_pct` | number(0.12) | 0 | - |
| `surge_exit_policy.dynamic_exit_ratio.repeat_stop_force_exit` | bool(True) | 0 | - |
| `surge_exit_policy.reversal_exit.enabled` | bool(True) | 0 | - |
| `surge_exit_policy.reversal_exit.trigger_count` | number(2) | 0 | - |
| `surge_exit_policy.reversal_exit.volume_exhaustion_ratio_max` | number(0.6) | 0 | - |
| `surge_exit_policy.reversal_exit.high_rejection_min_drawdown_pct` | number(0) | 0 | - |
| `surge_exit_policy.reversal_exit.early_close_loss_pct` | number(0.08) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.enabled` | bool(True) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.min_points` | number(3) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.lookback_points` | number(5) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.trigger_count` | number(2) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.high_rejection_min_drawdown_pct` | number(0.025) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.consecutive_down_points` | number(3) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.volume_fade_ratio_max` | number(0.35) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.spread_bps_hard` | number(80) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.orderflow_risk_score_max` | number(0.6) | 0 | - |
| `surge_exit_policy.intraday_reversal_exit.require_high_rejection_for_exit` | bool(True) | 0 | - |
| `surge_exit_policy.preemptive_close_pct` | number(-0.03) | 0 | - |
| `dynamic_stop_loss.enabled` | bool(True) | 0 | - |
| `dynamic_stop_loss.break_even_trigger_profit_pct` | number(4) | 0 | - |
| `dynamic_stop_loss.break_even_stop_pct` | number(-0.002) | 0 | - |
| `dynamic_stop_loss.time_decay_start_days` | number(2) | 0 | - |
| `dynamic_stop_loss.time_decay_tighten_step_pct` | number(0.003) | 0 | - |
| `dynamic_stop_loss.time_decay_tighten_cap_pct` | number(3) | 0 | - |
| `dynamic_stop_loss.atr_ref_pct` | number(4) | 0 | - |
| `dynamic_stop_loss.atr_tighten_scale` | number(0.5) | 0 | - |
| `dynamic_stop_loss.atr_tighten_cap_pct` | number(3) | 0 | - |
| `dynamic_stop_loss.min_stop_pct` | number(-30) | 0 | - |
| `dynamic_stop_loss.max_stop_pct` | number(-0.001) | 0 | - |
| `stale_signal_replay.enabled` | bool(True) | 0 | - |
| `stale_signal_replay.min_signal_age_days` | number(2) | 0 | - |
| `stale_signal_replay.require_no_open_positions` | bool(True) | 0 | - |
| `stale_signal_replay.order_id_include_entry_day` | bool(True) | 0 | - |
| `drawdown_manager.enabled` | bool(True) | 0 | - |
| `drawdown_manager.use_debug_lifetime_mdd` | bool(False) | 0 | - |
| `drawdown_manager.stages` | list[4] | 0 | - |
| `drawdown_manager.hard_block_conditions.mdd_threshold` | number(0.36) | 0 | - |
| `drawdown_manager.hard_block_conditions.consecutive_loss_days` | number(5) | 0 | - |
| `drawdown_manager.hard_block_conditions.require_system_stress` | bool(True) | 0 | - |
| `drawdown_manager.hard_block_conditions.comment` | str("Hard block only on composite AND conditi...") | 0 | - |
| `drawdown_manager.liquidation_price` | str("close") | 0 | - |
| `drawdown_manager.consecutive_loss_days_threshold` | number(4) | 0 | - |
| `drawdown_manager.consecutive_loss_exposure_multiplier` | number(0.5) | 0 | - |
| `drawdown_manager.vix_proxy_threshold` | number(30) | 0 | - |
| `drawdown_manager.high_vol_position_size_multiplier` | number(0.7) | 0 | - |
| `drawdown_manager.sector_concentration_block` | bool(True) | 0 | - |
| `drawdown_manager.sector_concentration_limit` | number(1) | 0 | - |
| `drawdown_manager.min_positions_for_partial_liquidation` | number(4) | 0 | - |
| `fx_entry_policy.enabled` | bool(True) | 0 | - |
| `fx_entry_policy.weak_fx_export_bias_level` | number(1450) | 0 | - |
| `fx_entry_policy.strong_fx_domestic_bias_level` | number(1380) | 0 | - |
| `fx_entry_policy.crisis_fx_level` | number(1520) | 0 | - |
| `fx_entry_policy.daily_abs_change_block_level` | number(23) | 0 | - |
| `fx_entry_policy.hard_block_requires_crisis_level` | bool(True) | 0 | - |
| `fx_entry_policy.soft_avg_abs_change_threshold` | number(11) | 0 | - |
| `fx_entry_policy.soft_daily_abs_change_threshold` | number(23) | 0 | - |
| `fx_entry_policy.caution_avg_abs_change_threshold` | number(11) | 0 | - |
| `fx_entry_policy.three_day_extreme_force_defensive` | bool(True) | 0 | - |
| `p1_entry_policy.enabled` | bool(True) | 0 | - |
| `p1_entry_policy.calendar.enabled` | bool(True) | 1 | paper_engine.py:4357 |
| `p1_entry_policy.calendar.block_dates` | list[0] | 1 | paper_engine.py:4362 |
| `p1_entry_policy.calendar.reduce_dates` | list[0] | 1 | paper_engine.py:4363 |
| `p1_entry_policy.calendar.reduce_max_new_cap` | number(1) | 1 | paper_engine.py:4370 |
| `p1_entry_policy.calendar.option_expiry_reduce_enabled` | bool(False) | 1 | paper_engine.py:4374 |
| `p1_entry_policy.calendar.option_expiry_week_of_month` | number(2) | 1 | paper_engine.py:4378 |
| `p1_entry_policy.calendar.option_expiry_weekday` | number(3) | 1 | paper_engine.py:4379 |
| `p1_entry_policy.calendar.option_expiry_max_new_cap` | number(1) | 1 | paper_engine.py:4382 |
| `p1_entry_policy.intraday.enabled` | bool(True) | 3 | paper_engine.py:3758, paper_engine.py:4396, paper_engine.py:17179 |
| `p1_entry_policy.intraday.morning_start_hhmm` | number(900) | 1 | paper_engine.py:4401 |
| `p1_entry_policy.intraday.morning_end_hhmm` | number(1000) | 1 | paper_engine.py:4402 |
| `p1_entry_policy.intraday.morning_sector_strength_min` | number(0.8) | 1 | paper_engine.py:4412 |
| `p1_entry_policy.intraday.lunch_start_hhmm` | number(1130) | 1 | paper_engine.py:4403 |
| `p1_entry_policy.intraday.lunch_end_hhmm` | number(1330) | 1 | paper_engine.py:4404 |
| `p1_entry_policy.intraday.lunch_max_new_cap` | number(1) | 1 | paper_engine.py:4430 |
| `p1_entry_policy.intraday.power_hour_start_hhmm` | number(1430) | 0 | - |
| `p1_entry_policy.intraday.power_hour_end_hhmm` | number(1530) | 0 | - |
| `p1_entry_policy.event_gate.enabled` | bool(True) | 0 | - |
| `p1_entry_policy.event_gate.events_file` | str("E:\1_Data\2_Logs\market_event_gate_lates...") | 0 | - |
| `p1_entry_policy.event_gate.auto_stub_when_missing` | bool(True) | 0 | - |
| `p1_entry_policy.event_gate.explicit_market_event_guard.enabled` | bool(True) | 2 | paper_engine.py:4468, paper_engine.py:4544 |
| `p1_entry_policy.event_gate.explicit_market_event_guard.block_event_types` | list[5] | 0 | - |
| `p1_entry_policy.event_gate.explicit_market_event_guard.market_wide_event_types` | list[3] | 0 | - |
| `p1_entry_policy.event_gate.high_risk_levels` | list[2] | 0 | - |
| `p1_entry_policy.event_gate.high_risk_action` | str("REDUCE") | 0 | - |
| `p1_entry_policy.event_gate.high_risk_max_new_cap` | number(1) | 0 | - |
| `p1_entry_policy.technical_gate.enabled` | bool(True) | 0 | - |
| `p1_entry_policy.technical_gate.min_pool_size` | number(8) | 0 | - |
| `p1_entry_policy.technical_gate.rsi_min` | number(45) | 0 | - |
| `p1_entry_policy.technical_gate.rsi_max` | number(75) | 0 | - |
| `p1_entry_policy.technical_gate.rsi_ok_min_ratio` | number(0.45) | 0 | - |
| `p1_entry_policy.technical_gate.macd_golden_min_ratio` | number(0) | 0 | - |
| `p1_entry_policy.technical_gate.volcorr_min` | number(0.03) | 0 | - |
| `p1_entry_policy.technical_gate.volcorr_ok_min_ratio` | number(0.5) | 0 | - |
| `p1_entry_policy.technical_gate.stoch_k_min` | number(20) | 0 | - |
| `p1_entry_policy.technical_gate.stoch_k_max` | number(85) | 0 | - |
| `p1_entry_policy.technical_gate.stoch_ok_min_ratio` | number(0.5) | 0 | - |
| `p1_entry_policy.technical_gate.boll_mid_min` | number(0) | 0 | - |
| `p1_entry_policy.technical_gate.boll_mid_ok_min_ratio` | number(0.5) | 0 | - |
| `p1_entry_policy.technical_gate.obv_slope_min` | number(0) | 0 | - |
| `p1_entry_policy.technical_gate.obv_ok_min_ratio` | number(0.5) | 0 | - |
| `p1_entry_policy.technical_gate.sma_ema_ok_min_ratio` | number(0.5) | 0 | - |
| `p1_entry_policy.technical_gate.low_quality_action` | str("REDUCE") | 0 | - |
| `p1_entry_policy.technical_gate.low_quality_max_new_cap` | number(1) | 0 | - |
| `global_outlier_watcher.enabled` | bool(True) | 0 | - |
| `global_outlier_watcher.stale_max_age_days` | number(2) | 0 | - |
| `global_outlier_watcher.block_on_snapshot_missing` | bool(True) | 0 | - |
| `global_outlier_watcher.block_stage_statuses` | list[3] | 0 | - |
| `global_outlier_watcher.caution_stage_statuses` | list[2] | 0 | - |
| `global_outlier_watcher.ignore_stage_keys` | list[2] | 0 | - |
| `global_outlier_watcher.block_calc_issue_states` | list[1] | 0 | - |
| `global_outlier_watcher.ignore_blocking_issue_keys` | list[3] | 0 | - |
| `cross_source_integrity.enabled` | bool(True) | 0 | - |
| `cross_source_integrity.max_skew_days` | number(1) | 0 | - |
| `cross_source_integrity.block_on_missing_required` | bool(True) | 0 | - |
| `cross_source_integrity.required_sources` | list[2] | 0 | - |
| `cross_source_integrity.optional_sources` | list[3] | 0 | - |
| `sigma_outlier_guard.enabled` | bool(True) | 0 | - |
| `sigma_outlier_guard.lookback_files` | number(20) | 0 | - |
| `sigma_outlier_guard.min_history` | number(5) | 0 | - |
| `sigma_outlier_guard.zscore_caution` | number(3) | 0 | - |
| `sigma_outlier_guard.zscore_block` | number(5) | 0 | - |
| `sigma_outlier_guard.fallback_ratio_caution` | number(0.4) | 0 | - |
| `sigma_outlier_guard.fallback_ratio_block` | number(0.2) | 0 | - |
| `sigma_outlier_guard.check_param_ranges` | bool(True) | 0 | - |
| `sigma_outlier_guard.block_on_param_out_of_range` | bool(True) | 0 | - |
| `stable_params_quality_gate.require_promoted` | bool(True) | 0 | - |
| `stable_params_quality_gate.min_oos_trades` | number(20) | 0 | - |
| `stable_params_quality_gate.min_oos_pf` | number(1.0) | 0 | - |
| `stable_params_quality_gate.min_stable_score` | number(-20) | 0 | - |
| `stable_params_quality_gate.min_mean_pf` | number(1.0) | 0 | - |
| `execution_health_guard.enabled` | bool(True) | 0 | - |
| `execution_health_guard.block_lifecycle_statuses` | list[3] | 0 | - |
| `execution_health_guard.block_state_machine_statuses` | list[3] | 0 | - |
| `execution_health_guard.caution_symbol_stop_statuses` | list[3] | 0 | - |
| `execution_health_guard.slippage_lookback_rows` | number(30) | 0 | - |
| `execution_health_guard.slippage_bps_caution` | number(10) | 0 | - |
| `execution_health_guard.slippage_bps_block` | number(20) | 0 | - |
| `macro_news_guard.enabled` | bool(True) | 0 | - |
| `macro_news_guard.macro_use_critical_guard` | bool(True) | 0 | - |
| `macro_news_guard.macro_critical_bad_block` | number(1) | 0 | - |
| `macro_news_guard.unknown_critical_no_data_action` | str("CAUTION") | 0 | - |
| `macro_news_guard.unknown_critical_no_data_allowed_run_labels` | list[3] | 0 | - |
| `macro_news_guard.macro_stale_ratio_caution` | number(0.5) | 0 | - |
| `macro_news_guard.macro_stale_ratio_block` | number(0.8) | 0 | - |
| `macro_news_guard.news_quota_guard_block` | bool(True) | 0 | - |
| `macro_news_guard.news_quality_block_statuses` | list[3] | 0 | - |
| `macro_gate_policy.mode` | str("HARD") | 0 | - |
| `macro_gate_policy.hard_block_regimes` | list[2] | 0 | - |
| `macro_gate_policy.hard_block_when_risk_on_false` | bool(True) | 0 | - |
| `sell_rules.enabled` | bool(True) | 2 | paper_engine.py:18407, paper_engine.py:18483 |
| `sell_rules.stop_loss.default_pct` | number(-12) | 0 | - |
| `sell_rules.stop_loss.stop_sell_ratio_pct` | number(60) | 0 | - |
| `sell_rules.stop_loss.stop_gap_sell_ratio_pct` | number(70) | 0 | - |
| `sell_rules.stop_loss.preemptive_sell_ratio_pct` | number(25) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.성장주` | number(-12) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.가치주` | number(-8) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.테마주` | number(-6) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.배당주` | number(-8) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.경기민감주` | number(-10) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.경기방어주` | number(-7) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.GROWTH` | number(-12) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.VALUE` | number(-12) | 0 | - |
| `sell_rules.stop_loss.asset_type_overrides.THEME` | number(-10) | 0 | - |
| `sell_rules.stop_loss.trailing_stop_enabled` | bool(True) | 0 | - |
| `sell_rules.stop_loss.trailing_stop_activation_profit_pct` | number(12) | 0 | - |
| `sell_rules.stop_loss.trailing_stop_pct` | number(-10) | 0 | - |
| `sell_rules.stop_loss.preemptive_close_enabled` | bool(True) | 0 | - |
| `sell_rules.stop_loss.preemptive_close_pct` | number(-8) | 0 | - |
| `sell_rules.take_profit.enabled` | bool(True) | 0 | - |
| `sell_rules.take_profit.levels` | list[3] | 1 | paper_engine.py:4230 |
| `sell_rules.take_profit.ratios` | list[3] | 1 | paper_engine.py:4231 |
| `sell_rules.take_profit.asset_type_adjustments.성장주` | number(0.7) | 0 | - |
| `sell_rules.take_profit.asset_type_adjustments.가치주` | number(1.2) | 0 | - |
| `sell_rules.take_profit.asset_type_adjustments.테마주` | number(1.5) | 0 | - |
| `sell_rules.take_profit.asset_type_adjustments.배당주` | number(0.5) | 0 | - |
| `sell_rules.fundamental_risk.enabled` | bool(True) | 0 | - |
| `sell_rules.fundamental_risk.critical_debt_ratio` | number(300) | 0 | - |
| `sell_rules.fundamental_risk.critical_roe` | number(-15) | 0 | - |
| `sell_rules.fundamental_risk.critical_revenue_growth_yoy` | number(-20) | 0 | - |
| `sell_rules.fundamental_risk.warning_operating_margin` | number(5) | 0 | - |
| `sell_rules.fundamental_risk.dividend_yield_floor` | number(3) | 0 | - |
| `sell_rules.fundamental_risk.warning_sell_ratio` | number(50) | 0 | - |
| `sell_rules.market_risk.enabled` | bool(True) | 0 | - |
| `sell_rules.market_risk.vix_high` | number(30) | 0 | - |
| `sell_rules.market_risk.vix_extreme` | number(40) | 0 | - |
| `sell_rules.market_risk.usd_krw_volatility` | number(15) | 0 | - |
| `sell_rules.market_risk.oil_shock_high` | number(0.45) | 0 | - |
| `sell_rules.market_risk.oil_shock_extreme` | number(0.75) | 0 | - |
| `sell_rules.market_risk.high_sell_ratio_profit` | number(30) | 0 | - |
| `sell_rules.market_risk.extreme_sell_ratio_profit` | number(70) | 0 | - |
| `sell_rules.market_risk.extreme_sell_ratio_loss` | number(30) | 0 | - |
| `sell_rules.candidate_dropout.enabled` | bool(False) | 1 | paper_engine.py:17524 |
| `sell_rules.candidate_dropout.min_hold_days` | number(2) | 1 | paper_engine.py:17525 |
| `sell_rules.candidate_dropout.sell_ratio_pct` | number(30) | 1 | paper_engine.py:17539 |
| `sell_rules.technical.enabled` | bool(True) | 1 | paper_engine.py:4565 |
| `sell_rules.technical.rsi_overbought` | number(75) | 0 | - |
| `sell_rules.technical.ma_periods` | list[3] | 0 | - |
| `sell_rules.technical.high_score_threshold` | number(60) | 0 | - |
| `sell_rules.technical.mid_score_threshold` | number(40) | 0 | - |
| `sell_rules.technical.high_score_sell_ratio` | number(50) | 0 | - |
| `sell_rules.technical.mid_score_sell_ratio` | number(30) | 0 | - |
| `sell_rules.asset_type_rules.테마주.trailing_stop_pct` | number(-8) | 0 | - |
| `sell_rules.asset_type_rules.테마주.take_profit_levels` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.테마주.take_profit_ratios` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.테마주.max_hold_days` | number(7) | 0 | - |
| `sell_rules.asset_type_rules.경기민감주.trailing_stop_pct` | number(-12) | 0 | - |
| `sell_rules.asset_type_rules.경기민감주.take_profit_levels` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.경기민감주.take_profit_ratios` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.경기민감주.max_hold_days` | number(10) | 0 | - |
| `sell_rules.asset_type_rules.성장주.trailing_stop_pct` | number(-15) | 0 | - |
| `sell_rules.asset_type_rules.성장주.take_profit_levels` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.성장주.take_profit_ratios` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.성장주.max_hold_days` | number(20) | 0 | - |
| `sell_rules.asset_type_rules.가치주.trailing_stop_pct` | number(-10) | 0 | - |
| `sell_rules.asset_type_rules.가치주.take_profit_levels` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.가치주.take_profit_ratios` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.가치주.max_hold_days` | number(25) | 0 | - |
| `sell_rules.asset_type_rules.배당주.trailing_stop_pct` | number(-8) | 0 | - |
| `sell_rules.asset_type_rules.배당주.take_profit_levels` | list[1] | 0 | - |
| `sell_rules.asset_type_rules.배당주.take_profit_ratios` | list[1] | 0 | - |
| `sell_rules.asset_type_rules.배당주.dividend_yield_floor` | number(3) | 1 | paper_engine.py:4280 |
| `sell_rules.asset_type_rules.배당주.max_hold_days` | number(30) | 0 | - |
| `sell_rules.asset_type_rules.경기방어주.trailing_stop_pct` | number(-8) | 0 | - |
| `sell_rules.asset_type_rules.경기방어주.take_profit_levels` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.경기방어주.take_profit_ratios` | list[3] | 0 | - |
| `sell_rules.asset_type_rules.경기방어주.max_hold_days` | number(20) | 0 | - |
| `stop_loss_pct` | number(-0.05) | 19 | paper_engine.py:3381, paper_engine.py:3577, paper_engine.py:5630, paper_engine.py:5631, paper_engine.py:10286, paper_engine.py:16982, paper_engine.py:17033, paper_engine.py:17240, paper_engine.py:18366, paper_engine.py:18469 ... |
| `risk_cap_per_trade_pct` | number(0.004) | 2 | paper_engine.py:15309, paper_engine.py:15312 |
| `gap_risk_multiplier` | number(2.5) | 2 | paper_engine.py:15310, paper_engine.py:15318 |
| `min_entry_score` | number(0.02) | 14 | paper_engine.py:12278, paper_engine.py:12279, paper_engine.py:12288, paper_engine.py:12289, paper_engine.py:12298, paper_engine.py:12301, paper_engine.py:12303, paper_engine.py:12304, paper_engine.py:12306, paper_engine.py:12310 ... |
| `sector_entry_fallback_top1` | bool(False) | 1 | paper_engine.py:12335 |
| `min_entry_score_policy.enabled` | bool(True) | 0 | - |
| `min_entry_score_policy.quantile` | number(0.6) | 0 | - |
| `min_entry_score_policy.min_floor` | number(0.015) | 0 | - |
| `min_entry_score_policy.max_cap` | number(0.05) | 0 | - |
| `min_entry_score_policy.min_candidates` | number(5) | 0 | - |
| `min_entry_score_policy.allow_fallback_top1` | bool(False) | 0 | - |
| `capital_budget_policy.enabled` | bool(True) | 1 | paper_engine.py:18922 |
| `capital_budget_policy.gross_exposure_pct` | number(0.55) | 1 | paper_engine.py:18926 |
| `capital_budget_policy.basic_alloc_pct` | number(0.15) | 1 | paper_engine.py:18932 |
| `capital_budget_policy.basic_target_positions` | number(18) | 0 | - |
| `capital_budget_policy.surge_alloc_pct` | number(0.15) | 1 | paper_engine.py:18933 |
| `capital_budget_policy.split_alloc_pct` | number(0.4) | 1 | paper_engine.py:18934 |
| `capital_budget_policy.recovery_alloc_pct` | number(0.1) | 1 | paper_engine.py:18935 |
| `capital_budget_policy.reserve_alloc_pct` | number(0.2) | 1 | paper_engine.py:18936 |
| `capital_budget_policy.caution_gross_exposure_pct` | number(0.55) | 1 | paper_engine.py:19345 |
| `capital_budget_policy.recovery_enabled` | bool(False) | 0 | - |
| `capital_budget_policy.reserve_trade_enabled` | bool(False) | 0 | - |
| `capital_budget_policy.defensive_floor_exposure_pct` | number(0.45) | 2 | paper_engine.py:18956, paper_engine.py:18982 |
| `capital_budget_policy.defensive_max_new` | number(0) | 2 | paper_engine.py:18963, paper_engine.py:18989 |
| `paper_validation_sample_policy.enabled` | bool(True) | 1 | paper_engine.py:19059 |
| `paper_validation_sample_policy.scope` | str("paper_virtual_only") | 0 | - |
| `paper_validation_sample_policy.allowed_run_labels` | list[3] | 1 | paper_engine.py:19063 |
| `paper_validation_sample_policy.allow_entry_gate_decisions` | list[2] | 1 | paper_engine.py:19070 |
| `paper_validation_sample_policy.require_account_risk_clear` | bool(True) | 1 | paper_engine.py:19075 |
| `paper_validation_sample_policy.require_no_es_hard_block` | bool(True) | 1 | paper_engine.py:19077 |
| `paper_validation_sample_policy.require_no_dd_stop_triggered` | bool(True) | 1 | paper_engine.py:19079 |
| `paper_validation_sample_policy.min_scale_floor` | number(0) | 1 | paper_engine.py:19082 |
| `sector_correlation_guard.enabled` | bool(True) | 0 | - |
| `sector_correlation_guard.lookback_days` | number(120) | 0 | - |
| `sector_correlation_guard.risk_budget_engine` | str("EWMA_HRP") | 0 | - |
| `sector_correlation_guard.ewma_halflife_days` | number(60) | 0 | - |
| `sector_correlation_guard.hrp_enabled` | bool(True) | 0 | - |
| `sector_correlation_guard.hrp_overweight_tolerance` | number(1.1) | 0 | - |
| `sector_correlation_guard.hrp_min_qty_multiplier` | number(0.35) | 0 | - |
| `sector_correlation_guard.reduce_threshold_abs_corr` | number(0.85) | 0 | - |
| `sector_correlation_guard.reduce_qty_multiplier` | number(0.75) | 0 | - |
| `sector_correlation_guard.allow_block` | bool(False) | 0 | - |
| `entry_overheat_policy.enabled` | bool(True) | 0 | - |
| `entry_overheat_policy.apply_to_normal` | bool(True) | 0 | - |
| `entry_overheat_policy.apply_to_surge` | bool(True) | 0 | - |
| `entry_overheat_policy.v_accel_threshold` | number(3.1520902710607213) | 0 | - |
| `entry_overheat_policy.ret1_pct_threshold` | number(8.849700179324415) | 0 | - |
| `entry_overheat_policy.atr14_pct_threshold` | number(0.09907047892735515) | 0 | - |
| `entry_overheat_policy.trigger` | str("v_accel") | 0 | - |
| `entry_overheat_policy.reduce_multiplier` | number(0.5) | 0 | - |
| `sector_rebalance.enabled` | bool(False) | 0 | - |
| `sector_rebalance.limit_pct` | number(0.4) | 0 | - |
| `sector_rebalance.sell_ratio_pct` | number(30) | 0 | - |
| `horizon_entry_policy.enabled` | bool(True) | 0 | - |
| `horizon_entry_policy.default_label` | str("MID") | 0 | - |
| `horizon_entry_policy.label_rules.SHORT.min_final_score` | number(0.03) | 0 | - |
| `horizon_entry_policy.label_rules.SHORT.min_v_accel` | number(1) | 0 | - |
| `horizon_entry_policy.label_rules.SHORT.min_rs_slope` | number(6) | 0 | - |
| `horizon_entry_policy.label_rules.SHORT.max_hold_days` | number(7) | 0 | - |
| `horizon_entry_policy.label_rules.SHORT.entry_weight` | number(0.8) | 0 | - |
| `horizon_entry_policy.label_rules.SWING.min_final_score` | number(0.02) | 0 | - |
| `horizon_entry_policy.label_rules.SWING.min_atr14_pct` | number(0.06) | 0 | - |
| `horizon_entry_policy.label_rules.SWING.min_stoch_k` | number(65) | 0 | - |
| `horizon_entry_policy.label_rules.SWING.max_hold_days` | number(12) | 0 | - |
| `horizon_entry_policy.label_rules.SWING.entry_weight` | number(0.7) | 0 | - |
| `horizon_entry_policy.label_rules.MID.min_final_score` | number(0.02) | 0 | - |
| `horizon_entry_policy.label_rules.MID.max_hold_days` | number(20) | 0 | - |
| `horizon_entry_policy.label_rules.MID.entry_weight` | number(1) | 0 | - |
| `horizon_entry_policy.label_rules.LONG.min_final_score` | number(0.015) | 0 | - |
| `horizon_entry_policy.label_rules.LONG.max_atr14_pct` | number(0.08) | 0 | - |
| `horizon_entry_policy.label_rules.LONG.max_hold_days` | number(30) | 0 | - |
| `horizon_entry_policy.label_rules.LONG.entry_weight` | number(1) | 0 | - |
| `backtest_validation_guard.enabled` | bool(True) | 0 | - |
| `backtest_validation_guard.stale_max_age_days` | number(3) | 0 | - |
| `backtest_validation_guard.block_gate_names` | list[2] | 0 | - |
| `backtest_validation_guard.caution_gate_names` | list[0] | 0 | - |
| `backtest_validation_guard.caution_on_overall_fail` | bool(False) | 0 | - |
| `backtest_validation_guard.caution_affects_entry` | bool(False) | 0 | - |
| `backtest_validation_guard.block_on_missing` | bool(False) | 0 | - |
| `news_implication_entry_policy.enabled` | bool(True) | 0 | - |
| `news_implication_entry_policy.reduce_size_observe_only` | bool(True) | 0 | - |
| `news_implication_entry_policy.reduce_size_multiplier` | number(0.5) | 0 | - |
| `news_implication_entry_policy.watch_observe_only` | bool(True) | 0 | - |
| `news_implication_entry_policy.block_observe_only` | bool(False) | 0 | - |
| `news_implication_entry_policy.lock_observe_only` | bool(True) | 0 | - |
| `entry_signal_date_top_score_cap.enabled` | bool(True) | 0 | - |
| `entry_signal_date_top_score_cap.top_n` | number(12) | 1 | paper_engine.py:13873 |
| `entry_signal_date_top_score_cap.fallback_when_selected_pool_has_no_top_overlap` | bool(True) | 0 | - |
| `entry_signal_date_top_score_cap.fallback_require_positive_entry` | bool(True) | 0 | - |
| `entry_signal_date_top_score_cap.fallback_if_pool_lt_n` | bool(True) | 0 | - |
| `positive_entry_criteria.enabled` | bool(True) | 0 | - |
| `positive_entry_criteria.hard_filter` | bool(True) | 0 | - |
| `positive_entry_criteria.min_score` | number(0) | 0 | - |
| `positive_entry_criteria.score_columns` | list[2] | 0 | - |
| `positive_entry_criteria.require_execution_pool_when_present` | bool(False) | 1 | paper_engine.py:12247 |
| `positive_entry_criteria.require_sector_entry_when_present` | bool(True) | 0 | - |
| `positive_entry_criteria.allow_sector_union` | bool(True) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.enabled` | bool(False) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.trigger_when_fresh_ok_lte` | number(0) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.max_candidates` | number(1) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.min_final_score` | number(0.1) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.min_sector_strength` | number(0.4) | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.candidate_origin` | str("SECTOR_PREFILTER_UNION") | 0 | - |
| `positive_entry_criteria.fresh_sector_allowed_fallback.allowed_sector_actions` | list[2] | 0 | - |
| `normal_intraday_realtime_policy.enabled` | bool(True) | 0 | - |
| `normal_intraday_realtime_policy.block_when_entry_gate_not_allow` | bool(True) | 0 | - |
| `normal_intraday_realtime_policy.blocked_entry_gate_decisions` | list[1] | 0 | - |
| `normal_intraday_realtime_policy.allow_dd_stop_validation_reduce` | bool(False) | 0 | - |
| `normal_intraday_realtime_policy.exclude_reduced_entry_fills_in_validation` | bool(True) | 0 | - |
| `normal_intraday_realtime_policy.block_when_p0_rolling_dd_ge_threshold` | bool(False) | 0 | - |
| `normal_intraday_realtime_policy.p0_rolling_dd_block_pct` | number(0.1) | 0 | - |
| `entry_gap_up_reduce.enabled` | bool(True) | 0 | - |
| `entry_gap_up_reduce.threshold_pct` | number(0.02) | 0 | - |
| `entry_gap_up_reduce.qty_multiplier` | number(0.5) | 0 | - |
| `entry_gap_up_reduce.min_qty` | number(1) | 0 | - |
| `entry_gap_up_reduce.apply_to_surge_no_lob_probe` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.enabled` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.shadow_only` | bool(False) | 0 | - |
| `intraday_residual_overnight_guard.scope` | str("intraday_realtime") | 0 | - |
| `intraday_residual_overnight_guard.trigger_on_same_day_loss` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.apply_to_surge` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.apply_to_non_surge` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.exit_before_overnight` | bool(True) | 0 | - |
| `intraday_residual_overnight_guard.evidence_artifact` | str("2_Logs/intraday_residual_overnight_risk_...") | 0 | - |
| `normal_entry_execution_quality.enabled` | bool(True) | 0 | - |
| `normal_entry_execution_quality.require_lob` | bool(True) | 0 | - |
| `normal_entry_execution_quality.max_spread_bps` | number(30) | 0 | - |
| `normal_entry_execution_quality.require_executable_qty` | bool(True) | 0 | - |
| `normal_entry_execution_quality.min_executable_qty_ratio` | number(1) | 0 | - |
| `normal_entry_execution_quality.max_depth_levels` | number(10) | 0 | - |
| `normal_entry_execution_quality.min_markout_1step_bps` | number(0) | 0 | - |
| `normal_entry_execution_quality.block_on_missing_markout` | bool(True) | 0 | - |
| `normal_entry_execution_quality.orderflow_tag_block_tags` | list[2] | 0 | - |
| `normal_entry_execution_quality.qty_reduction.enabled` | bool(False) | 0 | - |
| `normal_entry_execution_quality.qty_reduction.spread_bands` | list[2] | 0 | - |
| `normal_entry_execution_quality.qty_reduction.markout_bands` | list[1] | 0 | - |
| `normal_entry_execution_quality.qty_reduction.orderflow_caution_multiplier` | number(0.75) | 0 | - |
| `normal_entry_execution_quality.qty_reduction.orderflow_caution_reduce_tags` | list[1] | 0 | - |
| `normal_entry_execution_quality.qty_reduction.multi_condition.two_or_more_multiplier` | number(0.5) | 0 | - |
| `normal_candidate_staleness_check.enabled` | bool(True) | 0 | - |
| `normal_candidate_staleness_check.max_stale_days` | number(3) | 0 | - |
| `normal_candidate_staleness_check.block_if_stale` | bool(False) | 0 | - |
| `normal_realtime_gap_policy.enabled` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.close_auction.enabled` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.close_auction.block_close_pos_min` | number(0.5) | 0 | - |
| `normal_realtime_gap_policy.close_auction.reduce_close_pos_min` | number(0.7) | 0 | - |
| `normal_realtime_gap_policy.close_auction.max_day_range_pct` | number(0.07) | 0 | - |
| `normal_realtime_gap_policy.close_auction.block_v_accel_min` | number(1) | 0 | - |
| `normal_realtime_gap_policy.close_auction.reduce_v_accel_min` | number(1.5) | 0 | - |
| `normal_realtime_gap_policy.close_auction.reduce_qty_multiplier` | number(0.5) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.enabled` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.require_intraday` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.block_v_accel_min` | number(1) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.reduce_v_accel_min` | number(1.5) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.min_value_ratio` | number(0.7) | 0 | - |
| `normal_realtime_gap_policy.intraday_momentum_recheck.reduce_qty_multiplier` | number(0.5) | 0 | - |
| `normal_realtime_gap_policy.overnight_gap_exit.enabled` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.overnight_gap_exit.gap_down_exit_pct` | number(0.03) | 0 | - |
| `normal_realtime_gap_policy.overnight_gap_exit.gap_up_take_profit_pct` | number(0.12) | 0 | - |
| `normal_realtime_gap_policy.overnight_gap_exit.gap_down_sell_ratio_pct` | number(100) | 0 | - |
| `normal_realtime_gap_policy.overnight_gap_exit.gap_up_sell_ratio_pct` | number(100) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.enabled` | bool(True) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.low_trading_value_krw` | number(3000000000) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.very_low_trading_value_krw` | number(1000000000) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.low_trading_value_multiplier` | number(1.5) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.very_low_trading_value_multiplier` | number(2) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.low_turnover_ratio` | number(0.003) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.low_turnover_multiplier` | number(1.5) | 0 | - |
| `normal_realtime_gap_policy.dynamic_slippage.max_slippage_pct` | number(0.03) | 0 | - |

## Unused config keys

- `tiered_slippage.large_cap_krw`
- `tiered_slippage.mid_cap_krw`
- `tiered_slippage.large_slip_pct`
- `tiered_slippage.mid_slip_pct`
- `tiered_slippage.small_slip_pct`
- `entry_liquidity_check.enabled`
- `entry_liquidity_check.min_trading_value_krw`
- `limit_price_tolerance.enabled`
- `limit_price_tolerance.max_deviation_pct`
- `split_entry.first_ratio`
- `split_entry.second_dip_min_pct`
- `split_entry.second_dip_max_pct`
- `split_entry.second_entry_max_days`
- `split_entry.budget_alloc_pct`
- `split_entry.allow_split_second_carryover`
- `split_entry.max_open_to_entry_chase_pct`
- `split_entry.second_confirmation.enabled`
- `split_entry.second_confirmation.max_v_accel`
- `split_entry.second_confirmation.max_ret1_pct`
- `split_entry.second_confirmation.max_atr14_pct`
- `split_entry.second_confirmation.missing_feature_action`
- `split_entry.second_confirmation.require_ma60_support_bounce`
- `parquet_search.root`
- `parquet_search.top_n_recent`
- `parquet_search.max_open_files`
- `engine_log_dir`
- `cash_per_trade`
- `kill_switch.min_new_trades_per_day`
- `crash_risk_off.enabled`
- `crash_risk_off.index_market`
- `crash_risk_off.index_name_contains`
- `crash_risk_off.lookback_days`
- `crash_risk_off.rv20_floor`
- `crash_risk_off.rv20_spike_ratio`
- `crash_risk_off.day_drop_pct`
- `crash_risk_off.gap_down_pct`
- `crash_risk_off.reduce_factor`
- `crash_risk_off.min_new_trades_per_day`
- `crash_risk_off.index_code`
- `crash_risk_off.fallback_trigger_max_dd_pct`
- `crash_risk_off.fallback_trigger_day_ret_pct`
- `crash_risk_off.trigger_max_dd_pct`
- `crash_risk_off.trigger_day_ret_pct`
- `entry_gap_risk_guard.enabled`
- `entry_gap_risk_guard.lookback_sessions`
- `entry_gap_risk_guard.down_gap_threshold_pct`
- `entry_gap_risk_guard.max_down_gap_count`
- `hold_close_drop_guard.enabled`
- `hold_close_drop_guard.drop_pct`
- `hold_close_drop_guard.sell_ratio_pct`
- `hold_close_drop_guard.min_hold_days`
- `regime_entry_policy.rate_hike_fear_reduce_day_ret_floor`
- `regime_entry_policy.rally_gap_up_max_pct`
- `regime_entry_policy.p0_bear_promote_enabled`
- `regime_entry_policy.p0_bear_allowed_macro_regimes`
- `beta_harvest.enabled`
- `beta_harvest.mode`
- `beta_harvest.paper_only`
- `beta_harvest.order_type`
- `beta_harvest.etfs.KOSPI`
- `beta_harvest.etfs.KOSDAQ`
- `beta_harvest.largecaps.KOSPI`
- `beta_harvest.largecaps.KOSDAQ`
- `beta_harvest.allocation.KOSPI`
- `beta_harvest.allocation.KOSDAQ`
- `beta_harvest.rebalance_freq`
- `beta_harvest.use_regime_scaling`
- `beta_harvest.regime_fallback`
- `beta_harvest.min_order_amount_krw`
- `beta_harvest.max_single_etf_weight`
- `beta_harvest.order_output_dir`
- `beta_harvest.position_ledger_path`
- `regime_overrides.RALLY.max_gross_exposure_pct`
- `regime_overrides.RALLY.max_daily_new_exposure_pct`
- `regime_overrides.RALLY.gap_up_max_pct`
- `regime_overrides.RALLY.stop_loss_pct`
- `regime_overrides.RALLY.surge_entry_policy.total_alloc_pct`
- `regime_overrides.RALLY.surge_entry_policy.max_new_surge`
- `regime_overrides.RALLY.surge_entry_policy.type_policy.afternoon_session_filter.enabled`
- `regime_overrides.RALLY.surge_exit_policy.stop_loss_pct`
- `regime_overrides.RALLY.surge_exit_policy.max_hold_days`
- `regime_overrides.RALLY.sell_rules.stop_loss.trailing_stop_activation_profit_pct`
- `regime_overrides.RALLY.sell_rules.stop_loss.trailing_stop_pct`
- `regime_overrides.RALLY.split_entry.second_confirmation.max_v_accel`
- `regime_overrides.NORMAL.max_hold_days`
- `regime_overrides.NORMAL.stop_loss_pct`
- `regime_overrides.NORMAL.split_entry.budget_alloc_pct`
- `regime_overrides.NORMAL.surge_entry_policy.total_alloc_pct`
- `regime_overrides.NORMAL.surge_entry_policy.max_new_surge`
- `regime_overrides.NORMAL.surge_entry_policy.type_policy.afternoon_session_filter.enabled`
- `regime_overrides.NORMAL.sell_rules.stop_loss.trailing_stop_activation_profit_pct`
- `regime_overrides.NORMAL.sell_rules.stop_loss.trailing_stop_pct`
- `regime_overrides.BEAR.max_hold_days`
- `regime_overrides.BEAR.stop_loss_pct`
- `regime_overrides.BEAR.split_entry.budget_alloc_pct`
- `regime_overrides.BEAR.surge_entry_policy.total_alloc_pct`
- `regime_overrides.BEAR.surge_entry_policy.max_new_surge`
- `regime_overrides.BEAR.surge_entry_policy.type_policy.afternoon_session_filter.enabled`
- `regime_overrides.BEAR.sell_rules.stop_loss.trailing_stop_activation_profit_pct`
- `regime_overrides.BEAR.sell_rules.stop_loss.trailing_stop_pct`
- `regime_overrides.CRASH.max_new_trades_per_day`
- `regime_overrides.CRASH.max_gross_exposure_pct`
- `regime_overrides.CRASH.max_daily_new_exposure_pct`
- `regime_overrides.CRASH.split_entry.budget_alloc_pct`
- `regime_overrides.CRASH.surge_entry_policy.total_alloc_pct`
- `regime_overrides.CRASH.surge_entry_policy.max_new_surge`
- `trend_overlay_2026.enabled`
- `trend_overlay_2026.cutting_hawkish_max`
- `trend_overlay_2026.seasonal_risk_multiplier.Q1`
- `trend_overlay_2026.seasonal_risk_multiplier.Q2`
- `trend_overlay_2026.seasonal_risk_multiplier.Q3`
- `trend_overlay_2026.seasonal_risk_multiplier.Q4`
- `trend_overlay_2026.cutting_overlay.growth_entry_weight`
- `trend_overlay_2026.cutting_overlay.defensive_entry_weight`
- `trend_overlay_2026.cutting_overlay.growth_codes`
- `trend_overlay_2026.cutting_overlay.defensive_codes`
- `trend_overlay_2026.ai_semiconductor_overlay.daily_top_n`
- `trend_overlay_2026.ai_semiconductor_overlay.max_open_positions`
- `trend_overlay_2026.ai_semiconductor_overlay.single_name_cap_pct`
- `trend_overlay_2026.ai_semiconductor_overlay.semiconductor_codes`
- `trend_overlay_2026.ai_semiconductor_overlay.ai_software_codes`
- `trend_overlay_2026.ai_semiconductor_overlay.it_hardware_codes`
- `adaptive_entry_control.enabled`
- `adaptive_entry_control.kill_switch_override_block`
- `adaptive_entry_control.dd_ratio_soft`
- `adaptive_entry_control.dd_ratio_mid`
- `adaptive_entry_control.dd_ratio_hard`
- `adaptive_entry_control.reduce_soft`
- `adaptive_entry_control.reduce_mid`
- `adaptive_entry_control.reduce_hard`
- `adaptive_entry_control.probe_min_new`
- `adaptive_entry_control.relief_after_streak_days`
- `adaptive_entry_control.relief_min_new`
- `adaptive_entry_control.dynamic_relax_l5_factor`
- `adaptive_good_stock_entry.enabled`
- `adaptive_good_stock_entry.policy_design_path`
- `adaptive_good_stock_entry.open_chase_probe_routes`
- `adaptive_good_stock_entry.intraday_momentum_probe_routes`
- `adaptive_good_stock_entry.gapup_wait_route`
- `adaptive_good_stock_entry.tier1_post_split_qty_multiplier`
- `adaptive_good_stock_entry.tier2_post_split_qty_multiplier`
- `adaptive_good_stock_entry.pullback_post_split_qty_multiplier`
- `adaptive_good_stock_entry.tier1_gapup_override_max_pct`
- `adaptive_good_stock_entry.tier2_gapup_override_max_pct`
- `risk_orchestration.lookback_trades`
- `risk_orchestration.target_vol`
- `risk_orchestration.f_kelly_cap`
- `risk_orchestration.c_default`
- `risk_orchestration.c_min`
- `risk_orchestration.c_max`
- `risk_orchestration.vol_ratio_cap`
- `risk_orchestration.max_scale`
- `risk_orchestration.edge_zero_floor_scale`
- `risk_orchestration.regime_confidence.RALLY`
- `risk_orchestration.regime_confidence.NORMAL`
- `risk_orchestration.regime_confidence.BEAR`
- `risk_orchestration.regime_confidence.CRASH`
- `risk_orchestration.regime_confidence.STAGFLATION`
- `risk_orchestration.regime_confidence.RATE_HIKE_FEAR`
- `risk_orchestration.dd_taper.dd_cap`
- `risk_orchestration.dd_taper.dd_stop`
- `risk_orchestration.dd_taper.account_clear_strategy_dd_override.enabled`
- `risk_orchestration.dd_taper.account_clear_strategy_dd_override.scope`
- `risk_orchestration.dd_taper.account_clear_strategy_dd_override.reason`
- `risk_orchestration.dd_stop_validation.mode`
- `risk_orchestration.dd_stop_validation.max_new`
- `risk_orchestration.dd_stop_validation.max_new_surge`
- `risk_orchestration.dd_stop_validation.position_size_multiplier`
- `risk_orchestration.dd_stop_validation.allow_reasons`
- `risk_orchestration.dd_stop_validation.allowed_run_labels`
- `risk_orchestration.dd_stop_validation.max_positions_full_override.enabled`
- `risk_orchestration.dd_stop_validation.max_positions_full_override.mode`
- `risk_orchestration.dd_stop_validation.require_dd_stop`
- `risk_orchestration.es_gate.enabled`
- `risk_orchestration.es_gate.limit`
- `risk_orchestration.es_gate.hard_block`
- `risk_orchestration.es_gate.reduction_factor`
- `risk_orchestration.es_gate.by_regime.CRASH.limit`
- `risk_orchestration.es_gate.by_regime.CRASH.hard_block`
- `risk_orchestration.es_gate.by_regime.CRASH.reduction_factor`
- `risk_orchestration.es_gate.by_regime.STAGFLATION.limit`
- `risk_orchestration.es_gate.by_regime.STAGFLATION.hard_block`
- `risk_orchestration.es_gate.by_regime.STAGFLATION.reduction_factor`
- `risk_orchestration.es_gate.by_regime.BEAR.limit`
- `risk_orchestration.es_gate.by_regime.BEAR.hard_block`
- `risk_orchestration.es_gate.by_regime.BEAR.reduction_factor`
- `risk_orchestration.es_gate.by_regime.RALLY.limit`
- `risk_orchestration.es_gate.by_regime.RALLY.hard_block`
- `risk_orchestration.es_gate.by_regime.RALLY.reduction_factor`
- `risk_orchestration.es_gate.by_regime.NORMAL.limit`
- `risk_orchestration.es_gate.by_regime.NORMAL.hard_block`
- `risk_orchestration.es_gate.by_regime.NORMAL.reduction_factor`
- `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.limit`
- `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.hard_block`
- `risk_orchestration.es_gate.by_regime.RATE_HIKE_FEAR.reduction_factor`
- `risk_orchestration.kelly_role`
- `risk_orchestration.account_clear_min_scale.enabled`
- `risk_orchestration.account_clear_min_scale.min_scale`
- `risk_orchestration.account_clear_min_scale.require_dd_advisory_only`
- `production_risk_playbook.hard_entry_decision`
- `production_risk_playbook.hard_size_multiplier`
- `production_risk_playbook.drawdown_role`
- `production_risk_playbook.drawdown_owner`
- `production_risk_playbook.dd_pct.watch`
- `production_risk_playbook.dd_pct.soft`
- `production_risk_playbook.dd_pct.hard`
- `market_ops_policy.enabled`
- `market_ops_policy.slo_normal`
- `market_ops_policy.slo_rally_base`
- `market_ops_policy.slo_rally_per_ret`
- `market_ops_policy.slo_rally_cap`
- `market_ops_policy.slo_crash_base`
- `market_ops_policy.slo_crash_per_ret`
- `market_ops_policy.slo_crash_cap`
- `market_ops_policy.probe_rally_min`
- `market_ops_policy.probe_rally_ratio_base`
- `market_ops_policy.probe_rally_ratio_per_ret`
- `market_ops_policy.probe_rally_ratio_cap`
- `market_ops_policy.probe_crash_min`
- `market_ops_policy.probe_crash_ratio_base`
- `market_ops_policy.probe_crash_ratio_per_ret`
- `market_ops_policy.probe_crash_ratio_cap`
- `market_ops_policy.universe_shrink_min_candidates`
- `market_ops_policy.universe_shrink_min_price_codes`
- `market_ops_policy.universe_shrink_disable_signal_cap`
- `market_ops_policy.universe_shrink_disable_sector_cap`
- `market_ops_policy.universe_shrink_probe_uplift`
- `market_ops_policy.carryover_no_next_day_enabled`
- `market_ops_policy.carryover_max_age_days`
- `market_ops_policy.carryover_revalidate_score_gap_max`
- `market_ops_policy.carryover_revalidate_min_final_score`
- `market_ops_policy.entry_fallback_policy.enabled`
- `market_ops_policy.entry_fallback_policy.max_stage`
- `market_ops_policy.entry_fallback_policy.stage_gap_up_limits`
- `market_ops_policy.entry_fallback_policy.signal_valid_days`
- `market_ops_policy.entry_fallback_policy.auction_close_position_min`
- `market_ops_policy.entry_fallback_policy.auction_day_range_max_pct`
- `market_ops_policy.entry_fallback_policy.auction_v_accel_min`
- `market_ops_policy.entry_fallback_policy.next_open_limit_pct`
- `market_ops_policy.entry_fallback_policy.intraday_limit_pct`
- `market_ops_policy.entry_fallback_policy.next_open_gap_up_max_pct`
- `market_ops_policy.strict_same_day_only`
- `market_ops_policy.signal_ttl_minutes`
- `market_ops_policy.max_retry_per_code_per_day`
- `market_ops_policy.partial_fill_same_day_only`
- `market_ops_policy.exec_quality_max_slippage_pct`
- `market_ops_policy.close_cutoff_minutes`
- `market_ops_policy.quote_max_age_days`
- `market_ops_policy.fail_closed_propagate_block`
- `market_ops_policy.fail_closed_unfilled_ratio_threshold`
- `surge_entry_policy.enabled`
- `surge_entry_policy.realtime_only`
- `surge_entry_policy.top_n`
- `surge_entry_policy.min_score_final`
- `surge_entry_policy.max_new_surge`
- `surge_entry_policy.max_same_code_per_day`
- `surge_entry_policy.per_symbol_alloc_pct`
- `surge_entry_policy.total_alloc_pct`
- `surge_entry_policy.type_policy.type_qty_multiplier.PRICE_VOL_BREAKOUT`
- `surge_entry_policy.type_policy.type_qty_multiplier.REG_SHORT_5D60`
- `surge_entry_policy.type_policy.type_qty_multiplier.REG_MID_15D100`
- `surge_entry_policy.type_policy.type_qty_multiplier.PRICE_RANGE_BREAKOUT`
- `surge_entry_policy.type_policy.type_qty_multiplier.LIMIT_UP_NEAR`
- `surge_entry_policy.type_policy.type_qty_multiplier.OVERSOLD_REVERSAL`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.entry_timing`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.entry_timing`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.entry_timing`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.entry_timing`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.LIMIT_UP_NEAR.entry_timing`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.min_score_final`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.alloc_pct`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.first_ratio`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.stop_loss_pct`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.take_profit_pct`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.max_hold_days`
- `surge_entry_policy.type_policy.type_overrides.OVERSOLD_REVERSAL.entry_timing`
- `surge_entry_policy.market_event_guard.block_on_exclude_reasons`
- `surge_entry_policy.market_event_guard.blocked_terms`
- `surge_entry_policy.dynamic_max_new.hard_cap`
- `surge_entry_policy.dynamic_max_new.weak_cap`
- `surge_entry_policy.dynamic_max_new.medium_cap`
- `surge_entry_policy.dynamic_max_new.strong_cap`
- `surge_entry_policy.dynamic_max_new.min_score_final`
- `surge_entry_policy.dynamic_max_new.medium_score_final`
- `surge_entry_policy.dynamic_max_new.strong_score_final`
- `surge_entry_policy.dynamic_max_new.medium_rvol20_min`
- `surge_entry_policy.dynamic_max_new.strong_rvol20_min`
- `surge_entry_policy.dynamic_max_new.medium_spread_bps_max`
- `surge_entry_policy.dynamic_max_new.strong_spread_bps_max`
- `surge_entry_policy.dynamic_max_new.orderflow_risk_score_max`
- `surge_entry_policy.dynamic_max_new.no_lob_cap`
- `surge_entry_policy.no_lob_probe.enabled`
- `surge_entry_policy.no_lob_probe.max_selected`
- `surge_entry_policy.active_entry_gap_up_override.enabled`
- `surge_entry_policy.active_entry_gap_up_override.scope`
- `surge_entry_policy.active_entry_gap_up_override.max_gap_pct`
- `surge_entry_policy.active_entry_gap_up_override.max_spread_bps`
- `surge_entry_policy.active_entry_gap_up_override.min_markout_1step_bps`
- `surge_entry_policy.active_entry_gap_up_override.min_score_final`
- `surge_entry_policy.active_entry_gap_up_override.require_lob_ok`
- `surge_entry_policy.active_entry_gap_up_override.require_orderflow_tag_ok`
- `surge_entry_policy.active_entry_gap_up_override.require_paper_ready`
- `surge_entry_policy.active_entry_gap_up_override.require_paper_order_route`
- `surge_entry_policy.active_entry_gap_risk_override.enabled`
- `surge_entry_policy.active_entry_gap_risk_override.scope`
- `surge_entry_policy.active_entry_gap_risk_override.require_active_gap_up_override_pass`
- `surge_entry_policy.active_entry_gap_risk_override.max_down_gap_count`
- `surge_entry_policy.active_entry_gap_risk_override.min_current_gap_pct`
- `surge_entry_policy.paper_probe.max_selected`
- `surge_entry_policy.paper_probe.gap_up_override.enabled`
- `surge_entry_policy.paper_probe.gap_up_override.scope`
- `surge_entry_policy.paper_probe.gap_up_override.max_gap_pct`
- `surge_entry_policy.paper_probe.gap_up_override.max_spread_bps`
- `surge_entry_policy.paper_probe.gap_up_override.min_markout_1step_bps`
- `surge_entry_policy.paper_probe.gap_up_override.min_score_final`
- `surge_entry_policy.paper_probe.gap_up_override.require_lob_ok`
- `surge_entry_policy.paper_probe.gap_up_override.require_orderflow_tag_ok`
- `surge_entry_policy.paper_probe.gap_up_override.require_paper_ready`
- `surge_entry_policy.paper_probe.gap_up_override.require_paper_order_route`
- `surge_entry_policy.paper_probe.execution_quality_sizing.enabled`
- `surge_entry_policy.paper_probe.execution_quality_sizing.scope`
- `surge_entry_policy.paper_probe.execution_quality_sizing.lob_sweep_multiplier`
- `surge_entry_policy.paper_probe.execution_quality_sizing.no_history_multiplier`
- `surge_entry_policy.paper_probe.execution_quality_sizing.no_lob_fallback_multiplier`
- `surge_entry_policy.paper_probe.execution_quality_sizing.score_reduce_below`
- `surge_entry_policy.paper_probe.execution_quality_sizing.score_low_multiplier`
- `surge_entry_policy.paper_probe.execution_quality_sizing.rvol_bands`
- `surge_entry_policy.paper_probe.execution_quality_sizing.spread_bands`
- `surge_entry_policy.paper_probe.execution_quality_sizing.multi_condition.two_or_more_multiplier`
- `surge_entry_policy.paper_probe.execution_quality_sizing.multi_condition.three_or_more_multiplier`
- `surge_entry_policy.afternoon_session_filter.enabled`
- `surge_entry_policy.afternoon_session_filter.block_after_hhmm`
- `surge_entry_policy.afternoon_session_filter.blocked_types`
- `surge_entry_policy.wait_reclaim_paper_probe.enabled`
- `surge_entry_policy.wait_reclaim_paper_probe.scope`
- `surge_entry_policy.wait_reclaim_paper_probe.max_selected`
- `surge_entry_policy.wait_reclaim_paper_probe.min_change_pct`
- `surge_entry_policy.wait_reclaim_paper_probe.max_gap_pct`
- `surge_entry_policy.wait_reclaim_paper_probe.min_trading_value_krw`
- `surge_entry_policy.wait_reclaim_paper_probe.max_rvol20`
- `surge_entry_policy.wait_reclaim_paper_probe.max_spread_bps`
- `surge_entry_policy.wait_reclaim_paper_probe.min_markout_1step_bps`
- `surge_entry_policy.wait_reclaim_paper_probe.min_score_final`
- `surge_entry_policy.wait_reclaim_paper_probe.min_intraday_range_position_pct`
- `surge_entry_policy.wait_reclaim_paper_probe.max_intraday_high_drawdown_pct`
- `surge_entry_policy.wait_reclaim_paper_probe.require_lob_ok`
- `surge_entry_policy.wait_reclaim_paper_probe.require_orderflow_ok`
- `surge_entry_policy.wait_reclaim_paper_probe.allow_orderflow_no_history`
- `surge_entry_policy.wait_reclaim_paper_probe.broker_order_route`
- `surge_entry_policy.wait_reclaim_paper_probe.allowed_types`
- `surge_entry_policy.wait_reclaim_paper_probe.bypass_type_block`
- `entry_selection_policy.enabled`
- `entry_selection_policy.mode`
- `entry_selection_policy.prefer`
- `entry_selection_policy.surge_priority_first`
- `entry_selection_policy.one_pick_when_max_new_le`
- `entry_selection_policy.skip_same_code_day_already_buy`
- `entry_selection_policy.fallback_after_block.enabled`
- `entry_selection_policy.fallback_after_block.max_candidates`
- `entry_selection_policy.fallback_after_block.when_max_new_le`
- `entry_selection_policy_by_run_label.main.mode`
- `entry_selection_policy_by_run_label.main.one_pick_when_max_new_le`
- `entry_selection_policy_by_run_label.shadow.mode`
- `entry_selection_policy_by_run_label.shadow.one_pick_when_max_new_le`
- `surge_exit_policy.enabled`
- `surge_exit_policy.stop_loss_pct`
- `surge_exit_policy.take_profit_pct`
- `surge_exit_policy.max_hold_days`
- `surge_exit_policy.stop_sell_ratio_pct`
- `surge_exit_policy.preemptive_sell_ratio_pct`
- `surge_exit_policy.dynamic_exit_ratio.base_ratios.LIGHT`
- `surge_exit_policy.dynamic_exit_ratio.base_ratios.NORMAL`
- `surge_exit_policy.dynamic_exit_ratio.base_ratios.HARD`
- `surge_exit_policy.dynamic_exit_ratio.base_ratios.FORCE`
- `surge_exit_policy.dynamic_exit_ratio.t1_surge_relax_one_level`
- `surge_exit_policy.dynamic_exit_ratio.risk_off_raise_one_level`
- `surge_exit_policy.dynamic_exit_ratio.reversal_raise_one_level`
- `surge_exit_policy.dynamic_exit_ratio.repeat_stop_raise_one_level`
- `surge_exit_policy.dynamic_exit_ratio.force_gap_loss_pct`
- `surge_exit_policy.dynamic_exit_ratio.hard_gap_loss_pct`
- `surge_exit_policy.dynamic_exit_ratio.hard_atr14_pct`
- `surge_exit_policy.dynamic_exit_ratio.early_loss_hard_pct`
- `surge_exit_policy.dynamic_exit_ratio.early_loss_force_pct`
- `surge_exit_policy.dynamic_exit_ratio.repeat_stop_force_exit`
- `surge_exit_policy.reversal_exit.enabled`
- `surge_exit_policy.reversal_exit.trigger_count`
- `surge_exit_policy.reversal_exit.volume_exhaustion_ratio_max`
- `surge_exit_policy.reversal_exit.high_rejection_min_drawdown_pct`
- `surge_exit_policy.reversal_exit.early_close_loss_pct`
- `surge_exit_policy.intraday_reversal_exit.enabled`
- `surge_exit_policy.intraday_reversal_exit.min_points`
- `surge_exit_policy.intraday_reversal_exit.lookback_points`
- `surge_exit_policy.intraday_reversal_exit.trigger_count`
- `surge_exit_policy.intraday_reversal_exit.high_rejection_min_drawdown_pct`
- `surge_exit_policy.intraday_reversal_exit.consecutive_down_points`
- `surge_exit_policy.intraday_reversal_exit.volume_fade_ratio_max`
- `surge_exit_policy.intraday_reversal_exit.spread_bps_hard`
- `surge_exit_policy.intraday_reversal_exit.orderflow_risk_score_max`
- `surge_exit_policy.intraday_reversal_exit.require_high_rejection_for_exit`
- `surge_exit_policy.preemptive_close_pct`
- `dynamic_stop_loss.enabled`
- `dynamic_stop_loss.break_even_trigger_profit_pct`
- `dynamic_stop_loss.break_even_stop_pct`
- `dynamic_stop_loss.time_decay_start_days`
- `dynamic_stop_loss.time_decay_tighten_step_pct`
- `dynamic_stop_loss.time_decay_tighten_cap_pct`
- `dynamic_stop_loss.atr_ref_pct`
- `dynamic_stop_loss.atr_tighten_scale`
- `dynamic_stop_loss.atr_tighten_cap_pct`
- `dynamic_stop_loss.min_stop_pct`
- `dynamic_stop_loss.max_stop_pct`
- `stale_signal_replay.enabled`
- `stale_signal_replay.min_signal_age_days`
- `stale_signal_replay.require_no_open_positions`
- `stale_signal_replay.order_id_include_entry_day`
- `drawdown_manager.enabled`
- `drawdown_manager.use_debug_lifetime_mdd`
- `drawdown_manager.stages`
- `drawdown_manager.hard_block_conditions.mdd_threshold`
- `drawdown_manager.hard_block_conditions.consecutive_loss_days`
- `drawdown_manager.hard_block_conditions.require_system_stress`
- `drawdown_manager.hard_block_conditions.comment`
- `drawdown_manager.liquidation_price`
- `drawdown_manager.consecutive_loss_days_threshold`
- `drawdown_manager.consecutive_loss_exposure_multiplier`
- `drawdown_manager.vix_proxy_threshold`
- `drawdown_manager.high_vol_position_size_multiplier`
- `drawdown_manager.sector_concentration_block`
- `drawdown_manager.sector_concentration_limit`
- `drawdown_manager.min_positions_for_partial_liquidation`
- `fx_entry_policy.enabled`
- `fx_entry_policy.weak_fx_export_bias_level`
- `fx_entry_policy.strong_fx_domestic_bias_level`
- `fx_entry_policy.crisis_fx_level`
- `fx_entry_policy.daily_abs_change_block_level`
- `fx_entry_policy.hard_block_requires_crisis_level`
- `fx_entry_policy.soft_avg_abs_change_threshold`
- `fx_entry_policy.soft_daily_abs_change_threshold`
- `fx_entry_policy.caution_avg_abs_change_threshold`
- `fx_entry_policy.three_day_extreme_force_defensive`
- `p1_entry_policy.enabled`
- `p1_entry_policy.intraday.power_hour_start_hhmm`
- `p1_entry_policy.intraday.power_hour_end_hhmm`
- `p1_entry_policy.event_gate.enabled`
- `p1_entry_policy.event_gate.events_file`
- `p1_entry_policy.event_gate.auto_stub_when_missing`
- `p1_entry_policy.event_gate.explicit_market_event_guard.block_event_types`
- `p1_entry_policy.event_gate.explicit_market_event_guard.market_wide_event_types`
- `p1_entry_policy.event_gate.high_risk_levels`
- `p1_entry_policy.event_gate.high_risk_action`
- `p1_entry_policy.event_gate.high_risk_max_new_cap`
- `p1_entry_policy.technical_gate.enabled`
- `p1_entry_policy.technical_gate.min_pool_size`
- `p1_entry_policy.technical_gate.rsi_min`
- `p1_entry_policy.technical_gate.rsi_max`
- `p1_entry_policy.technical_gate.rsi_ok_min_ratio`
- `p1_entry_policy.technical_gate.macd_golden_min_ratio`
- `p1_entry_policy.technical_gate.volcorr_min`
- `p1_entry_policy.technical_gate.volcorr_ok_min_ratio`
- `p1_entry_policy.technical_gate.stoch_k_min`
- `p1_entry_policy.technical_gate.stoch_k_max`
- `p1_entry_policy.technical_gate.stoch_ok_min_ratio`
- `p1_entry_policy.technical_gate.boll_mid_min`
- `p1_entry_policy.technical_gate.boll_mid_ok_min_ratio`
- `p1_entry_policy.technical_gate.obv_slope_min`
- `p1_entry_policy.technical_gate.obv_ok_min_ratio`
- `p1_entry_policy.technical_gate.sma_ema_ok_min_ratio`
- `p1_entry_policy.technical_gate.low_quality_action`
- `p1_entry_policy.technical_gate.low_quality_max_new_cap`
- `global_outlier_watcher.enabled`
- `global_outlier_watcher.stale_max_age_days`
- `global_outlier_watcher.block_on_snapshot_missing`
- `global_outlier_watcher.block_stage_statuses`
- `global_outlier_watcher.caution_stage_statuses`
- `global_outlier_watcher.ignore_stage_keys`
- `global_outlier_watcher.block_calc_issue_states`
- `global_outlier_watcher.ignore_blocking_issue_keys`
- `cross_source_integrity.enabled`
- `cross_source_integrity.max_skew_days`
- `cross_source_integrity.block_on_missing_required`
- `cross_source_integrity.required_sources`
- `cross_source_integrity.optional_sources`
- `sigma_outlier_guard.enabled`
- `sigma_outlier_guard.lookback_files`
- `sigma_outlier_guard.min_history`
- `sigma_outlier_guard.zscore_caution`
- `sigma_outlier_guard.zscore_block`
- `sigma_outlier_guard.fallback_ratio_caution`
- `sigma_outlier_guard.fallback_ratio_block`
- `sigma_outlier_guard.check_param_ranges`
- `sigma_outlier_guard.block_on_param_out_of_range`
- `stable_params_quality_gate.require_promoted`
- `stable_params_quality_gate.min_oos_trades`
- `stable_params_quality_gate.min_oos_pf`
- `stable_params_quality_gate.min_stable_score`
- `stable_params_quality_gate.min_mean_pf`
- `execution_health_guard.enabled`
- `execution_health_guard.block_lifecycle_statuses`
- `execution_health_guard.block_state_machine_statuses`
- `execution_health_guard.caution_symbol_stop_statuses`
- `execution_health_guard.slippage_lookback_rows`
- `execution_health_guard.slippage_bps_caution`
- `execution_health_guard.slippage_bps_block`
- `macro_news_guard.enabled`
- `macro_news_guard.macro_use_critical_guard`
- `macro_news_guard.macro_critical_bad_block`
- `macro_news_guard.unknown_critical_no_data_action`
- `macro_news_guard.unknown_critical_no_data_allowed_run_labels`
- `macro_news_guard.macro_stale_ratio_caution`
- `macro_news_guard.macro_stale_ratio_block`
- `macro_news_guard.news_quota_guard_block`
- `macro_news_guard.news_quality_block_statuses`
- `macro_gate_policy.mode`
- `macro_gate_policy.hard_block_regimes`
- `macro_gate_policy.hard_block_when_risk_on_false`
- `sell_rules.stop_loss.default_pct`
- `sell_rules.stop_loss.stop_sell_ratio_pct`
- `sell_rules.stop_loss.stop_gap_sell_ratio_pct`
- `sell_rules.stop_loss.preemptive_sell_ratio_pct`
- `sell_rules.stop_loss.asset_type_overrides.성장주`
- `sell_rules.stop_loss.asset_type_overrides.가치주`
- `sell_rules.stop_loss.asset_type_overrides.테마주`
- `sell_rules.stop_loss.asset_type_overrides.배당주`
- `sell_rules.stop_loss.asset_type_overrides.경기민감주`
- `sell_rules.stop_loss.asset_type_overrides.경기방어주`
- `sell_rules.stop_loss.asset_type_overrides.GROWTH`
- `sell_rules.stop_loss.asset_type_overrides.VALUE`
- `sell_rules.stop_loss.asset_type_overrides.THEME`
- `sell_rules.stop_loss.trailing_stop_enabled`
- `sell_rules.stop_loss.trailing_stop_activation_profit_pct`
- `sell_rules.stop_loss.trailing_stop_pct`
- `sell_rules.stop_loss.preemptive_close_enabled`
- `sell_rules.stop_loss.preemptive_close_pct`
- `sell_rules.take_profit.enabled`
- `sell_rules.take_profit.asset_type_adjustments.성장주`
- `sell_rules.take_profit.asset_type_adjustments.가치주`
- `sell_rules.take_profit.asset_type_adjustments.테마주`
- `sell_rules.take_profit.asset_type_adjustments.배당주`
- `sell_rules.fundamental_risk.enabled`
- `sell_rules.fundamental_risk.critical_debt_ratio`
- `sell_rules.fundamental_risk.critical_roe`
- `sell_rules.fundamental_risk.critical_revenue_growth_yoy`
- `sell_rules.fundamental_risk.warning_operating_margin`
- `sell_rules.fundamental_risk.dividend_yield_floor`
- `sell_rules.fundamental_risk.warning_sell_ratio`
- `sell_rules.market_risk.enabled`
- `sell_rules.market_risk.vix_high`
- `sell_rules.market_risk.vix_extreme`
- `sell_rules.market_risk.usd_krw_volatility`
- `sell_rules.market_risk.oil_shock_high`
- `sell_rules.market_risk.oil_shock_extreme`
- `sell_rules.market_risk.high_sell_ratio_profit`
- `sell_rules.market_risk.extreme_sell_ratio_profit`
- `sell_rules.market_risk.extreme_sell_ratio_loss`
- `sell_rules.technical.rsi_overbought`
- `sell_rules.technical.ma_periods`
- `sell_rules.technical.high_score_threshold`
- `sell_rules.technical.mid_score_threshold`
- `sell_rules.technical.high_score_sell_ratio`
- `sell_rules.technical.mid_score_sell_ratio`
- `sell_rules.asset_type_rules.테마주.trailing_stop_pct`
- `sell_rules.asset_type_rules.테마주.take_profit_levels`
- `sell_rules.asset_type_rules.테마주.take_profit_ratios`
- `sell_rules.asset_type_rules.테마주.max_hold_days`
- `sell_rules.asset_type_rules.경기민감주.trailing_stop_pct`
- `sell_rules.asset_type_rules.경기민감주.take_profit_levels`
- `sell_rules.asset_type_rules.경기민감주.take_profit_ratios`
- `sell_rules.asset_type_rules.경기민감주.max_hold_days`
- `sell_rules.asset_type_rules.성장주.trailing_stop_pct`
- `sell_rules.asset_type_rules.성장주.take_profit_levels`
- `sell_rules.asset_type_rules.성장주.take_profit_ratios`
- `sell_rules.asset_type_rules.성장주.max_hold_days`
- `sell_rules.asset_type_rules.가치주.trailing_stop_pct`
- `sell_rules.asset_type_rules.가치주.take_profit_levels`
- `sell_rules.asset_type_rules.가치주.take_profit_ratios`
- `sell_rules.asset_type_rules.가치주.max_hold_days`
- `sell_rules.asset_type_rules.배당주.trailing_stop_pct`
- `sell_rules.asset_type_rules.배당주.take_profit_levels`
- `sell_rules.asset_type_rules.배당주.take_profit_ratios`
- `sell_rules.asset_type_rules.배당주.max_hold_days`
- `sell_rules.asset_type_rules.경기방어주.trailing_stop_pct`
- `sell_rules.asset_type_rules.경기방어주.take_profit_levels`
- `sell_rules.asset_type_rules.경기방어주.take_profit_ratios`
- `sell_rules.asset_type_rules.경기방어주.max_hold_days`
- `min_entry_score_policy.enabled`
- `min_entry_score_policy.quantile`
- `min_entry_score_policy.min_floor`
- `min_entry_score_policy.max_cap`
- `min_entry_score_policy.min_candidates`
- `min_entry_score_policy.allow_fallback_top1`
- `capital_budget_policy.basic_target_positions`
- `capital_budget_policy.recovery_enabled`
- `capital_budget_policy.reserve_trade_enabled`
- `paper_validation_sample_policy.scope`
- `sector_correlation_guard.enabled`
- `sector_correlation_guard.lookback_days`
- `sector_correlation_guard.risk_budget_engine`
- `sector_correlation_guard.ewma_halflife_days`
- `sector_correlation_guard.hrp_enabled`
- `sector_correlation_guard.hrp_overweight_tolerance`
- `sector_correlation_guard.hrp_min_qty_multiplier`
- `sector_correlation_guard.reduce_threshold_abs_corr`
- `sector_correlation_guard.reduce_qty_multiplier`
- `sector_correlation_guard.allow_block`
- `entry_overheat_policy.enabled`
- `entry_overheat_policy.apply_to_normal`
- `entry_overheat_policy.apply_to_surge`
- `entry_overheat_policy.v_accel_threshold`
- `entry_overheat_policy.ret1_pct_threshold`
- `entry_overheat_policy.atr14_pct_threshold`
- `entry_overheat_policy.trigger`
- `entry_overheat_policy.reduce_multiplier`
- `sector_rebalance.enabled`
- `sector_rebalance.limit_pct`
- `sector_rebalance.sell_ratio_pct`
- `horizon_entry_policy.enabled`
- `horizon_entry_policy.default_label`
- `horizon_entry_policy.label_rules.SHORT.min_final_score`
- `horizon_entry_policy.label_rules.SHORT.min_v_accel`
- `horizon_entry_policy.label_rules.SHORT.min_rs_slope`
- `horizon_entry_policy.label_rules.SHORT.max_hold_days`
- `horizon_entry_policy.label_rules.SHORT.entry_weight`
- `horizon_entry_policy.label_rules.SWING.min_final_score`
- `horizon_entry_policy.label_rules.SWING.min_atr14_pct`
- `horizon_entry_policy.label_rules.SWING.min_stoch_k`
- `horizon_entry_policy.label_rules.SWING.max_hold_days`
- `horizon_entry_policy.label_rules.SWING.entry_weight`
- `horizon_entry_policy.label_rules.MID.min_final_score`
- `horizon_entry_policy.label_rules.MID.max_hold_days`
- `horizon_entry_policy.label_rules.MID.entry_weight`
- `horizon_entry_policy.label_rules.LONG.min_final_score`
- `horizon_entry_policy.label_rules.LONG.max_atr14_pct`
- `horizon_entry_policy.label_rules.LONG.max_hold_days`
- `horizon_entry_policy.label_rules.LONG.entry_weight`
- `backtest_validation_guard.enabled`
- `backtest_validation_guard.stale_max_age_days`
- `backtest_validation_guard.block_gate_names`
- `backtest_validation_guard.caution_gate_names`
- `backtest_validation_guard.caution_on_overall_fail`
- `backtest_validation_guard.caution_affects_entry`
- `backtest_validation_guard.block_on_missing`
- `news_implication_entry_policy.enabled`
- `news_implication_entry_policy.reduce_size_observe_only`
- `news_implication_entry_policy.reduce_size_multiplier`
- `news_implication_entry_policy.watch_observe_only`
- `news_implication_entry_policy.block_observe_only`
- `news_implication_entry_policy.lock_observe_only`
- `entry_signal_date_top_score_cap.enabled`
- `entry_signal_date_top_score_cap.fallback_when_selected_pool_has_no_top_overlap`
- `entry_signal_date_top_score_cap.fallback_require_positive_entry`
- `entry_signal_date_top_score_cap.fallback_if_pool_lt_n`
- `positive_entry_criteria.enabled`
- `positive_entry_criteria.hard_filter`
- `positive_entry_criteria.min_score`
- `positive_entry_criteria.score_columns`
- `positive_entry_criteria.require_sector_entry_when_present`
- `positive_entry_criteria.allow_sector_union`
- `positive_entry_criteria.fresh_sector_allowed_fallback.enabled`
- `positive_entry_criteria.fresh_sector_allowed_fallback.trigger_when_fresh_ok_lte`
- `positive_entry_criteria.fresh_sector_allowed_fallback.max_candidates`
- `positive_entry_criteria.fresh_sector_allowed_fallback.min_final_score`
- `positive_entry_criteria.fresh_sector_allowed_fallback.min_sector_strength`
- `positive_entry_criteria.fresh_sector_allowed_fallback.candidate_origin`
- `positive_entry_criteria.fresh_sector_allowed_fallback.allowed_sector_actions`
- `normal_intraday_realtime_policy.enabled`
- `normal_intraday_realtime_policy.block_when_entry_gate_not_allow`
- `normal_intraday_realtime_policy.blocked_entry_gate_decisions`
- `normal_intraday_realtime_policy.allow_dd_stop_validation_reduce`
- `normal_intraday_realtime_policy.exclude_reduced_entry_fills_in_validation`
- `normal_intraday_realtime_policy.block_when_p0_rolling_dd_ge_threshold`
- `normal_intraday_realtime_policy.p0_rolling_dd_block_pct`
- `entry_gap_up_reduce.enabled`
- `entry_gap_up_reduce.threshold_pct`
- `entry_gap_up_reduce.qty_multiplier`
- `entry_gap_up_reduce.min_qty`
- `entry_gap_up_reduce.apply_to_surge_no_lob_probe`
- `intraday_residual_overnight_guard.enabled`
- `intraday_residual_overnight_guard.shadow_only`
- `intraday_residual_overnight_guard.scope`
- `intraday_residual_overnight_guard.trigger_on_same_day_loss`
- `intraday_residual_overnight_guard.apply_to_surge`
- `intraday_residual_overnight_guard.apply_to_non_surge`
- `intraday_residual_overnight_guard.exit_before_overnight`
- `intraday_residual_overnight_guard.evidence_artifact`
- `normal_entry_execution_quality.enabled`
- `normal_entry_execution_quality.require_lob`
- `normal_entry_execution_quality.max_spread_bps`
- `normal_entry_execution_quality.require_executable_qty`
- `normal_entry_execution_quality.min_executable_qty_ratio`
- `normal_entry_execution_quality.max_depth_levels`
- `normal_entry_execution_quality.min_markout_1step_bps`
- `normal_entry_execution_quality.block_on_missing_markout`
- `normal_entry_execution_quality.orderflow_tag_block_tags`
- `normal_entry_execution_quality.qty_reduction.enabled`
- `normal_entry_execution_quality.qty_reduction.spread_bands`
- `normal_entry_execution_quality.qty_reduction.markout_bands`
- `normal_entry_execution_quality.qty_reduction.orderflow_caution_multiplier`
- `normal_entry_execution_quality.qty_reduction.orderflow_caution_reduce_tags`
- `normal_entry_execution_quality.qty_reduction.multi_condition.two_or_more_multiplier`
- `normal_candidate_staleness_check.enabled`
- `normal_candidate_staleness_check.max_stale_days`
- `normal_candidate_staleness_check.block_if_stale`
- `normal_realtime_gap_policy.enabled`
- `normal_realtime_gap_policy.close_auction.enabled`
- `normal_realtime_gap_policy.close_auction.block_close_pos_min`
- `normal_realtime_gap_policy.close_auction.reduce_close_pos_min`
- `normal_realtime_gap_policy.close_auction.max_day_range_pct`
- `normal_realtime_gap_policy.close_auction.block_v_accel_min`
- `normal_realtime_gap_policy.close_auction.reduce_v_accel_min`
- `normal_realtime_gap_policy.close_auction.reduce_qty_multiplier`
- `normal_realtime_gap_policy.intraday_momentum_recheck.enabled`
- `normal_realtime_gap_policy.intraday_momentum_recheck.require_intraday`
- `normal_realtime_gap_policy.intraday_momentum_recheck.block_v_accel_min`
- `normal_realtime_gap_policy.intraday_momentum_recheck.reduce_v_accel_min`
- `normal_realtime_gap_policy.intraday_momentum_recheck.min_value_ratio`
- `normal_realtime_gap_policy.intraday_momentum_recheck.reduce_qty_multiplier`
- `normal_realtime_gap_policy.overnight_gap_exit.enabled`
- `normal_realtime_gap_policy.overnight_gap_exit.gap_down_exit_pct`
- `normal_realtime_gap_policy.overnight_gap_exit.gap_up_take_profit_pct`
- `normal_realtime_gap_policy.overnight_gap_exit.gap_down_sell_ratio_pct`
- `normal_realtime_gap_policy.overnight_gap_exit.gap_up_sell_ratio_pct`
- `normal_realtime_gap_policy.dynamic_slippage.enabled`
- `normal_realtime_gap_policy.dynamic_slippage.low_trading_value_krw`
- `normal_realtime_gap_policy.dynamic_slippage.very_low_trading_value_krw`
- `normal_realtime_gap_policy.dynamic_slippage.low_trading_value_multiplier`
- `normal_realtime_gap_policy.dynamic_slippage.very_low_trading_value_multiplier`
- `normal_realtime_gap_policy.dynamic_slippage.low_turnover_ratio`
- `normal_realtime_gap_policy.dynamic_slippage.low_turnover_multiplier`
- `normal_realtime_gap_policy.dynamic_slippage.max_slippage_pct`

### `max_new_trades_per_day`

- `paper_engine.py:18549`: `    base_max_new = int(cfg.get("max_new_trades_per_day", 3))`
- `paper_engine/config.py:110`: `    "max_new_trades_per_day": 3,`
- `paper_engine/config.py:508`: `            "max_new_trades_per_day": 0,`

### `fixed_qty`

- `paper_engine.py:15962`: `        note_parts = [f"signal_date={effective_signal_date}", f"sizing={str(cfg.get('sizing_mode','fixed_qty'))}", f"qty`
- `paper_engine.py:18861`: `    sizing_mode = str(cfg.get("sizing_mode", "fixed_qty"))`
- `paper_engine/config.py:111`: `    "fixed_qty": 1,`

### `max_hold_days`

- `paper_engine.py:1813`: `    "surge_type_max_hold_days",`
- `paper_engine.py:4235`: `def _resolve_position_max_hold_days(asset_type: str, sell_rules: Dict[str, Any], default_max_hold_days: int) -> int:`
- `paper_engine.py:4238`: `        v = int(type_rule.get("max_hold_days", default_max_hold_days) or default_max_hold_days)`

### `min_hold_days`

- `paper_engine.py:3307`: `        "min_hold_days": max(0, int(_to_int(raw.get("min_hold_days"), 1))),`
- `paper_engine.py:3323`: `    min_hold_days = max(0, int(_to_int((guard_cfg or {}).get("min_hold_days"), 1)))`
- `paper_engine.py:3324`: `    if int(hold_days_trading) < min_hold_days:`

### `min_hold_protect_stop_loss`

- `paper_engine.py:16997`: `    _min_hold_protect_stop_loss = bool(cfg.get("min_hold_protect_stop_loss", True))`
- `paper_engine.py:17210`: `                if _min_hold_protect_stop_loss:`
- `paper_engine.py:17322`: `            _protect_stop_loss = (_in_protected_period and _min_hold_protect_stop_loss)`

### `atr_stop_multiplier`

- `paper_engine.py:16985`: `    _atr_mult = float(cfg.get("atr_stop_multiplier", 0.0) or 0.0)`
- `paper_engine/config.py:115`: `    "atr_stop_multiplier": 2.0,         # if >0, use -(atr14_pct * multiplier) as stop (0=disabled)`

### `allow_same_code_reentry`

- `paper_engine.py:13586`: `    allow_same_signal_reentry = bool(cfg.get("allow_same_code_reentry", False))`
- `paper_engine.py:18862`: `    allow_reentry = bool(cfg.get("allow_same_code_reentry", False))`
- `paper_engine/config.py:116`: `    "allow_same_code_reentry": False,`

### `entry_timing_mode`

- `paper_engine.py:18863`: `    entry_timing_mode = str(cfg.get("entry_timing_mode", "next_open") or "next_open").strip().lower()`
- `paper_engine.py:18864`: `    same_close_entry_mode = entry_timing_mode in {"same_close", "close", "t_close", "close_entry"}`
- `paper_engine.py:18868`: `        f"[ENTRY_MODE] timing={entry_timing_mode} same_close={same_close_entry_mode} "`

### `fee_pct`

- `paper_engine.py:992`: `    fee_pct: float,`
- `paper_engine.py:1124`: `            pnl_pct = calc_net_ret(entry_price, exit_price, fee_pct, slip_pct, sell_tax_pct)`
- `paper_engine.py:1145`: `            fee = calc_roundtrip_fee(entry_price, exit_price, sell_qty, fee_pct)`

### `slippage_pct`

- `paper_engine.py:29`: `    resolve_slippage_pct_tiered,`
- `paper_engine.py:1341`: `        "normal_dynamic_slippage_pct",`
- `paper_engine.py:1820`: `    "surge_lob_slippage_pct",`

### `tiered_slippage.enabled`

- `paper_engine.py:2157`: `    """Return slippage rate tiered by market cap if tiered_slippage.enabled=True."""`

### `split_entry.enabled`

- `paper_engine.py:15467`: `            if bool((cfg.get("split_entry", {}) if isinstance(cfg, dict) else {}).get("enabled", False)):`
- `paper_engine.py:16035`: `        elif cfg.get("split_entry", {}).get("enabled") and not is_open_order_replay:`

### `split_entry.surge_first_ratio`

- `paper_engine.py:15471`: `                    (cfg.get("split_entry", {}) if isinstance(cfg, dict) else {}).get("surge_first_ratio", 1.0)`

### `sell_tax_pct`

- `paper_engine.py:994`: `    sell_tax_pct: float,`
- `paper_engine.py:1124`: `            pnl_pct = calc_net_ret(entry_price, exit_price, fee_pct, slip_pct, sell_tax_pct)`
- `paper_engine.py:1167`: `            net = calc_net_ret(entry_price, exit_price, fee_pct, slip_pct, sell_tax_pct)`

### `candidates_latest_data`

- `paper_engine.py:10389`: `    cpath = Path(cfg["candidates_latest_data"])`
- `paper_engine.py:10405`: `        canonical = LOG_DIR / ("candidates_latest_data" + suffix)`
- `paper_engine/config.py:264`: `    "candidates_latest_data": str(LOG_DIR / "candidates_latest_data.csv"),`

### `parquet_root`

- `paper_engine.py:10685`: `    root = Path(root_override or cfg["parquet_root"]).resolve()`
- `paper_engine.py:10735`: `        raise SystemExit("[FATAL] no parquet found under parquet_root")`
- `paper_engine/config.py:267`: `    "parquet_root": str(BASE_DIR),`

### `parquet_top_n_recent`

- `paper_engine.py:10686`: `    top_n = int(top_n_override or cfg.get("parquet_top_n_recent", 120))`
- `paper_engine/config.py:268`: `    "parquet_top_n_recent": 120,`

### `parquet_max_open_files`

- `paper_engine.py:10737`: `    max_open = int(cfg.get("parquet_max_open_files", 30))`
- `paper_engine/config.py:269`: `    "parquet_max_open_files": 30,`

### `sizing_mode`

- `paper_engine.py:15427`: `                print(f"[SKIP_SURGE_QTY] code={code} entry_price={entry_price:.4f} mode={str(cfg.get('sizing_mode',''))}`
- `paper_engine.py:15434`: `                print(f"[SKIP_QTY_ZERO] code={code} entry_price={entry_price:.4f} mode={str(cfg.get('sizing_mode',''))}"`
- `paper_engine.py:15962`: `        note_parts = [f"signal_date={effective_signal_date}", f"sizing={str(cfg.get('sizing_mode','fixed_qty'))}", f"qty`

### `capital_total`

- `paper_engine.py:3520`: `    capital_total: float,`
- `paper_engine.py:3541`: `    risk_budget = float(capital_total or 0.0) * float(risk_per_trade_pct)`
- `paper_engine.py:5794`: `    capital_total: float,`

### `max_positions`

- `paper_engine.py:7403`: `    max_positions_meta: Optional[Dict[str, Any]],`
- `paper_engine.py:7494`: `            "max_positions_meta": max_positions_meta or {},`
- `paper_engine.py:8302`: `    max_positions_meta: Optional[Dict[str, Any]] = None,`

### `min_qty`

- `paper_engine.py:3998`: `    hrp_min_qty_multiplier = max(0.05, min(float(_to_float(gcfg.get("hrp_min_qty_multiplier"), 0.35) or 0.35), 1.0))`
- `paper_engine.py:4011`: `        "hrp_min_qty_multiplier": float(hrp_min_qty_multiplier),`
- `paper_engine.py:4155`: `                mult = max(hrp_min_qty_multiplier, min(1.0, target_w / exposure_w))`

### `kill_switch.max_drawdown_pct`

- `paper_engine.py:4779`: `            source = str(p0_rolling_dd_source or "p0_kill_switch.max_drawdown_pct").strip()`
- `paper_engine.py:18617`: `        f"kill_switch.limit.max_dd={ks_limits.get('max_drawdown_pct')} "`

### `kill_switch.max_daily_loss_pct`

- `paper_engine.py:18618`: `        f"kill_switch.limit.max_daily_loss={ks_limits.get('max_daily_loss_pct')} "`

### `kill_switch.mode`

- `paper_engine.py:569`: `            "mode": str(kill_switch.get("mode", "") or ""),`
- `paper_engine.py:572`: `                f"모드={str(kill_switch.get('mode', '') or '')}"`
- `paper_engine.py:18574`: `        print(f"[RISK_GATE_CFG_WARN] kill_switch.mode invalid={ks_mode} -> BLOCK")`

### `kill_switch.reduce_factor`

- `paper_engine.py:18615`: `        f"kill_switch.reduce_factor={ks_reduce_factor:.4f} "`

### `crash_risk_off.mode`

- `paper_engine.py:18591`: `        print(f"[RISK_GATE_CFG_WARN] crash_risk_off.mode invalid={crash_mode} -> BLOCK")`

### `cap_signal_top_n`

- `paper_engine.py:12962`: `    cap_signal_top_n: int,`
- `paper_engine.py:12965`: `    if cap_signal_top_n > 0 and ("signal_date" in candidate_df.columns):`
- `paper_engine.py:12977`: `            candidate_df = candidate_df[candidate_df["_rk_sig"] <= cap_signal_top_n].copy()`

### `gap_up_max_pct`

- `paper_engine.py:13523`: `    gap_up_max_pct_runtime: float,`
- `paper_engine.py:14498`: `            _gap_max = float(_to_float(fb_policy.get("next_open_gap_up_max_pct"), 0.03) or 0.03)`
- `paper_engine.py:14726`: `            gap_up_max_pct = float(gap_up_max_pct_runtime or 0.0)`

### `max_per_sector`

- `paper_engine.py:12963`: `    max_per_sector: int,`
- `paper_engine.py:12983`: `    if max_per_sector > 0:`
- `paper_engine.py:12986`: `            print("[CAP] max_per_sector set but sector_code column missing; ignored")`