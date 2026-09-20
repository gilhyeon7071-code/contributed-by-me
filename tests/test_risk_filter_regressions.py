import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generate_candidates_v41_1 as gc
from gate_daily import _risk_off_engine_action
from paper_engine import (
    DrawdownAction,
    _calc_surge_exit_severity,
    _ddm_forced_sell_ratio_pct,
    _ddm_select_action,
    _evaluate_hold_close_drop_guard,
    _resolve_hold_close_drop_guard_cfg,
    _resolve_surge_exit_ratio_pct,
    compute_adaptive_kill_cap,
)
from p0_daily_check import _is_daily_loss_active
from utils.common import is_daily_loss_reason, is_risk_off_hard_block_reason


class RiskFilterRegressionTests(unittest.TestCase):
    @staticmethod
    def _const_risk_unit(value: float):
        def _inner(s, *args, **kwargs):
            try:
                idx = s.index
                n = len(s)
            except Exception:
                idx = [0]
                n = 1
            return pd.Series([value] * n, index=idx, dtype=float)
        return _inner

    def test_hard_block_reason_includes_krx_clean_date_max(self) -> None:
        self.assertTrue(is_risk_off_hard_block_reason("krx_clean_date_max(20260317) < prev_weekday_lag2(20260319)"))
        self.assertFalse(is_risk_off_hard_block_reason("kill_switch"))

    def test_daily_loss_reason_helper_matches_prefixed_and_plain(self) -> None:
        self.assertTrue(is_daily_loss_reason("DAILY_LOSS(-0.0900 <= -0.0800)"))
        self.assertTrue(is_daily_loss_reason("kill_switch:DAILY_LOSS(-0.0900 <= -0.0800)"))
        self.assertFalse(is_daily_loss_reason("kill_switch:MAX_DD(-0.3500 <= -0.1500)"))

    def test_daily_loss_active_only_for_current_ymd(self) -> None:
        self.assertTrue(_is_daily_loss_active("20260319", "20260319"))
        self.assertFalse(_is_daily_loss_active("20260318", "20260319"))
        self.assertFalse(_is_daily_loss_active("", "20260319"))

    def test_gate_action_blocks_hard_data_and_daily_loss(self) -> None:
        action = _risk_off_engine_action(
            {"kill_switch": {"limits": {"mode": "REDUCE"}}},
            ["kill_switch", "krx_clean_date_max(20260317) < prev_weekday_lag2(20260319)"],
        )
        self.assertEqual(action["action"], "BLOCK")

        action2 = _risk_off_engine_action(
            {"kill_switch": {"limits": {"mode": "REDUCE"}}},
            ["kill_switch", "kill_switch:DAILY_LOSS(-0.0900 <= -0.0800)"],
        )
        self.assertEqual(action2["action"], "BLOCK")

    def test_adaptive_kill_cap_returns_zero_on_daily_loss(self) -> None:
        cfg = {
            "adaptive_entry_control": {
                "enabled": True,
                "dd_ratio_soft": 1.0,
                "dd_ratio_mid": 1.1,
                "dd_ratio_hard": 1.25,
                "reduce_soft": 0.5,
                "reduce_mid": 0.3,
                "reduce_hard": 0.15,
                "probe_min_new": 1,
                "relief_after_streak_days": 3,
                "relief_min_new": 1,
            }
        }
        p0_snapshot = {
            "kill_switch": {
                "reasons": ["DAILY_LOSS(-0.0900 <= -0.0800)"],
                "metrics": {"max_drawdown_pct": -0.02, "last_day_ret": -0.09},
                "limits": {"max_drawdown_pct": 0.35, "max_daily_loss_pct": 0.08},
            }
        }
        cap, detail = compute_adaptive_kill_cap(30, cfg, p0_snapshot, 0)
        self.assertEqual(cap, 0)
        self.assertIn("daily_loss_block", detail)

    def test_ddm_ignores_strategy_metrics_and_fails_closed_without_account_basis(self) -> None:
        """[2026-09-09 계약 변경] 전략 계열 낙폭은 DDM 차단에 쓰지 않는다.

        이전 계약은 `test_ddm_prefers_rolling_metric_when_available` 이었다.
        account_basis 가 없을 때 rolling(-0.05)을 lifetime(-0.30)보다 우선해
        영구 차단을 피한다는 것이 목적이었다.

        사용자 결정으로 SSOT 가 둘로 갈렸다.
          Account Risk SSOT        account_equity  -> DDM stage / 신규진입 capacity
          Strategy Health Observer rolling_weighted_mean_60 -> 관측·경고·전략 검증 전용

        그래서 account_basis 가 없으면 **다른 지표로 대체하지 않고 신규 진입만 닫는다.**
        원래 취지(lifetime 이 조용히 영구 차단하는 것을 막는다)는 유지된다 -
        lifetime 도 차단 판정에 쓰이지 않기 때문이다. 다만 해제 조건이 바뀐다:
        "시간이 지나 rolling 창을 벗어나면" 이 아니라 "SSOT 가 돌아오면" 이다.
        """
        cfg = {
            "drawdown_manager": {
                "enabled": True,
                "use_debug_lifetime_mdd": True,
                "stages": [
                    {"mdd": 0.10, "new_entry_allowed_pct": 0.50, "liquidate_weakest_pct": 0.00, "max_exposure": 0.60},
                    {"mdd": 0.15, "new_entry_allowed_pct": 0.00, "liquidate_weakest_pct": 0.00, "max_exposure": 0.50},
                ],
            }
        }
        p0_snapshot = {
            "kill_switch": {
                "metrics": {
                    "mode": "rolling",
                    "max_drawdown_pct": -0.05,
                    "debug_lifetime_max_drawdown_pct": -0.30,
                }
            }
        }
        action = _ddm_select_action(cfg, p0_snapshot)
        # 어느 전략 지표도 current_mdd_abs 가 되지 않는다
        self.assertEqual(action.metric_basis, "account_equity_unavailable")
        self.assertAlmostEqual(action.current_mdd_abs, 0.0, places=6)
        self.assertNotAlmostEqual(action.current_mdd_abs, 0.05, places=6)
        self.assertNotAlmostEqual(action.current_mdd_abs, 0.30, places=6)
        # fail-closed: 신규 진입만 닫는다. 강제 청산은 하지 않는다
        self.assertEqual(action.stage_idx, -2)
        self.assertAlmostEqual(action.new_entry_allowed_pct, 0.0, places=6)
        self.assertAlmostEqual(action.liquidate_weakest_pct, 0.0, places=6)
        # 관측값은 버리지 않고 남긴다
        self.assertAlmostEqual(action.metric_details["strategy_rolling_mdd_abs"], 0.05, places=6)
        self.assertAlmostEqual(action.metric_details["strategy_lifetime_mdd_abs"], 0.30, places=6)
        self.assertEqual(
            action.metric_details["strategy_metrics_role"],
            "observer_only_not_used_for_blocking",
        )

    def test_ddm_uses_account_basis_when_present(self) -> None:
        """account_basis 가 PASS 면 그 값만 쓴다 - 전략 지표가 훨씬 나빠도 무시한다."""
        cfg = {
            "drawdown_manager": {
                "enabled": True,
                "stages": [
                    {"mdd": 0.10, "new_entry_allowed_pct": 0.50, "liquidate_weakest_pct": 0.00, "max_exposure": 0.60},
                ],
            }
        }
        p0_snapshot = {
            "kill_switch": {
                "metrics": {
                    "mode": "rolling",
                    "max_drawdown_pct": -0.55,
                    "account_basis": {"status": "PASS", "max_drawdown_pct": -0.05},
                }
            }
        }
        action = _ddm_select_action(cfg, p0_snapshot)
        self.assertEqual(action.metric_basis, "account_equity")
        self.assertAlmostEqual(action.current_mdd_abs, 0.05, places=6)
        self.assertEqual(action.stage_idx, -1)
        self.assertAlmostEqual(action.new_entry_allowed_pct, 1.0, places=6)

    def test_ddm_stage3_partial_stage4_full_sell_ratio(self) -> None:
        stage3 = DrawdownAction(
            current_mdd_abs=0.20,
            stage_idx=3,
            threshold=0.20,
            new_entry_allowed_pct=0.0,
            liquidate_weakest_pct=0.30,
            max_exposure=0.40,
        )
        stage4 = DrawdownAction(
            current_mdd_abs=0.25,
            stage_idx=4,
            threshold=0.25,
            new_entry_allowed_pct=0.0,
            liquidate_weakest_pct=0.50,
            max_exposure=0.30,
        )
        self.assertEqual(_ddm_forced_sell_ratio_pct(stage3), 30.0)
        self.assertEqual(_ddm_forced_sell_ratio_pct(stage4), 100.0)

    def test_surge_trail_exit_severity_ratios(self) -> None:
        dynamic_cfg = {
            "base_ratios": {
                "LIGHT": 25,
                "NORMAL": 50,
                "HARD": 75,
                "FORCE": 100,
            },
            "t1_surge_relax_one_level": False,
            "risk_off_raise_one_level": False,
            "reversal_raise_one_level": False,
            "repeat_stop_raise_one_level": False,
            "repeat_stop_force_exit": False,
        }
        trail = _calc_surge_exit_severity(
            exit_reason="TRAIL",
            hold_days_trading=3,
            entry_price=100.0,
            exit_price=120.0,
            atr14_pct=0.02,
            is_surge_pos=True,
            market_regime="NORMAL",
            kill_switch_active=False,
            reversal_count=0,
            prior_stop_count=0,
            dynamic_cfg=dynamic_cfg,
        )
        trail_gap = _calc_surge_exit_severity(
            exit_reason="TRAIL_GAP",
            hold_days_trading=3,
            entry_price=100.0,
            exit_price=112.0,
            atr14_pct=0.02,
            is_surge_pos=True,
            market_regime="NORMAL",
            kill_switch_active=False,
            reversal_count=0,
            prior_stop_count=0,
            dynamic_cfg=dynamic_cfg,
        )

        self.assertEqual(trail["severity"], "LIGHT")
        self.assertIn("base:trail", trail["severity_reasons"])
        self.assertEqual(
            _resolve_surge_exit_ratio_pct(severity=trail["severity"], dynamic_cfg=dynamic_cfg, fallback_pct=40.0),
            25.0,
        )
        self.assertEqual(trail_gap["severity"], "NORMAL")
        self.assertIn("base:trail_gap", trail_gap["severity_reasons"])
        self.assertEqual(
            _resolve_surge_exit_ratio_pct(severity=trail_gap["severity"], dynamic_cfg=dynamic_cfg, fallback_pct=40.0),
            50.0,
        )

    def test_hold_close_drop_guard_triggers_after_min_hold(self) -> None:
        cfg = _resolve_hold_close_drop_guard_cfg(
            {
                "hold_close_drop_guard": {
                    "enabled": True,
                    "drop_pct": 0.05,
                    "sell_ratio_pct": 100.0,
                    "min_hold_days": 1,
                }
            }
        )
        out = _evaluate_hold_close_drop_guard(
            prev_close=100.0,
            close_price=94.9,
            hold_days_trading=2,
            is_entry_date=False,
            guard_cfg=cfg,
        )
        self.assertTrue(out["triggered"])
        self.assertEqual(out["reason"], "close_drop")
        self.assertAlmostEqual(float(out["close_drop_pct"]), 0.051, places=6)
        self.assertEqual(float(out["sell_ratio_pct"]), 100.0)

    def test_hold_close_drop_guard_skips_entry_date_and_protected_hold(self) -> None:
        cfg = _resolve_hold_close_drop_guard_cfg(
            {
                "hold_close_drop_guard": {
                    "enabled": True,
                    "drop_pct": 5.0,
                    "sell_ratio_pct": 100.0,
                    "min_hold_days": 2,
                }
            }
        )
        entry_day = _evaluate_hold_close_drop_guard(
            prev_close=100.0,
            close_price=94.0,
            hold_days_trading=0,
            is_entry_date=True,
            guard_cfg=cfg,
        )
        protected = _evaluate_hold_close_drop_guard(
            prev_close=100.0,
            close_price=94.0,
            hold_days_trading=1,
            is_entry_date=False,
            guard_cfg=cfg,
        )
        self.assertFalse(entry_day["triggered"])
        self.assertEqual(entry_day["reason"], "entry_date")
        self.assertFalse(protected["triggered"])
        self.assertEqual(protected["reason"], "min_hold_days")

    def test_junk_risk_overlay_marks_missing_final_score_as_fallback(self) -> None:
        candidates = pd.DataFrame({"code": ["000001"]})
        out, info = gc._apply_junk_risk_overlay(candidates, {"junk_risk_enable": 1.0})
        self.assertEqual(len(out), 1)
        self.assertTrue(info["fallback_applied"])
        self.assertEqual(info["fallback_reason"], "missing_final_score")
        self.assertEqual(info["mode"], "fallback_missing_final_score")

    def test_junk_risk_overlay_exception_uses_safe_fallback(self) -> None:
        candidates = pd.DataFrame(
            {
                "code": ["000001"],
                "final_score": [1.0],
                "value": [10_000_000_000.0],
                "v_accel": [2.0],
                "stretch": [1.1],
                "ret1_pct": [10.0],
                "rsi14": [70.0],
            }
        )
        original = gc._risk_unit_linear
        try:
            gc._risk_unit_linear = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("forced"))
            out, info = gc._apply_junk_risk_overlay(candidates, {"junk_risk_enable": 1.0})
        finally:
            gc._risk_unit_linear = original

        self.assertEqual(info["mode"], "fallback_exception")
        self.assertTrue(info["fallback_applied"])
        self.assertEqual(info["fallback_reason"], "RuntimeError")
        self.assertEqual(float(out.loc[0, "junk_risk_score"]), 0.0)
        self.assertEqual(str(out.loc[0, "junk_risk_grade"]), "FALLBACK")
        self.assertEqual(float(out.loc[0, "junk_penalty"]), 0.0)

    def test_junk_risk_overlay_uses_group_thresholds(self) -> None:
        candidates = pd.DataFrame(
            {
                "code": ["000001", "000002"],
                "final_score": [1.0, 1.0],
                "value": [10_000_000_000.0, 10_000_000_000.0],
                "v_accel": [2.0, 2.0],
                "stretch": [1.1, 1.1],
                "ret1_pct": [10.0, 10.0],
                "rsi14": [70.0, 70.0],
                "market_cap": [100_000_000_000.0, 5_000_000_000_000.0],
                "market": ["KOSPI", "KOSPI"],
            }
        )
        original = gc._risk_unit_linear
        try:
            gc._risk_unit_linear = self._const_risk_unit(0.9)
            out, info = gc._apply_junk_risk_overlay(candidates, {"junk_risk_enable": 1.0})
        finally:
            gc._risk_unit_linear = original

        self.assertEqual(info["removed"], 1)
        self.assertEqual(len(out), 1)
        self.assertEqual(str(out.iloc[0]["code"]), "000002")

    def test_junk_risk_overlay_adds_korean_market_flags(self) -> None:
        candidates = pd.DataFrame(
            {
                "code": ["000001"],
                "final_score": [1.0],
                "value": [50_000_000_000.0],
                "v_accel": [4.5],
                "stretch": [1.24],
                "ret1_pct": [29.5],
                "rsi14": [82.0],
                "market_cap": [120_000_000_000.0],
                "market": ["KOSDAQ"],
                "krx_watch_note": ["단기과열 지정 예고"],
            }
        )
        original = gc._risk_unit_linear
        try:
            gc._risk_unit_linear = self._const_risk_unit(0.6)
            out, info = gc._apply_junk_risk_overlay(candidates, {"junk_risk_enable": 1.0, "junk_hard_exclude": 0.0})
        finally:
            gc._risk_unit_linear = original

        self.assertEqual(info["korean_special_rows"], 1)
        flags = str(out.iloc[0]["junk_flags"])
        self.assertIn("limit_up_like", flags)
        self.assertIn("krx_overheat_note", flags)
        self.assertIn("kosdaq_smallcap_spec", flags)
        self.assertGreater(float(out.iloc[0]["junk_risk_score"]), 0.0)


if __name__ == "__main__":
    unittest.main()
