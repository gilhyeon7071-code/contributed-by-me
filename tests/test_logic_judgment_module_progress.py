import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
BUFFETT_TOOLS = Path(r"E:\vibe\buffett\tools")
for path in (ROOT, TOOLS, BUFFETT_TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from backtest_module_progress import build_module_progress
from build_backtest_validation_checklist import build_checklist
from build_backtest_validation_screen import _render
from build_trading_stage_validation_report import _build_transition_gate
from ledger_cost_model import build_enriched_ledger
from optimize_if_due_v41_1 import _validation_gate, parse_args


class LogicJudgmentModuleProgressTests(unittest.TestCase):
    def test_output_ledger_reconciles_average_cost_realized_and_costs(self) -> None:
        ledger_path = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
        self.assertTrue(ledger_path.exists(), f"missing ledger artifact: {ledger_path}")

        ledger = pd.read_csv(ledger_path, encoding="utf-8-sig")
        self.assertGreater(len(ledger), 0)

        numeric_cols = [
            "fill_qty",
            "matched_qty",
            "unmatched_qty",
            "fill_price",
            "avg_cost_before_krw",
            "avg_cost_after_krw",
            "position_qty_before",
            "position_qty_after",
            "position_cost_before_krw",
            "position_cost_after_krw",
            "realized_basis_krw",
            "realized_pnl_krw",
            "fee_krw",
            "tax_krw",
        ]
        for col in numeric_cols:
            ledger[col] = pd.to_numeric(ledger[col], errors="coerce").fillna(0.0)

        tol = 1e-6

        before_cost_gap = (
            ledger["position_cost_before_krw"] - (ledger["position_qty_before"] * ledger["avg_cost_before_krw"])
        ).abs()
        after_cost_gap = (
            ledger["position_cost_after_krw"] - (ledger["position_qty_after"] * ledger["avg_cost_after_krw"])
        ).abs()
        self.assertLessEqual(float(before_cost_gap.max()), tol)
        self.assertLessEqual(float(after_cost_gap.max()), tol)

        buy_mask = ledger["side"].astype(str).str.upper().eq("BUY")
        sell_mask = ledger["side"].astype(str).str.upper().eq("SELL")
        self.assertGreater(int(buy_mask.sum()), 0)
        self.assertGreater(int(sell_mask.sum()), 0)

        self.assertLessEqual(float(ledger.loc[buy_mask, "realized_pnl_krw"].abs().max()), tol)
        self.assertLessEqual(float(ledger.loc[buy_mask, "tax_krw"].abs().max()), tol)
        self.assertTrue(
            ledger.loc[buy_mask, "realized_flag"].astype(str).str.lower().isin(["false", "0"]).all()
        )

        sell_alloc = (
            ledger.loc[sell_mask, "matched_qty"] / ledger.loc[sell_mask, "fill_qty"].replace(0.0, pd.NA)
        ).fillna(0.0)
        expected_realized = (
            ledger.loc[sell_mask, "matched_qty"] * ledger.loc[sell_mask, "fill_price"]
            - ledger.loc[sell_mask, "fee_krw"] * sell_alloc
            - ledger.loc[sell_mask, "tax_krw"] * sell_alloc
            - ledger.loc[sell_mask, "realized_basis_krw"]
        )
        realized_gap = (ledger.loc[sell_mask, "realized_pnl_krw"] - expected_realized).abs()
        self.assertLessEqual(float(realized_gap.max()), tol)

        expected_sell_flag = ledger.loc[sell_mask, "matched_qty"] > 0.0
        actual_sell_flag = ledger.loc[sell_mask, "realized_flag"].astype(str).str.lower().isin(["true", "1"])
        self.assertTrue(actual_sell_flag.equals(expected_sell_flag))

        qty_gap = (
            ledger.loc[sell_mask, "fill_qty"] - ledger.loc[sell_mask, "matched_qty"] - ledger.loc[sell_mask, "unmatched_qty"]
        ).abs()
        self.assertLessEqual(float(qty_gap.max()), tol)
        self.assertTrue((ledger["avg_cost_after_krw"] >= 0.0).all())

    def test_tc_fill_003_partial_fill_accounting(self) -> None:
        fills = pd.DataFrame(
            [
                {
                    "date": "20260320",
                    "datetime": "2026-03-20T09:00:00",
                    "code": "005930",
                    "side": "BUY",
                    "fill_price": 100.0,
                    "fill_qty": 100,
                    "order_id": "BUY001",
                    "intent_id": "ENTRY001",
                    "trace_id": "TRACE001",
                    "fill_id": "FILL001",
                    "note": "signal_date=20260319",
                    "source": "TEST",
                    "as_of": "20260320",
                },
                {
                    "date": "20260321",
                    "datetime": "2026-03-21T09:05:00",
                    "code": "005930",
                    "side": "SELL",
                    "fill_price": 110.0,
                    "fill_qty": 40,
                    "order_id": "SELL001",
                    "intent_id": "EXIT001",
                    "trace_id": "TRACE002",
                    "fill_id": "FILL002",
                    "source_order_id": "BUY001",
                    "source_intent_id": "ENTRY001",
                    "source_trace_id": "TRACE001",
                    "note": "signal_date=20260320;partial_fill=1",
                    "source": "TEST",
                    "as_of": "20260321",
                },
                {
                    "date": "20260321",
                    "datetime": "2026-03-21T09:06:00",
                    "code": "005930",
                    "side": "SELL",
                    "fill_price": 108.0,
                    "fill_qty": 60,
                    "order_id": "SELL001",
                    "intent_id": "EXIT001",
                    "trace_id": "TRACE003",
                    "fill_id": "FILL003",
                    "source_order_id": "BUY001",
                    "source_intent_id": "ENTRY001",
                    "source_trace_id": "TRACE001",
                    "note": "signal_date=20260320;partial_fill=2",
                    "source": "TEST",
                    "as_of": "20260321",
                },
            ]
        )

        led = build_enriched_ledger(fills, default_source="TEST")

        buy = led.iloc[0]
        sell_1 = led.iloc[1]
        sell_2 = led.iloc[2]

        self.assertAlmostEqual(float(buy["avg_cost_after_krw"]), 100.02, places=6)
        self.assertAlmostEqual(float(sell_1["matched_qty"]), 40.0, places=6)
        self.assertAlmostEqual(float(sell_1["unmatched_qty"]), 0.0, places=6)
        self.assertAlmostEqual(float(sell_1["position_qty_after"]), 60.0, places=6)
        self.assertAlmostEqual(float(sell_1["avg_cost_after_krw"]), 100.02, places=6)
        self.assertTrue(bool(sell_1["realized_flag"]))
        self.assertAlmostEqual(float(sell_1["realized_pnl_krw"]), 389.52, places=6)

        self.assertAlmostEqual(float(sell_2["matched_qty"]), 60.0, places=6)
        self.assertAlmostEqual(float(sell_2["unmatched_qty"]), 0.0, places=6)
        self.assertAlmostEqual(float(sell_2["position_qty_after"]), 0.0, places=6)
        self.assertAlmostEqual(float(sell_2["avg_cost_after_krw"]), 0.0, places=6)
        self.assertTrue(bool(sell_2["realized_flag"]))
        self.assertAlmostEqual(float(sell_2["realized_pnl_krw"]), 464.544, places=6)

        self.assertAlmostEqual(float(sell_1["fee_krw"]), 0.88, places=6)
        self.assertAlmostEqual(float(sell_1["tax_krw"]), 8.8, places=6)
        self.assertAlmostEqual(float(sell_2["fee_krw"]), 1.296, places=6)
        self.assertAlmostEqual(float(sell_2["tax_krw"]), 12.96, places=6)
        self.assertAlmostEqual(float(sell_1["position_cost_after_krw"]), 6001.2, places=6)
        self.assertAlmostEqual(float(sell_2["position_cost_after_krw"]), 0.0, places=6)

    def test_checklist_builds_shared_module_progress(self) -> None:
        report = {
            "gate_results": [
                {"name": "data_close_integrity", "passed": True, "details": {"nonpositive_close_count": 0, "close_min": 1}},
                {"name": "data_return_finite", "passed": True, "details": {"nonfinite_return_count": 1, "excess_nonfinite_count": 0}},
                {"name": "data_time_index_integrity", "passed": True, "details": {"index_monotonic": True, "duplicate_index_count": 0}},
                {"name": "look_ahead_proxy", "passed": True, "details": {"corr": 0.1, "threshold": 0.2}},
                {"name": "position_lag", "passed": True, "details": {"mismatch_ratio": 0.0}},
                {"name": "historical_scenario_response", "passed": True, "details": {"covered_scenarios": 2, "worst_mdd": -0.2, "median_sharpe": 0.1}},
                {"name": "temporal_consistency", "passed": True, "details": {"n_years": 4, "positive_year_ratio": 0.5, "worst_year_return": -0.1}},
                {"name": "strategy_parameter_validation", "passed": True, "details": {"robust_ratio": 0.9, "base_sharpe": 0.5, "boundary_ratio": 0.1, "domain_ok": True}},
                {"name": "walk_forward", "passed": True, "details": {"median_wfe": 55}},
                {"name": "monte_carlo", "passed": True, "details": {"mc95_mdd": -0.2, "limit": -0.3}},
                {"name": "deflated_sharpe_ratio", "passed": True, "details": {"deflated_sharpe_ratio": 0.42, "min_dsr": 0.10, "n_obs": 120, "n_trials": 9}},
                {"name": "inflation_real_return", "passed": True, "details": {"years_covered": 3, "median_real_return": 0.03, "worst_real_return": -0.05}},
                {"name": "psychological_tolerance", "passed": True, "details": {"max_drawdown": -0.2, "mdd_limit": -0.3}},
                {"name": "outlier_concentration", "passed": True, "details": {"top_contrib_ratio": 0.2, "sample_n": 25}},
                {"name": "market_regime_response", "passed": True, "details": {"valid_regimes": 3, "worst_mdd": -0.2, "median_regime_sharpe": 0.2}},
                {"name": "cpcv_pbo", "passed": True, "details": {"pbo_approx": 0.3, "median_oos_sharpe": 0.2}},
                # [2026-09-09] 이 둘은 뒤에 추가된 필수 체크포인트인데 픽스처가 안 따라와
                #   신호/리스크 모듈이 BLOCKED 로 남고 optimizer_ready 가 False 였다.
                #   backtest_module_progress.py:33,69 가 이 이름을 요구한다.
                #   (미충족 시 막히는지는 test_module_progress_blocks_when_required_checkpoint_missing
                #    이 따로 검사한다. 이 테스트는 **충족했을 때 READY 인가**를 본다)
                {"name": "signal_quality_ic_ir", "passed": True, "details": {"ic_mean": 0.05, "ic_std": 0.1, "ir": 0.5, "ess": 120.0, "min_ess": 30.0, "deferred": False}},
                {"name": "acceptance_pnl_turnover", "passed": True, "details": {"pnl_ci95_low": 0.01, "pnl_ci95_med": 0.03, "pnl_ci95_high": 0.05, "turnover_monthly": 1.5, "turnover_limit_monthly": 5.0}},
            ]
        }
        checklist = build_checklist(report, ROOT / "2_Logs" / "backtest_validation_latest.json")
        module_progress = checklist.get("module_progress", {})
        self.assertTrue(module_progress.get("optimizer_ready"))
        self.assertEqual(len(module_progress.get("modules", [])), 6)
        self.assertEqual(module_progress["modules"][0]["status"], "READY")

    def test_module_progress_blocks_when_required_checkpoint_missing(self) -> None:
        items = [
            {"name": "data_close_integrity", "item": "close_integrity", "status": "PASS", "metric": "-", "threshold": "-", "action": "-"},
            {"name": "data_return_finite", "item": "return_finite", "status": "PASS", "metric": "-", "threshold": "-", "action": "-"},
        ]
        module_progress = build_module_progress(items)
        self.assertFalse(module_progress["optimizer_ready"])
        self.assertGreaterEqual(len(module_progress["optimizer_blockers"]), 1)

    def test_validation_gate_reads_shared_module_progress(self) -> None:
        checklist = {
            "passed": True,
            "operation_judgment": "READY",
            "items": [
                {"name": "strategy_parameter_validation", "status": "PASS"},
            ],
            "module_progress": {
                "optimizer_ready": False,
                "optimizer_blockers": ["risk"],
                "modules": [],
            },
        }
        validation = {
            "gate_results": [
                {"name": "strategy_parameter_validation", "details": {"robust_ratio": 0.9}},
            ]
        }

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            checklist_path = td_path / "backtest_validation_checklist_latest.json"
            validation_path = td_path / "backtest_validation_latest.json"
            checklist_path.write_text(json.dumps(checklist, ensure_ascii=False), encoding="utf-8")
            validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")
            with patch("optimize_if_due_v41_1.BACKTEST_CHECKLIST", checklist_path), patch(
                "optimize_if_due_v41_1.BACKTEST_VALIDATION", validation_path
            ):
                with patch.object(sys, "argv", ["optimize_if_due_v41_1.py"]):
                    args = parse_args()
                ok, detail = _validation_gate(args)
        self.assertFalse(ok)
        self.assertFalse(detail["module_optimizer_ready"])
        self.assertIn("module_progress_not_ready(risk)", detail["reasons"])

    def test_transition_gate_classifies_warmup_and_policy_lock(self) -> None:
        # [2026-09-09] 이 픽스처는 **전환 게이트의 분류**를 격리해서 보려는 것이다.
        #   그래서 분류 대상(워밍업 완화 3종 + 카나리아) 외의 게이트는 전부 통과 상태여야 한다.
        #   그런데 그 뒤에 live_ws_status / live_preflight_health_prod 가 필수 게이트로 추가됐고
        #   paper_judgment_operational 은 "운영가능" 을 요구하는데 픽스처는 "조건부" 였다.
        #   테스트가 다른 이유로 먼저 죽어서 이 어긋남이 가려져 있었다. 픽스처를 현재 게이트에 맞춘다.
        paper = {
            "judgment": "운영가능",
            "items": [
                {"name": "paper_freshness", "status": "PASS"},
                {"name": "paper_trade_count", "status": "PASS"},
                {"name": "paper_risk_off_state", "status": "PASS"},
                {"name": "paper_bt_alignment", "status": "NOT_EVALUABLE", "metric": "alignment_ready=False, trades_used=16", "issue": "워밍업", "action": "표본 확장"},
                {"name": "paper_quality_gate", "status": "NOT_EVALUABLE", "metric": "optimize_gate_ok=False, trades_used=16", "issue": "워밍업", "action": "표본 확장"},
            ],
        }
        live = {
            "judgment": "보류",
            "items": [
                {"name": "live_canary_gate", "status": "PASS"},
                {"name": "live_ws_status", "status": "PASS"},
                {"name": "live_preflight_health_prod", "status": "PASS"},
                {"name": "live_preflight_health_mock", "status": "PASS"},
                {"name": "canary_execute_mode", "status": "NOT_EVALUABLE", "metric": "execute=False", "issue": "실주문 실행 여부", "action": "CANARY_EXECUTE=1"},
            ],
        }
        p2l = _build_transition_gate(paper, live)["paper_to_live"]
        check_by_name = {row["name"]: row for row in p2l["checks"]}
        self.assertEqual(p2l["policy_profile"], "WARMUP_RELAXED_TO_50_CANARY_OPTIONAL")
        self.assertFalse(check_by_name["paper_bt_alignment"]["required"])
        self.assertFalse(check_by_name["paper_quality_gate"]["required"])
        self.assertFalse(check_by_name["canary_execute_mode"]["required"])
        self.assertEqual(check_by_name["paper_bt_alignment"]["status"], "NOT_EVALUABLE")
        self.assertEqual(check_by_name["paper_quality_gate"]["status"], "NOT_EVALUABLE")
        self.assertEqual(check_by_name["canary_execute_mode"]["status"], "NOT_EVALUABLE")
        self.assertEqual(p2l["blocker_summary"]["warmup_n"], 0)
        self.assertEqual(p2l["blocker_summary"]["policy_lock_n"], 0)
        self.assertEqual(p2l["blockers"], [])

    def test_screen_renders_all_tabs_with_shared_interpretation_structure(self) -> None:
        checklist = {
            "generated_at": "2026-03-19 10:00:00",
            "source_json": str(ROOT / "2_Logs" / "backtest_validation_latest.json"),
            "total": 2,
            "pass_n": 2,
            "fail_n": 0,
            "not_evaluable_n": 0,
            "operation_judgment": "조건부운영",
            "items": [
                {
                    "name": "data_close_integrity",
                    "item": "close_integrity",
                    "status": "PASS",
                    "metric": "nonpositive_close_count=0",
                    "threshold": "nonpositive_close_count=0",
                    "issue": "-",
                    "action": "no action",
                },
                {
                    "name": "cpcv_pbo",
                    "item": "CPCV/PBO",
                    "status": "PASS",
                    "metric": "pbo=0.3, med_oos_sharpe=0.2",
                    "threshold": "pbo<=0.50 and med_oos_sharpe>=0",
                    "issue": "-",
                    "action": "keep cross-validation coverage",
                },
            ],
            "module_progress": {
                "optimizer_ready": True,
                "optimizer_blockers": [],
                "modules": [
                    {
                        "tab_no": 1,
                        "title": "Overview",
                        "objective": "Data and timeline integrity",
                        "status": "READY",
                        "completion_pct": 100.0,
                        "ready_for_optimize": True,
                        "pass_n": 3,
                        "total_items": 3,
                        "fail_n": 0,
                        "not_evaluable_n": 0,
                        "missing_n": 0,
                        "blockers": [],
                        "missing_names": [],
                        "evidence": [
                            {
                                "item": "close_integrity",
                                "status": "PASS",
                                "metric": "nonpositive_close_count=0",
                                "threshold": "nonpositive_close_count=0",
                                "action": "no action",
                            }
                        ],
                    }
                ],
            },
        }
        bt_report = {
            "artifacts": {
                "integration": {
                    "strategy_source": "builtin",
                    "backtest_source": "builtin",
                    "params": {"fast": 10, "slow": 100},
                    "n_param_grid": 9,
                    "grid_spec": {"fast": [8, 10, 12]},
                    "market_meta": {
                        "used_column_map": {"date": "date", "close": "close"},
                        "source_rows": 100,
                        "output_rows": 100,
                    },
                    "cost_model": {"commission_bps": 2.0},
                },
                "strategy_parameter_validation": {
                    "grid_eval": [
                        {"params": {"fast": 8, "slow": 100}, "sharpe": 0.4},
                        {"params": {"fast": 10, "slow": 100}, "sharpe": 0.5},
                    ]
                },
                "walk_forward": [
                    {"fold": 0, "is_sharpe": 1.2, "oos_sharpe": 0.8, "wfe": 55.0, "wfe_valid": True, "best_params": {"fast": 10, "slow": 100}},
                    {"fold": 1, "is_sharpe": 1.0, "oos_sharpe": -0.2, "wfe": 20.0, "wfe_valid": True, "best_params": {"fast": 8, "slow": 100}},
                ],
                "cpcv": {"folds": 7},
            }
        }
        final_output = {
            "generated_at": "2026-03-19 10:00:01",
            "final_gate_decision": "HOLD",
            "operation_judgment": "READY",
            "expected_range": {"annual_return": 0.1, "sharpe": 0.5, "max_drawdown": -0.2},
            "operating_parameters": {
                "strategy_source": "builtin",
                "backtest_source": "builtin",
                "params": {"fast": 10, "slow": 100},
                "cost_model": {"commission_bps": 2.0},
                "max_tolerable_mdd": 0.3,
            },
            "result_summary": {
                "profitability": {"message": "keep but review", "reasons": ["annret ok"]},
                "management": {"message": "stable", "needed_actions": ["watch drawdown"]},
            },
            "final_gate_criteria": [
                {"name": "PBO < 0.5", "status": "PASS", "evidence": "pbo=0.3"},
            ],
            "domain_results": [
                {"domain": "1. 로직 안정성", "status": "PASS", "checks": [{"name": "data_close_integrity", "metric": "ok"}]},
            ],
            "stop_triggers": ["stop on mdd breach"],
        }
        trading_stage = {
            "generated_at": "2026-03-19 10:00:02",
            "meta": {"operational_scope": {"rows_after": 16}},
            "paper": {
                "title": "2단계 가상매매 검증",
                "judgment": "조건부",
                "counts": {"total": 2, "pass_n": 1, "fail_n": 0, "not_evaluable_n": 1},
                "items": [
                    {
                        "name": "paper_trade_count",
                        "status": "PASS",
                        "metric": "trades_used=16",
                        "threshold": ">=9",
                        "issue": "가상매매 표본 거래 수",
                        "action": "표본 9회 이상 누적",
                        "required": True,
                    },
                    {
                        "name": "paper_quality_gate",
                        "status": "NOT_EVALUABLE",
                        "metric": "optimize_gate_ok=False",
                        "threshold": "True",
                        "issue": "가상매매 품질 게이트",
                        "action": "표본 확장 후 품질게이트 재검증",
                        "required": True,
                    },
                ],
                "key_actions": ["표본 확장 후 품질게이트 재검증"],
                "sources": {
                    "paper_pnl_summary": str(ROOT / "2_Logs" / "paper_pnl_summary_last.json"),
                    "alignment_snapshot": {
                        "window_start": "20260312",
                        "window_end": "20260318",
                        "live_n": 16,
                        "bt_n": 8,
                        "ret_col_live": "pnl_pct",
                        "ret_col_bt": "ret",
                        "ready": False,
                        "reason": "bt_too_few_trades_in_window",
                        "live_path": "paper/trades.csv",
                        "bt_path": "report_backtest_trades_v41_1.csv",
                        "oper_start_ymd": "20260301",
                        "live_sample_rows": [
                            {"trade_id": "L-1", "code": "005930", "entry_date": "20260314", "exit_date": "20260318", "ret_sample": 0.012},
                        ],
                        "bt_sample_rows": [
                            {"trade_id": "B-1", "code": "005930", "entry_date": "20260313", "exit_date": "20260318", "ret_sample": 0.009},
                        ],
                    },
                },
            },
            "live": {
                "title": "3단계 실전매매 검증",
                "judgment": "보류",
                "counts": {"total": 2, "pass_n": 1, "fail_n": 1, "not_evaluable_n": 0},
                "items": [
                    {
                        "name": "live_e2e_freshness",
                        "status": "FAIL",
                        "metric": "age_days=9.9",
                        "threshold": "<=7",
                        "issue": "실전 E2E 최신성",
                        "action": "run_kis_intraday_e2e.bat 재실행",
                        "required": True,
                    },
                    {
                        "name": "live_preflight_health",
                        "status": "PASS",
                        "metric": "ok=True",
                        "threshold": "ok=True",
                        "issue": "실전 전 헬스체크",
                        "action": "API 계정/키/잔고 환경 재확인",
                        "required": True,
                    },
                ],
                "key_actions": ["run_kis_intraday_e2e.bat 재실행"],
                "sources": {
                    "e2e": str(ROOT / "2_Logs" / "kis_intraday_e2e_latest.json"),
                    "e2e_snapshot": {
                        "generated_at": "2026-03-19T10:00:03",
                        "ok": False,
                        "pass_n": 4,
                        "fail_n": 1,
                        "iterations_done": 1,
                        "steps": [
                            {"name": "preflight_healthcheck", "ok": True, "returncode": 0, "duration_sec": 1.2},
                            {"name": "dispatch_orders", "ok": False, "returncode": 2, "error": "apply guard"},
                        ],
                    },
                    "canary_snapshot": {
                        "generated_at": "2026-03-19T10:00:04",
                        "ok": True,
                        "mode": "DRY",
                        "execute": False,
                        "steps": [
                            {"name": "virtual_trading_gate", "ok": True, "returncode": 0},
                            {"name": "canary_dispatch", "ok": True, "returncode": 0},
                        ],
                    },
                    "fault_snapshot": {
                        "generated_at": "2026-03-19T10:00:05",
                        "ok": True,
                        "pass_n": 4,
                        "fail_n": 0,
                        "results": [
                            {"name": "missing_kis_credentials_fail_closed", "passed": True, "actual": "returncode=2"},
                        ],
                    },
                },
            },
            "transition_gate": {
                "paper_to_live": {
                    "status": "HOLD",
                    "ready": False,
                    "next_step": "paper_fix",
                    "counts": {"total": 2, "pass_n": 1, "fail_n": 1},
                    "blockers": ["paper_quality_gate"],
                    "blocker_details": [
                        {
                            "name": "paper_quality_gate",
                            "blocker_type": "warmup",
                            "blocker_label": "워밍업",
                            "summary": "표본/정렬 준비 부족으로 아직 판정 확정 불가",
                        }
                    ],
                    "blocker_summary": {"warmup_n": 1, "policy_lock_n": 0, "validation_fail_n": 0, "data_gap_n": 0},
                    "policy_profile": "STRICT_PAPER_TO_LIVE",
                    "policy": {"require_canary_execute": True, "warmup_trades": 50},
                    "policy_notes": ["paper_bt_alignment은 정렬 비교 준비가 끝날 때까지 필수"],
                    "checks": [
                        {
                            "name": "paper_quality_gate",
                            "status": "FAIL",
                            "metric": "status=NOT_EVALUABLE",
                            "threshold": "PASS",
                            "issue": "가상매매 품질 게이트",
                            "action": "oos 품질 기준 보강",
                            "required": True,
                        }
                    ],
                }
            },
            "overall": {"judgment": "가상매매 보강", "next_step": "paper_recheck"},
        }
        market_data = {"date_min": "2020-01-02", "date_max": "2025-12-24", "rows": 100, "source_file_count": 2}
        symbol_panel = {"n_symbols": 50, "sector_coverage": 0.9, "market_cap_coverage": 0.95}
        rate_series = {"date_min": "2020-01-02", "date_max": "2025-12-24", "rows": 120, "source": "manual"}

        html = _render(checklist, bt_report, final_output, trading_stage, market_data, symbol_panel, rate_series)

        for needle in (
            "Overview",
            "module-1-filter-status",
            "2단계 가상매매 검증",
            "paper-detail-filter-status",
            "실전 전환 게이트",
            "STRICT_PAPER_TO_LIVE",
            "워밍업",
            "3단계 실전매매 검증",
            "live-detail-filter-status",
            "실전 진입 기준 참조",
            "bt-profile-select",
            "backtest_validation_latest.json",
            "CPCV/PBO",
            "keep cross-validation coverage",
            "2020-01-02",
        ):
            self.assertIn(needle, html)
        self.assertIn("정렬 비교 원본 샘플", html)
        self.assertIn("live 샘플 거래", html)
        self.assertIn("backtest 샘플 거래", html)
        self.assertIn("L-1", html)
        self.assertIn("B-1", html)
        self.assertIn("실행 단계 원본 추적", html)
        self.assertIn("E2E step 상세", html)
        self.assertIn("Canary step 상세", html)
        self.assertIn("Fault case 상세", html)
        self.assertIn("dispatch_orders", html)
        self.assertIn("blocker: module_progress_missing", html)
        self.assertIn("10초 요약", html)
        self.assertIn("한 줄 결론", html)
        self.assertIn("자동 해석 안내", html)
        self.assertIn("원본 로그 바로가기", html)
        self.assertIn("file:///", html)
        self.assertIn("워밍업", html)
        self.assertIn("실제실패", html)
        self.assertNotIn("save-config", html)

    def test_screen_renders_paper_parameter_review_sections(self) -> None:
        checklist = {
            "generated_at": "2026-03-19 10:00:00",
            "source_json": str(ROOT / "2_Logs" / "backtest_validation_latest.json"),
            "total": 1,
            "pass_n": 1,
            "fail_n": 0,
            "not_evaluable_n": 0,
            "operation_judgment": "READY",
            "items": [
                {
                    "name": "data_close_integrity",
                    "item": "close_integrity",
                    "status": "PASS",
                    "metric": "nonpositive_close_count=0",
                    "threshold": "nonpositive_close_count=0",
                    "issue": "-",
                    "action": "no action",
                }
            ],
            "module_progress": {
                "optimizer_ready": True,
                "optimizer_blockers": [],
                "modules": [],
            },
        }
        final_output = {
            "final_gate_decision": "HOLD",
            "final_gate_criteria": [],
            "domain_results": [],
            "operating_parameters": {"params": {"stop_loss": -0.05}},
            "stop_triggers": [],
        }
        trading_stage = {
            "generated_at": "2026-03-19 10:00:02",
            "paper": {"judgment": "조건부", "counts": {"total": 0, "pass_n": 0, "fail_n": 0, "not_evaluable_n": 0}, "items": [], "sources": {}},
            "live": {"judgment": "보류", "counts": {"total": 0, "pass_n": 0, "fail_n": 0, "not_evaluable_n": 0}, "items": [], "sources": {}},
            "transition_gate": {"paper_to_live": {"status": "HOLD", "ready": False, "checks": [], "blocker_details": [], "blocker_summary": {}}},
            "overall": {"judgment": "조건부", "next_step": "paper_recheck"},
        }
        paper_parameter_review = {
            "generated_at": "2026-03-19 14:45:17",
            "source_json": str(ROOT / "2_Logs" / "paper_parameter_review_latest.json"),
            "sample_gate": {"paper_trades_used": 16, "warmup_trades": 50, "enough_for_feedback": False},
            "paper_observations": {
                "risk": {"paper_mdd": -0.13, "last_day_ret": -0.07},
                "comparison": {"alignment_ready": False, "quality_gate_ok": False},
            },
            "parameters": [
                {
                    "name": "stop_loss",
                    "current_value": -0.05,
                    "paper_review": {
                        "status": "blocked",
                        "reason": "insufficient_sample",
                        "evidence": {
                            "paper_mdd": -0.13,
                            "last_day_ret": -0.07,
                            "alignment_ready": False,
                            "quality_gate_ok": False,
                        },
                    },
                    "candidate_update": {"proposed_value": None, "proposed_zone": None, "confidence": "low"},
                }
            ],
        }

        html = _render(checklist, {}, final_output, trading_stage, {}, {}, {}, paper_parameter_review)

        for needle in (
            "paper_parameter_review_latest.json",
            "bt-paper-review-filter-status",
            "paper-review-filter-status",
            "stop_loss",
            "insufficient_sample",
        ):
            self.assertIn(needle, html)


if __name__ == "__main__":
    unittest.main()
