import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd


def _load_target_module():
    root = Path(__file__).resolve().parents[1]
    mod_path = root / "tools" / "build_forward_estimate_snapshot.py"
    spec = importlib.util.spec_from_file_location("build_forward_estimate_snapshot", mod_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


class TestForwardSnapshotUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_target_module()

    # PASS expected
    def test_normalize_asof_valid(self):
        self.assertEqual(self.m._normalize_asof_ymd("20260406"), "20260406")

    # FAIL-path expected (test passes when exception is raised)
    def test_normalize_asof_invalid_format_raises(self):
        with self.assertRaises(Exception):
            self.m._normalize_asof_ymd("2026-04-06")

    # PASS expected
    def test_norm_code6_valid(self):
        self.assertEqual(self.m._norm_code6("5930"), "005930")

    # boundary/invalid
    def test_norm_code6_empty_and_nondigit(self):
        self.assertEqual(self.m._norm_code6(""), "")
        self.assertEqual(self.m._norm_code6("ABC"), "")

    # PASS expected
    def test_dedupe_codes_boundary_max(self):
        out = self.m._dedupe_codes(["005930", "005930", "000660", "035420"], 2)
        self.assertEqual(out, ["005930", "000660"])

    # FAIL-path expected (bad type)
    def test_compute_derived_type_mismatch_raises(self):
        with self.assertRaises(SystemExit):
            self.m._compute_derived(None, 1000.0, 10.0)  # type: ignore[arg-type]

    # FAIL-path expected (invalid transition)
    def test_fsm_invalid_transition_raises(self):
        with self.assertRaises(SystemExit):
            self.m._fsm_transition(self.m.FSM_INIT, "collect_done")

    # PASS expected (deterministic key for same input)
    def test_idempotency_key_deterministic_same_input(self):
        key1 = self.m._compute_idempotency_key(
            "20260406",
            ["005930", "000660"],
            skip_ttm=True,
            skip_wisereport=True,
            strict=True,
            max_external_fail_rate=0.5,
            min_source_coverage=0.3,
            pipeline_version="v1",
        )
        key2 = self.m._compute_idempotency_key(
            "20260406",
            ["000660", "005930"],
            skip_ttm=True,
            skip_wisereport=True,
            strict=True,
            max_external_fail_rate=0.5,
            min_source_coverage=0.3,
            pipeline_version="v1",
        )
        self.assertEqual(key1, key2)

    # FAIL-path expected (strict outlier block)
    def test_validate_transform_row_outlier_raises(self):
        row = {
            "code": "005930",
            "as_of_ymd": "20260406",
            "run_id": "R1",
            "updated_at": "2026-04-06 00:00:00",
            "forward_per": 999999.0,
            "current_per_wr": 10.0,
            "eps_wr": 100.0,
            "ttm_revenue": 1000.0,
            "ttm_operating_profit": 100.0,
            "ttm_net_income": 90.0,
            "forward_eps": 10.0,
            "ttm_opm": 10.0,
            "peg_ratio": 1.0,
        }
        with self.assertRaises(SystemExit):
            self.m._validate_transform_row(row, strict=True)

    # PASS expected for non-strict sanitize
    def test_validate_transform_row_outlier_sanitized_non_strict(self):
        row = {
            "code": "005930",
            "as_of_ymd": "20260406",
            "run_id": "R1",
            "updated_at": "2026-04-06 00:00:00",
            "forward_per": 999999.0,
            "current_per_wr": 10.0,
            "eps_wr": 100.0,
            "ttm_revenue": 1000.0,
            "ttm_operating_profit": 100.0,
            "ttm_net_income": 90.0,
            "forward_eps": 10.0,
            "ttm_opm": 10.0,
            "peg_ratio": 1.0,
        }
        self.m._validate_transform_row(row, strict=False)
        self.assertTrue(pd.isna(row["forward_per"]))

    # FAIL-path expected (duplicate rows)
    def test_validate_before_save_duplicate_rows_raises(self):
        df = pd.DataFrame(
            [
                {"code": "005930", "as_of_ymd": "20260406", "run_id": "R1", "updated_at": "2026-04-06", "forward_per": 10.0, "ttm_revenue": 1.0, "peg_ratio": 1.0},
                {"code": "005930", "as_of_ymd": "20260406", "run_id": "R1", "updated_at": "2026-04-06", "forward_per": 11.0, "ttm_revenue": 1.1, "peg_ratio": 1.1},
            ]
        )
        with self.assertRaises(SystemExit):
            self.m._validate_before_save(
                df,
                "20260406",
                expected_count=2,
                strict=True,
                require_wisereport=False,
                require_ttm=False,
                min_source_coverage=0.0,
            )

    # FAIL-path expected (missing run_id)
    def test_validate_before_save_missing_run_id_raises(self):
        df = pd.DataFrame(
            [{"code": "005930", "as_of_ymd": "20260406", "updated_at": "2026-04-06", "forward_per": 10.0, "ttm_revenue": 1.0, "peg_ratio": 1.0}]
        )
        with self.assertRaises(SystemExit):
            self.m._validate_before_save(
                df,
                "20260406",
                expected_count=1,
                strict=True,
                require_wisereport=False,
                require_ttm=False,
                min_source_coverage=0.0,
            )

    # external dependency failure path
    def test_scrape_wisereport_external_failure_increments_error(self):
        session = Mock()
        session.get.side_effect = RuntimeError("network down")
        stats = {"wr_attempts": 0, "wr_errors": 0}
        out = self.m._scrape_wisereport("005930", session, quality_stats=stats)
        self.assertEqual(out["code"], "005930")
        self.assertEqual(stats["wr_attempts"], 1)
        self.assertEqual(stats["wr_errors"], 1)


class TestForwardSnapshotIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_target_module()

    # integration: stale/as_of mismatch should not be reusable
    def test_is_reusable_output_false_when_asof_mismatch(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "out.csv"
            df = pd.DataFrame(
                [
                    {"code": "005930", "as_of_ymd": "20260405", "run_id": "R1", "updated_at": "2026-04-06", "forward_per": np.nan, "ttm_revenue": np.nan, "peg_ratio": np.nan}
                ]
            )
            df.to_csv(p, index=False, encoding="utf-8-sig")
            ok = self.m._is_reusable_output(p, expected_count=1, as_of_ymd="20260406")
            self.assertFalse(ok)

    # integration: lineage snapshot includes trace fields
    def test_build_lineage_snapshot_has_trace_fields(self):
        meta = {
            "updated_at": "2026-04-06 00:00:00",
            "as_of_ymd": "20260406",
            "run_id": "RID",
            "idempotency_key": "KEY",
            "code_set_sha": "SHA",
            "source_paths": {"codes_source": "a.csv"},
            "lineage": {"rows_out": 1},
            "output": "out.csv",
        }
        snap = self.m._build_lineage_snapshot(meta)
        for k in ["as_of_ymd", "run_id", "idempotency_key", "source_paths", "lineage", "output"]:
            self.assertIn(k, snap)
            self.assertTrue(str(snap[k]) != "")


if __name__ == "__main__":
    unittest.main()
