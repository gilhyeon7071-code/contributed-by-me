from pathlib import Path
import importlib.util
import sys


def _load_module():
    mod_path = Path(r"E:\1_Data\tools\surge_reversal_compare.py")
    spec = importlib.util.spec_from_file_location("surge_reversal_compare", mod_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_calc_reversal_signals_detects_multiple_signals():
    mod = _load_module()
    close_hist = [
        100, 102, 104, 107, 110, 113, 117, 120, 124, 127,
        130, 133, 137, 140, 143, 146, 150, 148, 145, 141, 138,
    ]
    volume_hist = [1000, 420, 380, 300, 280, 260, 250, 240, 230, 220, 210, 200, 190, 180, 170, 160, 150, 140, 130, 120, 110]
    signals = mod._calc_reversal_signals(
        close_hist,
        volume_hist,
        surge_volume=1000,
        high_since_entry=150,
        candidate_cfg=mod.DEFAULT_CANDIDATE_CFG,
    )
    assert "VOLUME_EXHAUSTION" in signals
    assert "HIGH_REJECTION" in signals
    assert len(signals) >= 2


def test_calc_reversal_signals_respects_high_rejection_threshold():
    mod = _load_module()
    close_hist = [100, 105, 110, 108, 107, 106]
    volume_hist = [1000, 600, 590, 580, 570, 560]
    signals = mod._calc_reversal_signals(
        close_hist,
        volume_hist,
        surge_volume=1000,
        high_since_entry=110,
        candidate_cfg={**mod.DEFAULT_CANDIDATE_CFG, "high_rejection_min_drawdown_pct": 0.05},
    )
    assert "HIGH_REJECTION" not in signals


def test_summarize_empty():
    mod = _load_module()
    out = mod._summarize(mod.pd.DataFrame())
    assert out["trades"] == 0
    assert out["win_rate"] == 0.0


def test_contribution_summary_counts_reversal_deltas():
    mod = _load_module()
    base_df = mod.pd.DataFrame(
        [
            {"code": "000001", "entry_date": "20260101", "ret": -0.05, "exit_reason": "STOP"},
            {"code": "000002", "entry_date": "20260102", "ret": 0.01, "exit_reason": "TIME"},
        ]
    )
    cand_df = mod.pd.DataFrame(
        [
            {"code": "000001", "entry_date": "20260101", "ret": -0.02, "exit_reason": "REVERSAL_NEXT_OPEN"},
            {"code": "000002", "entry_date": "20260102", "ret": 0.01, "exit_reason": "TIME"},
        ]
    )
    out = mod._contribution_summary(base_df, cand_df)
    assert out["reversal_exit_count"] == 1
    assert out["better_than_baseline_count"] == 1
