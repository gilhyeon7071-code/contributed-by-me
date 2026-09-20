from pathlib import Path
import importlib.util
import sys


def _load_module(name: str, path: str):
    mod_path = Path(path)
    spec = importlib.util.spec_from_file_location(name, mod_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_macro_unknown_regime_maps_to_correction():
    mod = _load_module("generate_candidates_v41_1", r"E:\1_Data\generate_candidates_v41_1.py")
    assert mod._map_macro_to_candidate_regime("", fallback_is_bull=True) == "CORRECTION"


def test_factor_guard_negative_shift_blocks():
    mod = _load_module("generate_candidates_v41_1_factor_guard", r"E:\1_Data\generate_candidates_v41_1.py")
    blocked, reason = mod._should_fail_closed_on_factor_guard(
        {
            "lookahead": {
                "negative_shift_detected": 1,
                "negative_shift_terms": [".shift(-1)"],
            }
        }
    )
    assert blocked is True
    assert "negative_shift_detected" in reason


def test_sector_score_fallback_is_normal():
    mod = _load_module("sector_score_daily", r"E:\1_Data\tools\sector_score_daily.py")
    assert mod._load_p0_regime() == "NORMAL"
