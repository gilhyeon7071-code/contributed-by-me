# -*- coding: utf-8 -*-
"""검사기가 **패치하려는 이름이 실제로 그 모듈에 있는지** 확인한다.

2026-09-11: validate_intraday_residual_overnight_guard_activation.py 가
`importlib.import_module("paper_engine")` 로 패키지를 잡고 있었다. 두 가지가 걸렸다.

1. 함수가 패키지 최상위에 없어 AttributeError -> 매일 rc=1 advisory (아무도 안 봄)
2. 더 나쁜 것: SHADOW_*_PATH 를 tmp 로 갈아끼우는 방어가 **무효**였다.
   exit.py 가 그 이름을 import 시점에 자기 네임스페이스로 바인딩하므로,
   패키지에 설정해도 안 바뀐다 -> 합성 데이터가 진짜 latest.json/csv 를 덮어쓴다.

주석으로 막으면 사람이 읽어야 작동한다. 이 테스트는 잊어도 작동한다.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 검사기가 잡는 모듈 -> 거기 있어야 하는 이름
EXPECTED = {
    "paper_engine.exit": [
        "_apply_intraday_residual_overnight_guard_exits",
        "_write_intraday_residual_overnight_guard_shadow",
        "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH",
        "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH",
    ],
    "paper_engine.positions": [
        "_reconcile_open_positions_with_fills",
    ],
}


def test_validator_targets_resolve():
    for mod_name, names in EXPECTED.items():
        mod = importlib.import_module(mod_name)
        for n in names:
            assert hasattr(mod, n), f"{n} 이 {mod_name} 에 없다 - 검사기가 다시 깨진다"


def test_shadow_paths_are_patchable_where_writer_reads_them():
    """쓰기 함수가 읽는 **바로 그 네임스페이스**에서 패치 가능해야 한다.

    패키지에 설정하는 것으로는 안 된다는 것을 함께 고정한다.
    """
    ex = importlib.import_module("paper_engine.exit")
    pkg = importlib.import_module("paper_engine")

    # 쓰기 함수가 사는 곳에는 있다
    assert hasattr(ex, "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH")
    # 패키지 최상위에는 없다 -> 거기 설정하면 패치가 무효다 (이것이 2026-09-11 결함)
    assert not hasattr(pkg, "INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH")


def test_validator_source_imports_the_right_modules():
    """소스가 다시 패키지를 잡으면 실패한다."""
    src = (ROOT / "tools" / "validate_intraday_residual_overnight_guard_activation.py").read_text(encoding="utf-8")
    assert 'importlib.import_module("paper_engine.exit")' in src
    assert 'importlib.import_module("paper_engine.positions")' in src
    assert 'importlib.import_module("paper_engine")' not in src, "패키지를 잡으면 tmp 패치가 무효가 된다"
