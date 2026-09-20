from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna('')


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_golden_match.py <golden_base_path>')
        return 2

    base = Path(sys.argv[1])
    in_dir = base / 'inputs'
    exp_dir = base / 'outputs'
    gen_dir = base / 'outputs_generated'

    if not in_dir.exists() or not exp_dir.exists() or not gen_dir.exists():
        print('[FAIL] missing golden directories')
        return 2

    files = sorted([p.name for p in in_dir.glob('*.csv')])
    if not files:
        print('[FAIL] no golden inputs')
        return 2

    fail = 0
    for name in files:
        exp = exp_dir / name
        gen = gen_dir / name
        if not exp.exists() or not gen.exists():
            print(f'[FAIL] missing pair: {name}')
            fail += 1
            continue

        dfe = _read(exp)
        dfg = _read(gen)
        if list(dfe.columns) != list(dfg.columns):
            print(f'[FAIL] columns mismatch: {name}')
            fail += 1
            continue

        if not dfe.equals(dfg):
            print(f'[FAIL] content mismatch: {name}')
            fail += 1

    if fail:
        return 2

    print('[PASS] golden match')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
