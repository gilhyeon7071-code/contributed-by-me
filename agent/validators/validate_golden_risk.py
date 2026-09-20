from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['code', 'score', 'risk_pass', 'risk_reason']
    for c in cols:
        if c not in df.columns:
            raise ValueError(f'missing column: {c}')
    out = df[cols].copy()
    out['code'] = out['code'].astype(str).str.zfill(6)
    out['score'] = pd.to_numeric(out['score'], errors='raise').astype(float).round(6)
    out['risk_pass'] = out['risk_pass'].astype(str).str.strip().str.lower().isin({'1','true','y','yes'}).astype(int)
    out['risk_reason'] = out['risk_reason'].astype(str).str.strip()
    out = out.sort_values(['code', 'score', 'risk_pass', 'risk_reason']).reset_index(drop=True)
    return out


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_golden_risk.py <golden_base_dir>')
        return 2

    base = Path(sys.argv[1])
    in_dir = base / 'inputs'
    exp_dir = base / 'outputs'
    got_dir = base / 'outputs_generated'

    if not in_dir.exists() or not exp_dir.exists() or not got_dir.exists():
        print('[FAIL] golden dirs missing')
        return 2

    inputs = sorted(in_dir.glob('*.csv'))
    if not inputs:
        print('[PASS] no golden inputs (skip)')
        return 0

    fails = 0
    for inp in inputs:
        name = inp.name
        exp = exp_dir / name
        got = got_dir / name
        if not exp.exists() or not got.exists():
            print(f'[FAIL] missing pair: {name}')
            fails += 1
            continue
        try:
            dfe = _norm(pd.read_csv(exp, dtype=str))
            dfg = _norm(pd.read_csv(got, dtype=str))
            same = dfe.equals(dfg)
        except Exception as e:
            print(f'[FAIL] {name} read/normalize error: {type(e).__name__}: {e}')
            fails += 1
            continue
        if not same:
            print(f'[FAIL] {name} mismatch')
            fails += 1

    if fails:
        return 2
    print('[PASS] golden risk')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
