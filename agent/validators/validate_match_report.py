from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


REQUIRED = ['code', 'proposed_qty', 'filled_qty', 'status', 'reason']
VALID_STATUS = {'MATCH', 'MISS', 'PARTIAL'}


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_match_report.py <csv_path>')
        return 2

    path = Path(sys.argv[1])
    if not path.exists():
        print(f'[FAIL] missing: {path}')
        return 2

    df = pd.read_csv(path, dtype=str)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        print(f'[FAIL] missing columns: {missing}')
        return 2

    if len(df) == 0:
        print('[PASS] empty match report allowed')
        return 0

    code = df['code'].astype(str)
    if not code.str.fullmatch(r'\d{6}').all():
        print('[FAIL] invalid code format')
        return 2

    pq = pd.to_numeric(df['proposed_qty'], errors='coerce')
    fq = pd.to_numeric(df['filled_qty'], errors='coerce')
    if pq.isna().any() or fq.isna().any():
        print('[FAIL] qty must be numeric')
        return 2
    if (pq < 0).any() or (fq < 0).any():
        print('[FAIL] qty must be non-negative')
        return 2

    if not df['status'].astype(str).isin(VALID_STATUS).all():
        print('[FAIL] invalid status value')
        return 2

    if df['code'].duplicated().any():
        print('[FAIL] duplicate code')
        return 2

    print('[PASS] match report schema/content')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
