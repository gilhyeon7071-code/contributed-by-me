from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


REQUIRED = ['code', 'side', 'qty', 'order_type', 'reason']


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_orders_proposal.py <csv_path>')
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
        print('[PASS] empty orders proposal allowed')
        return 0

    code = df['code'].astype(str)
    if not code.str.fullmatch(r'\d{6}').all():
        print('[FAIL] invalid code format')
        return 2

    if not (df['side'].astype(str) == 'BUY').all():
        print('[FAIL] side must be BUY')
        return 2

    if not (df['order_type'].astype(str) == 'MKT').all():
        print('[FAIL] order_type must be MKT')
        return 2

    qty = pd.to_numeric(df['qty'], errors='coerce')
    if qty.isna().any() or not (qty > 0).all():
        print('[FAIL] qty must be positive number')
        return 2

    if df['code'].duplicated().any():
        print('[FAIL] duplicate code')
        return 2

    print('[PASS] orders proposal schema/content')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
