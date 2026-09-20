from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = ['code', 'score', 'reason']


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_candidates_scored.py <csv_path>')
        return 2

    path = Path(sys.argv[1])
    if not path.exists():
        print(f'[FAIL] missing file: {path}')
        return 2

    try:
        df = pd.read_csv(path, dtype={'code': str})
    except Exception as e:
        print(f'[FAIL] read error: {type(e).__name__}: {e}')
        return 2

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            print(f'[FAIL] missing column: {col}')
            return 2

    if df.empty:
        print('[FAIL] empty rows')
        return 2

    code = df['code'].astype(str).str.zfill(6)
    if code.str.fullmatch(r'\d{6}').fillna(False).all() is False:
        print('[FAIL] invalid code format (expect 6-digit)')
        return 2

    score = pd.to_numeric(df['score'], errors='coerce')
    if score.isna().any():
        print('[FAIL] score NaN')
        return 2
    if not ((score >= 0) & (score <= 100)).all():
        print('[FAIL] score range (0~100)')
        return 2

    if code.duplicated().any():
        print('[FAIL] duplicate code')
        return 2

    if df['reason'].astype(str).str.strip().eq('').any():
        print('[FAIL] empty reason')
        return 2

    print('[PASS] schema/content')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
