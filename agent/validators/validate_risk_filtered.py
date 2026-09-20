from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = ['code', 'score', 'risk_pass', 'risk_reason']


def _to_int01(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin({'1', 'true', 'y', 'yes'}).astype(int)


def main() -> int:
    if len(sys.argv) < 2:
        print('[FAIL] usage: validate_risk_filtered.py <csv_path>')
        return 2

    path = Path(sys.argv[1])
    if not path.exists():
        print(f'[FAIL] missing file: {path}')
        return 2

    try:
        df = pd.read_csv(path, dtype=str)
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
    if not code.str.fullmatch(r'\d{6}').fillna(False).all():
        print('[FAIL] invalid code format')
        return 2

    score = pd.to_numeric(df['score'], errors='coerce')
    if score.isna().any() or not ((score >= 0) & (score <= 100)).all():
        print('[FAIL] invalid score')
        return 2

    rp = _to_int01(df['risk_pass'])
    if rp.isna().any():
        print('[FAIL] invalid risk_pass')
        return 2

    rr = df['risk_reason'].astype(str).str.strip()
    if rr.eq('').any():
        print('[FAIL] empty risk_reason')
        return 2

    if code.duplicated().any():
        print('[FAIL] duplicate code')
        return 2

    print('[PASS] risk schema/content')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
