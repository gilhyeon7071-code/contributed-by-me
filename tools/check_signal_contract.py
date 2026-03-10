from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _check(stage: str, root: Path) -> int:
    logs = root / "2_Logs"

    if stage == "sector":
        inp = logs / "candidates_latest_data.filtered.csv"
        out = logs / "candidates_latest_data.with_sector_score.csv"
        req_col = "sector_score"
    elif stage == "news":
        inp = logs / "candidates_latest_data.with_sector_score.csv"
        out = logs / "candidates_latest_data.with_news_score.csv"
        req_col = "news_score"
    elif stage == "final":
        inp = logs / "candidates_latest_data.with_news_score.csv"
        out = logs / "candidates_latest_data.with_final_score.csv"
        req_col = "final_score"
    else:
        print(f"[CONTRACT] unknown stage: {stage}")
        return 2

    if not inp.exists() or not out.exists():
        print(f"[CONTRACT] {stage} missing file inp={inp.exists()} out={out.exists()} in={inp} out={out}")
        return 2

    try:
        df_in = _read_csv(inp)
        df_out = _read_csv(out)
    except Exception as e:
        print(f"[CONTRACT] {stage} read_fail: {type(e).__name__}: {e}")
        return 2

    in_rows = int(len(df_in))
    out_rows = int(len(df_out))

    if req_col not in df_out.columns:
        print(f"[CONTRACT] {stage} missing required column: {req_col}")
        return 2

    ok = in_rows == out_rows and in_rows >= 0
    print(f"[CONTRACT] {stage} in_rows={in_rows} out_rows={out_rows} col={req_col} ok={ok}")
    return 0 if ok else 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["sector", "news", "final"])
    ap.add_argument("--root", default=str(Path(__file__).resolve().parent.parent))
    ns = ap.parse_args()
    return _check(ns.stage, Path(ns.root))


if __name__ == "__main__":
    raise SystemExit(main())
