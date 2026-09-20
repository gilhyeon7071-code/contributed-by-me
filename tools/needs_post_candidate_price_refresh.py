from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import logging




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="E:/1_Data")
    args = parser.parse_args()

    root = Path(args.root)
    cand_path = root / "2_Logs" / "candidates_latest_data.csv"
    price_path = root / "paper" / "prices" / "ohlcv_paper.parquet"

    if not cand_path.exists() or not price_path.exists():
        _log_print("[POST_REFRESH_CHECK] missing artifact -> NEED")
        return 10

    try:
        cand = pd.read_csv(cand_path)
        price = pd.read_parquet(price_path)
    except Exception as exc:
        _log_print(f"[POST_REFRESH_CHECK] read_error={exc} -> NEED")
        return 10

    cand_code_col = _find_col(cand, ["code", "Code", "종목코드"])
    price_code_col = _find_col(price, ["code", "Code", "종목코드"])
    cand_date_col = _find_col(cand, ["date", "Date", "일자"])
    price_date_col = _find_col(price, ["date", "Date", "일자"])

    if not cand_code_col or not price_code_col:
        _log_print("[POST_REFRESH_CHECK] code_col_missing -> NEED")
        return 10

    cand_codes = set(cand[cand_code_col].astype(str).str.zfill(6))
    price_codes = set(price[price_code_col].astype(str).str.zfill(6))
    missing_codes = sorted(cand_codes - price_codes)

    cand_date_max = None
    price_date_max = None
    if cand_date_col:
        cand_date_max = str(cand[cand_date_col].dropna().astype(str).max())
    if price_date_col:
        price_date_max = str(price[price_date_col].dropna().astype(str).max())

    need_by_code = len(missing_codes) > 0
    need_by_date = bool(cand_date_max and price_date_max and cand_date_max > price_date_max)
    need = need_by_code or need_by_date

    _log_print(
        "[POST_REFRESH_CHECK] "
        f"need={int(need)} "
        f"missing_codes={len(missing_codes)} "
        f"cand_date_max={cand_date_max} "
        f"price_date_max={price_date_max}"
    )
    if missing_codes:
        _log_print(f"[POST_REFRESH_CHECK] missing_sample={','.join(missing_codes[:10])}")

    return 10 if need else 0


if __name__ == "__main__":
    raise SystemExit(main())
