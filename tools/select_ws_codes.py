from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _truthy(v: object) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _add_code(out: List[str], seen: Set[str], raw: object, max_codes: int) -> bool:
    code = str(raw or "").strip().zfill(6)
    if not code.isdigit() or len(code) != 6 or code in seen:
        return False
    seen.add(code)
    out.append(code)
    return len(out) >= max_codes


def _read_csv(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def select_codes(max_codes: int) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    # 1) Currently open paper-engine positions: always worth tracking, money is already on the line.
    open_rows = _read_csv(ROOT / "paper" / "trades.csv")
    for row in open_rows:
        if str(row.get("exit_date") or "").strip():
            continue
        if _add_code(out, seen, row.get("code"), max_codes):
            return out

    # 2) Today's scored candidate pool: the most likely near-term entries.
    candidate_rows = _read_csv(LOG_DIR / "candidates_latest_data.with_final_score.csv")
    for row in candidate_rows:
        if _add_code(out, seen, row.get("code"), max_codes):
            return out

    # 3) Live real-time surge flags: catches intraday-emergent names outside the scored pool.
    surge_rows = _read_csv(LOG_DIR / "surge_realtime_latest.csv")
    for row in surge_rows:
        if not (_truthy(row.get("is_realtime_surge")) or _truthy(row.get("surge_flag"))):
            continue
        if _add_code(out, seen, row.get("code"), max_codes):
            return out

    # 4) Generic liquid-universe fallback to fill any remaining subscription slots.
    price_rows = _read_csv(LOG_DIR / "intraday_prices_latest.csv")
    for row in price_rows:
        if _add_code(out, seen, row.get("code"), max_codes):
            return out
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Select KIS websocket subscription codes from latest intraday artifacts")
    ap.add_argument("--max-codes", type=int, default=30)
    args = ap.parse_args()
    print(",".join(select_codes(max(1, int(args.max_codes)))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
