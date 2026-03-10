from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


def _detect_date(args_date: str) -> Optional[str]:
    s = "".join(ch for ch in str(args_date or "") if ch.isdigit())
    if len(s) == 8:
        return s

    files = sorted(PAPER_DIR.glob("orders_*_broker_submit_mock.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    stem = files[0].stem
    digits = "".join(ch for ch in stem if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def _summ(df: pd.DataFrame) -> Dict[str, object]:
    if len(df) == 0:
        return {"rows": 0, "counts": {}, "accepted": 0, "failed": 0, "accept_rate": None}
    st = df.get("dispatch_status", pd.Series(dtype=str)).astype(str)
    counts = st.value_counts(dropna=False).to_dict()
    accepted = int((st == "ACCEPTED").sum())
    failed = int(st.isin(["REJECTED", "ERROR"]).sum())
    return {
        "rows": int(len(df)),
        "counts": counts,
        "accepted": accepted,
        "failed": failed,
        "accept_rate": (accepted / len(df)) if len(df) > 0 else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare mock vs prod broker submit logs")
    ap.add_argument("--date", default="", help="YYYYMMDD")
    args = ap.parse_args()

    d = _detect_date(args.date)
    if not d:
        print("[STOP] no date detected")
        return 2

    mock_csv = PAPER_DIR / f"orders_{d}_broker_submit_mock.csv"
    prod_csv = PAPER_DIR / f"orders_{d}_broker_submit_prod.csv"

    df_mock = _load(mock_csv)
    df_prod = _load(prod_csv)

    sm = _summ(df_mock)
    sp = _summ(df_prod)

    out_json = LOG_DIR / f"kis_mode_compare_{d}.json"
    out_md = LOG_DIR / f"kis_mode_compare_{d}.md"

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "paths": {"mock_csv": str(mock_csv), "prod_csv": str(prod_csv)},
        "mock": sm,
        "prod": sp,
        "delta": {
            "rows": int(sm["rows"] or 0) - int(sp["rows"] or 0),
            "accepted": int(sm["accepted"] or 0) - int(sp["accepted"] or 0),
            "failed": int(sm["failed"] or 0) - int(sp["failed"] or 0),
        },
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md = [
        f"# KIS Mode Compare ({d})",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- mock rows/accepted/failed: {sm['rows']}/{sm['accepted']}/{sm['failed']}",
        f"- prod rows/accepted/failed: {sp['rows']}/{sp['accepted']}/{sp['failed']}",
        "",
        "## Counts (mock)",
        json.dumps(sm["counts"], ensure_ascii=False, indent=2),
        "",
        "## Counts (prod)",
        json.dumps(sp["counts"], ensure_ascii=False, indent=2),
    ]
    out_md.write_text("\n".join(md), encoding="utf-8")

    print(f"[OK] json={out_json}")
    print(f"[OK] md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
