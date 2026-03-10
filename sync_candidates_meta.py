from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
CAND_DATA = LOG_DIR / "candidates_latest_data.csv"
CAND_META = LOG_DIR / "candidates_latest_meta.json"


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_csv(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists() or p.stat().st_size == 0:
        return None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _safe_read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _safe_write_json(p: Path, obj: Dict[str, Any]) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    df = _load_csv(CAND_DATA)
    if df is None or df.empty or ("date" not in df.columns):
        print(f"[META_SYNC] skipped: missing or empty {CAND_DATA.name} or no 'date' column")
        return 0

    # normalize candidate date and take max
    # parse date robustly: supports 'YYYY-MM-DD', 'YYYYMMDD', and numeric yyyymmdd (int/float)
    s = df["date"]
    if pd.api.types.is_datetime64_any_dtype(s):
        dd = s
    else:
        if pd.api.types.is_numeric_dtype(s):
            s2 = pd.to_numeric(s, errors="coerce").astype("Int64").astype(str)
        else:
            s2 = s.astype(str).str.strip()
        s2 = s2.str.replace("-", "", regex=False).str.slice(0, 8)
        dd = pd.to_datetime(s2, format="%Y%m%d", errors="coerce")
    if dd.isna().all():
        print(f"[META_SYNC] skipped: cannot parse date column in {CAND_DATA.name}")
        return 0

    latest = dd.max()
    latest_str = latest.strftime("%Y-%m-%d")

    meta = _safe_read_json(CAND_META) if CAND_META.exists() else {}
    if not isinstance(meta, dict):
        meta = {}

    old = str(meta.get("latest_date") or "")

    if old != latest_str:
        # backup (only when change)
        bak = LOG_DIR / "candidates_latest_meta.autobak.json"
        if CAND_META.exists():
            try:
                bak.write_text(CAND_META.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception:
                try:
                    bak.write_text(CAND_META.read_text(encoding="utf-8-sig"), encoding="utf-8")
                except Exception:
                    pass

        meta["latest_date"] = latest_str
        meta["as_of"] = _now_ts()
        _safe_write_json(CAND_META, meta)
        print(f"[META_SYNC] updated latest_date: {old or '(empty)'} -> {latest_str}")
    else:
        # still refresh as_of to show the sync ran (optional)
        meta["as_of"] = _now_ts()
        _safe_write_json(CAND_META, meta)
        print(f"[META_SYNC] ok: latest_date={latest_str}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
