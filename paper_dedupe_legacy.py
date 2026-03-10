from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd
import re
import json

BASE_DIR = Path(__file__).resolve().parent
PAPER_DIR = BASE_DIR / "paper"
FILLS = PAPER_DIR / "fills.csv"
TRADES = PAPER_DIR / "trades.csv"
STATE = PAPER_DIR / "paper_state.json"

_SIGNAL_RE = re.compile(r"signal_date=(\d{8})")

def _read_csv_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def _backup(path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak_{ts}")
    shutil.copy2(path, bak)
    return bak

def _extract_signal_date(note: str | None) -> str | None:
    if not note:
        return None
    m = _SIGNAL_RE.search(str(note))
    return m.group(1) if m else None

def main() -> int:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)

    fills = _read_csv_any(FILLS)
    trades = _read_csv_any(TRADES)

    if len(fills) > 0:
        if "order_id" in fills.columns:
            fills_d = fills.drop_duplicates(subset=["order_id"], keep="first").reset_index(drop=True)
        else:
            fills_d = fills.drop_duplicates(keep="first").reset_index(drop=True)
        _backup(FILLS)
        fills_d.to_csv(FILLS, index=False, encoding="utf-8-sig")
        print(f"[OK] fills deduped: {len(fills)} -> {len(fills_d)}")
    else:
        print("[SKIP] fills empty or missing")

    if len(trades) > 0:
        # trade_id는 재생성(연속) + 나머지 컬럼으로 중복 제거
        key_cols = [c for c in trades.columns if c != "trade_id"]
        if key_cols:
            trades_d = trades.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
        else:
            trades_d = trades.drop_duplicates(keep="first").reset_index(drop=True)
        # renumber trade_id if present
        if "trade_id" in trades_d.columns:
            trades_d["trade_id"] = [f"T{i:06d}" for i in range(1, len(trades_d) + 1)]
        _backup(TRADES)
        trades_d.to_csv(TRADES, index=False, encoding="utf-8-sig")
        print(f"[OK] trades deduped: {len(trades)} -> {len(trades_d)}")
    else:
        print("[SKIP] trades empty or missing")

    # update paper_state.json processed_signals from trades notes (best-effort)
    if STATE.exists():
        try:
            st = json.loads(STATE.read_text(encoding="utf-8"))
        except Exception:
            st = {}
    else:
        st = {}
    st.setdefault("open_positions", [])
    st.setdefault("next_trade_seq", 1)
    st.setdefault("processed_signals", [])

    if len(trades) > 0 and "code" in trades.columns and "note" in trades.columns:
        ps = set()
        for _, r in trades.iterrows():
            code = str(r.get("code", "")).zfill(6)
            sd = _extract_signal_date(r.get("note"))
            if sd:
                ps.add(f"{code}:{sd}")
        st["processed_signals"] = sorted(ps)

    STATE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] state updated: {STATE}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
