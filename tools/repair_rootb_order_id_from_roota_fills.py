from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd


ROOTA = Path(__file__).resolve().parents[1]
ROOTB = ROOTA.parent / "vibe" / "buffett"
FILLS_PATH = ROOTA / "paper" / "fills.csv"
LIVE_PATH = ROOTB / "data" / "live" / "live_fills.csv"
LEDGER_PATH = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"
STATUS_PATH = ROOTA / "2_Logs" / "rootb_order_id_preserve_latest.json"


def _date8(v: Any) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())[:8]


def _code(v: Any) -> str:
    raw = str(v or "").strip().replace(".0", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _side(v: Any) -> str:
    s = str(v or "").strip().upper()
    return {"B": "BUY", "S": "SELL"}.get(s, s)


def _num_key(v: Any) -> str:
    try:
        return f"{float(str(v).replace(',', '')):.6f}"
    except Exception:
        return "0.000000"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _key_frame(df: pd.DataFrame, date_col: str, price_col: str, qty_col: str) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    return (
        df[date_col].map(_date8)
        + "|"
        + df["code"].map(_code)
        + "|"
        + df["side"].map(_side)
        + "|"
        + df[qty_col].map(_num_key)
        + "|"
        + df[price_col].map(_num_key)
    )


def _backup(run_id: str, apply: bool) -> str:
    if not apply:
        return ""
    backup_dir = ROOTA / "backup" / "20260429_ledger_order_id_preserve_apply" / run_id
    backup_dir.mkdir(parents=True, exist_ok=True)
    for src in (LIVE_PATH, LEDGER_PATH):
        if src.exists():
            shutil.copy2(src, backup_dir / f"{src.name}.bak")
    return str(backup_dir)


def _build_order_map(fills: pd.DataFrame, ymd: str) -> Dict[str, str]:
    if fills.empty or "order_id" not in fills.columns:
        return {}
    f = fills.copy()
    f["_date8"] = f["datetime"].map(_date8) if "datetime" in f.columns else ""
    f = f[f["_date8"] == ymd].copy()
    if f.empty:
        return {}
    for col in ("code", "side", "qty", "price"):
        if col not in f.columns:
            return {}
    f["_key"] = _key_frame(f, "_date8", "price", "qty")
    f["_order_id"] = f["order_id"].astype(str).str.strip()
    f = f[(f["_key"].str.len() > 0) & (f["_order_id"] != "")].copy()
    counts = f.groupby("_key")["_order_id"].nunique()
    unique_keys = set(counts[counts == 1].index.tolist())
    return dict(f[f["_key"].isin(unique_keys)].drop_duplicates("_key", keep="last")[["_key", "_order_id"]].itertuples(index=False, name=None))


def _apply_map(df: pd.DataFrame, ymd: str, order_map: Dict[str, str]) -> Tuple[pd.DataFrame, int]:
    if df.empty or not order_map:
        return df, 0
    out = df.copy()
    date_col = "date" if "date" in out.columns else ("datetime" if "datetime" in out.columns else "")
    price_col = "fill_price" if "fill_price" in out.columns else ("price" if "price" in out.columns else "")
    qty_col = "fill_qty" if "fill_qty" in out.columns else ("qty" if "qty" in out.columns else "")
    if not date_col or not price_col or not qty_col or "code" not in out.columns or "side" not in out.columns:
        return out, 0
    if "order_id" not in out.columns:
        out["order_id"] = ""
    keys = _key_frame(out, date_col, price_col, qty_col)
    date_mask = out[date_col].map(_date8).eq(ymd)
    mapped = keys.map(order_map).fillna("")
    cur = out["order_id"].astype(str).str.strip()
    mask = date_mask & mapped.ne("") & cur.ne(mapped)
    changed = int(mask.sum())
    if changed:
        out.loc[mask, "order_id"] = mapped.loc[mask]
    return out, changed


def repair(ymd: str, apply: bool) -> Dict[str, Any]:
    run_id = "ORDER_ID_PRESERVE_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    fills = _read_csv(FILLS_PATH)
    live = _read_csv(LIVE_PATH)
    ledger = _read_csv(LEDGER_PATH)
    order_map = _build_order_map(fills, ymd)
    live_new, live_changed = _apply_map(live, ymd, order_map)
    ledger_new, ledger_changed = _apply_map(ledger, ymd, order_map)
    backup_dir = _backup(run_id, apply and (live_changed > 0 or ledger_changed > 0))
    if apply:
        if live_changed > 0:
            live_new.to_csv(LIVE_PATH, index=False, encoding="utf-8-sig")
        if ledger_changed > 0:
            ledger_new.to_csv(LEDGER_PATH, index=False, encoding="utf-8-sig")
    status = "APPLIED" if apply and (live_changed > 0 or ledger_changed > 0) else ("DRY_RUN" if not apply else "NOOP")
    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "as_of_ymd": ymd,
        "apply": bool(apply),
        "status": status,
        "order_map_size": int(len(order_map)),
        "live_changed_rows": int(live_changed),
        "ledger_changed_rows": int(ledger_changed),
        "paths": {
            "fills": str(FILLS_PATH),
            "live": str(LIVE_PATH),
            "ledger": str(LEDGER_PATH),
        },
        "backup_dir": backup_dir,
    }
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Preserve RootA fill order_id in RootB live_fills and ledger rows.")
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    ymd = _date8(args.date)
    if len(ymd) != 8:
        print("[ORDER_ID_PRESERVE] invalid date")
        return 2
    payload = repair(ymd, bool(args.apply))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
