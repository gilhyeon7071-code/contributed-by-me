from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from kis_order_client import KISApiError, KISOrderClient


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    colset = {c.lower(): c for c in cols}
    for cand in candidates:
        hit = colset.get(cand.lower())
        if hit is not None:
            return hit
    return None


def _series_or_default(df: pd.DataFrame, col: Optional[str], default: str) -> pd.Series:
    if col and col in df.columns:
        return df[col].astype(str)
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _norm_side(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"02", "2", "BUY", "B", "매수"}:
        return "BUY"
    if s in {"01", "1", "SELL", "S", "매도"}:
        return "SELL"
    return ""


def _normalize_rows(rows: List[Dict[str, object]], d: str) -> Tuple[pd.DataFrame, Dict[str, object]]:
    info: Dict[str, object] = {"warnings": [], "detected_cols": {}}
    if not rows:
        df = pd.DataFrame(
            columns=[
                "date",
                "code",
                "side",
                "fill_qty",
                "fill_price",
                "order_no",
                "order_branch_no",
                "ord_tmd",
                "ccld_tmd",
                "raw_side",
                "raw_qty",
                "raw_price",
            ]
        )
        return df, info

    raw = pd.DataFrame(rows)

    side_col = _pick_col(raw.columns.tolist(), ["sll_buy_dvsn_cd", "sll_buy_dvsn", "side", "trad_dvsn_name"])
    code_col = _pick_col(raw.columns.tolist(), ["pdno", "item_no", "code", "symbol"])
    qty_col = _pick_col(raw.columns.tolist(), ["tot_ccld_qty", "ccld_qty", "exec_qty", "qty", "ord_qty"])
    price_col = _pick_col(raw.columns.tolist(), ["tot_ccld_unpr", "ccld_unpr", "avg_prvs", "exec_prc", "ord_unpr", "price"])
    odno_col = _pick_col(raw.columns.tolist(), ["odno", "ord_no", "order_no"])
    brno_col = _pick_col(raw.columns.tolist(), ["ord_gno_brno", "order_branch_no", "orgn_odno"])
    ord_tmd_col = _pick_col(raw.columns.tolist(), ["ord_tmd", "ord_tm", "order_time"])
    ccld_tmd_col = _pick_col(raw.columns.tolist(), ["ccld_tmd", "exec_time", "chegyul_time"])

    info["detected_cols"] = {
        "side_col": side_col,
        "code_col": code_col,
        "qty_col": qty_col,
        "price_col": price_col,
        "odno_col": odno_col,
        "brno_col": brno_col,
        "ord_tmd_col": ord_tmd_col,
        "ccld_tmd_col": ccld_tmd_col,
    }

    if code_col is None:
        info["warnings"].append("missing code col candidate")
    if side_col is None:
        info["warnings"].append("missing side col candidate")
    if qty_col is None:
        info["warnings"].append("missing qty col candidate")
    if price_col is None:
        info["warnings"].append("missing price col candidate")

    out = pd.DataFrame(index=raw.index)
    out["date"] = str(d)
    out["code"] = _series_or_default(raw, code_col, "").str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["raw_side"] = _series_or_default(raw, side_col, "")
    out["side"] = out["raw_side"].apply(_norm_side)
    out["raw_qty"] = _series_or_default(raw, qty_col, "0")
    out["fill_qty"] = out["raw_qty"].apply(_to_int)
    out["raw_price"] = _series_or_default(raw, price_col, "0")
    out["fill_price"] = out["raw_price"].apply(_to_int)
    out["order_no"] = _series_or_default(raw, odno_col, "").str.strip()
    out["order_branch_no"] = _series_or_default(raw, brno_col, "").str.strip()
    out["ord_tmd"] = _series_or_default(raw, ord_tmd_col, "").str.strip()
    out["ccld_tmd"] = _series_or_default(raw, ccld_tmd_col, "").str.strip()

    before = len(out)
    out = out[(out["code"].str.len() == 6) & (out["fill_qty"] > 0)].copy()
    out = out[out["side"].isin(["BUY", "SELL"])].copy()
    out = out.reset_index(drop=True)
    after = len(out)
    dropped = before - after
    if dropped > 0:
        info["warnings"].append(f"dropped_rows_after_normalization={dropped}")
    info["rows_before"] = int(before)
    info["rows_after"] = int(after)

    return out, info


def _bridge_to_live_fills(fills_df: pd.DataFrame, d: str, live_path: Path) -> Dict[str, object]:
    live_path.parent.mkdir(parents=True, exist_ok=True)

    bridge = pd.DataFrame(
        {
            "date": str(d),
            "code": fills_df.get("code", pd.Series(dtype=str)).astype(str).str.zfill(6),
            "side": fills_df.get("side", pd.Series(dtype=str)).astype(str).str.upper(),
            "fill_price": fills_df.get("fill_price", pd.Series(dtype=int)).apply(_to_int),
            "fill_qty": fills_df.get("fill_qty", pd.Series(dtype=int)).apply(_to_int),
            "ref_close": "",
        }
    )

    bridge = bridge[(bridge["code"].str.len() == 6) & (bridge["fill_qty"] > 0)].copy()
    bridge = bridge[bridge["side"].isin(["BUY", "SELL"])].copy()

    if live_path.exists():
        try:
            old = pd.read_csv(live_path, dtype=str)
        except Exception:
            old = pd.DataFrame(columns=["date", "code", "side", "fill_price", "fill_qty", "ref_close"])
    else:
        old = pd.DataFrame(columns=["date", "code", "side", "fill_price", "fill_qty", "ref_close"])

    if len(old):
        old["date"] = old["date"].astype(str).str[:8]
        old = old[old["date"] != str(d)].copy()

    out = pd.concat([old, bridge], ignore_index=True)
    out.to_csv(live_path, index=False, encoding="utf-8-sig")

    today_path = live_path.with_name("live_fills_today.csv")
    out[out["date"].astype(str) == str(d)].copy().to_csv(today_path, index=False, encoding="utf-8-sig")

    return {
        "live_path": str(live_path),
        "today_path": str(today_path),
        "rows_written_today": int(len(bridge)),
        "rows_total_live": int(len(out)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch KIS daily filled history and save normalized fills")
    ap.add_argument("--date", default="", help="YYYYMMDD (default=today)")
    ap.add_argument("--start-date", default="", help="YYYYMMDD, default=date")
    ap.add_argument("--end-date", default="", help="YYYYMMDD, default=date")
    ap.add_argument("--pd-dv", default="inner", choices=["before", "inner"])
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--bridge-write", action="store_true", help="Also write normalized fills to live_fills bridge")
    ap.add_argument("--bridge-live-path", default=r"E:\vibe\buffett\data\live\live_fills.csv")
    args = ap.parse_args()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today
    sdate = _norm_ymd(args.start_date) or d
    edate = _norm_ymd(args.end_date) or d

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    out_raw_json = PAPER_DIR / f"kis_daily_ccld_raw_{d}.json"
    out_raw_csv = PAPER_DIR / f"kis_daily_ccld_raw_{d}.csv"
    out_fill_csv = PAPER_DIR / f"kis_fills_api_{d}.csv"
    out_summary_json = LOG_DIR / f"kis_fills_sync_{d}.json"

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
        rsp = client.inquire_daily_ccld(
            start_ymd=sdate,
            end_ymd=edate,
            pd_dv=str(args.pd_dv),
            ccld_dvsn="01",
            inqr_dvsn="00",
            inqr_dvsn_3="00",
            sll_buy_dvsn_cd="00",
            excg_id_dvsn_cd="KRX",
        )
        rows = rsp.get("rows", []) or []
    except KISApiError as e:
        print(f"[STOP] KIS API error: {e}")
        return 2
    except Exception as e:
        print(f"[STOP] sync failed: {e}")
        return 2

    raw_df = pd.DataFrame(rows)
    raw_df.to_csv(out_raw_csv, index=False, encoding="utf-8-sig")
    out_raw_json.write_text(json.dumps({"rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")

    fills_df, norm_info = _normalize_rows(rows, d)
    fills_df.to_csv(out_fill_csv, index=False, encoding="utf-8-sig")

    bridge_info = None
    if args.bridge_write:
        try:
            bridge_info = _bridge_to_live_fills(fills_df, d, Path(args.bridge_live_path))
        except Exception as e:
            print(f"[STOP] bridge write failed: {e}")
            return 2

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "start_date": sdate,
        "end_date": edate,
        "pd_dv": args.pd_dv,
        "mock": args.mock,
        "tr_id": rsp.get("tr_id"),
        "pages": int(rsp.get("pages", 0)),
        "rows_raw": int(len(raw_df)),
        "rows_normalized": int(len(fills_df)),
        "normalize_info": norm_info,
        "bridge_write": bool(args.bridge_write),
        "bridge": bridge_info,
        "paths": {
            "raw_json": str(out_raw_json),
            "raw_csv": str(out_raw_csv),
            "fills_csv": str(out_fill_csv),
        },
    }
    out_summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] raw_csv={out_raw_csv} rows={len(raw_df)}")
    print(f"[OK] fills_csv={out_fill_csv} rows={len(fills_df)}")
    if norm_info.get("warnings"):
        print(f"[WARN] normalize warnings: {norm_info.get('warnings')}")
    if bridge_info:
        print(f"[OK] bridge_live={bridge_info['live_path']} rows_written_today={bridge_info['rows_written_today']}")
    print(f"[OK] summary={out_summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
