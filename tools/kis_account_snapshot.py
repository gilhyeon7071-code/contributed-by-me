from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from kis_order_client import KISOrderClient


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_account_snapshot")


def _api_error_fields(e: Exception) -> Dict[str, object]:
    return {
        "error_type": type(e).__name__,
        "error_code": str(getattr(e, "code", "") or ""),
        "error_category": str(getattr(e, "category", "") or ""),
        "status_code": getattr(e, "status_code", None),
        "path": str(getattr(e, "path", "") or ""),
        "message": str(e),
    }


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(default)


def _pick_first_float(d: Dict[str, object], keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d:
            v = _to_float(d.get(k), default=default)
            if v != 0.0:
                return float(v)
    return float(default)


def _pick_first_int(d: Dict[str, object], keys: List[str], default: int = 0) -> int:
    for k in keys:
        if k in d:
            v = _to_int(d.get(k), default=default)
            if v != 0:
                return int(v)
    return int(default)


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="Build centralized KIS account snapshot (avg price + unrealized pnl)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--with-quotes", action="store_true", help="Fetch latest quote per position")
    args = ap.parse_args()

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"kis_account_snapshot_{ts}.json"
    out_csv = LOG_DIR / f"kis_account_positions_{ts}.csv"
    out_latest = LOG_DIR / "kis_account_snapshot_latest.json"

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        logger.error("[STOP] KIS env/config failed: %s", e)
        return 2

    mode = "mock" if client.cfg.mock else "prod"

    try:
        bal = client.inquire_balance_positions(max_pages=10)
    except Exception as e:
        logger.error("[STOP] inquire_balance failed: %s", e)
        return 2

    rows = bal.get("rows", []) or []

    pos_rows: List[Dict[str, object]] = []
    quote_fail_count = 0
    for r in rows:
        code = str(r.get("pdno", "") or r.get("code", "") or "").strip().zfill(6)
        if not code.isdigit() or len(code) != 6:
            continue

        qty = _pick_first_int(r, ["hldg_qty", "hold_qty", "qty", "ord_psbl_qty"], default=0)
        if qty <= 0:
            continue

        avg_price = _pick_first_float(r, ["pchs_avg_pric", "avg_pric", "avg_price", "pchs_pric"], default=0.0)
        close_ref = _pick_first_float(r, ["prpr", "stck_prpr", "now_pric", "current_price"], default=0.0)

        last_price = close_ref
        if args.with_quotes:
            try:
                q = client.inquire_price(code=code)
                qo = q.get("output", {}) or {}
                lp = _to_float(qo.get("stck_prpr", 0), 0.0)
                if lp > 0:
                    last_price = lp
            except Exception as e:
                quote_fail_count += 1
                ef = _api_error_fields(e)
                logger.warning(
                    "[WARN] quote failed code=%s type=%s code=%s category=%s status=%s path=%s",
                    code,
                    ef.get("error_type", ""),
                    ef.get("error_code", ""),
                    ef.get("error_category", ""),
                    ef.get("status_code", ""),
                    ef.get("path", ""),
                )

        unrealized_krw = 0.0
        unrealized_pct = 0.0
        if avg_price > 0 and last_price > 0 and qty > 0:
            unrealized_krw = (last_price - avg_price) * qty
            unrealized_pct = (last_price / avg_price) - 1.0

        pos_rows.append(
            {
                "code": code,
                "name": str(r.get("prdt_name", "") or r.get("name", "")),
                "qty": int(qty),
                "avg_price": float(avg_price),
                "last_price": float(last_price),
                "unrealized_krw": float(unrealized_krw),
                "unrealized_pct": float(unrealized_pct),
                "notional_krw": float(last_price * qty if last_price > 0 else avg_price * qty),
            }
        )

    df = pd.DataFrame(pos_rows)
    if len(df):
        df = df.sort_values(["notional_krw", "code"], ascending=[False, True]).reset_index(drop=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    total_notional = float(df["notional_krw"].sum()) if len(df) else 0.0
    total_unreal = float(df["unrealized_krw"].sum()) if len(df) else 0.0

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "mode": mode,
        "with_quotes": bool(args.with_quotes),
        "positions": int(len(df)),
        "quote_fail_count": int(quote_fail_count),
        "total_notional_krw": total_notional,
        "total_unrealized_krw": total_unreal,
        "paths": {"positions_csv": str(out_csv), "snapshot_json": str(out_json)},
    }

    payload = {"summary": summary, "positions": df.to_dict(orient="records")}
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("[OK] positions_csv=%s rows=%s", out_csv, len(df))
    logger.info("[OK] snapshot_json=%s", out_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
