from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from kis_order_client import KISApiError, KISOrderClient
from notify_channels import send_alert


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _extract_first_int(d: Dict[str, object], keys: List[str], default: int = -1) -> int:
    for k in keys:
        if k in d:
            v = _to_int(d.get(k), default=-1)
            if v >= 0:
                return v
    return int(default)


def _map_sellable_qty(balance_rows: List[Dict[str, object]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in balance_rows:
        code = str(r.get("pdno", "") or r.get("code", "") or "").strip().zfill(6)
        if not code.isdigit() or len(code) != 6:
            continue
        qty = _extract_first_int(
            r,
            [
                "ord_psbl_qty",
                "ord_psbl_qty1",
                "ord_psbl_qty_1",
                "sell_psbl_qty",
                "hldg_qty",
                "hold_qty",
                "qty",
            ],
            default=-1,
        )
        if qty >= 0:
            out[code] = max(out.get(code, 0), int(qty))
    return out


def _mode_label(args_mock: str, client: KISOrderClient) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if client.cfg.mock else "prod"


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _cancel_open_orders(client: KISOrderClient, d: str, dry: bool) -> Dict[str, object]:
    try:
        rsp = client.inquire_open_orders(ymd=d)
        rows = rsp.get("rows", []) or []
    except Exception as e:
        return {"ok": False, "error": str(e), "canceled": 0, "open_rows": 0}

    canceled = 0
    for r in rows:
        odno = str(r.get("odno", "")).strip()
        br = str(r.get("ord_gno_brno", "")).strip()
        rem = _to_int(r.get("rmn_qty", 0), 0)
        if not odno or not br or rem <= 0:
            continue
        if dry:
            canceled += 1
            continue
        try:
            cr = client.cancel_order(org_order_no=odno, org_order_branch_no=br, qty=rem, cancel_all=True)
            if bool(cr.get("ok", False)):
                canceled += 1
        except Exception:
            continue

    return {"ok": True, "open_rows": int(len(rows)), "canceled": int(canceled)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Emergency full liquidation (market sell all sellable positions)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--apply", action="store_true", help="Actually place sell orders")
    ap.add_argument("--allow-offhours", action="store_true", help="Allow applying outside regular session")
    ap.add_argument("--max-orders", type=int, default=0, help="0 means all")
    ap.add_argument("--sleep-ms", type=int, default=120)
    ap.add_argument("--cancel-open-first", action="store_true")
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--reason", default="EMERGENCY")
    ap.add_argument("--date", default="")
    args = ap.parse_args()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        print(f"[STOP] KIS env/config failed: {e}")
        return 2

    mode = _mode_label(args.mock, client)
    now_ts = dt.datetime.now().isoformat(timespec="seconds")
    out_csv = PAPER_DIR / f"orders_{d}_emergency_liq_{mode}.csv"
    out_json = LOG_DIR / f"kis_emergency_liq_{d}_{mode}.json"

    summary: Dict[str, object] = {
        "generated_at": now_ts,
        "date": d,
        "mode": mode,
        "apply": bool(args.apply),
        "reason": str(args.reason),
        "cancel_open_first": bool(args.cancel_open_first),
        "cancel_open": None,
        "rows_positions": 0,
        "rows_orders": 0,
        "counts": {},
        "ok": False,
        "error": "",
    }

    if args.notify:
        send_alert(
            f"[EMERGENCY] liquidation start mode={mode} apply={bool(args.apply)} reason={args.reason}",
            level="warning",
            extra={"date": d},
        )

    if args.cancel_open_first:
        c = _cancel_open_orders(client, d=d, dry=(not args.apply))
        summary["cancel_open"] = c
        if not bool(c.get("ok", False)) and args.apply:
            summary["error"] = f"cancel_open_first failed: {c.get('error')}"
            out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[STOP] {summary['error']}")
            return 2

    try:
        bal = client.inquire_balance_positions(max_pages=10)
        rows = bal.get("rows", []) or []
        sellable = _map_sellable_qty(rows)
    except Exception as e:
        summary["error"] = f"balance inquiry failed: {e}"
        out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[STOP] {summary['error']}")
        return 2

    items = [{"code": c, "qty": int(q)} for c, q in sellable.items() if int(q) > 0]
    items = sorted(items, key=lambda x: x["code"])
    if args.max_orders > 0:
        items = items[: int(args.max_orders)]

    summary["rows_positions"] = int(len(sellable))

    records: List[Dict[str, object]] = []
    for it in items:
        rec = {
            "ts": dt.datetime.now().isoformat(timespec="seconds"),
            "code": str(it["code"]),
            "qty": int(it["qty"]),
            "side": "SELL",
            "order_type": "market",
            "status": "",
            "ok": False,
            "rt_cd": "",
            "msg1": "",
            "ord_no": "",
            "org_no": "",
            "tr_id": "",
            "error": "",
            "apply": bool(args.apply),
            "mode": mode,
            "reason": str(args.reason),
        }

        if not args.apply:
            rec["status"] = "DRY_RUN"
            rec["ok"] = True
            rec["msg1"] = "dry-run only"
            records.append(rec)
            continue

        try:
            rsp = client.place_order_cash(side="SELL", code=str(rec["code"]), qty=int(rec["qty"]), order_type="market", price=0)
            rec["ok"] = bool(rsp.get("ok", False))
            rec["rt_cd"] = str(rsp.get("rt_cd", ""))
            rec["msg1"] = str(rsp.get("msg1", ""))
            rec["ord_no"] = str(rsp.get("ord_no", ""))
            rec["org_no"] = str(rsp.get("org_no", ""))
            rec["tr_id"] = str(rsp.get("tr_id", ""))
            rec["status"] = "ACCEPTED" if rec["ok"] else "REJECTED"
        except KISApiError as e:
            rec["status"] = "ERROR"
            rec["error"] = str(e)
        except Exception as e:
            rec["status"] = "ERROR"
            rec["error"] = f"unexpected: {e}"

        records.append(rec)
        if args.sleep_ms > 0:
            time.sleep(float(args.sleep_ms) / 1000.0)

    df = pd.DataFrame(records)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    summary["rows_orders"] = int(len(df))
    summary["counts"] = df.get("status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict() if len(df) else {}
    fail_n = int(df["status"].isin(["REJECTED", "ERROR"]).sum()) if len(df) else 0
    summary["ok"] = fail_n == 0
    if fail_n > 0:
        summary["error"] = f"failed orders={fail_n}"

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] out_csv={out_csv} rows={len(df)}")
    print(f"[OK] out_json={out_json}")

    if args.notify:
        lv = "info" if bool(summary["ok"]) else "error"
        send_alert(
            f"[EMERGENCY] done mode={mode} apply={bool(args.apply)} ok={bool(summary['ok'])} rows={len(df)} fail={fail_n}",
            level=lv,
            extra={"summary_json": str(out_json), "reason": str(args.reason)},
        )

    if args.apply and fail_n > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
