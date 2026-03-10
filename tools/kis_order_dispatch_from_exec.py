from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from kis_order_client import KISApiError, KISOrderClient


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
DEFAULT_HOLIDAYS_PATH = ROOT / "holidays.json"


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _to_bool(v: object) -> bool:
    s = str(v or "").strip().lower()
    return s in {"1", "true", "y", "yes", "t"}



def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _parse_prefixes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip().upper()
        if t:
            vals.append(t)
    return vals


def _detect_latest_orders_path() -> Optional[Path]:
    cand = sorted(PAPER_DIR.glob("orders_*_exec.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cand[0] if cand else None


def _load_orders(
    path: Path,
    d: str,
    *,
    include_blocked: bool = False,
    allow_block_prefixes: Optional[List[str]] = None,
) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)

    need = ["exec_date", "side", "code", "fill_qty"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"orders_exec missing cols={miss} path={path}")

    out = df.copy()
    out["_source_row"] = out.index.astype(int)
    out["exec_date"] = out["exec_date"].apply(_norm_ymd)
    out["side"] = out["side"].astype(str).str.strip().str.upper()
    out["code"] = out["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["qty"] = out["fill_qty"].apply(_to_int)
    if "fill_price" in out.columns:
        out["price"] = out["fill_price"].apply(_to_int)
    else:
        out["price"] = 0
    if "signal_date" in out.columns:
        out["signal_date"] = out["signal_date"].apply(_norm_ymd)
    else:
        out["signal_date"] = ""
    if "is_stop" in out.columns:
        out["is_stop"] = out["is_stop"].apply(_to_bool)
    else:
        out["is_stop"] = False
    if "entry_blocked" in out.columns:
        out["entry_blocked"] = out["entry_blocked"].apply(_to_bool)
    else:
        out["entry_blocked"] = False
    if "entry_block_reason" in out.columns:
        out["entry_block_reason"] = out["entry_block_reason"].astype(str).fillna("")
    else:
        out["entry_block_reason"] = ""
    if "note" in out.columns:
        out["note"] = out["note"].astype(str)
    else:
        out["note"] = ""

    base_mask = (
        (out["exec_date"] == str(d))
        & (out["side"].isin(["BUY", "SELL"]))
        & (out["qty"] > 0)
    )

    if include_blocked:
        prefixes = [p for p in (allow_block_prefixes or []) if str(p).strip()]
        if len(prefixes) == 0:
            allow_mask = pd.Series([True] * len(out), index=out.index)
        else:
            up = out["entry_block_reason"].astype(str).str.upper()
            allow_blocked = out["entry_blocked"] & up.apply(lambda s: any(str(s).startswith(p) for p in prefixes))
            allow_mask = (~out["entry_blocked"]) | allow_blocked
        keep = out[base_mask & allow_mask].copy()
    else:
        keep = out[base_mask & (~out["entry_blocked"])].copy()

    if len(keep) == 0:
        return keep

    keep = keep.reset_index(drop=True)
    return keep

def _dispatch_key(rec: Dict[str, object]) -> str:
    return "|".join(
        [
            str(rec.get("exec_date", "")),
            str(rec.get("side", "")),
            str(rec.get("code", "")),
            str(rec.get("qty", "")),
            str(rec.get("price", "")),
            str(rec.get("signal_date", "")),
            str(rec.get("source_row", "")),
        ]
    )


def _load_done_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        old = pd.read_csv(path, dtype=str)
    except Exception:
        return set()

    if len(old) == 0:
        return set()

    ok_mask = old.get("dispatch_status", "").astype(str).isin(["ACCEPTED"])
    keys = set()
    for _, r in old[ok_mask].iterrows():
        keys.add(
            _dispatch_key(
                {
                    "exec_date": r.get("exec_date", ""),
                    "side": r.get("side", ""),
                    "code": r.get("code", ""),
                    "qty": r.get("qty", ""),
                    "price": r.get("price", ""),
                    "signal_date": r.get("signal_date", ""),
                    "source_row": r.get("source_row", ""),
                }
            )
        )
    return keys


def _env_mock_bool() -> bool:
    return str(os.getenv("KIS_MOCK", "0")).strip().lower() in {"1", "true", "y", "yes"}


def _mode_label(args_mock: str) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if _env_mock_bool() else "prod"


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


def _buy_possible_qty(client: KISOrderClient, code: str, order_type: str, price: int) -> int:
    ord_dvsn = "01" if str(order_type).lower() == "market" else "00"
    rsp = client.inquire_psbl_order(code=code, order_price=max(0, int(price)), ord_dvsn=ord_dvsn)
    output = rsp.get("output", {}) or {}
    qty = _extract_first_int(
        output,
        [
            "nrcvb_buy_qty",
            "max_buy_qty",
            "ord_psbl_qty",
            "psbl_qty_calc",
            "buy_psbl_qty",
        ],
        default=-1,
    )
    return int(qty)


def _mask_account(cano: str, prdt: str) -> str:
    c = str(cano or "")
    p = str(prdt or "")
    if len(c) >= 4:
        c = ("*" * (len(c) - 4)) + c[-4:]
    return f"{c}-{p}"


def _load_holidays_ymd(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    vals: List[str] = []
    if isinstance(obj, dict):
        for k in ["holidays", "krx_holidays", "dates", "holiday_dates"]:
            v = obj.get(k)
            if isinstance(v, list):
                vals.extend([str(x) for x in v])
    elif isinstance(obj, list):
        vals.extend([str(x) for x in obj])

    out = set()
    for x in vals:
        y = _norm_ymd(x)
        if len(y) == 8:
            out.add(y)
    return out


def _is_krx_session_open(holidays_ymd: set[str]) -> tuple[bool, str]:
    try:
        from zoneinfo import ZoneInfo

        now = dt.datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now = dt.datetime.utcnow() + dt.timedelta(hours=9)

    ymd = now.strftime("%Y%m%d")
    w = now.weekday()  # 0=Mon
    if w >= 5:
        return False, f"weekend weekday={w}"
    if ymd in holidays_ymd:
        return False, f"holiday ymd={ymd}"

    hm = now.hour * 100 + now.minute
    if hm < 900 or hm > 1530:
        return False, f"off-hours hm={hm}"

    return True, f"open ymd={ymd} hm={hm}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Dispatch KIS live orders from orders_{D}_exec.xlsx")
    ap.add_argument("--date", default="", help="YYYYMMDD. empty => infer from latest orders file name")
    ap.add_argument("--orders-path", default="", help="Path to orders_{D}_exec.xlsx")
    ap.add_argument("--apply", action="store_true", help="Actually send orders. default is dry-run")
    ap.add_argument("--order-type", default="market", choices=["market", "limit"])
    ap.add_argument("--max-orders", type=int, default=0, help="0 means all")
    ap.add_argument("--sleep-ms", type=int, default=120)
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--force-resend", action="store_true", help="Ignore existing submit log idempotency")
    ap.add_argument("--allow-non-today", action="store_true", help="Allow apply for non-today date")
    ap.add_argument("--allow-offhours", action="store_true", help="Allow apply outside KRX regular session")
    ap.add_argument("--holidays-file", default=str(DEFAULT_HOLIDAYS_PATH))
    ap.add_argument("--strict-precheck", dest="strict_precheck", action="store_true", default=True)
    ap.add_argument("--no-strict-precheck", dest="strict_precheck", action="store_false")
    ap.add_argument("--validation-mode", action="store_true", help="Validation mode: allow selected blocked rows for sample collection")
    ap.add_argument("--allow-block-prefixes", default="CAP_", help="Comma-separated entry_block_reason prefixes allowed in validation mode")
    ap.add_argument("--min-live-orders", type=int, default=0, help="If validation mode and eligible rows below this value, fallback include all blocked rows")
    args = ap.parse_args()

    if args.validation_mode and args.strict_precheck:
        print("[INFO] validation_mode: strict_precheck -> False (sample collection priority)")
        args.strict_precheck = False
    if args.validation_mode and int(args.sleep_ms) < 350:
        print(f"[INFO] validation_mode: sleep_ms {args.sleep_ms} -> 350")
        args.sleep_ms = 350


    d = _norm_ymd(args.date)
    orders_path = Path(args.orders_path) if args.orders_path else None
    if orders_path is None:
        orders_path = _detect_latest_orders_path()
        if orders_path is None:
            print("[STOP] no orders_*_exec.xlsx found")
            return 2

    if not orders_path.exists():
        print(f"[STOP] orders file not found: {orders_path}")
        return 2

    if not d:
        stem_digits = "".join(ch for ch in orders_path.stem if ch.isdigit())
        d = stem_digits[:8] if len(stem_digits) >= 8 else ""
    if not d:
        print("[STOP] failed to determine date D")
        return 2

    today_ymd = dt.datetime.now().strftime("%Y%m%d")
    if args.apply and (not args.allow_non_today) and d != today_ymd:
        print(f"[STOP] apply is blocked for non-today D={d} today={today_ymd}. Use --allow-non-today if intentional.")
        return 2

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        prefixes = _parse_prefixes(args.allow_block_prefixes)
        orders = _load_orders(
            orders_path,
            d,
            include_blocked=bool(args.validation_mode),
            allow_block_prefixes=prefixes,
        )
    except Exception as e:
        print(f"[STOP] load_orders failed: {e}")
        return 2

    if args.max_orders and args.max_orders > 0:
        orders = orders.head(int(args.max_orders)).copy()

    if args.validation_mode and args.min_live_orders > 0 and len(orders) < int(args.min_live_orders):
        try:
            orders_fallback = _load_orders(
                orders_path,
                d,
                include_blocked=True,
                allow_block_prefixes=[],
            )
            if args.max_orders and args.max_orders > 0:
                orders_fallback = orders_fallback.head(int(args.max_orders)).copy()
            if len(orders_fallback) > len(orders):
                print(
                    f"[WARN] validation fallback enabled: eligible {len(orders)} -> {len(orders_fallback)} "
                    f"(min_live_orders={args.min_live_orders})"
                )
                orders = orders_fallback
        except Exception as e:
            print(f"[WARN] validation fallback failed: {e}")

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = True if args.validation_mode else None
    else:
        mock_opt = args.mock == "true"

    client: Optional[KISOrderClient] = None
    if args.apply:
        try:
            client = KISOrderClient.from_env(mock=mock_opt)
        except Exception as e:
            print(f"[STOP] KIS env/config failed: {e}")
            return 2

    run_mode = _mode_label(args.mock)
    if client is not None:
        run_mode = "mock" if client.cfg.mock else "prod"

    out_csv = PAPER_DIR / f"orders_{d}_broker_submit_{run_mode}.csv"
    out_json = LOG_DIR / f"kis_order_dispatch_{d}_{run_mode}.json"

    print(f"[INFO] D={d} today={today_ymd} orders_path={orders_path}")
    print(f"[INFO] eligible_rows={len(orders)} apply={bool(args.apply)} order_type={args.order_type} mode={run_mode}")
    if args.validation_mode:
        print(
            f"[INFO] validation_mode=ON allow_block_prefixes={_parse_prefixes(args.allow_block_prefixes)} "
            f"min_live_orders={args.min_live_orders}"
        )
    if client is not None:
        print(f"[INFO] broker=KIS mock={client.cfg.mock} account={_mask_account(client.cfg.cano, client.cfg.acnt_prdt_cd)}")

    if args.apply and (not args.allow_offhours):
        holidays = _load_holidays_ymd(Path(args.holidays_file))
        ok_sess, msg_sess = _is_krx_session_open(holidays)
        if not ok_sess:
            print(f"[STOP] session guard blocked apply: {msg_sess}. Use --allow-offhours if intentional.")
            return 2
        print(f"[INFO] session guard pass: {msg_sess}")

    done_keys = set() if args.force_resend else _load_done_keys(out_csv)
    now_ts = dt.datetime.now().isoformat(timespec="seconds")

    records: List[Dict[str, object]] = []

    conflict_codes = set()
    if len(orders):
        side_map = orders.groupby("code")["side"].apply(lambda s: set(s.astype(str).tolist())).to_dict()
        conflict_codes = {c for c, s in side_map.items() if len(s) > 1}

    sellable_by_code: Dict[str, int] = {}
    reserved_sell_by_code: Dict[str, int] = {}
    if args.apply and client is not None:
        need_sell_check = bool((orders["side"] == "SELL").any()) if len(orders) else False
        if need_sell_check:
            try:
                bal = client.inquire_balance_positions(max_pages=10)
                sellable_by_code = _map_sellable_qty(bal.get("rows", []) or [])
            except Exception as e:
                if args.strict_precheck:
                    print(f"[STOP] precheck(balance) failed: {e}")
                    return 2
                print(f"[WARN] precheck(balance) skipped due to error: {e}")

    seen_batch: set[tuple[str, str]] = set()

    for _, row in orders.iterrows():
        rec = {
            "dispatch_ts": now_ts,
            "exec_date": str(row.get("exec_date", "")),
            "side": str(row.get("side", "")),
            "code": str(row.get("code", "")).zfill(6),
            "qty": int(row.get("qty", 0)),
            "price": int(row.get("price", 0)),
            "signal_date": str(row.get("signal_date", "")),
            "is_stop": bool(row.get("is_stop", False)),
            "note": str(row.get("note", "")),
            "entry_blocked": bool(row.get("entry_blocked", False)),
            "entry_block_reason": str(row.get("entry_block_reason", "")),
            "source_row": int(row.get("_source_row", -1)),
            "dispatch_status": "",
            "ok": False,
            "rt_cd": "",
            "msg1": "",
            "ord_no": "",
            "org_no": "",
            "tr_id": "",
            "error": "",
            "apply": bool(args.apply),
            "order_type": str(args.order_type),
            "mode": run_mode,
            "precheck": "SKIP" if not args.apply else "PASS",
            "precheck_msg": "",
        }

        k = _dispatch_key(rec)
        if k in done_keys:
            rec["dispatch_status"] = "SKIP_ALREADY_DISPATCHED"
            records.append(rec)
            continue

        key_side = (rec["code"], rec["side"])
        if key_side in seen_batch:
            rec["dispatch_status"] = "PRECHECK_DUPLICATE_IN_BATCH"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = "duplicate code+side in same batch"
            records.append(rec)
            continue
        seen_batch.add(key_side)

        if rec["code"] in conflict_codes:
            rec["dispatch_status"] = "PRECHECK_CONFLICT_SIDE"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = "both BUY and SELL exist for same code in batch"
            records.append(rec)
            continue

        if not args.apply:
            rec["dispatch_status"] = "DRY_RUN"
            rec["ok"] = True
            rec["msg1"] = "dry-run only"
            records.append(rec)
            continue

        assert client is not None

        if rec["side"] == "SELL":
            available = sellable_by_code.get(rec["code"], -1)
            reserved = reserved_sell_by_code.get(rec["code"], 0)
            remain = (available - reserved) if available >= 0 else -1
            if remain >= 0 and int(rec["qty"]) > remain:
                rec["dispatch_status"] = "PRECHECK_REJECT_SELL_QTY"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = f"sell qty exceeds available remain={remain}"
                records.append(rec)
                continue
            if remain < 0 and args.strict_precheck:
                rec["dispatch_status"] = "PRECHECK_REJECT_SELL_UNKNOWN"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = "sellable qty unavailable"
                records.append(rec)
                continue
            if available >= 0:
                reserved_sell_by_code[rec["code"]] = reserved + int(rec["qty"])

        if rec["side"] == "BUY":
            try:
                psbl_qty = _buy_possible_qty(
                    client,
                    code=str(rec["code"]),
                    order_type=str(args.order_type),
                    price=int(rec["price"]),
                )
                if psbl_qty >= 0 and int(rec["qty"]) > psbl_qty:
                    rec["dispatch_status"] = "PRECHECK_REJECT_BUY_QTY"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = f"buy qty exceeds possible qty={psbl_qty}"
                    records.append(rec)
                    continue
                if psbl_qty < 0 and args.strict_precheck:
                    rec["dispatch_status"] = "PRECHECK_REJECT_BUY_UNKNOWN"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = "buy possible qty unavailable"
                    records.append(rec)
                    continue
                rec["precheck_msg"] = f"buy_possible_qty={psbl_qty}"
            except Exception as e:
                if args.strict_precheck:
                    rec["dispatch_status"] = "PRECHECK_ERROR_BUY_PSBL"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = str(e)
                    records.append(rec)
                    continue
                rec["precheck_msg"] = f"buy_precheck_warn={e}"

        try:
            rsp = client.place_order_cash(
                side=rec["side"],
                code=rec["code"],
                qty=int(rec["qty"]),
                price=int(rec["price"]),
                order_type=str(args.order_type),
                exchange="KRX",
            )
            rec["ok"] = bool(rsp.get("ok", False))
            rec["rt_cd"] = str(rsp.get("rt_cd", ""))
            rec["msg1"] = str(rsp.get("msg1", ""))
            rec["ord_no"] = str(rsp.get("ord_no", ""))
            rec["org_no"] = str(rsp.get("org_no", ""))
            rec["tr_id"] = str(rsp.get("tr_id", ""))
            rec["dispatch_status"] = "ACCEPTED" if rec["ok"] else "REJECTED"
            records.append(rec)
            if rec["ok"]:
                done_keys.add(k)
        except KISApiError as e:
            rec["dispatch_status"] = "ERROR"
            rec["error"] = str(e)
            records.append(rec)
        except Exception as e:
            rec["dispatch_status"] = "ERROR"
            rec["error"] = f"unexpected: {e}"
            records.append(rec)

        if args.sleep_ms > 0:
            time.sleep(float(args.sleep_ms) / 1000.0)

    new_df = pd.DataFrame(records)
    if out_csv.exists():
        try:
            old = pd.read_csv(out_csv, dtype=str)
            out_df = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            out_df = new_df
    else:
        out_df = new_df

    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": now_ts,
        "D": d,
        "today": today_ymd,
        "orders_path": str(orders_path),
        "submit_log": str(out_csv),
        "apply": bool(args.apply),
        "order_type": str(args.order_type),
        "mode": run_mode,
        "strict_precheck": bool(args.strict_precheck),
        "validation_mode": bool(args.validation_mode),
        "allow_block_prefixes": _parse_prefixes(args.allow_block_prefixes),
        "min_live_orders": int(args.min_live_orders),
        "rows_eligible": int(len(orders)),
        "rows_new": int(len(new_df)),
        "counts": new_df.get("dispatch_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
    }

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] submit_log={out_csv} rows_new={len(new_df)}")
    print(f"[OK] summary={out_json}")

    if args.apply:
        fail_status = {
            "REJECTED",
            "ERROR",
            "PRECHECK_DUPLICATE_IN_BATCH",
            "PRECHECK_CONFLICT_SIDE",
            "PRECHECK_REJECT_SELL_QTY",
            "PRECHECK_REJECT_SELL_UNKNOWN",
            "PRECHECK_REJECT_BUY_QTY",
            "PRECHECK_REJECT_BUY_UNKNOWN",
            "PRECHECK_ERROR_BUY_PSBL",
        }
        fail_n = int(new_df["dispatch_status"].isin(list(fail_status)).sum()) if len(new_df) else 0
        if fail_n > 0:
            print(f"[STOP] apply mode had failed orders: {fail_n}")
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

