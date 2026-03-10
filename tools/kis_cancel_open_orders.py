from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from kis_order_client import KISApiError, KISConfig, KISOrderClient


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


def _parse_hhmmss(v: object) -> Optional[tuple[int, int, int]]:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(s) >= 6:
        s = s[:6]
    elif len(s) == 4:
        s = f"{s}00"
    else:
        return None
    hh = _to_int(s[:2], -1)
    mm = _to_int(s[2:4], -1)
    ss = _to_int(s[4:6], -1)
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        return None
    return hh, mm, ss


def _order_age_minutes(now_kst: dt.datetime, d: str, ord_tmd: object) -> Optional[float]:
    if str(d) != now_kst.strftime("%Y%m%d"):
        return 10_000.0
    hms = _parse_hhmmss(ord_tmd)
    if hms is None:
        return None
    hh, mm, ss = hms
    ts = now_kst.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if ts > now_kst:
        return 0.0
    return (now_kst - ts).total_seconds() / 60.0


def _norm_side(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"02", "2", "BUY", "B", "매수"}:
        return "BUY"
    if s in {"01", "1", "SELL", "S", "매도"}:
        return "SELL"
    return ""


def _mode_label(args_mock: str, client: KISOrderClient) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if client.cfg.mock else "prod"


def _run_self_test() -> int:
    cfg = KISConfig(
        app_key="k",
        app_secret="s",
        cano="12345678",
        acnt_prdt_cd="01",
        mock=True,
    )
    client = KISOrderClient(cfg)

    sample_rows = [
        {
            "pdno": "005930",
            "odno": "1111",
            "ord_gno_brno": "00001",
            "ord_qty": "10",
            "tot_ccld_qty": "3",
            "ord_tmd": "091000",
            "sll_buy_dvsn_cd": "02",
        },
        {
            "pdno": "000660",
            "odno": "2222",
            "ord_gno_brno": "00001",
            "ord_qty": "5",
            "tot_ccld_qty": "5",
            "ord_tmd": "091500",
            "sll_buy_dvsn_cd": "01",
        },
    ]

    def _fake_inq(**_: object) -> Dict[str, object]:
        return {"ok": True, "tr_id": "VTTC0081R", "rows": sample_rows, "pages": 1}

    def _fake_auth(**_: object) -> Dict[str, str]:
        return {"tr_id": "VTTC0803U"}

    def _fake_req(*_: object, **__: object):
        return {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "3333", "KRX_FWDG_ORD_ORGNO": "00001"}}, {}

    client.inquire_daily_ccld = _fake_inq  # type: ignore[assignment]
    client._auth_headers = _fake_auth  # type: ignore[assignment]
    client._request_json = _fake_req  # type: ignore[assignment]

    open_rsp = client.inquire_open_orders(ymd="20260309")
    rows = open_rsp.get("rows", []) or []
    assert len(rows) == 1, f"expected 1 open row, got {len(rows)}"
    assert int(rows[0]["rmn_qty"]) == 7, f"expected rem=7, got {rows[0]['rmn_qty']}"

    c_rsp = client.cancel_order(org_order_no="1111", org_order_branch_no="00001", qty=7, cancel_all=False)
    assert bool(c_rsp.get("ok", False)), "cancel_order should return ok"
    assert c_rsp.get("payload", {}).get("RVSE_CNCL_DVSN_CD") == "02", "cancel payload mismatch"

    now = dt.datetime(2026, 3, 9, 10, 0, 0)
    age = _order_age_minutes(now, "20260309", "093000")
    assert age is not None and age >= 30, f"age calc failed: {age}"

    print("[OK] self-test passed")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Cancel stale open orders from KIS daily order status")
    ap.add_argument("--date", default="", help="YYYYMMDD (default=today)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--apply", action="store_true", help="Actually send cancel orders")
    ap.add_argument("--allow-non-today", action="store_true", help="Allow apply for non-today date")
    ap.add_argument("--min-age-minutes", type=int, default=10, help="Cancel only if order age >= this threshold")
    ap.add_argument("--max-cancels", type=int, default=0, help="0 means all")
    ap.add_argument("--sll-buy-dvsn-cd", default="00", help="00=all, 01=sell, 02=buy")
    ap.add_argument("--code", default="", help="Optional 6-digit code filter")
    ap.add_argument("--tr-id", default="", help="Optional override for cancel TR ID")
    ap.add_argument("--self-test", action="store_true", help="Run offline logic self-test")
    args = ap.parse_args()

    if args.self_test:
        return _run_self_test()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today

    if args.apply and (not args.allow_non_today) and d != today:
        print(f"[STOP] apply is blocked for non-today D={d} today={today}. Use --allow-non-today if intentional.")
        return 2

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
    out_csv = PAPER_DIR / f"orders_{d}_broker_cancel_{mode}.csv"
    out_json = LOG_DIR / f"kis_cancel_open_orders_{d}_{mode}.json"

    try:
        rsp = client.inquire_open_orders(
            ymd=d,
            sll_buy_dvsn_cd=str(args.sll_buy_dvsn_cd),
            pdno=str(args.code or "").strip(),
            max_pages=30,
        )
        open_rows = rsp.get("rows", []) or []
    except KISApiError as e:
        print(f"[STOP] KIS API error: {e}")
        return 2
    except Exception as e:
        print(f"[STOP] inquire_open_orders failed: {e}")
        return 2

    try:
        from zoneinfo import ZoneInfo

        now_kst = dt.datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now_kst = dt.datetime.utcnow() + dt.timedelta(hours=9)

    records: List[Dict[str, object]] = []

    for r in open_rows:
        rec = {
            "dispatch_ts": dt.datetime.now().isoformat(timespec="seconds"),
            "date": d,
            "code": str(r.get("code", "")).zfill(6),
            "side": _norm_side(r.get("side_raw", "")),
            "org_order_no": str(r.get("odno", "")),
            "org_order_branch_no": str(r.get("ord_gno_brno", "")),
            "ord_qty": _to_int(r.get("ord_qty", 0)),
            "filled_qty": _to_int(r.get("ccld_qty", 0)),
            "remain_qty": _to_int(r.get("rmn_qty", 0)),
            "ord_tmd": str(r.get("ord_tmd", "")),
            "age_min": None,
            "cancel_status": "",
            "ok": False,
            "rt_cd": "",
            "msg1": "",
            "cancel_ord_no": "",
            "cancel_org_no": "",
            "tr_id": "",
            "error": "",
            "apply": bool(args.apply),
            "mode": mode,
        }

        age_min = _order_age_minutes(now_kst, d, rec["ord_tmd"])
        rec["age_min"] = age_min
        if age_min is not None and float(age_min) < float(args.min_age_minutes):
            rec["cancel_status"] = "SKIP_YOUNG_ORDER"
            records.append(rec)
            continue

        if rec["remain_qty"] <= 0:
            rec["cancel_status"] = "SKIP_NO_REMAIN"
            records.append(rec)
            continue

        if not rec["org_order_no"]:
            rec["cancel_status"] = "SKIP_NO_ORDER_NO"
            records.append(rec)
            continue

        if not rec["org_order_branch_no"]:
            rec["cancel_status"] = "SKIP_NO_BRANCH_NO"
            records.append(rec)
            continue

        if not args.apply:
            rec["cancel_status"] = "DRY_RUN"
            rec["ok"] = True
            rec["msg1"] = "dry-run only"
            records.append(rec)
            continue

        try:
            c_rsp = client.cancel_order(
                org_order_no=str(rec["org_order_no"]),
                org_order_branch_no=str(rec["org_order_branch_no"]),
                qty=int(rec["remain_qty"]),
                cancel_all=True,
                tr_id=str(args.tr_id or ""),
            )
            rec["ok"] = bool(c_rsp.get("ok", False))
            rec["rt_cd"] = str(c_rsp.get("rt_cd", ""))
            rec["msg1"] = str(c_rsp.get("msg1", ""))
            rec["cancel_ord_no"] = str(c_rsp.get("ord_no", ""))
            rec["cancel_org_no"] = str(c_rsp.get("org_no", ""))
            rec["tr_id"] = str(c_rsp.get("tr_id", ""))
            rec["cancel_status"] = "CANCEL_ACCEPTED" if rec["ok"] else "CANCEL_REJECTED"
            records.append(rec)
        except KISApiError as e:
            rec["cancel_status"] = "ERROR"
            rec["error"] = str(e)
            records.append(rec)
        except Exception as e:
            rec["cancel_status"] = "ERROR"
            rec["error"] = f"unexpected: {e}"
            records.append(rec)

        if args.max_cancels > 0:
            done_cnt = len([x for x in records if str(x.get("cancel_status", "")).startswith("CANCEL_")])
            if done_cnt >= int(args.max_cancels):
                break

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
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "today": today,
        "apply": bool(args.apply),
        "mode": mode,
        "min_age_minutes": int(args.min_age_minutes),
        "max_cancels": int(args.max_cancels),
        "sll_buy_dvsn_cd": str(args.sll_buy_dvsn_cd),
        "code": str(args.code or ""),
        "rows_open": int(len(open_rows)),
        "rows_new": int(len(new_df)),
        "counts": new_df.get("cancel_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "paths": {
            "cancel_log_csv": str(out_csv),
            "summary_json": str(out_json),
        },
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] cancel_log={out_csv} rows_new={len(new_df)}")
    print(f"[OK] summary={out_json}")

    if args.apply and len(new_df):
        fail_n = int(new_df["cancel_status"].isin(["CANCEL_REJECTED", "ERROR"]).sum())
        if fail_n > 0:
            print(f"[STOP] apply mode had failed cancels: {fail_n}")
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

