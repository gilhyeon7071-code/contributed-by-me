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
LOG_DIR = ROOT / "2_Logs"


def _parse_codes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip()
        if t:
            vals.append(t.zfill(6))
    out: List[str] = []
    for c in vals:
        if c not in out:
            out.append(c)
    return out


def _mode_label(args_mock: str, client: KISOrderClient) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if client.cfg.mock else "prod"


def main() -> int:
    ap = argparse.ArgumentParser(description="KIS soak test automation")
    ap.add_argument("--codes", default="005930", help="Comma-separated symbols")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--duration-hours", type=float, default=24.0)
    ap.add_argument("--interval-sec", type=float, default=30.0)
    ap.add_argument("--check-balance-every", type=int, default=20, help="0 disables")
    ap.add_argument("--fail-ratio-max", type=float, default=0.05)
    ap.add_argument("--max-consecutive-fail", type=int, default=5)
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--notify-every-min", type=int, default=30)
    args = ap.parse_args()

    codes = _parse_codes(args.codes)
    if not codes:
        print("[STOP] no valid codes")
        return 2

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ymd = dt.datetime.now().strftime("%Y%m%d")
    ts0 = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = LOG_DIR / f"kis_soak_events_{ts0}.csv"
    out_json = LOG_DIR / f"kis_soak_summary_{ts0}.json"
    latest_json = LOG_DIR / "kis_soak_latest.json"

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        print(f"[STOP] KIS env/config failed: {e}")
        return 2

    mode = _mode_label(args.mock, client)
    started = time.time()
    end_at = started + max(1.0, float(args.duration_hours) * 3600.0)

    events: List[Dict[str, object]] = []
    ok_n = 0
    fail_n = 0
    consec_fail = 0
    iter_n = 0
    last_notify_ts = 0.0

    if args.notify:
        send_alert(f"[SOAK] start mode={mode} duration_h={args.duration_hours} codes={','.join(codes)}", level="info")

    while time.time() < end_at:
        iter_n += 1
        loop_ts = dt.datetime.now().isoformat(timespec="seconds")
        loop_ok = True
        err_msgs: List[str] = []

        try:
            _ = client._ensure_token()  # noqa: SLF001
        except Exception as e:
            loop_ok = False
            err_msgs.append(f"token:{e}")

        for c in codes:
            try:
                q = client.inquire_price(code=c)
                out = q.get("output", {}) or {}
                if str(out.get("stck_prpr", "")).strip() == "":
                    loop_ok = False
                    err_msgs.append(f"price_empty:{c}")
            except Exception as e:
                loop_ok = False
                err_msgs.append(f"price:{c}:{e}")

        if int(args.check_balance_every) > 0 and (iter_n % int(args.check_balance_every) == 0):
            try:
                b = client.inquire_balance_positions(max_pages=3)
                _ = len(b.get("rows", []) or [])
            except Exception as e:
                loop_ok = False
                err_msgs.append(f"balance:{e}")

        if loop_ok:
            ok_n += 1
            consec_fail = 0
            status = "OK"
        else:
            fail_n += 1
            consec_fail += 1
            status = "FAIL"

        rec = {
            "ts": loop_ts,
            "iter": iter_n,
            "status": status,
            "errors": " | ".join(err_msgs),
            "ok_n": ok_n,
            "fail_n": fail_n,
            "consecutive_fail": consec_fail,
            "mode": mode,
            "codes": ",".join(codes),
        }
        events.append(rec)

        elapsed = time.time() - started
        total = ok_n + fail_n
        fail_ratio = (fail_n / total) if total > 0 else 0.0
        snap = {
            "ts": loop_ts,
            "mode": mode,
            "iter": iter_n,
            "ok_n": ok_n,
            "fail_n": fail_n,
            "fail_ratio": fail_ratio,
            "consecutive_fail": consec_fail,
            "elapsed_sec": elapsed,
            "remaining_sec": max(0.0, end_at - time.time()),
            "status": "RUNNING",
            "last_status": status,
        }
        latest_json.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")

        now = time.time()
        if args.notify and int(args.notify_every_min) > 0:
            if (now - last_notify_ts) >= int(args.notify_every_min) * 60:
                last_notify_ts = now
                send_alert(
                    f"[SOAK] heartbeat mode={mode} iter={iter_n} ok={ok_n} fail={fail_n} fail_ratio={fail_ratio:.3f}",
                    level="info",
                    extra={"consecutive_fail": consec_fail},
                )

        if consec_fail >= int(args.max_consecutive_fail):
            break

        time.sleep(max(1.0, float(args.interval_sec)))

    df = pd.DataFrame(events)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    total = ok_n + fail_n
    fail_ratio = (fail_n / total) if total > 0 else 0.0
    ok_final = (fail_ratio <= float(args.fail_ratio_max)) and (consec_fail < int(args.max_consecutive_fail))

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "mode": mode,
        "codes": codes,
        "duration_hours": float(args.duration_hours),
        "interval_sec": float(args.interval_sec),
        "iterations": int(iter_n),
        "ok_n": int(ok_n),
        "fail_n": int(fail_n),
        "fail_ratio": fail_ratio,
        "fail_ratio_max": float(args.fail_ratio_max),
        "consecutive_fail": int(consec_fail),
        "max_consecutive_fail": int(args.max_consecutive_fail),
        "ok": bool(ok_final),
        "paths": {"events_csv": str(out_csv), "summary_json": str(out_json), "latest_json": str(latest_json)},
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    latest = dict(summary)
    latest["status"] = "DONE"
    latest_json.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] events_csv={out_csv} rows={len(df)}")
    print(f"[OK] summary={out_json} ok={ok_final}")

    if args.notify:
        lv = "info" if ok_final else "error"
        send_alert(
            f"[SOAK] done mode={mode} ok={ok_final} iter={iter_n} fail_ratio={fail_ratio:.3f}",
            level=lv,
            extra={"summary_json": str(out_json)},
        )

    return 0 if ok_final else 2


if __name__ == "__main__":
    raise SystemExit(main())
