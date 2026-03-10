from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Optional

from kis_order_client import KISApiError, KISOrderClient
from notify_channels import send_alert


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _env_true(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in {"1", "true", "y", "yes"}


def main() -> int:
    ap = argparse.ArgumentParser(description="KIS API healthcheck (token/quote/balance/open-orders)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--code", default="005930", help="6-digit stock code for quote check")
    ap.add_argument("--date", default="", help="YYYYMMDD for open-order check (default=today)")
    ap.add_argument("--check-balance", action="store_true")
    ap.add_argument("--check-open-orders", action="store_true")
    ap.add_argument("--notify-on-fail", action="store_true")
    args = ap.parse_args()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    strict_optional = _env_true("KIS_HEALTHCHECK_STRICT_OPTIONAL", "0")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_json = LOG_DIR / f"kis_healthcheck_{today}.json"

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "today": today,
        "date": d,
        "args": {
            "mock": args.mock,
            "code": args.code,
            "check_balance": bool(args.check_balance),
            "check_open_orders": bool(args.check_open_orders),
            "notify_on_fail": bool(args.notify_on_fail),
            "strict_optional": bool(strict_optional),
        },
        "checks": {},
        "warnings": [],
        "ok": False,
        "error": "",
    }

    try:
        client = KISOrderClient.from_env(mock=mock_opt)

        token = client._ensure_token()  # noqa: SLF001
        payload["checks"]["token"] = {
            "ok": bool(token),
            "token_len": len(token),
            "mock": bool(client.cfg.mock),
            "base_url": client.cfg.base_url,
        }

        quote = client.inquire_price(code=str(args.code))
        out = quote.get("output", {}) or {}
        payload["checks"]["quote"] = {
            "ok": True,
            "code": str(args.code).zfill(6),
            "stck_prpr": str(out.get("stck_prpr", "")),
            "acml_vol": str(out.get("acml_vol", "")),
            "askp1": str(out.get("askp1", "")),
            "bidp1": str(out.get("bidp1", "")),
        }

        optional_fail_n = 0

        if args.check_balance:
            try:
                bal = client.inquire_balance_positions(max_pages=3)
                payload["checks"]["balance"] = {
                    "ok": True,
                    "rows": int(len(bal.get("rows", []) or [])),
                    "pages": int(bal.get("pages", 0) or 0),
                }
            except Exception as e:
                optional_fail_n += 1
                payload["checks"]["balance"] = {"ok": False, "error": str(e)}
                payload["warnings"].append(f"balance_check_failed: {e}")

        if args.check_open_orders:
            try:
                oo = client.inquire_open_orders(ymd=d)
                payload["checks"]["open_orders"] = {
                    "ok": True,
                    "rows": int(len(oo.get("rows", []) or [])),
                    "pages": int(oo.get("pages", 0) or 0),
                }
            except Exception as e:
                optional_fail_n += 1
                payload["checks"]["open_orders"] = {"ok": False, "error": str(e)}
                payload["warnings"].append(f"open_orders_check_failed: {e}")

        payload["ok"] = not (strict_optional and optional_fail_n > 0)
        if not payload["ok"] and optional_fail_n > 0:
            payload["error"] = f"optional_checks_failed={optional_fail_n} (strict mode)"

    except KISApiError as e:
        payload["error"] = str(e)
        payload["ok"] = False
    except Exception as e:
        payload["error"] = str(e)
        payload["ok"] = False

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[CHECK] ok={payload['ok']} json={out_json}")

    if payload.get("warnings"):
        print(f"[CHECK] warnings={len(payload['warnings'])}")

    if not payload["ok"]:
        print(f"[CHECK] error={payload['error']}")
        if args.notify_on_fail:
            send_alert(
                f"[HEALTHCHECK] FAIL code={args.code} mock={args.mock} error={payload['error']}",
                level="error",
                extra={"json": str(out_json)},
            )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
