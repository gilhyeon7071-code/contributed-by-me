from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from kis_order_client import KISApiError, KISOrderClient
from notify_channels import send_alert


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_healthcheck")


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _env_true(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in {"1", "true", "y", "yes"}


def _is_rate_limit_error(exc: Exception) -> bool:
    s = str(exc or "").lower()
    return ("egw00201" in s) or ("초당 거래건수" in s) or ("호출 제한" in s) or ("rate limit" in s)


def _run_optional_with_retry(fn, *, retries: int, sleep_sec: float):
    last_err: Optional[Exception] = None
    for i in range(max(1, int(retries) + 1)):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if (not _is_rate_limit_error(e)) or i >= int(retries):
                raise
            if float(sleep_sec) > 0:
                time.sleep(float(sleep_sec))
    if last_err is not None:
        raise last_err
    raise RuntimeError("optional check retry failed")


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="KIS API healthcheck (token/quote/balance/open-orders)")
    ap.add_argument("--mock", default="true", choices=["auto", "true", "false"])
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

    strict_optional = _env_true("KIS_HEALTHCHECK_STRICT_OPTIONAL", "1")
    optional_retry_n = max(0, int(float(os.getenv("KIS_HEALTHCHECK_OPTIONAL_RETRY", "3") or 3)))
    optional_retry_sleep = max(0.0, float(os.getenv("KIS_HEALTHCHECK_OPTIONAL_RETRY_SLEEP_SEC", "2.0") or 2.0))

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
            "optional_retry_n": int(optional_retry_n),
            "optional_retry_sleep_sec": float(optional_retry_sleep),
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
                bal = _run_optional_with_retry(
                    lambda: client.inquire_balance_positions(max_pages=3),
                    retries=optional_retry_n,
                    sleep_sec=optional_retry_sleep,
                )
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
                oo = _run_optional_with_retry(
                    lambda: client.inquire_open_orders(ymd=d),
                    retries=optional_retry_n,
                    sleep_sec=optional_retry_sleep,
                )
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
    logger.info("[CHECK] ok=%s json=%s", payload["ok"], out_json)

    if payload.get("warnings"):
        logger.warning("[CHECK] warnings=%s", len(payload["warnings"]))

    if not payload["ok"]:
        logger.error("[CHECK] error=%s", payload["error"])
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
