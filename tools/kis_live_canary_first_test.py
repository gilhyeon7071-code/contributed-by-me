from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


try:
    from notify_channels import send_alert
except Exception:  # pragma: no cover
    send_alert = None  # type: ignore


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _tail(text: str, n: int = 20) -> str:
    lines = str(text or "").splitlines()
    return "\n".join(lines[-n:]) if len(lines) > n else "\n".join(lines)


def _parse_ts(raw: object) -> Optional[dt.datetime]:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _check_virtual_gate(max_age_days: int = 7) -> Dict[str, Any]:
    latest = LOG_DIR / "kis_intraday_e2e_latest.json"
    if not latest.exists():
        return {
            "ok": False,
            "reason": "missing_virtual_e2e_latest",
            "path": str(latest),
            "iterations_done": 0,
            "age_days": None,
        }

    try:
        obj = json.loads(latest.read_text(encoding="utf-8"))
    except Exception as e:
        return {
            "ok": False,
            "reason": f"invalid_virtual_e2e_latest:{e}",
            "path": str(latest),
            "iterations_done": 0,
            "age_days": None,
        }

    e2e_ok = bool(obj.get("ok", False))
    iter_done = int(obj.get("iterations_done", 0) or 0)
    if iter_done <= 0 and isinstance(obj.get("runs"), list):
        iter_done = len(obj.get("runs") or [])

    gen_dt = _parse_ts(obj.get("generated_at"))
    age_days: Optional[float] = None
    fresh = False
    if gen_dt is not None:
        age_days = (dt.datetime.now() - gen_dt).total_seconds() / 86400.0
        fresh = age_days <= float(max(1, int(max_age_days)))

    gate_ok = bool(e2e_ok and iter_done > 0 and fresh)
    reason = "ok"
    if not e2e_ok:
        reason = "virtual_e2e_not_ok"
    elif iter_done <= 0:
        reason = "virtual_e2e_no_iterations"
    elif not fresh:
        reason = "virtual_e2e_stale_or_no_timestamp"

    return {
        "ok": gate_ok,
        "reason": reason,
        "path": str(latest),
        "iterations_done": int(iter_done),
        "age_days": age_days,
        "e2e_ok": bool(e2e_ok),
        "max_age_days": int(max(1, int(max_age_days))),
    }


def _run(cmd: List[str], timeout_sec: float = 180.0) -> Dict[str, Any]:
    started = time.time()
    try:
        cp = subprocess.run(
            cmd,
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=max(1.0, float(timeout_sec)),
            check=False,
        )
        return {
            "ok": cp.returncode == 0,
            "returncode": int(cp.returncode),
            "stdout_tail": _tail(cp.stdout, 30),
            "stderr_tail": _tail(cp.stderr, 30),
            "duration_sec": round(time.time() - started, 3),
            "cmd": cmd,
        }
    except subprocess.TimeoutExpired as e:
        return {
            "ok": False,
            "returncode": 124,
            "stdout_tail": _tail(str(e.stdout or ""), 30),
            "stderr_tail": _tail(str(e.stderr or ""), 30),
            "duration_sec": round(time.time() - started, 3),
            "cmd": cmd,
            "error": "timeout",
        }


def _step(name: str, cmd: List[str], timeout_sec: float) -> Dict[str, Any]:
    r = _run(cmd, timeout_sec=timeout_sec)
    return {
        "name": name,
        "ok": bool(r.get("ok", False)),
        "returncode": int(r.get("returncode", 1)),
        "duration_sec": float(r.get("duration_sec", 0.0) or 0.0),
        "cmd": r.get("cmd", []),
        "stdout_tail": r.get("stdout_tail", ""),
        "stderr_tail": r.get("stderr_tail", ""),
        "error": r.get("error", ""),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="First small live canary workflow (guarded)")
    ap.add_argument("--mock", default="false", choices=["auto", "true", "false"])
    ap.add_argument("--orders-path", default="")
    ap.add_argument("--max-orders", type=int, default=1)
    ap.add_argument("--max-total-qty", type=int, default=3)
    ap.add_argument("--execute", action="store_true", help="Actually send canary live orders")
    ap.add_argument("--confirm", default="", help="Required when --execute (must be LIVE_CANARY)")
    ap.add_argument("--skip-cancel-open", action="store_true")
    ap.add_argument("--skip-virtual-gate", action="store_true", help="Bypass virtual-trading pass gate (not recommended)")
    ap.add_argument("--virtual-max-age-days", type=int, default=7)
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--timeout-sec", type=float, default=180.0)
    ap.add_argument("--summary-json", default="")
    args = ap.parse_args()

    if args.execute and str(args.confirm).strip().upper() != "LIVE_CANARY":
        print("[STOP] --execute requires --confirm LIVE_CANARY")
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.summary_json) if args.summary_json else (LOG_DIR / f"kis_live_canary_first_{ts}.json")
    out_latest = LOG_DIR / "kis_live_canary_first_latest.json"

    py = sys.executable
    mode = "EXECUTE" if args.execute else "DRY"

    if args.notify and callable(send_alert):
        try:
            send_alert(
                f"[CANARY_FIRST] start mode={mode} mock={args.mock} max_orders={args.max_orders}",
                level="warning" if args.execute else "info",
            )
        except Exception:
            pass

    steps: List[Dict[str, Any]] = []

    gate = _check_virtual_gate(max_age_days=max(1, int(args.virtual_max_age_days)))
    gate_ok = bool(gate.get("ok", False))
    if args.skip_virtual_gate:
        gate_ok = True

    steps.append(
        {
            "name": "virtual_trading_gate",
            "ok": bool(gate_ok),
            "returncode": 0 if gate_ok else 2,
            "duration_sec": 0.0,
            "cmd": [],
            "stdout_tail": "",
            "stderr_tail": "",
            "error": "" if gate_ok else str(gate.get("reason", "virtual_gate_failed")),
            "gate": gate,
            "skip_virtual_gate": bool(args.skip_virtual_gate),
        }
    )

    if args.execute and (not gate_ok):
        payload = {
            "generated_at": _now_ts(),
            "mode": mode,
            "mock": str(args.mock),
            "execute": bool(args.execute),
            "ok": False,
            "blocked_reason": "virtual_gate_failed",
            "steps": steps,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[STOP] execute blocked: virtual trading evidence not satisfied")
        print(f"[STOP] reason={gate.get('reason')} path={gate.get('path')}")
        print(f"[OK] summary={out_json}")
        if args.notify and callable(send_alert):
            try:
                send_alert(
                    "[CANARY_FIRST] blocked: virtual trading gate failed",
                    level="error",
                    extra={"reason": gate.get("reason"), "summary_json": str(out_json)},
                )
            except Exception:
                pass
        return 2

    pre = [
        py,
        str(ROOT / "tools" / "kis_healthcheck.py"),
        "--mock",
        str(args.mock),
        "--code",
        "005930",
        "--check-balance",
        "--check-open-orders",
    ]
    if args.notify:
        pre.append("--notify-on-fail")
    steps.append(_step("preflight_healthcheck", pre, timeout_sec=args.timeout_sec))

    canary_cmd = [
        py,
        str(ROOT / "tools" / "kis_canary_run.py"),
        "--mock",
        str(args.mock),
        "--max-orders",
        str(max(1, int(args.max_orders))),
        "--max-total-qty",
        str(max(1, int(args.max_total_qty))),
    ]
    if str(args.orders_path).strip():
        canary_cmd.extend(["--orders-path", str(args.orders_path).strip()])
    if args.execute:
        canary_cmd.extend(["--apply", "--confirm", "CANARY"])
    steps.append(_step("canary_dispatch", canary_cmd, timeout_sec=args.timeout_sec))

    if not args.skip_cancel_open:
        cancel_cmd = [
            py,
            str(ROOT / "tools" / "kis_cancel_open_orders.py"),
            "--mock",
            str(args.mock),
            "--min-age-minutes",
            "1",
            "--max-cancels",
            str(max(1, int(args.max_orders))),
        ]
        if args.execute:
            cancel_cmd.append("--apply")
        steps.append(_step("cancel_open_orders", cancel_cmd, timeout_sec=args.timeout_sec))

    steps.append(
        _step(
            "mode_compare_report",
            [py, str(ROOT / "tools" / "kis_mode_compare_report.py")],
            timeout_sec=args.timeout_sec,
        )
    )

    steps.append(
        _step(
            "account_snapshot",
            [py, str(ROOT / "tools" / "kis_account_snapshot.py"), "--mock", str(args.mock), "--with-quotes"],
            timeout_sec=args.timeout_sec,
        )
    )

    ok = all(bool(s.get("ok", False)) for s in steps)
    payload = {
        "generated_at": _now_ts(),
        "mode": mode,
        "mock": str(args.mock),
        "execute": bool(args.execute),
        "ok": bool(ok),
        "steps": steps,
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] summary={out_json}")
    print(f"[OK] mode={mode} ok={ok}")

    if args.notify and callable(send_alert):
        try:
            send_alert(
                f"[CANARY_FIRST] done mode={mode} ok={ok}",
                level="info" if ok else "error",
                extra={"summary_json": str(out_json)},
            )
        except Exception:
            pass

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
