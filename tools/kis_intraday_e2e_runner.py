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


def _tail(text: str, lines: int = 20) -> str:
    arr = str(text or "").splitlines()
    if len(arr) <= lines:
        return "\n".join(arr)
    return "\n".join(arr[-lines:])


def _run_cmd(cmd: List[str], timeout_sec: float, cwd: Path) -> Dict[str, Any]:
    started = time.time()
    try:
        cp = subprocess.run(
            cmd,
            cwd=str(cwd),
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
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": _tail(cp.stdout, lines=30),
            "stderr_tail": _tail(cp.stderr, lines=30),
            "cmd": cmd,
        }
    except subprocess.TimeoutExpired as e:
        return {
            "ok": False,
            "returncode": 124,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": _tail(str(e.stdout or ""), lines=30),
            "stderr_tail": _tail(str(e.stderr or ""), lines=30),
            "cmd": cmd,
            "error": f"timeout after {timeout_sec}s",
        }
    except Exception as e:  # pragma: no cover
        return {
            "ok": False,
            "returncode": 1,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": "",
            "stderr_tail": "",
            "cmd": cmd,
            "error": str(e),
        }


def _step(name: str, cmd: List[str], timeout_sec: float) -> Dict[str, Any]:
    r = _run_cmd(cmd=cmd, timeout_sec=timeout_sec, cwd=ROOT)
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
    ap = argparse.ArgumentParser(description="Intraday E2E scenario runner (preflight->quote->dispatch->cancel->snapshot)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--codes", default="005930,000660")
    ap.add_argument("--health-code", default="005930")
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--interval-sec", type=float, default=120.0)
    ap.add_argument("--timeout-sec", type=float, default=180.0)
    ap.add_argument("--max-orders", type=int, default=1)
    ap.add_argument("--validation-mode", action="store_true")
    ap.add_argument("--skip-cancel-open", action="store_true")
    ap.add_argument("--apply", action="store_true", help="Live order submit mode")
    ap.add_argument("--allow-offhours", action="store_true")
    ap.add_argument("--confirm", default="", help="Required when --apply (must be E2E_APPLY)")
    ap.add_argument("--stop-on-fail", action="store_true")
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--summary-json", default="")
    args = ap.parse_args()

    if args.apply and str(args.confirm).strip().upper() != "E2E_APPLY":
        print("[STOP] --apply requires --confirm E2E_APPLY")
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.summary_json) if args.summary_json else (LOG_DIR / f"kis_intraday_e2e_{ts}.json")
    out_latest = LOG_DIR / "kis_intraday_e2e_latest.json"

    mode = "APPLY" if args.apply else "DRY"
    py = sys.executable

    if args.notify and callable(send_alert):
        try:
            send_alert(
                f"[E2E] start mode={mode} mock={args.mock} iterations={args.iterations}",
                level="info",
                extra={"codes": args.codes},
            )
        except Exception:
            pass

    runs: List[Dict[str, Any]] = []
    pass_n = 0
    fail_n = 0

    for i in range(1, max(1, int(args.iterations)) + 1):
        iter_steps: List[Dict[str, Any]] = []

        iter_steps.append(
            _step(
                "preflight_healthcheck",
                [
                    py,
                    str(ROOT / "tools" / "kis_healthcheck.py"),
                    "--mock",
                    str(args.mock),
                    "--code",
                    str(args.health_code),
                    "--check-balance",
                    "--check-open-orders",
                ],
                timeout_sec=args.timeout_sec,
            )
        )

        iter_steps.append(
            _step(
                "quote_poll",
                [
                    py,
                    str(ROOT / "tools" / "kis_quote_poll.py"),
                    "--mock",
                    str(args.mock),
                    "--codes",
                    str(args.codes),
                    "--iterations",
                    "3",
                    "--interval-sec",
                    "1.5",
                ],
                timeout_sec=args.timeout_sec,
            )
        )

        dispatch_cmd = [
            py,
            str(ROOT / "tools" / "kis_order_dispatch_from_exec.py"),
            "--mock",
            str(args.mock),
            "--max-orders",
            str(max(0, int(args.max_orders))),
        ]
        if args.validation_mode:
            dispatch_cmd.append("--validation-mode")
        if args.apply:
            dispatch_cmd.append("--apply")
            if args.allow_offhours:
                dispatch_cmd.append("--allow-offhours")
        iter_steps.append(_step("dispatch_orders", dispatch_cmd, timeout_sec=args.timeout_sec))

        if not args.skip_cancel_open:
            cancel_cmd = [
                py,
                str(ROOT / "tools" / "kis_cancel_open_orders.py"),
                "--mock",
                str(args.mock),
                "--min-age-minutes",
                "1",
                "--max-cancels",
                str(max(0, int(args.max_orders))),
            ]
            if args.apply:
                cancel_cmd.append("--apply")
            iter_steps.append(_step("cancel_open_orders", cancel_cmd, timeout_sec=args.timeout_sec))

        iter_steps.append(
            _step(
                "account_snapshot",
                [
                    py,
                    str(ROOT / "tools" / "kis_account_snapshot.py"),
                    "--mock",
                    str(args.mock),
                    "--with-quotes",
                ],
                timeout_sec=args.timeout_sec,
            )
        )

        iter_steps.append(
            _step(
                "status_monitor_short",
                [
                    py,
                    str(ROOT / "tools" / "kis_status_monitor.py"),
                    "--mock",
                    str(args.mock),
                    "--code",
                    str(args.health_code),
                    "--interval-sec",
                    "2",
                    "--duration-sec",
                    "6",
                ],
                timeout_sec=max(args.timeout_sec, 30.0),
            )
        )

        iter_ok = all(bool(s.get("ok", False)) for s in iter_steps)
        if iter_ok:
            pass_n += 1
        else:
            fail_n += 1

        rec = {
            "iteration": i,
            "ts": _now_ts(),
            "ok": bool(iter_ok),
            "steps": iter_steps,
        }
        runs.append(rec)

        if args.stop_on_fail and (not iter_ok):
            break

        if i < int(args.iterations):
            time.sleep(max(0.0, float(args.interval_sec)))

    payload = {
        "generated_at": _now_ts(),
        "mode": mode,
        "mock": str(args.mock),
        "iterations_requested": int(args.iterations),
        "iterations_done": int(len(runs)),
        "pass_n": int(pass_n),
        "fail_n": int(fail_n),
        "ok": fail_n == 0,
        "runs": runs,
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] summary={out_json}")
    print(f"[OK] pass={pass_n} fail={fail_n}")

    if args.notify and callable(send_alert):
        try:
            send_alert(
                f"[E2E] done mode={mode} mock={args.mock} pass={pass_n} fail={fail_n}",
                level=("info" if fail_n == 0 else "error"),
                extra={"summary_json": str(out_json)},
            )
        except Exception:
            pass

    return 0 if fail_n == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
