from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _tail(text: str, n: int = 20) -> str:
    lines = str(text or "").splitlines()
    return "\n".join(lines[-n:]) if len(lines) > n else "\n".join(lines)


def _run_cmd(cmd: List[str], env: Dict[str, str], timeout_sec: float = 120.0) -> Dict[str, Any]:
    started = time.time()
    try:
        cp = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
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
        }
    except subprocess.TimeoutExpired as e:
        return {
            "ok": False,
            "returncode": 124,
            "stdout_tail": _tail(str(e.stdout or ""), 30),
            "stderr_tail": _tail(str(e.stderr or ""), 30),
            "duration_sec": round(time.time() - started, 3),
            "error": "timeout",
        }


def _case_missing_kis_credentials(py: str) -> Dict[str, Any]:
    env = dict(os.environ)
    for k in ["KIS_APP_KEY", "KIS_APP_KEY_FILE", "KIS_APP_SECRET", "KIS_APP_SECRET_FILE", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_NO_FILE"]:
        env.pop(k, None)
    env["KIS_SECRETS_DIR"] = str(ROOT / "_fault_tmp_no_secrets")

    cmd = [py, str(ROOT / "tools" / "kis_healthcheck.py"), "--mock", "auto", "--code", "005930"]
    r = _run_cmd(cmd, env=env, timeout_sec=60)
    passed = int(r.get("returncode", 0)) != 0
    return {
        "name": "missing_kis_credentials_fail_closed",
        "passed": bool(passed),
        "expected": "nonzero returncode",
        "actual": f"returncode={r.get('returncode')}",
        "detail": r,
    }


def _case_apply_guard_e2e(py: str) -> Dict[str, Any]:
    cmd = [
        py,
        str(ROOT / "tools" / "kis_intraday_e2e_runner.py"),
        "--mock",
        "auto",
        "--apply",
        "--iterations",
        "1",
    ]
    r = _run_cmd(cmd, env=dict(os.environ), timeout_sec=30)
    passed = int(r.get("returncode", 0)) == 2
    return {
        "name": "e2e_apply_guard",
        "passed": bool(passed),
        "expected": "returncode=2 without confirmation",
        "actual": f"returncode={r.get('returncode')}",
        "detail": r,
    }


def _case_alert_channel_fault_tolerant() -> Dict[str, Any]:
    try:
        from notify_channels import send_alert
    except Exception as e:
        return {
            "name": "alert_channel_fault_tolerant",
            "passed": False,
            "expected": "send_alert callable",
            "actual": f"import_error={e}",
            "detail": {},
        }

    old = {
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),
        "KAKAO_ACCESS_TOKEN": os.getenv("KAKAO_ACCESS_TOKEN"),
    }
    try:
        os.environ.pop("TELEGRAM_BOT_TOKEN", None)
        os.environ.pop("TELEGRAM_CHAT_ID", None)
        os.environ.pop("KAKAO_ACCESS_TOKEN", None)
        payload = send_alert(
            "[FAULT] alert channel degradation test",
            level="warning",
            channels="telegram,kakao,file",
            fail_silent=True,
            extra={"fault_case": "missing_tokens"},
        )
        passed = (not bool(payload.get("ok", True))) and isinstance(payload.get("results"), list)
        return {
            "name": "alert_channel_fault_tolerant",
            "passed": bool(passed),
            "expected": "ok=false and no exception",
            "actual": f"ok={payload.get('ok')}",
            "detail": payload,
        }
    except Exception as e:
        return {
            "name": "alert_channel_fault_tolerant",
            "passed": False,
            "expected": "no exception",
            "actual": f"exception={e}",
            "detail": {},
        }
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _case_ws_bad_endpoint(py: str) -> Dict[str, Any]:
    env = dict(os.environ)
    env["KIS_WS_URL"] = "ws://127.0.0.1:1"
    for k in ["KIS_APP_KEY", "KIS_APP_KEY_FILE", "KIS_APP_SECRET", "KIS_APP_SECRET_FILE", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_NO_FILE"]:
        env.pop(k, None)
    env["KIS_SECRETS_DIR"] = str(ROOT / "_fault_tmp_no_secrets")

    cmd = [
        py,
        str(ROOT / "tools" / "kis_realtime_ws.py"),
        "--codes",
        "005930",
        "--mock",
        "auto",
        "--duration-sec",
        "5",
        "--reconnect-max",
        "1",
    ]
    r = _run_cmd(cmd, env=env, timeout_sec=40)
    passed = int(r.get("returncode", 0)) != 0
    return {
        "name": "websocket_bad_endpoint_fail_closed",
        "passed": bool(passed),
        "expected": "nonzero returncode",
        "actual": f"returncode={r.get('returncode')}",
        "detail": r,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Fault-injection automation (fail-closed / guard / alert degradation)")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--stop-on-fail", action="store_true")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.summary_json) if args.summary_json else (LOG_DIR / f"kis_fault_injection_{ts}.json")
    out_latest = LOG_DIR / "kis_fault_injection_latest.json"

    py = sys.executable

    cases = [
        _case_missing_kis_credentials,
        _case_apply_guard_e2e,
        lambda _py: _case_alert_channel_fault_tolerant(),
        _case_ws_bad_endpoint,
    ]

    results: List[Dict[str, Any]] = []
    pass_n = 0
    fail_n = 0

    for fn in cases:
        rec = fn(py)
        results.append(rec)
        if bool(rec.get("passed", False)):
            pass_n += 1
        else:
            fail_n += 1
            if args.stop_on_fail:
                break

    payload = {
        "generated_at": _now_ts(),
        "total": len(results),
        "pass_n": pass_n,
        "fail_n": fail_n,
        "ok": fail_n == 0,
        "results": results,
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] summary={out_json}")
    print(f"[OK] pass={pass_n} fail={fail_n}")
    return 0 if fail_n == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())




