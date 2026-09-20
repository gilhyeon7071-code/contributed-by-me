from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
EVIDENCE_DIR = LOG_DIR / "premarket_evidence"
KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(KST)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _age_seconds(path: Path, now: dt.datetime) -> float | None:
    if not path.exists():
        return None
    mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=KST)
    return max(0.0, (now - mtime).total_seconds())


def _sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_step(name: str, cmd: List[str], *, dry_run: bool, timeout_sec: int, cwd: Path | None = None) -> Dict[str, Any]:
    rec: Dict[str, Any] = {
        "name": name,
        "cmd": cmd,
        "dry_run": bool(dry_run),
        "returncode": None,
        "ok": False,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    if dry_run:
        rec["ok"] = True
        rec["skipped_reason"] = "dry_run"
        return rec
    try:
        env = os.environ.copy()
        p = subprocess.run(
            cmd,
            cwd=str(cwd or ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            shell=False,
            env=env,
        )
        rec["returncode"] = int(p.returncode)
        rec["ok"] = p.returncode == 0
        rec["stdout_tail"] = (p.stdout or "")[-1200:]
        rec["stderr_tail"] = (p.stderr or "")[-1200:]
    except subprocess.TimeoutExpired as e:
        rec["returncode"] = 124
        rec["ok"] = False
        rec["error"] = f"timeout_after_{timeout_sec}s"
        rec["stdout_tail"] = (e.stdout or "")[-1200:] if isinstance(e.stdout, str) else ""
        rec["stderr_tail"] = (e.stderr or "")[-1200:] if isinstance(e.stderr, str) else ""
    except Exception as e:
        rec["returncode"] = 125
        rec["ok"] = False
        rec["error"] = str(e)
    return rec


def _latest_kis_healthcheck() -> Path | None:
    items = sorted(LOG_DIR.glob("kis_healthcheck_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return items[0] if items else None


def _kis_healthcheck_cmd(kis_mock: str) -> List[str]:
    script = ROOT / "tools" / "kis_healthcheck.py"
    local_py = Path.home() / "AppData" / "Local" / "Programs" / "Python" / "Python312" / "python.exe"
    try:
        local_py_ok = local_py.exists()
    except PermissionError:
        local_py_ok = True
    if local_py_ok:
        return [str(local_py), str(script), "--mock", kis_mock]
    tools_path = str(ROOT / "tools")
    local_site = str(Path.home() / "AppData" / "Local" / "Programs" / "Python" / "Python312" / "Lib" / "site-packages")
    runner = (
        "import runpy,sys;"
        "sys.path=[p for p in sys.path if 'vibe\\\\buffett\\\\.venv' not in str(p).lower().replace('/', '\\\\')];"
        f"sys.path.insert(0,{tools_path!r});"
        f"sys.path.insert(1,{local_site!r});"
        f"sys.argv=[{str(script)!r}]+sys.argv[1:];"
        "runpy.run_path(sys.argv[0], run_name='__main__')"
    )
    return [sys.executable, "-c", runner, "--mock", kis_mock]


def _check_runtime(now: dt.datetime) -> Dict[str, Any]:
    path = LOG_DIR / "runtime_chain_status_latest.json"
    obj = _read_json(path)
    status = str(obj.get("overall", "")).upper()
    ok = status == "OK"
    return {
        "name": "runtime_chain",
        "ok": ok,
        "status": status or "MISSING",
        "path": str(path),
        "age_seconds": _age_seconds(path, now),
        "hard_fail": not ok,
    }


def _check_watchdog(now: dt.datetime) -> Dict[str, Any]:
    path = LOG_DIR / "intraday_watchdog_status_latest.json"
    obj = _read_json(path)
    status = str(obj.get("status", "")).upper()
    action = str(obj.get("action", ""))
    ok = status == "FRESH" or (action in {"restart", "duplicate_restart"} and bool(obj.get("started")))
    return {
        "name": "intraday_watchdog",
        "ok": ok,
        "status": status or "MISSING",
        "action": action,
        "in_restart_window": bool(obj.get("in_restart_window")),
        "started": bool(obj.get("started")),
        "path": str(path),
        "age_seconds": _age_seconds(path, now),
        "hard_fail": not ok,
    }


def _check_kis_health(now: dt.datetime, *, max_age_sec: int) -> Dict[str, Any]:
    path = _latest_kis_healthcheck()
    obj = _read_json(path) if path else {}
    ok = bool(obj.get("ok"))
    age = _age_seconds(path, now) if path else None
    fresh = age is not None and age <= max_age_sec
    return {
        "name": "kis_healthcheck",
        "ok": bool(ok and fresh),
        "source_ok": ok,
        "fresh": fresh,
        "max_age_seconds": max_age_sec,
        "path": str(path) if path else "",
        "age_seconds": age,
        "hard_fail": not bool(ok and fresh),
    }


def _check_gateway(now: dt.datetime, *, max_age_sec: int) -> Dict[str, Any]:
    paths = sorted(LOG_DIR.glob("gateway_health_*_latest.json"))
    rows = []
    ok = bool(paths)
    for path in paths:
        obj = _read_json(path)
        age = _age_seconds(path, now)
        row_ok = bool(obj.get("ok")) and age is not None and age <= max_age_sec
        rows.append({
            "path": str(path),
            "name": obj.get("name", path.stem),
            "ok": row_ok,
            "source_ok": bool(obj.get("ok")),
            "age_seconds": age,
            "latency_ms": obj.get("latency_ms"),
        })
        ok = ok and row_ok
    return {
        "name": "gateway_health",
        "ok": ok,
        "max_age_seconds": max_age_sec,
        "rows": rows,
        "hard_fail": not ok,
    }


def build(args: argparse.Namespace) -> Dict[str, Any]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    now = _now()
    run_id = now.strftime("premarket_%Y%m%d_%H%M%S")

    lock_path = LOG_DIR / "premarket_health.lock"
    if lock_path.exists() and not args.dry_run:
        is_stale = False
        try:
            import psutil
            lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
            pid = lock_data.get("pid")
            if pid is not None and not psutil.pid_exists(pid):
                is_stale = True
        except Exception:
            pass
        
        if is_stale:
            try:
                lock_path.unlink()
            except Exception:
                pass
        else:
            payload = {
                "generated_at": now.isoformat(timespec="seconds"),
                "run_id": run_id,
                "status_overall": "FAIL",
                "alerts": ["premarket_lock_exists"],
                "lock_path": str(lock_path),
                "policy_effect": False,
                "trading_effect": False,
            }
            return payload

    if not args.dry_run:
        lock_data = {
            "pid": os.getpid(),
            "run_id": run_id,
            "started_at": now.isoformat(timespec="seconds")
        }
        lock_path.write_text(json.dumps(lock_data, ensure_ascii=False), encoding="utf-8")

    refresh_results: List[Dict[str, Any]] = []
    try:
        if not args.skip_refresh:
            refresh_results.append(_run_step(
                "gateway_health_snapshot",
                [str(ROOT / "run_gateway_health_snapshot.bat")],
                dry_run=args.dry_run,
                timeout_sec=args.refresh_timeout_sec,
            ))
            refresh_results.append(_run_step(
                "intraday_watchdog",
                [str(ROOT / "run_intraday_watchdog.bat")],
                dry_run=args.dry_run,
                timeout_sec=args.refresh_timeout_sec,
            ))
            kis_existing = _check_kis_health(_now(), max_age_sec=args.kis_max_age_sec)
            if bool(kis_existing.get("ok")):
                refresh_results.append({
                    "name": "kis_healthcheck",
                    "cmd": [],
                    "dry_run": bool(args.dry_run),
                    "returncode": 0,
                    "ok": True,
                    "stdout_tail": "",
                    "stderr_tail": "",
                    "skipped_reason": "fresh_existing_artifact",
                    "path": kis_existing.get("path", ""),
                })
            else:
                refresh_results.append(_run_step(
                    "kis_healthcheck",
                    _kis_healthcheck_cmd(args.kis_mock),
                    dry_run=args.dry_run,
                    timeout_sec=args.kis_timeout_sec,
                ))

        now = _now()
        checks = [
            _check_runtime(now),
            _check_watchdog(now),
            _check_gateway(now, max_age_sec=args.gateway_max_age_sec),
            _check_kis_health(now, max_age_sec=args.kis_max_age_sec),
        ]
        refresh_failures = [r["name"] for r in refresh_results if not bool(r.get("ok"))]
        hard_failures = [c["name"] for c in checks if bool(c.get("hard_fail"))]
        refresh_warnings = [f"refresh_failed:{n}" for n in refresh_failures]
        alerts = [f"check_failed:{n}" for n in hard_failures]
        status = "PASS" if not hard_failures else "FAIL"

        evidence_files = [
            LOG_DIR / "runtime_chain_status_latest.json",
            LOG_DIR / "intraday_watchdog_status_latest.json",
            *_latest_gateway_files(),
        ]
        kh = _latest_kis_healthcheck()
        if kh:
            evidence_files.append(kh)
        hashes = [
            {"path": str(p), "sha256": _sha256(p)}
            for p in evidence_files
            if p.exists()
        ]

        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "run_id": run_id,
            "status_overall": status,
            "dry_run": bool(args.dry_run),
            "policy_effect": False,
            "trading_effect": False,
            "allowed_recovery_scope": "refresh_existing_health_and_watchdog_only",
            "excluded_actions": [
                "leader_lease_switch",
                "canary_execute_enable",
                "order_dispatch",
                "gate_or_lock_relaxation",
                "policy_threshold_change",
            ],
            "refresh_results": refresh_results,
            "checks": checks,
            "alerts_count": len(alerts),
            "alerts": alerts,
            "refresh_warnings": refresh_warnings,
            "hard_failures": hard_failures,
            "hashes": hashes,
        }
        return payload
    finally:
        if not args.dry_run:
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass


def _latest_gateway_files() -> List[Path]:
    return sorted(LOG_DIR.glob("gateway_health_*_latest.json"))


def main() -> int:
    ap = argparse.ArgumentParser(description="RootA premarket health wrapper status builder")
    ap.add_argument("--dry-run", action="store_true", help="Do not run refresh commands and do not fail exit for stale inputs")
    ap.add_argument("--skip-refresh", action="store_true", help="Only read current artifacts")
    ap.add_argument("--kis-mock", default="true", choices=["auto", "true", "false"])
    ap.add_argument("--gateway-max-age-sec", type=int, default=900)
    ap.add_argument("--kis-max-age-sec", type=int, default=900)
    ap.add_argument("--refresh-timeout-sec", type=int, default=90)
    ap.add_argument("--kis-timeout-sec", type=int, default=45)
    args = ap.parse_args()

    payload = build(args)
    ts = _now().strftime("%Y%m%d_%H%M%S")
    latest_path = LOG_DIR / "premarket_health_latest.json"
    archive_path = EVIDENCE_DIR / f"premarket_health_{ts}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    latest_path.write_text(text, encoding="utf-8-sig")
    archive_path.write_text(text, encoding="utf-8-sig")
    print(f"[PREMARKET] status={payload.get('status_overall')} alerts={payload.get('alerts_count', len(payload.get('alerts', [])))} latest={latest_path}")

    if args.dry_run:
        return 0
    return 0 if payload.get("status_overall") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
