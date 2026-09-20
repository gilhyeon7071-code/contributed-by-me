# -*- coding: utf-8 -*-
"""락이 **주인 없이 남은 것**인지 보고, 그렇다면 치운다.

[2026-09-13] 왜 있나

  장중 루프를 재기동하려고 죽였더니 `2_Logs/run_intraday_paper.lock/` 이 남았고,
  `run_intraday_paper.bat` 이 그걸 보고 `[SKIP] already running` 으로 물러났다.
  **루프가 하루 종일 안 올라온다.** 워치독도 못 살렸다.

  정보는 이미 있었다 - `heartbeat.json` 안에 `pid` 가 적혀 있다.
  배치가 그걸 안 볼 뿐이었다. 같은 결함을 오늘 아침 일일배치 락에서도 고쳤다
  (`intraday_paper_loop._daily_batch_lock_state`). 두 번째 사본이다.

판정 규칙 - **모르면 살아있다고 본다**

  주인 pid 를 못 읽거나 살았는지 판정 못 하면 락을 남긴다. 치우는 쪽이
  중복 실행을 만들고, 중복 실행은 중복 발주가 된다(2026-09-08 에 실제로 냈다).

    python tools/stale_lock_check.py --lock 2_Logs/run_intraday_paper.lock
    python tools/stale_lock_check.py --lock ... --remove     # 고아면 치운다
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]


def _pid_alive(pid: int) -> Optional[bool]:
    """살아있으면 True, 죽었으면 False, **판정 불가면 None**."""
    if pid <= 0:
        return None
    try:
        import subprocess
        r = subprocess.run(
            ["tasklist", "/FI", "PID eq %d" % pid, "/NH", "/FO", "CSV"],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="replace",
        )
        if r.returncode != 0:
            return None
        out = (r.stdout or "").strip()
        if not out:
            return None
        if ("No tasks" in out) or ("%d" % pid) not in out:
            return False
        return True
    except Exception:
        return None


def _owner_pid(lock: Path) -> Dict[str, Any]:
    hb = lock / "heartbeat.json"
    if not hb.is_file():
        return {"pid": None, "source": "no_heartbeat", "hb_ts": None}
    try:
        obj = json.loads(hb.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"pid": None, "source": "heartbeat_unreadable:%s" % type(exc).__name__, "hb_ts": None}
    try:
        pid = int(obj.get("pid"))
    except Exception:
        return {"pid": None, "source": "heartbeat_no_pid", "hb_ts": obj.get("ts")}
    return {"pid": pid, "source": "heartbeat", "hb_ts": obj.get("ts")}


def inspect(lock: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "checked_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "lock": str(lock),
        "exists": lock.exists(),
        "verdict": "NO_LOCK",
        "reason": "",
    }
    if not lock.exists():
        return out
    info = _owner_pid(lock)
    out.update({k: info[k] for k in ("pid", "source", "hb_ts")})
    if info["pid"] is None:
        out["verdict"] = "HELD"
        out["reason"] = "주인 pid 를 못 읽었다(%s) - 모르면 살아있다고 본다" % info["source"]
        return out
    alive = _pid_alive(int(info["pid"]))
    out["owner_alive"] = alive
    if alive is None:
        out["verdict"] = "HELD"
        out["reason"] = "pid %d 의 생사를 판정 못 했다 - 모르면 살아있다고 본다" % info["pid"]
    elif alive:
        out["verdict"] = "HELD"
        out["reason"] = "pid %d 가 살아있다" % info["pid"]
    else:
        out["verdict"] = "ORPHANED"
        out["reason"] = "pid %d 가 없다 - 주인 없는 락이다" % info["pid"]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lock", required=True)
    ap.add_argument("--remove", action="store_true", help="고아로 판정되면 치운다")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    lock = Path(args.lock)
    if not lock.is_absolute():
        lock = ROOT / lock
    rep = inspect(lock)

    if args.remove and rep["verdict"] == "ORPHANED":
        try:
            if lock.is_dir():
                shutil.rmtree(lock)
            else:
                lock.unlink()
            rep["removed"] = True
        except Exception as exc:
            rep["removed"] = False
            rep["remove_error"] = "%s: %s" % (type(exc).__name__, exc)

    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
    else:
        print("[LOCK] %s %s" % (rep["verdict"], rep["reason"]))
        if rep.get("removed"):
            print("[LOCK] 고아 락을 치웠다: %s" % rep["lock"])
        elif rep.get("remove_error"):
            print("[LOCK] 치우지 못했다: %s" % rep["remove_error"])

    # 0 = 진행해도 된다(락 없음/치웠다), 1 = 주인이 있다
    if rep["verdict"] == "NO_LOCK":
        return 0
    if rep["verdict"] == "ORPHANED" and rep.get("removed"):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
