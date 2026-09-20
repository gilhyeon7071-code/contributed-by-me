# -*- coding: utf-8 -*-
"""불가침 경계가 **실제로 지켜지고 있는지** 매일 기계적으로 잰다.

[2026-09-13] 사용자 지적: *"매번묻는게 지시하는게 똑같고 실수반복"*.

왜 스킬만으로는 안 되나
  2026-09-12 에 내가 `VIBE_Investor_Flow_Daily` 를 16:30 -> 20:30 으로 옮겼다.
  그것은 `OBJECTIVE_LEDGER.md` 의 **O5 불가침 경계**가 명시적으로 금지한 것인데,
  나는 그 문서를 읽지 않았다. 인계서에 "시작 시 OBJECTIVE_LEDGER 부터" 라고
  적혀 있었는데도 그랬다.

  **스킬은 내가 불러야 뜬다.** 그런데 실패는 "스킬을 안 불렀다" 가 아니라
  "아무것도 안 부르고 바로 시작했다" 였다. 의도를 하나 더 얹는 것으로는 안 막힌다.
  이 저장소에서 반복해서 확인된 것은 **의도보다 탐지가 이긴다** 는 것이다.

무엇을 하나
  경계 목록을 **문서에서 읽는다.** 하드코딩하면 문서가 바뀔 때 이 도구가 거짓말을 한다.
  그 대상들의 현재 상태(예약 시각 / sha256)를 재고, 날짜 없는 append-only 원장과 비교해
  **바뀐 것만** 알린다.

    python tools/boundary_watch.py            # 검사 + 원장 기록
    python tools/boundary_watch.py --show     # 지금 경계가 무엇인지만 출력 (착수 전에)
    python tools/boundary_watch.py --accept "사유"   # 현재 상태를 새 기준으로 승인
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LEDGER = LOG_DIR / "boundary_watch_ledger.jsonl"     # 날짜 없는 append-only
LATEST = LOG_DIR / "boundary_watch_latest.json"
OBJ = ROOT / "docs" / "references" / "OBJECTIVE_LEDGER.md"
SECTION = "불가침 경계"
_NL = chr(10)


def parse_boundaries() -> Dict[str, Any]:
    """문서의 불가침 경계 절에서 대상을 읽는다.

    **파싱에 실패하면 조용히 통과시키지 않는다.** 경계를 모르는 상태가
    경계가 없는 상태로 읽히면 이 도구를 만든 이유가 사라진다.
    """
    if not OBJ.is_file():
        return {"ok": False, "reason": "OBJECTIVE_LEDGER.md 가 없다", "tasks": [], "files": []}
    text = OBJ.read_text(encoding="utf-8")
    m = re.search(r"###\s*[^\n]*" + SECTION + r"[^\n]*\n(.*?)\n###", text, re.S)
    if not m:
        return {"ok": False, "reason": "불가침 경계 절을 찾지 못했다", "tasks": [], "files": []}
    block = m.group(1)
    fence = re.search(r"```(.*?)```", block, re.S)
    if not fence:
        return {"ok": False, "reason": "경계 절에 코드블록이 없다", "tasks": [], "files": []}
    body = fence.group(1)
    tasks = sorted(set(re.findall(r"\b(VIBE_[A-Za-z0-9_]+)", body)))
    files = sorted(set(re.findall(r"\b((?:tools|2_Logs|paper_engine)/[A-Za-z0-9_./-]+)", body)))
    rounds = sorted(set(re.findall(r"\b(RD_\d{8}_[A-Za-z0-9_]+)", block)))
    ok = bool(tasks or files)
    return {"ok": ok, "reason": "" if ok else "대상을 하나도 못 읽었다",
            "tasks": tasks, "files": files, "rounds": rounds,
            "source": str(OBJ)}


def _task_schedule(name: str) -> Dict[str, Any]:
    ps = ("$ErrorActionPreference='Stop';"
          "$t=Get-ScheduledTask -TaskName '%s';"
          "$g=$t.Triggers[0];"
          "'{0}|{1}|{2}' -f $t.State,$g.StartBoundary,$g.Repetition.Interval" % name)
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                           capture_output=True, text=True, timeout=30,
                           encoding="utf-8", errors="replace")
        raw = (r.stdout or "").strip()
        if not raw or "|" not in raw:
            return {"exists": False, "detail": (r.stderr or "").strip()[:120]}
        state, start, interval = raw.split("|")
        # Running 은 '지금 돌고 있다' 는 순간 상태지 경계 변화가 아니다 (2026-09-21).
        # 이것 때문에 감시가 4번 헛울었다. 다만 Disabled 는 진짜 경계 변화라 그대로 남긴다.
        if state == "Running":
            state = "Ready"
        return {"exists": True, "state": state,
                "start_hhmm": start[11:16] if len(start) > 15 else start,
                "interval": interval}
    except Exception as exc:
        return {"exists": False, "detail": "%s: %s" % (type(exc).__name__, exc)}


# 설계상 매일 줄이 붙는 파일. 전체 해시로 보면 **매일 운다** — 그러면 감시가 죽는다.
# 여기서 볼 것은 '바뀌었나' 가 아니라 **'앞부분이 고쳐졌거나 줄었나'** 다 (2026-09-21).
APPEND_ONLY = {"2_Logs/index_daily_history.csv"}
_HEAD_BYTES = 65536


def _file_sig(rel: str) -> Dict[str, Any]:
    p = ROOT / rel
    if not p.is_file():
        return {"exists": False}
    b = p.read_bytes()
    if rel in APPEND_ONLY:
        return {"exists": True, "append_only": True,
                "head16": hashlib.sha256(b[:_HEAD_BYTES]).hexdigest()[:16], "bytes": len(b)}
    return {"exists": True, "sha256_16": hashlib.sha256(b).hexdigest()[:16], "bytes": len(b)}


def _append_only_alarm(a: Dict[str, Any], c: Dict[str, Any]) -> Optional[str]:
    """붙기만 했으면 침묵, 앞이 바뀌었거나 줄었으면 운다. 둘 다 아니면 None."""
    if not (isinstance(a, dict) and isinstance(c, dict)):
        return "형태가 바뀌었다"
    if not c.get("exists"):
        return "파일이 사라졌다"
    if not a.get("exists"):
        return None                                  # 없다가 생긴 것은 경보 대상이 아니다
    if a.get("head16") != c.get("head16"):
        return "앞부분이 바뀌었다 (덮어쓰기·재생성 의심)"
    if int(c.get("bytes", 0)) < int(a.get("bytes", 0)):
        return "줄었다 %d -> %d bytes" % (a.get("bytes", 0), c.get("bytes", 0))
    return None


def snapshot() -> Dict[str, Any]:
    b = parse_boundaries()
    out: Dict[str, Any] = {
        "ts": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "parse": {k: b[k] for k in ("ok", "reason", "rounds", "source") if k in b},
        "tasks": {t: _task_schedule(t) for t in b.get("tasks") or []},
        "files": {f: _file_sig(f) for f in b.get("files") or []},
    }
    return out


def _prev() -> Optional[Dict[str, Any]]:
    if not LEDGER.is_file():
        return None
    last = None
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                last = json.loads(line)
            except Exception:
                continue
    return last


def diff(prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> List[str]:
    if not prev:
        return []
    out: List[str] = []
    for kind in ("tasks", "files"):
        pa, ca = prev.get(kind) or {}, cur.get(kind) or {}
        for name in sorted(set(pa) | set(ca)):
            a, c = pa.get(name), ca.get(name)
            if kind == "files" and isinstance(a, dict) and a.get("append_only"):
                why = _append_only_alarm(a, c)
                if why:
                    out.append("files %s: %s (%s -> %s)"
                               % (name, why, json.dumps(a, ensure_ascii=False),
                                  json.dumps(c, ensure_ascii=False)))
                continue
            if a != c:
                out.append("%s %s: %s -> %s"
                           % (kind, name,
                              json.dumps(a, ensure_ascii=False),
                              json.dumps(c, ensure_ascii=False)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true", help="경계만 출력하고 끝낸다 (착수 전)")
    ap.add_argument("--accept", default="", help="현재 상태를 새 기준으로 승인한다 (사유 필수)")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    b = parse_boundaries()
    if args.show:
        print("=" * 74)
        print(" 불가침 경계 (출처 %s)" % b.get("source"))
        print("=" * 74)
        if not b["ok"]:
            print("  [ERR] %s" % b["reason"])
            return 2
        print("  라운드   %s" % ", ".join(b.get("rounds") or ["-"]))
        print("  예약작업 %s" % ", ".join(b["tasks"] or ["-"]))
        print("  파일     %s" % ", ".join(b["files"] or ["-"]))
        print()
        print("  이 목록에 있는 것을 바꾸려면 **바꾸기 전에** OBJECTIVE_LEDGER 에 적고")
        print("  사용자 승인을 받는다. 2026-09-12 에 내가 안 그래서 경계를 한 번 넘었다.")
        return 0

    cur = snapshot()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    led = out_dir / LEDGER.name

    if not b["ok"]:
        # **경계를 못 읽은 것을 통과로 흡수하지 않는다**
        print("[BOUNDARY] 경계를 읽지 못했다: %s" % b["reason"])
        print("           문서 구조가 바뀌었을 수 있다. 이 도구가 조용해지면 감시가 사라진다")
        (out_dir / LATEST.name).write_text(json.dumps(cur, ensure_ascii=False, indent=2),
                                           encoding="utf-8")
        return 2

    prev = None
    if led.is_file():
        for line in led.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    prev = json.loads(line)
                except Exception:
                    continue
    changes = diff(prev, cur)
    if args.accept:
        cur["accepted"] = {"reason": args.accept,
                           "at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}
    rec = dict(cur, changes=changes)
    with led.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + _NL)
    (out_dir / LATEST.name).write_text(json.dumps(rec, ensure_ascii=False, indent=2),
                                       encoding="utf-8")

    print("[BOUNDARY] 예약작업 %d / 파일 %d  %s"
          % (len(cur["tasks"]), len(cur["files"]), cur["ts"]))
    for t, v in cur["tasks"].items():
        print("   %-32s %s" % (t, json.dumps(v, ensure_ascii=False)))
    for f, v in cur["files"].items():
        print("   %-32s %s" % (f, json.dumps(v, ensure_ascii=False)))
    if changes and not args.accept:
        print("  ** 경계가 바뀌었다 **")
        for c in changes:
            print("     %s" % c)
        print("  승인된 변경이면: python tools/boundary_watch.py --accept \"사유\"")
        return 3
    if args.accept:
        print("  [ACCEPT] %s" % args.accept)
    elif prev is None:
        print("  (첫 기록 - 다음 실행부터 비교한다)")
    else:
        print("  변화 없음")
    return 0


if __name__ == "__main__":
    sys.exit(main())
