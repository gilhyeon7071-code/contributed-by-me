# -*- coding: utf-8 -*-
"""동결된 모든 라운드를 **매일 검사한다.** 동결은 선언이 아니라 검사다.

[2026-09-12] PLANS (370)(371). 미결 대장 C12.

왜 필요한가
  RD_20260831_flow_h10 이 2026-09-10 부터 동결 위반 상태였는데 아무도 몰랐다.
  `tools/load_merged_panel.py` 가 거래대금 int32 수리(PLANS 306)로 바뀌었고,
  그 파일이 기준선에 들어 있었다. PLANS 에 `[OK] 변조 없음` 이 적힌 시점은 09-10 **이전**이다.
  그 뒤로 `round_preflight --check` 를 아무도 돌리지 않았다.

  **검사를 안 돌리면 동결은 유지되지 않는다.**

무엇을 하나
  docs/research/rounds/ 의 라운드 전부에 대해 round_preflight --check 를 돌리고
  결과를 **날짜 없는 append-only 원장**에 남긴다. 상태가 **바뀐 날**만 경보한다
  (매일 같은 FAIL 로 울리면 그 경보는 무시된다 - 예외 기록이 있는 FAIL 이 그렇다).

    python tools/round_freeze_watch.py              # 검사 + 원장 기록
    python tools/round_freeze_watch.py --no-alert
    python tools/round_freeze_watch.py --out-dir <경로>   # 격리 실행
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
ROUNDS = ROOT / "docs" / "research" / "rounds"
LOG_DIR = ROOT / "2_Logs"
LEDGER_NAME = "round_freeze_watch_ledger.jsonl"      # 날짜 없는 append-only
LATEST_NAME = "round_freeze_watch_latest.json"
_NL = chr(10)


def list_rounds() -> List[str]:
    """동결된 라운드 중 **종결되지 않은 것**만 감시한다.

    [2026-09-19] 종결된 라운드(closure.md)는 기준선이 움직여도 더 이상 측정하지 않으니
    FAIL 이 의미가 없다. 그걸 매일 울리면 진짜 위반이 그 속에 묻힌다.
    frozen.json 은 지우지 않는다 — 증거를 남기고 종결은 closure.md 로만 표시한다.
    """
    if not ROUNDS.is_dir():
        return []
    return sorted(p.name for p in ROUNDS.iterdir()
                  if p.is_dir() and (p / "frozen.json").is_file()
                  and not (p / "closure.md").is_file())


def list_closed() -> List[Dict[str, str]]:
    """종결된 라운드도 화면에 남긴다 — '감시 중' 과 '종결' 이 구별돼야 한다."""
    if not ROUNDS.is_dir():
        return []
    return [{"round": d.name, "state": "CLOSED(종결)"}
            for d in sorted(ROUNDS.iterdir())
            if d.is_dir() and (d / "closure.md").is_file()]


def list_unfrozen() -> List[Dict[str, str]]:
    """동결되지 않은 라운드도 **보이게 한다.**

    [2026-09-12] frozen.json 이 없으면 감시 대상이 아니다. 그런데 그것을 표시하지 않으면
    "감시하고 있다" 와 "감시 대상이 아니다" 가 화면에서 구별되지 않는다.
    RD_20260831_index_gap 이 그랬다 - DRAFT 에서 CLOSED 된 정상 상태인데 안 보였다.
    """
    if not ROUNDS.is_dir():
        return []
    out: List[Dict[str, str]] = []
    for d in sorted(ROUNDS.iterdir()):
        if not d.is_dir() or (d / "frozen.json").is_file():
            continue
        if (d / "closure.md").is_file():
            continue          # 종결분은 list_closed() 가 한 줄로 보여준다(중복 표시 방지)
        if (d / "registration.md").is_file():
            state = "DRAFT(동결 전)"
        else:
            state = "UNKNOWN"
        out.append({"round": d.name, "state": state})
    return out


def check_round(rid: str) -> Dict[str, Any]:
    py = sys.executable
    try:
        # [2026-09-12] **encoding 을 반드시 준다.** 기본값은 콘솔 코드페이지(cp949)라
        #   preflight 의 출력에서 UnicodeDecodeError 가 나고, 그 예외가 reader 스레드에서
        #   터지면서 stdout 이 비어 돌아온다 -> FAIL 줄을 못 보고 **거짓 OK** 를 냈다.
        r = subprocess.run([py, str(ROOT / "tools" / "round_preflight.py"), "--check", rid],
                           capture_output=True, text=True, timeout=300, cwd=str(ROOT),
                           encoding="utf-8", errors="replace")
    except Exception as exc:
        return {"round": rid, "verdict": "ERROR", "detail": "%s: %s" % (type(exc).__name__, exc)}
    out = (r.stdout or "") + (r.stderr or "")
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    fails = [l for l in lines if l.startswith("[FAIL]")]
    # 출력이 비었는데 rc!=0 이면 **판정 불가**다. OK 로 흡수하지 않는다
    if not lines and int(r.returncode) != 0:
        return {"round": rid, "verdict": "ERROR", "rc": int(r.returncode),
                "detail": "출력이 비었고 rc!=0 - 판정 불가"}
    # 예외가 기록돼 있으면 그 사실을 같이 싣는다 (판정은 사람이 한다)
    exc_file = ROUNDS / rid / "exceptions.md"
    return {
        "round": rid,
        "verdict": "FAIL" if (fails or int(r.returncode) != 0) else "OK",
        "rc": int(r.returncode),
        "fail_lines": fails[:8],
        "has_exceptions_doc": exc_file.is_file(),
        "exceptions_bytes": int(exc_file.stat().st_size) if exc_file.is_file() else 0,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ledger = out_dir / LEDGER_NAME          # **out_dir 를 따른다**

    rids = list_rounds()
    if not rids:
        print("[WATCH] 감시 중인 동결 라운드가 없다")
        for c in list_closed():
            print("  [x] %-30s %s  (감시 대상 아님)" % (c["round"], c["state"]))
        for u in list_unfrozen():
            print("  [ ] %-30s %s  (감시 대상 아님)" % (u["round"], u["state"]))
        return 0

    now = dt.datetime.now()
    results = [check_round(r) for r in rids]

    # 직전 상태를 읽어 **바뀐 것만** 경보한다
    prev: Dict[str, str] = {}
    if ledger.is_file():
        try:
            for line in ledger.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                d = json.loads(line)
                for x in d.get("results") or []:
                    prev[str(x.get("round"))] = str(x.get("verdict"))
        except Exception:
            prev = {}

    changed = [x for x in results if prev.get(x["round"]) not in (None, x["verdict"])]
    new_fail = [x for x in results if x["verdict"] == "FAIL" and prev.get(x["round"]) == "OK"]

    rec = {"ts": now.strftime("%Y-%m-%dT%H:%M:%S"), "ymd": now.strftime("%Y%m%d"),
           "rounds": len(rids), "results": results,
           "changed": [x["round"] for x in changed]}
    with ledger.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + _NL)
    (out_dir / LATEST_NAME).write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[WATCH] 라운드 %d개  %s" % (len(rids), now.strftime("%Y-%m-%d %H:%M")))
    for x in results:
        mark = "O" if x["verdict"] == "OK" else "X"
        note = "" if x["verdict"] == "OK" else (
            "  (예외문서 %s)" % ("있음" if x["has_exceptions_doc"] else "**없음**"))
        print("  [%s] %-30s %s%s" % (mark, x["round"], x["verdict"], note))
        for l in x.get("fail_lines") or []:
            print("        %s" % l[:100])
    for c in list_closed():
        print("  [x] %-30s %s  (감시 대상 아님)" % (c["round"], c["state"]))
    rec["closed"] = list_closed()
    unfrozen = list_unfrozen()
    for u in unfrozen:
        print("  [ ] %-30s %s  (감시 대상 아님)" % (u["round"], u["state"]))
    rec["unfrozen"] = unfrozen
    if changed:
        print("  * 상태 변화: %s" % ", ".join(x["round"] for x in changed))
    else:
        print("  * 상태 변화 없음 (전일 대비)")

    # 경보는 **새로 FAIL 이 된 것**에만. 예외가 기록된 지속 FAIL 로는 울리지 않는다
    if new_fail and not args.no_alert:
        text = "[동결 감시] 새로 위반: %s%s%s" % (
            ", ".join(x["round"] for x in new_fail), _NL,
            _NL.join(l for x in new_fail for l in (x.get("fail_lines") or [])[:3]))
        try:
            sys.path.insert(0, str(ROOT / "tools"))
            from notify_channels import send_alert  # type: ignore
            res = send_alert(text, level="error", extra={"source": "round_freeze_watch"},
                             cooldown_sec=6 * 3600)
            print("  [ALERT] ok=%s suppressed=%s" % (res.get("ok"), res.get("suppressed")))
        except Exception as exc:
            print("  [ALERT_FAILED] %s: %s" % (type(exc).__name__, exc))
    return 0


if __name__ == "__main__":
    sys.exit(main())
