# -*- coding: utf-8 -*-
"""등록된 모든 연구 라운드의 기준선 대조를 **매일 자동으로** 돌린다.

2026-09-07 신규. OBJECTIVE_LEDGER.md 의 O5 불가침 경계에 이렇게 적혀 있다:

```
매일 확인   python tools/round_preflight.py --check RD_20260831_flow_h10
```

실측 결과 **배치·예약작업 어디에도 없었다.** 2026-08-31 동결 이래 한 번도 안 돌았고,
그래서 기준선 FAIL 을 8일간 아무도 몰랐다.

"매일 확인하라"는 지시에 자동화가 없으면 한 번도 안 돌아간다. 이 도구가 그 자리를 메운다.

`round_preflight.py --check` 는 라운드 하나만 받는다. 여기서는 `docs/research/rounds/`
아래 전부를 돌린다. 라운드가 늘어도 손댈 필요가 없다 — 손 목록은 조용히 뒤처진다.

    python tools/round_preflight_daily.py
    python tools/round_preflight_daily.py --no-alert
    python tools/round_preflight_daily.py --strict    # FAIL 있으면 rc=2
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


def _force_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_force_utf8_stdout()

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
ROUNDS_DIR = ROOT / "docs" / "research" / "rounds"
OUT_DIR = ROOT / "2_Logs"


def _now() -> dt.datetime:
    return dt.datetime.now()


def discover() -> List[str]:
    if not ROUNDS_DIR.is_dir():
        return []
    return sorted(p.name for p in ROUNDS_DIR.iterdir() if p.is_dir())


def run_all() -> Dict[str, Any]:
    now = _now()
    out: Dict[str, Any] = {"generated_at": now.strftime("%Y-%m-%d %H:%M:%S"), "rounds": []}

    rids = discover()
    if not rids:
        out["status"] = "NO_ROUNDS"
        out["reason"] = f"등록된 라운드가 없다: {ROUNDS_DIR}"
        return out

    try:
        sys.path.insert(0, str(TOOLS_DIR))
        import round_preflight as RP  # type: ignore
    except Exception as exc:
        out["status"] = "NO_TOOL"
        out["reason"] = f"round_preflight 를 못 읽는다: {type(exc).__name__}: {exc}"
        return out

    for rid in rids:
        # [2026-09-08] 종료된 라운드는 검사하지 않는다.
        #   closure.md 가 있으면 그 라운드는 명시적으로 닫힌 것이다.
        #   닫힌 라운드는 동결(frozen.json)이 없으므로 검사하면 매일 영구히
        #   "[FAIL] 동결되지 않았다" 가 나가고, 그 소음이 진짜 FAIL 을 덮는다.
        #   오늘 (242) 에서 같은 형태(같은 실패의 반복 발송)로 텔레그램이 429 를 냈다.
        #   CLOSED 는 숨기지 않고 상태로 남긴다 - 조용히 빼면 "라운드가 사라졌다" 가 된다.
        closure = ROUNDS_DIR / rid / "closure.md"
        if closure.exists():
            out["rounds"].append({
                "round_id": rid, "rc": 0, "status": "CLOSED",
                "reason": "closure.md 존재 - 종료된 라운드라 기준선 검사 대상이 아니다",
                "fail_lines": [], "lines": [],
            })
            continue
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                rc = RP.cmd_check(rid)
        except Exception as exc:
            out["rounds"].append({"round_id": rid, "rc": None, "status": "ERROR",
                                  "reason": f"{type(exc).__name__}: {exc}", "lines": []})
            continue
        text = buf.getvalue()
        lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
        fails = [ln for ln in lines if "[FAIL]" in ln or "[NO-GO]" in ln]
        out["rounds"].append({
            "round_id": rid, "rc": int(rc),
            "status": "PASS" if int(rc) == 0 else "FAIL",
            "fail_lines": fails[:8],
            "lines": lines[-12:],
        })

    # [2026-09-08] CLOSED 는 실패가 아니다. 집계에서 분리한다.
    closed = [r for r in out["rounds"] if r["status"] == "CLOSED"]
    bad = [r for r in out["rounds"] if r["status"] not in ("PASS", "CLOSED")]
    out["status"] = "FAIL" if bad else "PASS"
    out["counts"] = {"total": len(out["rounds"]),
                     "pass": len(out["rounds"]) - len(bad) - len(closed),
                     "closed": len(closed),
                     "fail": len(bad)}
    return out


def build_alert_text(out: Dict[str, Any]) -> str | None:
    st = out.get("status")
    if st == "PASS":
        return None
    lines = [f"[연구 라운드 기준선] {out['generated_at']} {st}"]
    if st in ("NO_ROUNDS", "NO_TOOL"):
        lines.append(f"  {out.get('reason')}")
        return "\n".join(lines)
    c = out.get("counts") or {}
    lines.append(f"라운드 {c.get('total')}개 중 FAIL {c.get('fail')}개")
    for r in out["rounds"]:
        if r["status"] == "PASS":
            continue
        lines.append(f"■ {r['round_id']} (rc={r['rc']})")
        for ln in (r.get("fail_lines") or r.get("lines") or [])[:4]:
            lines.append(f"   {ln}")
    lines.append("기준선이 움직이면 그 라운드의 측정은 비교 불가가 된다.")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    out = run_all()

    d = Path(args.out_dir)
    d.mkdir(parents=True, exist_ok=True)
    body = json.dumps(out, ensure_ascii=False, indent=2)
    (d / f"round_preflight_daily_{_now():%Y%m%d_%H%M%S}.json").write_text(body, encoding="utf-8")
    (d / "round_preflight_daily_latest.json").write_text(body, encoding="utf-8")

    c = out.get("counts") or {}
    print(f"[ROUND_CHECK] status={out['status']} 라운드={c.get('total', 0)} "
          f"PASS={c.get('pass', 0)} FAIL={c.get('fail', 0)}")
    for r in out.get("rounds", []):
        print(f"  {r['status']:<5} {r['round_id']:<28} rc={r['rc']}")
        for ln in (r.get("fail_lines") or [])[:4]:
            print(f"        {ln}")
    if out["status"] in ("NO_ROUNDS", "NO_TOOL"):
        print(f"        {out.get('reason')}")

    text = build_alert_text(out)
    if text and not args.no_alert:
        try:
            sys.path.insert(0, str(TOOLS_DIR))
            from notify_channels import send_alert  # type: ignore

            res = send_alert(text, level="error", extra={"source": "round_preflight_daily"},
                             cooldown_sec=12 * 3600)
            print(f"[ROUND_CHECK] alert ok={res.get('ok')} suppressed={res.get('suppressed')}")
        except Exception as exc:
            print(f"[ROUND_CHECK] alert_failed {type(exc).__name__}: {exc}")

    return 2 if (args.strict and out["status"] != "PASS") else 0


if __name__ == "__main__":
    raise SystemExit(main())
