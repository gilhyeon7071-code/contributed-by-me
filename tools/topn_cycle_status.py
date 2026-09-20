# -*- coding: utf-8 -*-
"""topn 장중 사이클의 **진행 단계**를 한 눈에 낸다.

[2026-09-12] 미결 대장 D6 / PLANS RootA (207) 보류 1.
재료는 (207) 에서 다 찾아뒀고 착수만 안 돼 있었다.

왜 "지금 무슨 단계인가" 만으로는 쓸모가 없나
  1회 사이클이 약 5~11초인데 주기는 10분이다. 화면을 3초마다 새로 그려도
  **99% 는 대기 중**이고 단계가 보이는 건 몇 초뿐이다. 그래서 (207) 에 적은 대로
  네 가지를 같이 낸다.

    현재      실행 중 <단계>  /  대기 중(다음 ~N분 뒤)  /  창 밖(오늘 종료)
    직전 회차  단계별 rc 와 시각·소요
    최근 이력  최근 N회의 시각·rc  (막힘이 언제부터인지 한 줄로)
    창 밖 구분 15:20 이후를 "멈췄다" 로 오독하지 않게 별도 상태

    python tools/topn_cycle_status.py
    python tools/topn_cycle_status.py --json
    python tools/topn_cycle_status.py --out-dir <경로>   # 격리 실행
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TOPN_LOG = LOG_DIR / "topn" / "run_topn_intraday.log"
HOLIDAYS = ROOT / "holidays.json"
OUT_NAME = "topn_cycle_status_latest.json"

# (207) 실측. 장중 3단계 / 저녁 2단계
STAGES_INTRADAY = ["topn_build_orders", "topn_dispatch", "topn_reconcile"]
KNOWN_STAGES = set(STAGES_INTRADAY) | {"topn_candidates"}

# 예약을 못 읽을 때만 쓰는 대비값. 2026-09-12 실측:
#   VIBE_TopN_Intraday  start 09:05  repeat PT10M  duration PT6H15M  -> 09:05~15:20
FALLBACK_WINDOW = {"start_hhmm": 905, "interval_min": 10, "end_hhmm": 1520}

_TS = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)")
_ISO_MIN = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?")


def _parse_ts(line: str) -> Optional[dt.datetime]:
    m = _TS.search(line)
    if not m:
        return None
    txt = m.group(1)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


def read_window() -> Dict[str, Any]:
    """예약작업에서 창을 **읽는다.** 하드코딩한 숫자는 예약이 바뀌면 거짓말이 된다."""
    ps = ("$t=Get-ScheduledTask -TaskName 'VIBE_TopN_Intraday' -ErrorAction Stop; "
          "$g=$t.Triggers[0]; "
          "'{0}|{1}|{2}' -f $g.StartBoundary,$g.Repetition.Interval,$g.Repetition.Duration")
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                           capture_output=True, text=True, timeout=30,
                           encoding="utf-8", errors="replace")
        raw = (r.stdout or "").strip()
        start, interval, duration = raw.split("|")
        hh, mm = int(start[11:13]), int(start[14:16])
        im = _ISO_MIN.search(interval)
        iv = int(im.group(1) or 0) * 60 + int(im.group(2) or 0)
        dm = _ISO_MIN.search(duration)
        dur_min = int(dm.group(1) or 0) * 60 + int(dm.group(2) or 0)
        end = dt.datetime(2000, 1, 1, hh, mm) + dt.timedelta(minutes=dur_min)
        return {"start_hhmm": hh * 100 + mm, "interval_min": iv,
                "end_hhmm": end.hour * 100 + end.minute, "source": "scheduled_task"}
    except Exception as exc:
        out = dict(FALLBACK_WINDOW)
        # **대비값을 쓴 사실을 감추지 않는다.** 예약이 사라져도 화면이 정상으로 보이면 안 된다
        out["source"] = "fallback(%s)" % type(exc).__name__
        return out


def _is_trading_day(d: dt.date) -> bool:
    if d.weekday() >= 5:
        return False
    try:
        obj = json.loads(HOLIDAYS.read_text(encoding="utf-8-sig"))
        vals = obj.get("holidays") if isinstance(obj, dict) else obj
        hs = {re.sub(r"\D", "", str(v))[:8] for v in (vals or [])}
    except Exception:
        hs = set()
    return d.strftime("%Y%m%d") not in hs


def parse_cycles(limit: int = 12) -> List[Dict[str, Any]]:
    """로그 끝에서부터 사이클을 끊어 읽는다. 파일이 크므로 통째로 파싱하지 않는다."""
    if not TOPN_LOG.is_file():
        return []
    with io.open(str(TOPN_LOG), "rb") as fh:
        fh.seek(0, 2)
        size = fh.tell()
        span = min(size, 400 * 1024)
        fh.seek(size - span)
        text = fh.read().decode("utf-8", errors="replace")
    lines = text.splitlines()
    if span < size and lines:
        lines = lines[1:]                      # 잘린 첫 줄은 버린다

    cycles: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None
    for ln in lines:
        st = ln.strip()
        if st.startswith("[START]"):
            cur = {"start": _parse_ts(st), "stages": [], "end": None,
                   "rc": None, "skipped": False, "fail": []}
            continue
        if cur is None:
            continue
        if st.startswith("[END]"):
            cur["end"] = _parse_ts(st)
            cur["skipped"] = " skipped " in st
            m = re.search(r"rc=(-?\d+)", st)
            cur["rc"] = int(m.group(1)) if m else (0 if cur["skipped"] else None)
            if cur["start"] and cur["end"]:
                cur["elapsed_sec"] = round((cur["end"] - cur["start"]).total_seconds(), 2)
            cycles.append(cur)
            cur = None
            continue
        for tag in ("[OK]", "[WAIT]", "[FAIL]", "[HARD_FAIL]"):
            if st.startswith(tag):
                body = st[len(tag):].strip()
                name = body.split()[0].rstrip(":") if body else ""
                # 단계 표지와 가드 문구를 가른다. 가드는 한국어 문장이다
                if name in KNOWN_STAGES:
                    cur["stages"].append({"stage": name, "tag": tag.strip("[]")})
                    if tag in ("[FAIL]", "[HARD_FAIL]"):
                        cur["fail"].append(name)
                break
    if cur is not None:
        # [2026-09-12] **진행 중인 사이클도 목록에 넣는다.** 예전에는 `[END]` 에서만
        #   append 해서 실행 중인 회차가 통째로 안 보였다 - 그런데 "RUNNING" 이야말로
        #   이 표시가 존재하는 이유다. 시험이 잡았다.
        cycles.append(cur)
    return cycles[-limit:]


def build(now: Optional[dt.datetime] = None) -> Dict[str, Any]:
    now = now or dt.datetime.now()
    win = read_window()
    cycles = parse_cycles()
    hhmm = now.hour * 100 + now.minute
    trading = _is_trading_day(now.date())

    last = cycles[-1] if cycles else None
    running = bool(last and last.get("end") is None)

    if not trading:
        state = "NON_TRADING_DAY"
        note = "거래일이 아니다. 사이클은 돌지만 전부 건너뛴다"
    elif hhmm < win["start_hhmm"]:
        state = "BEFORE_WINDOW"
        note = "창 시작 전 (%04d)" % win["start_hhmm"]
    elif hhmm >= win["end_hhmm"]:
        # **여기를 구분하지 않으면 "멈췄다" 로 오독된다** - (207) 의 지적
        state = "WINDOW_CLOSED"
        note = "오늘 사이클 종료 (창 %04d~%04d)" % (win["start_hhmm"], win["end_hhmm"])
    elif running:
        stg = last["stages"][-1]["stage"] if last and last["stages"] else "(시작)"
        state = "RUNNING"
        note = "실행 중: %s" % stg
    else:
        nxt = ""
        if last and last.get("start"):
            due = last["start"] + dt.timedelta(minutes=int(win["interval_min"] or 10))
            mins = max(0, int(round((due - now).total_seconds() / 60.0)))
            nxt = " (다음 ~%d분 뒤, 마지막 %s)" % (mins, last["start"].strftime("%H:%M"))
        state = "IDLE"
        note = "대기 중" + nxt

    def _fmt(c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "start": c["start"].strftime("%H:%M:%S") if c.get("start") else "",
            "rc": c.get("rc"),
            "skipped": bool(c.get("skipped")),
            "elapsed_sec": c.get("elapsed_sec"),
            "stages": [s["stage"] for s in c.get("stages") or []],
            "failed_stages": c.get("fail") or [],
        }

    real = [c for c in cycles if not c.get("skipped")]
    last_real = _fmt(real[-1]) if real else None
    return {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "state": state,
        "note": note,
        "trading_day": trading,
        "window": win,
        "expected_stages": STAGES_INTRADAY,
        "last_real_cycle": last_real,
        "recent": [_fmt(c) for c in cycles],
        "recent_fail_count": sum(1 for c in cycles
                                 if (c.get("rc") not in (0, None)) or c.get("fail")),
        "source_log": str(TOPN_LOG),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    payload = build()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / OUT_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print("=" * 72)
    print(" topn 장중 사이클   %s" % payload["generated_at"])
    print("=" * 72)
    print("  상태   %-16s %s" % (payload["state"], payload["note"]))
    w = payload["window"]
    print("  창     %04d~%04d  %d분 주기   (출처 %s)"
          % (w["start_hhmm"], w["end_hhmm"], w["interval_min"], w["source"]))
    lr = payload["last_real_cycle"]
    if lr:
        print("  직전   %s rc=%s %ss  단계 %s"
              % (lr["start"], lr["rc"], lr["elapsed_sec"],
                 "->".join(lr["stages"]) or "(없음)"))
        missing = [s for s in STAGES_INTRADAY if s not in lr["stages"]]
        if missing:
            print("         **빠진 단계** %s" % ", ".join(missing))
    else:
        print("  직전   실제로 돈 사이클이 최근 기록에 없다 (전부 건너뜀)")
    print("-" * 72)
    print("  최근 이력")
    for c in payload["recent"][-10:]:
        mark = "skip" if c["skipped"] else ("rc=%s" % c["rc"])
        print("    %-9s %-8s %-6s %s"
              % (c["start"], mark, c["elapsed_sec"], "->".join(c["stages"]) or ""))
    if payload["recent_fail_count"]:
        print("  * 최근 %d회에 실패 %d건"
              % (len(payload["recent"]), payload["recent_fail_count"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
