# -*- coding: utf-8 -*-
"""리밸런싱 장부의 일일 운영. 오전에 결정하고 저녁에 체결한다.

2026-08-25 신규. (109) 에서 장부를 개시했는데 **아무도 이어 돌리지 않았다** -
예약작업 19개, bat 84개 중 rebalance 를 부르는 것이 0개였다.

## 왜 오전/저녁 두 국면인가

선견을 넣지 않으려면 신호일과 집행일이 갈려야 한다.
```
오전(장 시작 전)  어제 종가로 목표를 정해 오늘 집행할 주문을 만든다
저녁(마감 후)     오늘 종가 아카이브가 들어오면 그 종가로 체결하고 성적을 낸다
```
저녁에 한 번에 하면 "오늘 종가를 보고 오늘 살 종목을 고르는" 것이 된다.
`rebalance_backtest_sim.py` 가 정확히 그렇게 돼 있고, 그래서 그 1.79배는 인용하지 않는다.

## 휴장일과 늦은 아카이브를 가른다

둘 다 "집행일이 지났는데 체결이 안 된 주문" 으로 보인다. 그러나 처리가 반대다.
```
집행일에 패널 자료가 있다  -> 자료만 늦게 온 것. 지금 체결한다(따라잡기).
집행일에 패널 자료가 없다  -> 그날 장이 안 섰다. `_stale/` 로 치운다.
```
아카이브는 대개 당일 18~21시에 들어오지만 20260821분은 다음 영업일 08:46 에 들어왔다.
둘을 안 가르면 멀쩡한 주문을 휴장일 산물로 오해해 버린다.

## 주기

`--step` 거래일마다 리밸런싱한다(기본 10, 시뮬과 같은 값).
마지막 체결일 이후 패널에 쌓인 거래일 수로 센다.

읽기+쓰기. `--apply` 없이는 무엇을 할지만 출력한다.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged                      # noqa: E402

DIR = ROOT / "2_Logs" / "rebalance"
STATE = DIR / "rebal_state.json"
PY = sys.executable


def panel_dates() -> list[str]:
    d = load_merged()
    return sorted(str(x) for x in d["date"].unique())


def history_dates() -> list[str]:
    if not STATE.exists():
        return []
    st = json.loads(STATE.read_text(encoding="utf-8"))
    return [str(h.get("exec_date")) for h in (st.get("history") or [])]


def orders_files() -> list[tuple[Path, str]]:
    """(경로, exec_date) 목록."""
    out = []
    for f in sorted(DIR.glob("orders_*_rebal.xlsx")):
        try:
            v = pd.read_excel(f, dtype=str, usecols=["exec_date"])["exec_date"].dropna().unique()
        except Exception:
            continue
        if len(v) == 1:
            out.append((f, str(v[0])))
    return out


def run(cmd: list[str], label: str) -> int:
    print("  $ %s" % " ".join(str(c) for c in cmd[1:]))
    r = subprocess.run(cmd, cwd=str(ROOT))
    print("  -> %s rc=%d" % (label, r.returncode))
    return r.returncode


def phase_morning(args, today: str, dates: list[str], hist: list[str]) -> int:
    p_last = dates[-1]
    if p_last >= today:
        print("[STOP] 패널 최신일이 %s 로 오늘 이후다. 오전 국면은 어제까지의 자료로 정한다." % p_last)
        return 2

    # 1) 집행일이 지났는데 체결 안 된 주문을 처리한다.
    #   [2026-08-25] 처음엔 전부 휴장일 산물로 보고 치웠는데 **틀렸다.**
    #   아카이브 도착이 불규칙하다 - 대개 당일 18~21시지만 20260821분은
    #   다음 영업일 08:46 에 들어왔다. 그런 주문은 유효한데 자료만 늦은 것이다.
    #   그래서 **집행일에 패널 자료가 있으면 지금 체결(따라잡기)**, 없을 때만 치운다.
    dset = set(dates)
    for f, e in [(f, e) for f, e in orders_files() if e < today and e not in hist]:
        if e in dset:
            print("[따라잡기] %s (집행일 %s 자료가 뒤늦게 들어왔다) -> 지금 체결" % (f.name, e))
            if args.apply:
                run([PY, str(ROOT / "tools" / "rebalance_paper_fill.py"),
                     "--orders", str(f), "--capital", str(args.capital), "--apply"], "체결")
                hist = history_dates()
        else:
            print("[청소] %s (집행일 %s 에 거래 자료 없음 = 휴장) -> _stale/" % (f.name, e))
            if args.apply:
                (DIR / "_stale").mkdir(exist_ok=True)
                shutil.move(str(f), str(DIR / "_stale" / f.name))

    # 2) 오늘 집행할 주문이 이미 있으면 끝
    if any(e == today for _, e in orders_files()):
        print("[SKIP] 오늘(%s) 집행할 주문이 이미 있다." % today)
        return 0

    # 3) 주기 판정
    if not hist:
        print("[STOP] 체결 이력이 없다. 개시 주문이 아직 안 채워졌다 - 저녁 국면부터 돌 것.")
        return 0
    last = max(hist)
    since = len([x for x in dates if x > last])          # 마지막 체결일 이후 거래일 수
    due = (since + 1) >= args.step                       # 오늘이 그 다음 집행일이 되는가
    print("[주기] 마지막 체결 %s, 이후 거래일 %d, 오늘이면 %d번째 (간격 %d)"
          % (last, since, since + 1, args.step))
    if not due:
        print("[SKIP] 아직 리밸런싱 날이 아니다. %d거래일 남음" % (args.step - since - 1))
        return 0

    print("[리밸런싱] 신호 %s -> 집행 %s" % (p_last, today))
    if not args.apply:
        print("  dry-run. --apply 를 주면 주문을 만든다.")
        return 0
    return run([PY, str(ROOT / "tools" / "rebalance_portfolio.py"),
                "--as-of", p_last, "--exec-date", today,
                "--capital", str(args.capital), "--exclude-pct", str(args.exclude_pct),
                "--min-value", str(args.min_value), "--apply"], "주문 생성")


def phase_evening(args, today: str, dates: list[str], hist: list[str]) -> int:
    if dates[-1] != today:
        print("[WAIT] 패널 최신일 %s != 오늘 %s. 아카이브가 아직 안 들어왔다." % (dates[-1], today))
        print("  저녁 국면은 아카이브가 들어온 뒤 다시 돌리면 된다.")
        return 3

    todo = [f for f, e in orders_files() if e == today and e not in hist]
    if not todo:
        print("[SKIP] 오늘(%s) 체결할 주문이 없다." % today)
    for f in todo:
        print("[체결] %s" % f.name)
        if not args.apply:
            print("  dry-run.")
            continue
        rc = run([PY, str(ROOT / "tools" / "rebalance_paper_fill.py"),
                  "--orders", str(f), "--capital", str(args.capital), "--apply"], "체결")
        if rc not in (0, 3):
            return rc

    print("[성적]")
    return run([PY, str(ROOT / "tools" / "rebalance_report.py"),
                "--capital", str(args.capital)], "보고서")


def main() -> int:
    ap = argparse.ArgumentParser(description="리밸런싱 장부 일일 운영")
    ap.add_argument("--phase", required=True, choices=["morning", "evening"])
    ap.add_argument("--step", type=int, default=10, help="리밸런싱 간격(거래일)")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--exclude-pct", type=float, default=0.20)
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--today", default="", help="YYYYMMDD. 시험용")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    today = args.today or dt.date.today().strftime("%Y%m%d")
    dates = panel_dates()
    hist = history_dates()
    print("[REBAL_DAILY] %s  오늘=%s  패널 최신=%s  체결이력 %d회%s"
          % (args.phase, today, dates[-1], len(hist), "" if args.apply else "   (dry-run)"))

    if args.phase == "morning":
        return phase_morning(args, today, dates, hist)
    return phase_evening(args, today, dates, hist)


if __name__ == "__main__":
    sys.exit(main())
