# -*- coding: utf-8 -*-
"""목표 포트폴리오의 스프레드 게이트 통과율을 측정해 이력에 쌓는다.

2026-08-25 신규. 실주문에서 유동성 하위·중위 종목이 `spread_bps > 30` 으로 막히는 것을
보고, 505종목 전체 중 얼마가 통과하는지 재기 위해 만들었다.

한 번 재고 끝낼 값이 아니다 - 통과율은 장중 시각·변동성에 따라 움직인다.
**며칠 모아 안정된 값을 얻은 뒤에야 운용 배제 비율을 정할 수 있다.**

전 종목을 조회하면 API 부담이 크므로 **유동성 10개 층에서 층당 N종목**을 뽑아
층별 통과율을 재고, 층 크기로 가중해 전체를 추정한다.

읽기 전용이다. 주문을 넣지 않는다.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
sys.path.insert(0, str(ROOT / "tools"))
from load_merged_panel import load_merged, add_vol60          # noqa: E402
from rebalance_portfolio import build_target                  # noqa: E402

HIST = ROOT / "2_Logs" / "measure" / "spread_passrate_history.csv"


def main() -> int:
    ap = argparse.ArgumentParser(description="스프레드 게이트 통과율 측정")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--exclude-pct", type=float, default=0.20)
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--per-band", type=int, default=6, help="유동성 층당 표본 수")
    ap.add_argument("--gate-bps", type=float, default=30.0)
    ap.add_argument("--full", action="store_true",
                    help="층화표본 대신 목표 전 종목을 조회한다(505종목 약 8분). "
                         "표본오차가 사라지고 시점 변동만 남는다")
    ap.add_argument("--out", default=str(HIST))
    args = ap.parse_args()

    d = add_vol60(load_merged())
    as_of = str(d["date"].max())
    target, meta = build_target(d, as_of, args.capital, args.exclude_pct, args.min_value)
    t = target.copy()
    t["band"] = pd.qcut(t["value"].rank(method="first"), 10, labels=False, duplicates="drop")

    if args.full:
        s = t.copy()
        mode = "full"
    else:
        picks = []
        for b, g in t.groupby("band"):
            picks.append(g.sample(min(args.per_band, len(g)), random_state=int(b) + 1))
        s = pd.concat(picks)
        mode = "sample"
    codes = ",".join(s["code"].tolist())
    print("[SPREAD] as_of=%s  목표 %d종목  조회 %d종목 (%s, %d층)"
          % (as_of, meta["target_count"], len(s), mode, t["band"].nunique()))
    if args.full:
        print("  전수 조회는 약 %d분 걸린다" % max(1, round(len(s) * 0.98 / 60)))

    tmp = Path(tempfile.gettempdir()) / ("spread_q_%s.csv" % dt.datetime.now().strftime("%H%M%S"))
    cmd = [sys.executable, str(ROOT / "tools" / "kis_quote_poll.py"),
           "--codes", codes, "--mock", "false", "--iterations", "1",
           "--interval-sec", "0.3", "--out-csv", str(tmp)]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=1800 if args.full else 600)
    if not tmp.exists():
        print("[STOP] 호가 조회 실패: %s" % (r.stderr or "")[-200:])
        return 2

    q = pd.read_csv(tmp, dtype=str, encoding="utf-8-sig")
    q["code"] = q["code"].astype(str).str.zfill(6)
    for c in ("ask1", "bid1"):
        q[c] = pd.to_numeric(q[c], errors="coerce")
    m = q.merge(s[["code", "value", "band"]], on="code", how="inner")
    ok = m[(m["status"] == "OK") & (m["ask1"] > 0) & (m["bid1"] > 0)].copy()
    if len(ok) < 10:
        print("[STOP] 유효 호가가 %d개뿐" % len(ok))
        return 2
    ok["mid"] = (ok["ask1"] + ok["bid1"]) / 2
    ok["spread_bps"] = (ok["ask1"] - ok["bid1"]) / ok["mid"] * 10000
    ok["pass"] = ok["spread_bps"] <= args.gate_bps

    # 층별 통과율을 층 크기로 가중해 전체 추정
    band_size = t.groupby("band").size()
    rows = []
    wsum = num = 0.0
    for b, g in ok.groupby("band"):
        p = float(g["pass"].mean())
        w = float(band_size.get(b, 0))
        wsum += w
        num += p * w
        rows.append((int(b), float(g["value"].median()), float(g["spread_bps"].median()), p, len(g)))
    overall = num / wsum if wsum else float("nan")

    print("  스프레드 중앙 %.1fbp  평균 %.1fbp  최대 %.1fbp"
          % (ok["spread_bps"].median(), ok["spread_bps"].mean(), ok["spread_bps"].max()))
    print("  %.0fbp 게이트 통과율(가중) %.1f%%  ->  %d종목 중 약 %.0f개"
          % (args.gate_bps, 100 * overall, meta["target_count"], meta["target_count"] * overall))
    print("  %5s %12s %12s %8s %6s" % ("층", "거래대금(억)", "중앙spread", "통과율", "표본"))
    for b, v, sp, p, n in sorted(rows):
        print("  %5d %12.0f %12.1f %7.0f%% %6d" % (b, v / 1e8, sp, 100 * p, n))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    new = not out.exists()
    with out.open("a", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["ts", "as_of", "mode", "gate_bps", "exclude_pct", "target_count",
                        "sample_n", "median_bps", "mean_bps", "max_bps",
                        "passrate_weighted", "est_pass_count"])
        w.writerow([dt.datetime.now().isoformat(timespec="seconds"), as_of, mode, args.gate_bps,
                    args.exclude_pct, meta["target_count"], len(ok),
                    round(float(ok["spread_bps"].median()), 2),
                    round(float(ok["spread_bps"].mean()), 2),
                    round(float(ok["spread_bps"].max()), 2),
                    round(overall, 4), int(round(meta["target_count"] * overall))])
    print("  appended %s" % out)
    try:
        tmp.unlink()
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
