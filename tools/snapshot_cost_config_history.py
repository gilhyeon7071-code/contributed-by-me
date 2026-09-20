# -*- coding: utf-8 -*-
"""설정 백업에서 **비용 관련 값의 이력**을 뽑아 날짜 없는 산출물로 굳힌다.

## 왜 필요한가

2026-09-10 에 C7(생산 손익이 왕복 1.400%)의 **창을 판정하려고** `paper/` 의
설정 백업 20여 개를 열어 `fee_pct` / `slippage_pct` / `sell_tax_pct` /
`tiered_slippage` 를 대조했다. 그 대조가 **터미널 출력으로만 존재했다.**

백업 파일은 언젠가 정리된다. 그러면 "언제부터 fee 가 0 이었나" 를
**다시는 답할 수 없게 된다.** 지금 굳혀둔다.

## 주의 — mtime 은 변경 시각이 아니다

`.bak` 파일은 복사할 때 **원본 mtime 을 보존**한다.
이름의 timestamp 는 **백업 시각**이지 값이 바뀐 시각이 아니다.
그래서 이 표로 구간 경계를 확정할 수 없다. **값의 목록**으로만 쓴다.
(2026-09-10 에 이걸 모르고 창을 단정했다가 철회했다 - PLANS (325))

## 산출물

```
2_Logs/cost_config_history_snapshot.csv    파일별 비용 값 (이름에 날짜 없음)
```

[2026-09-10] 신설. PLANS (322)(325)(326) / BROKEN_WINDOW_REGISTER C7·C8
"""
from __future__ import annotations

import glob
import io
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "2_Logs" / "cost_config_history_snapshot.csv"

KEYS = ("fee_pct", "slippage_pct", "sell_tax_pct")
TIER = ("enabled", "large_slip_pct", "mid_slip_pct", "small_slip_pct")


def main() -> int:
    rows = []
    pats = [str(ROOT / "paper" / "paper_engine_config*"),
            str(ROOT / "config" / "paper_engine_config*")]
    seen = set()
    for pat in pats:
        for p in glob.glob(pat):
            b = os.path.basename(p)
            if b in seen or ".lock.json" in b:
                continue
            seen.add(b)
            try:
                c = json.load(io.open(p, encoding="utf-8-sig"))
            except Exception:
                continue
            if not isinstance(c, dict) or "fee_pct" not in c:
                continue
            ts = c.get("tiered_slippage") or {}
            fee = c.get("fee_pct")
            slip = c.get("slippage_pct")
            tax = c.get("sell_tax_pct")
            try:
                rt = float(fee or 0.0) * 2 + float(slip or 0.0) * 2 + float(tax or 0.0)
            except Exception:
                rt = None
            rows.append({
                "file": b,
                "file_mtime": datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S"),
                "fee_pct": fee, "slippage_pct": slip, "sell_tax_pct": tax,
                "stated_roundtrip": rt,
                # C7: fee_pct 가 정확히 0.0 이면 옛 `or` 함정이 물었다
                "or_trap_would_bite": (fee is not None and float(fee) == 0.0),
                **{("tier_" + k): ts.get(k) for k in TIER},
            })
    if not rows:
        print("[WARN] 설정 백업을 찾지 못했어요")
        return 1
    df = pd.DataFrame(rows).sort_values("file_mtime").reset_index(drop=True)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("파일 %d개 -> %s" % (len(df), OUT.name))
    print()
    print("주의: .bak 은 복사 시 원본 mtime 을 보존해요. **mtime 은 변경 시각이 아니에요.**")
    print("      이름의 timestamp 도 백업 시각이지 값이 바뀐 시각이 아니에요.")
    print()
    show = df[df["file_mtime"] >= "2026-07-01"]
    print("%-56s %-9s %-8s %-8s %-9s %s" % ("파일", "fee", "slip", "tax", "왕복", "or함정"))
    for _i, r in show.iterrows():
        print("%-56s %-9s %-8s %-8s %-9s %s" % (
            str(r["file"])[:56], r["fee_pct"], r["slippage_pct"], r["sell_tax_pct"],
            ("%.3f%%" % (r["stated_roundtrip"] * 100)) if r["stated_roundtrip"] is not None else "-",
            "**문다**" if r["or_trap_would_bite"] else "안 뭄"))
    n_bite = int(df["or_trap_would_bite"].sum())
    print()
    print("fee_pct == 0.0 인 스냅샷: %d / %d  (그 시점엔 C7 함정이 물었다)" % (n_bite, len(df)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
