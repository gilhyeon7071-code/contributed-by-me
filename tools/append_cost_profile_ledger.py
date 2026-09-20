# -*- coding: utf-8 -*-
"""그 실행에서 **실제로 적용된 비용 프로파일**을 append-only 원장에 남긴다.

## 왜 필요한가 (2026-09-10)

`paper/trades.csv` 는 찍힌 순손익만 있고 **비용 분해가 없다.**
`paper/trades_calc.csv` 는 분해가 있지만 **오늘 설정으로 전체를 재계산**한 것이다
(631행 전부 fee 0.0 / slip 0.001 / tax 0.002 로 동일. 2025-12 거래도 그렇다).

그래서 **어느 파일도 "그때 실제로 적용된 비용" 을 말하지 않는다.**
그 결과 C7(생산 손익이 왕복 1.400%~2.204% 를 청구)이 몇 주간 보이지 않았다.
두 숫자가 매일 나란히 있었는데도 - 하나는 기록, 하나는 재계산이라 비교가 안 됐다.

실측: 408건 매칭 중 **405건**이 0.1%p 넘게 다르고 중앙 차이 **+2.69%p** 다.
      2026-08-24 거래 138610 은 한쪽이 -1.78%, 다른 쪽이 +0.03% 로 **부호까지 반대**다.

## 어떻게 푸나 - 거래마다가 아니라 실행마다

비용 프로파일은 **한 실행 안에서 상수**다. 그러니 거래 행 스키마를 건드리지 않고
**실행마다 한 줄**만 남기면 어느 거래든 그날 프로파일로 복원된다.

그리고 **말한 값과 실효값을 둘 다** 적는다. 둘이 갈리면 그것이 C7 이다.
(설정에 fee_pct: 0.0 이 있는데 접근자가 0.005 를 내던 일)

## 산출물

```
2_Logs/cost_profile_history_ledger.jsonl   append-only, 이름에 날짜 없음
```
`tools/log_cleanup_30d.py` 대상이 아니다 (`[[feedback_dated_artifacts_get_deleted]]`).

[2026-09-10] 신설. BROKEN_WINDOW_REGISTER C7·C8 / PLANS (322)(326)(334)
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

LEDGER = ROOT / "2_Logs" / "cost_profile_history_ledger.jsonl"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"


def _sha8(p: Path) -> str | None:
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()[:8]
    except Exception:
        return None


def build_row(note: str = "") -> dict:
    from paper_engine.config import load_config
    from pricing_engine import build_cost_profile, resolve_slippage_pct_tiered

    cfg = load_config()

    # (1) 설정이 **말하는** 값
    raw = {}
    try:
        raw = json.load(io.open(CFG_PATH, encoding="utf-8-sig"))
    except Exception:
        raw = {}
    stated = {
        "fee_pct": raw.get("fee_pct"),
        "slippage_pct": raw.get("slippage_pct"),
        "sell_tax_pct": raw.get("sell_tax_pct"),
    }
    try:
        stated_rt = (float(stated["fee_pct"] or 0.0) * 2
                     + float(stated["slippage_pct"] or 0.0) * 2
                     + float(stated["sell_tax_pct"] or 0.0))
    except Exception:
        stated_rt = None

    # (2) 엔진이 **실제로 쓰는** 값
    p = build_cost_profile(cfg)
    eff = {"fee_pct": float(p.fee_pct), "slippage_pct": float(p.slippage_pct),
           "sell_tax_pct": float(p.sell_tax_pct)}
    eff_rt = eff["fee_pct"] * 2 + eff["slippage_pct"] * 2 + eff["sell_tax_pct"]

    # (3) 티어 슬리피지 실효값 (C8)
    ts = cfg.get("tiered_slippage") or {}
    tier = {"enabled": bool(ts.get("enabled"))}
    if tier["enabled"]:
        for label, mc in (("large", 2e12), ("mid", 5e11), ("small", 1e10)):
            tier[label] = round(resolve_slippage_pct_tiered(mc, cfg, eff["slippage_pct"]), 6)

    # (4) 시뮬 상수
    default_fee = None
    try:
        import optimize_params_v41_1 as OPT
        default_fee = float(OPT.DEFAULT_FEE)
    except Exception:
        pass

    diverged = (stated_rt is not None and abs(stated_rt - eff_rt) > 1e-9)
    return {
        "ts": dt.datetime.now().isoformat(timespec="seconds"),
        "ymd": dt.date.today().strftime("%Y%m%d"),
        "stated": stated,
        "stated_roundtrip": stated_rt,
        "effective": eff,
        "effective_roundtrip": round(eff_rt, 6),
        # **이 줄이 C7 을 드러낸다.** 설정이 말하는 값과 엔진이 쓰는 값이 갈리면 True
        "diverged": bool(diverged),
        "tiered_slippage": tier,
        "sim_default_fee": default_fee,
        "config_sha8": _sha8(CFG_PATH),
        "note": note,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="실행별 실효 비용 프로파일 기록 (읽기 전용 + append)")
    ap.add_argument("--note", default="", help="이 실행에 붙일 메모")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    row = build_row(a.note)

    # 같은 값이 연속되면 줄이 무한히 쌓인다. **바뀔 때만** 쓴다(+ 하루 첫 실행은 항상).
    last = None
    if LEDGER.exists():
        try:
            for line in io.open(LEDGER, encoding="utf-8"):
                if line.strip():
                    last = json.loads(line)
        except Exception:
            last = None
    same = bool(last and last.get("ymd") == row["ymd"]
                and last.get("effective") == row["effective"]
                and last.get("tiered_slippage") == row["tiered_slippage"]
                and last.get("config_sha8") == row["config_sha8"])
    if same:
        if not a.quiet:
            print("[COST_PROFILE] 오늘 같은 값이 이미 기록돼 있어요. 건너뜁니다.")
        return 0

    with io.open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + chr(10))

    if not a.quiet:
        print("[COST_PROFILE] 기록 -> %s" % LEDGER.name)
        print("  설정 왕복 %.5f / 엔진 실효 왕복 %.5f%s"
              % (row["stated_roundtrip"] or -1, row["effective_roundtrip"],
                 "   **갈림(C7 형태)**" if row["diverged"] else ""))
        if row["tiered_slippage"].get("enabled"):
            t = row["tiered_slippage"]
            print("  티어 large %s / mid %s / small %s" % (t.get("large"), t.get("mid"), t.get("small")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
