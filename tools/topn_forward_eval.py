# -*- coding: utf-8 -*-
"""전진 원장의 선택을 **사후 수익률에 붙여 평가한다.**

[2026-09-13] 사용자 지적: *"현재상태가 궁금해 하고는있는거같은데 보이는것은 없고"*.

왜 없었나 - 실측
  `2_Logs/topn/forward_ledger.csv` 에 10거래일 600행(arm x rank x code)이 쌓여 있는데,
  그 파일을 읽는 코드 넷이 **전부 일수만 센다**:
    topn_build_orders.py        있나 없나 (없으면 STOP)
    topn_candidates.py          오늘 것을 덧붙임
    build_status_digest.py      c["date"].nunique()
    artifact_freshness_guard.py 나이만
  원장에 수익률 컬럼도 없다(date, arm, rank, code, score).
  **모으기만 하고 아무도 안 읽는다.** 그래서 보이는 것이 없었다.

무엇을 하나
  선택일 종가 -> h일 뒤 종가의 수익률을 붙이고 **arm 별로 비교**한다.
  ARM_SCORE 가 ARM_RANDOM / ARM_LIQ 보다 나은가 - 이것은 **체결과 무관**하다.
  선택 기구만 보는 것이라 승인·발주 없이 오늘 답할 수 있다.

무엇을 하지 않나
  합격/불합격 판정. 이 라운드의 주지표는 사전등록서(A3/A4/A5)에 있고 전진 운용이 필요하다.
  여기서 내는 것은 **관측**이지 판정이 아니다. 표본이 작다는 사실을 항상 같이 낸다.

    python tools/topn_forward_eval.py                 # h=10 기본
    python tools/topn_forward_eval.py --horizons 1,5,10
    python tools/topn_forward_eval.py --json --out-dir <경로>
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
LOG_DIR = ROOT / "2_Logs"
LEDGER = LOG_DIR / "topn" / "forward_ledger.csv"
OUT_NAME = "topn_forward_eval_latest.json"

# 왕복 비용. 권위 원장 trades_calc.csv (사용자 승인). **가정하지 않고 표기만 한다**
ROUNDTRIP_COST_PCT = 0.00358


def _panel() -> pd.DataFrame:
    from tools.load_merged_panel import load_merged
    d = load_merged(cols=["date", "code", "close"])
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["date"] = d["date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    d = d[(d["close"].astype(float) > 0)]
    return d


def _ledger() -> pd.DataFrame:
    rows = pd.read_csv(LEDGER, dtype=str, encoding="utf-8-sig")
    rows["code"] = rows["code"].astype(str).str.zfill(6)
    rows["date"] = rows["date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    return rows


def forward_returns(horizons: List[int]) -> Dict[str, Any]:
    led = _ledger()
    pan = _panel()

    # 종목별 날짜순 종가 시계열에서 h거래일 뒤 종가를 붙인다
    pan = pan.sort_values(["code", "date"]).reset_index(drop=True)
    pan["close"] = pan["close"].astype(float)
    for h in horizons:
        pan["fwd_%d" % h] = pan.groupby("code")["close"].shift(-h)

    merged = led.merge(pan, on=["code", "date"], how="left")
    out: Dict[str, Any] = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "ledger_rows": int(len(led)),
        "ledger_days": int(led["date"].nunique()),
        "ledger_range": [str(led["date"].min()), str(led["date"].max())],
        "matched_rows": int(merged["close"].notna().sum()),
        "roundtrip_cost_pct": ROUNDTRIP_COST_PCT,
        "horizons": {},
    }
    for h in horizons:
        col = "fwd_%d" % h
        m = merged[merged[col].notna()].copy()
        if m.empty:
            out["horizons"][str(h)] = {"note": "만기 도달 표본 없음 (원장 %d일, h=%d)"
                                               % (out["ledger_days"], h)}
            continue
        m["ret"] = m[col] / m["close"] - 1.0

        # **신호일별로 먼저 평균내고 그 평균들을 평균한다.**
        #   종목-일 단위로 바로 평균하면 같은 날 종목들이 함께 움직인 것을
        #   독립 표본으로 세어 유의성을 과대평가한다 (일 클러스터).
        per_day = m.groupby(["date", "arm"])["ret"].mean().reset_index()
        arms: Dict[str, Any] = {}
        for arm, g in per_day.groupby("arm"):
            arms[str(arm)] = {
                "signal_days": int(len(g)),
                "mean_ret_pct": round(float(g["ret"].mean()) * 100.0, 4),
                "sd_day_pct": round(float(g["ret"].std(ddof=1)) * 100.0, 4) if len(g) > 1 else None,
                "rows": int((m["arm"] == arm).sum()),
            }
        base = arms.get("ARM_RANDOM", {}).get("mean_ret_pct")
        liq = arms.get("ARM_LIQ", {}).get("mean_ret_pct")
        sc = arms.get("ARM_SCORE", {}).get("mean_ret_pct")
        out["horizons"][str(h)] = {
            "arms": arms,
            "score_minus_random_pct": (round(sc - base, 4) if sc is not None and base is not None else None),
            "score_minus_liq_pct": (round(sc - liq, 4) if sc is not None and liq is not None else None),
            "signal_days": int(per_day["date"].nunique()),
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", default="1,5,10")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    horizons = [int(x) for x in str(args.horizons).split(",") if str(x).strip().isdigit()]
    if not LEDGER.is_file():
        print("[ERR] 전진 원장이 없다: %s" % LEDGER)
        return 2

    payload = forward_returns(horizons)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / OUT_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print("=" * 76)
    print(" 전진 선택 평가   원장 %d행 / %d거래일  %s ~ %s"
          % (payload["ledger_rows"], payload["ledger_days"],
             payload["ledger_range"][0], payload["ledger_range"][1]))
    print("=" * 76)
    print("  가격 매칭 %d행 · 왕복비용 %.3f%% (차감 전 수치다)"
          % (payload["matched_rows"], ROUNDTRIP_COST_PCT * 100))
    for h in horizons:
        blk = payload["horizons"].get(str(h)) or {}
        print("-" * 76)
        if "note" in blk:
            print("  h=%-3d %s" % (h, blk["note"]))
            continue
        print("  h=%-3d 신호일 %d" % (h, blk.get("signal_days", 0)))
        for arm in ("ARM_SCORE", "ARM_LIQ", "ARM_RANDOM"):
            a = (blk.get("arms") or {}).get(arm)
            if not a:
                continue
            print("        %-12s 평균 %+7.3f%%  일별sd %-8s  신호일 %-3d  행 %d"
                  % (arm, a["mean_ret_pct"],
                     ("%.3f" % a["sd_day_pct"]) if a["sd_day_pct"] is not None else "-",
                     a["signal_days"], a["rows"]))
        sr = blk.get("score_minus_random_pct")
        sl = blk.get("score_minus_liq_pct")
        if sr is not None:
            print("        SCORE - RANDOM = %+.3f%%p   SCORE - LIQ = %s"
                  % (sr, ("%+.3f%%p" % sl) if sl is not None else "-"))
    print("-" * 76)
    print("  * 이것은 **관측이지 판정이 아니다.** 사전등록서의 주지표는 A3/A4/A5 이고")
    print("    전진 운용이 필요하다. 표본이 작으면 부호가 쉽게 뒤집힌다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
