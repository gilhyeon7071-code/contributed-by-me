"""H5 수급 — 외국인·기관이 많이 산 종목이 그 뒤에 나은가. **검정력을 먼저 계산하고** 잰다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.measure_flow_signal --k 5 [--top 0.1]

설계 (2026-09-18 설계 규칙에 맞춰 사건 창으로)
  신호   장 마감 뒤 나오는 그날의 외국인+기관 순매수 금액 ÷ 시총
  진입   **다음 거래일 종가** (신호는 마감 뒤에 알 수 있으니 당일에는 못 산다)
  청산   진입 + k 거래일 종가
  대조   같은 날 전체 수급 표본의 **중앙값 수익률** — 같은 날·같은 시장이라 시장 방향이 지워진다
  관측 단위 **날짜** (같은 날 종목들은 서로 겹친다)

  먼저 이 표본으로 **잡을 수 있는 최소 효과**를 계산하고, 그게 우리가 찾는 크기(1~3%p)보다 크면
  "판정 불가" 로 적고 끝낸다 — 재기 전에 버리는 게 규칙이다.

자료: `_cache/investor_flow/*.parquet` (2026-08-28 부터 매일 수집 중), 시총은 D1 산출 input csv
기업행위 오염은 |하루 31%| 초과를 빼서 거른다(원주가 계열이라).
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
FLOW_DIR = ROOT / "_cache" / "investor_flow"
LIMIT = 0.31


def load_flow(flow_dir: Path = FLOW_DIR) -> pd.DataFrame:
    frames = [pd.read_parquet(p) for p in sorted(Path(flow_dir).glob("*.parquet"))]
    if not frames:
        raise SystemExit(f"수급 자료가 없어요: {flow_dir}")
    df = pd.concat(frames, ignore_index=True)
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    for c in ("close", "forgn_amt", "inst_amt"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close", "forgn_amt", "inst_amt"])
    return df.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last")


def forward_returns(df: pd.DataFrame, k: int) -> pd.DataFrame:
    """진입 t+1 종가 → 청산 t+1+k 종가. 기업행위 의심(|하루 31%| 초과)은 뺀다."""
    out = []
    for code, g in df.groupby("code"):
        g = g.sort_values("date")
        closes = list(g["close"])
        dates = list(g["date"])
        for i in range(len(dates) - 1 - k):
            entry, exit_ = closes[i + 1], closes[i + 1 + k]
            if entry <= 0 or exit_ <= 0:
                continue
            path = [closes[j + 1] / closes[j] - 1 for j in range(i, i + 1 + k)]
            if any(abs(x) > LIMIT for x in path):
                continue
            out.append({"code": code, "signal_date": dates[i], "entry_date": dates[i + 1],
                        "ret": exit_ / entry - 1})
    return pd.DataFrame(out)


def measure(df: pd.DataFrame, caps: Dict[str, float], k: int, top: float) -> Dict[str, Any]:
    fr = forward_returns(df, k)
    if fr.empty:
        return {"status": "NO_DATA"}
    flow = df.copy()
    flow["net"] = (flow["forgn_amt"] + flow["inst_amt"]) * 1_000_000.0   # 단위: 백만원
    flow["cap"] = flow["code"].map(caps)
    flow = flow.dropna(subset=["cap"])
    flow["ratio"] = flow["net"] / flow["cap"]
    merged = fr.merge(flow[["code", "date", "ratio"]], left_on=["code", "signal_date"],
                      right_on=["code", "date"], how="inner")
    rows = []
    for d, g in merged.groupby("signal_date"):
        if len(g) < 30:
            continue
        cut = g["ratio"].quantile(1 - top)
        hi = g[g["ratio"] >= cut]["ret"]
        base = g["ret"].median()
        if len(hi) < 3:
            continue
        rows.append({"date": d, "n": len(g), "n_top": len(hi), "top_mean": float(hi.mean()),
                     "universe_median": float(base), "spread": float(hi.mean() - base)})
    spr = [r["spread"] for r in rows]
    if len(spr) < 2:
        return {"status": "TOO_FEW_DAYS", "days": len(spr)}
    pos = sum(1 for x in spr if x > 0)
    sd = st.pstdev(spr)
    mde = 2 * sd / math.sqrt(len(spr))
    tail = sum(math.comb(len(spr), i) for i in range(max(pos, len(spr) - pos), len(spr) + 1)) + \
        sum(math.comb(len(spr), i) for i in range(0, min(pos, len(spr) - pos) + 1))
    return {"status": "OK", "k": k, "top": top, "days": len(spr),
            "avg_universe_per_day": st.mean([r["n"] for r in rows]),
            "avg_top_per_day": st.mean([r["n_top"] for r in rows]),
            "spread_mean": st.mean(spr), "spread_median": st.median(spr),
            "spread_sd": sd, "positive_days": pos, "positive_ratio": pos / len(spr),
            "binom_p": min(1.0, tail / 2 ** len(spr)),
            "min_detectable_effect": mde,
            "days_needed_for_1pct": (2 * sd / 0.01) ** 2,
            "days_needed_for_2pct": (2 * sd / 0.02) ** 2}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True, type=Path, help="D1 input_YYYYMMDD.csv (code·market_cap)")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--top", type=float, default=0.1)
    ap.add_argument("--market", default="KOSPI", help="빈 값이면 전체 시장")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    uni = pd.read_csv(args.universe, dtype={"code": str})
    if args.market:                       # 우리가 살 수 있는 시장만 — 기본 KOSPI
        uni = uni[uni["market"] == args.market]
    caps = {str(c): float(m) for c, m in zip(uni["code"], uni["market_cap"]) if m and m > 0}
    res = measure(load_flow(), caps, args.k, args.top)
    res["measured_at"] = datetime.now().isoformat(timespec="seconds")
    if args.out:
        args.out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
