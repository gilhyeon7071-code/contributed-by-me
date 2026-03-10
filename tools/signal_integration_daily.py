from __future__ import annotations

import json
import re
from bisect import bisect_right
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
CACHE = ROOT / "_cache"
PAPER = ROOT / "paper"

JOINED = LOGS / "joined_trades_latest.csv"
JOINED_FINAL = LOGS / "joined_trades_final_latest.csv"
PHASE2 = LOGS / "phase2_sector_daily_stub.csv"
PHASE3_REGIME = LOGS / "phase3_regime_daily_stub.csv"
PHASE3_NEWS = LOGS / "phase3_news_daily_stub.csv"

TRADES = PAPER / "trades.csv"
SECTOR_SSOT = CACHE / "sector_ssot.csv"
SECTOR_CODE_MAP = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"
NEWS_SCORE_STATUS_LATEST = LOGS / "news_score_status_latest.json"
CAND_WITH_NEWS_SCORE = LOGS / "candidates_latest_data.with_news_score.csv"


def _read_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except Exception:
            continue
    return pd.read_csv(path, **kwargs)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _safe_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code6(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""



def _news_gate_from_status() -> Dict[str, Any]:
    obj = _safe_json(NEWS_SCORE_STATUS_LATEST) or {}
    quality = str(obj.get("quality") or "FAIL").upper()
    reason = str((obj.get("meta") or {}).get("reason") or obj.get("reason") or "")
    mapped_rate = float(obj.get("mapped_rate") or 0.0)
    nonzero_rate = float(obj.get("nonzero_rate") or 0.0)

    gate_open = bool(
        quality == "PASS"
        and mapped_rate >= 0.80
        and nonzero_rate >= 0.20
        and reason not in {"signals_stale_lag_3d", "signals_stale_lag_4d"}
        and not str(reason).startswith("signals_stale_lag_")
    )

    return {
        "gate_open": gate_open,
        "quality": quality,
        "reason": reason,
        "mapped_rate": mapped_rate,
        "nonzero_rate": nonzero_rate,
        "asof_ymd": str(obj.get("asof_ymd") or ""),
    }


def _load_latest_news_score_map() -> Dict[str, float]:
    if not CAND_WITH_NEWS_SCORE.exists():
        return {}
    try:
        df = _read_csv(CAND_WITH_NEWS_SCORE, dtype={"code": str})
    except Exception:
        return {}
    if "code" not in df.columns or "news_score" not in df.columns:
        return {}

    x = df.copy()
    x["code"] = x["code"].map(_norm_code6)
    x["news_score"] = pd.to_numeric(x["news_score"], errors="coerce")
    x = x.dropna(subset=["news_score"])
    x = x[x["code"] != ""]
    if x.empty:
        return {}

    out: Dict[str, float] = {}
    for _, r in x.iterrows():
        out[str(r["code"])] = float(r["news_score"])
    return out

def _regime_score(risk_on: bool, regime: str) -> float:
    r = str(regime or "").upper()
    if not bool(risk_on):
        return -0.20
    if r in {"CRASH", "RISK_OFF", "BEAR"}:
        return -0.10
    if r in {"NORMAL", "BULL"}:
        return 0.20
    return 0.10


def main() -> int:
    if not JOINED.exists():
        raise SystemExit(f"[FATAL] missing {JOINED}")
    if not TRADES.exists():
        raise SystemExit(f"[FATAL] missing {TRADES}")

    joined = _read_csv(JOINED, dtype={"code": str, "trade_id": str})
    trades = _read_csv(TRADES, dtype={"code": str, "trade_id": str})

    # backup output targets
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if JOINED.exists():
        JOINED.with_suffix(JOINED.suffix + f".bak_signal_integ_{ts}").write_text(JOINED.read_text(encoding="utf-8"), encoding="utf-8")
    if JOINED_FINAL.exists():
        JOINED_FINAL.with_suffix(JOINED_FINAL.suffix + f".bak_signal_integ_{ts}").write_text(JOINED_FINAL.read_text(encoding="utf-8"), encoding="utf-8")

    # normalize base fields
    joined["code"] = joined["code"].map(_norm_code6)
    if "date" in joined.columns:
        joined["date"] = joined["date"].map(_norm_date8)
    else:
        joined["date"] = joined.get("exit_date", "").map(_norm_date8)

    # sync pnl_krw from trades SSOT
    if "pnl_krw" not in joined.columns:
        joined["pnl_krw"] = ""
    tr_map = {str(r.get("trade_id", "")): r.get("pnl_krw", "") for _, r in trades.iterrows()}
    joined["trade_id"] = joined["trade_id"].astype(str)
    joined["pnl_krw"] = joined["trade_id"].map(lambda x: tr_map.get(str(x), ""))

    # sector maps
    ssot = _read_csv(SECTOR_SSOT, dtype={"code": str}) if SECTOR_SSOT.exists() else pd.DataFrame(columns=["code", "krx_sector"])
    scmap = _read_csv(SECTOR_CODE_MAP, dtype={"krx_sector": str, "sector_code": str}) if SECTOR_CODE_MAP.exists() else pd.DataFrame(columns=["krx_sector", "sector_code"])

    if "code" in ssot.columns:
        ssot["code"] = ssot["code"].map(_norm_code6)
    if "krx_sector" not in ssot.columns:
        ssot["krx_sector"] = ""
    scmap["krx_sector"] = scmap.get("krx_sector", "").astype(str).str.strip()
    scmap["sector_code"] = scmap.get("sector_code", "000").astype(str).str.strip()
    sec_to_code = dict(zip(scmap["krx_sector"], scmap["sector_code"]))

    ssot["krx_sector"] = ssot["krx_sector"].astype(str).str.strip()
    ssot["sector_code"] = ssot["krx_sector"].map(lambda x: sec_to_code.get(x, "000"))
    code_to_sector = dict(zip(ssot["code"], ssot["krx_sector"]))
    code_to_sector_code = dict(zip(ssot["code"], ssot["sector_code"]))

    if "sector" not in joined.columns:
        joined["sector"] = ""
    joined["sector"] = joined.apply(
        lambda r: str(r.get("sector") or "").strip() or str(code_to_sector.get(str(r.get("code", "")), "")),
        axis=1,
    )
    joined["sector_code"] = joined["code"].map(lambda c: code_to_sector_code.get(str(c), "000"))

    # build sector score history from candidates_v41_1_*.csv
    cand_files = sorted(LOGS.glob("candidates_v41_1_*.csv"))
    hist: Dict[str, List[Tuple[str, float]]] = {}
    for p in cand_files:
        m = re.search(r"(\d{8})", p.stem)
        if not m:
            continue
        d8 = m.group(1)
        try:
            cdf = _read_csv(p, dtype={"code": str})
        except Exception:
            continue
        if "code" not in cdf.columns or "score" not in cdf.columns:
            continue

        cdf = cdf.copy()
        cdf["code"] = cdf["code"].map(_norm_code6)
        cdf["score"] = pd.to_numeric(cdf["score"], errors="coerce")
        cdf = cdf.dropna(subset=["score"])
        if cdf.empty:
            continue

        cdf["sector_code"] = cdf["code"].map(lambda x: code_to_sector_code.get(str(x), "000"))
        g = cdf.groupby("sector_code", as_index=False)["score"].max()
        mx = float(g["score"].max()) if len(g) else 0.0
        if mx <= 0:
            continue
        g["sector_score"] = (g["score"] / mx).clip(lower=0.0, upper=1.0)

        for _, rr in g.iterrows():
            sc = str(rr.get("sector_code", "000") or "000")
            sv = float(rr.get("sector_score", 0.0) or 0.0)
            hist.setdefault(sc, []).append((d8, sv))

    for sc in hist.keys():
        hist[sc] = sorted(hist[sc], key=lambda x: x[0])

    def sector_asof(date8: str, sector_code: str) -> Tuple[float, str, str]:
        arr = hist.get(str(sector_code or "000"), [])
        if not arr:
            return 0.0, "", "FAIL_SOFT"
        dates = [d for d, _ in arr]
        i = bisect_right(dates, str(date8)) - 1
        if i < 0:
            return 0.0, "", "FAIL_SOFT"
        d, v = arr[i]
        return float(v), d, "SECTOR_ASOF"

    ss_list: List[float] = []
    ss_src: List[str] = []
    ss_asof: List[str] = []
    for _, r in joined.iterrows():
        sc = str(r.get("sector_code", "000") or "000")
        d8 = str(r.get("date", "") or "")
        v, dsrc, src = sector_asof(d8, sc)
        ss_list.append(round(v, 6))
        ss_asof.append(dsrc)
        ss_src.append(src)
    joined["sector_score"] = ss_list
    joined["sector_asof_date"] = ss_asof
    joined["sector_source"] = ss_src

    # regime as-of from macro_signal_*.json
    macro_points: List[Tuple[str, bool, str]] = []
    for p in sorted(LOGS.glob("macro_signal_*.json")):
        if p.name == "macro_signal_latest.json":
            continue
        obj = _safe_json(p) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        if not d8:
            continue
        ro = bool(obj.get("risk_on", False))
        rg = str(obj.get("regime", "NORMAL") or "NORMAL")
        macro_points.append((d8, ro, rg))
    macro_points = sorted(macro_points, key=lambda x: x[0])

    def regime_asof(date8: str) -> Tuple[str, float, str]:
        if not macro_points:
            rg = "FAIL_SOFT"
            return rg, 0.0, "FAIL_SOFT"
        dates = [d for d, _, _ in macro_points]
        i = bisect_right(dates, str(date8)) - 1
        if i >= 0:
            d, ro, rg = macro_points[i]
            return rg, _regime_score(ro, rg), "MACRO_ASOF"
        # if no backward point, use earliest known macro as forward-fill fallback
        d, ro, rg = macro_points[0]
        return rg, _regime_score(ro, rg), "MACRO_FORWARD_FILL"

    reg_list: List[str] = []
    reg_score: List[float] = []
    reg_src: List[str] = []
    for _, r in joined.iterrows():
        rg, rv, src = regime_asof(str(r.get("date", "") or ""))
        reg_list.append(rg)
        reg_score.append(round(float(rv), 6))
        reg_src.append(src)
    joined["regime"] = reg_list
    joined["regime_score"] = reg_score
    joined["source"] = reg_src

    # news stays fail-soft unless existing non-zero feed exists
    if "news_score" not in joined.columns:
        joined["news_score"] = 0.0
    if "news_sentiment" not in joined.columns:
        joined["news_sentiment"] = 0.0
    if "news_source" not in joined.columns:
        joined["news_source"] = "FAIL_SOFT"
    joined["news_score"] = pd.to_numeric(joined["news_score"], errors="coerce").fillna(0.0)
    joined["news_sentiment"] = pd.to_numeric(joined["news_sentiment"], errors="coerce").fillna(0.0)
    joined["news_source"] = joined["news_source"].fillna("FAIL_SOFT").astype(str)
    news_nonzero_input = int((joined["news_score"] != 0).sum())
    news_backfill_source = "joined_existing"

    # Backfill from latest candidate news scores when joined feed is empty.
    if news_nonzero_input == 0:
        cand_news_map = _load_latest_news_score_map()
        if cand_news_map:
            joined["news_score"] = joined["code"].map(lambda c: float(cand_news_map.get(str(c), 0.0)))
            joined["news_sentiment"] = joined["news_score"]
            joined["news_source"] = joined.apply(
                lambda r: "CAND_LATEST" if str(r.get("code") or "") in cand_news_map else str(r.get("news_source") or "FAIL_SOFT"),
                axis=1,
            )
            news_nonzero_input = int((pd.to_numeric(joined["news_score"], errors="coerce").fillna(0.0) != 0).sum())
            news_backfill_source = "candidates_latest_data.with_news_score.csv"
        else:
            news_backfill_source = "none"

    # final score blend (as-of) with quality gate on news leg
    news_gate = _news_gate_from_status()
    news_gate_effective = bool(news_gate.get("gate_open")) and (news_nonzero_input > 0)
    if news_gate_effective:
        w_sector, w_regime, w_news = 0.60, 0.30, 0.10
        blend_policy = "ASOF_BLEND_NEWS_ON"
    else:
        w_sector, w_regime, w_news = 0.65, 0.35, 0.00
        blend_policy = "ASOF_BLEND_NEWS_OFF"

    joined["news_gate"] = "OPEN" if news_gate_effective else "CLOSED"
    joined["final_score"] = (
        pd.to_numeric(joined["sector_score"], errors="coerce").fillna(0.0) * w_sector
        + pd.to_numeric(joined["regime_score"], errors="coerce").fillna(0.0) * w_regime
        + pd.to_numeric(joined["news_score"], errors="coerce").fillna(0.0) * w_news
    ).round(6)
    joined["final_score_source"] = joined["final_score"].map(
        lambda v: blend_policy if abs(float(v)) > 1e-12 else "FAIL_SOFT"
    )

    # write phase2/3 stubs
    phase2_df = joined[["date", "code", "sector", "sector_score"]].copy()
    _write_csv(PHASE2, phase2_df)

    phase3_regime_df = (
        joined[["date", "regime", "regime_score", "source"]]
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    _write_csv(PHASE3_REGIME, phase3_regime_df)

    phase3_news_df = joined[["date", "code", "news_score", "news_sentiment", "news_source"]].copy()
    _write_csv(PHASE3_NEWS, phase3_news_df)

    # write joined latest (pre-final schema)
    latest_cols = [
        "trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "pnl_krw",
        "exit_reason", "note", "sell_oid", "order_id", "qty", "pnl_krw_net", "date", "sector", "sector_score",
        "regime", "regime_score", "source", "news_score", "news_sentiment", "news_source", "news_gate",
    ]
    for c in latest_cols:
        if c not in joined.columns:
            joined[c] = ""
    _write_csv(JOINED, joined[latest_cols].copy())

    # write final
    final_cols = latest_cols + ["final_score", "final_score_source", "sector_code"]
    for c in final_cols:
        if c not in joined.columns:
            joined[c] = ""
    final_df = joined[final_cols].copy()
    _write_csv(JOINED_FINAL, final_df)

    # update signal integration status
    asof_ymd = datetime.now().strftime("%Y%m%d")
    st_path = LOGS / f"signal_integration_status_{asof_ymd}.json"

    sector_nonzero = int((pd.to_numeric(final_df["sector_score"], errors="coerce").fillna(0.0) != 0).sum())
    regime_nonzero = int((pd.to_numeric(final_df["regime_score"], errors="coerce").fillna(0.0) != 0).sum())
    news_nonzero = int((pd.to_numeric(final_df["news_score"], errors="coerce").fillna(0.0) != 0).sum())
    final_nonzero = int((pd.to_numeric(final_df["final_score"], errors="coerce").fillna(0.0) != 0).sum())

    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "phase2_sector": {
            "join_ready": "PASS",
            "score_fill": "PASS" if sector_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": sector_nonzero,
        },
        "phase3_regime": {
            "join_ready": "PASS",
            "score_fill": "PASS" if regime_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": regime_nonzero,
        },
        "phase3_news": {
            "join_ready": "PASS",
            "score_fill": "PASS" if news_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": news_nonzero,
            "gate": "OPEN" if news_gate_effective else "CLOSED",
            "input_nonzero_rows": news_nonzero_input,
            "backfill_source": news_backfill_source,
            "quality": str(news_gate.get("quality") or "FAIL"),
            "reason": str(news_gate.get("reason") or ""),
            "mapped_rate": float(news_gate.get("mapped_rate") or 0.0),
            "nonzero_rate": float(news_gate.get("nonzero_rate") or 0.0),
        },
        "final_score_merge": {
            "join_ready": "PASS",
            "score_fill": "PASS" if final_nonzero > 0 else "FAIL_SOFT_0",
            "policy": blend_policy if final_nonzero > 0 else "FAIL_SOFT_0",
            "weights": {"sector": w_sector, "regime": w_regime, "news": w_news},
            "news_gate": "OPEN" if news_gate_effective else "CLOSED",
            "nonzero_rows": final_nonzero,
        },
        "artifacts": {
            "joined_latest": str(JOINED),
            "joined_final_latest": str(JOINED_FINAL),
            "phase2_sector": str(PHASE2),
            "phase3_regime": str(PHASE3_REGIME),
            "phase3_news": str(PHASE3_NEWS),
        },
    }
    st_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[SIGNAL_INTEGRATION] wrote {JOINED}")
    print(f"[SIGNAL_INTEGRATION] wrote {JOINED_FINAL}")
    print(f"[SIGNAL_INTEGRATION] final_nonzero={final_nonzero}/{len(final_df)}")
    print(f"[SIGNAL_INTEGRATION] sector_nonzero={sector_nonzero} regime_nonzero={regime_nonzero} news_nonzero={news_nonzero}")
    print(f"[SIGNAL_INTEGRATION] status={st_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
