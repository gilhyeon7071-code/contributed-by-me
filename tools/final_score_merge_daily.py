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

IN_NEWS = LOGS / "candidates_latest_data.with_news_score.csv"
IN_SECTOR = LOGS / "candidates_latest_data.with_sector_score.csv"
IN_FILTERED = LOGS / "candidates_latest_data.filtered.csv"
IN_BASE = LOGS / "candidates_latest_data.csv"
OUT = LOGS / "candidates_latest_data.with_final_score.csv"

W_SECTOR = 0.65
W_REGIME = 0.35
W_NEWS = 0.0


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _safe_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _pick_input() -> Path:
    if IN_NEWS.exists():
        return IN_NEWS
    if IN_SECTOR.exists():
        return IN_SECTOR
    if IN_FILTERED.exists():
        return IN_FILTERED
    return IN_BASE


def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return datetime.now().strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    return str(d8.max()) if len(d8) else datetime.now().strftime("%Y%m%d")


def _regime_score(risk_on: bool, regime: str) -> float:
    r = str(regime or "").upper()
    if not bool(risk_on):
        return -0.20
    if r in {"CRASH", "RISK_OFF", "BEAR"}:
        return -0.10
    if r in {"NORMAL", "BULL"}:
        return 0.20
    return 0.10


def _load_macro_points() -> List[Tuple[str, bool, str]]:
    points: List[Tuple[str, bool, str]] = []
    for p in sorted(LOGS.glob("macro_signal_*.json")):
        obj = _safe_json(p) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        if not d8:
            continue
        ro = bool(obj.get("risk_on", False))
        rg = str(obj.get("regime", "NORMAL") or "NORMAL")
        points.append((d8, ro, rg))

    latest = LOGS / "macro_signal_latest.json"
    if latest.exists():
        obj = _safe_json(latest) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        if d8:
            ro = bool(obj.get("risk_on", False))
            rg = str(obj.get("regime", "NORMAL") or "NORMAL")
            points.append((d8, ro, rg))

    if not points:
        return []

    # dedupe by date (keep latest record per date)
    by_date: Dict[str, Tuple[bool, str]] = {}
    for d8, ro, rg in points:
        by_date[d8] = (ro, rg)

    out = [(d8, v[0], v[1]) for d8, v in by_date.items()]
    out = sorted(out, key=lambda x: x[0])
    return out


def _regime_asof(date8: str, points: List[Tuple[str, bool, str]]) -> Tuple[str, float, str]:
    if not points:
        return "FAIL_SOFT", 0.0, "FAIL_SOFT"
    dates = [d for d, _, _ in points]
    i = bisect_right(dates, str(date8)) - 1
    if i >= 0:
        d, ro, rg = points[i]
        return rg, _regime_score(ro, rg), "MACRO_ASOF"
    d, ro, rg = points[0]
    return rg, _regime_score(ro, rg), "MACRO_FORWARD_FILL"


def _row_date8(df: pd.DataFrame) -> pd.Series:
    if "date_yyyymmdd" in df.columns:
        return df["date_yyyymmdd"].astype(str).map(_norm_date8)
    if "date" in df.columns:
        return df["date"].astype(str).map(_norm_date8)
    if "signal_date" in df.columns:
        return df["signal_date"].astype(str).map(_norm_date8)
    return pd.Series([datetime.now().strftime("%Y%m%d")] * len(df), index=df.index)


def main() -> int:
    in_path = _pick_input()
    if not in_path.exists():
        raise SystemExit(f"[FATAL] missing input candidates file: {in_path}")

    df = _read_csv(in_path)
    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] missing code column: {in_path}")

    d8 = _row_date8(df)
    points = _load_macro_points()

    regimes: List[str] = []
    regime_scores: List[float] = []
    regime_sources: List[str] = []
    for v in d8.tolist():
        rg, rv, src = _regime_asof(str(v or ""), points)
        regimes.append(rg)
        regime_scores.append(float(rv))
        regime_sources.append(src)

    df["regime"] = regimes
    df["regime_score"] = regime_scores
    df["regime_source"] = regime_sources

    if "sector_score" not in df.columns:
        df["sector_score"] = 0.0
    if "news_score" not in df.columns:
        df["news_score"] = 0.0

    df["sector_score"] = pd.to_numeric(df["sector_score"], errors="coerce").fillna(0.0)
    df["news_score"] = pd.to_numeric(df["news_score"], errors="coerce").fillna(0.0)
    df["regime_score"] = pd.to_numeric(df["regime_score"], errors="coerce").fillna(0.0)

    df["final_score"] = (
        df["sector_score"] * float(W_SECTOR)
        + df["regime_score"] * float(W_REGIME)
        + df["news_score"] * float(W_NEWS)
    ).round(6)

    df["final_score_source"] = df["final_score"].map(lambda x: "ASOF_BLEND" if abs(float(x)) > 1e-12 else "FAIL_SOFT")

    _write_csv(OUT, df)

    asof_ymd = _max_date8(df)
    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": str(in_path),
        "output": str(OUT),
        "asof_ymd": asof_ymd,
        "rows": int(len(df)),
        "weights": {
            "sector": W_SECTOR,
            "regime": W_REGIME,
            "news": W_NEWS,
        },
        "nonzero_rows": {
            "sector": int((df["sector_score"] != 0).sum()),
            "regime": int((df["regime_score"] != 0).sum()),
            "news": int((df["news_score"] != 0).sum()),
            "final": int((df["final_score"] != 0).sum()),
        },
        "macro_points": int(len(points)),
    }

    st = LOGS / f"final_score_merge_status_{asof_ymd}.json"
    st_latest = LOGS / "final_score_merge_status_latest.json"
    st.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    st_latest.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[FINAL_SCORE] input={in_path.name} rows={len(df)} final_nonzero={status['nonzero_rows']['final']}")
    print(f"[FINAL_SCORE] wrote {OUT}")
    print(f"[FINAL_SCORE] status={st}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())