from __future__ import annotations

import json
import os
import re
from bisect import bisect_right
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import logging

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
CACHE = ROOT / "_cache"
PAPER = ROOT / "paper"

JOINED = LOGS / "joined_trades_latest.csv"
JOINED_FINAL = LOGS / "joined_trades_final_latest.csv"
PHASE2 = LOGS / "phase2_sector_daily_stub.csv"
PHASE3_REGIME = LOGS / "phase3_regime_daily_stub.csv"
PHASE3_NEWS = LOGS / "phase3_news_daily_stub.csv"
PHASE3_MICRO = LOGS / "phase3_micro_daily_stub.csv"

TRADES = PAPER / "trades.csv"
SECTOR_SSOT = CACHE / "sector_ssot.csv"
SECTOR_CODE_MAP = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"
NEWS_SCORE_STATUS_LATEST = LOGS / "news_score_status_latest.json"
CAND_WITH_SECTOR_SCORE = LOGS / "candidates_latest_data.with_sector_score.csv"
CAND_WITH_NEWS_SCORE = LOGS / "candidates_latest_data.with_news_score.csv"
DART_FUNDAMENTAL_LATEST = CACHE / "dart_fundamental_latest.csv"
DART_FUNDAMENTAL_META = CACHE / "dart_fundamental_meta.json"
KRX_WATCHLIST_LATEST = CACHE / "krx_watchlist_latest.csv"
KRX_WATCHLIST_META = CACHE / "krx_watchlist_meta.json"
MICRO_SIGNAL_LATEST = LOGS / "micro_signal_latest.json"
MICRO_GATE_MIN_MAPPED_RATE = 0.20
MACRO_FEATURE_LATEST = LOGS / "macro_feature_external_latest.json"
SECTOR_SCORE_HISTORY_CACHE = LOGS / "sector_score_history_cache_latest.json"




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
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


def _sector_scores_from_candidate_file(path: Path, code_to_sector_code: Dict[str, str]) -> Tuple[str, Dict[str, float]]:
    m = re.search(r"(\d{8})", path.stem)
    if not m:
        return "", {}
    d8 = m.group(1)
    try:
        cdf = _read_csv(path, dtype={"code": str})
    except Exception:
        return d8, {}
    if "code" not in cdf.columns or "score" not in cdf.columns:
        return d8, {}

    cdf = cdf.copy()
    cdf["code"] = cdf["code"].map(_norm_code6)
    cdf["score"] = pd.to_numeric(cdf["score"], errors="coerce")
    cdf = cdf.dropna(subset=["score"])
    if cdf.empty:
        return d8, {}

    cdf["sector_code"] = cdf["code"].map(lambda x: code_to_sector_code.get(str(x), "000"))
    g = cdf.groupby("sector_code", as_index=False)["score"].max()
    mx = float(g["score"].max()) if len(g) else 0.0
    if mx <= 0:
        return d8, {}
    g["sector_score"] = (g["score"] / mx).clip(lower=0.0, upper=1.0)
    score_map: Dict[str, float] = {}
    for _, rr in g.iterrows():
        sc = str(rr.get("sector_code", "000") or "000")
        score_map[sc] = float(rr.get("sector_score", 0.0) or 0.0)
    return d8, score_map


def _build_sector_score_history_incremental(code_to_sector_code: Dict[str, str]) -> Dict[str, List[Tuple[str, float]]]:
    cand_files = sorted(LOGS.glob("candidates_v41_1_*.csv"))
    ssot_mtime = int(SECTOR_SSOT.stat().st_mtime) if SECTOR_SSOT.exists() else 0
    scmap_mtime = int(SECTOR_CODE_MAP.stat().st_mtime) if SECTOR_CODE_MAP.exists() else 0

    cache = _safe_json(SECTOR_SCORE_HISTORY_CACHE) or {}
    cache_meta = cache.get("meta") if isinstance(cache.get("meta"), dict) else {}
    cache_files = cache.get("files") if isinstance(cache.get("files"), dict) else {}

    cache_valid = (
        int(cache_meta.get("ssot_mtime") or 0) == ssot_mtime
        and int(cache_meta.get("scmap_mtime") or 0) == scmap_mtime
    )
    if not cache_valid:
        cache_files = {}

    next_files: Dict[str, Any] = {}
    for p in cand_files:
        name = p.name
        mtime = int(p.stat().st_mtime)
        prev = cache_files.get(name) if isinstance(cache_files.get(name), dict) else {}
        if int(prev.get("mtime") or -1) == mtime and isinstance(prev.get("sector_scores"), dict):
            next_files[name] = prev
            continue

        d8, sector_scores = _sector_scores_from_candidate_file(p, code_to_sector_code)
        if not d8:
            continue
        next_files[name] = {"date8": d8, "mtime": mtime, "sector_scores": sector_scores}

    hist: Dict[str, List[Tuple[str, float]]] = {}
    rows: List[Tuple[str, Dict[str, float]]] = []
    for meta in next_files.values():
        if not isinstance(meta, dict):
            continue
        d8 = str(meta.get("date8") or "")
        scores = meta.get("sector_scores") if isinstance(meta.get("sector_scores"), dict) else {}
        if not d8:
            continue
        rows.append((d8, {str(k): float(v) for k, v in scores.items()}))
    rows.sort(key=lambda x: x[0])
    for d8, score_map in rows:
        for sc, sv in score_map.items():
            hist.setdefault(str(sc or "000"), []).append((d8, float(sv)))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "meta": {"ssot_mtime": ssot_mtime, "scmap_mtime": scmap_mtime},
        "files": next_files,
    }
    try:
        SECTOR_SCORE_HISTORY_CACHE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return hist


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code6(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _clean_text(v: Any) -> str:
    if v is None or pd.isna(v):
        return ""
    return str(v).strip()



def _news_gate_from_status() -> Dict[str, Any]:
    obj = _safe_json(NEWS_SCORE_STATUS_LATEST) or {}
    quality = str(obj.get("quality") or "FAIL").upper()
    reason = str((obj.get("meta") or {}).get("reason") or obj.get("reason") or "")
    mapped_rate = float(obj.get("mapped_rate") or 0.0)
    nonzero_rate = float(obj.get("nonzero_rate") or 0.0)
    gate_quality_levels = {
        x.strip().upper()
        for x in str(os.getenv("NEWS_GATE_QUALITY_LEVELS", "PASS,WARN")).split(",")
        if x.strip()
    }
    # [설계 의도] 게이트 임계값(0.50)은 news_score_daily의 PASS 판정 임계값(0.60)보다 낮게 설정.
    # 이유: WARN 품질(50~60% 매핑)이어도 절반 이상 종목에 신호가 존재하면 보조 신호로 활용.
    # 뉴스는 w=0.10 보조 레그이므로 FAIL(50% 미만)만 차단하고 WARN은 허용하는 보수적 fail-soft 정책.
    # 변경 시 NEWS_GATE_MIN_MAPPED_RATE 환경변수로 조정 가능.
    min_mapped_rate = max(0.0, min(1.0, float(str(os.getenv("NEWS_GATE_MIN_MAPPED_RATE", "0.50")).strip() or "0.50")))
    min_nonzero_rate = max(0.0, min(1.0, float(str(os.getenv("NEWS_GATE_MIN_NONZERO_RATE", "0.05")).strip() or "0.05")))

    stale_block = bool(
        reason in {"signals_stale_lag_3d", "signals_stale_lag_4d"}
        or str(reason).startswith("signals_stale_lag_")
    )
    gate_open = bool(
        quality in gate_quality_levels
        and mapped_rate >= min_mapped_rate
        and nonzero_rate >= min_nonzero_rate
        and not stale_block
    )
    gate_close_reasons: List[str] = []
    if quality not in gate_quality_levels:
        gate_close_reasons.append(f"quality_not_allowed:{quality}")
    if mapped_rate < min_mapped_rate:
        gate_close_reasons.append(f"mapped_rate_low:{mapped_rate:.4f}<{min_mapped_rate:.4f}")
    if nonzero_rate < min_nonzero_rate:
        gate_close_reasons.append(f"nonzero_rate_low:{nonzero_rate:.4f}<{min_nonzero_rate:.4f}")
    if stale_block:
        gate_close_reasons.append(f"stale_reason:{reason or 'signals_stale'}")
    effective_reason = "ok" if gate_open else (";".join(gate_close_reasons) if gate_close_reasons else (reason or "gate_closed"))

    return {
        "gate_open": gate_open,
        "quality": quality,
        "reason": effective_reason,
        "source_reason": reason,
        "mapped_rate": mapped_rate,
        "nonzero_rate": nonzero_rate,
        "asof_ymd": str(obj.get("asof_ymd") or ""),
        "gate_quality_levels": sorted(gate_quality_levels),
        "gate_min_mapped_rate": min_mapped_rate,
        "gate_min_nonzero_rate": min_nonzero_rate,
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


def _latest_candidate_sector_nonzero() -> int:
    if not CAND_WITH_SECTOR_SCORE.exists():
        return 0
    try:
        df = _read_csv(CAND_WITH_SECTOR_SCORE, dtype={"code": str})
    except Exception:
        return 0
    if "sector_score" not in df.columns:
        return 0
    return int((pd.to_numeric(df["sector_score"], errors="coerce").fillna(0.0) != 0).sum())


def _safe_float(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if pd.isna(x):
        return None
    return float(x)


def _as_bool_flag(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"1", "true", "t", "y", "yes", "on"}:
        return True
    if s in {"0", "false", "f", "n", "no", "off", "", "none", "nan"}:
        return False
    try:
        return float(s) > 0
    except Exception:
        return False


def _micro_label(score_01: float) -> str:
    s = float(score_01)
    if s >= 0.67:
        return "POSITIVE"
    if s < 0.34:
        return "NEGATIVE"
    return "NEUTRAL"


def _age_days_from_ymd(d8: str) -> Optional[int]:
    ds = _norm_date8(d8)
    if not ds:
        return None
    try:
        dt = datetime.strptime(ds, "%Y%m%d").date()
    except Exception:
        return None
    return int((datetime.now().date() - dt).days)


def _load_micro_feature_maps() -> Dict[str, Any]:
    score_map: Dict[str, float] = {}
    source_map: Dict[str, str] = {}
    signal_map: Dict[str, str] = {}

    dart_rows = 0
    watch_rows = 0
    watch_flagged = 0

    dart_meta = _safe_json(DART_FUNDAMENTAL_META) or {}
    watch_meta = _safe_json(KRX_WATCHLIST_META) or {}

    if DART_FUNDAMENTAL_LATEST.exists():
        try:
            ddf = _read_csv(DART_FUNDAMENTAL_LATEST, dtype={"code": str})
        except Exception:
            ddf = pd.DataFrame()
        if not ddf.empty and "code" in ddf.columns:
            ddf = ddf.copy()
            ddf["code"] = ddf["code"].map(_norm_code6)
            ddf = ddf[ddf["code"] != ""]
            dart_rows = int(len(ddf))

            for _, r in ddf.iterrows():
                code = str(r.get("code") or "")
                if not code:
                    continue

                comps: List[float] = []

                op = _safe_float(r.get("operating_profit"))
                if op is not None:
                    comps.append(1.0 if op > 0 else 0.0)

                roe = _safe_float(r.get("ROE"))
                if roe is not None:
                    comps.append(1.0 if roe > 0 else 0.0)

                npm = _safe_float(r.get("NPM"))
                if npm is not None:
                    comps.append(1.0 if npm > 0 else 0.0)

                debt = _safe_float(r.get("debt_ratio"))
                if debt is not None:
                    comps.append(1.0 if debt <= 200.0 else 0.0)

                current_ratio = _safe_float(r.get("current_ratio"))
                if current_ratio is not None:
                    comps.append(1.0 if current_ratio >= 100.0 else 0.0)

                score = (sum(comps) / len(comps)) if comps else 0.50
                score = max(0.0, min(1.0, float(score)))
                score_map[code] = round(score, 6)
                source_map[code] = "DART"
                signal_map[code] = _micro_label(score)

    if KRX_WATCHLIST_LATEST.exists():
        try:
            wdf = _read_csv(KRX_WATCHLIST_LATEST, dtype={"code": str})
        except Exception:
            wdf = pd.DataFrame()
        if not wdf.empty and "code" in wdf.columns:
            wdf = wdf.copy()
            wdf["code"] = wdf["code"].map(_norm_code6)
            wdf = wdf[wdf["code"] != ""]
            watch_rows = int(len(wdf))

            for _, r in wdf.iterrows():
                code = str(r.get("code") or "")
                if not code:
                    continue
                penalty = 0.0
                if _as_bool_flag(r.get("krx_caution")):
                    penalty += 0.10
                if _as_bool_flag(r.get("krx_warning")):
                    penalty += 0.20
                if _as_bool_flag(r.get("krx_risk")):
                    penalty += 0.35
                if _as_bool_flag(r.get("krx_admin")):
                    penalty += 0.35

                if penalty <= 0.0:
                    continue
                watch_flagged += 1
                base = float(score_map.get(code, 0.50))
                adj = max(0.0, min(1.0, base - penalty))
                score_map[code] = round(adj, 6)
                prev_src = source_map.get(code, "")
                source_map[code] = "DART+WATCH" if prev_src == "DART" else "WATCH_ONLY"
                signal_map[code] = _micro_label(adj)

    dart_asof = _norm_date8(dart_meta.get("as_of_ymd"))
    dart_age_days = _age_days_from_ymd(dart_asof)
    stale = bool(dart_age_days is not None and dart_age_days > 14)

    gate_open = bool(dart_rows > 0 and not stale)
    quality = "PASS" if gate_open else ("WARN" if dart_rows > 0 else "FAIL_SOFT")
    reason = "ok" if gate_open else ("dart_stale" if stale else "dart_missing")

    return {
        "gate_open": gate_open,
        "quality": quality,
        "reason": reason,
        "maps": {
            "score": score_map,
            "source": source_map,
            "signal": signal_map,
        },
        "stats": {
            "dart_rows": int(dart_rows),
            "watch_rows": int(watch_rows),
            "watch_flagged": int(watch_flagged),
            "mapped_codes": int(len(score_map)),
            "dart_asof_ymd": dart_asof,
            "dart_age_days": dart_age_days,
            "watch_asof_ymd": _norm_date8(watch_meta.get("as_of_ymd")),
        },
    }


def _load_fundamental_feature_maps() -> Dict[str, Any]:
    score_map: Dict[str, float] = {}
    source_map: Dict[str, str] = {}
    dart_rows = 0

    dart_meta = _safe_json(DART_FUNDAMENTAL_META) or {}

    if DART_FUNDAMENTAL_LATEST.exists():
        try:
            ddf = _read_csv(DART_FUNDAMENTAL_LATEST, dtype={"code": str})
        except Exception:
            ddf = pd.DataFrame()
        if not ddf.empty and "code" in ddf.columns:
            ddf = ddf.copy()
            ddf["code"] = ddf["code"].map(_norm_code6)
            ddf = ddf[ddf["code"] != ""]
            dart_rows = int(len(ddf))

            for _, r in ddf.iterrows():
                code = str(r.get("code") or "")
                if not code:
                    continue

                comps: List[float] = []

                roe = _safe_float(r.get("ROE"))
                if roe is not None:
                    if roe > 15:
                        comps.append(1.0)
                    elif roe > 5:
                        comps.append(0.6)
                    elif roe > 0:
                        comps.append(0.3)
                    else:
                        comps.append(0.0)

                debt = _safe_float(r.get("debt_ratio"))
                if debt is not None:
                    if debt < 100:
                        comps.append(1.0)
                    elif debt < 200:
                        comps.append(0.6)
                    else:
                        comps.append(0.2)

                opm = _safe_float(r.get("operating_margin"))
                if opm is not None:
                    if opm > 15:
                        comps.append(1.0)
                    elif opm > 5:
                        comps.append(0.6)
                    elif opm > 0:
                        comps.append(0.3)
                    else:
                        comps.append(0.0)

                rev = _safe_float(r.get("revenue_growth_yoy"))
                if rev is not None:
                    if rev > 20:
                        comps.append(1.0)
                    elif rev > 5:
                        comps.append(0.6)
                    elif rev > 0:
                        comps.append(0.3)
                    elif rev > -10:
                        comps.append(0.1)
                    else:
                        comps.append(0.0)

                divy = _safe_float(r.get("dividend_yield"))
                if divy is not None:
                    if divy >= 4:
                        comps.append(1.0)
                    elif divy >= 2:
                        comps.append(0.6)
                    elif divy > 0:
                        comps.append(0.3)
                    else:
                        comps.append(0.0)

                if not comps:
                    continue

                score_map[code] = round(float(sum(comps) / len(comps)), 6)
                source_map[code] = "DART_FUNDAMENTAL"

    dart_asof = _norm_date8(dart_meta.get("as_of_ymd"))
    dart_age_days = _age_days_from_ymd(dart_asof)
    stale = bool(dart_age_days is not None and dart_age_days > 14)
    gate_open = bool(dart_rows > 0 and not stale and len(score_map) > 0)
    quality = "PASS" if gate_open else ("WARN" if dart_rows > 0 else "FAIL_SOFT")
    reason = "ok" if gate_open else ("dart_stale" if stale else "dart_missing")

    return {
        "gate_open": gate_open,
        "quality": quality,
        "reason": reason,
        "maps": {
            "score": score_map,
            "source": source_map,
        },
        "stats": {
            "dart_rows": int(dart_rows),
            "mapped_codes": int(len(score_map)),
            "dart_asof_ymd": dart_asof,
            "dart_age_days": dart_age_days,
        },
    }


def _regime_score(risk_on: bool, regime: str) -> float:
    r = str(regime or "").upper()
    if not bool(risk_on):
        return -0.20
    if r in {"CRASH", "RISK_OFF", "BEAR"}:
        return -0.10
    if r in {"NORMAL", "BULL"}:
        return 0.20
    return 0.10


def _sector_fx_bucket(name: str, sector: str) -> str:
    text = f"{name} {sector}".strip().upper()
    export_keywords = [
        "자동차", "AUTO", "반도체", "SEMICON", "전자", "ELECT", "조선", "SHIP",
        "철강", "STEEL", "기계", "MACH", "방산", "DEFENSE", "정유", "OIL", "에너지", "ENERGY",
        "운송", "SHIPPING", "해상", "물류", "LOGISTICS",
        "일렉트릭", "전력기기", "변압기", "전선", "배전", "전력설비",
        "2차전지", "배터리", "BATTERY", "CELL", "이차전지",
        "바이오", "BIO", "제약", "PHARMA", "의약품", "DRUG", "헬스케어", "HEALTH",
    ]
    domestic_keywords = [
        "유통", "RETAIL", "백화점", "호텔", "여행", "항공", "AIR", "식품", "FOOD",
        "화장품", "COSMETIC", "의류", "패션", "건설", "REIT", "부동산", "전기가스", "UTILITY",
        "소비", "CONSUMER",
        "통신", "TELECOM", "게임", "GAME", "엔터", "ENTERTAIN", "콘텐츠", "CONTENT",
        "화학", "CHEM", "석유화학", "PETRO", "PETROCHEM",
    ]
    financial_keywords = ["금융", "은행", "보험", "증권", "FINANC"]

    if any(k in text for k in export_keywords):
        return "EXPORT"
    if any(k in text for k in domestic_keywords):
        return "DOMESTIC"
    if any(k in text for k in financial_keywords):
        return "FINANCIAL"
    return "NEUTRAL"


def _load_fx_feature_points() -> List[Dict[str, Any]]:
    latest_obj = _safe_json(MACRO_FEATURE_LATEST) or {}
    latest_fx = latest_obj.get("fx_context") if isinstance(latest_obj.get("fx_context"), dict) else {}
    fallback_trend = str(latest_fx.get("trend_direction") or "").upper()

    rows: List[Dict[str, Any]] = []
    for p in sorted(LOGS.glob("macro_feature_external_*.json")):
        if p.name == MACRO_FEATURE_LATEST.name:
            continue
        obj = _safe_json(p) or {}
        d8 = _norm_date8(obj.get("as_of_ymd"))
        fx = obj.get("fx_context") if isinstance(obj.get("fx_context"), dict) else {}
        if not d8 or not fx:
            continue
        trend = str(fx.get("trend_direction") or "").upper()
        if not trend:
            if fallback_trend:
                fx["trend_direction"] = fallback_trend
            else:
                fx["trend_direction"] = "UNKNOWN"
        rows.append({"date8": d8, **fx})
    return sorted(rows, key=lambda x: str(x.get("date8") or ""))


def _fx_score_for_row(name: str, sector: str, fx_ctx: Dict[str, Any]) -> Tuple[float, str]:
    if not fx_ctx or not bool(fx_ctx.get("available")):
        return 0.0, "FAIL_SOFT"

    bucket = _sector_fx_bucket(name, sector)
    band = str(fx_ctx.get("level_band") or "UNKNOWN").upper()
    vol_band = str(fx_ctx.get("volatility_band") or "UNKNOWN").upper()
    trend_direction = str(fx_ctx.get("trend_direction") or "UNKNOWN").upper()
    three_day_extreme = bool(fx_ctx.get("three_day_extreme"))
    text = f"{name} {sector}".strip().upper()
    fin_sub = "GENERIC"
    if bucket == "FINANCIAL":
        if ("은행" in text) or ("BANK" in text):
            fin_sub = "BANK"
        elif ("증권" in text) or ("BROKER" in text) or ("SECURIT" in text):
            fin_sub = "BROKER"
        elif ("보험" in text) or ("INSUR" in text):
            fin_sub = "INSURANCE"
    score = 0.0

    if band in {"OVER_STRONG", "STRONG"}:
        if bucket == "DOMESTIC":
            score = 0.12 if band == "OVER_STRONG" else 0.10
        elif bucket == "EXPORT":
            score = -0.15 if band == "OVER_STRONG" else -0.10
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = 0.04 if band == "STRONG" else 0.01
            elif fin_sub == "BROKER":
                score = 0.03
            elif fin_sub == "INSURANCE":
                score = 0.02
            else:
                score = 0.02 if band == "STRONG" else 0.00
    elif band == "FAIR":
        score = 0.0
    elif band == "WEAK":
        if bucket == "EXPORT":
            score = 0.12
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = 0.06
            elif fin_sub == "BROKER":
                score = 0.04
            elif fin_sub == "INSURANCE":
                score = 0.03
            else:
                score = 0.04
        elif bucket == "DOMESTIC":
            score = -0.10
        else:
            score = -0.02
    elif band == "CRISIS":
        if bucket == "EXPORT":
            score = 0.08
        elif bucket == "DOMESTIC":
            score = -0.18
        elif bucket == "FINANCIAL":
            if fin_sub == "BANK":
                score = -0.08
            elif fin_sub == "BROKER":
                score = -0.12
            elif fin_sub == "INSURANCE":
                score = -0.11
            else:
                score = -0.10
        else:
            score = -0.08

    if vol_band == "HIGH":
        if score > 0:
            score *= 0.70
        else:
            score -= 0.03
    elif vol_band == "EXTREME":
        if score > 0:
            score *= 0.40
        else:
            score -= 0.08

    if three_day_extreme:
        if score > 0:
            score *= 0.60
        else:
            score -= 0.05

    # 추세 반영: 원화 약세 추세(WEAKENING)에서는 EXPORT 가산, 강세 추세에서는 EXPORT 감산.
    if trend_direction == "WEAKENING":
        if bucket == "EXPORT":
            score += 0.03
        elif bucket == "DOMESTIC":
            score -= 0.01
    elif trend_direction == "STRENGTHENING":
        if bucket == "EXPORT":
            score -= 0.03
        elif bucket == "DOMESTIC":
            score += 0.01

    # 스케일 보정: fx 영향이 과소평가되지 않도록 범위만 확대(캡 적용).
    score = max(-0.30, min(0.20, float(score) * 1.35))
    return round(float(score), 6), f"FX_{band}_{vol_band}_{trend_direction}_{bucket}_{fin_sub}"


def main() -> int:
    if not JOINED.exists():
        raise SystemExit(f"[FATAL] missing {JOINED}")
    if not TRADES.exists():
        raise SystemExit(f"[FATAL] missing {TRADES}")

    joined = _read_csv(JOINED, dtype={"code": str, "trade_id": str})
    trades = _read_csv(TRADES, dtype={"code": str, "trade_id": str})

    # backup output targets
    # 내용이 직전 백업과 같으면 새 파일을 만들지 않는다 (PLANS 2026-08-21 (29)).
    # 이전에는 매 실행마다 무조건 백업해서 joined_trades_latest 백업만 17,764개 / 240.8MB 가 쌓였다
    # (2026-03-05 부터, 정리 로직 없음). 기존 파일은 건드리지 않는다 - 증식만 멈춘다.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _backup_if_changed(target: Path) -> None:
        if not target.exists():
            return
        try:
            body = target.read_text(encoding="utf-8")
        except Exception:
            body = None
        if body is None:
            # 읽지 못하면 예전처럼 무조건 백업한다(더 나빠지지 않는 쪽)
            try:
                target.with_suffix(target.suffix + f".bak_signal_integ_{ts}").write_bytes(target.read_bytes())
            except Exception:
                pass
            return
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        stamp = target.with_suffix(target.suffix + ".bak_signal_integ.last_sha256")
        prev = ""
        try:
            prev = stamp.read_text(encoding="utf-8").strip() if stamp.exists() else ""
        except Exception:
            prev = ""
        if prev == digest:
            return  # 직전 백업과 동일 -> 생성하지 않는다
        target.with_suffix(target.suffix + f".bak_signal_integ_{ts}").write_text(body, encoding="utf-8")
        try:
            stamp.write_text(digest, encoding="utf-8")
        except Exception:
            pass

    _backup_if_changed(JOINED)
    _backup_if_changed(JOINED_FINAL)

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
        lambda r: _clean_text(r.get("sector")) or _clean_text(code_to_sector.get(str(r.get("code", "")), "")),
        axis=1,
    )
    joined["sector_code"] = joined["code"].map(lambda c: code_to_sector_code.get(str(c), "000"))

    # build sector score history from candidates_v41_1_*.csv (incremental cache)
    hist = _build_sector_score_history_incremental(code_to_sector_code)

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

    fx_points = _load_fx_feature_points()

    def fx_context_asof(date8: str) -> Dict[str, Any]:
        if not fx_points:
            return {}
        dates = [str(row.get("date8") or "") for row in fx_points]
        i = bisect_right(dates, str(date8)) - 1
        if i >= 0:
            return dict(fx_points[i])
        return dict(fx_points[0])

    fx_band_list: List[str] = []
    fx_vol_band_list: List[str] = []
    fx_score_list: List[float] = []
    fx_source_list: List[str] = []
    for _, r in joined.iterrows():
        fx_ctx = fx_context_asof(str(r.get("date", "") or ""))
        fx_score, fx_src = _fx_score_for_row(_clean_text(r.get("name")), _clean_text(r.get("sector")), fx_ctx)
        fx_band_list.append(str(fx_ctx.get("level_band") or "UNKNOWN"))
        fx_vol_band_list.append(str(fx_ctx.get("volatility_band") or "UNKNOWN"))
        fx_score_list.append(fx_score)
        fx_source_list.append(fx_src)
    joined["fx_band"] = fx_band_list
    joined["fx_volatility_band"] = fx_vol_band_list
    joined["fx_score"] = fx_score_list
    joined["fx_source"] = fx_source_list

    # [설계 의도 — fail-soft] 뉴스 신호는 w=0.10 보조 레그이므로 DB 장애/수집 실패 시 0.0으로 처리하고 매매를 계속한다.
    # 뉴스 부재가 매매 차단 사유가 되지 않는다. news_gate_effective=False 시 w_news=0.00으로 자동 제외된다.
    # 뉴스 장애로 매매를 멈춰야 하는 경우(예: 중대 악재 감지 전용 모드)는 NEWS_GATE_QUALITY_LEVELS=PASS 로만 제한한다.
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

    # micro overlay (DART + KRX watchlist) - fail-soft by default
    micro_bundle = _load_micro_feature_maps()
    micro_maps = micro_bundle.get("maps") or {}
    micro_score_map = dict(micro_maps.get("score") or {})
    micro_source_map = dict(micro_maps.get("source") or {})
    micro_signal_map = dict(micro_maps.get("signal") or {})

    joined["micro_score"] = joined["code"].map(lambda c: float(micro_score_map.get(str(c), 0.50)))
    joined["micro_source"] = joined["code"].map(lambda c: str(micro_source_map.get(str(c), "FAIL_SOFT")))
    joined["micro_signal"] = joined["code"].map(lambda c: str(micro_signal_map.get(str(c), "NEUTRAL")))
    micro_mapped_rows = int((joined["micro_source"] != "FAIL_SOFT").sum())
    micro_mapped_rate = float(micro_mapped_rows / max(len(joined), 1))
    micro_gate_effective = bool(micro_bundle.get("gate_open")) and (micro_mapped_rate >= MICRO_GATE_MIN_MAPPED_RATE)

    fundamental_bundle = _load_fundamental_feature_maps()
    fundamental_maps = fundamental_bundle.get("maps") or {}
    fundamental_score_map = dict(fundamental_maps.get("score") or {})
    fundamental_source_map = dict(fundamental_maps.get("source") or {})
    joined["fundamental_score"] = joined["code"].map(lambda c: float(fundamental_score_map.get(str(c), 0.0)))
    joined["fundamental_source"] = joined["code"].map(lambda c: str(fundamental_source_map.get(str(c), "FAIL_SOFT")))
    fundamental_nonzero = int((pd.to_numeric(joined["fundamental_score"], errors="coerce").fillna(0.0) != 0.0).sum())
    fundamental_mapped_rate = float(fundamental_nonzero / max(len(joined), 1))
    fundamental_gate_effective = bool(fundamental_bundle.get("gate_open")) and (fundamental_mapped_rate > 0.0)

    # final score blend (as-of) with quality gates on news/micro legs
    news_gate = _news_gate_from_status()
    news_gate_effective = bool(news_gate.get("gate_open")) and (news_nonzero_input > 0)
    if news_gate_effective:
        w_sector, w_regime, w_news, w_fx = 0.40, 0.19, 0.10, 0.16
        blend_policy = "ASOF_BLEND_NEWS_ON"
    else:
        w_sector, w_regime, w_news, w_fx = 0.44, 0.23, 0.00, 0.16
        blend_policy = "ASOF_BLEND_NEWS_OFF"

    if fundamental_gate_effective:
        w_fundamental = 0.10
        blend_policy = blend_policy + "_FUND_ON"
    else:
        w_fundamental = 0.00
        blend_policy = blend_policy + "_FUND_OFF"

    if micro_gate_effective:
        w_micro = 0.10
        w_sector = max(0.0, w_sector - 0.05)
        w_regime = max(0.0, w_regime - 0.05)
        blend_policy = blend_policy + "_MICRO_ON"
    else:
        w_micro = 0.00
        blend_policy = blend_policy + "_MICRO_OFF"

    joined["news_gate"]        = "OPEN" if news_gate_effective else "CLOSED"
    joined["micro_gate"]       = "OPEN" if micro_gate_effective else "CLOSED"
    joined["fundamental_gate"] = "OPEN" if fundamental_gate_effective else "CLOSED"
    joined["final_score"] = (
        pd.to_numeric(joined["sector_score"],     errors="coerce").fillna(0.0) * w_sector
        + pd.to_numeric(joined["regime_score"],   errors="coerce").fillna(0.0) * w_regime
        + pd.to_numeric(joined["news_score"],     errors="coerce").fillna(0.0) * w_news
        + pd.to_numeric(joined["fx_score"],       errors="coerce").fillna(0.0) * w_fx
        + pd.to_numeric(joined["fundamental_score"], errors="coerce").fillna(0.0) * w_fundamental
        + (pd.to_numeric(joined["micro_score"],   errors="coerce").fillna(0.5) - 0.5) * w_micro
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

    phase3_micro_df = joined[["date", "code", "micro_score", "micro_signal", "micro_source"]].copy()
    _write_csv(PHASE3_MICRO, phase3_micro_df)

    # write joined latest (pre-final schema)
    latest_cols = [
        "trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "pnl_krw",
        "exit_reason", "note", "sell_oid", "order_id", "qty", "pnl_krw_net", "date", "sector", "sector_score",
        "regime", "regime_score", "source", "fx_band", "fx_volatility_band", "fx_score", "fx_source",
        "news_score", "news_sentiment", "news_source", "news_gate",
        "fundamental_score", "fundamental_source", "fundamental_gate",
        "micro_score", "micro_signal", "micro_source", "micro_gate",
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
    candidate_sector_nonzero = _latest_candidate_sector_nonzero()
    candidate_rows = 0
    joined_rows = int(len(final_df))
    overlap_code_count = 0
    overlap_date_count = 0
    overlap_code_date_count = 0
    try:
        cand_df = _read_csv(CAND_WITH_SECTOR_SCORE, dtype={"code": str}) if CAND_WITH_SECTOR_SCORE.exists() else pd.DataFrame()
        if isinstance(cand_df, pd.DataFrame) and len(cand_df) > 0:
            candidate_rows = int(len(cand_df))
            if "code" in cand_df.columns and "code" in final_df.columns:
                c_code = set(cand_df["code"].astype(str).str.zfill(6).tolist())
                j_code = set(final_df["code"].astype(str).str.zfill(6).tolist())
                overlap_code_count = int(len(c_code & j_code))
            if "date" in cand_df.columns and "date" in final_df.columns:
                c_date = set(cand_df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8).tolist())
                j_date = set(final_df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8).tolist())
                overlap_date_count = int(len(c_date & j_date))
            if "code" in cand_df.columns and "date" in cand_df.columns and "code" in final_df.columns and "date" in final_df.columns:
                c_pair = set(
                    zip(
                        cand_df["code"].astype(str).str.zfill(6).tolist(),
                        cand_df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8).tolist(),
                    )
                )
                j_pair = set(
                    zip(
                        final_df["code"].astype(str).str.zfill(6).tolist(),
                        final_df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8).tolist(),
                    )
                )
                overlap_code_date_count = int(len(c_pair & j_pair))
    except Exception:
        pass
    regime_nonzero = int((pd.to_numeric(final_df["regime_score"], errors="coerce").fillna(0.0) != 0).sum())
    fx_nonzero = int((pd.to_numeric(final_df["fx_score"], errors="coerce").fillna(0.0) != 0).sum())
    news_nonzero = int((pd.to_numeric(final_df["news_score"], errors="coerce").fillna(0.0) != 0).sum())
    micro_nonzero = int((pd.to_numeric(final_df["micro_score"], errors="coerce").fillna(0.5) != 0.5).sum())
    fundamental_nonzero = int((pd.to_numeric(final_df["fundamental_score"], errors="coerce").fillna(0.0) != 0.0).sum())
    final_nonzero = int((pd.to_numeric(final_df["final_score"], errors="coerce").fillna(0.0) != 0).sum())

    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "phase2_sector": {
            "join_ready": "PASS",
            "score_fill": "PASS" if candidate_sector_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": candidate_sector_nonzero,
            "joined_nonzero_rows": sector_nonzero,
            "definition": "candidate_nonzero_rows vs joined_nonzero_rows are different universes",
            "candidate_rows": candidate_rows,
            "joined_rows": joined_rows,
            "overlap_code_count": overlap_code_count,
            "overlap_date_count": overlap_date_count,
            "overlap_code_date_count": overlap_code_date_count,
            "coverage_status": (
                "OVERLAP_OK"
                if overlap_code_date_count > 0
                else ("NO_CODE_OR_DATE_OVERLAP" if (overlap_code_count == 0 and overlap_date_count == 0) else "PARTIAL_OVERLAP")
            ),
        },
        "phase3_regime": {
            "join_ready": "PASS",
            "score_fill": "PASS" if regime_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": regime_nonzero,
        },
        "phase3_fx": {
            "join_ready": "PASS",
            "score_fill": "PASS" if fx_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": fx_nonzero,
            "bands": sorted({str(x) for x in final_df.get("fx_band", pd.Series(dtype=str)).dropna().tolist() if str(x)}),
            "volatility_bands": sorted({str(x) for x in final_df.get("fx_volatility_band", pd.Series(dtype=str)).dropna().tolist() if str(x)}),
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
        "phase3_micro": {
            "join_ready": "PASS",
            "score_fill": "PASS" if micro_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": micro_nonzero,
            "mapped_rows": micro_mapped_rows,
            "mapped_rate": round(float(micro_mapped_rate), 6),
            "gate": "OPEN" if micro_gate_effective else "CLOSED",
            "quality": str(micro_bundle.get("quality") or "FAIL_SOFT"),
            "reason": str(micro_bundle.get("reason") or ""),
            "stats": dict(micro_bundle.get("stats") or {}),
        },
        "phase3_fundamental": {
            "join_ready": "PASS",
            "score_fill": "PASS" if fundamental_nonzero > 0 else "FAIL_SOFT_0",
            "nonzero_rows": fundamental_nonzero,
            "mapped_rows": fundamental_nonzero,
            "mapped_rate": round(float(fundamental_mapped_rate), 6),
            "gate": "OPEN" if fundamental_gate_effective else "CLOSED",
            "quality": str(fundamental_bundle.get("quality") or "FAIL_SOFT"),
            "reason": str(fundamental_bundle.get("reason") or ""),
            "stats": dict(fundamental_bundle.get("stats") or {}),
        },
        "final_score_merge": {
            "join_ready": "PASS",
            "score_fill": "PASS" if final_nonzero > 0 else "FAIL_SOFT_0",
            "policy": blend_policy if final_nonzero > 0 else "FAIL_SOFT_0",
            "weights": {"sector": w_sector, "regime": w_regime, "news": w_news, "fx": w_fx, "fundamental": w_fundamental, "micro": w_micro},
            "news_gate": "OPEN" if news_gate_effective else "CLOSED",
            "fundamental_gate": "OPEN" if fundamental_gate_effective else "CLOSED",
            "micro_gate": "OPEN" if micro_gate_effective else "CLOSED",

            "nonzero_rows": final_nonzero,
        },
        "artifacts": {
            "joined_latest": str(JOINED),
            "joined_final_latest": str(JOINED_FINAL),
            "phase2_sector": str(PHASE2),
            "phase3_regime": str(PHASE3_REGIME),
            "phase3_news": str(PHASE3_NEWS),
            "phase3_micro": str(PHASE3_MICRO),
        },
    }
    st_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    micro_latest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "gate_open": bool(micro_gate_effective),
        "quality": str(micro_bundle.get("quality") or "FAIL_SOFT"),
        "reason": str(micro_bundle.get("reason") or ""),
        "mapped_rows": int(micro_mapped_rows),
        "mapped_rate": round(float(micro_mapped_rate), 6),
        "stats": dict(micro_bundle.get("stats") or {}),
    }
    MICRO_SIGNAL_LATEST.write_text(json.dumps(micro_latest, ensure_ascii=False, indent=2), encoding="utf-8")

    _log_print(f"[SIGNAL_INTEGRATION] wrote {JOINED}")
    _log_print(f"[SIGNAL_INTEGRATION] wrote {JOINED_FINAL}")
    _log_print(f"[SIGNAL_INTEGRATION] final_nonzero={final_nonzero}/{len(final_df)}")
    _log_print(f"[SIGNAL_INTEGRATION] sector_nonzero={sector_nonzero} regime_nonzero={regime_nonzero} news_nonzero={news_nonzero} micro_nonzero={micro_nonzero}")
    _log_print(f"[SIGNAL_INTEGRATION] status={st_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
