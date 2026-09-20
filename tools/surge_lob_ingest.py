from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.kis_order_client import KISOrderClient
LOG_DIR = ROOT / "2_Logs"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
MARKET_RISING_PATH = LOG_DIR / "market_rising_latest.csv"
OUT_CSV = LOG_DIR / "surge_lob_latest.csv"
OUT_JSON = LOG_DIR / "surge_lob_latest.json"
STATE_JSON = LOG_DIR / "surge_lob_state_latest.json"
SURGE_REALTIME_JSON = LOG_DIR / "surge_realtime_latest.json"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
NEWS_LOB_REFRESH_QUEUE_CSV = LOG_DIR / "news_signal_lob_refresh_queue_latest.csv"
NEWS_SIGNAL_SHADOW_STAGE_CSV = LOG_DIR / "news_signal_shadow_stage_latest.csv"
NORMAL_CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
NORMAL_ACTION_PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
NORMAL_ENTRY_DECISION_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
WS_TICKS_GLOB = "kis_ws_ticks_*.jsonl"
HOGA_LEVELS = 10
_WS_HOGA_LOAD_META: Dict[str, Any] = {}
_LOB_UNIVERSE_META: Dict[str, Any] = {}


def _hoga_level_columns(prefix: str) -> list[str]:
    return [f"{prefix}{i}" for i in range(1, HOGA_LEVELS + 1)]


def _hoga_numeric_columns() -> list[str]:
    return (
        _hoga_level_columns("ask")
        + _hoga_level_columns("bid")
        + _hoga_level_columns("askq")
        + _hoga_level_columns("bidq")
    )


def _clip01(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _zscore(values: pd.Series) -> pd.Series:
    s = pd.to_numeric(values, errors="coerce").fillna(0.0)
    std = float(s.std(ddof=0) or 0.0)
    if std <= 1e-12:
        return pd.Series(0.0, index=s.index)
    return (s - float(s.mean())) / std



def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        s = str(v or "").replace(",", "").strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def _ws_tick_file_ymd(path: Path) -> str:
    parts = path.stem.split("_")
    if not parts:
        return ""
    tail = parts[-1]
    return tail if len(tail) == 8 and tail.isdigit() else ""


def _candidate_ws_ticks_jsonl() -> list[Path]:
    files = []
    for path in LOG_DIR.glob(WS_TICKS_GLOB):
        if _ws_tick_file_ymd(path):
            files.append(path)
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _latest_ws_ticks_jsonl() -> Optional[Path]:
    files = _candidate_ws_ticks_jsonl()
    if not files:
        return None
    return files[0]


def _iter_recent_jsonl_lines(path: Path, tail_bytes: int) -> list[str]:
    size = path.stat().st_size
    if tail_bytes <= 0 or size <= tail_bytes:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    with path.open("rb") as f:
        f.seek(max(0, size - tail_bytes))
        chunk = f.read()
    text = chunk.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if lines and not text.startswith(("{", "[")):
        lines = lines[1:]
    return lines


def _load_latest_ws_hoga(max_age_sec: float = 300.0) -> Dict[str, Dict[str, float]]:
    global _WS_HOGA_LOAD_META
    started = time.monotonic()
    if str(os.getenv("SURGE_LOB_WS_SKIP", "")).strip().lower() in {"1", "true", "yes", "y"}:
        _WS_HOGA_LOAD_META = {"mode": "skip_env", "elapsed_sec": round(time.monotonic() - started, 3)}
        return {}
    candidates = _candidate_ws_ticks_jsonl()
    if not candidates:
        _WS_HOGA_LOAD_META = {"mode": "missing", "elapsed_sec": round(time.monotonic() - started, 3)}
        return {}
    max_files = max(1, int(str(os.getenv("SURGE_LOB_WS_MAX_FILES", "3")).strip() or "3"))
    fresh_candidates: list[Path] = []
    for path in candidates:
        try:
            if time.time() - path.stat().st_mtime <= max(1.0, float(max_age_sec)):
                fresh_candidates.append(path)
                if len(fresh_candidates) >= max_files:
                    break
        except Exception:
            continue
    if not fresh_candidates:
        _WS_HOGA_LOAD_META = {
            "mode": "stale",
            "candidate_files": [str(p) for p in candidates[:5]],
            "max_files": int(max_files),
            "elapsed_sec": round(time.monotonic() - started, 3),
        }
        return {}

    out: Dict[str, Dict[str, float]] = {}
    tail_mb = max(0.0, float(str(os.getenv("SURGE_LOB_WS_TAIL_MB", "1")).strip() or "1"))
    allow_full_scan = str(os.getenv("SURGE_LOB_WS_ALLOW_FULL_SCAN", "")).strip().lower() in {"1", "true", "yes", "y"}
    if tail_mb <= 0 and not allow_full_scan:
        _WS_HOGA_LOAD_META = {
            "mode": "skip_full_scan_disabled",
            "path": str(path),
            "tail_mb": tail_mb,
            "elapsed_sec": round(time.monotonic() - started, 3),
        }
        return {}
    tail_bytes = int(tail_mb * 1024 * 1024)
    line_count = 0
    try:
        for path in fresh_candidates:
            for line in _iter_recent_jsonl_lines(path, tail_bytes):
                line_count += 1
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if str(obj.get("tr_id") or "").strip() != "H0STASP0":
                    continue
                norm = obj.get("normalized")
                if not isinstance(norm, dict):
                    continue
                fields = norm.get("fields")
                if not isinstance(fields, list) or len(fields) < 34:
                    raw = str(norm.get("raw_payload") or "")
                    fields = raw.split("^") if raw else []
                if len(fields) < 34:
                    continue
                code = "".join(ch for ch in str(fields[0]) if ch.isdigit()).zfill(6)
                if len(code) != 6:
                    continue
                hoga: Dict[str, float] = {}
                for level in range(1, HOGA_LEVELS + 1):
                    hoga[f"ask{level}"] = _to_float(fields[2 + level] if len(fields) > 2 + level else 0.0, 0.0)
                    hoga[f"bid{level}"] = _to_float(fields[12 + level] if len(fields) > 12 + level else 0.0, 0.0)
                    hoga[f"askq{level}"] = _to_float(fields[22 + level] if len(fields) > 22 + level else 0.0, 0.0)
                    hoga[f"bidq{level}"] = _to_float(fields[32 + level] if len(fields) > 32 + level else 0.0, 0.0)
                if hoga.get("ask1", 0.0) > 0 or hoga.get("bid1", 0.0) > 0:
                    out[code] = hoga
    except Exception:
        _WS_HOGA_LOAD_META = {
            "mode": "error",
            "candidate_files": [str(p) for p in fresh_candidates[:5]],
            "max_files": int(max_files),
            "tail_mb": tail_mb,
            "line_count": int(line_count),
            "elapsed_sec": round(time.monotonic() - started, 3),
        }
        return {}
    _WS_HOGA_LOAD_META = {
        "mode": "tail" if tail_bytes > 0 else "full",
        "candidate_files": [str(p) for p in fresh_candidates[:5]],
        "files_scanned": int(len(fresh_candidates)),
        "max_files": int(max_files),
        "tail_mb": tail_mb,
        "file_size_bytes": int(sum(p.stat().st_size for p in fresh_candidates)),
        "line_count": int(line_count),
        "codes": int(len(out)),
        "elapsed_sec": round(time.monotonic() - started, 3),
    }
    return out


def _fill_hoga_from_ws(rt: pd.DataFrame) -> pd.DataFrame:
    hoga_map = _load_latest_ws_hoga(
        max_age_sec=float(str(os.getenv("SURGE_LOB_WS_MAX_AGE_SEC", "300")).strip() or "300")
    )
    if not hoga_map:
        return rt
    if "hoga_fetch_status" not in rt.columns:
        rt["hoga_fetch_status"] = ""
    for idx, row in rt.iterrows():
        code = str(row.get("code", "")).strip().zfill(6)
        hoga = hoga_map.get(code)
        if not hoga:
            continue
        for col in _hoga_numeric_columns():
            if col not in rt.columns:
                rt[col] = 0.0
            if _to_float(row.get(col), 0.0) <= 0 and hoga.get(col, 0.0) > 0:
                rt.at[idx, col] = hoga[col]
        rt.at[idx, "hoga_fetch_status"] = "WS_HOGA"
    return rt


def _load_surge_priority_codes() -> set[str]:
    if not SURGE_REALTIME_JSON.exists():
        return set()
    try:
        payload = json.loads(SURGE_REALTIME_JSON.read_text(encoding="utf-8"))
    except Exception:
        return set()
    alerts = payload.get("alerts") if isinstance(payload, dict) else None
    if not isinstance(alerts, list):
        return set()
    out: set[str] = set()
    for item in alerts:
        if not isinstance(item, dict):
            continue
        code = "".join(ch for ch in str(item.get("code") or "") if ch.isdigit()).zfill(6)
        if len(code) == 6:
            out.add(code)
    return out


def _load_no_lob_recheck_priority_codes() -> set[str]:
    if not NO_LOB_RECHECK_CSV.exists():
        return set()
    try:
        df = pd.read_csv(NO_LOB_RECHECK_CSV, dtype={"code": str})
    except Exception:
        return set()
    if df.empty or "code" not in df.columns:
        return set()
    if "probe_class" in df.columns:
        df = df[df["probe_class"].astype(str).str.upper().eq("LOB_COLLECTED_RECHECKABLE")]
    out: set[str] = set()
    for value in df["code"].astype(str):
        code = "".join(ch for ch in value if ch.isdigit()).zfill(6)
        if len(code) == 6:
            out.add(code)
    return out


def _load_news_lob_refresh_priority_codes() -> set[str]:
    out: set[str] = set()
    if not NEWS_LOB_REFRESH_QUEUE_CSV.exists():
        df = pd.DataFrame()
    else:
        try:
            df = pd.read_csv(NEWS_LOB_REFRESH_QUEUE_CSV, dtype={"code": str})
        except Exception:
            df = pd.DataFrame()
    if not df.empty and "code" in df.columns:
        if "shadow_only" in df.columns:
            df = df[df["shadow_only"].astype(str).str.lower().eq("true")]
        if "trading_effect" in df.columns:
            df = df[df["trading_effect"].astype(str).str.lower().eq("false")]
        if "policy_effect" in df.columns:
            df = df[df["policy_effect"].astype(str).str.lower().eq("false")]
        if "publisher_gate_state" in df.columns:
            df = df[
                df["publisher_gate_state"].astype(str).str.upper().isin(
                    ["PUBLISHER_VERIFIED", "PUBLISHER_AUTHENTICATED_PROVISIONAL"]
                )
            ]
        else:
            df = df.iloc[0:0]
        for value in df["code"].astype(str):
            code = "".join(ch for ch in value if ch.isdigit()).zfill(6)
            if len(code) == 6:
                out.add(code)
    if NEWS_SIGNAL_SHADOW_STAGE_CSV.exists():
        try:
            stage = pd.read_csv(NEWS_SIGNAL_SHADOW_STAGE_CSV, dtype={"code": str})
        except Exception:
            stage = pd.DataFrame()
        if not stage.empty and {"code", "news_signal_stage"}.issubset(stage.columns):
            stage = stage[stage["news_signal_stage"].astype(str).str.upper().isin(["PRE_SIGNAL", "CONFIRMED_SHADOW"])]
            if "news_signal_shadow_only" in stage.columns:
                stage = stage[stage["news_signal_shadow_only"].astype(str).str.lower().eq("true")]
            if "news_signal_trading_effect" in stage.columns:
                stage = stage[stage["news_signal_trading_effect"].astype(str).str.lower().eq("false")]
            if "publisher_gate_state" in stage.columns:
                stage = stage[
                    stage["publisher_gate_state"].astype(str).str.upper().isin(
                        ["PUBLISHER_VERIFIED", "PUBLISHER_AUTHENTICATED_PROVISIONAL"]
                    )
                ]
            else:
                stage = stage.iloc[0:0]
            for value in stage["code"].astype(str):
                code = "".join(ch for ch in value if ch.isdigit()).zfill(6)
                if len(code) == 6:
                    out.add(code)
    return out


def _hoga_priority_score(
    row: pd.Series,
    surge_codes: set[str],
    recheck_codes: set[str],
    news_refresh_codes: set[str],
    normal_candidate_codes: set[str],
) -> float:
    code = str(row.get("code", "")).strip().zfill(6)
    score = 1000.0 if code in surge_codes else 0.0
    recheck_bonus = float(str(os.getenv("SURGE_LOB_RECHECK_PRIORITY_BONUS", "950")).strip() or "950")
    if code in recheck_codes:
        score += max(0.0, recheck_bonus)
    news_refresh_bonus = float(str(os.getenv("SURGE_LOB_NEWS_REFRESH_PRIORITY_BONUS", "1250")).strip() or "1250")
    if code in news_refresh_codes:
        score += max(0.0, news_refresh_bonus)
    normal_candidate_bonus = float(str(os.getenv("SURGE_LOB_NORMAL_CANDIDATE_PRIORITY_BONUS", "1100")).strip() or "1100")
    if code in normal_candidate_codes:
        score += max(0.0, normal_candidate_bonus)
    current = _to_float(row.get("current_price"), 0.0)
    open_px = _to_float(row.get("open"), 0.0)
    high = _to_float(row.get("high"), 0.0)
    low = _to_float(row.get("low"), 0.0)
    volume = _to_float(row.get("volume"), 0.0)
    if current > 0 and open_px > 0:
        score += abs(current / open_px - 1.0) * 100.0
    if current > 0 and high > 0 and low > 0:
        score += max(0.0, (high - low) / current) * 50.0
    score += min(volume / 1_000_000.0, 20.0)
    return float(score)


def _fill_hoga_from_kis(rt: pd.DataFrame) -> pd.DataFrame:
    need = (pd.to_numeric(rt.get("ask1", 0), errors="coerce").fillna(0.0) <= 0) | (pd.to_numeric(rt.get("bid1", 0), errors="coerce").fillna(0.0) <= 0)
    if not bool(need.any()):
        return rt
    max_fetch = max(0, int(str(os.getenv("SURGE_LOB_HOGA_MAX_FETCH", "0")).strip() or "0"))
    soft_timeout_sec = max(1.0, float(str(os.getenv("SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC", "18")).strip() or "18"))
    request_sleep_sec = max(0.0, float(str(os.getenv("SURGE_LOB_KIS_SLEEP_SEC", "0.35")).strip() or "0.35"))
    need_rows = rt.loc[need].copy()
    surge_codes = _load_surge_priority_codes()
    recheck_codes = _load_no_lob_recheck_priority_codes()
    news_refresh_codes = _load_news_lob_refresh_priority_codes()
    normal_candidate_codes = _load_normal_candidate_priority_codes()
    need_rows["_hoga_priority"] = need_rows.apply(
        lambda row: _hoga_priority_score(row, surge_codes, recheck_codes, news_refresh_codes, normal_candidate_codes),
        axis=1,
    )
    need_indexes = list(need_rows.sort_values("_hoga_priority", ascending=False).index)
    if max_fetch <= 0:
        rt.loc[need_indexes, "hoga_fetch_status"] = "SKIP_DISABLED"
        return rt
    try:
        # KIS does not serve order-book/hoga quotes on the mock-trading domain regardless
        # of overall paper-trading mode, so this read-only fetch always uses the prod
        # quote-only credentials (kis_*_prod.txt) and never the order-execution client.
        client = KISOrderClient.from_env(mock=False)
    except Exception as e:
        rt["hoga_fetch_status"] = "CLIENT_ERROR"
        rt["hoga_fetch_error"] = f"{type(e).__name__}: {e}"[:240]
        return rt
    client.cfg.timeout_sec = max(
        1.0,
        min(
            float(client.cfg.timeout_sec or 10.0),
            float(str(os.getenv("SURGE_LOB_KIS_TIMEOUT_SEC", "3")).strip() or "3"),
        ),
    )
    client.cfg.rate_limit_retries = max(
        0,
        min(
            int(client.cfg.rate_limit_retries or 0),
            int(float(str(os.getenv("SURGE_LOB_KIS_RATE_LIMIT_RETRIES", "0")).strip() or "0")),
        ),
    )
    client.cfg.shared_budget_lock_timeout_sec = max(
        0.1,
        min(
            float(client.cfg.shared_budget_lock_timeout_sec or 5.0),
            float(str(os.getenv("SURGE_LOB_KIS_LOCK_TIMEOUT_SEC", "1")).strip() or "1"),
        ),
    )
    if "hoga_fetch_status" not in rt.columns:
        rt["hoga_fetch_status"] = ""
    if "hoga_fetch_error" not in rt.columns:
        rt["hoga_fetch_error"] = ""
    start = time.monotonic()
    fetched = 0
    for idx, row in rt.loc[need_indexes].iterrows():
        if fetched >= max_fetch:
            rt.at[idx, "hoga_fetch_status"] = "SKIP_MAX_FETCH"
            continue
        if (time.monotonic() - start) >= soft_timeout_sec:
            rt.at[idx, "hoga_fetch_status"] = "SKIP_SOFT_TIMEOUT"
            continue
        code = str(row.get("code", "")).strip().zfill(6)
        if not code.isdigit() or len(code) != 6:
            rt.at[idx, "hoga_fetch_status"] = "BAD_CODE"
            continue
        try:
            fetched += 1
            hoga = client.inquire_hoga(code=code)
            out = hoga.get("output") if isinstance(hoga.get("output"), dict) else {}
            got_quote = False
            for level in range(1, HOGA_LEVELS + 1):
                ask = _to_float(out.get(f"askp{level}"), 0.0)
                bid = _to_float(out.get(f"bidp{level}"), 0.0)
                askq = _to_float(out.get(f"askp_rsqn{level}"), 0.0)
                bidq = _to_float(out.get(f"bidp_rsqn{level}"), 0.0)
                if ask > 0:
                    rt.at[idx, f"ask{level}"] = ask
                    got_quote = True
                if bid > 0:
                    rt.at[idx, f"bid{level}"] = bid
                    got_quote = True
                if askq > 0:
                    rt.at[idx, f"askq{level}"] = askq
                if bidq > 0:
                    rt.at[idx, f"bidq{level}"] = bidq
            rt.at[idx, "hoga_fetch_status"] = "OK" if got_quote else "EMPTY"
        except Exception as e:
            rt.at[idx, "hoga_fetch_status"] = "ERROR"
            rt.at[idx, "hoga_fetch_error"] = f"{type(e).__name__}: {e}"[:240]
        if request_sleep_sec > 0:
            time.sleep(request_sleep_sec)
    return rt


def _load_market_rising_supplement(today_ymd: str) -> pd.DataFrame:
    if len(str(today_ymd or "")) != 8 or not MARKET_RISING_PATH.exists():
        return pd.DataFrame()
    try:
        mdf = pd.read_csv(MARKET_RISING_PATH, dtype={"code": str})
    except Exception:
        return pd.DataFrame()
    if mdf.empty or "code" not in mdf.columns:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["code"] = mdf["code"].astype(str).str.zfill(6)
    column_map = {
        "current_price": ["current_price", "price"],
        "open": ["open", "open_price", "current_price", "price"],
        "high": ["high", "high_price", "current_price", "price"],
        "low": ["low", "low_price", "current_price", "price"],
        "volume": ["volume", "acml_vol"],
        "trading_value": ["trading_value", "value"],
    }
    for out_col, candidates in column_map.items():
        src = next((c for c in candidates if c in mdf.columns), None)
        if src is None:
            out[out_col] = 0.0
        else:
            out[out_col] = pd.to_numeric(mdf[src], errors="coerce").fillna(0.0)
    tv_zero = pd.to_numeric(out["trading_value"], errors="coerce").fillna(0.0) <= 0
    px = pd.to_numeric(out["current_price"], errors="coerce").fillna(0.0)
    vol = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    out.loc[tv_zero & (px > 0) & (vol > 0), "trading_value"] = px * vol
    out = out[out["code"].str.len() == 6].copy()
    out = out[out["current_price"] > 0].copy()
    return out


def _load_news_lob_refresh_supplement() -> pd.DataFrame:
    codes = sorted(_load_news_lob_refresh_priority_codes())
    if not codes:
        return pd.DataFrame()
    out = pd.DataFrame({"code": codes})
    for col in ["current_price", "open", "high", "low", "volume", "trading_value"]:
        out[col] = 0.0
    out["lob_universe_source"] = "news_signal_lob_refresh_queue"
    return out


def _load_normal_candidate_supplement() -> pd.DataFrame:
    source_codes = _load_normal_candidate_priority_code_sources()
    target_codes = set().union(*source_codes.values()) if source_codes else set()
    if not NORMAL_CANDIDATES_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(NORMAL_CANDIDATES_CSV, dtype={"code": str})
    except Exception:
        return pd.DataFrame()
    if df.empty or "code" not in df.columns:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str).str.zfill(6)
    if target_codes:
        df = df[df["code"].isin(target_codes)].copy()
    elif "execution_pool" in df.columns:
        df = df[df["execution_pool"].map(_truthy)].copy()
    else:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["code"] = df["code"].astype(str).str.zfill(6)
    close = pd.to_numeric(df.get("close", 0), errors="coerce").fillna(0.0)
    value = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0.0)
    volume = (value / close.replace(0, float("nan"))).fillna(0.0)
    out["current_price"] = close
    out["open"] = close
    out["high"] = close
    out["low"] = close
    out["volume"] = volume
    out["trading_value"] = value
    out["lob_universe_source"] = "normal_candidate_priority"
    out = out[(out["code"].str.len() == 6) & (out["current_price"] > 0)].copy()
    return out.drop_duplicates(subset=["code"], keep="first")


def _safe_code_set(df: pd.DataFrame) -> set[str]:
    if df.empty or "code" not in df.columns:
        return set()
    try:
        return {
            str(code).zfill(6)
            for code in df["code"].astype(str)
            if len(str(code).zfill(6)) == 6
        }
    except KeyError:
        return set()


def _load_normal_candidate_priority_code_sources() -> Dict[str, set[str]]:
    sources: Dict[str, set[str]] = {
        "execution_pool": set(),
        "action_plan_buy_candidate": set(),
        "entry_decision_normal_recheck": set(),
    }
    if NORMAL_CANDIDATES_CSV.exists():
        try:
            df = pd.read_csv(NORMAL_CANDIDATES_CSV, dtype={"code": str})
        except Exception:
            df = pd.DataFrame()
        if not df.empty and {"code", "execution_pool"}.issubset(df.columns):
            pool = df[df["execution_pool"].map(_truthy)].copy()
            sources["execution_pool"] = _safe_code_set(pool)
    if NORMAL_ACTION_PLAN_CSV.exists():
        try:
            plan = pd.read_csv(NORMAL_ACTION_PLAN_CSV, dtype={"code": str})
        except Exception:
            plan = pd.DataFrame()
        if not plan.empty and "code" in plan.columns:
            if "source" in plan.columns:
                plan = plan[plan["source"].astype(str).str.lower().eq("daily_candidate")]
            if "next_action" in plan.columns:
                plan = plan[plan["next_action"].astype(str).str.upper().eq("BUY_CANDIDATE")]
            else:
                plan = plan.iloc[0:0]
            if "trading_allowed" in plan.columns:
                plan = plan[plan["trading_allowed"].map(_truthy)]
            sources["action_plan_buy_candidate"] = _safe_code_set(plan)
    if NORMAL_ENTRY_DECISION_CSV.exists():
        try:
            decision = pd.read_csv(NORMAL_ENTRY_DECISION_CSV, dtype={"code": str})
        except Exception:
            decision = pd.DataFrame()
        if not decision.empty and "code" in decision.columns:
            if "execution_reason" in decision.columns:
                decision = decision[
                    decision["execution_reason"].astype(str).str.upper().str.startswith("NORMAL_")
                ].copy()
            else:
                decision = decision.iloc[0:0]
            if "positive_entry_ok" in decision.columns:
                decision = decision[decision["positive_entry_ok"].map(_truthy)]
            sources["entry_decision_normal_recheck"] = _safe_code_set(decision)
    return sources


def _load_normal_candidate_priority_codes() -> set[str]:
    sources = _load_normal_candidate_priority_code_sources()
    return set().union(*sources.values()) if sources else set()


def _load_lob_universe() -> pd.DataFrame:
    global _LOB_UNIVERSE_META
    rt = pd.read_csv(INTRADAY_PRICES, dtype={"code": str})
    if "code" not in rt.columns:
        return rt
    rt["code"] = rt["code"].astype(str).str.zfill(6)
    if len(rt) == 0:
        return rt
    base_rows = int(len(rt))
    if "date" in rt.columns:
        today_ymd = str(
            rt["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8).max()
        )
    else:
        today_ymd = ""
    added_market_rising_rows = 0
    rising = _load_market_rising_supplement(today_ymd)
    if not rising.empty:
        hoga_backup = rt.copy()
        rt = pd.concat([rt, rising], ignore_index=True)
        rt = rt.drop_duplicates(subset=["code"], keep="last").copy()
        added_market_rising_rows = max(0, int(len(rt) - base_rows))
        if not hoga_backup.empty:
            hoga_backup["code"] = hoga_backup["code"].astype(str).str.zfill(6)
            for col in _hoga_numeric_columns():
                if col not in hoga_backup.columns:
                    continue
                backup = pd.to_numeric(hoga_backup.set_index("code")[col], errors="coerce")
                if col not in rt.columns:
                    rt[col] = 0.0
                current = pd.to_numeric(rt[col], errors="coerce").fillna(0.0)
                fill_mask = current <= 0
                if bool(fill_mask.any()):
                    restored = rt.loc[fill_mask, "code"].map(backup).fillna(0.0)
                    rt.loc[fill_mask, col] = restored
    before_news_rows = int(len(rt))
    news_refresh = _load_news_lob_refresh_supplement()
    if not news_refresh.empty:
        existing_codes = set(rt["code"].astype(str).str.zfill(6))
        news_refresh = news_refresh[~news_refresh["code"].astype(str).str.zfill(6).isin(existing_codes)].copy()
        if not news_refresh.empty:
            rt = pd.concat([rt, news_refresh], ignore_index=True)
            rt = rt.drop_duplicates(subset=["code"], keep="first").copy()
    news_added_rows = max(0, int(len(rt) - before_news_rows))
    before_normal_rows = int(len(rt))
    normal_refresh = _load_normal_candidate_supplement()
    if not normal_refresh.empty:
        existing_codes = set(rt["code"].astype(str).str.zfill(6))
        normal_refresh = normal_refresh[~normal_refresh["code"].astype(str).str.zfill(6).isin(existing_codes)].copy()
        if not normal_refresh.empty:
            rt = pd.concat([rt, normal_refresh], ignore_index=True)
            rt = rt.drop_duplicates(subset=["code"], keep="first").copy()
    normal_code_sources = _load_normal_candidate_priority_code_sources()
    normal_priority_codes = set().union(*normal_code_sources.values()) if normal_code_sources else set()
    _LOB_UNIVERSE_META = {
        "base_rows": base_rows,
        "market_rising_added_rows": int(added_market_rising_rows),
        "news_lob_refresh_queue_codes": int(len(_load_news_lob_refresh_priority_codes())),
        "news_lob_refresh_added_rows": int(news_added_rows),
        "normal_candidate_execution_pool_codes": int(len(normal_code_sources.get("execution_pool", set()))),
        "normal_candidate_action_plan_codes": int(len(normal_code_sources.get("action_plan_buy_candidate", set()))),
        "normal_candidate_entry_decision_codes": int(len(normal_code_sources.get("entry_decision_normal_recheck", set()))),
        "normal_candidate_priority_codes": int(len(normal_priority_codes)),
        "normal_candidate_added_rows": max(0, int(len(rt) - before_normal_rows)),
    }
    return rt


def _load_state() -> Dict[str, Dict[str, Any]]:
    if not STATE_JSON.exists():
        return {}
    try:
        payload = json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}
    by_code = payload.get("by_code", {})
    return by_code if isinstance(by_code, dict) else {}


def _best_level_ofi(row: pd.Series) -> float:
    if not bool(row.get("ofi_valid", False)):
        return 0.0
    bid = float(row.get("bid1", 0.0) or 0.0)
    ask = float(row.get("ask1", 0.0) or 0.0)
    bidq = float(row.get("bidq1", 0.0) or 0.0)
    askq = float(row.get("askq1", 0.0) or 0.0)
    prev_bid = float(row.get("prev_bid1", 0.0) or 0.0)
    prev_ask = float(row.get("prev_ask1", 0.0) or 0.0)
    prev_bidq = float(row.get("prev_bidq1", 0.0) or 0.0)
    prev_askq = float(row.get("prev_askq1", 0.0) or 0.0)

    if bid > prev_bid:
        bid_part = bidq
    elif bid < prev_bid:
        bid_part = -prev_bidq
    else:
        bid_part = bidq - prev_bidq

    if ask < prev_ask:
        ask_part = -askq
    elif ask > prev_ask:
        ask_part = prev_askq
    else:
        ask_part = prev_askq - askq
    return float(bid_part + ask_part)


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    if not INTRADAY_PRICES.exists():
        OUT_JSON.write_text(
            json.dumps({"ts": ts, "status": "MISSING_INTRADAY_PRICES", "path": str(INTRADAY_PRICES)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        pd.DataFrame().to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return 0

    rt = _load_lob_universe()
    if "code" not in rt.columns:
        OUT_JSON.write_text(json.dumps({"ts": ts, "status": "MISSING_CODE_COL"}, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame().to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return 0
    rt["code"] = rt["code"].astype(str).str.zfill(6)
    if len(rt) == 0:
        OUT_JSON.write_text(json.dumps({"ts": ts, "status": "NO_CODES"}, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame().to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return 0

    for c in ["current_price"] + _hoga_numeric_columns():
        if c not in rt.columns:
            rt[c] = 0.0
        rt[c] = pd.to_numeric(rt[c], errors="coerce").fillna(0.0)

    rt = _fill_hoga_from_ws(rt)
    rt = _fill_hoga_from_kis(rt)
    for c in ["current_price"] + _hoga_numeric_columns():
        rt[c] = pd.to_numeric(rt[c], errors="coerce").fillna(0.0)
    status_cols = [c for c in ["hoga_fetch_status", "hoga_fetch_error"] if c in rt.columns]
    source_cols = [c for c in ["lob_universe_source"] if c in rt.columns]
    df = rt[["code", "current_price"] + source_cols + _hoga_numeric_columns() + status_cols].copy()
    df.insert(0, "ts", ts)
    normal_candidate_codes = _load_normal_candidate_priority_codes()
    df["normal_candidate_flag"] = df["code"].astype(str).str.zfill(6).isin(normal_candidate_codes)
    spread = (df["ask1"] - df["bid1"]).clip(lower=0.0)
    mid = ((df["ask1"] + df["bid1"]) / 2.0).where((df["ask1"] > 0) & (df["bid1"] > 0), df["current_price"].clip(lower=0.0))
    df["mid_price"] = mid.fillna(0.0)
    df["spread"] = spread
    df["spread_bps"] = (spread / mid.replace(0, float("nan")) * 10000.0).fillna(0.0)
    denom = (df["bidq1"] + df["askq1"])
    df["order_imbalance_l1"] = ((df["bidq1"] - df["askq1"]) / denom.replace(0, float("nan"))).fillna(0.0)
    ask_depth_cols = [f"ask{i}" for i in range(1, HOGA_LEVELS + 1)]
    askq_depth_cols = [f"askq{i}" for i in range(1, HOGA_LEVELS + 1)]
    df["ask_depth_levels"] = sum(
        ((df[ask_col] > 0) & (df[askq_col] > 0)).astype(int)
        for ask_col, askq_col in zip(ask_depth_cols, askq_depth_cols)
    )
    lob_mask = (df["ask1"] > 0) & (df["bid1"] > 0)
    bid_only_limit_mask = (df["ask1"] <= 0) & (df["bid1"] > 0) & (df["current_price"] > 0) & (df["bid1"] >= df["current_price"])
    df["lob_available"] = lob_mask | bid_only_limit_mask
    df["lob_status"] = "NO_LOB"
    df.loc[lob_mask, "lob_status"] = "OK"
    df.loc[bid_only_limit_mask, "lob_status"] = "BID_ONLY_LIMIT"
    prev_state = _load_state()
    prev = pd.DataFrame.from_dict(prev_state, orient="index").reset_index().rename(columns={"index": "code"})
    if prev.empty:
        for c in ["prev_current_price", "prev_ask1", "prev_bid1", "prev_askq1", "prev_bidq1", "prev_mid_price"]:
            df[c] = 0.0
    else:
        prev["code"] = prev["code"].astype(str).str.zfill(6)
        prev = prev.rename(
            columns={
                "current_price": "prev_current_price",
                "ask1": "prev_ask1",
                "bid1": "prev_bid1",
                "askq1": "prev_askq1",
                "bidq1": "prev_bidq1",
                "mid_price": "prev_mid_price",
            }
        )
        keep_cols = ["code", "prev_current_price", "prev_ask1", "prev_bid1", "prev_askq1", "prev_bidq1", "prev_mid_price"]
        df = df.merge(prev[[c for c in keep_cols if c in prev.columns]], on="code", how="left")
        for c in keep_cols:
            if c != "code" and c not in df.columns:
                df[c] = 0.0
    for c in ["prev_current_price", "prev_ask1", "prev_bid1", "prev_askq1", "prev_bidq1", "prev_mid_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["ofi_valid"] = (
        df["lob_available"].astype(bool)
        & (df["prev_bid1"] > 0)
        & (df["prev_ask1"] > 0)
        & (df["prev_mid_price"] > 0)
    )
    df["ofi_l1"] = df.apply(_best_level_ofi, axis=1)
    depth_denom = (df["prev_bidq1"] + df["prev_askq1"] + df["bidq1"] + df["askq1"]).replace(0, float("nan"))
    df["ofi_norm"] = (df["ofi_l1"] / depth_denom).where(df["ofi_valid"], 0.0).fillna(0.0)
    df["markout_1step_bps"] = (
        (df["mid_price"] - df["prev_mid_price"]) / df["prev_mid_price"].replace(0, float("nan")) * 10000.0
    ).where(df["ofi_valid"], 0.0).fillna(0.0)
    df["kyle_lambda_l1"] = (df["markout_1step_bps"] / df["ofi_l1"].replace(0, float("nan"))).fillna(0.0)
    df["kyle_lambda_norm"] = (df["markout_1step_bps"] / df["ofi_norm"].replace(0, float("nan"))).fillna(0.0)
    valid_mask = df["ofi_valid"].astype(bool)
    df["ofi_z"] = 0.0
    df["kyle_lambda_z"] = 0.0
    df["markout_worse_z"] = 0.0
    if bool(valid_mask.any()):
        df.loc[valid_mask, "ofi_z"] = _zscore(df.loc[valid_mask, "ofi_norm"]).round(6)
        df.loc[valid_mask, "kyle_lambda_z"] = _zscore(df.loc[valid_mask, "kyle_lambda_norm"].abs()).round(6)
        df.loc[valid_mask, "markout_worse_z"] = _zscore(-df.loc[valid_mask, "markout_1step_bps"]).round(6)
    ofi_adverse = (-pd.to_numeric(df["ofi_z"], errors="coerce").fillna(0.0)).clip(lower=0.0) / 3.0
    lambda_risk = pd.to_numeric(df["kyle_lambda_z"], errors="coerce").fillna(0.0).clip(lower=0.0) / 3.0
    markout_risk = pd.to_numeric(df["markout_worse_z"], errors="coerce").fillna(0.0).clip(lower=0.0) / 3.0
    df["orderflow_risk_score"] = (ofi_adverse * 0.45 + lambda_risk * 0.35 + markout_risk * 0.20).map(_clip01).round(6)
    df.loc[~valid_mask, "orderflow_risk_score"] = 0.0
    df["orderflow_tag"] = "NO_HISTORY"
    df.loc[valid_mask, "orderflow_tag"] = "OK"
    df.loc[valid_mask & (df["orderflow_risk_score"] >= 0.60), "orderflow_tag"] = "CAUTION"
    df.loc[valid_mask & (df["orderflow_risk_score"] >= 0.85), "orderflow_tag"] = "PAUSE"
    df["orderflow_caution"] = df["orderflow_tag"].isin(["CAUTION", "PAUSE"])
    df["orderflow_pause"] = df["orderflow_tag"].eq("PAUSE")
    df["status"] = "OK"
    df["error"] = ""
    df = df.sort_values(["lob_status", "spread_bps"], ascending=[True, True])

    codes_ok = int((df["current_price"] > 0).sum())
    with_lob = int(lob_mask.sum())
    with_ask_depth = int((df["ask_depth_levels"] > 0).sum())
    lob_coverage_pct = round(with_lob / max(len(df), 1) * 100.0, 1)
    df["lob_evidence_usable"] = df["lob_available"].astype(bool)

    def _lob_evidence_reason(row: pd.Series) -> str:
        status = str(row.get("lob_status", "") or "")
        fetch_status = str(row.get("hoga_fetch_status", "") or "")
        if status == "OK":
            return "lob_available"
        if status == "BID_ONLY_LIMIT":
            return "bid_only_limit_available"
        parts = ["no_lob"]
        if fetch_status and fetch_status.lower() != "nan":
            parts.append(f"hoga_fetch_status={fetch_status}")
        return ";".join(parts)

    def _orderflow_evidence_reason(row: pd.Series) -> str:
        if bool(row.get("ofi_valid", False)):
            return "ofi_valid_with_previous_best_level"
        return (
            "ofi_unusable;"
            f"lob_available={bool(row.get('lob_available', False))};"
            f"prev_bid1={float(row.get('prev_bid1', 0.0) or 0.0):.3f};"
            f"prev_ask1={float(row.get('prev_ask1', 0.0) or 0.0):.3f};"
            f"prev_mid_price={float(row.get('prev_mid_price', 0.0) or 0.0):.3f}"
        )

    df["lob_evidence_reason"] = df.apply(_lob_evidence_reason, axis=1)
    df["orderflow_evidence_usable"] = df["ofi_valid"].astype(bool)
    df["orderflow_evidence_reason"] = df.apply(_orderflow_evidence_reason, axis=1)
    hoga_fetch_status_counts = (
        df["hoga_fetch_status"].astype(str).value_counts().to_dict()
        if "hoga_fetch_status" in df.columns
        else {}
    )
    evidence_unusable_reasons = []
    if with_lob <= 0:
        evidence_unusable_reasons.append(f"lob_coverage_pct={lob_coverage_pct}")
    if int(valid_mask.sum()) <= 0:
        evidence_unusable_reasons.append("ofi_valid_rows=0")
    ws_mode = str(_WS_HOGA_LOAD_META.get("mode", "") or "")
    if ws_mode:
        evidence_unusable_reasons.append(f"ws_hoga_load.mode={ws_mode}")
    for key, val in hoga_fetch_status_counts.items():
        evidence_unusable_reasons.append(f"hoga_fetch_status={key}:{val}")
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    state = {
        "ts": ts,
        "by_code": {
            str(row["code"]): {
                "current_price": float(row.get("current_price", 0.0) or 0.0),
                **{
                    col: float(row.get(col, 0.0) or 0.0)
                    for col in _hoga_numeric_columns()
                },
                "mid_price": float(row.get("mid_price", 0.0) or 0.0),
            }
            for _, row in df.iterrows()
        },
    }
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    payload = {
        "ts": ts,
        "status": "OK",
        "rows": int(len(df)),
        "codes_ok": int(codes_ok),
        "codes_with_lob": int(with_lob),
        "codes_with_ask_depth": int(with_ask_depth),
        "hoga_depth_levels": int(HOGA_LEVELS),
        "codes_no_lob": int(len(df) - with_lob),
        "lob_coverage_pct": lob_coverage_pct,
        "orderflow": {
            "source": "best_level_snapshot_delta",
            "state_path": str(STATE_JSON),
            "ofi_valid_rows": int(valid_mask.sum()),
            "evidence_usable": bool(valid_mask.sum() > 0),
            "evidence_reason": "ofi_valid_rows>0" if int(valid_mask.sum()) > 0 else "ofi_valid_rows=0",
            "caution_count": int((df["orderflow_tag"] == "CAUTION").sum()),
            "pause_count": int((df["orderflow_tag"] == "PAUSE").sum()),
            "max_risk_score": float(pd.to_numeric(df["orderflow_risk_score"], errors="coerce").fillna(0.0).max() if len(df) else 0.0),
        },
        "evidence_contract": {
            "lob_evidence_usable": bool(with_lob > 0),
            "orderflow_evidence_usable": bool(valid_mask.sum() > 0),
            "must_not_use_lob_for_entry": bool(with_lob <= 0),
            "must_not_use_orderflow_for_entry": bool(valid_mask.sum() <= 0),
            "unusable_reasons": evidence_unusable_reasons,
        },
        "lob_evidence_reason": "lob_available_rows>0" if with_lob > 0 else f"lob_coverage_pct={lob_coverage_pct}",
        "orderflow_evidence_reason": "ofi_valid_rows>0" if int(valid_mask.sum()) > 0 else "ofi_valid_rows=0",
        "source": "intraday_prices_latest.csv+market_rising_latest.csv+news_signal_lob_refresh_queue_latest.csv+candidates_latest_data.with_final_score.csv",
        "lob_universe": _LOB_UNIVERSE_META,
        "ws_hoga_load": _WS_HOGA_LOAD_META,
        "hoga_fetch_status_counts": hoga_fetch_status_counts,
        "out_csv": str(OUT_CSV),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
