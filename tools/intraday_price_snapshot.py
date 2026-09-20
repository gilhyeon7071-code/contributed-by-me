"""intraday_price_snapshot.py
Fetch current prices for candidate codes through the KIS inquire_price API.
Write the latest snapshot to 2_Logs/intraday_prices_latest.csv.

Usage:
    python intraday_price_snapshot.py --codes 005930,000660 [--mock auto|true|false]
    python intraday_price_snapshot.py --from-candidates [--mock auto|true|false]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from kis_order_client import KISApiError, KISOrderClient
from market_data_adapter import KISMarketDataAdapter

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_CSV = LOG_DIR / "intraday_prices_latest.csv"
CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.csv"
CANDIDATES_FALLBACK_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FUTURE_SIGNAL_PREVIEW_JSON = LOG_DIR / "future_signal_preview_latest.json"
PAPER_STATE_JSON = ROOT / "paper" / "paper_state.json"
SURGE_UNIVERSE_CSV = ROOT / "paper" / "surge_universe.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
RECHECK_QUEUE_JSON = LOG_DIR / "candidate_action_recheck_queue_latest.json"
SURGE_EV_SIMULATION_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
SURGE_NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SURGE_SCORE_RVOL_RECHECK_CSV = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.csv"
WS_TICKS_GLOB = "kis_ws_ticks_*.jsonl"
REGULAR_MARKET_CLOSE_HOUR = 15
REGULAR_MARKET_CLOSE_MINUTE = 30

HEADER = [
    "ts",
    "date",
    "code",
    "current_price",
    "open",
    "high",
    "low",
    "volume",
    "trading_value",
    "ask1",
    "bid1",
    "askq1",
    "bidq1",
]
HISTORY_ENV = "INTRADAY_PRICE_HISTORY_APPEND"

logger = logging.getLogger("intraday_price_snapshot")


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _non_trading_day_reason(now: Optional[dt.datetime] = None) -> str:
    """비거래일이면 사유를, 거래일이면 빈 문자열을 준다.

    [2026-09-13] holidays.json 이 정본이다 (주말 + 공휴일).
    읽지 못하면 **주말 검사만** 적용한다 - 못 읽었다고 통과시키지 않는다.
    """
    now = now or dt.datetime.now()
    if now.weekday() >= 5:
        return "주말(%s)" % ["월","화","수","목","금","토","일"][now.weekday()]
    try:
        obj = json.loads((ROOT / "holidays.json").read_text(encoding="utf-8-sig"))
        vals = obj.get("holidays") if isinstance(obj, dict) else obj
        hs = {"".join(ch for ch in str(v) if ch.isdigit())[:8] for v in (vals or [])}
    except Exception as exc:
        logging.getLogger(__name__).warning("[SNAP] holidays.json 을 읽지 못했다(%s). 주말 검사만 적용한다", type(exc).__name__)
        hs = set()
    return "휴장일" if now.strftime("%Y%m%d") in hs else ""


def _today_ymd() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def _resolve_mock_arg(args_mock: str, *, default_auto_mock: bool = True) -> bool:
    if args_mock == "true":
        return True
    if args_mock == "false":
        return False
    raw = str(os.getenv("KIS_MOCK", "")).strip().lower()
    if raw in {"1", "true", "y", "yes"}:
        return True
    if raw in {"0", "false", "n", "no"}:
        return False
    return bool(default_auto_mock)


def _intraday_status_output_paths(out_path: Path, status_path: Optional[Path] = None) -> dict[str, Path]:
    if status_path is not None:
        return {"status": status_path}
    paths = {"status": LOG_DIR / "intraday_prices_status.json"}
    if out_path.resolve() == OUT_CSV.resolve():
        paths["latest"] = LOG_DIR / "intraday_prices_status_latest.json"
        paths["dated"] = LOG_DIR / f"intraday_prices_status_{_today_ymd()}.json"
    return paths


def _write_intraday_status_payload(out_path: Path, payload: dict, status_path: Optional[Path] = None) -> dict[str, str]:
    paths = _intraday_status_output_paths(out_path, status_path=status_path)
    outputs = {key: str(path) for key, path in paths.items()}
    payload["status_outputs"] = outputs
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload_json, encoding="utf-8")
    return outputs


def _parse_codes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip()
        if not t:
            continue
        digits = "".join(ch for ch in t if ch.isdigit())
        if digits:
            vals.append(digits.zfill(6))
    return sorted(set(vals))


def _is_true_like(v: object) -> bool:
    s = str(v or "").strip().lower()
    return s in {"1", "true", "t", "y", "yes"}


def _to_int(v: object) -> int:
    try:
        return int(str(v or 0).replace(",", "") or 0)
    except Exception:
        return 0


def _to_float(v: object) -> float:
    try:
        return float(str(v or 0).replace(",", "") or 0)
    except Exception:
        return 0.0


def _parse_iso_dt(value: object) -> Optional[dt.datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _is_regular_session_ts(value: object) -> bool:
    parsed = _parse_iso_dt(value)
    if parsed is None:
        return False
    close_ts = parsed.replace(
        hour=REGULAR_MARKET_CLOSE_HOUR,
        minute=REGULAR_MARKET_CLOSE_MINUTE,
        second=0,
        microsecond=0,
    )
    return parsed <= close_ts


def _extract_codes_from_df(df) -> List[str]:
    col = next((c for c in df.columns if c.lower() == "code"), None)
    if col is None:
        return []
    return [str(v).strip().zfill(6) for v in df[col].dropna() if str(v).strip()]


def _load_surge_universe_codes(max_codes: int = 50, offset: int = 0) -> List[str]:
    """Load additional codes from surge_universe.csv. Returns [] if file missing."""
    if not SURGE_UNIVERSE_CSV.exists():
        return []
    try:
        import pandas as pd
        df = pd.read_csv(SURGE_UNIVERSE_CSV, dtype={"code": str})
        if "code" not in df.columns:
            return []
        codes = [str(v).strip().zfill(6) for v in df["code"].dropna() if str(v).strip()]
        if not codes:
            return []
        max_codes = max(1, int(max_codes or 1))
        offset = max(0, int(offset or 0)) % len(codes)
        rotated = codes[offset:] + codes[:offset]
        return rotated[:max_codes]
    except Exception as e:
        logger.warning("[SNAP] surge_universe load failed: %s", e)
        return []


def _load_realtime_surge_code_selection(max_codes: int = 50) -> tuple[List[str], dict]:
    """Load realtime surge codes for price-observation coverage, not entry approval."""
    include_policy_blocked = str(
        os.getenv("INTRADAY_PRICE_INCLUDE_POLICY_BLOCKED_SURGE", "1") or "1"
    ).strip().lower() not in {"0", "false", "no", "off"}
    meta = {
        "include_policy_blocked": bool(include_policy_blocked),
        "policy_change": False,
        "entry_approval_changed": False,
        "entry_signal": False,
        "trading_allowed": False,
        "purpose": "price_recheck_coverage_only",
    }
    if not SURGE_REALTIME_CSV.exists():
        meta["status"] = "NO_INPUT"
        return [], meta
    try:
        import pandas as pd

        df = pd.read_csv(SURGE_REALTIME_CSV, dtype=str).fillna("")
        if "code" not in df.columns:
            meta["status"] = "NO_CODE_COLUMN"
            return [], meta
        mask = pd.Series([True] * len(df), index=df.index)
        if "surge_flag" in df.columns:
            mask = mask & df["surge_flag"].apply(_is_true_like)
        if "is_realtime_surge" in df.columns:
            mask = mask & df["is_realtime_surge"].apply(_is_true_like)
        policy_blocked = pd.Series([False] * len(df), index=df.index)
        if "excluded_by_policy" in df.columns:
            policy_blocked = df["excluded_by_policy"].apply(_is_true_like)
        base = df[mask].copy()
        if not include_policy_blocked:
            work = df[mask & ~policy_blocked].copy()
        else:
            work = base
        if "surge_score_final" in work.columns:
            work["_score"] = pd.to_numeric(work["surge_score_final"], errors="coerce").fillna(0.0)
            work = work.sort_values("_score", ascending=False)
        codes = _extract_codes_from_df(work)
        out = sorted(set(codes), key=codes.index)[:max_codes]
        blocked_codes = _extract_codes_from_df(base[policy_blocked.loc[base.index]].copy()) if len(base) else []
        meta.update({
            "status": "OK",
            "source_rows": int(len(df)),
            "surge_rows": int(len(base)),
            "policy_blocked_surge_rows": int(policy_blocked.loc[base.index].sum()) if len(base) else 0,
            "policy_blocked_surge_codes": int(len(set(blocked_codes))),
            "selected_codes": int(len(out)),
            "selected_policy_blocked_codes": int(len(set(out) & set(blocked_codes))),
        })
        return out, meta
    except Exception as e:
        logger.warning("[SNAP] realtime_surge load failed: %s", e)
        meta.update({"status": "ERROR", "error": _format_error(e)})
        return [], meta


def _load_realtime_surge_codes(max_codes: int = 50) -> List[str]:
    codes, _meta = _load_realtime_surge_code_selection(max_codes=max_codes)
    return codes


def _load_recheck_observation_code_selection(max_codes: int = 30) -> tuple[List[str], dict]:
    """Load due recheck codes for price observation only, not entry approval."""
    enabled = str(os.getenv("INTRADAY_PRICE_INCLUDE_RECHECK_QUEUE", "1") or "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    meta = {
        "enabled": bool(enabled),
        "policy_change": False,
        "entry_approval_changed": False,
        "entry_signal": False,
        "trading_allowed": False,
        "purpose": "price_recheck_coverage_only",
    }
    if not enabled:
        meta["status"] = "DISABLED"
        return [], meta
    if not RECHECK_QUEUE_JSON.exists():
        meta["status"] = "NO_INPUT"
        return [], meta
    try:
        payload = json.loads(RECHECK_QUEUE_JSON.read_text(encoding="utf-8-sig"))
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            meta["status"] = "NO_ROWS"
            return [], meta

        selected_rows = []
        due_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or "").strip()
            digits = "".join(ch for ch in code if ch.isdigit())
            if not digits:
                continue
            status = str(row.get("recheck_status") or "").strip().upper()
            action = str(row.get("next_action") or "").strip().upper()
            if status != "DUE" or action not in {"RECHECK_15M", "PROMOTE_TO_RECHECK"}:
                continue
            due_at = _parse_iso_dt(row.get("due_at"))
            due_age_min = None
            if due_at is not None:
                due_age_min = max(0.0, (dt.datetime.now() - due_at).total_seconds() / 60.0)
            if due_age_min is None:
                recency_bucket = 4
            elif due_age_min <= 30.0:
                recency_bucket = 0
            elif due_age_min <= 120.0:
                recency_bucket = 1
            elif due_age_min <= 1440.0:
                recency_bucket = 2
            else:
                recency_bucket = 3
            reason = str(row.get("action_reason") or row.get("reason") or "").strip().upper()
            priority = 0.0
            try:
                priority = float(str(row.get("priority") or 0).strip() or 0)
            except Exception:
                priority = 0.0
            item = {
                "code": digits.zfill(6)[-6:],
                "action": action,
                "action_priority": 0 if action == "PROMOTE_TO_RECHECK" else 1,
                "priority": priority,
                "reason": reason,
                "due_age_min": due_age_min,
                "recency_bucket": recency_bucket,
                "policy_review_like": any(
                    token in reason
                    for token in (
                        "MISSED_MOVE",
                        "EARLY_",
                        "ENTRY_CHANGE_BLOCK",
                        "HIGH_REJECTION",
                        "RVOL",
                        "NO_LOB",
                    )
                ),
            }
            due_rows.append(item)

        selected_rows = sorted(
            due_rows,
            key=lambda x: (
                int(x.get("recency_bucket") or 4),
                int(x.get("action_priority") or 1),
                0 if bool(x.get("policy_review_like")) else 1,
                float(x.get("due_age_min") or 0.0),
                -float(x.get("priority") or 0.0),
                str(x.get("code") or ""),
            ),
        )
        codes = [str(row.get("code") or "") for row in selected_rows if str(row.get("code") or "")]
        out = sorted(set(codes), key=codes.index)[:max_codes]
        meta.update({
            "status": "OK",
            "source_rows": int(len(rows)),
            "due_rows": int(len(due_rows)),
            "selected_codes": int(len(out)),
            "selected_policy_review_like_codes": int(
                len({
                    str(row.get("code") or "")
                    for row in selected_rows[:max_codes]
                    if bool(row.get("policy_review_like")) and str(row.get("code") or "")
                })
            ),
            "selected_promote_to_recheck_codes": int(
                len({
                    str(row.get("code") or "")
                    for row in selected_rows[:max_codes]
                    if str(row.get("action") or "") == "PROMOTE_TO_RECHECK" and str(row.get("code") or "")
                })
            ),
            "selected_recheck_15m_codes": int(
                len({
                    str(row.get("code") or "")
                    for row in selected_rows[:max_codes]
                    if str(row.get("action") or "") == "RECHECK_15M" and str(row.get("code") or "")
                })
            ),
            "selected_fresh_30m_codes": int(
                len({
                    str(row.get("code") or "")
                    for row in selected_rows[:max_codes]
                    if int(row.get("recency_bucket") or 4) == 0 and str(row.get("code") or "")
                })
            ),
            "selected_fresh_2h_codes": int(
                len({
                    str(row.get("code") or "")
                    for row in selected_rows[:max_codes]
                    if int(row.get("recency_bucket") or 4) <= 1 and str(row.get("code") or "")
                })
            ),
            "max_codes": int(max_codes),
        })
        return out, meta
    except Exception as e:
        logger.warning("[SNAP] recheck queue load failed: %s", e)
        meta.update({"status": "ERROR", "error": _format_error(e)})
        return [], meta


def _load_ev_pending_observation_code_selection(max_codes: int = 30) -> tuple[List[str], dict]:
    """Load EV shadow rows that still need price history for evaluation."""
    enabled = str(os.getenv("INTRADAY_PRICE_INCLUDE_EV_PENDING", "1") or "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    meta = {
        "enabled": bool(enabled),
        "policy_change": False,
        "entry_approval_changed": False,
        "entry_signal": False,
        "trading_allowed": False,
        "purpose": "ev_shadow_price_coverage_only",
    }
    if not enabled:
        meta["status"] = "DISABLED"
        return [], meta
    if not SURGE_EV_SIMULATION_JSON.exists():
        meta["status"] = "NO_INPUT"
        return [], meta
    try:
        payload = json.loads(SURGE_EV_SIMULATION_JSON.read_text(encoding="utf-8-sig"))
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            meta["status"] = "NO_ROWS"
            return [], meta
        selected: List[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status") or "").strip().upper()
            if status == "EVALUATED":
                continue
            code = "".join(ch for ch in str(row.get("code") or "") if ch.isdigit()).zfill(6)
            if not code or code == "000000":
                continue
            selected.append({
                "code": code,
                "status": status,
                "signal_ts": str(row.get("signal_ts") or ""),
                "source": str(row.get("source") or ""),
            })
        selected = sorted(
            selected,
            key=lambda x: (
                0 if x["status"] == "NOT_EVALUABLE_WAITING_PRIMARY_TIMEBOX" else 1,
                str(x.get("signal_ts") or ""),
                str(x.get("code") or ""),
            ),
        )
        codes = []
        for item in selected:
            if item["code"] not in codes:
                codes.append(item["code"])
            if len(codes) >= max_codes:
                break
        meta.update({
            "status": "OK",
            "source_rows": int(len(rows)),
            "pending_rows": int(len(selected)),
            "selected_codes": int(len(codes)),
            "selected_status_counts": [
                {"status": key, "count": int(value)}
                for key, value in __import__("collections").Counter(item["status"] for item in selected).most_common()
            ],
        })
        return codes, meta
    except Exception as e:
        logger.warning("[SNAP] ev_pending_observation load failed: %s", e)
        meta.update({"status": "ERROR", "error": _format_error(e)})
        return [], meta


def _load_ev_source_queue_observation_code_selection(max_codes: int = 30) -> tuple[List[str], dict]:
    """Load EV source queue codes for price observation before EV simulation exists."""
    enabled = str(os.getenv("INTRADAY_PRICE_INCLUDE_EV_SOURCE_QUEUES", "1") or "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    meta = {
        "enabled": bool(enabled),
        "policy_change": False,
        "entry_approval_changed": False,
        "entry_signal": False,
        "trading_allowed": False,
        "purpose": "ev_source_queue_price_coverage_only",
    }
    if not enabled:
        meta["status"] = "DISABLED"
        return [], meta

    source_paths = [
        ("NO_LOB_RECHECK_SOURCE", SURGE_NO_LOB_RECHECK_CSV),
        ("SCORE_RVOL_SOURCE", SURGE_SCORE_RVOL_RECHECK_CSV),
    ]
    selected: List[dict] = []
    source_stats: List[dict] = []
    try:
        for source_name, path in source_paths:
            rows: List[dict] = []
            if path.exists() and path.stat().st_size > 5:
                with path.open("r", encoding="utf-8-sig", newline="") as fp:
                    rows = list(csv.DictReader(fp))
            kept = 0
            skipped_after_market = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if not _is_regular_session_ts(row.get("ts")):
                    skipped_after_market += 1
                    continue
                code = "".join(ch for ch in str(row.get("code") or "") if ch.isdigit()).zfill(6)
                if not code or code == "000000":
                    continue
                selected.append({
                    "code": code,
                    "source": source_name,
                    "ts": str(row.get("ts") or ""),
                    "score": _to_float(row.get("surge_score_final")),
                })
                kept += 1
            source_stats.append({
                "source": source_name,
                "path": str(path),
                "rows": int(len(rows)),
                "kept_regular_session_rows": int(kept),
                "skipped_after_market_rows": int(skipped_after_market),
            })
        selected = sorted(
            selected,
            key=lambda x: (
                str(x.get("ts") or ""),
                -float(x.get("score") or 0.0),
                str(x.get("code") or ""),
            ),
        )
        codes: List[str] = []
        for item in selected:
            code = str(item.get("code") or "")
            if code and code not in codes:
                codes.append(code)
            if len(codes) >= max_codes:
                break
        meta.update({
            "status": "OK",
            "source_stats": source_stats,
            "selected_rows": int(len(selected)),
            "selected_codes": int(len(codes)),
            "max_codes": int(max_codes),
        })
        return codes, meta
    except Exception as e:
        logger.warning("[SNAP] ev_source_queue_observation load failed: %s", e)
        meta.update({"status": "ERROR", "error": _format_error(e)})
        return [], meta


def _load_future_signal_preview_codes() -> List[str]:
    """Load read-only future-signal preview codes for intraday validation coverage."""
    if not FUTURE_SIGNAL_PREVIEW_JSON.exists():
        return []
    try:
        import pandas as pd

        meta = json.loads(FUTURE_SIGNAL_PREVIEW_JSON.read_text(encoding="utf-8"))
        csv_path = Path(str((meta.get("outputs") or {}).get("csv", "")))
        if not csv_path.exists():
            return []
        df = pd.read_csv(csv_path, dtype=str)
        if "code" not in df.columns:
            return []
        if "horizon_type" in df.columns:
            df = df[df["horizon_type"].astype(str).str.upper() == "INTRADAY"].copy()
        return sorted(set(_extract_codes_from_df(df)))
    except Exception as e:
        logger.warning("[SNAP] future_signal_preview load failed: %s", e)
        return []


def _load_codes_from_candidates() -> List[str]:
    """Load intraday codes from candidates with safe fallbacks."""
    try:
        import pandas as pd
        future_codes = _load_future_signal_preview_codes()
        open_codes = _load_open_position_codes()
        if CANDIDATES_CSV.exists():
            df = pd.read_csv(CANDIDATES_CSV, dtype=str)
            codes = _extract_codes_from_df(df)
            if codes:
                merged = sorted(set(codes + future_codes + open_codes))
                if future_codes:
                    logger.info("[SNAP] future_signal_preview merged: +%d codes", len(merged) - len(set(codes)))
                if open_codes:
                    logger.info("[SNAP] open_positions merged: +%d codes", len(set(open_codes) - set(codes)))
                return merged
            logger.warning("[SNAP] candidates primary empty: %s", CANDIDATES_CSV)
        else:
            logger.warning("[SNAP] candidates csv not found: %s", CANDIDATES_CSV)

        if CANDIDATES_FALLBACK_CSV.exists():
            fb = pd.read_csv(CANDIDATES_FALLBACK_CSV, dtype=str)
            if "execution_pool" in fb.columns:
                fb = fb[fb["execution_pool"].apply(_is_true_like)].copy()
            codes = _extract_codes_from_df(fb)
            if codes:
                logger.info("[SNAP] fallback candidates used: %s rows=%d", CANDIDATES_FALLBACK_CSV, len(codes))
                return sorted(set(codes + future_codes + open_codes))

        if PAPER_STATE_JSON.exists():
            obj = json.loads(PAPER_STATE_JSON.read_text(encoding="utf-8"))
            pos = obj.get("open_positions", []) if isinstance(obj, dict) else []
            codes: List[str] = []
            if isinstance(pos, list):
                for row in pos:
                    code = str((row or {}).get("code", "")).strip()
                    digits = "".join(ch for ch in code if ch.isdigit())
                    if len(digits) <= 6 and digits:
                        codes.append(digits.zfill(6))
            codes = [c for c in codes if len(c) == 6 and c.isdigit()]
            if codes:
                logger.info("[SNAP] paper_state fallback used: open_positions=%d", len(codes))
                return sorted(set(codes + future_codes))
        if future_codes:
            logger.info("[SNAP] future_signal_preview-only codes used: rows=%d", len(future_codes))
            return sorted(set(future_codes))
    except Exception as e:
        logger.warning("[SNAP] failed to read candidates csv: %s", e)
        return []
    return []


def _load_open_position_codes() -> List[str]:
    if not PAPER_STATE_JSON.exists():
        return []
    try:
        obj = json.loads(PAPER_STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return []
    pos = obj.get("open_positions", []) if isinstance(obj, dict) else []
    out: List[str] = []
    if not isinstance(pos, list):
        return out
    for row in pos:
        code = str((row or {}).get("code", "")).strip()
        digits = "".join(ch for ch in code if ch.isdigit())
        if digits:
            out.append(digits.zfill(6))
    return sorted(set(out))


def _latest_ws_ticks_jsonl() -> Optional[Path]:
    files = []
    for path in LOG_DIR.glob(WS_TICKS_GLOB):
        suffix = path.stem.removeprefix("kis_ws_ticks_")
        if len(suffix) == 8 and suffix.isdigit():
            files.append(path)
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0] if files else None


def _iter_recent_jsonl_lines(path: Path, tail_bytes: int) -> List[str]:
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


def _ws_trade_chunks(fields: List[str]) -> List[List[str]]:
    if len(fields) < 20:
        return []
    chunk_size = 46 if len(fields) >= 46 else len(fields)
    chunks: List[List[str]] = []
    for i in range(0, len(fields), chunk_size):
        part = fields[i : i + chunk_size]
        if len(part) >= 20:
            chunks.append(part)
    return chunks


def _load_ws_trade_prefill(codes: List[str]) -> tuple[dict[str, dict], dict]:
    enabled = str(os.getenv("INTRADAY_PRICE_WS_PREFILL", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
    started = time.time()
    code_set = {str(c).zfill(6) for c in codes}
    meta = {"enabled": bool(enabled), "source": "kis_ws_ticks_tail", "codes": 0, "elapsed_sec": 0.0}
    if not enabled or not code_set:
        meta["elapsed_sec"] = round(time.time() - started, 3)
        return {}, meta
    path = _latest_ws_ticks_jsonl()
    if path is None:
        meta.update({"status": "NO_INPUT", "elapsed_sec": round(time.time() - started, 3)})
        return {}, meta
    max_age_sec = max(1.0, float(str(os.getenv("INTRADAY_PRICE_WS_MAX_AGE_SEC", "300")).strip() or "300"))
    try:
        age_sec = max(0.0, time.time() - path.stat().st_mtime)
        if age_sec > max_age_sec:
            meta.update({"status": "STALE_INPUT", "path": str(path), "age_sec": round(age_sec, 3), "max_age_sec": max_age_sec, "elapsed_sec": round(time.time() - started, 3)})
            return {}, meta
    except Exception:
        meta.update({"status": "STAT_ERROR", "path": str(path), "elapsed_sec": round(time.time() - started, 3)})
        return {}, meta
    tail_mb = max(0.0, float(str(os.getenv("INTRADAY_PRICE_WS_TAIL_MB", "24")).strip() or "24"))
    tail_bytes = int(tail_mb * 1024 * 1024)
    out: dict[str, dict] = {}
    line_count = 0
    try:
        for line in _iter_recent_jsonl_lines(path, tail_bytes):
            line_count += 1
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if str(obj.get("tr_id") or "").strip() != "H0STCNT0":
                continue
            norm = obj.get("normalized") if isinstance(obj.get("normalized"), dict) else {}
            fields = norm.get("fields") if isinstance(norm.get("fields"), list) else obj.get("fields")
            if not isinstance(fields, list) or len(fields) < 20:
                raw = str(norm.get("raw_payload") or obj.get("payload") or "")
                fields = raw.split("^") if raw else []
            for part in _ws_trade_chunks([str(x) for x in fields]):
                code = "".join(ch for ch in str(part[0]) if ch.isdigit()).zfill(6)
                if code not in code_set:
                    continue
                current_price = _to_int(part[2] if len(part) > 2 else 0)
                if current_price <= 0:
                    continue
                row = {
                    "ts": _now_ts(),
                    "date": _today_ymd(),
                    "code": code,
                    "current_price": current_price,
                    "open": _to_int(part[7] if len(part) > 7 else 0),
                    "high": _to_int(part[8] if len(part) > 8 else 0),
                    "low": _to_int(part[9] if len(part) > 9 else 0),
                    "volume": _to_int(part[13] if len(part) > 13 else 0),
                    "trading_value": _to_int(part[14] if len(part) > 14 else 0),
                    "ask1": 0,
                    "bid1": 0,
                    "askq1": 0,
                    "bidq1": 0,
                }
                out[code] = row
    except Exception as e:
        meta.update({"status": "ERROR", "path": str(path), "error": _format_error(e), "elapsed_sec": round(time.time() - started, 3)})
        return {}, meta
    meta.update({
        "status": "OK",
        "path": str(path),
        "tail_mb": float(tail_mb),
        "file_size_bytes": int(path.stat().st_size),
        "line_count": int(line_count),
        "codes": int(len(out)),
        "elapsed_sec": round(time.time() - started, 3),
    })
    return out, meta


def _write_csv(rows: List[dict]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)


def _append_history_csv(rows: List[dict], history_path: Path) -> dict:
    guard = {
        "enabled": True,
        "input_rows": int(len(rows)),
        "valid_price_rows": 0,
        "appended_rows": 0,
        "skipped_trading_value_decrease": 0,
        "skipped_volume_decrease": 0,
        "skipped_zero_reset": 0,
        "skipped_rows": [],
    }
    valid_rows = [r for r in rows if int(r.get("current_price", 0) or 0) > 0]
    guard["valid_price_rows"] = int(len(valid_rows))
    if not valid_rows:
        return guard

    previous_by_code: dict[str, dict] = {}
    if history_path.exists() and history_path.stat().st_size > 0:
        try:
            import pandas as pd

            hist = pd.read_csv(history_path, dtype={"code": str})
            if not hist.empty and "code" in hist.columns:
                hist["code"] = hist["code"].astype(str).str.zfill(6)
                if "ts" in hist.columns:
                    hist = hist.sort_values(["code", "ts"])
                for code, part in hist.groupby("code", sort=False):
                    last = part.iloc[-1]
                    previous_by_code[str(code).zfill(6)] = {
                        "trading_value": _to_int(last.get("trading_value", 0)),
                        "volume": _to_int(last.get("volume", 0)),
                    }
        except Exception as e:
            guard["previous_read_error"] = _format_error(e)

    filtered_rows: List[dict] = []
    for row in valid_rows:
        code = str(row.get("code", "")).zfill(6)
        prev = previous_by_code.get(code)
        tv = _to_int(row.get("trading_value", 0))
        vol = _to_int(row.get("volume", 0))
        skip_reasons: List[str] = []
        if prev:
            prev_tv = int(prev.get("trading_value", 0) or 0)
            prev_vol = int(prev.get("volume", 0) or 0)
            if prev_tv > 0 and tv == 0:
                skip_reasons.append("trading_value_zero_reset")
            elif prev_tv > 0 and 0 < tv < prev_tv:
                skip_reasons.append("trading_value_decrease")
                guard["skipped_trading_value_decrease"] += 1
            if prev_vol > 0 and vol == 0:
                if "trading_value_zero_reset" not in skip_reasons:
                    skip_reasons.append("volume_zero_reset")
            elif prev_vol > 0 and 0 < vol < prev_vol:
                skip_reasons.append("volume_decrease")
                guard["skipped_volume_decrease"] += 1
        if skip_reasons:
            if any(reason.endswith("zero_reset") for reason in skip_reasons):
                guard["skipped_zero_reset"] += 1
            if len(guard["skipped_rows"]) < 20:
                guard["skipped_rows"].append(
                    {
                        "code": code,
                        "reason": ",".join(dict.fromkeys(skip_reasons)),
                        "prev_trading_value": int((prev or {}).get("trading_value", 0) or 0),
                        "trading_value": int(tv),
                        "prev_volume": int((prev or {}).get("volume", 0) or 0),
                        "volume": int(vol),
                    }
                )
            continue
        filtered_rows.append(row)

    if not filtered_rows:
        return guard
    history_path.parent.mkdir(parents=True, exist_ok=True)
    exists = history_path.exists() and history_path.stat().st_size > 0
    with history_path.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        if not exists:
            w.writeheader()
        w.writerows(filtered_rows)
    guard["appended_rows"] = int(len(filtered_rows))
    return guard

def _count_valid_rows(csv_path: Path) -> int:
    try:
        import pandas as pd

        if not csv_path.exists():
            return 0
        df = pd.read_csv(csv_path, dtype={"code": str})
        if "current_price" not in df.columns:
            return 0
        s = pd.to_numeric(df["current_price"], errors="coerce").fillna(0.0)
        return int((s > 0).sum())
    except Exception:
        return 0


def _apply_snapshot_failfast_defaults() -> None:
    """Keep realtime price refresh bounded; engine can fall back to last/parquet prices."""
    os.environ.setdefault("KIS_TOKEN_RETRY_MAX", "0")
    os.environ.setdefault("KIS_TOKEN_RETRY_WAIT_SEC", "0")
    os.environ.setdefault("KIS_SHARED_BUDGET_LOCK_TIMEOUT_SEC", "1.0")
    os.environ.setdefault("KIS_RATE_LIMIT_RETRIES", "0")


def _format_error(exc: Optional[Exception]) -> str:
    if exc is None:
        return ""
    text = str(exc).strip()
    if len(text) > 240:
        text = text[:237] + "..."
    return f"{type(exc).__name__}:{text}"


def _is_rate_limit_error(exc: Optional[Exception]) -> bool:
    text = str(exc or "")
    return "EGW00201" in text or "초당 거래건수" in text


def fetch_prices(
    client: KISOrderClient,
    codes: List[str],
    retry: int = 2,
    retry_sleep: float = 0.5,
    hoga_fallback_mode: str = "surge_only",
    hoga_fallback_codes: Optional[set[str]] = None,
    workers: int = 1,
    request_interval_sec: float = 0.0,
    rate_limit_retry_max: int = 0,
    rate_limit_retry_sleep: float = 1.0,
    max_elapsed_sec: float = 0.0,
) -> tuple[List[dict], List[dict]]:
    adapter = KISMarketDataAdapter()
    today = _today_ymd()
    ts = _now_ts()

    def _zero_pair(code: str, error: str, elapsed_sec: float = 0.0) -> tuple[dict, dict]:
        return (
            {
                "ts": ts,
                "date": today,
                "code": code,
                "current_price": 0,
                "open": 0,
                "high": 0,
                "low": 0,
                "volume": 0,
                "trading_value": 0,
                "ask1": 0,
                "bid1": 0,
                "askq1": 0,
                "bidq1": 0,
            },
            {
                "code": code,
                "elapsed_sec": round(float(elapsed_sec), 3),
                "attempts": 0,
                "hoga_fallback": False,
                "hoga_fallback_allowed": False,
                "hoga_fallback_error": "",
                "rate_limit_retries": 0,
                "ok": False,
                "error": str(error),
            },
        )

    def _fetch_one(code: str) -> tuple[dict, dict]:
        code_started = time.time()
        last_err: Optional[Exception] = None
        attempts_used = 0
        hoga_fallback_used = False
        hoga_fallback_error = ""
        rate_limit_retries_used = 0
        row: dict = {}
        if hoga_fallback_mode == "off":
            allow_hoga_fallback = False
        elif hoga_fallback_mode == "all":
            allow_hoga_fallback = True
        else:
            allow_hoga_fallback = code in (hoga_fallback_codes or set())
        attempt = 0
        while True:
            attempts_used += 1
            try:
                body = adapter.fetch_ticker(client, code)
                quote = adapter.parse_ticker(body)
                current_price = int(quote.get("current_price", 0) or 0)
                open_price = int(quote.get("open", 0) or 0)
                high_price = int(quote.get("high", 0) or 0)
                low_price = int(quote.get("low", 0) or 0)
                volume = int(quote.get("volume", 0) or 0)
                trading_value = int(quote.get("trading_value", 0) or 0)
                ask1 = int(quote.get("ask1", 0) or 0)
                bid1 = int(quote.get("bid1", 0) or 0)
                askq1 = int(quote.get("askq1", 0) or 0)
                bidq1 = int(quote.get("bidq1", 0) or 0)

                # H0STASP0-equivalent REST fallback:
                # inquire-price 응답에서 호가가 비면 호가 전용 API로 보강.
                if allow_hoga_fallback and (ask1 <= 0 or bid1 <= 0):
                    try:
                        hoga = adapter.fetch_orderbook(client, code)
                        hoga_fallback_used = True
                        book = adapter.parse_orderbook(hoga)
                        ask1 = max(ask1, int(book.get("ask1", 0) or 0))
                        bid1 = max(bid1, int(book.get("bid1", 0) or 0))
                        askq1 = max(askq1, int(book.get("askq1", 0) or 0))
                        bidq1 = max(bidq1, int(book.get("bidq1", 0) or 0))
                    except Exception as hoga_err:
                        hoga_fallback_error = _format_error(hoga_err)
                        logger.debug("[SNAP] %s hoga fallback unavailable: %s", code, hoga_err)
                row = {
                    "ts": ts,
                    "date": today,
                    "code": code,
                    "current_price": current_price,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "volume": volume,
                    "trading_value": trading_value,
                    "ask1": ask1,
                    "bid1": bid1,
                    "askq1": askq1,
                    "bidq1": bidq1,
                }
                logger.info("[SNAP] %s current=%d open=%d high=%d low=%d vol=%d",
                            code, current_price, open_price, high_price, low_price, volume)
                last_err = None
                break
            except KISApiError as e:
                last_err = e
                if _is_rate_limit_error(e) and rate_limit_retries_used < max(0, int(rate_limit_retry_max or 0)):
                    rate_limit_retries_used += 1
                    time.sleep(max(0.0, float(rate_limit_retry_sleep or 0.0)))
                    continue
                if attempt < retry:
                    attempt += 1
                    time.sleep(retry_sleep)
                    continue
                break
            except Exception as e:
                last_err = e
                if attempt < retry:
                    attempt += 1
                    time.sleep(retry_sleep)
                    continue
                break
        if last_err is not None:
            logger.warning("[SNAP] %s fetch failed: %s", code, last_err)
            # Record failed symbols as zero so paper_engine can fall back to parquet prices.
            row = {
                "ts": ts,
                "date": today,
                "code": code,
                "current_price": 0,
                "open": 0,
                "high": 0,
                "low": 0,
                "volume": 0,
                "trading_value": 0,
                "ask1": 0,
                "bid1": 0,
                "askq1": 0,
                "bidq1": 0,
            }
        perf = {
            "code": code,
            "elapsed_sec": round(time.time() - code_started, 3),
            "attempts": int(attempts_used),
            "hoga_fallback": bool(hoga_fallback_used),
            "hoga_fallback_allowed": bool(allow_hoga_fallback),
            "hoga_fallback_error": hoga_fallback_error,
            "rate_limit_retries": int(rate_limit_retries_used),
            "ok": bool(last_err is None),
            "error": _format_error(last_err),
        }
        return row, perf

    max_workers = max(1, int(workers or 1))
    fetch_started = time.time()
    budget = max(0.0, float(max_elapsed_sec or 0.0))
    if max_workers <= 1 or len(codes) <= 1:
        pairs = []
        last_request_started = 0.0
        interval = max(0.0, float(request_interval_sec or 0.0))
        for idx, code in enumerate(codes):
            if budget > 0.0 and (time.time() - fetch_started) >= budget:
                remaining = codes[idx:]
                logger.warning(
                    "[SNAP] max_elapsed budget reached: fetched=%d remaining=%d budget=%.1fs",
                    len(pairs),
                    len(remaining),
                    budget,
                )
                pairs.extend(
                    _zero_pair(rest_code, "MAX_ELAPSED_BUDGET_EXCEEDED", time.time() - fetch_started)
                    for rest_code in remaining
                )
                break
            if interval > 0.0 and last_request_started > 0.0:
                wait_sec = interval - (time.time() - last_request_started)
                if wait_sec > 0.0:
                    time.sleep(wait_sec)
            last_request_started = time.time()
            pairs.append(_fetch_one(code))
    else:
        pairs_by_code: dict[str, tuple[dict, dict]] = {}
        with ThreadPoolExecutor(max_workers=min(max_workers, len(codes))) as executor:
            futures = {executor.submit(_fetch_one, code): code for code in codes}
            for fut in as_completed(futures):
                code = futures[fut]
                try:
                    pairs_by_code[code] = fut.result()
                except Exception as e:
                    logger.warning("[SNAP] %s worker failed: %s", code, e)
                    pairs_by_code[code] = (
                        {
                            "ts": ts,
                            "date": today,
                            "code": code,
                            "current_price": 0,
                            "open": 0,
                            "high": 0,
                            "low": 0,
                            "volume": 0,
                            "trading_value": 0,
                            "ask1": 0,
                            "bid1": 0,
                            "askq1": 0,
                            "bidq1": 0,
                        },
                        {
                            "code": code,
                            "elapsed_sec": 0.0,
                            "attempts": 0,
                            "hoga_fallback": False,
                            "hoga_fallback_allowed": False,
                            "hoga_fallback_error": "",
                            "rate_limit_retries": 0,
                            "ok": False,
                            "error": _format_error(e),
                        },
                    )
        pairs = [pairs_by_code[code] for code in codes]
    rows = [row for row, _perf in pairs]
    perf_rows = [perf for _row, perf in pairs]
    return rows, perf_rows


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(levelname)s] %(asctime)s %(name)s - %(message)s",
        )

    ap = argparse.ArgumentParser(description="Fetch real-time prices for candidates via KIS API")
    ap.add_argument("--codes", default="", help="Comma-separated 6-digit codes")
    ap.add_argument("--codes-only", action="store_true", help="Use only --codes and skip candidate/surge/recheck/EV/future source merges.")
    ap.add_argument("--from-candidates", action="store_true", help="Load codes from candidates_latest_data.csv")
    ap.add_argument("--with-surge-universe", action="store_true", help="Merge surge_universe.csv codes for surge detection")
    ap.add_argument("--surge-universe-max", type=int, default=50, help="Max codes to load from surge_universe.csv")
    ap.add_argument("--surge-universe-offset", type=int, default=0, help="Rotating offset for surge_universe.csv")
    ap.add_argument("--max-total-codes", type=int, default=int(str(os.getenv("INTRADAY_PRICE_MAX_TOTAL_CODES", "0") or "0")),
                    help="Cap final requested codes while preserving non-surge priority observation codes; 0 disables")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--allow-non-trading-day", action="store_true",
                    help="비거래일에도 스냅샷을 찍는다 (시험·복구용). 기본은 건너뛴다")
    ap.add_argument("--retry", type=int, default=2)
    ap.add_argument("--retry-sleep", type=float, default=0.5)
    ap.add_argument("--kis-timeout-sec", type=float, default=float(str(os.getenv("INTRADAY_KIS_TIMEOUT_SEC", "3.5") or "3.5")))
    ap.add_argument("--workers", type=int, default=int(str(os.getenv("INTRADAY_PRICE_WORKERS", "1") or "1")))
    ap.add_argument("--request-interval-sec", type=float, default=float(str(os.getenv("INTRADAY_PRICE_REQUEST_INTERVAL_SEC", "1.05") or "1.05")))
    ap.add_argument("--rate-limit-retry-max", type=int, default=int(str(os.getenv("INTRADAY_PRICE_RATE_LIMIT_RETRY_MAX", "1") or "1")))
    ap.add_argument("--rate-limit-retry-sleep", type=float, default=float(str(os.getenv("INTRADAY_PRICE_RATE_LIMIT_RETRY_SLEEP", "1.15") or "1.15")))
    ap.add_argument("--max-elapsed-sec", type=float, default=float(str(os.getenv("INTRADAY_PRICE_MAX_ELAPSED_SEC", "0") or "0")))
    ap.add_argument("--hoga-fallback-mode", default=str(os.getenv("INTRADAY_HOGA_FALLBACK_MODE", "off") or "off"),
                    choices=["surge_only", "all", "off"])
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-status-json", default="", help="Write status JSON to this path only; default preserves canonical status outputs.")
    ap.add_argument("--history-csv", default="", help="Append valid snapshots to this history CSV.")
    ap.add_argument("--no-history-append", action="store_true", help="Disable intraday history append.")
    ap.add_argument("--ev-source-queues-only", action="store_true", help="Fetch only EV source queue observation codes.")
    args = ap.parse_args()

    # [2026-09-13] **비거래일에는 '장중' 스냅샷을 찍지 않는다.**
    #   근본 원인이었다 - 2026-09-12(토) 배치가 여기서 35종목을 받아
    #   `date=20260912` 로 도장을 찍었다. 그날은 장이 없었으므로 그 값은 09-11 종가다.
    #   다음날 surge_detector_realtime 이 '기대 20260911 / 실제 20260912' 로 거부하고
    #   rc=1 로 죽었다(출력 0바이트). 배치 실패의 진짜 출처다.
    #   비거래일은 결함이 아니므로 rc=0 으로 끝내고 **기존 파일을 덮지 않는다** -
    #   그래야 마지막 실제 세션 스냅샷이 남아 다음날 날짜 대조가 맞는다.
    #   끄려면 --allow-non-trading-day (시험·복구용).
    if not args.allow_non_trading_day:
        _reason = _non_trading_day_reason()
        if _reason:
            logging.getLogger(__name__).warning(
                "[SNAP] skip - %s (%s). 기존 %s 를 보존한다",
                _reason, _today_ymd(), OUT_CSV.name)
            return 0

    mock_flag: Optional[bool] = _resolve_mock_arg(args.mock, default_auto_mock=True)

    codes: List[str] = []
    priority_codes: List[str] = []

    def _remember_priority(new_codes: List[str]) -> None:
        seen = set(priority_codes)
        for code in new_codes:
            code_s = str(code or "").strip().zfill(6)
            if code_s and code_s not in seen:
                priority_codes.append(code_s)
                seen.add(code_s)

    if args.codes_only:
        explicit_codes = _parse_codes(args.codes)
        _remember_priority(explicit_codes)
        codes = sorted(set(explicit_codes))
        realtime_surge_codes, realtime_surge_selection = [], {"status": "SKIPPED_CODES_ONLY"}
        recheck_observation_codes, recheck_observation_selection = [], {"status": "SKIPPED_CODES_ONLY"}
    elif args.ev_source_queues_only:
        realtime_surge_codes, realtime_surge_selection = [], {"status": "SKIPPED_EV_SOURCE_QUEUES_ONLY"}
        recheck_observation_codes, recheck_observation_selection = [], {"status": "SKIPPED_EV_SOURCE_QUEUES_ONLY"}
    else:
        if args.from_candidates:
            codes = _load_codes_from_candidates()
            _remember_priority(codes)
            logger.info("[SNAP] loaded %d codes from candidates", len(codes))
        if args.codes:
            explicit_codes = _parse_codes(args.codes)
            _remember_priority(explicit_codes)
            codes = sorted(set(codes + explicit_codes))
        if args.with_surge_universe:
            surge_codes = _load_surge_universe_codes(
                max_codes=args.surge_universe_max,
                offset=args.surge_universe_offset,
            )
            before = len(codes)
            codes = sorted(set(codes + surge_codes))
            logger.info(
                "[SNAP] surge_universe merged: +%d codes (total %d, max=%d offset=%d)",
                len(codes) - before,
                len(codes),
                int(args.surge_universe_max),
                int(args.surge_universe_offset),
            )
        realtime_surge_codes, realtime_surge_selection = _load_realtime_surge_code_selection(
            max_codes=args.surge_universe_max
        )
        if realtime_surge_codes:
            _remember_priority(realtime_surge_codes)
            before = len(codes)
            codes = sorted(set(codes + realtime_surge_codes))
            logger.info("[SNAP] realtime_surge merged: +%d codes (total %d)", len(codes) - before, len(codes))
        recheck_observation_codes, recheck_observation_selection = _load_recheck_observation_code_selection(
            max_codes=int(str(os.getenv("INTRADAY_PRICE_RECHECK_QUEUE_MAX", "30") or "30"))
        )
        if recheck_observation_codes:
            _remember_priority(recheck_observation_codes)
            before = len(codes)
            codes = sorted(set(codes + recheck_observation_codes))
            logger.info(
                "[SNAP] recheck_observation merged: +%d codes (total %d)",
                len(codes) - before,
                len(codes),
            )
    if args.codes_only:
        ev_source_queue_observation_codes, ev_source_queue_observation_selection = [], {"status": "SKIPPED_CODES_ONLY"}
    else:
        ev_source_queue_observation_codes, ev_source_queue_observation_selection = _load_ev_source_queue_observation_code_selection(
            max_codes=int(str(os.getenv("INTRADAY_PRICE_EV_SOURCE_QUEUE_MAX", "40") or "40"))
        )
    if ev_source_queue_observation_codes:
        _remember_priority(ev_source_queue_observation_codes)
        before = len(codes)
        codes = sorted(set(codes + ev_source_queue_observation_codes))
        logger.info(
            "[SNAP] ev_source_queue_observation merged: +%d codes (total %d)",
            len(codes) - before,
            len(codes),
        )
    if args.ev_source_queues_only or args.codes_only:
        skip_status = "SKIPPED_CODES_ONLY" if args.codes_only else "SKIPPED_EV_SOURCE_QUEUES_ONLY"
        ev_pending_observation_codes, ev_pending_observation_selection = [], {"status": skip_status}
        future_signal_codes = []
    else:
        ev_pending_observation_codes, ev_pending_observation_selection = _load_ev_pending_observation_code_selection(
            max_codes=int(str(os.getenv("INTRADAY_PRICE_EV_PENDING_MAX", "40") or "40"))
        )
        if ev_pending_observation_codes:
            _remember_priority(ev_pending_observation_codes)
            before = len(codes)
            codes = sorted(set(codes + ev_pending_observation_codes))
            logger.info(
                "[SNAP] ev_pending_observation merged: +%d codes (total %d)",
                len(codes) - before,
                len(codes),
            )
        future_signal_codes = _load_future_signal_preview_codes()
        _remember_priority([c for c in future_signal_codes if c in set(codes)])
    max_total_codes = max(0, int(args.max_total_codes or 0))
    max_total_codes_before = int(len(codes))
    max_total_codes_applied = False
    max_total_codes_priority_kept = 0
    if max_total_codes > 0 and len(codes) > max_total_codes:
        before_codes = list(codes)
        priority_set = set(priority_codes)
        capped: List[str] = []
        seen_capped = set()
        for code in priority_codes:
            if code in before_codes and code not in seen_capped:
                capped.append(code)
                seen_capped.add(code)
                if len(capped) >= max_total_codes:
                    break
        if len(capped) < max_total_codes:
            for code in before_codes:
                if code not in seen_capped:
                    capped.append(code)
                    seen_capped.add(code)
                    if len(capped) >= max_total_codes:
                        break
        codes = sorted(capped)
        max_total_codes_applied = True
        max_total_codes_priority_kept = len(set(codes) & priority_set)
        logger.info(
            "[SNAP] max_total_codes applied: before=%d after=%d cap=%d priority_kept=%d",
            len(before_codes),
            len(codes),
            max_total_codes,
            max_total_codes_priority_kept,
        )
    future_signal_missing_codes = sorted(set(future_signal_codes) - set(codes))
    realtime_surge_missing_codes = sorted(set(realtime_surge_codes) - set(codes))
    recheck_observation_missing_codes = sorted(set(recheck_observation_codes) - set(codes))
    ev_source_queue_observation_missing_codes = sorted(set(ev_source_queue_observation_codes) - set(codes))
    ev_pending_observation_missing_codes = sorted(set(ev_pending_observation_codes) - set(codes))

    out_path = Path(args.out_csv) if args.out_csv else OUT_CSV
    status_path = Path(args.out_status_json) if args.out_status_json else None
    if not codes:
        if args.ev_source_queues_only:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=HEADER).writeheader()
            _write_intraday_status_payload(out_path, {
                "ts": _now_ts(),
                "status": "OK",
                "mode": "ev_source_queues_only",
                "codes_requested": 0,
                "codes_ok": 0,
                "codes_failed": 0,
                "history_append_enabled": False,
                "history_appended_rows": 0,
                "ev_source_queue_observation_codes_requested": 0,
                "ev_source_queue_observation_codes_covered": 0,
                "ev_source_queue_observation_codes_missing": ev_source_queue_observation_missing_codes,
                "ev_source_queue_observation_codes": ev_source_queue_observation_codes,
                "ev_source_queue_observation_selection": ev_source_queue_observation_selection,
                "policy_change": False,
                "entry_approval_changed": False,
                "entry_signal": False,
                "trading_allowed": False,
            }, status_path=status_path)
            logger.info("[SNAP] ev_source_queues_only no regular-session source codes")
            return 0
        logger.error("[SNAP] no codes to fetch. use --codes or --from-candidates")
        return 2

    _apply_snapshot_failfast_defaults()
    try:
        client = KISOrderClient.from_env(mock=False)  # FORCED PROD FOR MARKET DATA
    except Exception as e:
        logger.error("[SNAP] KISOrderClient init failed: %s", e)
        return 1
    try:
        client.cfg.timeout_sec = max(1.0, float(args.kis_timeout_sec))
    except Exception:
        client.cfg.timeout_sec = 3.5
    client.cfg.rate_limit_retries = max(0, min(int(client.cfg.rate_limit_retries or 0), 0))
    client.cfg.shared_budget_lock_timeout_sec = max(0.1, min(float(client.cfg.shared_budget_lock_timeout_sec or 1.0), 1.0))

    logger.info("[SNAP] fetching %d codes mock=%s", len(codes), client.cfg.mock)
    hoga_fallback_codes: set[str] = set()
    if args.hoga_fallback_mode == "surge_only":
        hoga_fallback_codes.update(codes)
        hoga_fallback_codes.update(_load_surge_universe_codes(max_codes=args.surge_universe_max))
        hoga_fallback_codes.update(_load_open_position_codes())
    logger.info(
        "[SNAP] hoga_fallback mode=%s scope=%d",
        str(args.hoga_fallback_mode),
        int(len(hoga_fallback_codes)),
    )
    ws_prefill_rows, ws_prefill_meta = _load_ws_trade_prefill(codes)
    rest_codes = [code for code in codes if code not in ws_prefill_rows]
    if ws_prefill_rows:
        logger.info(
            "[SNAP] ws_trade_prefill rows=%d rest_codes=%d elapsed=%.3fs",
            len(ws_prefill_rows),
            len(rest_codes),
            float(ws_prefill_meta.get("elapsed_sec", 0.0) or 0.0),
        )
    started = time.time()
    if rest_codes:
        rest_rows, perf_rows = fetch_prices(
            client,
            rest_codes,
            retry=args.retry,
            retry_sleep=args.retry_sleep,
            hoga_fallback_mode=str(args.hoga_fallback_mode),
            hoga_fallback_codes=hoga_fallback_codes,
            workers=int(args.workers),
            request_interval_sec=float(args.request_interval_sec),
            rate_limit_retry_max=int(args.rate_limit_retry_max),
            rate_limit_retry_sleep=float(args.rate_limit_retry_sleep),
            max_elapsed_sec=float(args.max_elapsed_sec),
        )
    else:
        rest_rows, perf_rows = [], []
    rest_by_code = {str(row.get("code", "")).zfill(6): row for row in rest_rows}
    rows = [ws_prefill_rows.get(code) or rest_by_code.get(code) for code in codes]
    rows = [row for row in rows if isinstance(row, dict)]
    perf_rows = [
        {
            "code": code,
            "elapsed_sec": 0.0,
            "attempts": 0,
            "hoga_fallback": False,
            "hoga_fallback_allowed": False,
            "hoga_fallback_error": "",
            "rate_limit_retries": 0,
            "ok": True,
            "error": "",
            "source": "WS_TRADE",
        }
        for code in codes
        if code in ws_prefill_rows
    ] + perf_rows
    elapsed_total = round(time.time() - started, 3)

    valid = [r for r in rows if r["current_price"] > 0]
    valid_count = int(len(valid))
    logger.info("[SNAP] ok=%d / total=%d -> %s", valid_count, len(rows), out_path)
    if perf_rows:
        slow_top = sorted(perf_rows, key=lambda x: float(x.get("elapsed_sec", 0.0)), reverse=True)[:5]
        logger.info("[SNAP] elapsed_total=%.3fs slow_top=%s", elapsed_total, slow_top)
    failed_perf_rows = [
        {
            "code": str(p.get("code", "")),
            "elapsed_sec": float(p.get("elapsed_sec", 0.0) or 0.0),
            "attempts": int(p.get("attempts", 0) or 0),
            "hoga_fallback_allowed": bool(p.get("hoga_fallback_allowed", False)),
            "hoga_fallback": bool(p.get("hoga_fallback", False)),
            "rate_limit_retries": int(p.get("rate_limit_retries", 0) or 0),
            "error": str(p.get("error", "")),
        }
        for p in perf_rows
        if not bool(p.get("ok", False))
    ][:20]
    rate_limit_retry_rows = [
        {
            "code": str(p.get("code", "")),
            "elapsed_sec": float(p.get("elapsed_sec", 0.0) or 0.0),
            "attempts": int(p.get("attempts", 0) or 0),
            "rate_limit_retries": int(p.get("rate_limit_retries", 0) or 0),
        }
        for p in perf_rows
        if int(p.get("rate_limit_retries", 0) or 0) > 0
    ]
    rate_limit_retry_count = sum(int(p.get("rate_limit_retries", 0) or 0) for p in perf_rows)
    hoga_fallback_error_rows = [
        {
            "code": str(p.get("code", "")),
            "hoga_fallback_error": str(p.get("hoga_fallback_error", "")),
        }
        for p in perf_rows
        if str(p.get("hoga_fallback_error", "")).strip()
    ][:20]

    fallback_mode = "NONE"
    prev_valid = _count_valid_rows(out_path)
    history_append_enabled = (
        not bool(args.no_history_append)
        and str(os.getenv(HISTORY_ENV, "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
    )
    history_path = Path(args.history_csv) if args.history_csv else (LOG_DIR / f"intraday_prices_history_{_today_ymd()}.csv")
    history_appended_rows = 0
    history_append_guard = {
        "enabled": bool(history_append_enabled),
        "appended_rows": 0,
    }
    if valid_count <= 0 and prev_valid > 0:
        # Keep last good snapshot when current fetch fully fails.
        fallback_mode = "KEEP_PREVIOUS_SNAPSHOT"
        logger.warning("[SNAP] all fetch failed, keep previous snapshot: prev_ok=%d path=%s", prev_valid, out_path)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER)
            w.writeheader()
            w.writerows(rows)
        if history_append_enabled:
            history_append_guard = _append_history_csv(rows, history_path)
            history_appended_rows = int(history_append_guard.get("appended_rows", 0) or 0)

    # Write status JSON for operational checks and preserve the default run by date.
    try:
        payload = {
            "ts": _now_ts(),
            "codes_requested": len(codes),
            "codes_ok": valid_count,
            "codes_failed": len(rows) - valid_count,
            "elapsed_total_sec": float(elapsed_total),
            "out_csv": str(out_path),
            "mock": client.cfg.mock,
            "kis_timeout_sec": float(client.cfg.timeout_sec),
            "workers": int(args.workers),
            "request_interval_sec": float(args.request_interval_sec),
            "ws_prefill": ws_prefill_meta,
            "ws_prefill_rows": int(len(ws_prefill_rows)),
            "rest_fetch_codes": int(len(rest_codes)),
            "rate_limit_retry_max": int(args.rate_limit_retry_max),
            "rate_limit_retry_sleep": float(args.rate_limit_retry_sleep),
            "max_elapsed_sec": float(args.max_elapsed_sec),
            "rate_limit_retry_count": int(rate_limit_retry_count),
            "rate_limit_retry_code_count": int(len(rate_limit_retry_rows)),
            "rate_limit_retry_rows": rate_limit_retry_rows[:20],
            "hoga_fallback_mode": str(args.hoga_fallback_mode),
            "hoga_fallback_scope": int(len(hoga_fallback_codes)),
            "fallback_mode": fallback_mode,
            "previous_valid_rows": int(prev_valid),
            "history_append_enabled": bool(history_append_enabled),
            "history_csv": str(history_path),
            "history_appended_rows": int(history_appended_rows),
            "history_append_guard": history_append_guard,
            "max_total_codes": int(max_total_codes),
            "max_total_codes_before": int(max_total_codes_before),
            "max_total_codes_applied": bool(max_total_codes_applied),
            "max_total_codes_priority_kept": int(max_total_codes_priority_kept),
            "future_signal_preview_codes_requested": int(len(future_signal_codes)),
            "future_signal_preview_codes_covered": int(len(set(future_signal_codes) & set(codes))),
            "future_signal_preview_codes_missing": future_signal_missing_codes,
            "future_signal_preview_codes": future_signal_codes,
            "realtime_surge_codes_requested": int(len(realtime_surge_codes)),
            "realtime_surge_codes_covered": int(len(set(realtime_surge_codes) & set(codes))),
            "realtime_surge_codes_missing": realtime_surge_missing_codes,
            "realtime_surge_codes": realtime_surge_codes,
            "realtime_surge_selection": realtime_surge_selection,
            "recheck_observation_codes_requested": int(len(recheck_observation_codes)),
            "recheck_observation_codes_covered": int(len(set(recheck_observation_codes) & set(codes))),
            "recheck_observation_codes_missing": recheck_observation_missing_codes,
            "recheck_observation_codes": recheck_observation_codes,
            "recheck_observation_selection": recheck_observation_selection,
            "ev_source_queue_observation_codes_requested": int(len(ev_source_queue_observation_codes)),
            "ev_source_queue_observation_codes_covered": int(len(set(ev_source_queue_observation_codes) & set(codes))),
            "ev_source_queue_observation_codes_missing": ev_source_queue_observation_missing_codes,
            "ev_source_queue_observation_codes": ev_source_queue_observation_codes,
            "ev_source_queue_observation_selection": ev_source_queue_observation_selection,
            "ev_pending_observation_codes_requested": int(len(ev_pending_observation_codes)),
            "ev_pending_observation_codes_covered": int(len(set(ev_pending_observation_codes) & set(codes))),
            "ev_pending_observation_codes_missing": ev_pending_observation_missing_codes,
            "ev_pending_observation_codes": ev_pending_observation_codes,
            "ev_pending_observation_selection": ev_pending_observation_selection,
            "codes_requested_list": codes,
            "slow_top": sorted(perf_rows, key=lambda x: float(x.get("elapsed_sec", 0.0)), reverse=True)[:10],
            "failed_perf_rows": failed_perf_rows,
            "failed_error_counts": {
                str(p.get("error", "")): sum(1 for x in failed_perf_rows if str(x.get("error", "")) == str(p.get("error", "")))
                for p in failed_perf_rows
                if str(p.get("error", "")).strip()
            },
            "hoga_fallback_error_rows": hoga_fallback_error_rows,
        }
        _write_intraday_status_payload(out_path, payload, status_path=status_path)
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
