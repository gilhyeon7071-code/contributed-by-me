# -*- coding: utf-8 -*-
r"""
p0_onepass_from_fills.py

Purpose
- SSOT: derive D(as_of) from fills.csv and generate one-day comparison artifacts
- outputs:
  - RootA: paper\\orders_{D}_exec.xlsx   (fills 湲곕컲, STOP ?ы븿; entry_blocked ?쒖떇 ?ы븿)
  - RootA: 2_Logs\\p0_live_vs_bt_core_{D}.json
  - RootA: 2_Logs\\p0_stop_report_{D}.json

Patch (2026-02-13)
- Strategy risk mitigation (Plan-only):
  - For BUY & non-stop rows, apply CAP per signal_date using candidates score (top N).
  - Mark blocked rows: entry_blocked=True, entry_block_reason="CAP_SIGNALDATE_TOP{N}_BY_SCORE"
  - Exclude blocked rows from core calculations.
  - Fail the orders_exec contract if posthoc/execution-blocked BUY fills are present.

Notes
- This does NOT block live broker execution. It only marks/excludes rows in this onepass pipeline (Plan-only).
- Surge-immediate BUY rows are excluded from the daily score CAP because they use the realtime surge policy.
"""

from __future__ import annotations

import json
import logging
import hashlib
import re
import sys
from datetime import datetime, time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
FILLS = ROOT / "paper" / "fills.csv"
OUT_ORDERS_DIR = ROOT / "paper"
OUT_LOGS_DIR = ROOT / "2_Logs"
PAPER_ENGINE_CONFIG = ROOT / "paper" / "paper_engine_config.json"
DISCLOSURE_RISK_JSON = OUT_LOGS_DIR / "disclosure_risk_latest.json"
logger = logging.getLogger("p0_onepass_from_fills")

# Ensure utils import works regardless of cwd
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from utils.pipeline_audit import log_pipeline_event, count_rows


def _ymd_from_datetime(s: str) -> str:
    s = str(s)
    return s[:8]


def _z6(x: str) -> str:
    return str(x).zfill(6)


def _parse_signal_date(note: str) -> Optional[str]:
    m = re.search(r"signal_date=(\d{8})", str(note))
    return m.group(1) if m else None


def _parse_signal_ts(note: str) -> str:
    m = re.search(r"signal_ts=([^;|]+)", str(note))
    return m.group(1).strip() if m else ""


def _parse_note_field(note: object, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(str(key))}=([^;|]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _to_bool_value(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "y", "yes"}


def _source_entry_block_from_note(note: object) -> Tuple[bool, str]:
    decision = _parse_note_field(note, "source_entry_decision").strip().upper()
    blocked_text = _parse_note_field(note, "source_entry_blocked")
    reason = _parse_note_field(note, "source_entry_reason")
    exclude_reasons = _parse_note_field(note, "source_exclude_reasons")
    blocked = _to_bool_value(blocked_text) or decision == "ENTRY_BLOCKED"
    if not blocked:
        return False, ""
    parts = []
    if decision:
        parts.append(f"source_entry_decision={decision}")
    if blocked_text:
        parts.append(f"source_entry_blocked={blocked_text}")
    if reason:
        parts.append(f"source_entry_reason={reason}")
    if exclude_reasons:
        parts.append(f"source_exclude_reasons={exclude_reasons}")
    return True, "SOURCE_ENTRY_BLOCKED:" + ";".join(parts)


def _parse_dt_text(value: object) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    return None


def _row_policy_event_dt(row: pd.Series, D: str) -> Optional[datetime]:
    for key in ("fill_datetime", "datetime", "fill_time"):
        parsed_fill = _parse_dt_text(row.get(key, ""))
        if parsed_fill is not None:
            return parsed_fill
    signal_ts = str(row.get("signal_ts", "") or "").strip()
    parsed = _parse_dt_text(signal_ts)
    if parsed is not None:
        return parsed
    try:
        return datetime.combine(datetime.strptime(str(D), "%Y%m%d").date(), time.max)
    except Exception:
        return None


def _stable_digest(*parts: object) -> str:
    text = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _is_stop_row(side_u: str, note: str) -> bool:
    # Current observed SELL rows with exit_reason=STOP / STOP_GAP.
    side_u = str(side_u).upper().strip()
    note = str(note)
    if side_u != "SELL":
        return False
    return ("STOP" in note) or ("STOP_GAP" in note)


def _build_trader_instruction(side: str, is_stop: bool, entry_blocked: bool, entry_block_reason: str, note: str) -> str:
    side_u = str(side or "").upper().strip()
    reason = str(entry_block_reason or "").strip()
    note_s = str(note or "").strip()
    if side_u == "DIAG":
        return "진단행(주문없음)"
    if side_u == "BUY":
        if bool(entry_blocked):
            return f"진입보류: {reason}" if reason else "진입보류: 사유확인"
        return "매수진행: 주문수량/호가 확인 후 집행"
    if side_u == "SELL":
        if bool(is_stop):
            return "손절청산: 시장상황 확인 후 즉시 집행"
        return "매도진행: 보유수량 확인 후 집행"
    return f"확인필요: side={side_u or '-'} note={note_s[:40]}"


def _pick_candidates_path_for_signal_date(signal_date: str) -> Optional[Path]:
    r"""
    Prefer v41_1 daily snapshot if exists:
      2_Logs\\candidates_v41_1_YYYYMMDD.csv
    """
    if not signal_date or not re.fullmatch(r"\d{8}", str(signal_date)):
        return None
    p = OUT_LOGS_DIR / f"candidates_v41_1_{signal_date}.csv"
    if p.exists():
        return p

    candidates: list[tuple[str, Path]] = []
    for fp in OUT_LOGS_DIR.glob("candidates_v41_1_*.csv"):
        m = re.fullmatch(r"candidates_v41_1_(\d{8})\.csv", fp.name)
        if not m:
            continue
        ymd = m.group(1)
        if ymd <= signal_date:
            candidates.append((ymd, fp))
    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])[1]


def _load_top_codes_by_score(signal_date: str, top_n: int = 3) -> Tuple[Optional[set], str]:
    """
    Returns (top_codes_set, debug_msg).
    - top_codes are code6 strings
    - tie-breaker: value(desc) if present
    """
    p = _pick_candidates_path_for_signal_date(signal_date)
    if p is None:
        return None, f"candidates_missing_for_signal_date={signal_date}"
    picked_match = re.fullmatch(r"candidates_v41_1_(\d{8})\.csv", p.name)
    picked_date = picked_match.group(1) if picked_match else ""
    date_note = "" if picked_date == signal_date else f" fallback_from_signal_date={signal_date} picked_date={picked_date}"

    try:
        c = pd.read_csv(p, dtype=str)
    except Exception as e:
        return None, f"candidates_read_error={p}: {e}"

    if "code" not in c.columns or "score" not in c.columns:
        return None, f"candidates_missing_cols(code/score) path={p}"

    c["code6"] = c["code"].astype(str).str.zfill(6)
    c["score_num"] = pd.to_numeric(c["score"], errors="coerce")

    sort_cols = ["score_num"]
    ascending = [False]
    if "value" in c.columns:
        c["value_num"] = pd.to_numeric(c["value"], errors="coerce")
        sort_cols.append("value_num")
        ascending.append(False)

    c = c.sort_values(sort_cols, ascending=ascending)
    top = c["code6"].dropna().astype(str).head(int(top_n)).tolist()
    return set(top), f"candidates_ok path={p}{date_note} top_n={top_n} top_codes={top}"


def _cap_source_generated_after_event(signal_date: str, row: pd.Series, D: str) -> Tuple[bool, str]:
    p = _pick_candidates_path_for_signal_date(signal_date)
    event_dt = _row_policy_event_dt(row, D)
    if p is None or event_dt is None:
        return False, ""
    try:
        source_dt = datetime.fromtimestamp(p.stat().st_mtime)
    except Exception:
        return False, ""
    if source_dt > event_dt:
        return True, (
            f"cap_temporal_skip signal_date={signal_date} code={_z6(row.get('code', ''))} "
            f"event_dt={event_dt.strftime('%Y-%m-%dT%H:%M:%S')} "
            f"source_mtime={source_dt.strftime('%Y-%m-%dT%H:%M:%S')} path={p}"
        )
    return False, ""


def _load_runtime_top_score_cap_n() -> Tuple[int, str]:
    try:
        cfg = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
    except Exception as e:
        return 3, f"cap_top_n_source=default_3 config_read_error={e}"
    cap_cfg = cfg.get("entry_signal_date_top_score_cap")
    if isinstance(cap_cfg, dict) and bool(cap_cfg.get("enabled", True)):
        try:
            top_n = max(1, int(cap_cfg.get("top_n", 5) or 5))
        except Exception:
            top_n = 5
        return top_n, "cap_top_n_source=entry_signal_date_top_score_cap.top_n"
    try:
        top_n = max(0, int(cfg.get("cap_signal_top_n", 3) or 0))
    except Exception:
        top_n = 3
    return top_n, "cap_top_n_source=cap_signal_top_n"


def _load_runtime_top_score_cap_policy() -> dict:
    try:
        cfg = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {
            "fallback_if_pool_lt_n": True,
            "fallback_when_selected_pool_has_no_top_overlap": True,
            "fallback_require_positive_entry": True,
        }
    cap_cfg = cfg.get("entry_signal_date_top_score_cap")
    if not isinstance(cap_cfg, dict):
        cap_cfg = {}
    return {
        "fallback_if_pool_lt_n": bool(cap_cfg.get("fallback_if_pool_lt_n", True)),
        "fallback_when_selected_pool_has_no_top_overlap": bool(cap_cfg.get("fallback_when_selected_pool_has_no_top_overlap", True)),
        "fallback_require_positive_entry": bool(cap_cfg.get("fallback_require_positive_entry", True)),
    }


def _load_candidate_positive_proxy(signal_date: str, code6: str) -> bool:
    p = _pick_candidates_path_for_signal_date(signal_date)
    if p is None:
        return False
    try:
        c = pd.read_csv(p, dtype=str)
    except Exception:
        return False
    if "code" not in c.columns:
        return False
    rows = c[c["code"].astype(str).str.zfill(6).eq(str(code6).zfill(6))]
    if rows.empty:
        return False
    row = rows.iloc[0]
    for col in ("positive_entry_ok", "execution_pool", "sector_entry_allowed"):
        if col in rows.columns and str(row.get(col, "")).strip().lower() in {"1", "true", "y", "yes"}:
            return True
    try:
        return float(pd.to_numeric(pd.Series([row.get("score")]), errors="coerce").iloc[0]) > 0
    except Exception:
        return False


def _load_disclosure_negative_codes() -> Tuple[set, str, Optional[datetime]]:
    if not DISCLOSURE_RISK_JSON.exists():
        return set(), "disclosure_missing", None
    try:
        obj = json.loads(DISCLOSURE_RISK_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        return set(), f"disclosure_read_error={e}", None
    generated_at = _parse_dt_text(obj.get("generated_at"))
    items = obj.get("items")
    if not isinstance(items, list):
        return set(), "disclosure_items_missing", generated_at
    blocked: set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        sig = str(it.get("disclosure_signal", "")).strip().upper()
        if sig != "NEGATIVE":
            continue
        code6 = _z6(str(it.get("code", "")).strip())
        if re.fullmatch(r"\d{6}", code6):
            blocked.add(code6)
    gen_note = generated_at.strftime("%Y-%m-%d %H:%M:%S") if generated_at else "unknown"
    return blocked, f"disclosure_negative_codes={len(blocked)} generated_at={gen_note}", generated_at


def _to_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "y", "yes"})


def _note_is_surge_immediate(note: str) -> bool:
    return "surge_immediate=1" in str(note or "").lower()


def _build_orders_contract(
    out_df: pd.DataFrame,
    D: str,
    disclosure_blocked_codes: set[str],
    dup_rows_precheck: int,
) -> dict:
    exec_unique = sorted({str(v).strip() for v in out_df["exec_date"].astype(str).tolist() if str(v).strip()})
    qty_num = pd.to_numeric(out_df["fill_qty"], errors="coerce")
    qty_nan = int(qty_num.isna().sum())
    qty_le_zero = int((qty_num <= 0).sum())

    dup_key_cols = ["exec_date", "side", "code"]
    dup_key_rows = int(out_df.duplicated(subset=dup_key_cols, keep=False).sum())

    side_u = out_df["side"].astype(str).str.upper().str.strip()
    stop_b = _to_bool_series(out_df["is_stop"])
    blocked_b = _to_bool_series(out_df["entry_blocked"])
    posthoc_b = _to_bool_series(out_df.get("posthoc_policy_violation", pd.Series(False, index=out_df.index)))
    execution_blocked_b = _to_bool_series(out_df.get("execution_blocked", pd.Series(False, index=out_df.index)))
    reason_nonempty = out_df["entry_block_reason"].astype(str).str.strip().ne("")
    paper_probe_source_block_b = (
        out_df["note"].apply(lambda v: _to_bool_value(_parse_note_field(v, "surge_paper_probe_allowed")))
        & blocked_b
        & (~posthoc_b)
        & (~execution_blocked_b)
        & out_df["entry_block_reason"].astype(str).str.startswith("SOURCE_ENTRY_BLOCKED:")
    )

    executable_buy = side_u.eq("BUY") & (~stop_b) & (~blocked_b)
    buy_nonstop = side_u.eq("BUY") & (~stop_b)
    posthoc_violation_rows = int((buy_nonstop & posthoc_b).sum())
    execution_blocked_rows = int((buy_nonstop & execution_blocked_b).sum())
    paper_probe_blocked_filled_rows = int((buy_nonstop & paper_probe_source_block_b & (qty_num > 0)).sum())
    blocked_filled_rows = int(
        (buy_nonstop & (blocked_b | posthoc_b | execution_blocked_b) & (~paper_probe_source_block_b) & (qty_num > 0)).sum()
    )
    disclosure_code_b = out_df["code"].astype(str).str.zfill(6).isin(disclosure_blocked_codes)
    if "disclosure_policy_available" in out_df.columns:
        disclosure_available_b = _to_bool_series(out_df["disclosure_policy_available"])
    else:
        disclosure_available_b = pd.Series(True, index=out_df.index)
    disclosure_in_executable = int((executable_buy & disclosure_code_b & disclosure_available_b).sum())
    block_reason_mismatch = int((reason_nonempty & (~blocked_b)).sum())

    fail_reasons = []
    review_reasons = []
    if exec_unique != [D]:
        fail_reasons.append(f"exec_date_mismatch expected=[{D}] got={exec_unique}")
    if qty_nan > 0:
        fail_reasons.append(f"qty_nan={qty_nan}")
    if qty_le_zero > 0:
        fail_reasons.append(f"qty_le_zero={qty_le_zero}")
    if dup_key_rows > 0:
        fail_reasons.append(f"duplicate_orders dup_rows_precheck={dup_rows_precheck} dup_key_rows={dup_key_rows}")
    if disclosure_in_executable > 0:
        fail_reasons.append(f"excluded_in_executable={disclosure_in_executable}")
    if block_reason_mismatch > 0:
        fail_reasons.append(f"block_reason_mismatch={block_reason_mismatch}")
    if posthoc_violation_rows > 0:
        fail_reasons.append(f"posthoc_policy_violation_rows={posthoc_violation_rows}")
    if execution_blocked_rows > 0:
        fail_reasons.append(f"execution_blocked_rows={execution_blocked_rows}")
    if blocked_filled_rows > 0:
        fail_reasons.append(f"blocked_filled_rows={blocked_filled_rows}")
    if paper_probe_blocked_filled_rows > 0:
        review_reasons.append(f"paper_probe_blocked_filled_rows={paper_probe_blocked_filled_rows}")

    return {
        "as_of": D,
        "status": "PASS" if not fail_reasons else "FAIL",
        "summary": {
            "orders_rows": int(len(out_df)),
            "exec_date_unique": exec_unique,
            "qty_total": float(qty_num.sum(skipna=True)),
            "qty_min": float(qty_num.min(skipna=True)) if len(out_df) else 0.0,
            "qty_max": float(qty_num.max(skipna=True)) if len(out_df) else 0.0,
            "qty_nan": qty_nan,
            "qty_le_zero": qty_le_zero,
            "duplicate_rows_precheck": int(dup_rows_precheck),
            "duplicate_rows_by_key": dup_key_rows,
            "excluded_in_executable": disclosure_in_executable,
            "entry_block_reason_mismatch": block_reason_mismatch,
            "posthoc_policy_violation_rows": posthoc_violation_rows,
            "execution_blocked_rows": execution_blocked_rows,
            "blocked_filled_rows": blocked_filled_rows,
            "paper_probe_blocked_filled_rows": paper_probe_blocked_filled_rows,
        },
        "checks": {
            "exec_date_unique_eq_D": exec_unique == [D],
            "qty_positive_only": qty_nan == 0 and qty_le_zero == 0,
            "duplicate_orders_zero": dup_key_rows == 0,
            "excluded_in_executable_zero": disclosure_in_executable == 0,
            "entry_block_reason_consistent": block_reason_mismatch == 0,
            "posthoc_policy_violation_zero": posthoc_violation_rows == 0,
            "execution_blocked_zero": execution_blocked_rows == 0,
            "blocked_filled_zero": blocked_filled_rows == 0,
        },
        "fail_reasons": fail_reasons,
        "review_reasons": review_reasons,
        "review_status": "REVIEW" if review_reasons else "PASS",
    }


_NOTE_PRIORITY_KEYS = (
    "source_entry_decision",
    "source_entry_reason",
    "source_entry_allowed",
    "source_entry_blocked",
    "source_exclude_reasons",
    "surge_paper_probe_allowed",
    "surge_paper_probe_block_reasons",
)


def _merge_note_values(note_vals: list[str], limit: int = 500) -> str:
    seen_notes = list(dict.fromkeys(v for v in note_vals if v))
    priority_tokens: list[str] = []
    other_tokens: list[str] = []
    seen_tokens: set[str] = set()

    for note in seen_notes:
        for raw in str(note).split(";"):
            token = raw.strip()
            if not token or token in seen_tokens:
                continue
            seen_tokens.add(token)
            key = token.split("=", 1)[0].strip()
            if key in _NOTE_PRIORITY_KEYS:
                priority_tokens.append(token)
            else:
                other_tokens.append(token)

    return ";".join(priority_tokens + other_tokens)[:limit]


def _merge_duplicate_orders(out_df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    key_cols = ["exec_date", "side", "code"]
    dup_rows = int(out_df.duplicated(subset=key_cols, keep=False).sum())
    if dup_rows == 0:
        return out_df, 0

    merged_rows = []
    for _, g in out_df.groupby(key_cols, sort=False, dropna=False):
        qty_num = pd.to_numeric(g["fill_qty"], errors="coerce").fillna(0.0)
        px_num = pd.to_numeric(g["fill_price"], errors="coerce").fillna(0.0)
        qty_sum = float(qty_num.sum())
        if qty_sum > 0:
            px = float((qty_num * px_num).sum() / qty_sum)
        else:
            px = float(px_num.iloc[-1]) if len(px_num) else 0.0
        stop_level = px * 0.95 if str(g["side"].iloc[0]).upper().strip() == "BUY" else None

        signal_date = next((str(v) for v in g["signal_date"].tolist() if str(v).strip()), "")
        signal_ts = next((str(v) for v in g["signal_ts"].tolist() if str(v).strip()), "") if "signal_ts" in g.columns else ""
        note_vals = [str(v).strip() for v in g["note"].tolist() if str(v).strip()]
        note = _merge_note_values(note_vals)
        reason_vals = [str(v).strip() for v in g["reason"].tolist() if str(v).strip()]
        reason = " | ".join(dict.fromkeys(reason_vals).keys())[:300]
        entry_block_reason_vals = [str(v).strip() for v in g["entry_block_reason"].tolist() if str(v).strip()]
        entry_block_reason = " | ".join(dict.fromkeys(entry_block_reason_vals).keys())[:300]

        merged_rows.append(
            {
                "exec_date": str(g["exec_date"].iloc[0]),
                "side": str(g["side"].iloc[0]),
                "code": str(g["code"].iloc[0]),
                "fill_qty": qty_sum,
                "fill_price": px,
                "stop_level": stop_level,
                "signal_date": signal_date,
                "signal_ts": signal_ts,
                "is_stop": bool(_to_bool_series(g["is_stop"]).any()),
                "note": note,
                "reason": reason,
                "entry_blocked": bool(_to_bool_series(g["entry_blocked"]).any()),
                "entry_block_reason": entry_block_reason,
                "posthoc_policy_violation": bool(_to_bool_series(g.get("posthoc_policy_violation", pd.Series(False, index=g.index))).any()),
                "posthoc_policy_reason": " | ".join(
                    dict.fromkeys(
                        str(v).strip()
                        for v in g.get("posthoc_policy_reason", pd.Series("", index=g.index)).tolist()
                        if str(v).strip()
                    ).keys()
                )[:300],
                "execution_blocked": bool(_to_bool_series(g.get("execution_blocked", pd.Series(False, index=g.index))).any()),
                "execution_block_reason": " | ".join(
                    dict.fromkeys(
                        str(v).strip()
                        for v in g.get("execution_block_reason", pd.Series("", index=g.index)).tolist()
                        if str(v).strip()
                    ).keys()
                )[:300],
                "disclosure_policy_available": bool(
                    _to_bool_series(g.get("disclosure_policy_available", pd.Series(False, index=g.index))).any()
                ),
            }
        )

    return pd.DataFrame(merged_rows), dup_rows


def _detect_D_by_rule(df: pd.DataFrame) -> str:
    """
    D rule:
    - latest BUY ymd
    - if no BUY exists, latest ymd(datetime first 8 chars)
    """
    df = df.copy()
    df["ymd"] = df["datetime"].apply(_ymd_from_datetime)
    side_u = df.get("side", "").astype(str).str.upper().str.strip()
    buy_ymd = df.loc[side_u == "BUY", "ymd"].dropna().astype(str)
    buy_ymd = buy_ymd[buy_ymd.str.len() == 8]
    if len(buy_ymd):
        return str(buy_ymd.max())

    all_ymd = df["ymd"].dropna().astype(str)
    all_ymd = all_ymd[all_ymd.str.len() == 8]
    return str(all_ymd.max()) if len(all_ymd) else ""



def _resolve_D(df: pd.DataFrame, argv) -> Optional[str]:
    """
    D resolution contract (fail-closed):
    1) If argv[1] exists, use it as authoritative D.
    2) Validate argv D format (YYYYMMDD) and presence in fills ymd.
    3) If argv D != detected D by rule, STOP to prevent SSOT divergence.
    4) If argv[1] missing, fallback to detected D by rule.
    """
    detected = _detect_D_by_rule(df)
    arg_d = ""
    if argv and len(argv) > 1:
        arg_d = str(argv[1]).strip()

    if not arg_d:
        return detected

    if not re.fullmatch(r"\d{8}", arg_d):
        logger.error("STOP invalid_arg_date=%s expected_YYYYMMDD", arg_d)
        return None

    ymd = df["datetime"].apply(_ymd_from_datetime)
    ymd_set = set(ymd.dropna().astype(str).tolist())
    if arg_d not in ymd_set:
        logger.error("STOP arg_date_not_found_in_fills arg=%s", arg_d)
        return None

    if arg_d != detected:
        logger.error("STOP d_mismatch arg=%s detected=%s", arg_d, detected)
        return None

    return arg_d

def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    _audit_ymd = datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="orders_exec",
        batch_label="[11/14]",
        event="START",
        date=_audit_ymd,
        input_files={
            "fills": str(FILLS),
        },
    )

    # ---- load fills ----
    if not FILLS.exists():
        logger.error("STOP fills_missing=%s", FILLS)
        return 2

    OUT_ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FILLS, dtype=str)
    need = ["datetime", "code", "side", "qty", "price"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        logger.error("STOP missing_cols=%s", missing)
        logger.info("[INFO] cols=%s", df.columns.tolist())
        return 2

    D = _resolve_D(df, argv)
    if not D:
        return 2


    df["ymd"] = df["datetime"].apply(_ymd_from_datetime)
    asof = df[df["ymd"] == D].copy()

    if len(asof) == 0:
        outj = {
            "status": "NA",
            "as_of": D,
            "summary": {
                "source_fills": str(FILLS.name),
                "fills_rows_as_of": 0,
                "as_of": D,
                "exec_date": D,
            },
            "notes": ["as_of ?꾪꽣 ?곸슜 ???좏슚??0 ??NA"],
        }
        outp = OUT_LOGS_DIR / f"p0_live_vs_bt_core_{D}.json"
        outp.write_text(json.dumps(outj, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("WROTE %s status=NA", outp)
        return 0

    # ---- normalize ----
    asof["code6"] = asof["code"].apply(_z6)
    asof["side_u"] = asof["side"].astype(str).str.upper().str.strip()
    asof["fill_qty"] = pd.to_numeric(asof["qty"], errors="coerce")
    asof["fill_price"] = pd.to_numeric(asof["price"], errors="coerce")
    asof["stop_level"] = asof["fill_price"].where(asof["side_u"] == "BUY") * 0.95
    asof["note"] = asof.get("note", "").astype(str)
    asof["signal_date"] = asof["note"].apply(_parse_signal_date)
    asof["signal_ts"] = asof["note"].apply(_parse_signal_ts)
    asof["is_stop"] = asof.apply(lambda r: _is_stop_row(r["side_u"], r["note"]), axis=1)
    source_entry_guard = asof["note"].apply(_source_entry_block_from_note)
    asof["source_entry_blocked_flag"] = source_entry_guard.apply(lambda x: bool(x[0]))
    asof["source_entry_block_reason"] = source_entry_guard.apply(lambda x: str(x[1]))

    # ---- write orders_exec (fills-based) ----
    out_orders = OUT_ORDERS_DIR / f"orders_{D}_exec.xlsx"
    out_df = pd.DataFrame(
        {
            "exec_date": D,
            "side": asof["side_u"],
            "code": asof["code6"],
            "fill_qty": asof["fill_qty"],
            "fill_price": asof["fill_price"],
            "stop_level": asof["stop_level"],
            "signal_date": asof["signal_date"],
            "fill_datetime": asof.get("datetime", ""),
            "signal_ts": asof["signal_ts"],
            "is_stop": asof["is_stop"],
            "note": asof["note"],
            "reason": "",
            "entry_blocked": asof["source_entry_blocked_flag"],
            "entry_block_reason": asof["source_entry_block_reason"],
            "posthoc_policy_violation": False,
            "posthoc_policy_reason": "",
            "execution_blocked": False,
            "execution_block_reason": "",
            "disclosure_policy_available": False,
        }
    )

    # ---- CAP policy (Plan-only marking) ----
    # Apply to BUY & non-stop rows only.
    CAP_TOP_N, cap_top_n_source = _load_runtime_top_score_cap_n()
    cap_policy = _load_runtime_top_score_cap_policy()
    cap_notes = []
    cap_notes.append(cap_top_n_source)
    cap_surge_excluded_rows = 0
    disclosure_notes = []
    disclosure_blocked_rows = 0
    try:
        buy_mask = (out_df["side"].astype(str).str.upper().str.strip() == "BUY")
        nonstop_mask = (out_df["is_stop"] == False)
        surge_mask = out_df["note"].apply(_note_is_surge_immediate)
        cap_surge_excluded_rows = int((buy_mask & nonstop_mask & surge_mask).sum())
        entry_mask = buy_mask & nonstop_mask & (~surge_mask)
        if cap_surge_excluded_rows > 0:
            cap_notes.append(f"cap_surge_immediate_excluded_rows={cap_surge_excluded_rows}")

        # group by signal_date
        for sd, idx in out_df[entry_mask].groupby(out_df.loc[entry_mask, "signal_date"]).groups.items():
            sd_str = str(sd) if sd is not None else ""
            if not re.fullmatch(r"\d{8}", sd_str):
                # Skip CAP when signal_date is invalid or missing.
                cap_notes.append(f"cap_skip_invalid_signal_date={sd}")
                continue

            top_set, msg = _load_top_codes_by_score(sd_str, top_n=CAP_TOP_N)
            cap_notes.append(msg)
            if top_set is None:
                # Skip CAP when candidate score data is missing.
                continue

            # allow only top_set, block others
            codes = out_df.loc[idx, "code"].astype(str)
            pool_codes = {str(c).zfill(6) for c in codes.tolist() if str(c).strip()}
            pool_has_overlap = bool(pool_codes & set(top_set))
            fallback_pool_lt_n = bool(cap_policy.get("fallback_if_pool_lt_n", True)) and len(top_set) < CAP_TOP_N
            fallback_no_overlap = bool(cap_policy.get("fallback_when_selected_pool_has_no_top_overlap", True)) and not pool_has_overlap
            blocked_idx = []
            for i, c in zip(idx, codes):
                temporal_skip, temporal_msg = _cap_source_generated_after_event(sd_str, out_df.loc[i], D)
                if temporal_skip:
                    cap_notes.append(temporal_msg)
                    continue
                code6 = str(c).zfill(6)
                if code6 in top_set:
                    continue
                fallback_allowed = False
                if fallback_pool_lt_n:
                    fallback_allowed = True
                elif fallback_no_overlap:
                    if bool(cap_policy.get("fallback_require_positive_entry", True)):
                        fallback_allowed = _load_candidate_positive_proxy(sd_str, code6)
                    else:
                        fallback_allowed = True
                if fallback_allowed:
                    cap_notes.append(f"cap_fallback_allowed signal_date={sd_str} code={code6} pool_has_overlap={pool_has_overlap}")
                    continue
                blocked_idx.append(i)
            if blocked_idx:
                out_df.loc[blocked_idx, "entry_blocked"] = True
                out_df.loc[blocked_idx, "entry_block_reason"] = f"CAP_SIGNALDATE_TOP{CAP_TOP_N}_BY_SCORE"
                out_df.loc[blocked_idx, "posthoc_policy_violation"] = True
                out_df.loc[blocked_idx, "posthoc_policy_reason"] = f"CAP_SIGNALDATE_TOP{CAP_TOP_N}_BY_SCORE"
                out_df.loc[blocked_idx, "execution_blocked"] = True
                out_df.loc[blocked_idx, "execution_block_reason"] = f"CAP_SIGNALDATE_TOP{CAP_TOP_N}_BY_SCORE"
    except Exception as e:
        cap_notes.append(f"cap_exception={e}")

    # ---- Disclosure risk policy (NEGATIVE => block) ----
    try:
        disclosure_blocked_codes, disclosure_msg, disclosure_generated_at = _load_disclosure_negative_codes()
        disclosure_notes.append(disclosure_msg)
        if disclosure_blocked_codes:
            buy_mask = (out_df["side"].astype(str).str.upper().str.strip() == "BUY")
            nonstop_mask = (out_df["is_stop"] == False)
            code_mask = out_df["code"].astype(str).str.zfill(6).isin(disclosure_blocked_codes)
            disclosure_idx = out_df[buy_mask & nonstop_mask & code_mask].index
            if disclosure_generated_at is not None and len(disclosure_idx) > 0:
                temporal_idx = []
                temporal_skipped = 0
                for i in disclosure_idx:
                    event_dt = _row_policy_event_dt(out_df.loc[i], D)
                    if event_dt is not None and disclosure_generated_at > event_dt:
                        temporal_skipped += 1
                        continue
                    temporal_idx.append(i)
                if temporal_skipped:
                    disclosure_notes.append(f"disclosure_temporal_skipped_rows={temporal_skipped}")
                disclosure_idx = temporal_idx
            if len(disclosure_idx) > 0:
                out_df.loc[disclosure_idx, "disclosure_policy_available"] = True
            if len(disclosure_idx) > 0:
                disclosure_blocked_rows = int(len(disclosure_idx))
                out_df.loc[disclosure_idx, "entry_blocked"] = True
                out_df.loc[disclosure_idx, "posthoc_policy_violation"] = True
                out_df.loc[disclosure_idx, "execution_blocked"] = True
                for i in disclosure_idx:
                    prev = str(out_df.at[i, "entry_block_reason"] or "").strip()
                    prev_posthoc = str(out_df.at[i, "posthoc_policy_reason"] or "").strip()
                    prev_exec = str(out_df.at[i, "execution_block_reason"] or "").strip()
                    add = "DISCLOSURE_NEGATIVE"
                    out_df.at[i, "entry_block_reason"] = add if not prev else (prev if add in prev else f"{prev}|{add}")
                    out_df.at[i, "posthoc_policy_reason"] = add if not prev_posthoc else (prev_posthoc if add in prev_posthoc else f"{prev_posthoc}|{add}")
                    out_df.at[i, "execution_block_reason"] = add if not prev_exec else (prev_exec if add in prev_exec else f"{prev_exec}|{add}")
    except Exception as e:
        disclosure_notes.append(f"disclosure_exception={e}")

    # ---- duplicate guard (merge by key, then fail-closed verify) ----
    dup_rows_precheck = int(out_df.duplicated(subset=["exec_date", "side", "code"], keep=False).sum())
    if dup_rows_precheck > 0:
        out_df, merged_dup_rows = _merge_duplicate_orders(out_df)
        logger.warning(
            "[ORDER_GUARD] duplicate_orders_detected rows=%s merged_rows=%s key=%s",
            dup_rows_precheck,
            merged_dup_rows,
            ["exec_date", "side", "code"],
        )

    # ---- trace identifiers (deterministic, evidence-only lineage) ----
    out_df["intent_id"] = out_df.apply(
        lambda r: f"INTENT_{D}_{r.get('side')}_{r.get('code')}_{_stable_digest(D, r.get('side'), r.get('code'), r.get('signal_date'), r.get('note'))}",
        axis=1,
    )
    out_df["order_id"] = out_df.apply(
        lambda r: f"ORDER_{D}_{r.get('side')}_{r.get('code')}_{_stable_digest(D, r.get('side'), r.get('code'), r.get('fill_qty'), r.get('fill_price'), r.get('intent_id'))}",
        axis=1,
    )
    out_df["trace_id"] = out_df.apply(
        lambda r: f"TRACE_{_stable_digest(D, r.get('side'), r.get('code'), r.get('intent_id'), r.get('order_id'), r.get('entry_block_reason'), r.get('note'))}",
        axis=1,
    )

    # ---- write file ----
    qty_num = pd.to_numeric(out_df["fill_qty"], errors="coerce")
    logger.info(
        "[QTY_TRACE] rows=%s qty_total=%.4f qty_min=%.4f qty_max=%.4f qty_nan=%s qty_le_zero=%s",
        int(len(out_df)),
        float(qty_num.sum(skipna=True)),
        float(qty_num.min(skipna=True)) if len(out_df) else 0.0,
        float(qty_num.max(skipna=True)) if len(out_df) else 0.0,
        int(qty_num.isna().sum()),
        int((qty_num <= 0).sum()),
    )

    out_df["trader_instruction"] = out_df.apply(
        lambda r: _build_trader_instruction(
            r.get("side"),
            bool(r.get("is_stop")),
            bool(r.get("entry_blocked")),
            r.get("entry_block_reason", ""),
            r.get("note", ""),
        ),
        axis=1,
    )

    contract = _build_orders_contract(
        out_df=out_df,
        D=D,
        disclosure_blocked_codes=disclosure_blocked_codes if "disclosure_blocked_codes" in locals() else set(),
        dup_rows_precheck=dup_rows_precheck,
    )
    out_contract = OUT_LOGS_DIR / f"p0_orders_exec_contract_{D}.json"
    out_contract.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("WROTE %s status=%s", out_contract, contract.get("status"))
    if contract.get("status") != "PASS":
        logger.error("STOP orders_exec_contract_failed reasons=%s", contract.get("fail_reasons", []))
        return 2

    out_df.to_excel(out_orders, index=False)
    logger.info("WROTE %s", out_orders)

    # ---- STOP report ----
    stop_df = out_df[out_df["is_stop"] == True].copy()
    stopj = {
        "as_of": D,
        "stop_rows": int(len(stop_df)),
        "by_side": stop_df["side"].value_counts(dropna=False).to_dict(),
        "by_code": stop_df["code"].value_counts(dropna=False).to_dict(),
    }
    out_stop = OUT_LOGS_DIR / f"p0_stop_report_{D}.json"
    out_stop.write_text(json.dumps(stopj, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("WROTE %s stop_rows=%s", out_stop, stopj["stop_rows"])

    # ---- core = STOP ?쒖쇅 + entry_blocked ?쒖쇅 ----
    core = out_df[(out_df["is_stop"] == False) & (out_df["entry_blocked"] == False)].copy()
    core_rows = int(len(core))

    # Minimal core json (keep compatible shape)
    outj = {
        "status": "PASS",
        "as_of": D,
        "summary": {
            "source_fills": str(FILLS.name),
            "fills_rows_as_of": int(len(asof)),
            "exec_date": D,
            "core_rows": core_rows,
            "cap_top_n": CAP_TOP_N,
            "source_entry_blocked_rows": int(_to_bool_series(out_df["entry_blocked"]).sum()),
            "posthoc_policy_violation_rows": int(_to_bool_series(out_df["posthoc_policy_violation"]).sum()),
            "execution_blocked_rows": int(_to_bool_series(out_df["execution_blocked"]).sum()),
        },
        "cap": {
            "notes": cap_notes[:50],  # avoid huge
            "blocked_rows": int(out_df["entry_blocked"].sum()),
            "surge_immediate_excluded_rows": int(cap_surge_excluded_rows),
        },
        "disclosure": {
            "notes": disclosure_notes[:20],
            "blocked_rows": int(disclosure_blocked_rows),
        },
    }
    outp = OUT_LOGS_DIR / f"p0_live_vs_bt_core_{D}.json"
    outp.write_text(json.dumps(outj, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("WROTE %s status=PASS core_rows=%s", outp, core_rows)
    log_pipeline_event(
        stage="orders_exec",
        batch_label="[11/14]",
        event="END",
        date=_audit_ymd,
        output_files={
            "orders_exec": {"path": str(out_orders), "rows": int(len(out_df))},
        },
        metrics={
            "entry_blocked_rows": int(_to_bool_series(out_df["entry_blocked"]).sum()),
            "posthoc_policy_violation_rows": int(_to_bool_series(out_df["posthoc_policy_violation"]).sum()),
            "core_rows": int(core_rows),
        },
        status="PASS",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
