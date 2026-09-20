from __future__ import annotations

import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, List
import pandas as pd
import logging


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from state_containers import PortfolioState, json_safe as state_json_safe
from utils.pipeline_audit import log_pipeline_event, count_rows

PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
RECONCILE_STATUS = LOG_DIR / "reconcile_paper_state_from_fills_latest.json"


def _path_from_env(var_name: str, default_path: Path) -> Path:
    raw = str(os.getenv(var_name, "") or "").strip()
    return Path(raw) if raw else default_path


FILLS = _path_from_env("PAPER_FILLS_PATH", PAPER / "fills.csv")
TRADES = _path_from_env("PAPER_TRADES_PATH", PAPER / "trades.csv")
STATE = _path_from_env("PAPER_STATE_PATH", PAPER / "paper_state.json")
SECTOR_SSOT = ROOT / "_cache" / "sector_ssot.csv"




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
def _norm_code(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[-6:].zfill(6)


def _to_int(v: Any, d: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(d)


def _to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(d)


def _extract_signal_date(note: Any) -> str:
    m = re.search(r"signal_date=(\d{8})", str(note or ""))
    return m.group(1) if m else ""


def _extract_note_field(note: Any, key: str) -> str:
    m = re.search(rf"{re.escape(str(key))}=([^;]*)", str(note or ""))
    return (m.group(1).strip() if m else "")


def _truthy_note(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _derive_exit_state_from_fills(fills_df: pd.DataFrame, code: str, source_order_id: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "stop_exit_count": 0,
        "last_exit_severity": "",
        "executed_sell_tags": [],
        "tp_taken_levels": [],
    }
    src = str(source_order_id or "").strip()
    if not src or not isinstance(fills_df, pd.DataFrame) or fills_df.empty or "note" not in fills_df.columns:
        return out
    stop_reasons = {"STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN"}
    last_ts = ""
    for _, row in fills_df.iterrows():
        if _norm_code(row.get("code", "")) != code:
            continue
        if str(row.get("side", "")).strip().upper() != "SELL":
            continue
        note = str(row.get("note", "") or "")
        row_src = _extract_note_field(note, "source_order_id") or _extract_note_field(note, "entry_order_id")
        if row_src != src:
            continue
        exit_reason = _extract_note_field(note, "exit_reason").strip().upper()
        sell_tag = (_extract_note_field(note, "sell_tag") or exit_reason).strip().upper()
        if sell_tag:
            out["executed_sell_tags"].append(sell_tag)
        m_tp = re.match(r"^TP_L(\d+)$", sell_tag or exit_reason)
        if m_tp:
            out["tp_taken_levels"].append(m_tp.group(1))
        if exit_reason not in stop_reasons:
            continue
        if not _truthy_note(_extract_note_field(note, "partial_exit")):
            continue
        out["stop_exit_count"] = int(out.get("stop_exit_count", 0) or 0) + 1
        row_ts = str(row.get("datetime") or row.get("ts") or "")
        if row_ts >= last_ts:
            last_ts = row_ts
            out["last_exit_severity"] = _extract_note_field(note, "exit_severity")
    return out


def _load_sector_map() -> Dict[str, str]:
    if not SECTOR_SSOT.exists():
        return {}
    try:
        df = pd.read_csv(SECTOR_SSOT, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        return {}
    if "code" not in df.columns or "krx_sector" not in df.columns:
        return {}
    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        code = _norm_code(row.get("code", ""))
        sector = str(row.get("krx_sector", "") or "").strip()
        if code and sector:
            out[code] = sector
    return out


def _backup_state(run_id: str) -> str:
    backup_dir = ROOT / "backup" / "reconcile_paper_state_from_fills" / run_id
    backup_dir.mkdir(parents=True, exist_ok=True)
    if STATE.exists():
        backup_path = backup_dir / "paper_state.json.bak"
        shutil.copy2(STATE, backup_path)
        return str(backup_path)
    return ""


def _write_status(payload: Dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RECONCILE_STATUS.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _restore_state(backup_path: str) -> bool:
    if not backup_path:
        return False
    src = Path(backup_path)
    if not src.exists():
        return False
    STATE.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, STATE)
    return True


# [2026-08-22] 복구 경로용 시가총액 조회 (PLANS 53).
#
# 체결에서 포지션을 복구할 때 후보행이 없어 market_cap 을 0.0 으로 박아 넣었다.
# 그런데 exit.py:752 가 청산 슬리피지 티어를 그 값으로 정한다 - 0 이면 항상
# small_slip_pct(1.0%) 다. 그리고 common.py 의 "테마주" 분류도 0 < 시총 < 3000억
# 조건이라 영원히 안 걸렸다.
#
# 실측(2026-08-22): 보유 중이던 005690 이 market_cap=0.0 이었고, 이 도구가
# 매 사이클 포지션을 재구성하므로 상태 파일을 손으로 고쳐도 60초 안에 되돌아갔다.
# 즉 여기가 진짜 수리 지점이다.
#
# 같은 헬퍼가 paper_engine/positions.py 와 tools/reconcile_paper_state_from_fills.py
# 두 곳에 있다. 독립 실행 도구에 paper_engine 을 import 시키지 않으려고 복제했다.
# 순수 함수이고 파일 하나만 읽는다. 한쪽을 고치면 다른 쪽도 같이 고칠 것.
@lru_cache(maxsize=1)
def _market_cap_table() -> Dict[str, float]:
    table: Dict[str, float] = {}
    try:
        path = ROOT / "_cache" / "pykrx_fundamental_latest.csv"
        if not path.exists():
            return table
        df = pd.read_csv(path, dtype={"code": str})
        if "code" not in df.columns or "market_cap" not in df.columns:
            return table
        caps = pd.to_numeric(df["market_cap"], errors="coerce")
        codes = df["code"].astype(str).str.strip().str.zfill(6)
        for code, cap in zip(codes, caps):
            if pd.notna(cap) and float(cap) > 0:
                table[str(code)] = float(cap)
    except Exception:
        return {}
    return table


def _lookup_market_cap(code: Any) -> float:
    """모르면 0.0 을 돌려준다 - 기존 동작과 같다. 알면 실제 값을 준다."""
    key = str(code or "").strip().zfill(6)
    return float(_market_cap_table().get(key, 0.0))


def _write_state_with_backup(st: Dict[str, Any], expected_open_positions: int, run_id: str) -> int:
    backup_path = _backup_state(run_id)
    payload: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "state_path": str(STATE),
        "backup_path": backup_path,
        "expected_open_positions": int(expected_open_positions),
        "rollback_attempted": False,
        "rollback_restored": False,
        "status": "UNKNOWN",
        "issues": [],
    }
    try:
        STATE.parent.mkdir(parents=True, exist_ok=True)
        STATE.write_text(json.dumps(state_json_safe(st), ensure_ascii=True, indent=2, allow_nan=False), encoding="utf-8")
        reread = json.loads(STATE.read_text(encoding="utf-8"))
        actual = len(reread.get("open_positions") or []) if isinstance(reread, dict) else -1
        payload["actual_open_positions"] = int(actual)
        if actual != int(expected_open_positions):
            payload["issues"].append(f"open_positions_mismatch:{actual}!={expected_open_positions}")
        payload["status"] = "PASS" if not payload["issues"] else "FAIL"
    except Exception as exc:
        payload["status"] = "FAIL"
        payload["issues"].append(f"write_or_validate_failed:{type(exc).__name__}:{exc}")

    if payload["status"] != "PASS":
        payload["rollback_attempted"] = True
        payload["rollback_restored"] = _restore_state(backup_path)
        _write_status(payload)
        return 3

    _write_status(payload)
    return 0


def main() -> int:
    run_id = "RECONCILE_STATE_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    _audit_ymd = datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="state_reconcile",
        batch_label="[6.95b/9]",
        event="START",
        date=_audit_ymd,
        input_files={
            "fills": str(FILLS),
            "trades": str(TRADES),
        },
    )
    if not FILLS.exists():
        _log_print(f"[FAIL] missing {FILLS}")
        return 2
    fills = pd.read_csv(FILLS, dtype=str)
    if fills.empty:
        _log_print("[OK] fills empty -> write empty state")
        st = PortfolioState(open_positions=[], next_trade_seq=1, processed_signals=[]).to_dict()
        rc = _write_state_with_backup(st, 0, run_id)
        if rc == 0:
            _log_print(f"[OK] reconcile status={RECONCILE_STATUS}")
        return rc

    qty_col = "qty" if "qty" in fills.columns else ("fill_qty" if "fill_qty" in fills.columns else None)
    px_col = "price" if "price" in fills.columns else ("fill_price" if "fill_price" in fills.columns else None)
    ts_col = "datetime" if "datetime" in fills.columns else ("ts" if "ts" in fills.columns else ("date" if "date" in fills.columns else None))
    if qty_col is None or px_col is None or ts_col is None:
        _log_print("[FAIL] fills schema missing qty/price/datetime")
        return 2

    fills = fills.copy()
    fills["_ts"] = fills[ts_col].astype(str)
    fills["_code_n"] = fills.get("code", pd.Series([""] * len(fills), index=fills.index)).astype(str).apply(_norm_code)
    fills["_side_n"] = fills.get("side", pd.Series([""] * len(fills), index=fills.index)).astype(str).str.strip().str.upper()
    fills["_qty_n"] = fills.get(qty_col, pd.Series([0] * len(fills), index=fills.index)).apply(_to_int)
    fills["_px_n"] = fills.get(px_col, pd.Series([0] * len(fills), index=fills.index)).apply(_to_int)
    if "order_id" in fills.columns:
        fills["_oid_n"] = fills.get("order_id", "").astype(str).str.strip()
    else:
        fills["_oid_n"] = ""
    before_dedup = len(fills)
    fills = fills.drop_duplicates(
        subset=["_ts", "_code_n", "_side_n", "_qty_n", "_px_n", "_oid_n"],
        keep="last",
    ).copy()
    dedup_removed = max(0, int(before_dedup - len(fills)))
    if dedup_removed > 0:
        _log_print(f"[INFO] reconcile dedup removed duplicate fill rows={dedup_removed}")
    fills = fills.sort_values("_ts", kind="mergesort")

    net_qty: Dict[str, int] = {}
    net_qty_by_lineage: Dict[tuple[str, str], int] = {}
    last_buy: Dict[str, Dict[str, Any]] = {}
    last_buy_by_lineage: Dict[tuple[str, str], Dict[str, Any]] = {}
    processed = set()
    sector_map = _load_sector_map()
    for _, r in fills.iterrows():
        code = _norm_code(r.get("code", ""))
        side = str(r.get("side", "")).strip().upper()
        qty = _to_int(r.get(qty_col), 0)
        if not code or qty <= 0:
            continue
        net_qty[code] = int(net_qty.get(code, 0))
        note = str(r.get("note", "") or "")
        sd = _extract_signal_date(note)
        if sd:
            processed.add(f"{code}:{sd}")
        if side == "BUY":
            net_qty[code] += qty
            ts = str(r.get("_ts", "") or "")
            ymd = re.sub(r"[^0-9]", "", ts)[:8]
            surge_immediate = _truthy_note(_extract_note_field(note, "surge_immediate"))
            surge_type = str(_extract_note_field(note, "surge_type") or "").strip()
            horizon_label = str(_extract_note_field(note, "horizon") or "").strip()
            if horizon_label.upper() in {"NAN", "NONE", "NULL"}:
                horizon_label = ""
            entry_order_id = str(_extract_note_field(note, "entry_order_id") or r.get("order_id", "") or "").strip()
            entry_intent_id = str(_extract_note_field(note, "entry_intent_id") or "").strip()
            entry_trace_id = str(_extract_note_field(note, "entry_trace_id") or "").strip()
            lineage_origin = str(_extract_note_field(note, "lineage_origin") or "").strip()
            source_order_id = str(_extract_note_field(note, "source_order_id") or "").strip()
            source_intent_id = str(_extract_note_field(note, "source_intent_id") or "").strip()
            source_trace_id = str(_extract_note_field(note, "source_trace_id") or "").strip()
            replay_chain_id = str(_extract_note_field(note, "replay_chain_id") or "").strip()
            replay_depth = _to_int(_extract_note_field(note, "replay_depth"), 0)
            has_explicit_source_lineage = bool(source_order_id or source_intent_id or source_trace_id)
            if replay_chain_id:
                source_order_id = source_order_id or entry_order_id
                source_intent_id = source_intent_id or entry_intent_id
                source_trace_id = source_trace_id or entry_trace_id
            if not lineage_origin:
                lineage_origin = "OPEN_ORDER_REPLAY" if has_explicit_source_lineage else "FRESH_SIGNAL"
            last_buy[code] = {
                "name": str(r.get("name", "") or "").strip(),
                "entry_date": ymd,
                "entry_ts": ts if ts else f"{ymd}T09:00:00",
                "signal_date": sd if sd else ymd,
                "entry_price": _to_float(r.get(px_col), 1.0),
                "order_id": str(r.get("order_id", "") or "").strip(),
                "entry_order_id": entry_order_id,
                "entry_intent_id": entry_intent_id,
                "entry_trace_id": entry_trace_id,
                "lineage_origin": lineage_origin,
                "source_order_id": source_order_id,
                "source_intent_id": source_intent_id,
                "source_trace_id": source_trace_id,
                "replay_chain_id": replay_chain_id,
                "replay_depth": replay_depth,
                "_surge_immediate": 1 if surge_immediate else 0,
                "surge_type": surge_type,
                "horizon_label": horizon_label,
            }
            if entry_order_id:
                key = (code, entry_order_id)
                net_qty_by_lineage[key] = int(net_qty_by_lineage.get(key, 0)) + int(qty)
                last_buy_by_lineage[key] = dict(last_buy[code])
        elif side == "SELL":
            net_qty[code] = max(0, net_qty[code] - qty)
            sell_source_order_id = (
                _extract_note_field(note, "source_order_id")
                or _extract_note_field(note, "entry_order_id")
                or ""
            ).strip()
            if sell_source_order_id:
                key = (code, sell_source_order_id)
                net_qty_by_lineage[key] = int(net_qty_by_lineage.get(key, 0)) - int(qty)

    def _lineage_buy_sort_value(key: tuple[str, str]) -> str:
        buy = last_buy_by_lineage.get(key, {})
        return str(buy.get("entry_ts") or buy.get("entry_date") or key[1])

    def _select_info_for_open_code(code: str, code_net_qty: int) -> tuple[Dict[str, Any], int]:
        info = last_buy.get(code, {})
        entry_order_id = str(info.get("entry_order_id") or info.get("order_id", "") or "").strip()
        if entry_order_id and int(net_qty_by_lineage.get((code, entry_order_id), 0)) <= 0:
            candidates = [
                key
                for key, qty in net_qty_by_lineage.items()
                if key[0] == code and int(qty) > 0 and key[1] != entry_order_id
            ]
            if candidates:
                best_key = sorted(candidates, key=_lineage_buy_sort_value, reverse=True)[0]
                return dict(last_buy_by_lineage.get(best_key, info)), min(int(code_net_qty), int(net_qty_by_lineage.get(best_key, code_net_qty)))
        return info, int(code_net_qty)

    open_positions: List[Dict[str, Any]] = []
    for code in sorted(k for k, v in net_qty.items() if int(v) > 0):
        info, position_qty = _select_info_for_open_code(code, int(net_qty[code]))
        entry_date = str(info.get("entry_date", "") or "19700101")
        entry_price = _to_float(info.get("entry_price"), 1.0)
        entry_order_id = str(info.get("entry_order_id") or info.get("order_id", "") or f"RECOVERED_BUY_{code}_{entry_date}")
        entry_intent_id = str(info.get("entry_intent_id") or f"RECOVERED_INTENT_{code}_{entry_date}")
        entry_trace_id = str(info.get("entry_trace_id") or f"RECOVERED_TRACE_{code}_{entry_date}")
        lineage_origin = str(info.get("lineage_origin") or "RECOVERED_FROM_FILLS")
        source_order_id = str(info.get("source_order_id") or "")
        source_intent_id = str(info.get("source_intent_id") or "")
        source_trace_id = str(info.get("source_trace_id") or "")
        replay_chain_id = str(info.get("replay_chain_id") or f"RECOVERED_CHAIN_{code}_{entry_date}")
        replay_depth = _to_int(info.get("replay_depth"), 0)
        if replay_chain_id:
            source_order_id = source_order_id or entry_order_id
            source_intent_id = source_intent_id or entry_intent_id
            source_trace_id = source_trace_id or entry_trace_id
        pos = {
            "code": code,
            "name": str(info.get("name", "") or ""),
            "qty": int(position_qty),
            "signal_date": str(info.get("signal_date", "") or entry_date),
            "entry_date": entry_date,
            "entry_ts": str(info.get("entry_ts", "") or f"{entry_date}T09:00:00"),
            "entry_price": float(entry_price),
            "max_close": float(entry_price),
            "asset_type": "",
            "sector": str(sector_map.get(code, "") or ""),
            "fundamentals": {},
            "tp_taken_levels": [],
            "executed_sell_tags": [],
            "stop_loss": -0.05,
            "take_profit": None,
            "trail_pct": None,
            "entry_order_id": entry_order_id,
            "entry_intent_id": entry_intent_id,
            "entry_trace_id": entry_trace_id,
            "lineage_origin": lineage_origin,
            "source_order_id": source_order_id,
            "source_intent_id": source_intent_id,
            "source_trace_id": source_trace_id,
            "replay_chain_id": replay_chain_id,
            "replay_depth": replay_depth,
            "market_cap": _lookup_market_cap(code),
            "_surge_immediate": int(_to_int(info.get("_surge_immediate", 0), 0) if info else 0),
            "surge_type": str(info.get("surge_type", "") or "") if info else "",
            "horizon_label": str(info.get("horizon_label", "") or "") if info else "",
            "horizon_max_hold_days": 0,
        }
        exit_state = _derive_exit_state_from_fills(fills, code, source_order_id or entry_order_id)
        executed_tags = sorted({str(x).strip() for x in exit_state.get("executed_sell_tags", []) if str(x).strip()})
        tp_levels = sorted(
            {str(x).strip() for x in exit_state.get("tp_taken_levels", []) if str(x).strip()},
            key=lambda x: int(x) if str(x).isdigit() else str(x),
        )
        if executed_tags:
            pos["executed_sell_tags"] = executed_tags
        if tp_levels:
            pos["tp_taken_levels"] = tp_levels
        if int(exit_state.get("stop_exit_count", 0) or 0) > 0:
            pos["stop_exit_count"] = int(exit_state.get("stop_exit_count", 0) or 0)
            pos["last_exit_severity"] = str(exit_state.get("last_exit_severity", "") or "")
        open_positions.append(pos)

    next_trade_seq = 1
    if TRADES.exists():
        tdf = pd.read_csv(TRADES, dtype=str)
        if "trade_id" in tdf.columns:
            nums = []
            for x in tdf["trade_id"].astype(str):
                m = re.search(r"(\d+)$", x)
                if m:
                    nums.append(int(m.group(1)))
            if nums:
                next_trade_seq = max(nums) + 1

    st_raw = {
        "open_positions": open_positions,
        "next_trade_seq": int(next_trade_seq),
        "processed_signals": sorted(processed),
    }
    st = PortfolioState.from_raw(st_raw).to_dict()
    rc = _write_state_with_backup(st, len(open_positions), run_id)
    if rc != 0:
        _log_print(f"[FAIL] reconcile write validation failed status={RECONCILE_STATUS}")
        return rc
    _log_print(f"[OK] wrote {STATE} open_positions={len(open_positions)} next_trade_seq={next_trade_seq}")
    _log_print(f"[OK] reconcile status={RECONCILE_STATUS}")
    log_pipeline_event(
        stage="state_reconcile",
        batch_label="[6.95b/9]",
        event="END",
        date=_audit_ymd,
        output_files={
            "paper_state": {"path": str(STATE), "rows": int(len(open_positions))},
        },
        metrics={
            "open_positions": int(len(open_positions)),
            "dedup_removed": int(dedup_removed),
            "next_trade_seq": int(next_trade_seq),
        },
        status="PASS",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


