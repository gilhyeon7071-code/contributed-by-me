from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOTA = Path(__file__).resolve().parents[1]
ROOTB = ROOTA.parent / "vibe" / "buffett"
FILLS_PATH = ROOTA / "paper" / "fills.csv"
LIVE_PATH = ROOTB / "data" / "live" / "live_fills.csv"
LEDGER_PATH = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"
STATUS_PATH = ROOTA / "2_Logs" / "rootb_fills_from_roota_latest.json"


def _date8(v: Any) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())[:8]


def _code(v: Any) -> str:
    raw = str(v or "").strip().replace(".0", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _side(v: Any) -> str:
    s = str(v or "").strip().upper()
    return {"B": "BUY", "S": "SELL"}.get(s, s)


def _num_key(v: Any) -> str:
    try:
        return f"{float(str(v).replace(',', '')):.6f}"
    except Exception:
        return "0.000000"


def _stable_id(*parts: Any) -> str:
    text = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp_{dt.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)


def _atomic_write_excel(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp_{dt.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}{path.suffix}")
    df.to_excel(tmp, index=False)
    tmp.replace(path)


def _resolve_d(fills: pd.DataFrame, requested: str) -> str:
    if requested:
        return _date8(requested)
    if fills.empty or "datetime" not in fills.columns:
        return ""
    side = fills.get("side", pd.Series([""] * len(fills), index=fills.index)).map(_side)
    work = fills[side.eq("BUY")].copy()
    if work.empty:
        work = fills.copy()
    dates = work["datetime"].map(_date8)
    dates = dates[dates.str.len().eq(8)]
    return str(dates.max() or "")


def _load_ledger_helpers():
    sys.path.insert(0, str(ROOTB / "tools"))
    from ledger_cost_model import build_enriched_ledger, deduplicate_ledger  # type: ignore

    return build_enriched_ledger, deduplicate_ledger


def _orders_exec_path(ymd: str) -> Path:
    return ROOTB / "data" / "orders" / f"orders_{ymd}_exec.xlsx"


def _roota_orders_exec_path(ymd: str) -> Path:
    return ROOTA / "paper" / f"orders_{ymd}_exec.xlsx"


def _read_orders_exec(ymd: str) -> pd.DataFrame:
    path = _orders_exec_path(ymd)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, dtype=str).fillna("")


def _read_roota_orders_exec(ymd: str) -> pd.DataFrame:
    path = _roota_orders_exec_path(ymd)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, dtype=str).fillna("")


POLICY_SYNC_COLUMNS = (
    "stop_level",
    "signal_date",
    "signal_ts",
    "is_stop",
    "note",
    "reason",
    "entry_blocked",
    "entry_block_reason",
    "posthoc_policy_violation",
    "posthoc_policy_reason",
    "execution_blocked",
    "execution_block_reason",
    "disclosure_policy_available",
    "trader_instruction",
)


def _sync_orders_exec_policy_from_roota(orders: pd.DataFrame, roota_orders: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if orders.empty or roota_orders.empty:
        return orders, 0
    if not {"code", "side"}.issubset(set(orders.columns)) or not {"code", "side"}.issubset(set(roota_orders.columns)):
        return orders, 0

    out = orders.copy()
    src = roota_orders.copy()
    out["_sync_code"] = out["code"].map(_code)
    out["_sync_side"] = out["side"].map(_side)
    src["_sync_code"] = src["code"].map(_code)
    src["_sync_side"] = src["side"].map(_side)
    src = src[(src["_sync_code"].str.len().eq(6)) & (src["_sync_side"].isin(["BUY", "SELL"]))].copy()
    if src.empty:
        return orders, 0

    changed = 0
    for (code, side), g in src.groupby(["_sync_code", "_sync_side"], sort=False):
        if len(g) != 1:
            continue
        mask = out["_sync_code"].eq(code) & out["_sync_side"].eq(side)
        if not bool(mask.any()):
            continue
        row = g.iloc[0]
        before = out.loc[mask].astype(str).copy()
        for col in POLICY_SYNC_COLUMNS:
            if col in out.columns and col in src.columns:
                out.loc[mask, col] = row.get(col, "")
        if not before.equals(out.loc[mask].astype(str)):
            changed += int(mask.sum())

    out = out.drop(columns=["_sync_code", "_sync_side"], errors="ignore")
    return out, changed


def _sync_orders_exec_from_rows(orders: pd.DataFrame, rows: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if orders.empty or rows.empty or "order_id" not in orders.columns or "order_id" not in rows.columns:
        return orders, 0
    out = orders.copy()
    work = rows.copy()
    work["_order_id"] = work["order_id"].astype(str).str.strip()
    work = work[work["_order_id"].ne("")].copy()
    if work.empty:
        return out, 0
    fill_ids_by_order = work.groupby("_order_id")["fill_id"].apply(
        lambda s: "|".join(str(x).strip() for x in s.astype(str).tolist() if str(x).strip())
    )
    qty_by_order = work.groupby("_order_id")["fill_qty"].sum()
    order_ids = out["order_id"].astype(str).str.strip()
    changed = 0
    for order_id, fill_ids in fill_ids_by_order.items():
        mask = order_ids.eq(order_id)
        if not bool(mask.any()):
            continue
        qty = float(qty_by_order.get(order_id, 0.0) or 0.0)
        qty_text = str(int(qty)) if abs(qty - int(qty)) <= 1e-9 else f"{qty:.6f}".rstrip("0").rstrip(".")
        before = out.loc[mask].astype(str).copy()
        if "source_fill_ids" in out.columns:
            out.loc[mask, "source_fill_ids"] = fill_ids
        for col in ("fill_qty", "filled_qty", "qty"):
            if col in out.columns:
                out.loc[mask, col] = qty_text
        if not before.equals(out.loc[mask].astype(str)):
            changed += int(mask.sum())
    return out, changed


def _append_missing_orders_exec_from_rows(orders: pd.DataFrame, rows: pd.DataFrame, ymd: str) -> tuple[pd.DataFrame, int]:
    if rows.empty:
        return orders, 0
    need = {"code", "side", "fill_qty", "fill_price", "fill_id"}
    if not need.issubset(set(rows.columns)):
        return orders, 0

    out = orders.copy()
    existing_keys: set[tuple[str, str]] = set()
    if not out.empty and {"code", "side"}.issubset(set(out.columns)):
        existing_keys = {
            (_code(r.get("code")), _side(r.get("side")))
            for _, r in out.iterrows()
            if _code(r.get("code")) and _side(r.get("side")) in {"BUY", "SELL"}
        }

    work = rows.copy()
    work["_code"] = work["code"].map(_code)
    work["_side"] = work["side"].map(_side)
    work["_qty"] = pd.to_numeric(work["fill_qty"], errors="coerce").fillna(0.0)
    work["_price"] = pd.to_numeric(work["fill_price"], errors="coerce").fillna(0.0)
    work = work[(work["_code"].str.len().eq(6)) & (work["_side"].isin(["BUY", "SELL"])) & (work["_qty"] > 0)].copy()
    if work.empty:
        return out, 0

    add_rows: list[dict[str, Any]] = []
    for (code, side), g in work.groupby(["_code", "_side"], sort=False):
        if (code, side) in existing_keys:
            continue
        qty_sum = float(g["_qty"].sum())
        if qty_sum <= 0:
            continue
        px = float((g["_qty"] * g["_price"]).sum() / qty_sum)
        source_fill_ids = "|".join(
            str(x).strip() for x in g["fill_id"].astype(str).tolist() if str(x).strip()
        )
        source_trace_ids = "|".join(
            str(x).strip() for x in g.get("trace_id", pd.Series("", index=g.index)).astype(str).tolist() if str(x).strip()
        )
        note_vals = [str(x).strip() for x in g.get("note", pd.Series("", index=g.index)).astype(str).tolist() if str(x).strip()]
        note = " | ".join(dict.fromkeys(note_vals).keys())[:500]
        order_id = f"EXEC_INTENT_{ymd}_{side}_{code}"
        add_rows.append(
            {
                "exec_date": ymd,
                "side": side,
                "code": code,
                "fill_qty": qty_sum,
                "fill_price": px,
                "stop_level": px * 0.95 if side == "BUY" else "",
                "signal_date": ymd,
                "signal_ts": "",
                "is_stop": bool(side == "SELL" and "STOP" in note.upper()),
                "note": note,
                "reason": "EXEC_FROM_FILLS_MISSING_ORDER",
                "entry_blocked": False,
                "entry_block_reason": "",
                "posthoc_policy_violation": False,
                "posthoc_policy_reason": "",
                "execution_blocked": False,
                "execution_block_reason": "",
                "disclosure_policy_available": False,
                "intent_id": f"EXEC_INTENT_{ymd}_{side}_{code}",
                "order_id": order_id,
                "trace_id": "TRACE_" + _stable_id(ymd, side, code, source_fill_ids),
                "trader_instruction": "fills 기반 실행행 보정",
                "order_qty": qty_sum,
                "filled_qty": qty_sum,
                "remaining_qty": 0,
                "order_state": "FILLED",
                "fill_session": ymd,
                "intended_session": ymd,
                "time_in_force": "",
                "replay_policy": "",
                "recovery_decision": "",
                "remaining_action": "",
                "next_session_ymd": "",
                "expire_session_ymd": "",
                "replay_depth": 0,
                "last_event_at": str(g["datetime"].astype(str).max() if "datetime" in g.columns else ""),
                "fill_id": "FILL_" + _stable_id(ymd, side, code, source_fill_ids),
                "source_fill_ids": source_fill_ids,
                "source_trace_ids": source_trace_ids,
                "replay_chain_id": "",
                "replay_source_order_id": "",
                "replay_source_intent_id": "",
                "replay_source_trace_id": "",
                "fill_count": int(len(g)),
                "first_fill_time": str(g["datetime"].astype(str).min() if "datetime" in g.columns else ""),
                "last_fill_time": str(g["datetime"].astype(str).max() if "datetime" in g.columns else ""),
            }
        )
    if not add_rows:
        return out, 0

    add_df = pd.DataFrame(add_rows)
    if out.empty:
        out = add_df.copy()
    else:
        for col in add_df.columns:
            if col not in out.columns:
                out[col] = ""
        for col in out.columns:
            if col not in add_df.columns:
                add_df[col] = ""
        out = pd.concat([out, add_df[out.columns]], ignore_index=True)
    return out, int(len(add_rows))


def _build_exec_order_map(orders: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    if orders.empty or not {"code", "side", "order_id"}.issubset(set(orders.columns)):
        return {}
    work = orders.copy()
    work["_code"] = work["code"].map(_code)
    work["_side"] = work["side"].map(_side)
    work["_order_id"] = work["order_id"].astype(str).str.strip()
    work = work[(work["_code"].str.len() == 6) & (work["_side"].isin(["BUY", "SELL"])) & (work["_order_id"] != "")].copy()
    qty_col = next((c for c in ("fill_qty", "filled_qty", "order_qty", "qty") if c in work.columns), "")
    if qty_col:
        work["_target_qty"] = pd.to_numeric(work[qty_col], errors="coerce").fillna(0.0)
    else:
        work["_target_qty"] = 0.0
    counts = work.groupby(["_code", "_side"])["_order_id"].nunique()
    unique_keys = {k for k, v in counts.items() if int(v) == 1}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for _, row in work.iterrows():
        key = (str(row["_code"]), str(row["_side"]))
        if key in unique_keys:
            out[key] = {
                "order_id": str(row["_order_id"]),
                "target_qty": float(row.get("_target_qty") or 0.0),
            }
    return out


def _assign_exec_order_ids(rows: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    out = rows.copy()
    out["order_id"] = out["source_order_id"].astype(str).str.strip()
    exec_order_map = _build_exec_order_map(orders)
    if not exec_order_map:
        return out

    for (code, side), meta in exec_order_map.items():
        order_id = str(meta.get("order_id") or "").strip()
        target_qty = float(meta.get("target_qty") or 0.0)
        if not order_id or target_qty <= 0:
            continue
        idxs = out.index[(out["code"].astype(str).eq(code)) & (out["side"].astype(str).eq(side))].tolist()
        if not idxs:
            continue

        selected: list[Any] = []
        total = 0.0
        for idx in idxs:
            qty = float(pd.to_numeric(pd.Series([out.at[idx, "fill_qty"]]), errors="coerce").fillna(0.0).iloc[0])
            if qty <= 0:
                continue
            if total + qty <= target_qty + 1e-6:
                selected.append(idx)
                total += qty
            if abs(total - target_qty) <= 1e-6:
                break

        if selected and abs(total - target_qty) <= 1e-6:
            out.loc[selected, "order_id"] = order_id
        elif len(idxs) == 1:
            qty = float(pd.to_numeric(pd.Series([out.at[idxs[0], "fill_qty"]]), errors="coerce").fillna(0.0).iloc[0])
            if abs(qty - target_qty) <= 1e-6:
                out.loc[idxs, "order_id"] = order_id
    return out


def _backup(run_id: str, apply: bool) -> str:
    if not apply:
        return ""
    today = dt.datetime.now().strftime("%Y%m%d")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = ROOTA / "backup" / f"{today}_rootb_fills_from_roota_sync" / stamp
    backup_dir.mkdir(parents=True, exist_ok=True)
    d = _resolve_d(_read_csv(FILLS_PATH), "")
    for src in (LIVE_PATH, LEDGER_PATH, _orders_exec_path(d)):
        if src.exists():
            shutil.copy2(src, backup_dir / f"{src.name}.bak")
    return str(backup_dir)


def _roota_rows_for_d(fills: pd.DataFrame, ymd: str, orders: pd.DataFrame) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    need = {"datetime", "code", "side", "qty", "price"}
    if not need.issubset(set(fills.columns)):
        missing = sorted(need - set(fills.columns))
        raise ValueError(f"fills.csv missing columns: {missing}")
    work = fills.copy()
    work["_date8"] = work["datetime"].map(_date8)
    work = work[work["_date8"].eq(ymd)].copy()
    if work.empty:
        return pd.DataFrame()
    note = work["note"] if "note" in work.columns else pd.Series([""] * len(work), index=work.index)
    roota_order_id = work["order_id"] if "order_id" in work.columns else pd.Series([""] * len(work), index=work.index)
    out = pd.DataFrame(
        {
            "date": ymd,
            "datetime": work["datetime"].astype(str),
            "code": work["code"].map(_code),
            "side": work["side"].map(_side),
            "fill_price": pd.to_numeric(work["price"], errors="coerce"),
            "fill_qty": pd.to_numeric(work["qty"], errors="coerce"),
            "qty": pd.to_numeric(work["qty"], errors="coerce"),
            "price": pd.to_numeric(work["price"], errors="coerce"),
            "source_order_id": roota_order_id.astype(str).str.strip(),
            "note": note.astype(str),
            "source": "PAPER_ENGINE_BRIDGE",
            "as_of": ymd,
        }
    )
    out = out[(out["code"].str.len().eq(6)) & (out["side"].isin(["BUY", "SELL"]))].copy()
    out = out[(out["fill_qty"] > 0) & (out["fill_price"] > 0)].copy()
    out = _assign_exec_order_ids(out, orders)
    out["fill_id"] = out.apply(
        lambda r: "FILL_" + _stable_id(
            ymd,
            r.get("datetime"),
            r.get("code"),
            r.get("side"),
            r.get("source_order_id"),
            r.get("order_id"),
            _num_key(r.get("fill_qty")),
            _num_key(r.get("fill_price")),
        ),
        axis=1,
    )
    out.attrs["duplicate_fill_id_rows_dropped"] = int(out["fill_id"].duplicated().sum())
    if out.attrs["duplicate_fill_id_rows_dropped"] > 0:
        out = out.drop_duplicates(subset=["fill_id"], keep="first").copy()
    return out


def repair(ymd: str = "", apply: bool = False) -> dict[str, Any]:
    run_id = "ROOTB_FROM_ROOTA_FILLS_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    fills = _read_csv(FILLS_PATH)
    d = _resolve_d(fills, ymd)
    if len(d) != 8:
        raise ValueError("cannot resolve D from RootA fills.csv")
    orders_exec = _read_orders_exec(d)
    roota_orders_exec = _read_roota_orders_exec(d)
    orders_for_mapping = roota_orders_exec if not roota_orders_exec.empty else orders_exec
    roota_d = _roota_rows_for_d(fills, d, orders_for_mapping)
    live = _read_csv(LIVE_PATH)
    ledger = _read_csv(LEDGER_PATH)
    live_d_before = int(live["date"].map(_date8).eq(d).sum()) if not live.empty and "date" in live.columns else 0
    ledger_d_before = int(ledger["date"].map(_date8).eq(d).sum()) if not ledger.empty and "date" in ledger.columns else 0
    backup_dir = _backup(run_id, apply)

    result: dict[str, Any] = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "as_of_ymd": d,
        "apply": bool(apply),
        "paths": {
            "roota_fills": str(FILLS_PATH),
            "rootb_live": str(LIVE_PATH),
            "rootb_ledger": str(LEDGER_PATH),
            "orders_exec": str(_orders_exec_path(d)),
            "roota_orders_exec": str(_roota_orders_exec_path(d)),
        },
        "roota_rows_D": int(len(roota_d)),
        "orders_exec_rows": int(len(orders_exec)),
        "roota_orders_exec_rows": int(len(roota_orders_exec)),
        "rootb_order_id_mapped_rows": int(roota_d["order_id"].astype(str).str.startswith("ORDER_").sum()) if "order_id" in roota_d.columns else 0,
        "fill_id_rows": int(roota_d["fill_id"].astype(str).str.strip().ne("").sum()) if "fill_id" in roota_d.columns else 0,
        "duplicate_fill_id_rows_dropped": int(roota_d.attrs.get("duplicate_fill_id_rows_dropped", 0) or 0),
        "orders_exec_replaced_from_roota_rows": 0,
        "orders_exec_policy_synced_rows": 0,
        "orders_exec_synced_rows": 0,
        "live_rows_D_before": live_d_before,
        "ledger_rows_D_before": ledger_d_before,
        "live_rows_D_after": int(len(roota_d)) if apply else live_d_before,
        "ledger_rows_D_after": int(len(roota_d)) if apply else ledger_d_before,
        "backup_dir": backup_dir,
        "status": "DRY_RUN" if not apply else "APPLIED",
    }
    if not apply:
        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATUS_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result

    if not roota_orders_exec.empty:
        orders_path = _orders_exec_path(d)
        old_cmp = orders_exec.astype(str).reset_index(drop=True) if not orders_exec.empty else pd.DataFrame()
        new_cmp = roota_orders_exec.astype(str).reset_index(drop=True)
        if orders_exec.empty or not old_cmp.equals(new_cmp):
            _atomic_write_excel(orders_path, roota_orders_exec)
            orders_exec = roota_orders_exec.copy()
            result["orders_exec_replaced_from_roota_rows"] = int(len(roota_orders_exec))
        orders_exec_policy_synced_rows = 0
        orders_exec_synced_rows = 0
        orders_exec_appended_rows = 0
    else:
        orders_synced, orders_exec_policy_synced_rows = _sync_orders_exec_policy_from_roota(orders_exec, roota_orders_exec)
        orders_synced, orders_exec_synced_rows = _sync_orders_exec_from_rows(orders_synced, roota_d)
        orders_synced, orders_exec_appended_rows = _append_missing_orders_exec_from_rows(orders_synced, roota_d, d)
        if orders_exec_policy_synced_rows or orders_exec_synced_rows or orders_exec_appended_rows:
            orders_path = _orders_exec_path(d)
            _atomic_write_excel(orders_path, orders_synced)
            orders_exec = orders_synced
    result["orders_exec_policy_synced_rows"] = int(orders_exec_policy_synced_rows)
    result["orders_exec_synced_rows"] = int(orders_exec_synced_rows)
    result["orders_exec_appended_rows"] = int(orders_exec_appended_rows)

    if live.empty:
        live_out = roota_d.copy()
    else:
        live_keep = live[live["date"].map(_date8).ne(d)].copy() if "date" in live.columns else live.copy()
        live_out = pd.concat([live_keep, roota_d], ignore_index=True)
        for col in live.columns:
            if col not in live_out.columns:
                live_out[col] = ""
        live_out = live_out[live.columns].copy()
    _atomic_write_csv(LIVE_PATH, live_out)
    _atomic_write_csv(LIVE_PATH.with_name("live_fills_today.csv"), live_out[live_out["date"].map(_date8).eq(d)].copy())

    ledger_keep = ledger[ledger["date"].map(_date8).ne(d)].copy() if not ledger.empty and "date" in ledger.columns else ledger.copy()
    build_enriched_ledger, deduplicate_ledger = _load_ledger_helpers()
    merged = pd.concat([ledger_keep, roota_d], ignore_index=True)
    enriched = build_enriched_ledger(merged, root=str(ROOTB), default_source="PAPER")
    ledger_out = deduplicate_ledger(enriched)
    _atomic_write_csv(LEDGER_PATH, ledger_out)
    result["live_rows_D_after"] = int(live_out["date"].map(_date8).eq(d).sum())
    result["ledger_rows_D_after"] = int(ledger_out["date"].map(_date8).eq(d).sum())
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync RootB live/ledger D rows from RootA paper/fills.csv.")
    ap.add_argument("--date", default="", help="YYYYMMDD. Defaults to RootA fills D rule.")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    payload = repair(ymd=str(args.date or ""), apply=bool(args.apply))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"DRY_RUN", "APPLIED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
