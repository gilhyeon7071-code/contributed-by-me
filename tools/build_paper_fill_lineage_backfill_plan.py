from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOGS = ROOT / "2_Logs"

FILLS = PAPER / "fills.csv"
CLASSIFICATION = LOGS / "paper_sync_lineage_backfill_classification_latest.json"
OUT_LATEST = LOGS / "paper_fill_lineage_backfill_plan_latest.json"


def _note_value(note: Any, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return str(m.group(1)).strip() if m else ""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _lineage_order(fills: pd.DataFrame, code: str) -> list[str]:
    rows = fills[fills["code"].astype(str).str.zfill(6).eq(code)].copy()
    rows["datetime_s"] = rows.get("datetime", "").astype(str)
    rows = rows.sort_values(["datetime_s", "side", "order_id"], kind="mergesort")
    out: list[str] = []
    for _, row in rows.iterrows():
        if str(row.get("side", "")).upper() != "BUY":
            continue
        oid = str(row.get("order_id", "") or "").strip()
        if oid and oid not in out:
            out.append(oid)
    return out


def _sell_rows_for_lineage(fills: pd.DataFrame, code: str, entry_order_id: str) -> list[dict[str, Any]]:
    rows = fills[fills["code"].astype(str).str.zfill(6).eq(code)].copy()
    rows["side_u"] = rows["side"].astype(str).str.upper()
    rows["qty_n"] = pd.to_numeric(rows["qty"], errors="coerce").fillna(0).astype(int)
    rows["lineage_oid"] = rows["note"].map(lambda x: _note_value(x, "entry_order_id") or _note_value(x, "source_order_id"))
    rows = rows[(rows["side_u"].eq("SELL")) & (rows["lineage_oid"].eq(entry_order_id))]
    rows = rows.sort_values(["datetime", "order_id"], kind="mergesort")
    return rows.to_dict("records")


def build_plan() -> dict[str, Any]:
    fills = _read_csv(FILLS)
    if fills.empty:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "fills_missing_or_empty",
            "actions": [],
        }
    if not CLASSIFICATION.exists():
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "classification_missing",
            "actions": [],
        }
    classification = json.loads(CLASSIFICATION.read_text(encoding="utf-8"))
    actions: list[dict[str, Any]] = []
    manual: list[dict[str, Any]] = []
    items = list(classification.get("items") or [])
    capacity_by_code: dict[str, dict[str, int]] = {}
    for item in items:
        code = str(item.get("code") or "").zfill(6)
        bucket = capacity_by_code.setdefault(code, {})
        for pos in list(item.get("positive_counterparts") or []):
            oid = str(pos.get("entry_order_id") or "").strip()
            cap = int(pos.get("net_qty") or 0)
            if oid and cap > 0:
                bucket[oid] = max(int(bucket.get(oid, 0)), cap)

    for item in items:
        code = str(item.get("code") or "").zfill(6)
        target_oid = str(item.get("entry_order_id") or "").strip()
        over_qty = int(item.get("over_qty") or 0)
        cls = str(item.get("classification") or "")
        if cls != "AUTO_SPLIT_CANDIDATE":
            manual.append({
                "code": code,
                "entry_order_id": target_oid,
                "classification": cls,
                "reason": item.get("reason", ""),
            })
            continue
        order = _lineage_order(fills, code)
        positive_oids = [
            oid
            for oid, cap in capacity_by_code.get(code, {}).items()
            if int(cap) > 0 and str(oid) != target_oid
        ]
        positive_oids.sort(key=lambda oid: order.index(str(oid)) if str(oid) in order else 9999)
        sell_rows = _sell_rows_for_lineage(fills, code, target_oid)
        if not sell_rows:
            manual.append({
                "code": code,
                "entry_order_id": target_oid,
                "classification": "PLAN_BLOCKED",
                "reason": "target_sell_row_missing",
            })
            continue

        remaining_over = over_qty
        sell_plan_rows = [
            {
                "row": row,
                "remaining_qty": int(row.get("qty_n") or row.get("qty") or 0),
                "original_qty": int(row.get("qty_n") or row.get("qty") or 0),
            }
            for row in sell_rows
        ]
        sell_idx = len(sell_plan_rows) - 1
        for dst_oid in positive_oids:
            if remaining_over <= 0:
                break
            move_qty = min(remaining_over, int(capacity_by_code.get(code, {}).get(dst_oid, 0)))
            while move_qty > 0 and sell_idx >= 0:
                src_plan = sell_plan_rows[sell_idx]
                src = src_plan["row"]
                src_qty = int(src_plan["remaining_qty"])
                if src_qty <= 0:
                    sell_idx -= 1
                    continue
                split_qty = min(move_qty, src_qty)
                actions.append({
                    "action": "SPLIT_SELL_LINEAGE_DRY_RUN",
                    "code": code,
                    "source_sell_order_id": str(src.get("order_id") or ""),
                    "source_sell_datetime": str(src.get("datetime") or ""),
                    "source_entry_order_id": target_oid,
                    "target_entry_order_id": dst_oid,
                    "split_qty": int(split_qty),
                    "source_sell_qty_before": int(src_plan["original_qty"]),
                    "source_sell_remaining_before_split": int(src_qty),
                    "source_sell_price": str(src.get("price") or ""),
                    "reason": "lineage_over_sell_reallocation_plan",
                })
                capacity_by_code[code][dst_oid] = int(capacity_by_code[code].get(dst_oid, 0)) - int(split_qty)
                src_plan["remaining_qty"] = int(src_qty - split_qty)
                move_qty -= split_qty
                remaining_over -= split_qty
                if int(src_plan["remaining_qty"]) <= 0:
                    sell_idx -= 1
        if remaining_over > 0:
            manual.append({
                "code": code,
                "entry_order_id": target_oid,
                "classification": "PLAN_PARTIAL",
                "reason": f"unallocated_over_qty={remaining_over}",
            })

    status = "PASS" if actions and not any(x.get("classification") in {"PLAN_BLOCKED", "PLAN_PARTIAL"} for x in manual) else "PARTIAL"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "source_fills": str(FILLS),
        "source_classification": str(CLASSIFICATION),
        "action_count": len(actions),
        "manual_review_count": len(manual),
        "actions": actions,
        "manual_review": manual,
        "apply_performed": False,
    }


def main() -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    payload = build_plan()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = LOGS / f"paper_fill_lineage_backfill_plan_{stamp}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    OUT_LATEST.write_text(text, encoding="utf-8")
    print(f"[PAPER_FILL_LINEAGE_BACKFILL_PLAN] status={payload.get('status')} actions={payload.get('action_count')} manual={payload.get('manual_review_count')} -> {OUT_LATEST}")
    return 0 if payload.get("status") in {"PASS", "PARTIAL"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
