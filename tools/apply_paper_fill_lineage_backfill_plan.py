from __future__ import annotations

import argparse
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
PLAN = LOGS / "paper_fill_lineage_backfill_plan_latest.json"
REPORT_LATEST = LOGS / "paper_fill_lineage_backfill_apply_latest.json"


def _note_set(note: Any, updates: dict[str, str]) -> str:
    parts = str(note or "").split(";")
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if "=" not in part:
            out.append(part)
            continue
        key, _value = part.split("=", 1)
        key = key.strip()
        if key in updates:
            out.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out.append(part)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")
    return ";".join(out)


def _safe_suffix(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "")).strip("_")
    return text[-24:] if text else "UNKNOWN"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def build_apply_preview(apply: bool) -> dict[str, Any]:
    fills = _read_csv(FILLS)
    if fills.empty:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "fills_missing_or_empty",
            "apply_performed": False,
        }
    if not PLAN.exists():
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "plan_missing",
            "apply_performed": False,
        }
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    if plan.get("status") != "PASS" or plan.get("apply_performed"):
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": f"plan_not_applyable status={plan.get('status')} apply_performed={plan.get('apply_performed')}",
            "apply_performed": False,
        }

    actions = list(plan.get("actions") or [])
    if not actions:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "plan_actions_empty",
            "apply_performed": False,
        }

    work = fills.copy()
    work["_row_idx"] = range(len(work))
    work["_qty_n"] = pd.to_numeric(work["qty"], errors="coerce").fillna(0).astype(int)
    new_rows: list[dict[str, Any]] = []
    reductions: dict[int, int] = {}
    issues: list[str] = []
    action_results: list[dict[str, Any]] = []
    drop_source_rows: set[int] = set()

    for action in actions:
        order_id = str(action.get("source_sell_order_id") or "").strip()
        source_oid = str(action.get("source_entry_order_id") or "").strip()
        target_oid = str(action.get("target_entry_order_id") or "").strip()
        split_qty = int(action.get("split_qty") or 0)
        if not order_id or not source_oid or not target_oid or split_qty <= 0:
            issues.append(f"invalid_action:{order_id}:{split_qty}")
            continue

        candidates = work[work["order_id"].astype(str).eq(order_id)]
        if candidates.empty:
            issues.append(f"source_row_missing:{order_id}")
            continue
        if len(candidates) != 1:
            issues.append(f"source_row_not_unique:{order_id}:{len(candidates)}")
            continue
        src_idx = int(candidates.iloc[0]["_row_idx"])
        current_reduction = int(reductions.get(src_idx, 0))
        src_qty = int(candidates.iloc[0]["_qty_n"])
        if src_qty - current_reduction < split_qty:
            issues.append(f"source_qty_insufficient:{order_id}:remaining={src_qty-current_reduction}:split={split_qty}")
            continue

        src = candidates.iloc[0].to_dict()
        split_order_id = f"{order_id}_LINEAGE_SPLIT_{_safe_suffix(target_oid)}_Q{split_qty}"
        if split_order_id in set(work["order_id"].astype(str)) or any(r.get("order_id") == split_order_id for r in new_rows):
            issues.append(f"split_order_id_duplicate:{split_order_id}")
            continue

        reductions[src_idx] = current_reduction + split_qty
        split_row = {k: src.get(k, "") for k in fills.columns}
        split_row["qty"] = str(split_qty)
        split_row["order_id"] = split_order_id
        split_row["note"] = _note_set(
            src.get("note", ""),
            {
                "sell_qty": str(split_qty),
                "entry_order_id": target_oid,
                "source_order_id": target_oid,
                "lineage_backfill_from_order_id": order_id,
                "lineage_backfill_original_entry_order_id": source_oid,
            },
        )
        new_rows.append(split_row)
        action_results.append({
            "source_sell_order_id": order_id,
            "split_order_id": split_order_id,
            "source_entry_order_id": source_oid,
            "target_entry_order_id": target_oid,
            "split_qty": split_qty,
        })

    for src_idx, reduce_qty in reductions.items():
        mask = work["_row_idx"].eq(src_idx)
        before_qty = int(work.loc[mask, "_qty_n"].iloc[0])
        after_qty = before_qty - int(reduce_qty)
        if after_qty < 0:
            issues.append(f"source_qty_negative_after_split:{work.loc[mask, 'order_id'].iloc[0]}:{after_qty}")
            continue
        if after_qty == 0:
            drop_source_rows.add(src_idx)
            continue
        work.loc[mask, "qty"] = str(after_qty)
        work.loc[mask, "note"] = work.loc[mask, "note"].map(lambda x, q=after_qty: _note_set(x, {"sell_qty": str(q)}))

    status = "PASS" if not issues else "FAIL"
    if status == "PASS" and apply:
        out = work[~work["_row_idx"].isin(drop_source_rows)].drop(columns=["_row_idx", "_qty_n"], errors="ignore")
        if new_rows:
            out = pd.concat([out, pd.DataFrame(new_rows, columns=fills.columns)], ignore_index=True)
        out.to_csv(FILLS, index=False, encoding="utf-8-sig")

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "apply_requested": bool(apply),
        "apply_performed": bool(apply and status == "PASS"),
        "source_fills": str(FILLS),
        "source_plan": str(PLAN),
        "source_rows": int(len(fills)),
        "new_rows": int(len(new_rows)),
        "reduced_rows": int(len(reductions)),
        "dropped_source_rows": int(len(drop_source_rows)),
        "actions": action_results,
        "issues": issues,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write updated paper/fills.csv")
    args = parser.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    payload = build_apply_preview(apply=bool(args.apply))
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = LOGS / f"paper_fill_lineage_backfill_apply_{stamp}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    REPORT_LATEST.write_text(text, encoding="utf-8")
    print(
        f"[PAPER_FILL_LINEAGE_BACKFILL_APPLY] status={payload.get('status')} "
        f"apply_performed={payload.get('apply_performed')} new_rows={payload.get('new_rows')} "
        f"reduced_rows={payload.get('reduced_rows')} -> {REPORT_LATEST}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
