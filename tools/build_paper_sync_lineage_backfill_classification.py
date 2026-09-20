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
TRADES_CALC = PAPER / "trades_calc.csv"
AUDIT = LOGS / "paper_sync_lineage_audit_latest.json"
OUT_LATEST = LOGS / "paper_sync_lineage_backfill_classification_latest.json"


def _note_value(note: Any, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return str(m.group(1)).strip() if m else ""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def build_classification() -> dict[str, Any]:
    fills = _read_csv(FILLS)
    calc = _read_csv(TRADES_CALC)
    if fills.empty:
        return {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "FAIL", "reason": "fills_missing", "items": []}
    if not AUDIT.exists():
        return {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "FAIL", "reason": "audit_missing", "items": []}
    audit = json.loads(AUDIT.read_text(encoding="utf-8"))
    items: list[dict[str, Any]] = []

    fills_work = fills.copy()
    fills_work["code"] = fills_work["code"].astype(str).str.zfill(6)
    fills_work["side_u"] = fills_work["side"].astype(str).str.upper()
    fills_work["qty_n"] = pd.to_numeric(fills_work["qty"], errors="coerce").fillna(0).astype(int)
    fills_work["lineage_oid"] = fills_work.apply(
        lambda r: str(r.get("order_id", "")).strip()
        if str(r.get("side_u", "")) == "BUY"
        else (_note_value(r.get("note", ""), "entry_order_id") or _note_value(r.get("note", ""), "source_order_id")),
        axis=1,
    )

    calc_work = calc.copy()
    if not calc_work.empty:
        calc_work["code"] = calc_work["code"].astype(str).str.zfill(6)
        calc_work["qty_n"] = pd.to_numeric(calc_work["qty"], errors="coerce").fillna(0).astype(int)
        calc_work["lineage_oid"] = calc_work["note"].map(lambda x: _note_value(x, "entry_order_id") or _note_value(x, "source_order_id"))

    for issue in audit.get("issues", []):
        code = str(issue.get("code") or "").zfill(6)
        oid = str(issue.get("entry_order_id") or "").strip()
        over_qty = max(0, int(issue.get("sell_qty") or 0) - int(issue.get("buy_qty") or 0))
        code_rows = fills_work[fills_work["code"].eq(code)].copy()
        lineages: list[dict[str, Any]] = []
        for loid, grp in code_rows[code_rows["lineage_oid"].astype(str).str.strip().ne("")].groupby("lineage_oid", sort=True):
            buy_qty = int(grp[grp["side_u"].eq("BUY")]["qty_n"].sum())
            sell_qty = int(grp[grp["side_u"].eq("SELL")]["qty_n"].sum())
            lineages.append({"entry_order_id": str(loid), "buy_qty": buy_qty, "sell_qty": sell_qty, "net_qty": buy_qty - sell_qty})
        positives = [x for x in lineages if int(x.get("net_qty") or 0) > 0 and str(x.get("entry_order_id")) != oid]
        calc_target_qty = 0
        if not calc_work.empty:
            calc_target_qty = int(calc_work[(calc_work["code"].eq(code)) & (calc_work["lineage_oid"].eq(oid))]["qty_n"].sum())
        capacity = int(sum(int(x.get("net_qty") or 0) for x in positives))
        if over_qty > 0 and positives and capacity >= over_qty and calc_target_qty == int(issue.get("buy_qty") or 0):
            cls = "AUTO_SPLIT_CANDIDATE"
            reason = "current trades_calc caps target lineage to buy_qty and positive lineage capacity can absorb over-sell"
        elif over_qty > 0 and positives and capacity >= over_qty:
            cls = "MANUAL_SPLIT_REVIEW"
            reason = "positive lineage capacity exists, but regenerated trades_calc does not exactly cap target lineage to buy_qty"
        else:
            cls = "AUDIT_ONLY_UNRESOLVED"
            reason = "no sufficient positive lineage capacity found"
        items.append({
            "code": code,
            "entry_order_id": oid,
            "over_qty": over_qty,
            "fill_lineage_buy": int(issue.get("buy_qty") or 0),
            "fill_lineage_sell": int(issue.get("sell_qty") or 0),
            "fill_lineage_net": int(issue.get("net_qty") or 0),
            "classification": cls,
            "reason": reason,
            "positive_counterpart_capacity": capacity,
            "positive_counterparts": positives,
            "trades_calc_target_lineage_qty": calc_target_qty,
        })

    class_counts: dict[str, int] = {}
    for item in items:
        cls = str(item.get("classification") or "")
        class_counts[cls] = class_counts.get(cls, 0) + 1
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "source_audit": str(AUDIT),
        "issue_count": len(items),
        "class_counts": class_counts,
        "items": items,
    }


def main() -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    payload = build_classification()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = LOGS / f"paper_sync_lineage_backfill_classification_{stamp}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    OUT_LATEST.write_text(text, encoding="utf-8")
    print(f"[PAPER_SYNC_LINEAGE_CLASSIFICATION] status={payload.get('status')} class_counts={payload.get('class_counts')} -> {OUT_LATEST}")
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
