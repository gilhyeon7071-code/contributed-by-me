from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
FILLS_NORM = ROOT / "paper" / "fills_norm.csv"
OUT_JSON = LOG_DIR / "mdd_ddm_stop_stacking_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "mdd_ddm_stop_stacking_diagnostic_latest.csv"


def _note_value(note: Any, key: str) -> str:
    m = re.search(r"(?:^|[;|\s])" + re.escape(key) + r"=([^;|\s]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        return default if out != out else out
    except Exception:
        return default


def _load_fills() -> pd.DataFrame:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    fills = fills.copy()
    fills["code_norm"] = fills["code"].astype(str).str.zfill(6)
    fills["qty_n"] = pd.to_numeric(fills["qty"], errors="coerce").fillna(0.0)
    fills["price_n"] = pd.to_numeric(fills["price"], errors="coerce").fillna(0.0)
    fills["note_s"] = fills["note"].astype(str)
    fills["entry_oid"] = fills["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    fills["exit_reason_note"] = fills["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    fills["sell_ratio_pct_note"] = fills["note_s"].map(lambda s: _to_float(_note_value(s, "sell_ratio_pct"), 0.0))
    fills["partial_exit_note"] = fills["note_s"].map(lambda s: _note_value(s, "partial_exit"))
    fills["prior_stop_count_note"] = fills["note_s"].map(lambda s: _to_float(_note_value(s, "prior_stop_count"), 0.0))
    return fills


def _group_label(buy_qty: float, sell_qty: float, reasons: list[str], ordered_reasons: list[str]) -> str:
    if sell_qty > buy_qty + 0.0001:
        return "OVER_SELL_BUG_CANDIDATE"
    if any(r.startswith("DDM_LIQUIDATE") for r in reasons) and "STOP" in reasons:
        if sell_qty >= buy_qty - 0.0001:
            return "DDM_PARTIAL_THEN_STOP_FULL_CLOSE_STACKING"
        return "DDM_STOP_PARTIAL_STACKING"
    if any(r.startswith("DDM_LIQUIDATE") for r in reasons):
        return "DDM_ONLY"
    return "OTHER"


def main() -> None:
    fills = _load_fills()
    buys = fills[fills["side"].str.upper().eq("BUY")].copy()
    sells = fills[fills["side"].str.upper().eq("SELL")].copy()
    buy_qty_by_oid = buys.groupby("order_id")["qty_n"].sum().to_dict()

    rows: list[dict[str, Any]] = []
    for (date, code, entry_oid), grp in sells.groupby(["date", "code_norm", "entry_oid"], dropna=False):
        reasons = sorted({str(v) for v in grp["exit_reason_note"].tolist() if str(v)})
        if not any(r.startswith("DDM_LIQUIDATE") for r in reasons) or "STOP" not in reasons:
            continue
        ordered = grp.sort_values(["ts", "order_id"]).copy()
        ordered_reasons = ordered["exit_reason_note"].astype(str).tolist()
        buy_qty = float(buy_qty_by_oid.get(str(entry_oid), 0.0))
        sell_qty = float(ordered["qty_n"].sum())
        ddm_qty = float(ordered.loc[ordered["exit_reason_note"].str.startswith("DDM_LIQUIDATE"), "qty_n"].sum())
        stop_qty = float(ordered.loc[ordered["exit_reason_note"].eq("STOP"), "qty_n"].sum())
        rows.append(
            {
                "date": str(date),
                "code": str(code).zfill(6),
                "entry_order_id": str(entry_oid),
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "remaining_after_sells": buy_qty - sell_qty,
                "ddm_qty": ddm_qty,
                "stop_qty": stop_qty,
                "reasons": ",".join(reasons),
                "ordered_reasons": ",".join(ordered_reasons),
                "sell_rows": int(len(ordered)),
                "first_sell_ts": str(ordered["ts"].iloc[0]) if not ordered.empty else "",
                "last_sell_ts": str(ordered["ts"].iloc[-1]) if not ordered.empty else "",
                "sell_order_ids": ",".join(ordered["order_id"].astype(str).tolist()),
                "sell_ratio_pcts": ",".join(str(v) for v in ordered["sell_ratio_pct_note"].tolist()),
                "partial_exit_flags": ",".join(str(v) for v in ordered["partial_exit_note"].tolist()),
                "prior_stop_counts": ",".join(str(int(v)) for v in ordered["prior_stop_count_note"].tolist()),
                "stacking_label": _group_label(buy_qty, sell_qty, reasons, ordered_reasons),
            }
        )

    out = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    label_counts = out["stacking_label"].value_counts().to_dict() if not out.empty else {}
    oversell_count = int((out["sell_qty"] > out["buy_qty"] + 0.0001).sum()) if not out.empty else 0
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if len(rows) else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {"fills_norm": str(FILLS_NORM)},
        "group_count": len(rows),
        "oversell_count": oversell_count,
        "label_counts": label_counts,
        "groups": rows,
        "interpretation": [
            "Read-only diagnostic for same-day DDM plus STOP stacking by entry_order_id.",
            "oversell_count=0 means the observed stacking is not a quantity over-sell bug.",
            "Stacking may still be a policy sequencing issue when DDM partial liquidation and STOP consume the same lineage on the same day.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "group_count": len(rows), "oversell_count": oversell_count, "label_counts": label_counts, "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
