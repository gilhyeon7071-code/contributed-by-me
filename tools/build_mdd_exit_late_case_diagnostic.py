from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
QUALITY_CSV = LOG_DIR / "mdd_exit_quality_diagnostic_latest.csv"
FILLS_NORM = ROOT / "paper" / "fills_norm.csv"
PRICES = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
OUT_JSON = LOG_DIR / "mdd_exit_late_case_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "mdd_exit_late_case_diagnostic_latest.csv"


def _norm_ymd(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s.zfill(6) if s else ""


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        if out != out:
            return default
        return out
    except Exception:
        return default


def _note_value(note: Any, key: str) -> str:
    m = re.search(r"(?:^|[;|\s])" + re.escape(key) + r"=([^;|\s]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _load_prices() -> dict[str, pd.DataFrame]:
    px = pd.read_parquet(PRICES)
    px = px.copy()
    px["code_norm"] = px["code"].map(_norm_code)
    px["date_norm"] = px["date"].map(_norm_ymd)
    for col in ["open", "high", "low", "close"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")
    px = px.sort_values(["code_norm", "date_norm"])
    return {code: g.copy() for code, g in px.groupby("code_norm", sort=False)}


def _load_fills() -> pd.DataFrame:
    fills = pd.read_csv(FILLS_NORM, dtype=str, encoding="utf-8-sig").fillna("")
    fills = fills.copy()
    fills["code_norm"] = fills["code"].map(_norm_code)
    fills["date_norm"] = fills["date"].map(_norm_ymd)
    fills["qty_n"] = pd.to_numeric(fills["qty"], errors="coerce").fillna(0.0)
    fills["price_n"] = pd.to_numeric(fills["price"], errors="coerce").fillna(0.0)
    fills["note_s"] = fills["note"].astype(str)
    fills["entry_order_id_note"] = fills["note_s"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    fills["exit_reason_note"] = fills["note_s"].map(lambda s: _note_value(s, "exit_reason"))
    return fills


def _price_context(code: str, entry_date: str, exit_date: str, px_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    px = px_by_code.get(code)
    if px is None or px.empty:
        return {"price_context_status": "NO_PRICE_HISTORY"}
    around = px.loc[px["date_norm"].between(entry_date, exit_date)].copy()
    after = px.loc[px["date_norm"].gt(exit_date)].head(5).copy()
    if around.empty:
        return {"price_context_status": "NO_ENTRY_EXIT_BARS"}
    entry_bar = around.loc[around["date_norm"].eq(entry_date)].head(1)
    exit_bar = around.loc[around["date_norm"].eq(exit_date)].head(1)
    entry_bar = entry_bar.iloc[0] if not entry_bar.empty else {}
    exit_bar = exit_bar.iloc[0] if not exit_bar.empty else {}
    return {
        "price_context_status": "PASS",
        "entry_open": _to_float(entry_bar.get("open") if isinstance(entry_bar, pd.Series) else None),
        "entry_high": _to_float(entry_bar.get("high") if isinstance(entry_bar, pd.Series) else None),
        "entry_low": _to_float(entry_bar.get("low") if isinstance(entry_bar, pd.Series) else None),
        "entry_close": _to_float(entry_bar.get("close") if isinstance(entry_bar, pd.Series) else None),
        "exit_open": _to_float(exit_bar.get("open") if isinstance(exit_bar, pd.Series) else None),
        "exit_high": _to_float(exit_bar.get("high") if isinstance(exit_bar, pd.Series) else None),
        "exit_low": _to_float(exit_bar.get("low") if isinstance(exit_bar, pd.Series) else None),
        "exit_close": _to_float(exit_bar.get("close") if isinstance(exit_bar, pd.Series) else None),
        "post_dates": ",".join(after["date_norm"].astype(str).tolist()) if not after.empty else "",
    }


def _same_day_fills(code: str, exit_date: str, fills: pd.DataFrame) -> dict[str, Any]:
    same = fills.loc[fills["code_norm"].eq(code) & fills["date_norm"].eq(exit_date)].copy()
    if same.empty:
        return {"same_day_fill_status": "NO_FILLS"}
    sells = same.loc[same["side"].astype(str).str.upper().eq("SELL")].copy()
    buys = same.loc[same["side"].astype(str).str.upper().eq("BUY")].copy()
    exit_counts = sells["exit_reason_note"].replace("", "UNKNOWN").value_counts().to_dict()
    return {
        "same_day_fill_status": "PASS",
        "same_day_buy_rows": int(len(buys)),
        "same_day_sell_rows": int(len(sells)),
        "same_day_sell_qty": float(sells["qty_n"].sum()) if not sells.empty else 0.0,
        "same_day_exit_reasons": exit_counts,
        "same_day_sell_order_ids": ",".join(sells["order_id"].astype(str).tolist()[:8]) if not sells.empty else "",
    }


def _classify_case(row: dict[str, Any]) -> str:
    is_surge = str(row.get("is_surge_like") or "").lower() == "true"
    r1 = _to_float(row.get("ret_close_1d"), 0.0) or 0.0
    r3 = _to_float(row.get("ret_close_3d"), 0.0) or 0.0
    min_low = _to_float(row.get("min_low_5d_ret"), 0.0) or 0.0
    reason = str(row.get("exit_reason") or "")
    if is_surge and r1 >= 0.08 and min_low >= 0.0:
        return "SURGE_WHIPSAW_EXIT_THEN_RECOVERY"
    if reason == "STOP_GAP" and r1 >= 0.05:
        return "GAP_STOP_WHIPSAW"
    if "DDM_LIQUIDATE" in reason and r3 >= 0.10:
        return "DDM_LATE_OR_OVER_EXIT_CANDIDATE"
    if not is_surge and r3 >= 0.10:
        return "NON_SURGE_DDM_REBOUND_CANDIDATE"
    return "CASE_REVIEW_REQUIRED"


def build() -> dict[str, Any]:
    quality = pd.read_csv(QUALITY_CSV, dtype=str, encoding="utf-8-sig").fillna("")
    cases = quality.loc[
        quality["axis"].astype(str).eq("detail")
        & quality["quality_label"].astype(str).eq("POTENTIALLY_LATE_OR_OVER_EXIT")
    ].copy()
    px_by_code = _load_prices()
    fills = _load_fills()

    rows: list[dict[str, Any]] = []
    for _, src in cases.iterrows():
        code = _norm_code(src.get("code"))
        entry_date = _norm_ymd(src.get("entry_date"))
        exit_date = _norm_ymd(src.get("exit_date"))
        item = {k: src.get(k) for k in src.index}
        item["code"] = code
        item["entry_date"] = entry_date
        item["exit_date"] = exit_date
        item.update(_price_context(code, entry_date, exit_date, px_by_code))
        item.update(_same_day_fills(code, exit_date, fills))
        item["case_label"] = _classify_case(item)
        rows.append(item)

    out = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    case_counts = out["case_label"].value_counts().to_dict() if not out.empty else {}
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "quality_csv": str(QUALITY_CSV),
            "fills_norm": str(FILLS_NORM),
            "price_parquet": str(PRICES),
        },
        "case_count": int(len(rows)),
        "case_counts": case_counts,
        "cases": rows,
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "interpretation": [
            "Read-only case diagnostic for potentially late or over-exits.",
            "A case label is a review bucket, not a policy change recommendation.",
            "Threshold changes still require a separate counterfactual simulation.",
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    payload = build()
    print(json.dumps({
        "status": payload.get("status"),
        "case_count": payload.get("case_count"),
        "case_counts": payload.get("case_counts"),
        "out_json": str(OUT_JSON),
        "out_csv": str(OUT_CSV),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
