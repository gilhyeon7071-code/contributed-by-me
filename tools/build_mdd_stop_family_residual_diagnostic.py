from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TARGET_REASONS = {"STOP", "STOP_PREEMPTIVE_CLOSE"}


def _note_value(note: Any, key: str) -> str:
    text = "" if note is None else str(note)
    m = re.search(rf"(?:^|;){re.escape(key)}=([^;]*)", text)
    return m.group(1).strip() if m else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        text = str(value).strip()
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _entry_oid_from_trade_note(note: str) -> str:
    return _note_value(note, "entry_order_id") or _note_value(note, "order_id")


def _extract_sell_meta(fills: pd.DataFrame) -> pd.DataFrame:
    sells = fills[fills["side"].astype(str).str.upper().eq("SELL")].copy()
    sells["exit_reason"] = sells["note"].map(lambda s: _note_value(s, "exit_reason"))
    sells["entry_order_id"] = sells["note"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    sells["prior_stop_count"] = sells["note"].map(lambda s: _to_int(_note_value(s, "prior_stop_count")))
    sells["sell_ratio_pct"] = sells["note"].map(lambda s: _to_float(_note_value(s, "sell_ratio_pct")))
    sells["partial_exit"] = sells["note"].map(lambda s: _to_int(_note_value(s, "partial_exit")))
    sells["gap_loss_pct"] = sells["note"].map(lambda s: _to_float(_note_value(s, "gap_loss_pct")))
    sells["exit_severity"] = sells["note"].map(lambda s: _note_value(s, "exit_severity"))
    sells["severity_reasons"] = sells["note"].map(lambda s: _note_value(s, "severity_reasons"))
    sells["hold_days_trading"] = sells["note"].map(lambda s: _to_int(_note_value(s, "hold_days_trading")))
    sells["hold_days_calendar"] = sells["note"].map(lambda s: _to_int(_note_value(s, "hold_days_calendar")))
    sells["surge_type"] = sells["note"].map(lambda s: _note_value(s, "surge_type"))
    sells["surge_score_final"] = sells["note"].map(lambda s: _to_float(_note_value(s, "surge_score_final"), default=float("nan")))
    return sells


def _attach_sell_meta(trades: pd.DataFrame, sells: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    out["entry_order_id"] = out["note"].map(_entry_oid_from_trade_note)
    out["qty_i"] = out["qty"].map(_to_int)
    out["net_ret_f"] = out["net_ret"].map(_to_float)
    out["entry_date"] = out["entry_ts"].astype(str).str.slice(0, 10).str.replace("-", "", regex=False)
    out["exit_date"] = out["exit_ts"].astype(str).str.slice(0, 10).str.replace("-", "", regex=False)
    out["exit_time"] = out["exit_ts"].astype(str).str.slice(11, 19)

    # Match trade rows to SELL rows by code, entry lineage, qty, exit date, and price.
    meta_rows: list[dict[str, Any]] = []
    for _, tr in out.iterrows():
        code = str(tr.get("code", "")).strip()
        oid = str(tr.get("entry_order_id", "")).strip()
        qty = _to_int(tr.get("qty"))
        price = _to_float(tr.get("exit_price"))
        exit_date = str(tr.get("exit_date", ""))
        cand = sells[
            sells["code"].astype(str).str.strip().eq(code)
            & sells["entry_order_id"].astype(str).str.strip().eq(oid)
            & sells["ts"].astype(str).str.slice(0, 10).str.replace("-", "", regex=False).eq(exit_date)
        ].copy()
        if not cand.empty:
            cand["_price_diff"] = cand["price"].map(lambda x: abs(_to_float(x) - price))
            cand["_qty_diff"] = cand["qty"].map(lambda x: abs(_to_int(x) - qty))
            cand = cand.sort_values(["_qty_diff", "_price_diff", "order_id"], kind="mergesort")
            row = cand.iloc[0].to_dict()
        else:
            row = {}
        meta_rows.append(
            {
                "sell_order_id": str(row.get("order_id", "")),
                "exit_reason": str(row.get("exit_reason", "")),
                "prior_stop_count": _to_int(row.get("prior_stop_count")),
                "sell_ratio_pct": _to_float(row.get("sell_ratio_pct")),
                "partial_exit": _to_int(row.get("partial_exit")),
                "gap_loss_pct": _to_float(row.get("gap_loss_pct")),
                "exit_severity": str(row.get("exit_severity", "")),
                "severity_reasons": str(row.get("severity_reasons", "")),
                "hold_days_trading": _to_int(row.get("hold_days_trading")),
                "hold_days_calendar": _to_int(row.get("hold_days_calendar")),
                "surge_type_sell": str(row.get("surge_type", "")),
                "sell_ts": str(row.get("ts", "")),
            }
        )
    return pd.concat([out.reset_index(drop=True), pd.DataFrame(meta_rows)], axis=1)


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    trades = _load_csv(ROOT / "paper" / "trades_calc.csv")
    fills = _load_csv(ROOT / "paper" / "fills_norm.csv")
    residual_path = LOG_DIR / "mdd_post_stopgap_shadow_residual_latest.json"
    if not residual_path.exists():
        raise FileNotFoundError(residual_path)
    residual = json.loads(residual_path.read_text(encoding="utf-8"))
    top_loss = pd.DataFrame(residual.get("top_loss_trades", []))
    if top_loss.empty:
        raise RuntimeError("No top_loss_trades in residual artifact")

    target_trade_ids = set(str(x) for x in top_loss.get("trade_id", []).tolist())
    work = trades[trades["trade_id"].astype(str).isin(target_trade_ids)].copy()
    sells = _extract_sell_meta(fills)
    enriched = _attach_sell_meta(work, sells)
    stop_family = enriched[enriched["exit_reason"].isin(TARGET_REASONS)].copy()

    all_sells = sells[sells["exit_reason"].isin(TARGET_REASONS)].copy()
    grouped = []
    for (code, oid), g in all_sells.groupby(["code", "entry_order_id"], sort=True):
        grouped.append(
            {
                "code": str(code),
                "entry_order_id": str(oid),
                "sell_count": int(len(g)),
                "sell_qty_total": int(g["qty"].map(_to_int).sum()),
                "exit_reasons": ",".join(sorted(set(str(x) for x in g["exit_reason"].tolist() if str(x)))),
                "max_prior_stop_count": int(g["prior_stop_count"].max()) if not g.empty else 0,
                "partial_exit_count": int((g["partial_exit"] == 1).sum()),
                "same_day_count": int((g["hold_days_calendar"] == 0).sum()),
                "zero_time_count": int(g["ts"].astype(str).str.endswith("00:00:00").sum()),
            }
        )
    repeated_groups = [x for x in grouped if x["sell_count"] >= 2 or x["max_prior_stop_count"] >= 1]

    reason_breakdown = []
    for reason, g in stop_family.groupby("exit_reason", sort=True):
        reason_breakdown.append(
            {
                "exit_reason": str(reason),
                "trade_count": int(len(g)),
                "sum_net_ret": float(g["net_ret_f"].sum()),
                "avg_net_ret": float(g["net_ret_f"].mean()) if len(g) else 0.0,
                "unique_codes": int(g["code"].nunique()),
                "zero_time_count": int(g["exit_time"].eq("00:00:00").sum()),
                "same_day_count": int((g["entry_date"] == g["exit_date"]).sum()),
                "repeat_prior_stop_count": int((g["prior_stop_count"] >= 1).sum()),
                "blank_severity_count": int(g["exit_severity"].astype(str).str.strip().eq("").sum()),
                "zero_gap_loss_count": int((g["gap_loss_pct"] == 0.0).sum()),
            }
        )

    quality_flags = Counter()
    rows = []
    for _, row in stop_family.iterrows():
        flags = []
        if str(row.get("exit_time", "")) == "00:00:00":
            flags.append("NO_INTRADAY_EXIT_TIME")
        if _to_int(row.get("prior_stop_count")) >= 1:
            flags.append("REPEAT_STOP_ON_LINEAGE")
        if _to_int(row.get("partial_exit")) == 1:
            flags.append("PARTIAL_STOP")
        if not str(row.get("exit_severity", "")).strip():
            flags.append("BLANK_EXIT_SEVERITY")
        if _to_float(row.get("gap_loss_pct")) == 0.0:
            flags.append("ZERO_GAP_LOSS_META")
        for flag in flags:
            quality_flags[flag] += 1
        rows.append(
            {
                "trade_id": str(row.get("trade_id", "")),
                "code": str(row.get("code", "")),
                "entry_order_id": str(row.get("entry_order_id", "")),
                "exit_reason": str(row.get("exit_reason", "")),
                "entry_ts": str(row.get("entry_ts", "")),
                "exit_ts": str(row.get("exit_ts", "")),
                "sell_ts": str(row.get("sell_ts", "")),
                "qty": _to_int(row.get("qty")),
                "entry_price": _to_float(row.get("entry_price")),
                "exit_price": _to_float(row.get("exit_price")),
                "net_ret": _to_float(row.get("net_ret")),
                "prior_stop_count": _to_int(row.get("prior_stop_count")),
                "sell_ratio_pct": _to_float(row.get("sell_ratio_pct")),
                "partial_exit": _to_int(row.get("partial_exit")),
                "hold_days_trading": _to_int(row.get("hold_days_trading")),
                "hold_days_calendar": _to_int(row.get("hold_days_calendar")),
                "quality_flags": ",".join(flags),
                "sell_order_id": str(row.get("sell_order_id", "")),
            }
        )

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL",
        "policy_change": False,
        "scope": "STOP and STOP_PREEMPTIVE_CLOSE rows among residual top_loss_trades after STOP_GAP legacy shadow.",
        "source": {
            "residual": str(residual_path),
            "trades": str(ROOT / "paper" / "trades_calc.csv"),
            "fills": str(ROOT / "paper" / "fills_norm.csv"),
        },
        "summary": {
            "stop_family_top_loss_trade_count": int(len(stop_family)),
            "stop_family_top_loss_sum_net_ret": float(stop_family["net_ret_f"].sum()) if not stop_family.empty else 0.0,
            "reason_breakdown": reason_breakdown,
            "quality_flags": dict(quality_flags),
            "repeated_stop_lineage_group_count_all_fills": int(len(repeated_groups)),
        },
        "top_loss_stop_family_rows": rows,
        "repeated_stop_lineage_groups_all_fills": repeated_groups,
        "interpretation": [
            "STOP family residual top losses are quantity-consistent but include repeated partial exits on the same lineage.",
            "Several rows have 00:00:00 timestamps, so current artifacts cannot prove whether the stop was timely intraday protection or late end-of-day liquidation.",
            "This diagnostic does not justify threshold relaxation; it identifies missing timing evidence and repeated-stop sequencing as the next policy-quality review axis.",
        ],
        "trading_effect": {
            "policy_value_validity": "NOT_PROVEN",
            "root_cause_class": "STOP_FAMILY_RESIDUAL_LOSS_WITH_TIMING_EVIDENCE_GAP",
        },
    }

    json_path = LOG_DIR / "mdd_stop_family_residual_diagnostic_latest.json"
    csv_path = LOG_DIR / "mdd_stop_family_residual_diagnostic_latest.csv"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "summary": result["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
