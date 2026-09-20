from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

VARIANT_TRADES = LOG_DIR / "c3_mkt_vol_variant_compare_trades_latest.csv"
VARIANT_SUMMARY = LOG_DIR / "c3_mkt_vol_variant_compare_summary_latest.csv"
VARIANT_JSON = LOG_DIR / "c3_mkt_vol_variant_compare_latest.json"

LATEST_JSON = LOG_DIR / "c3_revived_2026h1_trade_analysis_latest.json"
LATEST_ROWS_CSV = LOG_DIR / "c3_revived_2026h1_trade_analysis_rows_latest.csv"
LATEST_GROUP_CSV = LOG_DIR / "c3_revived_2026h1_trade_analysis_groups_latest.csv"
LATEST_MD = LOG_DIR / "c3_revived_2026h1_trade_analysis_latest.md"

STATUS = "READ_ONLY_C3_REVIVED_2026H1_TRADE_ANALYSIS_NOT_OPERATIONAL"


TRADE_KEY = ["signal_date", "entry_date", "exit_date", "code"]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _profit_factor(ret: Iterable[float]) -> float | str:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return round(gains / losses, 6)
    if gains > 0:
        return "inf"
    return "NA"


def _summarize(group: str, df: pd.DataFrame, note: str = "") -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    if df.empty:
        return {
            "group": group,
            "n": 0,
            "win_n": 0,
            "loss_n": 0,
            "stop_n": 0,
            "win_rate": "NA",
            "ret_sum": 0.0,
            "ret_mean": "NA",
            "profit_factor": "NA",
            "unique_codes": 0,
            "first_entry_date": "",
            "last_entry_date": "",
            "note": note,
        }
    return {
        "group": group,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "stop_n": int(df.get("exit_reason", pd.Series(dtype=str)).astype(str).str.upper().eq("STOP").sum()),
        "win_rate": round(float((ret > 0).mean()), 6),
        "ret_sum": round(float(ret.sum()), 6),
        "ret_mean": round(float(ret.mean()), 6),
        "profit_factor": _profit_factor(ret.tolist()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns else 0,
        "first_entry_date": str(df["entry_date"].min()),
        "last_entry_date": str(df["entry_date"].max()),
        "note": note,
    }


def _classify_variants(variants: set[str]) -> str:
    if "relax_mkt_vol20_q25_2026_pool" in variants:
        return "q25_subset"
    if "relax_mkt_vol20_median_2026_pool" in variants:
        return "median_only_extra"
    if "relax_mkt_vol20_min_2026_pool" in variants:
        return "min_only_extra"
    if "remove_mkt_vol20_only" in variants:
        return "remove_only_extra"
    return "other"


def _row_quality(row: pd.Series) -> str:
    ret = float(row.get("ret", 0.0) or 0.0)
    exit_reason = str(row.get("exit_reason", "") or "").upper()
    if ret > 0.10:
        return "large_single_win"
    if ret > 0:
        return "small_win"
    if exit_reason == "STOP":
        return "stop_loss"
    return "non_stop_loss"


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 Revived 2026H1 Trade Analysis",
        "",
        f"- status: `{payload['status']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Group Summary",
        "",
        "| group | n | win_rate | ret_sum | profit_factor | stop_n | note |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["group_summary"]:
        lines.append(
            "| {group} | {n} | {win_rate} | {ret_sum} | {profit_factor} | {stop_n} | {note} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Revived Unique Trades",
            "",
            "| group | code | signal | entry | ret | exit | quality | variants |",
            "|---|---|---|---|---:|---|---|---|",
        ]
    )
    for row in payload["unique_trade_rows"]:
        lines.append(
            "| {membership_group} | {code} | {signal_date} | {entry_date} | {ret} | {exit_reason} | {row_quality} | {variants} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Read-only row analysis only.",
            "- Uses existing variant comparison trades; no re-simulation and no operating policy changes.",
            "- q25 positive result is evaluated as evidence quality, not as approval.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not VARIANT_TRADES.exists():
        raise FileNotFoundError(VARIANT_TRADES)
    variant_payload = _read_json(VARIANT_JSON) if VARIANT_JSON.exists() else {}
    trades = pd.read_csv(VARIANT_TRADES, dtype=str, encoding="utf-8-sig")
    trades["entry_date_norm"] = trades.get("entry_date_norm", trades["entry_date"]).astype(str)
    target = trades[(trades["entry_date_norm"] >= "2026-01-01") & (trades["entry_date_norm"] <= "2026-06-30")].copy()
    target["ret"] = pd.to_numeric(target["ret"], errors="coerce").fillna(0.0)
    if target.empty:
        raise RuntimeError("no revived target rows found")

    unique_rows: list[dict[str, Any]] = []
    for key, part in target.groupby(TRADE_KEY, dropna=False, sort=True):
        base = part.iloc[0].copy()
        variants = set(part["variant"].astype(str))
        membership = _classify_variants(variants)
        out = {col: str(base.get(col, "")) for col in TRADE_KEY}
        feature_cols = [
            "market",
            "market_regime",
            "ret",
            "exit_reason",
            "score",
            "entry_gap_pct",
            "entry_trigger_reason",
            "entry_day_open_to_close_ret",
            "mfe_1d",
            "mae_1d",
            "followthrough_1d",
            "signal_ret1_pct",
            "signal_rs",
            "signal_v_accel",
            "signal_stretch",
            "signal_atr_pct",
            "signal_high_52w_gap",
            "signal_value",
        ]
        for col in feature_cols:
            out[col] = base.get(col, "")
        out["ret"] = round(float(base.get("ret", 0.0) or 0.0), 6)
        out["variants"] = "|".join(sorted(variants))
        out["variant_count"] = len(variants)
        out["membership_group"] = membership
        out["row_quality"] = _row_quality(base)
        unique_rows.append(out)

    unique_df = pd.DataFrame(unique_rows)
    unique_df["ret"] = pd.to_numeric(unique_df["ret"], errors="coerce").fillna(0.0)

    group_rows = [
        _summarize("all_revived_unique", unique_df, "unique 2026H1 trades revived by mkt_vol20 variants"),
        _summarize("remove_only_variant_unique", unique_df[unique_df["variants"].str.contains("remove_mkt_vol20_only", regex=False)], "all trades when mkt_vol20 is removed"),
        _summarize("q25_subset", unique_df[unique_df["membership_group"].eq("q25_subset")], "trades included by the q25 relaxed threshold"),
        _summarize("median_only_extra", unique_df[unique_df["membership_group"].eq("median_only_extra")], "additional trades included by median threshold beyond q25"),
        _summarize("remove_only_extra", unique_df[unique_df["membership_group"].eq("remove_only_extra")], "trades only revived when mkt_vol20 is removed"),
    ]

    q25 = unique_df[unique_df["membership_group"].eq("q25_subset")].copy()
    large_wins = q25[q25["row_quality"].eq("large_single_win")]
    q25_ret_sum = float(q25["ret"].sum()) if not q25.empty else 0.0
    if len(q25) <= 3 and len(large_wins) == 1 and q25_ret_sum > 0:
        conclusion = (
            "The q25-positive 2026H1 result is not yet repeatable evidence; it is driven by one large winning trade while the same subset has more losing than winning rows."
        )
    else:
        conclusion = "The revived subset needs broader validation before any threshold change."

    payload = {
        "status": STATUS,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "variant_trades": str(VARIANT_TRADES),
            "variant_summary": str(VARIANT_SUMMARY),
            "variant_json": str(VARIANT_JSON),
        },
        "variant_compare_status": variant_payload.get("status"),
        "input_target_rows_with_variant_duplicates": int(len(target)),
        "unique_trade_count": int(len(unique_df)),
        "group_summary": group_rows,
        "unique_trade_rows": unique_rows,
        "conclusion": conclusion,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required": [
            "Do not promote q25 mkt_vol20 relaxation from 3 rows.",
            "Search for an additional pre-entry guard that separates the single large win from repeated STOP-like revived rows.",
            "If no repeatable guard appears, keep mkt_vol20 as a volatility exclusion for C3 and explore a separate high-volatility strategy family.",
        ],
    }

    row_fields = [
        "membership_group",
        "row_quality",
        "variants",
        "variant_count",
        "signal_date",
        "entry_date",
        "exit_date",
        "code",
        "market",
        "market_regime",
        "ret",
        "exit_reason",
        "score",
        "entry_gap_pct",
        "entry_trigger_reason",
        "entry_day_open_to_close_ret",
        "mfe_1d",
        "mae_1d",
        "followthrough_1d",
        "signal_ret1_pct",
        "signal_rs",
        "signal_v_accel",
        "signal_stretch",
        "signal_atr_pct",
        "signal_high_52w_gap",
        "signal_value",
    ]
    group_fields = [
        "group",
        "n",
        "win_n",
        "loss_n",
        "stop_n",
        "win_rate",
        "ret_sum",
        "ret_mean",
        "profit_factor",
        "unique_codes",
        "first_entry_date",
        "last_entry_date",
        "note",
    ]
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(LATEST_ROWS_CSV, unique_rows, row_fields)
    _write_csv(LATEST_GROUP_CSV, group_rows, group_fields)
    _write_md(LATEST_MD, payload)
    print(json.dumps({"status": STATUS, "json": str(LATEST_JSON), "rows_csv": str(LATEST_ROWS_CSV), "groups_csv": str(LATEST_GROUP_CSV), "md": str(LATEST_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
