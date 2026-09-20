from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

SOURCE_NATIVE_CSV = LOG_DIR / "c3_strategy_param_replay_native_latest.csv"
TIMING_AUDIT_JSON = LOG_DIR / "c3_definition_timing_audit_latest.json"
CONTRACT_JSON = LOG_DIR / "c3_e_definition_contract_latest.json"

LATEST_JSON = LOG_DIR / "c3_tradable_proxy_oos_latest.json"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_tradable_proxy_oos_summary_latest.csv"
LATEST_ROWS_CSV = LOG_DIR / "c3_tradable_proxy_oos_rows_latest.csv"
LATEST_MD = LOG_DIR / "c3_tradable_proxy_oos_latest.md"

STATUS = "READ_ONLY_C3_TRADABLE_PROXY_OOS_NOT_OPERATIONAL"
DEFINITION_ID = "C3_TRADABLE_PROXY_V0"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _parse_date(value: Any) -> str:
    text = str(value or "").strip()[:10]
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return ""


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


def _max_drawdown(cumulative: list[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    for value in cumulative:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
    return round(max_dd, 6)


def _summarize(name: str, df: pd.DataFrame, notes: str = "") -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    cumulative = ret.cumsum().tolist()
    if len(ret) == 0:
        return {
            "segment": name,
            "n": 0,
            "win_n": 0,
            "loss_n": 0,
            "win_rate": math.nan,
            "ret_sum": 0.0,
            "ret_mean": math.nan,
            "profit_factor": "NA",
            "max_drawdown_sum_ret": 0.0,
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "notes": notes,
        }
    return {
        "segment": name,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6),
        "ret_sum": round(float(ret.sum()), 6),
        "ret_mean": round(float(ret.mean()), 6),
        "profit_factor": _profit_factor(ret.tolist()),
        "max_drawdown_sum_ret": _max_drawdown(cumulative),
        "first_entry_date": str(df["entry_date_norm"].min()),
        "last_entry_date": str(df["entry_date_norm"].max()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns else 0,
        "notes": notes,
    }


def _segment(df: pd.DataFrame, start: str = "", end: str = "") -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if start:
        mask &= df["entry_date_norm"] >= start
    if end:
        mask &= df["entry_date_norm"] <= end
    return df[mask].copy()


def _month_summaries(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out
    for month, part in df.groupby(df["entry_date_norm"].str[:7], sort=True):
        out.append(_summarize(f"month_{month}", part, "calendar month bucket"))
    return out


def _quarter_summaries(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out
    tmp = df.copy()
    dates = pd.to_datetime(tmp["entry_date_norm"], errors="coerce")
    tmp["_quarter"] = dates.dt.to_period("Q").astype(str)
    for quarter, part in tmp.groupby("_quarter", sort=True):
        out.append(_summarize(f"quarter_{quarter}", part.drop(columns=["_quarter"]), "calendar quarter bucket"))
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "segment",
        "n",
        "win_n",
        "loss_n",
        "win_rate",
        "ret_sum",
        "ret_mean",
        "profit_factor",
        "max_drawdown_sum_ret",
        "first_entry_date",
        "last_entry_date",
        "unique_codes",
        "notes",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 Tradable Proxy OOS Check",
        "",
        f"- status: `{payload['status']}`",
        f"- definition_id: `{payload['definition_id']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Core Segments",
        "",
        "| segment | n | win_rate | ret_sum | profit_factor | first | last | notes |",
        "|---|---:|---:|---:|---:|---|---|---|",
    ]
    for row in payload["core_segments"]:
        lines.append(
            "| {segment} | {n} | {win_rate} | {ret_sum} | {profit_factor} | {first_entry_date} | {last_entry_date} | {notes} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Quarter Segments",
            "",
            "| segment | n | win_rate | ret_sum | profit_factor |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["quarter_segments"]:
        lines.append(
            "| {segment} | {n} | {win_rate} | {ret_sum} | {profit_factor} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Uses only native C3 rows from `c3_strategy_param_replay_native_latest.csv`.",
            "- Excludes `followthrough_1d`, `market_after_recheck`, and `c3_after_recheck` as mandatory filters.",
            "- Does not change candidate generation, official backtest, HPO, paper, live orders, gates, or stable parameters.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not SOURCE_NATIVE_CSV.exists():
        raise FileNotFoundError(SOURCE_NATIVE_CSV)
    timing_audit = _read_json(TIMING_AUDIT_JSON) if TIMING_AUDIT_JSON.exists() else {}
    contract = _read_json(CONTRACT_JSON) if CONTRACT_JSON.exists() else {}

    df = pd.read_csv(SOURCE_NATIVE_CSV, dtype=str, encoding="utf-8-sig")
    if df.empty:
        raise RuntimeError("native C3 source is empty")
    df["entry_date_norm"] = df["entry_date"].map(_parse_date)
    df["signal_date_norm"] = df["signal_date"].map(_parse_date)
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    df = df[df["entry_date_norm"].ne("")].copy()
    df = df.sort_values(["entry_date_norm", "code"]).reset_index(drop=True)

    rows_out = df.copy()
    rows_out["definition_id"] = DEFINITION_ID
    rows_out["timing_class"] = "tradable_proxy_candidate"
    rows_out["mandatory_post_replay_filters_used"] = 0
    rows_out.to_csv(LATEST_ROWS_CSV, index=False, encoding="utf-8-sig")

    core_segments = [
        _summarize("all_observed_proxy_rows", df, "all native C3 proxy rows"),
        _summarize("early_reference_2024_to_2025H1", _segment(df, "", "2025-06-30"), "chronological reference segment"),
        _summarize("later_proxy_2025H2", _segment(df, "2025-07-01", "2025-12-31"), "chronological later holdout proxy"),
        _summarize("prior_user_window_2025-06-01_to_2026-06-30", _segment(df, "2025-06-01", "2026-06-30"), "requested prior window; no 2026 proxy rows present"),
        _summarize("calendar_2026H1", _segment(df, "2026-01-01", "2026-06-30"), "not evaluable because no proxy rows in 2026H1"),
    ]
    month_segments = _month_summaries(df)
    quarter_segments = _quarter_summaries(df)

    later = next(r for r in core_segments if r["segment"] == "later_proxy_2025H2")
    h1_2026 = next(r for r in core_segments if r["segment"] == "calendar_2026H1")
    if h1_2026["n"] == 0:
        conclusion = (
            "Later proxy remains positive in 2025H2, but true 2026H1 OOS is not evaluable because C3_TRADABLE_PROXY_V0 has no 2026 rows."
        )
        readiness = "SHARE_WITH_CAVEATS_RESEARCH_ONLY"
    elif later["n"] >= 10 and float(later["ret_sum"]) > 0:
        conclusion = "Later proxy segment is positive, but operational integration still requires parameter approval and broader replay."
        readiness = "SHARE_WITH_CAVEATS_RESEARCH_ONLY"
    else:
        conclusion = "Later proxy evidence is insufficient for integration."
        readiness = "NEEDS_MORE_EVIDENCE"

    payload = {
        "status": STATUS,
        "definition_id": DEFINITION_ID,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "native_rows_csv": str(SOURCE_NATIVE_CSV),
            "timing_audit_json": str(TIMING_AUDIT_JSON),
            "contract_json": str(CONTRACT_JSON),
        },
        "source_contract_status": contract.get("contract_status"),
        "timing_audit_status": timing_audit.get("status"),
        "observed_entry_date_range": {
            "min": str(df["entry_date_norm"].min()),
            "max": str(df["entry_date_norm"].max()),
        },
        "observed_signal_date_range": {
            "min": str(df["signal_date_norm"].min()),
            "max": str(df["signal_date_norm"].max()),
        },
        "core_segments": core_segments,
        "quarter_segments": quarter_segments,
        "month_segments": month_segments,
        "conclusion": conclusion,
        "readiness": readiness,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required_before_integration": [
            "Broader replay beyond the current 37-row proxy sample.",
            "Explain why 2026H1 has zero C3_TRADABLE_PROXY_V0 rows before treating 2026 as a negative or positive result.",
            "Strategy-scoped parameter approval remains required before operational use.",
        ],
    }

    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(LATEST_SUMMARY_CSV, core_segments + quarter_segments + month_segments)
    _write_md(LATEST_MD, payload)

    print(json.dumps({"status": STATUS, "json": str(LATEST_JSON), "summary_csv": str(LATEST_SUMMARY_CSV), "rows_csv": str(LATEST_ROWS_CSV), "md": str(LATEST_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
