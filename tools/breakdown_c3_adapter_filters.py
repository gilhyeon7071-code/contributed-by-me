from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
NATIVE = LOG_DIR / "c3_strategy_param_replay_native_latest.csv"
ADAPTER = LOG_DIR / "c3_strategy_param_replay_adapter_latest.csv"
CROSS_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_c3_cross_variant_validation_latest.json"
OVERLAP_JSON = LOG_DIR / "c3_strategy_param_replay_overlap_latest.json"

LATEST_JSON = LOG_DIR / "c3_adapter_filter_breakdown_latest.json"
LATEST_ROWS_CSV = LOG_DIR / "c3_adapter_filter_breakdown_rows_latest.csv"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_adapter_filter_breakdown_summary_latest.csv"
LATEST_SEQUENCE_CSV = LOG_DIR / "c3_adapter_filter_breakdown_sequence_latest.csv"
LATEST_MD = LOG_DIR / "c3_adapter_filter_breakdown_latest.md"


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _norm_date(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _prep_trade(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["code"] = out["code"].map(_norm_code)
    for col in ["signal_date", "entry_date", "exit_date"]:
        if col in out.columns:
            out[col] = out[col].map(_norm_date)
    return out


def _load_cross_balanced() -> pd.DataFrame:
    payload = json.loads(CROSS_JSON.read_text(encoding="utf-8-sig"))
    rows = payload.get("rows") or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["variant"].astype(str).eq("balanced_original")].copy()
    df["code"] = df["code"].map(_norm_code)
    for col in ["signal_date", "entry_date", "exit_date"]:
        if col in df.columns:
            df[col] = df[col].map(_norm_date)
    return df


def _join_native_cross(native: pd.DataFrame, cross: pd.DataFrame) -> pd.DataFrame:
    join_keys = ["code", "signal_date", "entry_date", "exit_date"]
    join_cols = join_keys + ["c3_after_recheck", "market_after_recheck", "market_raw"]
    if cross.empty:
        out = native.copy()
        out["cross_joined"] = 0
        out["c3_after_recheck"] = 0
        out["market_after_recheck"] = ""
        out["market_raw"] = ""
        return out
    joined = native.merge(cross[[c for c in join_cols if c in cross.columns]].drop_duplicates(join_keys), on=join_keys, how="left", indicator=True)
    joined["cross_joined"] = joined["_merge"].eq("both").astype(int)
    joined = joined.drop(columns=["_merge"])
    return joined


def _summarize_ret(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty or "ret" not in df.columns:
        return {"n": int(len(df)), "win_n": 0, "loss_n": 0, "win_rate": None, "ret_sum": 0.0, "profit_factor": None}
    ret = pd.to_numeric(df["ret"], errors="coerce").dropna()
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss = float(-losses.sum())
    if gross_loss > 0:
        pf: Any = round(gross_profit / gross_loss, 6)
    elif gross_profit > 0:
        pf = "inf"
    else:
        pf = None
    return {
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6) if len(ret) else None,
        "ret_sum": round(float(ret.sum()), 6) if len(ret) else 0.0,
        "profit_factor": pf,
    }


def main() -> int:
    native = _prep_trade(_read_csv(NATIVE))
    adapter = _prep_trade(_read_csv(ADAPTER))
    cross = _load_cross_balanced()
    joined = _join_native_cross(native, cross)

    for col in ["score", "followthrough_1d", "signal_v_accel"]:
        joined[col] = pd.to_numeric(joined[col], errors="coerce")
    joined["c3_after_recheck"] = pd.to_numeric(joined.get("c3_after_recheck"), errors="coerce").fillna(0).astype(int)
    joined["market_after_recheck"] = joined.get("market_after_recheck", "").fillna("").astype(str)

    conditions = [
        ("cross_joined", joined["cross_joined"].eq(1)),
        ("c3_after_recheck_eq_1", joined["c3_after_recheck"].eq(1)),
        ("score_ge_1", joined["score"] >= 1.0),
        ("followthrough_1d_eq_1", joined["followthrough_1d"] == 1),
        ("signal_v_accel_ge_0_9", joined["signal_v_accel"] >= 0.9),
        ("market_after_recheck_nonempty", joined["market_after_recheck"].str.strip().ne("")),
    ]

    detail = joined.copy()
    for name, mask in conditions:
        detail[name] = mask.fillna(False).astype(bool)
    detail["adapter_all_conditions"] = detail[[name for name, _ in conditions]].all(axis=1)
    detail["failed_conditions"] = detail.apply(
        lambda r: "|".join([name for name, _ in conditions if not bool(r[name])]),
        axis=1,
    )

    summary_rows: list[dict[str, Any]] = []
    for name, _ in conditions:
        passed_df = detail[detail[name]].copy()
        row = {"condition": name, "pass_n": int(detail[name].sum()), "fail_n": int((~detail[name]).sum())}
        row.update({f"pass_{k}": v for k, v in _summarize_ret(passed_df).items()})
        summary_rows.append(row)
    final_df = detail[detail["adapter_all_conditions"]].copy()
    final_row = {"condition": "adapter_all_conditions", "pass_n": int(len(final_df)), "fail_n": int(len(detail) - len(final_df))}
    final_row.update({f"pass_{k}": v for k, v in _summarize_ret(final_df).items()})
    summary_rows.append(final_row)

    sequence_rows: list[dict[str, Any]] = []
    current = detail.copy()
    sequence_rows.append({"step": "native_start", **_summarize_ret(current)})
    for name, _ in conditions:
        before = len(current)
        current = current[current[name]].copy()
        row = {"step": name, "before_n": int(before), "after_n": int(len(current)), "removed_n": int(before - len(current))}
        row.update(_summarize_ret(current))
        sequence_rows.append(row)

    fail_counts = (
        detail.loc[~detail["adapter_all_conditions"], "failed_conditions"]
        .str.get_dummies(sep="|")
        .sum()
        .sort_values(ascending=False)
        .to_dict()
        if not detail.empty
        else {}
    )
    overlap = json.loads(OVERLAP_JSON.read_text(encoding="utf-8-sig")) if OVERLAP_JSON.exists() else {}
    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_ADAPTER_FILTER_BREAKDOWN_NOT_OPERATIONAL",
        "native_rows": int(len(native)),
        "joined_rows": int(len(detail)),
        "adapter_rows": int(len(adapter)),
        "adapter_all_conditions_rows": int(detail["adapter_all_conditions"].sum()) if not detail.empty else 0,
        "condition_fail_counts_nonexclusive": {str(k): int(v) for k, v in fail_counts.items()},
        "sequence": sequence_rows,
        "overlap_reference": {
            "old_rows": overlap.get("old_rows"),
            "old_in_native": overlap.get("old_in_native"),
            "old_in_adapter": overlap.get("old_in_adapter"),
        },
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "detail_csv": str(LATEST_ROWS_CSV),
        "summary_csv": str(LATEST_SUMMARY_CSV),
        "sequence_csv": str(LATEST_SEQUENCE_CSV),
    }

    detail.to_csv(LATEST_ROWS_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(summary_rows).to_csv(LATEST_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(sequence_rows).to_csv(LATEST_SEQUENCE_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 adapter filter breakdown",
        "",
        f"- status: {payload['status']}",
        f"- native_rows: {payload['native_rows']}",
        f"- adapter_rows: {payload['adapter_rows']}",
        f"- adapter_all_conditions_rows: {payload['adapter_all_conditions_rows']}",
        "",
        "## Sequential filter path",
        "",
    ]
    for row in sequence_rows:
        lines.append(f"- {row['step']}: n={row.get('n')}, before={row.get('before_n', '')}, removed={row.get('removed_n', '')}, ret_sum={row.get('ret_sum')}, pf={row.get('profit_factor')}")
    lines.extend(["", "## Non-exclusive fail counts", ""])
    for key, value in payload["condition_fail_counts_nonexclusive"].items():
        lines.append(f"- {key}: {value}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
