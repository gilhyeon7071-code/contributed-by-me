from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
ARCHIVE_DIR = ROOT / "krx_daily_archive"

CONTRACT_JSON = LOG_DIR / "c3_normal_official_replay_contract_latest.json"
NATIVE_SELECTION_JSON = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
CROSS_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_c3_cross_variant_validation_latest.json"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
RESEARCH_PARAMS = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = LOG_DIR / f"c3_readonly_official_replay_{RUN_ID}"
LATEST_JSON = LOG_DIR / "c3_readonly_official_replay_latest.json"
LATEST_NATIVE_CSV = LOG_DIR / "c3_readonly_official_replay_native_latest.csv"
LATEST_ADAPTER_CSV = LOG_DIR / "c3_readonly_official_replay_adapter_latest.csv"
LATEST_REPAIR_CSV = LOG_DIR / "c3_readonly_official_replay_repair_latest.csv"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_readonly_official_replay_summary_latest.csv"
LATEST_MD = LOG_DIR / "c3_readonly_official_replay_latest.md"

STATUS = "READ_ONLY_C3_OFFICIAL_REPLAY_WRAPPER_NOT_OPERATIONAL"
TP_THRESHOLD_PCT = 5.0
TP_THRESHOLD_DECIMAL = 0.05
TP_FRACTION = 0.5
NATIVE_COLUMNS = [
    "signal_date",
    "entry_date",
    "exit_date",
    "code",
    "name",
    "market",
    "entry_px",
    "exit_px",
    "ret",
    "exit_reason",
    "score",
    "relax_level",
    "entry_gap_pct",
    "entry_trigger_type",
    "entry_trigger_px",
    "entry_signal_basis_px",
    "entry_trigger_reason",
    "entry_trigger_window_days",
    "entry_day_open_to_close_ret",
    "entry_day_high_from_open_pct",
    "entry_day_low_from_open_pct",
    "mfe_1d",
    "mae_1d",
    "followthrough_1d",
    "setup_family",
    "signal_ret1_pct",
    "signal_rs",
    "signal_v_accel",
    "signal_stretch",
    "signal_atr_pct",
    "signal_high_52w_gap",
    "signal_value",
]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _parse_ymd(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return ""
    s = s[:10].replace("-", "")
    if len(s) != 8 or not s.isdigit():
        return ""
    return s


def _norm_iso_date(value: Any) -> str:
    ymd = _parse_ymd(value)
    if not ymd:
        return ""
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _archive_file_range(path: Path) -> tuple[str, str] | None:
    m = re.search(r"krx_daily_(\d{8})_(\d{8})_clean\.parquet$", path.name)
    if not m:
        return None
    return m.group(1), m.group(2)


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    return a_start <= b_end and b_start <= a_end


def _load_archive_price_frame(min_ymd: str, max_ymd: str, codes: set[str]) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []
    for path in sorted(ARCHIVE_DIR.glob("krx_daily_*_clean.parquet")):
        file_range = _archive_file_range(path)
        if not file_range or not _overlaps(file_range[0], file_range[1], min_ymd, max_ymd):
            continue
        df = pd.read_parquet(path, columns=["date", "code", "open", "high", "low", "close"])
        df["date"] = df["date"].map(_parse_ymd)
        df["code"] = df["code"].map(_norm_code)
        df = df[(df["date"] >= min_ymd) & (df["date"] <= max_ymd) & (df["code"].isin(codes))].copy()
        if df.empty:
            continue
        df["_archive_file"] = path.name
        frames.append(df)
        loaded_files.append(path.name)
    if not frames:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "_archive_file"]), loaded_files
    out = pd.concat(frames, ignore_index=True)
    before = len(out)
    out = out.sort_values(["date", "code", "_archive_file"]).drop_duplicates(["date", "code"], keep="last")
    out.attrs["duplicate_price_rows_removed"] = before - len(out)
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out, loaded_files


def _price_by_code(price_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if price_df.empty:
        return {}
    return {code: part.sort_values("date").reset_index(drop=True) for code, part in price_df.groupby("code", sort=False)}


def _path_metrics(row: pd.Series, lookup: dict[str, pd.DataFrame]) -> dict[str, Any]:
    entry_px = _safe_float(row.get("entry_px"))
    code = _norm_code(row.get("code"))
    entry_ymd = _parse_ymd(row.get("entry_date"))
    exit_ymd = _parse_ymd(row.get("exit_date"))
    if not math.isfinite(entry_px) or entry_px <= 0:
        return {"path_available": 0, "path_missing_reason": "entry_px_invalid", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    part = lookup.get(code)
    if part is None or part.empty:
        return {"path_available": 0, "path_missing_reason": "code_not_found", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    span = part[(part["date"] >= entry_ymd) & (part["date"] <= exit_ymd)]
    if span.empty:
        return {"path_available": 0, "path_missing_reason": "no_path_rows", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    high = pd.to_numeric(span["high"], errors="coerce").max()
    low = pd.to_numeric(span["low"], errors="coerce").min()
    return {
        "path_available": 1,
        "path_missing_reason": "",
        "path_rows": int(len(span)),
        "max_high_pct_to_exit": float((high / entry_px - 1.0) * 100.0) if pd.notna(high) else math.nan,
        "min_low_pct_to_exit": float((low / entry_px - 1.0) * 100.0) if pd.notna(low) else math.nan,
    }


def _summarize(name: str, df: pd.DataFrame, ret_col: str = "ret") -> dict[str, Any]:
    ret = pd.to_numeric(df.get(ret_col, pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss_abs = float(-losses.sum())
    if len(df) == 0:
        pf_value: float | str = "NA"
    elif gross_loss_abs > 0:
        pf_value = round(float(gross_profit / gross_loss_abs), 6)
    elif gross_profit > 0:
        pf_value = "inf"
    else:
        pf_value = "NA"
    return {
        "layer": name,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6) if len(df) else math.nan,
        "ret_sum": round(float(ret.sum()), 6),
        "ret_mean": round(float(ret.mean()), 6) if len(df) else math.nan,
        "profit_factor": pf_value,
    }

def _run_native_report() -> dict[str, Any]:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "REPORT_RESEARCH_MODE": "1",
            "REPORT_RESEARCH_PARAMS_PATH": str(RESEARCH_PARAMS),
            "REPORT_RESEARCH_OUTPUT_DIR": str(RUN_DIR),
            "REPORT_RESEARCH_SELECTION_CONTRACT_PATH": str(NATIVE_SELECTION_JSON),
            "REPORT_ALLOW_UNAPPROVED_FALLBACK": "1",
        }
    )
    proc = subprocess.run(
        [sys.executable, str(REPORT_BACKTEST)],
        cwd=str(ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=300,
    )
    (RUN_DIR / "report_backtest_stdout.txt").write_text(proc.stdout or "", encoding="utf-8")
    (RUN_DIR / "report_backtest_stderr.txt").write_text(proc.stderr or "", encoding="utf-8")
    return {"returncode": int(proc.returncode), "stdout_tail": (proc.stdout or "")[-4000:], "stderr_tail": (proc.stderr or "")[-4000:]}


def _read_native_trades(path: Path) -> tuple[pd.DataFrame, bool, str]:
    try:
        df = pd.read_csv(path, dtype={"code": str})
        return df, False, ""
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=NATIVE_COLUMNS), True, "empty_native_trades_csv"


def _load_cross_balanced() -> pd.DataFrame:
    payload = _read_json(CROSS_JSON)
    rows = payload.get("rows") or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["variant"].astype(str).eq("balanced_original")].copy()
    df["code"] = df["code"].map(_norm_code)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = df[col].map(_norm_iso_date)
    return df


def main() -> int:
    contract = _read_json(CONTRACT_JSON)
    native_run = _run_native_report()
    if native_run["returncode"] != 0:
        payload = {
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "READ_ONLY_C3_OFFICIAL_REPLAY_WRAPPER_FAILED",
            "native_run": native_run,
            "operation_effect": {
                "candidate_generation_changed": False,
                "backtest_changed": False,
                "hpo_changed": False,
                "paper_or_live_order_changed": False,
                "policy_changed": False,
            },
            "full_logic_application": "NOT_APPLIED",
            "run_dir": str(RUN_DIR),
        }
        LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"status": payload["status"], "returncode": native_run["returncode"], "run_dir": str(RUN_DIR)}, ensure_ascii=False))
        return 2

    native_path = RUN_DIR / "report_backtest_trades_v41_1.csv"
    summary_path = RUN_DIR / "report_backtest_summary_v41_1.json"
    if not native_path.exists() or not summary_path.exists():
        raise SystemExit("native report output missing")

    native, native_empty_file, native_empty_reason = _read_native_trades(native_path)
    native["code"] = native["code"].map(_norm_code)
    for col in ["signal_date", "entry_date", "exit_date"]:
        if col in native.columns:
            native[col] = native[col].map(_norm_iso_date)
    native.to_csv(LATEST_NATIVE_CSV, index=False, encoding="utf-8-sig")

    cross = _load_cross_balanced()
    join_keys = ["code", "signal_date", "entry_date", "exit_date"]
    join_cols = join_keys + ["c3_after_recheck", "market_after_recheck", "market_raw"]
    joined = native.merge(cross[[c for c in join_cols if c in cross.columns]].drop_duplicates(join_keys), on=join_keys, how="left", indicator=True)
    joined["cross_joined"] = joined["_merge"].eq("both").astype(int)
    joined = joined.drop(columns=["_merge"])

    for col in ["score", "followthrough_1d", "signal_v_accel"]:
        joined[col] = pd.to_numeric(joined[col], errors="coerce")
    joined["c3_after_recheck"] = pd.to_numeric(joined.get("c3_after_recheck"), errors="coerce").fillna(0).astype(int)
    joined["market_after_recheck"] = joined.get("market_after_recheck", "").fillna("").astype(str)
    adapter = joined[
        joined["c3_after_recheck"].eq(1)
        & (joined["score"] >= 1.0)
        & (joined["followthrough_1d"] == 1)
        & (joined["signal_v_accel"] >= 0.9)
        & joined["market_after_recheck"].str.strip().ne("")
    ].copy()

    if not adapter.empty:
        min_ymd = min(adapter["entry_date"].map(_parse_ymd))
        max_ymd = max(adapter["exit_date"].map(_parse_ymd))
        price_df, loaded_files = _load_archive_price_frame(min_ymd, max_ymd, set(adapter["code"].astype(str)))
        lookup = _price_by_code(price_df)
        metrics = adapter.apply(lambda row: _path_metrics(row, lookup), axis=1, result_type="expand")
        adapter = pd.concat([adapter.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)
        duplicate_removed = int(price_df.attrs.get("duplicate_price_rows_removed", 0))
    else:
        loaded_files = []
        duplicate_removed = 0
        for col, default in [
            ("path_available", 0),
            ("path_missing_reason", "no_adapter_rows"),
            ("path_rows", 0),
            ("max_high_pct_to_exit", math.nan),
            ("min_low_pct_to_exit", math.nan),
        ]:
            adapter[col] = default

    adapter["targeted_repair_hit"] = (
        adapter["exit_reason"].astype(str).str.upper().str.contains("STOP", na=False)
        & (pd.to_numeric(adapter["max_high_pct_to_exit"], errors="coerce") >= TP_THRESHOLD_PCT)
    ).astype(int)
    adapter["targeted_repair_ret"] = pd.to_numeric(adapter["ret"], errors="coerce")
    hit_mask = adapter["targeted_repair_hit"].eq(1)
    adapter.loc[hit_mask, "targeted_repair_ret"] = TP_FRACTION * TP_THRESHOLD_DECIMAL + (1.0 - TP_FRACTION) * pd.to_numeric(adapter.loc[hit_mask, "ret"], errors="coerce")
    adapter["targeted_repair_reason"] = adapter["targeted_repair_hit"].map({1: "stop_after_5pct_high_repaired", 0: "not_repaired"})

    adapter.to_csv(LATEST_ADAPTER_CSV, index=False, encoding="utf-8-sig")
    adapter.to_csv(LATEST_REPAIR_CSV, index=False, encoding="utf-8-sig")

    summary_rows = [
        _summarize("native_report_all", native, "ret"),
        _summarize("c3_adapter_baseline", adapter, "ret"),
        _summarize("c3_adapter_targeted_repair", adapter, "targeted_repair_ret"),
    ]
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(LATEST_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    report_summary = _read_json(summary_path)
    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": STATUS,
        "run_id": RUN_ID,
        "run_dir": str(RUN_DIR),
        "contract_file": str(CONTRACT_JSON),
        "native_selection_contract_file": str(NATIVE_SELECTION_JSON),
        "research_params_path": str(RESEARCH_PARAMS),
        "native_run": native_run,
        "native_rows": int(len(native)),
        "native_empty_file": bool(native_empty_file),
        "native_empty_reason": str(native_empty_reason),
        "cross_balanced_rows": int(len(cross)),
        "cross_joined_rows": int(joined["cross_joined"].sum()) if "cross_joined" in joined.columns else 0,
        "adapter_rows": int(len(adapter)),
        "targeted_repair_hit_rows": int(adapter["targeted_repair_hit"].sum()) if "targeted_repair_hit" in adapter.columns else 0,
        "path_available_rows": int(pd.to_numeric(adapter.get("path_available", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not adapter.empty else 0,
        "loaded_archive_files": loaded_files,
        "archive_duplicate_price_rows_removed": duplicate_removed,
        "report_summary_file": str(summary_path),
        "report_param_gate": report_summary.get("param_gate", {}),
        "summary": summary_df.to_dict(orient="records"),
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "outputs": {
            "native_csv": str(LATEST_NATIVE_CSV),
            "adapter_csv": str(LATEST_ADAPTER_CSV),
            "repair_csv": str(LATEST_REPAIR_CSV),
            "summary_csv": str(LATEST_SUMMARY_CSV),
            "json": str(LATEST_JSON),
            "markdown": str(LATEST_MD),
        },
        "boundary": [
            "Native report output and C3 adapter output are separate.",
            "REPORT_ALLOW_UNAPPROVED_FALLBACK=1 is used only inside research mode.",
            "No paper/live/gate/stable files are modified.",
        ],
    }
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 read-only official replay wrapper",
        "",
        f"- status: `{STATUS}`",
        f"- run id: `{RUN_ID}`",
        f"- native rows: {len(native)}",
        f"- C3 adapter rows: {len(adapter)}",
        f"- targeted repair hit rows: {payload['targeted_repair_hit_rows']}",
        f"- full logic application: `NOT_APPLIED`",
        "",
        "## Summary",
        "",
    ]
    for row in summary_rows:
        lines.append(
            f"- {row['layer']}: n={row['n']}, ret_sum={row['ret_sum']}, pf={row['profit_factor']}, win_rate={row['win_rate']}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Research mode only.",
            "- Native report output is not overwritten in `12_Risk_Controlled`.",
            "- C3 adapter and targeted repair are post-replay outputs only.",
            "- No paper/live order route, gate, policy, HPO, or candidate generation changed.",
        ]
    )
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "status": STATUS,
                "native_rows": int(len(native)),
        "native_empty_file": bool(native_empty_file),
        "native_empty_reason": str(native_empty_reason),
                "adapter_rows": int(len(adapter)),
                "targeted_repair_hit_rows": int(payload["targeted_repair_hit_rows"]),
                "summary_csv": str(LATEST_SUMMARY_CSV),
                "json": str(LATEST_JSON),
                "run_dir": str(RUN_DIR),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
