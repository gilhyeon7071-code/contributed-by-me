from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from utils.price_history_contract import KEY_COL, apply_price_history_contract


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"
OLD_TRADES = SCAN_DIR / "balanced_original_trades.csv"
CURRENT_REPLAY_JSON = LOG_DIR / "c3_readonly_official_replay_latest.json"

LATEST_JSON = LOG_DIR / "c3_old27_current_data_probe_latest.json"
LATEST_ROWS_CSV = LOG_DIR / "c3_old27_current_data_probe_rows_latest.csv"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_old27_current_data_probe_summary_latest.csv"
LATEST_MD = LOG_DIR / "c3_old27_current_data_probe_latest.md"


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _ymd(value: Any) -> str:
    if pd.isna(value):
        return ""
    s = str(value)[:10].replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y%m%d")


def _iso(value: Any) -> str:
    y = _ymd(value)
    return f"{y[:4]}-{y[4:6]}-{y[6:8]}" if y else ""


def _read_old() -> pd.DataFrame:
    df = pd.read_csv(OLD_TRADES, dtype={"code": str}, encoding="utf-8-sig")
    df["code"] = df["code"].map(_norm_code)
    for col in ["signal_date", "entry_date", "exit_date"]:
        df[col] = df[col].map(_iso)
    return df


def _bounded_parquets() -> list[Path]:
    patterns = ["krx_daily_*_clean.parquet", "ohlcv_paper.parquet"]
    seen: set[str] = set()
    files: list[Path] = []
    for directory in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if not directory.exists() or not directory.is_dir():
            continue
        for pattern in patterns:
            for path in sorted(directory.glob(pattern)):
                key = str(path.resolve()).lower()
                if key in seen:
                    continue
                seen.add(key)
                files.append(path)
    return files


def _source_priority(path: Path) -> int:
    if path.parent == ROOT / "_krx_manual":
        return 2
    if path.parent == ROOT / "krx_daily_archive":
        return 1
    return 0


def _load_scoped_prices(codes: set[str], start_ymd: str, end_ymd: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    loaded: list[str] = []
    required = ["date", "code", "open", "high", "low", "close"]
    optional = ["volume", "value", "market", "name"]
    for path in _bounded_parquets():
        try:
            cols = list(pd.read_parquet(path, nrows=0).columns)
        except TypeError:
            cols = list(pd.read_parquet(path).head(0).columns)
        use_cols = [c for c in required + optional if c in cols]
        if not set(required).issubset(use_cols):
            continue
        try:
            part = pd.read_parquet(path, columns=use_cols)
        except TypeError:
            part = pd.read_parquet(path)[use_cols]
        part["code"] = part["code"].map(_norm_code)
        part["date"] = part["date"].map(_ymd)
        part = part[(part["code"].isin(codes)) & (part["date"] >= start_ymd) & (part["date"] <= end_ymd)].copy()
        if part.empty:
            continue
        if "market" not in part.columns:
            part["market"] = "KRX"
        part["_src_priority"] = _source_priority(path)
        part["_src_mtime"] = path.stat().st_mtime
        part["_source_file"] = str(path)
        frames.append(part)
        loaded.append(str(path))
    if not frames:
        return pd.DataFrame(columns=required), {"loaded_files": [], "raw_rows": 0, "dedup_rows": 0}
    raw = pd.concat(frames, ignore_index=True)
    raw_rows = int(len(raw))
    dedup = raw.sort_values(["_src_priority", "_src_mtime"]).drop_duplicates(["code", "date"], keep="last").copy()
    dedup_rows = int(len(dedup))
    dedup = dedup.drop(columns=["_src_priority", "_src_mtime"])
    for col in ["open", "high", "low", "close", "volume", "value"]:
        if col in dedup.columns:
            dedup[col] = pd.to_numeric(dedup[col], errors="coerce")
    return dedup, {"loaded_files": loaded, "raw_rows": raw_rows, "dedup_rows": dedup_rows}


def _row_key(df: pd.DataFrame) -> pd.Series:
    return df["code"].map(_norm_code) + "|" + df["date"].map(_iso)


def _lookup(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if df.empty:
        return {}
    work = df.copy()
    work["date"] = work["date"].map(_iso)
    return {str(k): v for k, v in zip(_row_key(work), work.to_dict("records"))}


def _price_row_status(row: dict[str, Any] | None) -> tuple[bool, str]:
    if row is None:
        return False, "missing"
    needed = ["open", "high", "low", "close"]
    bad = [c for c in needed if c not in row or not math.isfinite(float(row[c])) or float(row[c]) <= 0]
    if bad:
        return False, "invalid_ohlc"
    return True, "ok"


def _read_current_replay() -> dict[str, Any]:
    if not CURRENT_REPLAY_JSON.exists():
        return {}
    return json.loads(CURRENT_REPLAY_JSON.read_text(encoding="utf-8-sig"))


def _parse_stdout_price_integrity(run_dir: str) -> dict[str, Any]:
    path = Path(run_dir) / "report_backtest_stdout.txt"
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8", errors="replace")
    m = re.search(
        r"PRICE_HISTORY_INTEGRITY\] contract=(\S+) rows_in=(\d+) rows_out=(\d+) invalid_rows=(\d+) gap_breaks=(\d+) extreme_breaks=(\d+) segments=(\d+)",
        text,
    )
    if not m:
        return {}
    return {
        "contract": m.group(1),
        "rows_in": int(m.group(2)),
        "rows_out": int(m.group(3)),
        "invalid_rows": int(m.group(4)),
        "gap_breaks": int(m.group(5)),
        "extreme_breaks": int(m.group(6)),
        "segments": int(m.group(7)),
    }



def _trigger_probe(contracted: pd.DataFrame, code: str, signal_date: str, old_trigger_px: Any, old_entry_date: str) -> dict[str, Any]:
    try:
        trigger_px = float(old_trigger_px)
    except (TypeError, ValueError):
        return {"trigger_probe_status": "missing_old_trigger_px", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    if not math.isfinite(trigger_px) or trigger_px <= 0:
        return {"trigger_probe_status": "invalid_old_trigger_px", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    if contracted.empty:
        return {"trigger_probe_status": "contracted_empty", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    work = contracted.copy()
    work["date_iso"] = work["date"].map(_iso)
    cdf = work[work["code"].map(_norm_code).eq(code)].sort_values("date_iso").copy()
    signal_row = cdf[cdf["date_iso"].eq(signal_date)]
    if signal_row.empty:
        return {"trigger_probe_status": "signal_missing_after_contract", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    signal_seg = str(signal_row.iloc[0].get(KEY_COL, ""))
    window = cdf[(cdf["date_iso"] > signal_date) & (cdf[KEY_COL].astype(str).eq(signal_seg))].head(5).copy()
    if window.empty:
        return {"trigger_probe_status": "no_forward_window", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    hit = window[pd.to_numeric(window["high"], errors="coerce") >= trigger_px]
    if hit.empty:
        return {"trigger_probe_status": "no_hit_5d", "trigger_hit_5d": False, "trigger_first_date": "", "trigger_entry_date_match": False}
    first = hit.iloc[0]
    first_date = str(first["date_iso"])
    return {
        "trigger_probe_status": "ok",
        "trigger_hit_5d": True,
        "trigger_first_date": first_date,
        "trigger_entry_date_match": bool(first_date == old_entry_date),
        "trigger_px_old": trigger_px,
        "trigger_first_open": float(first["open"]),
        "trigger_first_high": float(first["high"]),
    }

def main() -> int:
    old = _read_old()
    codes = set(old["code"].map(_norm_code))
    min_signal = min(old["signal_date"].map(_ymd))
    max_exit = max(old["exit_date"].map(_ymd))
    start_ymd = str(int(min_signal[:4]) - 1) + "0101"
    end_ymd = max_exit
    raw, load_audit = _load_scoped_prices(codes, start_ymd, end_ymd)

    raw_lookup = _lookup(raw)
    try:
        contracted, contract_audit = apply_price_history_contract(raw)
    except Exception as exc:  # read-only diagnosis must preserve failure as data
        contracted = pd.DataFrame(columns=list(raw.columns) + [KEY_COL])
        contract_audit = {"error": type(exc).__name__, "message": str(exc), "rows_in": int(len(raw)), "rows_out": 0}
    contracted_lookup = _lookup(contracted)

    rows: list[dict[str, Any]] = []
    for idx, rec in old.reset_index(drop=True).iterrows():
        code = _norm_code(rec["code"])
        key_signal = f"{code}|{rec['signal_date']}"
        key_entry = f"{code}|{rec['entry_date']}"
        key_exit = f"{code}|{rec['exit_date']}"
        raw_signal = raw_lookup.get(key_signal)
        raw_entry = raw_lookup.get(key_entry)
        raw_exit = raw_lookup.get(key_exit)
        con_signal = contracted_lookup.get(key_signal)
        con_entry = contracted_lookup.get(key_entry)
        con_exit = contracted_lookup.get(key_exit)
        raw_signal_ok, raw_signal_status = _price_row_status(raw_signal)
        con_signal_ok, con_signal_status = _price_row_status(con_signal)
        raw_entry_ok, raw_entry_status = _price_row_status(raw_entry)
        con_entry_ok, con_entry_status = _price_row_status(con_entry)
        raw_exit_ok, raw_exit_status = _price_row_status(raw_exit)
        con_exit_ok, con_exit_status = _price_row_status(con_exit)
        signal_seg = con_signal.get(KEY_COL) if con_signal else ""
        entry_seg = con_entry.get(KEY_COL) if con_entry else ""
        exit_seg = con_exit.get(KEY_COL) if con_exit else ""
        trigger = _trigger_probe(contracted, code, rec["signal_date"], rec.get("entry_trigger_px"), rec["entry_date"])
        rows.append(
            {
                "old_index": int(idx),
                "code": code,
                "signal_date": rec["signal_date"],
                "entry_date": rec["entry_date"],
                "exit_date": rec["exit_date"],
                "old_ret": rec.get("ret"),
                "old_exit_reason": rec.get("exit_reason"),
                "raw_signal_status": raw_signal_status,
                "contract_signal_status": con_signal_status,
                "raw_entry_status": raw_entry_status,
                "contract_entry_status": con_entry_status,
                "raw_exit_status": raw_exit_status,
                "contract_exit_status": con_exit_status,
                "signal_entry_same_segment": bool(signal_seg and signal_seg == entry_seg),
                "signal_exit_same_segment": bool(signal_seg and signal_seg == exit_seg),
                "contract_signal_segment": signal_seg,
                "contract_entry_segment": entry_seg,
                "contract_exit_segment": exit_seg,
                "raw_signal_source": raw_signal.get("_source_file", "") if raw_signal else "",
                **trigger,
                "contract_signal_open": con_signal.get("open", "") if con_signal else "",
                "contract_signal_high": con_signal.get("high", "") if con_signal else "",
                "contract_signal_low": con_signal.get("low", "") if con_signal else "",
                "contract_signal_close": con_signal.get("close", "") if con_signal else "",
                "contract_entry_open": con_entry.get("open", "") if con_entry else "",
                "contract_entry_high": con_entry.get("high", "") if con_entry else "",
                "contract_entry_low": con_entry.get("low", "") if con_entry else "",
                "contract_entry_close": con_entry.get("close", "") if con_entry else "",
                "contract_exit_close": con_exit.get("close", "") if con_exit else "",
            }
        )

    detail = pd.DataFrame(rows)
    summary_rows = []
    for col in [
        "raw_signal_status",
        "contract_signal_status",
        "raw_entry_status",
        "contract_entry_status",
        "raw_exit_status",
        "contract_exit_status",
        "signal_entry_same_segment",
        "signal_exit_same_segment",
        "trigger_probe_status",
        "trigger_hit_5d",
        "trigger_entry_date_match",
    ]:
        vc = detail[col].value_counts(dropna=False).to_dict()
        for key, value in vc.items():
            summary_rows.append({"metric": col, "value": str(key), "n": int(value)})
    summary = pd.DataFrame(summary_rows)

    replay = _read_current_replay()
    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_OLD27_CURRENT_DATA_PROBE_NOT_OPERATIONAL",
        "old_rows": int(len(old)),
        "old_codes": int(len(codes)),
        "price_load_audit": load_audit,
        "scoped_contract_audit": contract_audit,
        "current_replay_price_integrity": _parse_stdout_price_integrity(str(replay.get("run_dir", ""))),
        "raw_signal_ok": int((detail["raw_signal_status"] == "ok").sum()),
        "contract_signal_ok": int((detail["contract_signal_status"] == "ok").sum()),
        "raw_entry_ok": int((detail["raw_entry_status"] == "ok").sum()),
        "contract_entry_ok": int((detail["contract_entry_status"] == "ok").sum()),
        "raw_exit_ok": int((detail["raw_exit_status"] == "ok").sum()),
        "contract_exit_ok": int((detail["contract_exit_status"] == "ok").sum()),
        "signal_entry_same_segment": int(detail["signal_entry_same_segment"].sum()),
        "signal_exit_same_segment": int(detail["signal_exit_same_segment"].sum()),
        "trigger_hit_5d": int(detail["trigger_hit_5d"].sum()) if "trigger_hit_5d" in detail.columns else 0,
        "trigger_entry_date_match": int(detail["trigger_entry_date_match"].sum()) if "trigger_entry_date_match" in detail.columns else 0,
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
    }

    detail.to_csv(LATEST_ROWS_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(LATEST_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# C3 old27 current data probe",
        "",
        f"- status: {payload['status']}",
        f"- old_rows: {payload['old_rows']}",
        f"- old_codes: {payload['old_codes']}",
        f"- raw_signal_ok: {payload['raw_signal_ok']}",
        f"- contract_signal_ok: {payload['contract_signal_ok']}",
        f"- raw_entry_ok: {payload['raw_entry_ok']}",
        f"- contract_entry_ok: {payload['contract_entry_ok']}",
        f"- raw_exit_ok: {payload['raw_exit_ok']}",
        f"- contract_exit_ok: {payload['contract_exit_ok']}",
        f"- signal_entry_same_segment: {payload['signal_entry_same_segment']}",
        f"- signal_exit_same_segment: {payload['signal_exit_same_segment']}",
        f"- trigger_hit_5d: {payload['trigger_hit_5d']}",
        f"- trigger_entry_date_match: {payload['trigger_entry_date_match']}",
        "",
        "## Summary counts",
        "",
    ]
    for row in summary.to_dict("records"):
        lines.append(f"- {row['metric']}={row['value']}: {row['n']}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
