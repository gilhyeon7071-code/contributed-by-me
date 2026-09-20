import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_HISTORY_CSV = LOGS / "future_signal_preview_history.csv"
LATEST_JSON = LOGS / "future_signal_history_integrity_audit_latest.json"
LATEST_CSV = LOGS / "future_signal_history_integrity_audit_latest.csv"
STALE_CSV = LOGS / "future_signal_history_integrity_stale_rows_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def _norm_ymd(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s[:8]


def _code_set(df: pd.DataFrame) -> Set[str]:
    if df.empty or "code" not in df.columns:
        return set()
    return set(df["code"].map(_norm_code).dropna().astype(str))


def _dated_csv(prefix: str, d: str) -> Path:
    return LOGS / f"{prefix}_{d}.csv"


def _latest_preview_path(d: str) -> Path:
    return _dated_csv("future_signal_preview", d)


def _feature_path(d: str) -> Path:
    return _dated_csv("future_signal_features", d)


def _candidate_path(d: str) -> Path:
    return _dated_csv("candidates_v41_1", d)


def _load_codes(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        return _code_set(_read_csv(path))
    except Exception:
        return set()


def _reason_for_code(
    code: str,
    preview_codes: Set[str],
    feature_codes: Set[str],
    candidate_codes: Set[str],
    preview_exists: bool,
    feature_exists: bool,
) -> str:
    if not preview_exists:
        return "PREVIEW_FILE_MISSING_FOR_D"
    if code not in preview_codes:
        return "STALE_HISTORY_UNION_NOT_IN_CURRENT_PREVIEW"
    if not feature_exists:
        return "FEATURE_FILE_MISSING_FOR_D"
    if code not in feature_codes:
        if code not in candidate_codes:
            return "HISTORY_CODE_ABSENT_FROM_CURRENT_CANDIDATES_AND_FEATURES"
        return "HISTORY_CODE_ABSENT_FROM_CURRENT_FEATURES"
    return "OK"


def _counter_rows(values: Iterable[str]) -> List[Dict[str, Any]]:
    return [{"reason": k, "rows": int(v)} for k, v in sorted(Counter(values).items())]


def build_audit(history_path: Path, output_json: Path, output_csv: Path, stale_csv: Path) -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    if not history_path.exists():
        payload = {
            "generated_at": generated_at,
            "status": "FAIL",
            "reason": "HISTORY_CSV_MISSING",
            "history_csv": str(history_path),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload

    hist = _read_csv(history_path)
    if hist.empty:
        payload = {
            "generated_at": generated_at,
            "status": "FAIL",
            "reason": "HISTORY_EMPTY",
            "history_csv": str(history_path),
            "policy": _policy(),
        }
        _write_json(output_json, payload)
        return payload

    for col in ("D", "code", "name", "horizon_type", "horizon", "run_id"):
        if col not in hist.columns:
            hist[col] = ""
    hist["D"] = hist["D"].map(_norm_ymd)
    hist["code"] = hist["code"].map(_norm_code)
    hist = hist[(hist["D"] != "") & (hist["code"] != "")].copy()

    day_rows: List[Dict[str, Any]] = []
    stale_rows: List[Dict[str, Any]] = []
    row_reasons: List[str] = []
    code_reasons: List[str] = []

    for d in sorted(hist["D"].dropna().astype(str).unique()):
        day = hist[hist["D"].astype(str) == d].copy()
        history_codes = _code_set(day)
        preview_path = _latest_preview_path(d)
        feature_path = _feature_path(d)
        candidate_path = _candidate_path(d)
        preview_codes = _load_codes(preview_path)
        feature_codes = _load_codes(feature_path)
        candidate_codes = _load_codes(candidate_path)
        feature_exists = feature_path.exists()

        code_reason_map = {
            code: _reason_for_code(code, preview_codes, feature_codes, candidate_codes, preview_path.exists(), feature_exists)
            for code in sorted(history_codes)
        }
        stale_codes = {code for code, reason in code_reason_map.items() if reason != "OK"}
        code_reasons.extend(code_reason_map.values())

        for _, r in day.iterrows():
            code = _norm_code(r.get("code"))
            reason = code_reason_map.get(code, "UNKNOWN")
            row_reasons.append(reason)
            if reason != "OK":
                stale_rows.append(
                    {
                        "D": d,
                        "code": code,
                        "name": r.get("name", ""),
                        "horizon_type": r.get("horizon_type", ""),
                        "horizon": r.get("horizon", ""),
                        "run_id": r.get("run_id", ""),
                        "reason": reason,
                        "preview_csv": str(preview_path) if preview_path.exists() else "",
                        "feature_csv": str(feature_path) if feature_path.exists() else "",
                        "candidate_csv": str(candidate_path) if candidate_path.exists() else "",
                    }
                )

        day_reason_counts = Counter(code_reason_map.values())
        day_rows.append(
            {
                "D": d,
                "history_rows": int(len(day)),
                "history_codes": int(len(history_codes)),
                "preview_exists": bool(preview_path.exists()),
                "preview_codes": int(len(preview_codes)),
                "feature_exists": bool(feature_exists),
                "feature_codes": int(len(feature_codes)),
                "candidate_exists": bool(candidate_path.exists()),
                "candidate_codes": int(len(candidate_codes)),
                "stale_or_unmatched_codes": int(len(stale_codes)),
                "stale_history_union_codes": int(day_reason_counts.get("STALE_HISTORY_UNION_NOT_IN_CURRENT_PREVIEW", 0)),
                "preview_file_missing_codes": int(day_reason_counts.get("PREVIEW_FILE_MISSING_FOR_D", 0)),
                "feature_file_missing_codes": int(day_reason_counts.get("FEATURE_FILE_MISSING_FOR_D", 0)),
                "candidate_feature_absent_codes": int(day_reason_counts.get("HISTORY_CODE_ABSENT_FROM_CURRENT_CANDIDATES_AND_FEATURES", 0)),
                "feature_absent_codes": int(day_reason_counts.get("HISTORY_CODE_ABSENT_FROM_CURRENT_FEATURES", 0)),
            }
        )

    day_df = pd.DataFrame(day_rows)
    stale_df = pd.DataFrame(stale_rows)
    _write_csv(output_csv, day_df)
    _write_csv(stale_csv, stale_df)

    payload = {
        "generated_at": generated_at,
        "status": "PASS",
        "reason": "ok",
        "history_csv": str(history_path),
        "history_rows": int(len(hist)),
        "unique_D": int(hist["D"].nunique()),
        "audit_rows": int(len(day_df)),
        "stale_or_unmatched_rows": int(len(stale_df)),
        "stale_or_unmatched_codes_by_day_sum": int(day_df["stale_or_unmatched_codes"].sum()) if not day_df.empty else 0,
        "row_reason_breakdown": _counter_rows(row_reasons),
        "code_reason_breakdown": _counter_rows(code_reasons),
        "worst_days": day_df.sort_values("stale_or_unmatched_codes", ascending=False).head(10).to_dict("records") if not day_df.empty else [],
        "outputs": {
            "json": str(output_json),
            "csv": str(output_csv),
            "stale_rows_csv": str(stale_csv),
        },
        "policy": _policy(),
    }
    _write_json(output_json, payload)
    return payload


def _policy() -> Dict[str, Any]:
    return {
        "diagnostic_only": True,
        "used_for_trading": False,
        "history_modified": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit future signal history integrity without modifying trading state.")
    ap.add_argument("--history", default=str(DEFAULT_HISTORY_CSV))
    ap.add_argument("--output-json", default=str(LATEST_JSON))
    ap.add_argument("--output-csv", default=str(LATEST_CSV))
    ap.add_argument("--stale-csv", default=str(STALE_CSV))
    args = ap.parse_args()

    payload = build_audit(
        history_path=Path(args.history),
        output_json=Path(args.output_json),
        output_csv=Path(args.output_csv),
        stale_csv=Path(args.stale_csv),
    )
    print(
        f"[FUTURE_SIGNAL_HISTORY_INTEGRITY] status={payload.get('status')} reason={payload.get('reason')} "
        f"history_rows={payload.get('history_rows', 0)} stale_rows={payload.get('stale_or_unmatched_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
