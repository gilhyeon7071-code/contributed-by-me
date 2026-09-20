from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd
import logging




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _date8_series(df: pd.DataFrame) -> pd.Series:
    for col in ("date_yyyymmdd", "as_of_ymd", "date8", "date"):
        if col not in df.columns:
            continue
        raw = df[col]
        if col == "date":
            parsed = pd.to_datetime(raw, errors="coerce")
            return parsed.dt.strftime("%Y%m%d")
        s = raw.astype(str).str.replace(r"\.0$", "", regex=True).str.replace(r"[^0-9]", "", regex=True)
        s = s.where(s.str.len() >= 8, "")
        return s.str[:8]
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _max_date8(df: pd.DataFrame) -> str:
    s = _date8_series(df)
    s = s[s.astype(str).str.len() == 8]
    if s.empty:
        return ""
    return str(s.max())


def _score_bounds(stage: str) -> tuple[float, float]:
    if stage == "final":
        return (0.0, 1.0)
    return (-1.0, 1.0)


def _write_status(logs: Path, payload: dict) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    latest = logs / f"signal_contract_{payload.get('stage', 'unknown')}_status_latest.json"
    stamped = logs / f"signal_contract_{payload.get('stage', 'unknown')}_status_{payload.get('checked_at', '').replace(':', '').replace('-', '').replace('T', '_')}.json"
    latest.write_text(text, encoding="utf-8")
    if payload.get("checked_at"):
        stamped.write_text(text, encoding="utf-8")


def _read_json(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {}


def _news_constant_zero_allowed(logs: Path) -> tuple[bool, str]:
    score_status = _read_json(logs / "news_score_status_latest.json")
    collect_status = _read_json(logs / "news_collect_status_latest.json")
    quality = str(score_status.get("quality") or "").strip().upper()
    collect_reason = str(collect_status.get("reason") or "").strip()
    weekend_skip = bool(collect_status.get("weekend_skip"))
    if quality == "WARN" and weekend_skip and collect_reason == "weekend_guard_skip":
        return True, "news_score_constant_zero_allowed_weekend_warn"
    return False, ""


def _check(stage: str, root: Path) -> int:
    logs = root / "2_Logs"
    allowed_row_expansion = 0
    allowed_row_expansion_reason = ""

    if stage == "sector":
        inp = logs / "candidates_latest_data.filtered.csv"
        out = logs / "candidates_latest_data.with_sector_score.csv"
        req_col = "sector_score"
    elif stage == "news":
        inp = logs / "candidates_latest_data.with_sector_score.csv"
        out = logs / "candidates_latest_data.with_news_score.csv"
        req_col = "news_score"
    elif stage == "final":
        policy_inp = logs / "candidates_latest_data.with_policy_score.csv"
        legacy_inp = logs / "candidates_latest_data.with_news_score.csv"
        status_inp = None
        status_files = sorted(logs.glob("final_score_merge_status_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        status_path = status_files[0] if status_files else (logs / "final_score_merge_status_latest.json")
        if status_path.exists():
            try:
                status_obj = json.loads(status_path.read_text(encoding="utf-8"))
                raw_input = str(status_obj.get("input") or "").strip()
                if raw_input:
                    status_inp = Path(raw_input)
                news_candidates = status_obj.get("news_candidates") if isinstance(status_obj.get("news_candidates"), dict) else {}
                added_news_only = int(float(news_candidates.get("added_news_only") or 0))
                if added_news_only > 0:
                    allowed_row_expansion = added_news_only
                    allowed_row_expansion_reason = "final_merge_news_only_append"
            except Exception:
                status_inp = None
        inp = status_inp if status_inp is not None and status_inp.exists() else (policy_inp if policy_inp.exists() else legacy_inp)
        out = logs / "candidates_latest_data.with_final_score.csv"
        req_col = "final_score"
    else:
        _log_print(f"[CONTRACT] unknown stage: {stage}")
        return 2

    if not inp.exists() or not out.exists():
        _log_print(f"[CONTRACT] {stage} missing file inp={inp.exists()} out={out.exists()} in={inp} out={out}")
        _write_status(logs, {
            "stage": stage,
            "checked_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
            "status": "FAIL",
            "reason": "missing_file",
            "input": str(inp),
            "output": str(out),
            "input_exists": inp.exists(),
            "output_exists": out.exists(),
        })
        return 2

    try:
        df_in = _read_csv(inp)
        df_out = _read_csv(out)
    except Exception as e:
        _log_print(f"[CONTRACT] {stage} read_fail: {type(e).__name__}: {e}")
        _write_status(logs, {
            "stage": stage,
            "checked_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
            "status": "FAIL",
            "reason": f"read_fail:{type(e).__name__}",
            "input": str(inp),
            "output": str(out),
        })
        return 2

    in_rows = int(len(df_in))
    out_rows = int(len(df_out))
    reasons = []

    if req_col not in df_out.columns:
        _log_print(f"[CONTRACT] {stage} missing required column: {req_col}")
        _write_status(logs, {
            "stage": stage,
            "checked_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
            "status": "FAIL",
            "reason": f"missing_required_column:{req_col}",
            "input": str(inp),
            "output": str(out),
            "in_rows": in_rows,
            "out_rows": out_rows,
        })
        return 2

    row_count_allowed = bool(allowed_row_expansion > 0 and out_rows == in_rows + allowed_row_expansion)
    if stage == "final" and in_rows == 0 and out_rows == 0:
        status = {
            "stage": stage,
            "checked_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
            "status": "PASS",
            "reason": "empty_input_output_allowed",
            "input": str(inp),
            "output": str(out),
            "required_column": req_col,
            "in_rows": in_rows,
            "out_rows": out_rows,
            "allowed_row_expansion": int(allowed_row_expansion),
            "allowed_row_expansion_reason": allowed_row_expansion_reason,
            "input_max_date8": "",
            "output_max_date8": "",
            "score": {
                "non_null": 0,
                "nan_rate": 0.0,
                "min": None,
                "max": None,
                "unique_values": 0,
                "allowed_min": _score_bounds(stage)[0],
                "allowed_max": _score_bounds(stage)[1],
            },
            "allowance": "final_empty_candidate_contract",
        }
        _write_status(logs, status)
        _log_print(
            f"[CONTRACT] {stage} in_rows=0 out_rows=0 col={req_col} "
            "ok=True reason=empty_input_output_allowed"
        )
        return 0

    if in_rows != out_rows and not row_count_allowed:
        reasons.append(f"row_count_mismatch:{in_rows}->{out_rows}")
    if out_rows <= 0:
        reasons.append("empty_output")

    input_max_date8 = _max_date8(df_in)
    output_max_date8 = _max_date8(df_out)
    if input_max_date8 and output_max_date8 and input_max_date8 != output_max_date8:
        reasons.append(f"max_date_mismatch:{input_max_date8}->{output_max_date8}")
    if not output_max_date8:
        reasons.append("output_date_missing")

    score = pd.to_numeric(df_out[req_col], errors="coerce")
    non_null = int(score.notna().sum())
    nan_rate = float(score.isna().mean()) if out_rows else 1.0
    min_value = float(score.min()) if non_null else None
    max_value = float(score.max()) if non_null else None
    unique_values = int(score.nunique(dropna=True))
    if nan_rate > 0.05:
        reasons.append(f"{req_col}_nan_rate_high:{nan_rate:.4f}")

    low, high = _score_bounds(stage)
    finite = score.dropna()
    finite = finite[finite.map(lambda v: math.isfinite(float(v)))]
    if len(finite) != non_null:
        reasons.append(f"{req_col}_non_finite")
    if non_null and (float(score.min()) < low or float(score.max()) > high):
        reasons.append(f"{req_col}_out_of_range:{min_value}->{max_value}:allowed={low}->{high}")
    constant_distribution_allowed = False
    constant_distribution_allow_reason = ""
    if out_rows >= 3 and unique_values <= 1:
        if stage == "news":
            constant_distribution_allowed, constant_distribution_allow_reason = _news_constant_zero_allowed(logs)
        if not constant_distribution_allowed:
            reasons.append(f"{req_col}_constant_distribution:{unique_values}")

    ok = len(reasons) == 0
    status = {
        "stage": stage,
        "checked_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
        "status": "PASS" if ok else "FAIL",
        "reason": "ok" if ok else "|".join(reasons),
        "input": str(inp),
        "output": str(out),
        "required_column": req_col,
        "in_rows": in_rows,
        "out_rows": out_rows,
        "allowed_row_expansion": int(allowed_row_expansion),
        "allowed_row_expansion_reason": allowed_row_expansion_reason,
        "input_max_date8": input_max_date8,
        "output_max_date8": output_max_date8,
        "score": {
            "non_null": non_null,
            "nan_rate": round(nan_rate, 6),
            "min": min_value,
            "max": max_value,
            "unique_values": unique_values,
            "allowed_min": low,
            "allowed_max": high,
        },
    }
    if constant_distribution_allowed:
        status["allowance"] = constant_distribution_allow_reason
    _write_status(logs, status)
    _log_print(
        f"[CONTRACT] {stage} in_rows={in_rows} out_rows={out_rows} col={req_col} "
        f"input_max_date8={input_max_date8 or 'NA'} output_max_date8={output_max_date8 or 'NA'} "
        f"nan_rate={nan_rate:.4f} range={min_value}->{max_value} ok={ok} reason={status['reason']}"
    )
    return 0 if ok else 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["sector", "news", "final"])
    ap.add_argument("--root", default=str(Path(__file__).resolve().parent.parent))
    ns = ap.parse_args()
    return _check(ns.stage, Path(ns.root))


if __name__ == "__main__":
    raise SystemExit(main())
