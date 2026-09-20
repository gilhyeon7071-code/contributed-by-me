from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import logging

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "_cache"
LOG_DIR = BASE_DIR / "2_Logs"

DEFAULT_INPUT_CSV = CACHE_DIR / "dart_fundamental_latest.csv"
DEFAULT_META_JSON = CACHE_DIR / "dart_fundamental_meta.json"
DEFAULT_OUTPUT_JSON = LOG_DIR / "disclosure_risk_latest.json"




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
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in {"", "-", "--", "NA", "N/A", "nan", "None"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _norm_code6(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s.zfill(6) if s else ""


def _risk_label(score_01: float) -> str:
    s = float(score_01)
    if s >= 0.67:
        return "POSITIVE"
    if s < 0.34:
        return "NEGATIVE"
    return "NEUTRAL"


def _score_row(row: pd.Series) -> Dict[str, Any]:
    comps: List[float] = []
    failed: List[str] = []

    op = _safe_float(row.get("operating_profit"))
    if op is not None:
        good = op > 0
        comps.append(1.0 if good else 0.0)
        if not good:
            failed.append("operating_profit_non_positive")

    roe = _safe_float(row.get("ROE"))
    if roe is not None:
        good = roe > 0
        comps.append(1.0 if good else 0.0)
        if not good:
            failed.append("roe_non_positive")

    npm = _safe_float(row.get("NPM"))
    if npm is not None:
        good = npm > 0
        comps.append(1.0 if good else 0.0)
        if not good:
            failed.append("npm_non_positive")

    debt = _safe_float(row.get("debt_ratio"))
    if debt is not None:
        good = debt <= 200.0
        comps.append(1.0 if good else 0.0)
        if not good:
            failed.append("debt_ratio_above_200")

    current_ratio = _safe_float(row.get("current_ratio"))
    if current_ratio is not None:
        good = current_ratio >= 100.0
        comps.append(1.0 if good else 0.0)
        if not good:
            failed.append("current_ratio_below_100")

    score = (sum(comps) / len(comps)) if comps else 0.50
    signal = _risk_label(score)
    is_blocked = signal == "NEGATIVE"

    return {
        "code": _norm_code6(row.get("code")),
        "corp_name": str(row.get("corp_name") or "").strip(),
        "as_of_ymd": str(row.get("as_of_ymd") or "").strip(),
        "disclosure_score_01": round(float(score), 6),
        "disclosure_signal": signal,
        "failed_checks": failed,
        "available_metrics": int(len(comps)),
        "is_blocked": is_blocked,
        "block_reason": "disclosure_signal_negative" if is_blocked else "",
    }


def build_disclosure_risk_log(input_csv: Path, meta_path: Path, output_path: Path) -> Dict[str, Any]:
    df = _read_csv(input_csv) if input_csv.exists() else pd.DataFrame()
    meta = _read_json(meta_path) if meta_path.exists() else {}

    if not df.empty and "code" in df.columns:
        df = df.copy()
        df["code"] = df["code"].map(_norm_code6)
        df = df[df["code"] != ""].reset_index(drop=True)

    items = [_score_row(row) for _, row in df.iterrows()] if not df.empty else []

    negative_items = [x for x in items if x["disclosure_signal"] == "NEGATIVE"]
    neutral_items = [x for x in items if x["disclosure_signal"] == "NEUTRAL"]
    positive_items = [x for x in items if x["disclosure_signal"] == "POSITIVE"]
    blocked_items = [x for x in items if bool(x.get("is_blocked"))]

    payload: Dict[str, Any] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "as_of_ymd": str(meta.get("as_of_ymd") or (items[0]["as_of_ymd"] if items else "")),
        "source_csv": str(input_csv),
        "source_meta": str(meta_path),
        "requested_codes": int(meta.get("requested_codes") or 0),
        "mapped_codes": int(meta.get("mapped_codes") or 0),
        "rows_scored": int(len(items)),
        "negative_count": int(len(negative_items)),
        "neutral_count": int(len(neutral_items)),
        "positive_count": int(len(positive_items)),
        "error_count": int(meta.get("errors") or 0),
        "block_policy": "NEGATIVE_SIGNAL_BLOCK",
        "block_policy_label": "NEGATIVE signal block",
        "blocked_count": int(len(blocked_items)),
        "items": items,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build disclosure risk log from DART fundamentals")
    ap.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV))
    ap.add_argument("--meta-json", default=str(DEFAULT_META_JSON))
    ap.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    payload = build_disclosure_risk_log(
        input_csv=Path(args.input_csv),
        meta_path=Path(args.meta_json),
        output_path=Path(args.output_json),
    )
    _log_print(
        "[DISCLOSURE_RISK] rows={0} negative={1} neutral={2} positive={3} blocked={4} output={5}".format(
            payload.get("rows_scored", 0),
            payload.get("negative_count", 0),
            payload.get("neutral_count", 0),
            payload.get("positive_count", 0),
            payload.get("blocked_count", 0),
            args.output_json,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

