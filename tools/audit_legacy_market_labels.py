"""Read-only audit of historical market-label coverage for V2 research splits."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
OUT_CSV = ROOT / "2_Logs" / "legacy_market_label_coverage_latest.csv"
OUT_JSON = ROOT / "2_Logs" / "legacy_market_label_coverage_latest.json"
SPLITS = {
    "TRAIN_202102_202312": (pd.Timestamp("2021-02-01"), pd.Timestamp("2023-12-29")),
    "VAL_202401_202412": (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-30")),
    "OOS_202501_202505": (pd.Timestamp("2025-01-02"), pd.Timestamp("2025-05-30")),
}


def _load_report_module():
    spec = importlib.util.spec_from_file_location("report_backtest", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load report backtest module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    report = _load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["market_label"] = raw["market"].fillna("").astype(str).str.upper().str.strip().replace("", "<EMPTY>")
    rows: list[dict[str, object]] = []
    for split, (start, end) in SPLITS.items():
        frame = raw.loc[raw["date"].between(start, end)]
        for label, group in frame.groupby("market_label", sort=True):
            rows.append(
                {
                    "split": split,
                    "market_label": label,
                    "rows": int(len(group)),
                    "unique_dates": int(group["date"].nunique()),
                    "unique_codes": int(group["code"].nunique()),
                }
            )
    result = pd.DataFrame(rows)
    result.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "price_history_integrity": integrity,
        "rows": result.to_dict(orient="records"),
        "operational_change": False,
        "broker_order": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "rows": len(result)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
