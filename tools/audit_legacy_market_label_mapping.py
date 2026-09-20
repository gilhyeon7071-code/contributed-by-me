"""Read-only coverage audit for mapping empty historical market labels by later labeled codes."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
OUT_CSV = ROOT / "2_Logs" / "legacy_market_label_mapping_coverage_latest.csv"
OUT_JSON = ROOT / "2_Logs" / "legacy_market_label_mapping_coverage_latest.json"
SPLITS = {
    "TRAIN_202102_202312": (pd.Timestamp("2021-02-01"), pd.Timestamp("2023-12-29")),
    "VAL_202401_202412": (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-30")),
    "OOS_202501_202505": (pd.Timestamp("2025-01-02"), pd.Timestamp("2025-05-30")),
}
LABEL_START = pd.Timestamp("2025-06-01")


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
    raw["market_label"] = raw["market"].fillna("").astype(str).str.upper().str.strip()
    labeled = raw.loc[
        raw["date"].ge(LABEL_START) & raw["market_label"].isin(["KOSPI", "KOSDAQ"]), ["code", "market_label"]
    ].drop_duplicates()
    label_count = labeled.groupby("code", sort=False)["market_label"].nunique()
    label_map = labeled.loc[labeled["code"].isin(label_count[label_count.eq(1)].index)].drop_duplicates("code").set_index("code")["market_label"]
    rows: list[dict[str, object]] = []
    for split, (start, end) in SPLITS.items():
        historical = raw.loc[raw["date"].between(start, end)].copy()
        codes = historical["code"].drop_duplicates()
        mapped = codes.isin(label_map.index)
        ambiguous = codes.isin(label_count[label_count.gt(1)].index)
        row_mapped = historical["code"].isin(label_map.index)
        rows.append(
            {
                "split": split,
                "historical_rows": int(len(historical)),
                "historical_codes": int(len(codes)),
                "uniquely_mapped_codes": int(mapped.sum()),
                "ambiguous_codes": int(ambiguous.sum()),
                "unmapped_codes": int((~mapped & ~ambiguous).sum()),
                "mapped_row_coverage": float(row_mapped.mean()) if len(historical) else None,
            }
        )
    result = pd.DataFrame(rows)
    result.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "label_reference_start": str(LABEL_START.date()),
        "labeled_codes": int(len(label_count)),
        "uniquely_labeled_codes": int(len(label_map)),
        "ambiguous_labeled_codes": int(label_count.gt(1).sum()),
        "price_history_integrity": integrity,
        "rows": result.to_dict(orient="records"),
        "operational_change": False,
        "broker_order": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "splits": len(result)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
