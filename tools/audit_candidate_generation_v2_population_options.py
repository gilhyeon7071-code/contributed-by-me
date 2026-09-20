"""Read-only population-option audit for Candidate-Generation Validation Guard V2."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
LISTING_PATH = ROOT / "_cache" / "krx_listing.csv"
SECTOR_PATH = ROOT / "_cache" / "krx_sector_master_20260109.csv"
DELISTED_PATH = ROOT / "_cache" / "survivorship_delisted_seed.csv"
PREFIX = LOG_DIR / "candidate_generation_v2_population_options"
LABEL_START = pd.Timestamp("2025-06-01")


def _report_module():
    spec = importlib.util.spec_from_file_location("report_backtest", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load report module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _file_meta(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    frame = pd.read_csv(path, dtype=str)
    valid_code = frame.get("code", pd.Series(dtype=str)).fillna("").astype(str).str.fullmatch(r"\d{6}", na=False)
    return {"path": str(path), "exists": True, "rows": int(len(frame)), "columns": list(frame.columns), "valid_six_digit_codes": int(valid_code.sum())}


def main() -> int:
    report = _report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    panel = raw.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.normalize()
    panel["market_source"] = panel["market"].fillna("").astype(str).str.upper().str.strip()
    source_counts = panel["market_source"].replace("", "<EMPTY>").value_counts(dropna=False).rename_axis("market_source").reset_index(name="rows")
    labels = panel.loc[panel["date"].ge(LABEL_START) & panel["market_source"].isin(["KOSPI", "KOSDAQ"]), ["code", "market_source"]].drop_duplicates()
    label_counts = labels.groupby("code", sort=False)["market_source"].nunique()
    unique_codes = label_counts[label_counts.eq(1)].index
    later_map = labels.loc[labels["code"].isin(unique_codes)].drop_duplicates("code").set_index("code")["market_source"]
    legacy_rows = panel["market_source"].isin(["", "KRX"])
    later_covered = legacy_rows & panel["code"].map(later_map).isin(["KOSPI", "KOSDAQ"])
    current_listing = _file_meta(LISTING_PATH)
    sector = _file_meta(SECTOR_PATH)
    delisted = _file_meta(DELISTED_PATH)
    options = [
        {
            "option_id": "DIRECT_SOURCE_KOSPI_KOSDAQ",
            "status": "REJECT",
            "reason": "historical 2021-2024 source labels are empty; direct labels create a time-dependent population exclusion",
            "point_in_time_membership": True,
            "survivorship_risk": "LOW_WHERE_PRESENT",
            "coverage_rows": int(panel["market_source"].isin(["KOSPI", "KOSDAQ"]).sum()),
        },
        {
            "option_id": "LATER_UNIQUE_CODE_MAPPING",
            "status": "RESEARCH_ONLY",
            "reason": "high legacy-row coverage but later labels condition on future observation and can create survivorship bias",
            "point_in_time_membership": False,
            "survivorship_risk": "HIGH",
            "coverage_rows": int(later_covered.sum()),
        },
        {
            "option_id": "ALL_OBSERVED_OHLCV_CODES",
            "status": "REJECT",
            "reason": "exchange membership and security type are not proven; no point-in-time eligibility rule",
            "point_in_time_membership": False,
            "survivorship_risk": "UNKNOWN",
            "coverage_rows": int(len(panel)),
        },
        {
            "option_id": "CURRENT_LISTING_OR_SECTOR_SNAPSHOT",
            "status": "REJECT",
            "reason": "current snapshots are not historical point-in-time membership and cannot repair past eligibility",
            "point_in_time_membership": False,
            "survivorship_risk": "HIGH",
            "coverage_rows": None,
        },
        {
            "option_id": "POINT_IN_TIME_KRX_LISTING_AND_SECURITY_TYPE",
            "status": "REQUIRED_SOURCE",
            "reason": "required to pre-register a historically valid KOSPI/KOSDAQ equity population without later-label survivorship",
            "point_in_time_membership": True,
            "survivorship_risk": "CONTROLLED_IF_AVAILABLE",
            "coverage_rows": None,
        },
    ]
    options_frame = pd.DataFrame(options)
    source_counts.to_csv(f"{PREFIX}_source_label_coverage_latest.csv", index=False, encoding="utf-8-sig")
    options_frame.to_csv(f"{PREFIX}_options_latest.csv", index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "round_type": "PREREGISTRATION_PREPARATION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "price_history_integrity": integrity,
        "label_reference_start": str(LABEL_START.date()),
        "raw_rows": int(len(panel)),
        "later_unique_label_codes": int(len(later_map)),
        "later_ambiguous_label_codes": int(label_counts.gt(1).sum()),
        "legacy_rows": int(legacy_rows.sum()),
        "legacy_rows_covered_by_later_mapping": int(later_covered.sum()),
        "source_label_coverage": source_counts.to_dict(orient="records"),
        "supporting_source_metadata": {"current_listing": current_listing, "current_sector": sector, "delisted_seed": delisted},
        "options": options,
        "conclusion": "NO_ACCEPTABLE_V2_POPULATION_WITH_CURRENT_LOCAL_SOURCES",
    }
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Candidate-Generation V2 Population Options Audit", "", f"- Generated: {payload['generated_at']}", "- Performance calculated: false", "- Candidate selection calculated: false", f"- Conclusion: {payload['conclusion']}", "", "## Options", ""]
    for option in options:
        lines.append(f"- {option['option_id']}: {option['status']} — {option['reason']}")
    Path(f"{PREFIX}_latest.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "conclusion": payload["conclusion"], "options": len(options)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
