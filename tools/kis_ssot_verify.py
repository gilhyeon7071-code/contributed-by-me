from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_ssot_verify")


def _pick_latest(pattern: str) -> Optional[Path]:
    matches = sorted(PAPER_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _read_csv(path: Optional[Path]) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _nonempty_set(df: pd.DataFrame, cols: Iterable[str]) -> Set[str]:
    vals: Set[str] = set()
    if df.empty:
        return vals
    for col in cols:
        if col not in df.columns:
            continue
        series = df[col].astype(str).str.strip()
        vals.update(x for x in series.tolist() if x)
    return vals


def _coverage(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    series = df[col].astype(str).str.strip()
    return round(float((series != "").mean()), 4)


def _status(ok: bool, partial: bool = False) -> str:
    if ok:
        return "PASS"
    if partial:
        return "PARTIAL"
    return "WARN"


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="Verify submit -> fills -> live_fills -> ledger SSOT lineage")
    ap.add_argument("--submit-csv", default="", help="broker submit csv; default latest orders_*_broker_submit_*.csv")
    ap.add_argument("--fills-csv", default="", help="fills api csv; default latest kis_fills_api_*.csv")
    ap.add_argument("--live-fills", default=str(Path(__file__).resolve().parents[1].parent / "vibe" / "buffett" / "data" / "live" / "live_fills.csv"))
    ap.add_argument("--ledger-csv", default=str(Path(__file__).resolve().parents[1].parent / "vibe" / "buffett" / "data" / "ledger" / "paper_fills_ledger.csv"))
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    submit_path = Path(args.submit_csv) if str(args.submit_csv).strip() else _pick_latest("orders_*_broker_submit_*.csv")
    fills_path = Path(args.fills_csv) if str(args.fills_csv).strip() else _pick_latest("kis_fills_api_*.csv")
    live_path = Path(args.live_fills)
    ledger_path = Path(args.ledger_csv)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json) if str(args.out_json).strip() else (LOG_DIR / "kis_ssot_verify_latest.json")

    submit_df = _read_csv(submit_path)
    fills_df = _read_csv(fills_path)
    live_df = _read_csv(live_path)
    ledger_df = _read_csv(ledger_path)

    submit_effective_df = submit_df.copy()
    if not submit_effective_df.empty and "ord_no" in submit_effective_df.columns:
        submit_effective_df = submit_effective_df[submit_effective_df["ord_no"].astype(str).str.strip() != ""].copy()

    submit_order_nos = _nonempty_set(submit_effective_df, ["ord_no"])
    fills_order_nos = _nonempty_set(fills_df, ["order_no"])
    live_order_ids = _nonempty_set(live_df, ["order_id"])
    ledger_order_ids = _nonempty_set(ledger_df, ["order_id", "source_order_id", "entry_order_id"])

    fills_vs_submit = round(len(submit_order_nos & fills_order_nos) / len(submit_order_nos), 4) if submit_order_nos else 0.0
    live_vs_ledger = round(len(live_order_ids & ledger_order_ids) / len(live_order_ids), 4) if live_order_ids else 0.0

    summary: Dict[str, object] = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "paths": {
            "submit_csv": str(submit_path) if submit_path else "",
            "fills_csv": str(fills_path) if fills_path else "",
            "live_fills": str(live_path),
            "ledger_csv": str(ledger_path),
        },
        "counts": {
            "submit_rows": int(len(submit_df)),
            "submit_effective_rows": int(len(submit_effective_df)),
            "fills_rows": int(len(fills_df)),
            "live_rows": int(len(live_df)),
            "ledger_rows": int(len(ledger_df)),
        },
        "coverage": {
            "submit_order_no": _coverage(submit_effective_df, "ord_no"),
            "fills_order_no": _coverage(fills_df, "order_no"),
            "live_order_id": _coverage(live_df, "order_id"),
            "live_intent_id": _coverage(live_df, "intent_id"),
            "live_trace_id": _coverage(live_df, "trace_id"),
            "ledger_order_id": _coverage(ledger_df, "order_id"),
            "ledger_source_order_id": _coverage(ledger_df, "source_order_id"),
            "ledger_entry_order_id": _coverage(ledger_df, "entry_order_id"),
            "ledger_entry_intent_id": _coverage(ledger_df, "entry_intent_id"),
            "ledger_entry_trace_id": _coverage(ledger_df, "entry_trace_id"),
            "fills_vs_submit_order_no": fills_vs_submit,
            "live_vs_ledger_order_id": live_vs_ledger,
        },
    }

    checks = {
        "submit_to_fills": {
            "status": _status(fills_vs_submit >= 0.95, partial=bool(submit_order_nos or fills_order_nos)),
            "message": "order_no lineage from broker submit to fills api",
        },
        "live_to_ledger": {
            "status": _status(live_vs_ledger >= 0.95, partial=bool(live_order_ids or ledger_order_ids)),
            "message": "order_id lineage from live fills to ledger",
        },
        "live_lineage_columns": {
            "status": _status(
                min(
                    summary["coverage"]["live_order_id"],  # type: ignore[index]
                    summary["coverage"]["live_intent_id"],  # type: ignore[index]
                    summary["coverage"]["live_trace_id"],  # type: ignore[index]
                )
                >= 0.95,
                partial=not live_df.empty,
            ),
            "message": "live fills carries order_id / intent_id / trace_id",
        },
        "ledger_lineage_columns": {
            "status": _status(
                min(
                    summary["coverage"]["ledger_order_id"],  # type: ignore[index]
                    summary["coverage"]["ledger_entry_order_id"],  # type: ignore[index]
                    summary["coverage"]["ledger_entry_intent_id"],  # type: ignore[index]
                    summary["coverage"]["ledger_entry_trace_id"],  # type: ignore[index]
                )
                >= 0.95,
                partial=not ledger_df.empty,
            ),
            "message": "ledger carries order and entry lineage",
        },
    }

    warnings = []
    if not submit_order_nos:
        warnings.append("effective broker submit order_no evidence is empty")
    if fills_df.empty:
        warnings.append("fills api evidence is empty")
    if fills_vs_submit < 0.95 and submit_order_nos:
        warnings.append("submit -> fills coverage is below target")
    if live_vs_ledger < 0.95 and live_order_ids:
        warnings.append("live -> ledger order lineage is below target")

    summary["checks"] = checks
    summary["warnings"] = warnings
    summary["overall_status"] = "PASS" if all(v["status"] == "PASS" for v in checks.values()) else "PARTIAL"

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[OK] ssot_verify=%s", out_json)
    logger.info("[OK] overall_status=%s", summary["overall_status"])
    if warnings:
        logger.warning("warnings=%s", warnings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

