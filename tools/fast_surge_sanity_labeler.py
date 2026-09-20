# -*- coding: utf-8 -*-
"""Shadow-only sanity labels for realtime surge candidates.

This script does not change orders, fills, ledger, stats, gates, locks, or
scores. Missing evidence is treated as UNKNOWN/BLOCK rather than pass.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOTA = Path(r"E:\1_Data")
LOGS = ROOTA / "2_Logs"
PAPER = ROOTA / "paper"

REQUIRED_SURGE_COLUMNS = {"code", "ts", "date", "current_price", "volume_now", "trading_value"}


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        out = float(text)
        if not math.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _norm_code(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    return text.zfill(6)[-6:] if text else ""


def _date8(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    if len(text) >= 8:
        return text[:8]
    return ""


def _parse_iso_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _extract_ymd_from_fills(path: Path) -> str:
    rows = _read_csv_rows(path)
    if not rows:
        return ""
    buy_dates: List[str] = []
    any_dates: List[str] = []
    for row in rows:
        dt = row.get("datetime") or row.get("ts") or row.get("date") or row.get("entry_date") or ""
        ymd = _date8(dt)
        if ymd:
            any_dates.append(ymd)
        side = str(row.get("side") or row.get("action") or row.get("type") or row.get("signal") or "").upper()
        if ymd and side == "BUY":
            buy_dates.append(ymd)
    if buy_dates:
        return sorted(buy_dates)[-1]
    if any_dates:
        return sorted(any_dates)[-1]
    return ""


def _index_by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _norm_code(row.get("code"))
        if code:
            out[code] = row
    return out


def _news_index(rows: Iterable[Dict[str, str]], ymd: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _norm_code(row.get("code"))
        row_ymd = _date8(row.get("date"))
        if code and (not ymd or row_ymd == ymd):
            out[code] = row
    return out


def _derive_ymd_from_rows(rows: Iterable[Dict[str, str]]) -> str:
    dates: List[str] = []
    for row in rows:
        ymd = _date8(row.get("date") or row.get("ts"))
        if ymd:
            dates.append(ymd)
    if not dates:
        return ""
    return sorted(dates)[-1]


def _check_cross_source(row: Dict[str, str], intraday: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    unknown_reasons: List[str] = []
    evidence: Dict[str, Any] = {}
    if not intraday:
        return "BLOCK", ["missing_intraday_ref"], evidence

    cur_px = _to_float(row.get("current_price"))
    ref_px = _to_float(intraday.get("current_price"))
    cur_vol = _to_float(row.get("volume_now"))
    ref_vol = _to_float(intraday.get("volume"))
    cur_value = _to_float(row.get("trading_value"))
    ref_value = _to_float(intraday.get("trading_value"))
    row_ts = _parse_iso_ts(row.get("ts"))
    ref_ts = _parse_iso_ts(intraday.get("ts"))
    ts_lag_sec = (row_ts - ref_ts).total_seconds() if row_ts and ref_ts else 0.0
    no_lob_probe_allowed = _to_bool(row.get("no_lob_probe_allowed"))
    evidence["source_ts_lag_sec"] = round(ts_lag_sec, 3)

    if cur_px <= 0 or ref_px <= 0:
        reasons.append("missing_px_ref")
    else:
        px_bps = abs(cur_px - ref_px) / max(cur_px, 1e-9) * 10000.0
        evidence["px_diff_bps"] = round(px_bps, 6)
        if px_bps > 10.0:
            reasons.append(f"xsrc_px_mismatch:{px_bps:.2f}bps")

    if cur_vol <= 0 or ref_vol <= 0:
        reasons.append("missing_vol_ref")
    else:
        vol_diff = abs(cur_vol - ref_vol) / max(cur_vol, 1.0)
        evidence["vol_diff_ratio"] = round(vol_diff, 6)
        if vol_diff > 0.02:
            reasons.append(f"xsrc_vol_mismatch:{vol_diff:.4f}")

    if cur_value <= 0 or ref_value <= 0:
        reasons.append("missing_value_ref")
    else:
        value_diff = abs(cur_value - ref_value) / max(cur_value, 1.0)
        evidence["value_diff_ratio"] = round(value_diff, 6)
        if value_diff > 0.02:
            if no_lob_probe_allowed and ts_lag_sec > 60.0 and evidence.get("px_diff_bps", 999999.0) <= 10.0:
                unknown_reasons.append(f"xsrc_value_stale_ref:{value_diff:.4f};lag_sec={ts_lag_sec:.0f}")
            else:
                reasons.append(f"xsrc_value_mismatch:{value_diff:.4f}")

    if reasons:
        return "BLOCK", reasons, evidence
    if unknown_reasons:
        return "UNKNOWN", unknown_reasons, evidence
    return ("PASS" if not reasons else "BLOCK"), reasons, evidence


def _check_trade_fill(_: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    return "UNKNOWN", ["missing_trade_tick_evidence"], {}


def _depth_sum(row: Dict[str, str], prefix: str) -> float:
    return sum(_to_float(row.get(f"{prefix}{i}")) for i in range(1, 11))


def _check_orderbook(row: Dict[str, str], lob: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    evidence: Dict[str, Any] = {}
    source = lob or row
    lob_available = _to_bool(source.get("lob_available"))
    lob_status = str(source.get("lob_status") or "").upper()
    if not lob_available or lob_status in {"", "NO_LOB"}:
        if _to_bool(row.get("no_lob_probe_allowed")):
            return "UNKNOWN", ["no_lob_probe_policy_accepts_missing_lob_for_paper_probe"], {"lob_status": lob_status or "MISSING"}
        return "BLOCK", ["missing_lob"], {"lob_status": lob_status or "MISSING"}

    spread_bps = _to_float(source.get("spread_bps"))
    risk_score = _to_float(source.get("orderflow_risk_score"))
    tag = str(source.get("orderflow_tag") or "").upper()
    bid_sum = _depth_sum(source, "bidq")
    ask_sum = _depth_sum(source, "askq")
    bid_ask_ratio = bid_sum / max(ask_sum, 1.0) if ask_sum > 0 else 0.0
    evidence.update(
        {
            "lob_status": lob_status,
            "spread_bps": round(spread_bps, 6),
            "orderflow_risk_score": round(risk_score, 6),
            "orderflow_tag": tag,
            "bid_ask_depth_ratio": round(bid_ask_ratio, 6),
        }
    )
    if spread_bps > 150.0:
        reasons.append(f"wide_spread:{spread_bps:.1f}bps")
    if bid_ask_ratio < 0.6:
        reasons.append(f"weak_bid_depth:{bid_ask_ratio:.3f}")
    if risk_score >= 0.6 or tag in {"CAUTION", "PAUSE"}:
        reasons.append(f"orderflow_risk:{risk_score:.3f}:{tag or 'NA'}")
    return ("PASS" if not reasons else "BLOCK"), reasons, evidence


def _check_news(row: Dict[str, str], news: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    evidence: Dict[str, Any] = {
        "surge_news_score": _to_float(row.get("news_score")),
        "surge_news_implication_rows": _to_float(row.get("news_implication_rows")),
    }
    if not news:
        return "UNKNOWN", ["missing_news_candidate_row"], evidence
    article_count = _to_float(news.get("news_article_count"))
    freshest_age = _to_float(news.get("news_freshest_age_hours"), default=-1.0)
    evidence.update(
        {
            "news_article_count": article_count,
            "news_freshest_age_hours": freshest_age,
            "news_source": str(news.get("news_source") or ""),
        }
    )
    if article_count <= 0:
        return "UNKNOWN", ["no_news_article"], evidence
    if freshest_age < 0:
        return "UNKNOWN", ["missing_news_age"], evidence
    if freshest_age > (5.0 / 60.0):
        return "UNKNOWN", [f"news_not_timestamp_aligned:{freshest_age:.3f}h"], evidence
    return "PASS", [], evidence


def _check_peer_cluster(_: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    return "UNKNOWN", ["missing_peer_cluster_evidence"], {}


def _check_reversion(row: Dict[str, str]) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    change_pct = _to_float(row.get("change_pct"))
    drawdown_pct = _to_float(row.get("intraday_high_drawdown_pct"))
    atr_cap = _to_float(row.get("atr_entry_cap_pct"))
    relaxations = {
        part.strip().upper()
        for part in str(row.get("paper_policy_relaxations") or "").split("|")
        if part.strip()
    }
    evidence = {
        "change_pct": change_pct,
        "intraday_high_drawdown_pct": drawdown_pct,
        "atr_entry_cap_pct": atr_cap,
        "paper_policy_relaxations": sorted(relaxations),
    }
    if atr_cap > 0 and change_pct > atr_cap and "ENTRY_ATR_CAP" not in relaxations:
        reasons.append(f"far_above_atr_cap:{change_pct:.4f}>{atr_cap:.4f}")
    if drawdown_pct <= -0.025 and "HIGH_REJECTION_ENTRY_BLOCK" not in relaxations:
        reasons.append(f"high_rejection:{drawdown_pct:.4f}")
    if not reasons:
        reasons.append("missing_vwap_rsi_reversion_evidence")
        return "UNKNOWN", reasons, evidence
    return "BLOCK", reasons, evidence


def _overall_status(checks: Dict[str, Dict[str, Any]]) -> str:
    statuses = [str(v.get("status") or "") for v in checks.values()]
    if any(s == "BLOCK" for s in statuses):
        return "BLOCK"
    if any(s == "UNKNOWN" for s in statuses):
        return "UNKNOWN"
    return "PASS"


def _label_row(
    row: Dict[str, str],
    intraday_by_code: Dict[str, Dict[str, str]],
    lob_by_code: Dict[str, Dict[str, str]],
    news_by_code: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    code = _norm_code(row.get("code"))
    checks: Dict[str, Dict[str, Any]] = {}
    for name, result in [
        ("cross_source", _check_cross_source(row, intraday_by_code.get(code, {}))),
        ("trade_fill", _check_trade_fill(row)),
        ("orderbook", _check_orderbook(row, lob_by_code.get(code, {}))),
        ("news_alignment", _check_news(row, news_by_code.get(code, {}))),
        ("peer_cluster", _check_peer_cluster(row)),
        ("short_reversion", _check_reversion(row)),
    ]:
        status, reasons, evidence = result
        checks[name] = {"status": status, "reasons": reasons, "evidence": evidence}

    status = _overall_status(checks)
    reasons: List[str] = []
    for name, check in checks.items():
        for reason in check["reasons"]:
            reasons.append(f"{name}:{reason}")
    label = dict(row)
    label.update(
        {
            "surge_sanity_status": status,
            "surge_sanity_actionable": 1 if status == "PASS" else 0,
            "surge_sanity_reasons": "|".join(reasons),
            "surge_sanity_reasons_json": json.dumps(reasons, ensure_ascii=False),
            "surge_sanity_checks_json": json.dumps(checks, ensure_ascii=False, sort_keys=True),
        }
    )
    evidence = {
        "code": code,
        "date": _date8(row.get("date")),
        "surge_type": row.get("surge_type", ""),
        "status": status,
        "reasons": reasons,
        "checks": checks,
    }
    return label, evidence


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Shadow-only surge sanity labeler")
    ap.add_argument("--ymd", default="", help="Target yyyymmdd. Defaults to D rule from paper/fills.csv.")
    ap.add_argument("--surge-csv", default=str(LOGS / "surge_realtime_latest.csv"))
    ap.add_argument("--lob-csv", default=str(LOGS / "surge_lob_latest.csv"))
    ap.add_argument("--intraday-csv", default=str(LOGS / "intraday_prices_latest.csv"))
    ap.add_argument("--news-csv", default=str(LOGS / "news_candidates_latest.csv"))
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--evidence-dir", default="")
    args = ap.parse_args(argv)

    surge_path = Path(args.surge_csv)
    rows = _read_csv_rows(surge_path)
    if not rows:
        raise SystemExit(f"missing_or_empty_surge_csv:{surge_path}")
    missing = sorted(REQUIRED_SURGE_COLUMNS - set(rows[0].keys()))
    if missing:
        raise SystemExit(f"surge_csv_missing_columns:{','.join(missing)}")
    ymd = (
        _date8(args.ymd)
        or _derive_ymd_from_rows(rows)
        or _extract_ymd_from_fills(PAPER / "fills.csv")
        or datetime.now().strftime("%Y%m%d")
    )

    intraday_by_code = _index_by_code(_read_csv_rows(Path(args.intraday_csv)))
    lob_by_code = _index_by_code(_read_csv_rows(Path(args.lob_csv)))
    news_by_code = _news_index(_read_csv_rows(Path(args.news_csv)), ymd)

    labeled_rows: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, Any]] = []
    for row in rows:
        labeled, evidence = _label_row(row, intraday_by_code, lob_by_code, news_by_code)
        labeled_rows.append(labeled)
        evidence_rows.append(evidence)

    out_csv = Path(args.out_csv) if args.out_csv else LOGS / f"surge_sanity_labeled_{ymd}.csv"
    out_json = Path(args.out_json) if args.out_json else LOGS / f"surge_sanity_labeled_{ymd}.json"
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else LOGS / "fast_flagger_evidence" / ymd

    fieldnames = list(rows[0].keys()) + [
        "surge_sanity_status",
        "surge_sanity_actionable",
        "surge_sanity_reasons",
        "surge_sanity_reasons_json",
        "surge_sanity_checks_json",
    ]
    _write_csv_rows(out_csv, labeled_rows, fieldnames)
    latest_csv = LOGS / "surge_sanity_labeled_latest.csv"
    _write_csv_rows(latest_csv, labeled_rows, fieldnames)

    counts: Dict[str, int] = {}
    for row in labeled_rows:
        status = str(row.get("surge_sanity_status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "shadow_only": True,
        "trading_approved": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
        "ymd": ymd,
        "input": str(surge_path),
        "rows": len(labeled_rows),
        "counts": counts,
        "actionable": int(counts.get("PASS", 0)),
        "outputs": {
            "csv": str(out_csv),
            "csv_latest": str(latest_csv),
            "json": str(out_json),
            "json_latest": str(LOGS / "surge_sanity_labeled_latest.json"),
            "evidence_dir": str(evidence_dir),
        },
    }
    _write_json(out_json, summary)
    _write_json(LOGS / "surge_sanity_labeled_latest.json", summary)

    for evidence in evidence_rows:
        code = str(evidence.get("code") or "UNKNOWN")
        row_date = str(evidence.get("date") or ymd)
        _write_json(evidence_dir / f"{row_date}_{code}.json", evidence)

    print(
        "[FINAL] surge_sanity shadow labeled -> "
        f"{out_csv} rows={len(labeled_rows)} counts={counts} actionable={summary['actionable']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
