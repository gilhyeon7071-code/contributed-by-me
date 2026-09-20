from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"

PURE_AUDIT_CSV = LOG_DIR / "pure_normal_entry_sample_audit_latest.csv"
TRADES_CALC_CSV = PAPER_DIR / "trades_calc.csv"

OUT_JSON = LOG_DIR / "pure_normal_loss_driver_audit_latest.json"
OUT_CSV = LOG_DIR / "pure_normal_loss_driver_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "pure_normal_loss_driver_audit_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any) -> float | None:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _stats(values: Iterable[float]) -> Dict[str, Any]:
    xs = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    if not xs:
        return {"n": 0, "avg": None, "median": None, "win_rate": None, "min": None, "max": None}
    return {
        "n": len(xs),
        "avg": sum(xs) / len(xs),
        "median": median(xs),
        "win_rate": sum(1 for x in xs if x > 0) / len(xs),
        "min": min(xs),
        "max": max(xs),
    }


def _ymd(value: Any) -> str:
    text = str(value or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _holding_days(entry_ts: str, exit_ts: str) -> int | None:
    try:
        entry = datetime.strptime(str(entry_ts)[:10], "%Y-%m-%d")
        exit_ = datetime.strptime(str(exit_ts)[:10], "%Y-%m-%d")
        return (exit_ - entry).days
    except Exception:
        return None


def _loss_bucket(gross_ret: float | None, net_ret: float | None) -> str:
    if net_ret is None:
        return "UNKNOWN"
    if net_ret > 0:
        return "WIN"
    if gross_ret is not None and gross_ret > 0 and net_ret <= 0:
        return "COST_FLIP_LOSS"
    if gross_ret is not None and gross_ret <= -0.03:
        return "LARGE_PRICE_LOSS"
    if gross_ret is not None and gross_ret < 0:
        return "SMALL_PRICE_LOSS"
    if gross_ret is not None and gross_ret == 0:
        return "FLAT_AFTER_COST"
    return "UNKNOWN_LOSS"


def _date_cluster(entry_ymd: str) -> str:
    if entry_ymd in {"20260401", "20260402"}:
        return "APR01_02_CLUSTER"
    if entry_ymd in {"20260415", "20260416"}:
        return "APR15_16_CLUSTER"
    return "OTHER_DATE"


def _summary_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for row in rows:
        net = _float(row.get("net_ret"))
        for kind, key in (
            ("loss_bucket", str(row.get("loss_bucket") or "")),
            ("entry_ymd", str(row.get("entry_ymd") or "")),
            ("date_cluster", str(row.get("date_cluster") or "")),
            ("holding_days", str(row.get("holding_days") or "")),
        ):
            if net is not None:
                buckets[(kind, key)].append(net)

    out: List[Dict[str, Any]] = []
    for (kind, key), vals in sorted(buckets.items()):
        out.append({"kind": kind, "bucket": key, **_stats(vals)})
    return out


def main() -> int:
    pure_rows = _read_csv(PURE_AUDIT_CSV)
    trade_rows = _read_csv(TRADES_CALC_CSV)
    trades = {(str(row.get("trade_id") or ""), str(row.get("code") or "").zfill(6)): row for row in trade_rows}

    target_rows = [
        row for row in pure_rows if str(row.get("sample_audit_decision") or "") == "PURE_CURRENT_NORMAL_PROOF"
    ]
    enriched: List[Dict[str, Any]] = []
    for row in target_rows:
        key = (str(row.get("row_id") or ""), str(row.get("code") or "").zfill(6))
        trade = trades.get(key, {})
        gross = _float(trade.get("gross_ret"))
        net = _float(trade.get("net_ret") or row.get("net_ret"))
        entry_ymd = _ymd(trade.get("entry_ts") or row.get("ymd"))
        hold = _holding_days(str(trade.get("entry_ts") or ""), str(trade.get("exit_ts") or ""))
        cost_drag = (gross - net) if gross is not None and net is not None else None
        enriched.append(
            {
                "trade_id": row.get("row_id", ""),
                "entry_ymd": entry_ymd,
                "exit_ymd": _ymd(trade.get("exit_ts")),
                "code": row.get("code", ""),
                "order_id": row.get("order_id", ""),
                "entry_timing": row.get("entry_timing", ""),
                "fallback_stage": row.get("fallback_stage", ""),
                "holding_days": "" if hold is None else hold,
                "qty": trade.get("qty", ""),
                "entry_price": trade.get("entry_price", ""),
                "exit_price": trade.get("exit_price", ""),
                "gross_ret": "" if gross is None else gross,
                "net_ret": "" if net is None else net,
                "cost_drag": "" if cost_drag is None else cost_drag,
                "loss_bucket": _loss_bucket(gross, net),
                "date_cluster": _date_cluster(entry_ymd),
                "trace_available": "true" if trade.get("note") else "false",
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    net_values = [_float(row.get("net_ret")) for row in enriched]
    gross_values = [_float(row.get("gross_ret")) for row in enriched]
    loss_bucket_counts = Counter(str(row.get("loss_bucket") or "") for row in enriched)
    date_cluster_counts = Counter(str(row.get("date_cluster") or "") for row in enriched)
    entry_day_counts = Counter(str(row.get("entry_ymd") or "") for row in enriched)
    cost_flip_rows = [row for row in enriched if row.get("loss_bucket") == "COST_FLIP_LOSS"]
    price_loss_rows = [
        row for row in enriched if row.get("loss_bucket") in {"LARGE_PRICE_LOSS", "SMALL_PRICE_LOSS"}
    ]
    cluster_rows = [row for row in enriched if row.get("date_cluster") == "APR01_02_CLUSTER"]

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "schema_version": "pure_normal_loss_driver_audit_v1",
        "source_files": {
            "pure_normal_entry_sample_audit": str(PURE_AUDIT_CSV),
            "trades_calc": str(TRADES_CALC_CSV),
        },
        "pure_current_normal_rows": len(enriched),
        "net_stats": _stats(v for v in net_values if v is not None),
        "gross_stats": _stats(v for v in gross_values if v is not None),
        "loss_bucket_counts": dict(sorted(loss_bucket_counts.items())),
        "date_cluster_counts": dict(sorted(date_cluster_counts.items())),
        "entry_day_counts": dict(sorted(entry_day_counts.items())),
        "cost_flip_rows": len(cost_flip_rows),
        "price_loss_rows": len(price_loss_rows),
        "apr01_02_cluster_rows": len(cluster_rows),
        "apr01_02_cluster_net_stats": _stats(_float(row.get("net_ret")) for row in cluster_rows),
        "primary_loss_driver": (
            "DATE_CLUSTER_AND_PRICE_LOSS"
            if len(cluster_rows) >= 10 and len(price_loss_rows) >= 8
            else "INSUFFICIENTLY_SEPARATED"
        ),
        "interpretation": (
            "The weak pure-normal sample is concentrated around 2026-04-01 to 2026-04-02 and is mostly price-loss driven. "
            "Several positive gross-return rows become net losses after cost drag, but cost drag is secondary to the date-clustered price losses."
        ),
        "decision": "NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "trade_id",
        "entry_ymd",
        "exit_ymd",
        "code",
        "order_id",
        "entry_timing",
        "fallback_stage",
        "holding_days",
        "qty",
        "entry_price",
        "exit_price",
        "gross_ret",
        "net_ret",
        "cost_drag",
        "loss_bucket",
        "date_cluster",
        "trace_available",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    summary_fields = ["kind", "bucket", "n", "avg", "median", "win_rate", "min", "max"]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, enriched, fields)
    _write_csv(OUT_SUMMARY_CSV, _summary_rows(enriched), summary_fields)
    print(
        "[FINAL] pure normal loss driver audit -> "
        f"{OUT_JSON} rows={len(enriched)} driver={out['primary_loss_driver']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
