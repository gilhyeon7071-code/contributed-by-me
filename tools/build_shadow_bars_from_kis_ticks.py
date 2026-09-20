from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TRADE_TR_ID = "H0STCNT0"
TRADE_RECORD_WIDTH = 46


@dataclass(frozen=True)
class Tick:
    ts: datetime
    code: str
    price: float
    volume: float


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_iso(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _parse_tick_time(base: datetime, hhmmss: Any) -> datetime:
    raw = "".join(ch for ch in str(hhmmss or "") if ch.isdigit())
    if len(raw) < 6:
        return base
    try:
        return base.replace(hour=int(raw[:2]), minute=int(raw[2:4]), second=int(raw[4:6]), microsecond=0)
    except Exception:
        return base


def _to_float(raw: Any) -> Optional[float]:
    try:
        text = str(raw).replace(",", "").strip()
        if text == "":
            return None
        return float(text)
    except Exception:
        return None


def _iter_trade_records(raw_payload: str, fallback_code: str) -> Iterable[List[str]]:
    fields = str(raw_payload or "").split("^")
    if len(fields) < 3:
        return
    if len(fields) % TRADE_RECORD_WIDTH == 0:
        for i in range(0, len(fields), TRADE_RECORD_WIDTH):
            rec = fields[i : i + TRADE_RECORD_WIDTH]
            if len(rec) >= 13:
                yield rec
        return
    # Fallback for truncated frames: use the first visible record only.
    if fields and str(fields[0]).strip().zfill(6) == fallback_code:
        yield fields[: min(len(fields), TRADE_RECORD_WIDTH)]


def load_ticks(path: Path) -> Tuple[List[Tick], Dict[str, Any]]:
    ticks: List[Tick] = []
    total_lines = 0
    malformed_lines = 0
    trade_frames = 0
    trade_records = 0
    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                rec = json.loads(line)
            except Exception:
                malformed_lines += 1
                continue
            norm = rec.get("normalized") if isinstance(rec.get("normalized"), dict) else {}
            if str(norm.get("tr_id") or "").strip() != TRADE_TR_ID:
                continue
            if str(norm.get("event_type") or "").strip().lower() != "trade":
                continue
            base_ts = _parse_iso(rec.get("ts"))
            if base_ts is None:
                malformed_lines += 1
                continue
            raw_payload = str(norm.get("raw_payload") or "")
            fallback_code = str(norm.get("code") or "").strip().zfill(6)
            trade_frames += 1
            for fields in _iter_trade_records(raw_payload, fallback_code):
                code = str(fields[0] if len(fields) > 0 else fallback_code).strip().zfill(6)
                if not code.isdigit() or len(code) != 6:
                    continue
                price = _to_float(fields[2] if len(fields) > 2 else None)
                if price is None or price <= 0:
                    continue
                volume = _to_float(fields[12] if len(fields) > 12 else 0.0) or 0.0
                event_ts = _parse_tick_time(base_ts, fields[1] if len(fields) > 1 else "")
                ticks.append(Tick(ts=event_ts, code=code, price=float(price), volume=max(0.0, float(volume))))
                trade_records += 1
    ticks.sort(key=lambda x: (x.code, x.ts))
    meta = {
        "total_lines": int(total_lines),
        "malformed_lines": int(malformed_lines),
        "trade_frames": int(trade_frames),
        "trade_records": int(trade_records),
        "tick_count": int(len(ticks)),
    }
    return ticks, meta


def build_1m_bars(ticks: List[Tick]) -> pd.DataFrame:
    buckets: Dict[Tuple[str, datetime], List[Tick]] = defaultdict(list)
    for tick in ticks:
        minute = tick.ts.replace(second=0, microsecond=0)
        buckets[(tick.code, minute)].append(tick)

    rows: List[Dict[str, Any]] = []
    for (code, minute), vals in sorted(buckets.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        vals = sorted(vals, key=lambda x: x.ts)
        prices = [x.price for x in vals]
        rows.append(
            {
                "ts_minute": minute.isoformat(timespec="seconds"),
                "date": minute.strftime("%Y%m%d"),
                "code": code,
                "open": float(prices[0]),
                "high": float(max(prices)),
                "low": float(min(prices)),
                "close": float(prices[-1]),
                "volume": float(sum(x.volume for x in vals)),
                "tick_count": int(len(vals)),
                "source": "kis_ws_ticks_shadow",
                "status": "OK",
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ret_1m_pct"] = None
    for _, idx in df.groupby("code", sort=False).groups.items():
        closes = pd.to_numeric(df.loc[idx, "close"], errors="coerce")
        df.loc[idx, "ret_1m_pct"] = closes.pct_change() * 100.0
    df["ret_1m_pct"] = pd.to_numeric(df["ret_1m_pct"], errors="coerce").fillna(0.0).round(8)
    return df


def write_outputs(df: pd.DataFrame, meta: Dict[str, Any], *, input_path: Path, out_csv: Path, out_json: Path) -> Dict[str, Any]:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    payload: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "status": "PASS" if len(df) > 0 else "FAIL",
        "reason": "SHADOW_1M_BARS_BUILT" if len(df) > 0 else "NO_BARS_BUILT",
        "input": str(input_path),
        "outputs": {"csv": str(out_csv), "json": str(out_json)},
        "meta": {
            **meta,
            "bar_rows": int(len(df)),
            "unique_codes": int(df["code"].astype(str).nunique()) if len(df) and "code" in df.columns else 0,
            "ts_min": str(df["ts_minute"].min()) if len(df) and "ts_minute" in df.columns else None,
            "ts_max": str(df["ts_minute"].max()) if len(df) and "ts_minute" in df.columns else None,
        },
        "policy": {
            "shadow_only": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "stop_meaning_modified": False,
            "runtime_config_modified": False,
            "threshold_applied": False,
        },
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build shadow 1-minute bars from KIS websocket tick jsonl.")
    p.add_argument("--input-jsonl", required=True)
    p.add_argument("--out-csv", default=str(LOG_DIR / "arl_shadow_1m_bars_20260429.csv"))
    p.add_argument("--out-json", default=str(LOG_DIR / "arl_shadow_1m_bars_20260429.json"))
    return p


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input_jsonl)
    out_csv = Path(args.out_csv)
    out_json = Path(args.out_json)
    if not input_path.exists():
        payload = {
            "generated_at": _now_ts(),
            "status": "FAIL",
            "reason": "INPUT_MISSING",
            "input": str(input_path),
            "outputs": {"csv": str(out_csv), "json": str(out_json)},
            "policy": {"shadow_only": True, "threshold_applied": False, "gate_modified": False},
        }
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        print(json.dumps({"status": "FAIL", "reason": "INPUT_MISSING", "json": str(out_json)}, ensure_ascii=True))
        return 1
    ticks, meta = load_ticks(input_path)
    df = build_1m_bars(ticks)
    payload = write_outputs(df, meta, input_path=input_path, out_csv=out_csv, out_json=out_json)
    print(json.dumps({"status": payload.get("status"), "reason": payload.get("reason"), "json": str(out_json), "csv": str(out_csv)}, ensure_ascii=True))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
