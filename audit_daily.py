from __future__ import annotations

import json
import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
import sys
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
from holiday_manager import HolidayManager

LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"
PRICES_PARQUET = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
ENGINE_CONFIG = PAPER_DIR / "paper_engine_config.json"
CAND_DATA = LOG_DIR / "candidates_latest_data.csv"
CAND_META = LOG_DIR / "candidates_latest_meta.json"
FILLS = PAPER_DIR / "fills.csv"
# 손익 권위 원장 = trades_calc.csv (PLANS 2026-08-18 (4), 사용자 승인).
# v41.1 분기(entry_ts)가 legacy 분기와 동등한 검사를 하도록 확장한 뒤 전환했다 (PLANS 2026-08-21 (23)).
TRADES = PAPER_DIR / "trades_calc.csv"

_SIGNAL_RE = re.compile(r"signal_date=(\d{8})")


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ymd_today() -> str:
    return date.today().strftime("%Y%m%d")


def _safe_read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        try:
            return p.read_text(encoding="utf-8-sig")
        except Exception:
            return None


def _safe_read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def _entry_timing_mode() -> str:
    obj = _safe_read_json(ENGINE_CONFIG) if ENGINE_CONFIG.exists() else None
    mode = str((obj or {}).get("entry_timing_mode") or "next_open").strip().lower()
    return mode if mode else "next_open"


def _load_csv(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists() or p.stat().st_size == 0:
        return None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _to_yyyymmdd(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("-", "").replace("/", "").replace(".", "")
    m = re.match(r"^(\d{8})", s)
    return m.group(1) if m else None


def _extract_signal_date(note: Any) -> Optional[str]:
    if note is None:
        return None
    s = str(note)
    m = _SIGNAL_RE.search(s)
    return m.group(1) if m else None


def _next_weekday_yyyymmdd(ymd: str) -> Optional[str]:
    """Backward-compatible wrapper around the trading calendar SSOT."""
    try:
        return HolidayManager().next_trading_day(ymd)
    except Exception:
        return None


def _load_holidays() -> set[str]:
    return set()


def _next_open_day_yyyymmdd(ymd: str, holidays: Optional[set[str]] = None) -> Optional[str]:
    try:
        return HolidayManager().next_trading_day(ymd)
    except Exception:
        return None


def _load_prices() -> Tuple[Optional[pd.DataFrame], List[str]]:
    flags: List[str] = []
    if not PRICES_PARQUET.exists() or PRICES_PARQUET.stat().st_size == 0:
        flags.append("prices_parquet_missing")
        return None, flags
    try:
        df = pd.read_parquet(PRICES_PARQUET)  # requires pyarrow/fastparquet on user machine
    except Exception as e:
        flags.append("prices_parquet_unreadable:" + (str(e)[:120]))
        return None, flags

    # Normalize common schemas
    # expected columns: date, code, open/high/low/close
    if "date" not in df.columns:
        flags.append("prices_missing_date_col")
        return None, flags

    if "code" not in df.columns:
        flags.append("prices_missing_code_col")
        return None, flags

    df = df.copy()
    df["date"] = df["date"].astype(str).str.replace("-", "").str[:8]
    df["code"] = df["code"].astype(str).str.zfill(6)

    # normalize open/close names
    if "open" not in df.columns:
        # sometimes Korean headers exist
        for k in ("시가", "Open", "OPEN"):
            if k in df.columns:
                df["open"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["open"] = pd.to_numeric(df["open"], errors="coerce")

    if "low" not in df.columns:
        for k in ("저가", "Low", "LOW"):
            if k in df.columns:
                df["low"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["low"] = pd.to_numeric(df["low"], errors="coerce")

    if "high" not in df.columns:
        for k in ("고가", "High", "HIGH"):
            if k in df.columns:
                df["high"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["high"] = pd.to_numeric(df["high"], errors="coerce")

    if "close" not in df.columns:
        for k in ("종가", "Close", "CLOSE"):
            if k in df.columns:
                df["close"] = pd.to_numeric(df[k], errors="coerce")
                break
    else:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

    return df, flags


def _next_trading_date_from_prices(px: pd.DataFrame, code: str, signal_date: str, holidays: Optional[set[str]] = None) -> Optional[str]:
    code_dates = px[px["code"] == code][["date"]]
    if code_dates.empty:
        return None

    next_open_day = _next_open_day_yyyymmdd(signal_date, holidays=holidays)
    code_min_date = str(code_dates["date"].min())

    # Current paper prices parquet can be a rolling subset. When the code's first
    # available date is already later than the expected next open day, treat the
    # parquet as lacking historical coverage and fall back to the calendar-based
    # expectation instead of falsely flagging a mismatch.
    if next_open_day and code_min_date > next_open_day:
        return next_open_day

    dd = code_dates[code_dates["date"] > signal_date][["date"]]
    if dd.empty:
        return next_open_day
    # dd["date"] is yyyymmdd string, lexicographic works
    return str(dd["date"].min())


def _open_price(px: pd.DataFrame, code: str, ymd: str) -> Optional[float]:
    r = px[(px["code"] == code) & (px["date"] == ymd)]
    if r.empty:
        return None
    if "open" not in r.columns:
        return None
    try:
        v = float(r.iloc[0]["open"])
        return v
    except Exception:
        return None


def _close_price(px: pd.DataFrame, code: str, ymd: str) -> Optional[float]:
    r = px[(px["code"] == code) & (px["date"] == ymd)]
    if r.empty:
        return None
    if "close" not in r.columns:
        return None
    try:
        v = float(r.iloc[0]["close"])
        return v
    except Exception:
        return None


def _day_low_high(px: pd.DataFrame, code: str, ymd: str) -> Tuple[Optional[float], Optional[float]]:
    r = px[(px["code"] == code) & (px["date"] == ymd)]
    if r.empty:
        return None, None
    lo = None
    hi = None
    try:
        if "low" in r.columns:
            lo = float(r.iloc[0]["low"])
    except Exception:
        lo = None
    try:
        if "high" in r.columns:
            hi = float(r.iloc[0]["high"])
    except Exception:
        hi = None
    return lo, hi


def _is_same_close_entry(note: Any, default_mode: str, signal_date: Optional[str], event_date: Optional[str]) -> bool:
    s = str(note or "")
    if "entry_timing=same_close" in s:
        return True
    if default_mode not in {"same_close", "close", "t_close", "close_entry"}:
        return False
    if not signal_date or not event_date:
        return False
    return event_date == signal_date


def _approx_equal(a: Optional[float], b: Optional[float], tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def _in_range(v: Optional[float], lo: Optional[float], hi: Optional[float], tol: float = 1e-3) -> bool:
    if v is None or lo is None or hi is None:
        return False
    low = min(float(lo), float(hi)) - float(tol)
    high = max(float(lo), float(hi)) + float(tol)
    return low <= float(v) <= high


def _krx_tick(price: Optional[float]) -> float:
    """OHLC 경계 허용오차용 호가단위.

    [2026-09-08] 표 자체는 utils/krx_tick.py 로 옮겼다. 같은 표가 여기와
      tools/topn_build_orders.py 두 벌이었고 그쪽이 5단계로 틀려 발주가 거절됐다.
      이 함수는 None 허용 + float 반환이라는 호출부 계약만 유지하는 얇은 껍데기다.
    """
    if price is None:
        return 0.0
    from utils.krx_tick import tick_size
    return float(tick_size(float(price)))


def _is_legacy_minimal_note(note: Any) -> bool:
    """
    Legacy rows often carry only signal_date and no modern lineage keys.
    For these rows, strict expected-entry-date matching is treated as non-actionable.
    """
    s = str(note or "")
    if "signal_date=" not in s:
        return False
    has_lineage = ("entry_order_id=" in s) or ("entry_intent_id=" in s) or ("entry_trace_id=" in s)
    return not has_lineage


def main() -> int:
    out_path = LOG_DIR / f"audit_daily_{_ymd_today()}_{datetime.now().strftime('%H%M%S')}.json"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    report: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "base_dir": str(BASE_DIR),
        "entry_timing_mode": _entry_timing_mode(),
        "paths": {
            "candidates_latest_data": str(CAND_DATA),
            "candidates_latest_meta": str(CAND_META),
            "fills": str(FILLS),
            "trades": str(TRADES),
            "prices_parquet": str(PRICES_PARQUET),
        },
        "flags": [],
        "summary": {},
        "suspects": [],
    }

    # Load inputs
    cand_df = _load_csv(CAND_DATA)
    meta = _safe_read_json(CAND_META) if CAND_META.exists() else None
    fills_df = _load_csv(FILLS)
    trades_df = _load_csv(TRADES)
    entry_timing_mode = str(report.get("entry_timing_mode") or "next_open")

    px, px_flags = _load_prices()
    holidays = _load_holidays()
    report["flags"].extend(px_flags)

    # Normalize candidate signals
    signals: List[Tuple[str, str]] = []  # (code, signal_date)
    if isinstance(cand_df, pd.DataFrame) and not cand_df.empty:
        df = cand_df.copy()
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
        if "signal_date" not in df.columns:
            if "date" in df.columns:
                df["signal_date"] = df["date"].astype(str).str.replace("-", "").str[:8]
            else:
                df["signal_date"] = _ymd_today()

        # keep unique (code, signal_date)
        for _, r in df.iterrows():
            code = str(r.get("code", "")).zfill(6)
            sd = _to_yyyymmdd(r.get("signal_date"))
            if code and sd:
                signals.append((code, sd))
        signals = sorted(set(signals))

    # Audit records
    suspects: List[Dict[str, Any]] = []

    # 1) Trades timing checks (strongest signal)
    trade_checked = 0
    if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
        tdf = trades_df.copy()

        # detect legacy vs v41.1
        if "entry_date" in tdf.columns:
            # legacy
            tdf["code"] = tdf["code"].astype(str).str.zfill(6)
            for idx, r in tdf.iterrows():
                code = str(r.get("code", "")).zfill(6)
                entry_date = _to_yyyymmdd(r.get("entry_date"))
                entry_price = None
                try:
                    entry_price = float(r.get("entry_price")) if r.get("entry_price") is not None else None
                except Exception:
                    entry_price = None

                note = r.get("note")
                sd = _extract_signal_date(note)
                same_close_entry = _is_same_close_entry(note, entry_timing_mode, sd, entry_date)
                trade_checked += 1

                if sd is None:
                    suspects.append({"type": "missing_signal_date", "source": "trades", "row": int(idx), "code": code})
                    continue
                if entry_date is None:
                    suspects.append({"type": "missing_entry_date", "source": "trades", "row": int(idx), "code": code, "signal_date": sd})
                    continue

                if (not same_close_entry) and entry_date <= sd:
                    suspects.append({
                        "type": "entry_not_after_signal",
                        "source": "trades",
                        "row": int(idx),
                        "code": code,
                        "signal_date": sd,
                        "entry_date": entry_date,
                    })

                expected_entry = None
                if same_close_entry:
                    expected_entry = sd
                elif px is not None:
                    expected_entry = _next_trading_date_from_prices(px, code, sd, holidays=holidays)
                else:
                    expected_entry = _next_open_day_yyyymmdd(sd, holidays=holidays)

                legacy_min_note = _is_legacy_minimal_note(note)
                if expected_entry and entry_date != expected_entry and (not legacy_min_note):
                    suspects.append({
                        "type": "entry_date_mismatch_expected",
                        "source": "trades",
                        "row": int(idx),
                        "code": code,
                        "signal_date": sd,
                        "entry_date": entry_date,
                        "expected_entry_date": expected_entry,
                    })

                # price check:
                # - next_open: entry_price == open(entry_date)
                # - same_close: entry_price must be within [low, high] of entry_date
                if px is not None and entry_date:
                    if same_close_entry:
                        lo, hi = _day_low_high(px, code, entry_date)
                        tick_tol = max(1e-3, _krx_tick(entry_price))
                        if entry_price is not None and lo is not None and hi is not None and (not _in_range(entry_price, lo, hi, tol=tick_tol)):
                            suspects.append({
                                "type": "entry_price_out_of_day_range",
                                "source": "trades",
                                "row": int(idx),
                                "code": code,
                                "entry_date": entry_date,
                                "entry_price": entry_price,
                                "day_low": lo,
                                "day_high": hi,
                            })
                    else:
                        expected_price = _open_price(px, code, entry_date)
                        if expected_price is not None and entry_price is not None and (not _approx_equal(expected_price, entry_price, tol=1e-3)):
                            suspects.append({
                                "type": "entry_price_mismatch_open",
                                "source": "trades",
                                "row": int(idx),
                                "code": code,
                                "entry_date": entry_date,
                                "entry_price": entry_price,
                                "expected_open": expected_price,
                            })
        else:
            # v41.1 style (entry_ts)
            if "entry_ts" in tdf.columns:
                tdf["code"] = tdf["code"].astype(str).str.zfill(6)
                for idx, r in tdf.iterrows():
                    code = str(r.get("code", "")).zfill(6)
                    entry_ts = str(r.get("entry_ts", "")).strip()
                    entry_date = _to_yyyymmdd(entry_ts)
                    entry_price = None
                    try:
                        entry_price = float(r.get("entry_price")) if r.get("entry_price") is not None else None
                    except Exception:
                        entry_price = None
                    note = r.get("note")
                    sd = _extract_signal_date(note)
                    same_close_entry = _is_same_close_entry(note, entry_timing_mode, sd, entry_date)
                    trade_checked += 1

                    if sd is None:
                        suspects.append({"type": "missing_signal_date", "source": "trades", "row": int(idx), "code": code})
                        continue
                    if entry_date is None:
                        suspects.append({"type": "missing_entry_date", "source": "trades", "row": int(idx), "code": code, "signal_date": sd})
                        continue
                    if (not same_close_entry) and entry_date <= sd:
                        suspects.append({
                            "type": "entry_not_after_signal",
                            "source": "trades",
                            "row": int(idx),
                            "code": code,
                            "signal_date": sd,
                            "entry_date": entry_date,
                        })

                    # legacy 분기(entry_date)와 동등한 검사 2종.
                    # 이전에는 v41.1 분기에 없어서, 원장을 trades_calc.csv 로 바꾸면
                    # 기대진입일·진입가 검사가 조용히 사라졌다 (PLANS 2026-08-21 (23)).
                    expected_entry = None
                    if same_close_entry:
                        expected_entry = sd
                    elif px is not None:
                        expected_entry = _next_trading_date_from_prices(px, code, sd, holidays=holidays)
                    else:
                        expected_entry = _next_open_day_yyyymmdd(sd, holidays=holidays)

                    legacy_min_note = _is_legacy_minimal_note(note)
                    if expected_entry and entry_date != expected_entry and (not legacy_min_note):
                        suspects.append({
                            "type": "entry_date_mismatch_expected",
                            "source": "trades",
                            "row": int(idx),
                            "code": code,
                            "signal_date": sd,
                            "entry_date": entry_date,
                            "expected_entry_date": expected_entry,
                        })

                    if px is not None and entry_date:
                        if same_close_entry:
                            lo, hi = _day_low_high(px, code, entry_date)
                            tick_tol = max(1e-3, _krx_tick(entry_price))
                            if entry_price is not None and lo is not None and hi is not None and (not _in_range(entry_price, lo, hi, tol=tick_tol)):
                                suspects.append({
                                    "type": "entry_price_out_of_day_range",
                                    "source": "trades",
                                    "row": int(idx),
                                    "code": code,
                                    "entry_date": entry_date,
                                    "entry_price": entry_price,
                                    "day_low": lo,
                                    "day_high": hi,
                                })
                        else:
                            expected_price = _open_price(px, code, entry_date)
                            if expected_price is not None and entry_price is not None and (not _approx_equal(expected_price, entry_price, tol=1e-3)):
                                suspects.append({
                                    "type": "entry_price_mismatch_open",
                                    "source": "trades",
                                    "row": int(idx),
                                    "code": code,
                                    "entry_date": entry_date,
                                    "entry_price": entry_price,
                                    "expected_open": expected_price,
                                })

    # 2) BUY fills timing/price checks (secondary)
    fills_buy_checked = 0
    if isinstance(fills_df, pd.DataFrame) and not fills_df.empty:
        fdf = fills_df.copy()
        # legacy uses 'datetime', v41.1 uses 'ts' and 'date'
        if "code" in fdf.columns:
            fdf["code"] = fdf["code"].astype(str).str.zfill(6)

        for idx, r in fdf.iterrows():
            side = str(r.get("side", "")).upper()
            if side != "BUY":
                continue
            code = str(r.get("code", "")).zfill(6)
            note = r.get("note")
            sd = _extract_signal_date(note)
            fills_buy_checked += 1

            # fill date
            fill_date = None
            if "date" in fdf.columns:
                fill_date = _to_yyyymmdd(r.get("date"))
            if fill_date is None:
                fill_date = _to_yyyymmdd(r.get("datetime") or r.get("ts"))
            same_close_entry = _is_same_close_entry(note, entry_timing_mode, sd, fill_date)

            # fill price
            fill_price = None
            try:
                fill_price = float(r.get("price")) if r.get("price") is not None else None
            except Exception:
                fill_price = None

            if sd is None:
                suspects.append({"type": "missing_signal_date", "source": "fills", "row": int(idx), "code": code})
                continue
            if fill_date is None:
                suspects.append({"type": "missing_fill_date", "source": "fills", "row": int(idx), "code": code, "signal_date": sd})
                continue

            if (not same_close_entry) and fill_date <= sd:
                suspects.append({
                    "type": "fill_not_after_signal",
                    "source": "fills",
                    "row": int(idx),
                    "code": code,
                    "signal_date": sd,
                    "fill_date": fill_date,
                })

            expected_entry = None
            if same_close_entry:
                expected_entry = sd
            elif px is not None:
                expected_entry = _next_trading_date_from_prices(px, code, sd, holidays=holidays)
            else:
                expected_entry = _next_open_day_yyyymmdd(sd, holidays=holidays)

            legacy_min_note = _is_legacy_minimal_note(note)
            if expected_entry and fill_date != expected_entry and (not legacy_min_note):
                suspects.append({
                    "type": "fill_date_mismatch_expected",
                    "source": "fills",
                    "row": int(idx),
                    "code": code,
                    "signal_date": sd,
                    "fill_date": fill_date,
                    "expected_entry_date": expected_entry,
                })

            # price check:
            # - next_open: fill_price == open(fill_date)
            # - same_close: fill_price must be within [low, high] of fill_date
            if px is not None and fill_date and fill_price is not None:
                if same_close_entry:
                    lo, hi = _day_low_high(px, code, fill_date)
                    tick_tol = max(1e-3, _krx_tick(fill_price))
                    if lo is not None and hi is not None and (not _in_range(fill_price, lo, hi, tol=tick_tol)):
                        suspects.append({
                            "type": "fill_price_out_of_day_range",
                            "source": "fills",
                            "row": int(idx),
                            "code": code,
                            "fill_date": fill_date,
                            "fill_price": fill_price,
                            "day_low": lo,
                            "day_high": hi,
                        })
                else:
                    expected_price = _open_price(px, code, fill_date)
                    if expected_price is not None and (not _approx_equal(expected_price, fill_price, tol=1e-3)):
                        suspects.append({
                            "type": "fill_price_mismatch_open",
                            "source": "fills",
                            "row": int(idx),
                            "code": code,
                            "fill_date": fill_date,
                            "fill_price": fill_price,
                            "expected_open": expected_price,
                        })

    # Compose summary
    suspect_types = {}
    if suspects:
        counts = pd.Series([s.get("type") for s in suspects]).value_counts()
        suspect_types = {str(k): int(v) for k, v in counts.items()}

    # 입력 무결성 - "검사했는데 없음"과 "검사 자체를 못 함"을 구분한다 (PLANS 2026-08-21 (24)).
    # 이전에는 원장/체결/후보가 없거나 비어도 검사 블록이 통째로 스킵되어 suspects 0 -> PASS 였다.
    missing_inputs: List[str] = []
    for _name, _df in (("candidates", cand_df), ("fills", fills_df), ("trades", trades_df)):
        if not (isinstance(_df, pd.DataFrame) and not _df.empty):
            missing_inputs.append(_name)
            report["flags"].append(f"input_missing_or_empty:{_name}")
    if px is None:
        # _load_prices() 가 이미 사유 flag 를 남긴다. 진입가 검사 2종이 실행되지 않는다
        missing_inputs.append("prices")

    # suspects 가 있으면 FAIL 이 우선한다. 실제 결함을 INCOMPLETE 로 덮지 않기 위해서다
    if suspects:
        _status = "FAIL"
    elif missing_inputs:
        _status = "INCOMPLETE"
    else:
        _status = "PASS"

    report["summary"] = {
        "signals_rows": int(len(cand_df)) if isinstance(cand_df, pd.DataFrame) else 0,
        "signals_unique": int(len(signals)),
        "fills_rows": int(len(fills_df)) if isinstance(fills_df, pd.DataFrame) else 0,
        "trades_rows": int(len(trades_df)) if isinstance(trades_df, pd.DataFrame) else 0,
        "checked_trades": int(trade_checked),
        "checked_buy_fills": int(fills_buy_checked),
        "lookahead_suspects": int(len(suspects)),
        "suspect_types": suspect_types,
        "missing_inputs": missing_inputs,
        "prices_loaded": bool(px is not None),
        "status": _status,
        "meta_latest_date": _to_yyyymmdd((meta or {}).get("latest_date")),
        "meta_market_regime": (meta or {}).get("market_regime"),
    }
    report["suspects"] = suspects[:500]  # hard cap to keep file small

    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[AUDIT] wrote: {out_path}")
    print(f"[AUDIT] status={report['summary']['status']} lookahead_suspects={report['summary']['lookahead_suspects']} checked_trades={trade_checked} checked_buy_fills={fills_buy_checked}")

    if suspects:
        # print first few for quick visibility
        for s in suspects[:5]:
            print("[AUDIT] suspect:", json.dumps(s, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
