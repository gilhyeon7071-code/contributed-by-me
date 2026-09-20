from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

OUT_CSV_LATEST = CACHE / "krx_point_in_time_listing_history_latest.csv"
OUT_META_LATEST = LOGS / "krx_point_in_time_listing_history_meta_latest.json"
OUT_STATUS_LATEST = LOGS / "krx_point_in_time_listing_history_status_latest.json"
OUT_MD_LATEST = LOGS / "krx_point_in_time_listing_history_latest.md"

MARKET_MAP = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}

KRX_MKT_ID = {
    "KOSPI": "STK",
    "KOSDAQ": "KSQ",
}

PREFERRED_NAME_PATTERNS = [
    re.compile(r"우$"),
    re.compile(r"우B$"),
    re.compile(r"우C$"),
    re.compile(r"우\(전환\)$"),
    re.compile(r"\d+우[BC]?$"),
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _norm_ymd(value: object) -> str:
    return str(value or "").replace("-", "").strip()[:8]


def _parse_ymd(value: str) -> datetime:
    return datetime.strptime(_norm_ymd(value), "%Y%m%d")


def _norm_code(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _date_range(start: str, end: str, frequency: str) -> list[str]:
    cur = _parse_ymd(start)
    stop = _parse_ymd(end)
    if cur > stop:
        raise ValueError("start must be <= end")

    dates: list[str] = []
    if frequency == "daily":
        while cur <= stop:
            dates.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)
        return dates

    if frequency == "monthly":
        seen: set[str] = set()
        while cur <= stop:
            month_key = cur.strftime("%Y%m")
            last = cur
            probe = cur
            while probe <= stop and probe.strftime("%Y%m") == month_key:
                last = probe
                probe += timedelta(days=1)
            ymd = last.strftime("%Y%m%d")
            if ymd not in seen:
                dates.append(ymd)
                seen.add(ymd)
            cur = probe
        return dates

    if frequency == "single":
        return [_norm_ymd(start)]

    raise ValueError(f"unsupported frequency: {frequency}")


def _infer_security_type(name: str) -> tuple[str, str]:
    clean = str(name or "").strip()
    if any(p.search(clean) for p in PREFERRED_NAME_PATTERNS):
        return "PREFERRED_OR_OTHER_EQUITY_HEURISTIC", "NAME_HEURISTIC"
    return "COMMON_OR_UNSPECIFIED_EQUITY_HEURISTIC", "NAME_HEURISTIC"


def _fetch_pykrx(as_of_date: str, market: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        from pykrx import stock  # type: ignore
    except Exception as exc:
        return [], {
            "source_status": "FAIL_IMPORT",
            "error": f"{type(exc).__name__}: {exc}",
            "rows": 0,
        }

    try:
        tickers = stock.get_market_ticker_list(as_of_date, market=market)
    except Exception as exc:
        return [], {
            "source_status": "FAIL_FETCH",
            "error": f"{type(exc).__name__}: {exc}",
            "rows": 0,
        }

    rows: list[dict[str, Any]] = []
    for raw_code in tickers or []:
        code = _norm_code(raw_code)
        if not code:
            continue
        try:
            name = str(stock.get_market_ticker_name(code) or "").strip()
        except Exception:
            name = ""
        security_type, security_type_rule = _infer_security_type(name)
        rows.append(
            {
                "as_of_date": as_of_date,
                "code": code,
                "name": name,
                "market": MARKET_MAP.get(market, market),
                "security_type": security_type,
                "security_type_rule": security_type_rule,
                "source": "pykrx.stock.get_market_ticker_list",
                "source_status": "OK",
            }
        )

    return rows, {
        "source_status": "OK" if rows else "FAIL_SOURCE_EMPTY",
        "source": "pykrx.stock.get_market_ticker_list",
        "rows": len(rows),
        "market": market,
        "as_of_date": as_of_date,
    }




def _decode_csv_bytes(content: bytes) -> str:
    for encoding in ("euc-kr", "cp949", "utf-8-sig", "utf-8"):
        try:
            return content.decode(encoding)
        except Exception:
            continue
    return content.decode("utf-8", errors="replace")


def _pick_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    normalized = {str(c).strip().lower(): str(c) for c in df.columns}
    for alias in aliases:
        key = alias.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _fetch_krx_otp(as_of_date: str, market: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
        "Origin": "https://data.krx.co.kr",
    }
    payload = {
        "locale": "ko_KR",
        "mktId": KRX_MKT_ID[market],
        "trdDd": as_of_date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01501",
    }
    try:
        session.get("https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd", headers=headers, timeout=20)
        otp_resp = session.post(
            "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd",
            headers={**headers, "X-Requested-With": "XMLHttpRequest"},
            data=payload,
            timeout=20,
        )
        otp = (otp_resp.text or "").strip()
        if otp == "LOGOUT":
            return [], {
                "source_status": "FAIL_KRX_OTP_LOGOUT",
                "source": "krx.fileDn.GenerateOTP.MDCSTAT01501",
                "rows": 0,
                "market": market,
                "as_of_date": as_of_date,
                "http_status": otp_resp.status_code,
            }
        if not otp:
            return [], {
                "source_status": "FAIL_KRX_OTP_EMPTY",
                "source": "krx.fileDn.GenerateOTP.MDCSTAT01501",
                "rows": 0,
                "market": market,
                "as_of_date": as_of_date,
                "http_status": otp_resp.status_code,
            }
        csv_resp = session.post(
            "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
            headers=headers,
            data={"code": otp},
            timeout=30,
        )
        text = _decode_csv_bytes(csv_resp.content)
        if not text.strip():
            return [], {
                "source_status": "FAIL_KRX_CSV_EMPTY",
                "source": "krx.fileDn.download_csv.MDCSTAT01501",
                "rows": 0,
                "market": market,
                "as_of_date": as_of_date,
                "http_status": csv_resp.status_code,
            }
        raw = pd.read_csv(io.StringIO(text))
    except Exception as exc:
        return [], {
            "source_status": "FAIL_KRX_FETCH",
            "source": "krx.fileDn.MDCSTAT01501",
            "error": f"{type(exc).__name__}: {exc}",
            "rows": 0,
            "market": market,
            "as_of_date": as_of_date,
        }

    code_col = _pick_col(raw, ["ISU_SRT_CD", "종목코드", "단축코드", "code"])
    name_col = _pick_col(raw, ["ISU_ABBRV", "종목명", "한글 종목약명", "name"])
    market_col = _pick_col(raw, ["MKT_NM", "시장구분", "시장", "market"])
    if code_col is None:
        return [], {
            "source_status": "FAIL_KRX_SCHEMA_NO_CODE",
            "source": "krx.fileDn.download_csv.MDCSTAT01501",
            "rows": 0,
            "market": market,
            "as_of_date": as_of_date,
            "columns": [str(c) for c in raw.columns],
        }

    rows: list[dict[str, Any]] = []
    for _, rec in raw.iterrows():
        code = _norm_code(rec.get(code_col, ""))
        if not code:
            continue
        name = str(rec.get(name_col, "") if name_col else "").strip()
        raw_market = str(rec.get(market_col, "") if market_col else "").strip().upper()
        if raw_market in {"KOSDAQ", "KSQ", "코스닥"}:
            normalized_market = "KOSDAQ"
        elif raw_market in {"KOSPI", "STK", "유가증권"}:
            normalized_market = "KOSPI"
        else:
            normalized_market = MARKET_MAP[market]
        security_type, security_type_rule = _infer_security_type(name)
        rows.append(
            {
                "as_of_date": as_of_date,
                "code": code,
                "name": name,
                "market": normalized_market,
                "security_type": security_type,
                "security_type_rule": security_type_rule,
                "source": "krx.fileDn.download_csv.MDCSTAT01501",
                "source_status": "OK",
            }
        )

    return rows, {
        "source_status": "OK" if rows else "FAIL_KRX_SOURCE_EMPTY",
        "source": "krx.fileDn.download_csv.MDCSTAT01501",
        "rows": len(rows),
        "market": market,
        "as_of_date": as_of_date,
        "columns": [str(c) for c in raw.columns],
    }
def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "as_of_date",
        "code",
        "name",
        "market",
        "security_type",
        "security_type_rule",
        "source",
        "source_status",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_md(path: Path, status: dict[str, Any]) -> None:
    lines = [
        "# KRX/PyKRX Point-in-Time Listing History",
        "",
        f"- generated_at: {status['generated_at']}",
        f"- status: {status['status']}",
        f"- reason: {status.get('reason', '')}",
        f"- start: {status['start']}",
        f"- end: {status['end']}",
        f"- frequency: {status['frequency']}",
        f"- rows: {status['rows']}",
        f"- unique_dates: {status['unique_dates']}",
        f"- unique_codes: {status['unique_codes']}",
        f"- operational_change: {str(status['operational_change']).lower()}",
        f"- broker_order: {str(status['broker_order']).lower()}",
        f"- candidate_selection_calculated: {str(status['candidate_selection_calculated']).lower()}",
        f"- performance_calculated: {str(status['performance_calculated']).lower()}",
        "",
        "## Source Checks",
        "",
    ]
    for item in status.get("source_checks", []):
        lines.append(
            f"- {item.get('as_of_date')} {item.get('market')}: {item.get('source_status')} rows={item.get('rows')} error={item.get('error', '')}"
        )
    if status.get("output_csv"):
        lines.extend(["", f"output_csv: {status['output_csv']}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_history(start: str, end: str, frequency: str, markets: list[str], sleep_sec: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    for as_of_date in _date_range(start, end, frequency):
        for market in markets:
            fetched, check = _fetch_pykrx(as_of_date, market)
            checks.append({"as_of_date": as_of_date, **check})
            if not fetched:
                fetched, krx_check = _fetch_krx_otp(as_of_date, market)
                checks.append({"as_of_date": as_of_date, **krx_check})
            rows.extend(fetched)
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["as_of_date"]), str(row["market"]), str(row["code"]))
        dedup[key] = row
    return [dedup[k] for k in sorted(dedup)], checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Build research-only point-in-time KRX listing history through pykrx.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--frequency", choices=["single", "daily", "monthly"], default="single")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ")
    parser.add_argument("--min-rows-per-date", type=int, default=1000)
    parser.add_argument("--sleep-sec", type=float, default=0.0)
    parser.add_argument("--output-csv", default=str(OUT_CSV_LATEST))
    args = parser.parse_args()

    start = _norm_ymd(args.start)
    end = _norm_ymd(args.end)
    markets = [m.strip().upper() for m in str(args.markets).split(",") if m.strip()]
    invalid_markets = [m for m in markets if m not in MARKET_MAP]
    if invalid_markets:
        raise SystemExit(f"[FATAL] unsupported markets: {invalid_markets}")

    rows, checks = build_history(start, end, args.frequency, markets, args.sleep_sec)
    df = pd.DataFrame(rows)
    counts_by_date: dict[str, int] = {}
    if not df.empty:
        counts_by_date = {str(k): int(v) for k, v in df.groupby("as_of_date")["code"].nunique().to_dict().items()}

    required_dates = _date_range(start, end, args.frequency)
    min_rows = int(args.min_rows_per_date)
    low_dates = [d for d in required_dates if int(counts_by_date.get(d, 0)) < min_rows]
    status_value = "PASS" if rows and not low_dates else "FAIL"
    reason = "OK" if status_value == "PASS" else "FAIL_SOURCE_EMPTY_OR_BELOW_MIN_ROWS"

    output_csv = Path(args.output_csv)
    if rows:
        _write_csv(output_csv, rows)
    else:
        _write_csv(output_csv, [])

    status = {
        "generated_at": _now(),
        "status": status_value,
        "reason": reason,
        "round_type": "PREREGISTRATION_SOURCE_RECONSTRUCTION_ONLY",
        "performance_calculated": False,
        "candidate_selection_calculated": False,
        "operational_change": False,
        "broker_order": False,
        "start": start,
        "end": end,
        "frequency": args.frequency,
        "markets": markets,
        "min_rows_per_date": min_rows,
        "rows": int(len(rows)),
        "unique_dates": int(df["as_of_date"].nunique()) if not df.empty else 0,
        "unique_codes": int(df["code"].nunique()) if not df.empty else 0,
        "counts_by_date": counts_by_date,
        "low_count_dates": low_dates,
        "source_checks": checks,
        "schema": ["as_of_date", "code", "name", "market", "security_type", "security_type_rule", "source", "source_status"],
        "security_type_limitation": "pykrx ticker list does not provide an explicit security-type field here; security_type is name-heuristic and must be treated as research-only unless replaced by an official field.",
        "output_csv": str(output_csv),
        "output_sha256": _sha256_file(output_csv),
    }
    meta = {
        "generated_at": status["generated_at"],
        "source": "pykrx.stock.get_market_ticker_list",
        "contract": "as_of_date x code x market with heuristic security_type",
        "status_path": str(OUT_STATUS_LATEST),
        "output_csv": str(output_csv),
        "output_sha256": status["output_sha256"],
    }
    _write_json(OUT_STATUS_LATEST, status)
    _write_json(OUT_META_LATEST, meta)
    _write_md(OUT_MD_LATEST, status)
    print(json.dumps({"status": status_value, "reason": reason, "rows": len(rows), "output_csv": str(output_csv)}, ensure_ascii=False))
    return 0 if status_value == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
