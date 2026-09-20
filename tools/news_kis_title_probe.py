from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
LOGS = ROOT / "2_Logs"
DEFAULT_CANDIDATES = LOGS / "candidates_latest_data.with_final_score.csv"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"
DB_TABLE = "news_articles_kis_title"
API_PATH = "/uapi/domestic-stock/v1/quotations/news-title"
TR_ID = "FHKST01011800"

if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from kis_order_client import KISApiError, KISOrderClient  # noqa: E402


def _now() -> dt.datetime:
    return dt.datetime.now()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _read_candidates(path: Path, max_symbols: int) -> List[Tuple[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"candidate file not found: {path}")
    rows: List[Tuple[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            code = str(r.get("code") or r.get("ticker") or "").strip()
            name = str(r.get("name") or r.get("symbol") or "").strip()
            digits = "".join(ch for ch in code if ch.isdigit())
            if len(digits) < 6:
                continue
            code6 = digits[-6:]
            rows.append((code6, name))
            if len(rows) >= max(1, int(max_symbols)):
                break
    return rows


def _ensure_db(con: sqlite3.Connection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DB_TABLE} (
            kis_id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT,
            data_dt TEXT,
            data_tm TEXT,
            title TEXT,
            provider_code TEXT,
            news_lrdv_code TEXT,
            source_org TEXT,
            iscd1 TEXT,
            iscd2 TEXT,
            iscd3 TEXT,
            iscd4 TEXT,
            iscd5 TEXT,
            collected_at TEXT NOT NULL
        )
        """
    )
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_code_dt ON {DB_TABLE}(code, data_dt, data_tm)")


def _save_rows_to_db(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    NEWS_DB.parent.mkdir(parents=True, exist_ok=True)
    collected_at = _now().isoformat(timespec="seconds")
    saved = 0
    with sqlite3.connect(str(NEWS_DB), timeout=30.0) as con:
        con.execute("PRAGMA busy_timeout=30000")
        _ensure_db(con)
        for row in rows:
            kis_id = "|".join(
                [
                    str(row.get("code") or ""),
                    str(row.get("data_dt") or ""),
                    str(row.get("data_tm") or ""),
                    str(row.get("provider_code") or ""),
                    str(row.get("title") or ""),
                ]
            )
            cur = con.execute(
                f"""
                INSERT OR IGNORE INTO {DB_TABLE} (
                    kis_id, code, name, data_dt, data_tm, title, provider_code,
                    news_lrdv_code, source_org, iscd1, iscd2, iscd3, iscd4, iscd5, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kis_id,
                    str(row.get("code") or ""),
                    str(row.get("name") or ""),
                    str(row.get("data_dt") or ""),
                    str(row.get("data_tm") or ""),
                    str(row.get("title") or ""),
                    str(row.get("provider_code") or ""),
                    str(row.get("news_lrdv_code") or ""),
                    str(row.get("source_org") or ""),
                    str(row.get("iscd1") or ""),
                    str(row.get("iscd2") or ""),
                    str(row.get("iscd3") or ""),
                    str(row.get("iscd4") or ""),
                    str(row.get("iscd5") or ""),
                    collected_at,
                ),
            )
            saved += int(cur.rowcount or 0)
    return saved


def _items_from_output(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    output = body.get("output")
    if output is None:
        output = body.get("output1")
    if isinstance(output, list):
        return [x for x in output if isinstance(x, dict)]
    if isinstance(output, dict):
        return [output]
    return []


def _mock_arg(raw: str) -> Optional[bool]:
    val = str(raw or "auto").strip().lower()
    if val == "auto":
        return None
    return val in {"1", "true", "y", "yes"}


def _build_params(
    *,
    code: str,
    provider_code: str,
    market_cls: str,
    title_keyword: str,
    input_date: str,
    input_hour: str,
    rank_sort: str,
    input_srno: str,
) -> Dict[str, str]:
    return {
        "FID_NEWS_OFER_ENTP_CODE": str(provider_code or ""),
        "FID_COND_MRKT_CLS_CODE": str(market_cls or ""),
        "FID_INPUT_ISCD": str(code or "").zfill(6),
        "FID_TITL_CNTT": str(title_keyword or ""),
        "FID_INPUT_DATE_1": str(input_date or ""),
        "FID_INPUT_HOUR_1": str(input_hour or ""),
        "FID_RANK_SORT_CLS_CODE": str(rank_sort or ""),
        "FID_INPUT_SRNO": str(input_srno or ""),
    }


def _fetch_for_code(
    client: KISOrderClient,
    *,
    code: str,
    name: str,
    args: argparse.Namespace,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    tr_cont = ""
    for depth in range(max(1, int(args.max_pages))):
        params = _build_params(
            code=code,
            provider_code=args.provider_code,
            market_cls=args.market_cls,
            title_keyword=args.title_keyword,
            input_date=args.input_date,
            input_hour=args.input_hour,
            rank_sort=args.rank_sort,
            input_srno=args.input_srno,
        )
        try:
            headers = client._auth_headers(tr_id=TR_ID, tr_cont=tr_cont)
            body, response_headers = client._request_json("GET", API_PATH, headers=headers, params=params)
        except KISApiError as e:
            errors.append({"code": code, "name": name, "error": e.to_dict()})
            break
        except Exception as e:
            errors.append({"code": code, "name": name, "error": f"{type(e).__name__}:{e}"})
            break

        for item in _items_from_output(body):
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "data_dt": str(item.get("data_dt") or ""),
                    "data_tm": str(item.get("data_tm") or ""),
                    "title": str(item.get("hts_pbnt_titl_cntt") or ""),
                    "provider_code": str(item.get("news_ofer_entp_code") or ""),
                    "news_lrdv_code": str(item.get("news_lrdv_code") or ""),
                    "source_org": str(item.get("dorg") or ""),
                    "iscd1": str(item.get("iscd1") or ""),
                    "iscd2": str(item.get("iscd2") or ""),
                    "iscd3": str(item.get("iscd3") or ""),
                    "iscd4": str(item.get("iscd4") or ""),
                    "iscd5": str(item.get("iscd5") or ""),
                }
            )

        next_cont = str(response_headers.get("tr_cont") or "").strip()
        if next_cont != "M":
            break
        tr_cont = "N"
    return rows, errors


def main(argv: Optional[Iterable[str]] = None) -> int:
    today = _now().strftime("%Y%m%d")
    ap = argparse.ArgumentParser(description="Read-only KIS domestic news-title probe")
    ap.add_argument("--candidate-csv", default=str(DEFAULT_CANDIDATES))
    ap.add_argument("--max-symbols", type=int, default=10)
    ap.add_argument("--mock", choices=["auto", "true", "false"], default="auto")
    ap.add_argument("--dry-run", action="store_true", help="Build request payload only; do not call KIS")
    ap.add_argument("--write-db", action="store_true", help=f"Write rows to {DB_TABLE}")
    ap.add_argument("--provider-code", default="")
    ap.add_argument("--market-cls", default="")
    ap.add_argument("--title-keyword", default="")
    ap.add_argument("--input-date", default=today)
    ap.add_argument("--input-hour", default="")
    ap.add_argument("--rank-sort", default="")
    ap.add_argument("--input-srno", default="")
    ap.add_argument("--max-pages", type=int, default=1)
    ap.add_argument("--out-json", default="")
    args = ap.parse_args(list(argv) if argv is not None else None)

    started = _now()
    out_path = Path(args.out_json) if args.out_json else LOGS / f"kis_news_title_probe_{today}.json"
    latest_path = LOGS / "kis_news_title_probe_latest.json"

    status: Dict[str, Any] = {
        "ts": started.isoformat(timespec="seconds"),
        "source": "kis_openapi_news_title",
        "mode": "dry_run" if args.dry_run else "live_probe",
        "api_path": API_PATH,
        "tr_id": TR_ID,
        "db_write": bool(args.write_db),
        "db_path": str(NEWS_DB),
        "db_table": DB_TABLE,
        "candidate_csv": str(Path(args.candidate_csv)),
        "out_json": str(out_path),
        "latest_json": str(latest_path),
    }

    try:
        symbols = _read_candidates(Path(args.candidate_csv), int(args.max_symbols))
        status["symbols"] = [{"code": code, "name": name} for code, name in symbols]
        if args.dry_run:
            status["quality"] = "PASS"
            status["reason"] = "dry_run_ok"
            status["sample_params"] = (
                _build_params(
                    code=symbols[0][0],
                    provider_code=args.provider_code,
                    market_cls=args.market_cls,
                    title_keyword=args.title_keyword,
                    input_date=args.input_date,
                    input_hour=args.input_hour,
                    rank_sort=args.rank_sort,
                    input_srno=args.input_srno,
                )
                if symbols
                else {}
            )
            status["rows"] = []
            status["errors"] = []
        else:
            client = KISOrderClient.from_env(mock=_mock_arg(args.mock))
            all_rows: List[Dict[str, Any]] = []
            all_errors: List[Dict[str, Any]] = []
            for code, name in symbols:
                rows, errors = _fetch_for_code(client, code=code, name=name, args=args)
                all_rows.extend(rows)
                all_errors.extend(errors)
            status["quality"] = "PASS" if all_rows and not all_errors else ("WARN" if all_rows else "FAIL")
            status["reason"] = "ok" if status["quality"] == "PASS" else ("partial_errors" if all_rows else "no_rows")
            status["row_count"] = len(all_rows)
            status["error_count"] = len(all_errors)
            status["db_rows_saved"] = _save_rows_to_db(all_rows) if args.write_db else 0
            status["rows"] = all_rows
            status["errors"] = all_errors
    except Exception as e:
        status["quality"] = "FAIL"
        status["reason"] = f"{type(e).__name__}:{e}"
        status.setdefault("rows", [])
        status.setdefault("errors", [])

    _write_json(out_path, status)
    _write_json(latest_path, status)
    print(json.dumps(_json_safe(status), ensure_ascii=False, indent=2))
    return 0 if str(status.get("quality")) in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
