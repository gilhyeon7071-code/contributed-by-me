# -*- coding: utf-8 -*-
"""
Build DART fundamental snapshot and save to _cache/dart_fundamental_latest.csv.

Default behavior:
- Reads API key from DART_API_KEY env, --api-key-file, or _cache/dart_api_key.txt
- Picks target codes from:
  1) --codes-file (if provided)
  2) 2_Logs/candidates_latest_data.csv
  3) 2_Logs/candidates_latest_data.with_final_score.csv
  4) _cache/krx_listing.csv
- Fetches annual statements (reprt_code=11011) with year fallback.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import time
import zipfile
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

BASE_DIR = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
CACHE_DIR = BASE_DIR / "_cache"
LOG_DIR = BASE_DIR / "2_Logs"

CORP_MAP_PATH = CACHE_DIR / "dart_corp_code_map.csv"
DEFAULT_KEY_FILE = CACHE_DIR / "dart_api_key.txt"
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
FNLTT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"

FATAL_STATUS = {"010", "011", "012", "013", "014", "020", "800", "900"}

METRIC_RULES = {
    "revenue": {
        "ids": ["ifrs-full_Revenue", "ifrs_Revenue"],
        "names": ["Revenue", "Sales", "매출액", "영업수익"],
    },
    "operating_profit": {
        "ids": ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        "names": ["Operating income", "영업이익"],
    },
    "net_income": {
        "ids": ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"],
        "names": ["Profit", "Net income", "당기순이익", "순이익"],
    },
    "equity": {
        "ids": ["ifrs-full_Equity", "ifrs_Equity"],
        "names": ["Equity", "자본총계", "자본"],
    },
    "assets": {
        "ids": ["ifrs-full_Assets", "ifrs_Assets"],
        "names": ["Assets", "자산총계", "자산"],
    },
    "liabilities": {
        "ids": ["ifrs-full_Liabilities", "ifrs_Liabilities"],
        "names": ["Liabilities", "부채총계", "부채"],
    },
    "current_assets": {
        "ids": ["ifrs-full_CurrentAssets", "ifrs_CurrentAssets"],
        "names": ["Current assets", "유동자산"],
    },
    "current_liabilities": {
        "ids": ["ifrs-full_CurrentLiabilities", "ifrs_CurrentLiabilities"],
        "names": ["Current liabilities", "유동부채"],
    },
    "interest_cost": {
        "ids": ["ifrs-full_FinanceCosts", "ifrs_FinanceCosts"],
        "names": ["Finance costs", "Interest expense", "이자비용", "금융비용"],
    },
}


def _norm_code6(v: object) -> str:
    s = str(v).strip()
    s = "".join(ch for ch in s if ch.isalnum())
    if s.isdigit():
        return s.zfill(6)
    return s.upper()


def _to_float(v: object) -> float:
    if v is None:
        return np.nan
    s = str(v).strip().replace(",", "")
    if s == "" or s in {"-", "--", "N/A", "nan", "None"}:
        return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan


def _safe_ratio(num: float, den: float, scale: float = 100.0) -> float:
    if not np.isfinite(num) or not np.isfinite(den) or abs(den) < 1e-12:
        return np.nan
    return float((num / den) * scale)


def _safe_growth(cur: float, prev: float) -> float:
    if not np.isfinite(cur) or not np.isfinite(prev) or abs(prev) < 1e-12:
        return np.nan
    return float(((cur - prev) / abs(prev)) * 100.0)


def _resolve_api_key(api_key: str, api_key_file: str) -> str:
    k = str(api_key or "").strip()
    if k:
        return k

    k = str(os.environ.get("DART_API_KEY", "")).strip()
    if k:
        return k

    if api_key_file:
        p = Path(api_key_file)
        if p.exists():
            k = p.read_text(encoding="utf-8").replace("\ufeff", "").strip()
            if k:
                return k

    if DEFAULT_KEY_FILE.exists():
        k = DEFAULT_KEY_FILE.read_text(encoding="utf-8").replace("\ufeff", "").strip()
        if k:
            return k

    return ""


def _load_codes_from_path(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, dtype={"code": str, "ticker": str, "종목코드": str})
    except Exception:
        return []

    code_col = next((c for c in ["code", "ticker", "종목코드"] if c in df.columns), None)
    if code_col is None:
        return []

    ser = df[code_col].astype(str).map(_norm_code6)
    ser = ser[ser.str.match(r"^[0-9]{6}$", na=False)]
    return sorted(ser.drop_duplicates().tolist())


def _load_target_codes(codes_file: str | None) -> tuple[list[str], str]:
    sources: list[Path] = []
    if codes_file:
        sources.append(Path(codes_file))
    sources.extend(
        [
            LOG_DIR / "candidates_latest_data.csv",
            LOG_DIR / "candidates_latest_data.with_final_score.csv",
            CACHE_DIR / "krx_listing.csv",
        ]
    )

    for src in sources:
        codes = _load_codes_from_path(src)
        if codes:
            return codes, str(src)

    return [], ""


def _load_or_refresh_corp_map(api_key: str, force_refresh: bool = False) -> pd.DataFrame:
    if CORP_MAP_PATH.exists() and (not force_refresh):
        try:
            df = pd.read_csv(CORP_MAP_PATH, dtype={"code": str, "corp_code": str})
            if not df.empty and {"code", "corp_code"}.issubset(df.columns):
                df["code"] = df["code"].map(_norm_code6)
                df["corp_code"] = df["corp_code"].astype(str).str.strip()
                return df[["code", "corp_code", "corp_name", "modify_date"]].drop_duplicates("code")
        except Exception:
            pass

    r = requests.get(CORP_CODE_URL, params={"crtfc_key": api_key}, timeout=40)
    r.raise_for_status()

    payload = r.content
    xml_bytes = b""
    try:
        zf = zipfile.ZipFile(io.BytesIO(payload))
        xml_name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
        if not xml_name:
            raise RuntimeError("corpCode.xml payload missing xml")
        xml_bytes = zf.read(xml_name)
    except zipfile.BadZipFile:
        if payload.lstrip().startswith(b"<"):
            xml_bytes = payload
        else:
            msg = payload[:240].decode("utf-8", errors="ignore")
            try:
                j = json.loads(payload.decode("utf-8", errors="ignore"))
                st = str(j.get("status") or "")
                ms = str(j.get("message") or "")
                raise RuntimeError(f"corpCode request failed: status={st} message={ms}")
            except Exception:
                raise RuntimeError(f"corpCode response not zip/xml: {msg}")

    root = ET.fromstring(xml_bytes)
    rows = []
    for node in root.findall(".//list"):
        corp_code = str(node.findtext("corp_code") or "").strip()
        corp_name = str(node.findtext("corp_name") or "").strip()
        stock_code = _norm_code6(node.findtext("stock_code") or "")
        modify_date = str(node.findtext("modify_date") or "").strip()
        if len(stock_code) == 6 and stock_code.isdigit() and corp_code:
            rows.append(
                {
                    "code": stock_code,
                    "corp_code": corp_code,
                    "corp_name": corp_name,
                    "modify_date": modify_date,
                }
            )

    if not rows:
        raise RuntimeError("corpCode.xml parsed but no stock_code rows")

    out = pd.DataFrame(rows).drop_duplicates("code")
    out.to_csv(CORP_MAP_PATH, index=False, encoding="utf-8-sig")
    return out


def _fetch_single_statement(
    session: requests.Session,
    api_key: str,
    corp_code: str,
    bsns_year: int,
    fs_div: str,
    reprt_code: str = "11011",
) -> dict:
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(int(bsns_year)),
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    r = session.get(FNLTT_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _pick_amount(rows: list[dict], metric_key: str) -> tuple[float, float]:
    rule = METRIC_RULES[metric_key]
    ids = {x.strip() for x in rule["ids"]}
    names = [x.lower() for x in rule["names"]]

    for row in rows:
        account_id = str(row.get("account_id") or "").strip()
        account_nm = str(row.get("account_nm") or "").strip()
        account_nm_l = account_nm.lower()

        matched = (account_id in ids) or any(k in account_nm_l for k in names)
        if not matched:
            continue

        cur = _to_float(row.get("thstrm_amount"))
        prev = _to_float(row.get("frmtrm_amount"))
        if np.isfinite(cur) or np.isfinite(prev):
            return cur, prev

    return np.nan, np.nan


def _fetch_with_year_fallback(
    session: requests.Session,
    api_key: str,
    corp_code: str,
    years: list[int],
    sleep_sec: float,
) -> tuple[int, str, list[dict], str, str] | None:
    for y in years:
        for fs_div in ("CFS", "OFS"):
            data = _fetch_single_statement(session, api_key, corp_code, y, fs_div=fs_div)
            status = str(data.get("status") or "")
            message = str(data.get("message") or "")
            rows = data.get("list") or []

            if status == "000" and rows:
                return int(y), fs_div, rows, status, message

            if status in FATAL_STATUS and status != "013":
                raise RuntimeError(f"DART status={status} message={message}")

            if sleep_sec > 0:
                time.sleep(sleep_sec)

    return None


def _build_metric_row(
    code: str,
    corp_code: str,
    corp_name: str,
    fs_year: int,
    fs_div: str,
    rows: list[dict],
    as_of_ymd: str,
) -> dict:
    rev_cur, rev_prev = _pick_amount(rows, "revenue")
    op_cur, op_prev = _pick_amount(rows, "operating_profit")
    np_cur, np_prev = _pick_amount(rows, "net_income")
    eq_cur, _ = _pick_amount(rows, "equity")
    as_cur, _ = _pick_amount(rows, "assets")
    liab_cur, _ = _pick_amount(rows, "liabilities")
    ca_cur, _ = _pick_amount(rows, "current_assets")
    cl_cur, _ = _pick_amount(rows, "current_liabilities")
    int_cost_cur, _ = _pick_amount(rows, "interest_cost")

    revenue_growth = _safe_growth(rev_cur, rev_prev)
    op_growth = _safe_growth(op_cur, op_prev)
    np_growth = _safe_growth(np_cur, np_prev)

    roe = _safe_ratio(np_cur, eq_cur)
    roa = _safe_ratio(np_cur, as_cur)
    opm = _safe_ratio(op_cur, rev_cur)
    npm = _safe_ratio(np_cur, rev_cur)
    debt_ratio = _safe_ratio(liab_cur, eq_cur)
    current_ratio = _safe_ratio(ca_cur, cl_cur)

    if np.isfinite(op_cur) and np.isfinite(int_cost_cur) and abs(int_cost_cur) > 1e-12:
        interest_coverage = float(op_cur / abs(int_cost_cur))
    else:
        interest_coverage = np.nan

    return {
        "code": code,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "as_of_ymd": str(as_of_ymd),
        "dart_fs_year": int(fs_year),
        "dart_fs_div": fs_div,
        "revenue": rev_cur,
        "operating_profit": op_cur,
        "net_income": np_cur,
        "equity": eq_cur,
        "assets": as_cur,
        "liabilities": liab_cur,
        "current_assets": ca_cur,
        "current_liabilities": cl_cur,
        "interest_cost": int_cost_cur,
        "revenue_growth": revenue_growth,
        "op_growth": op_growth,
        "np_growth": np_growth,
        "ROE": roe,
        "ROA": roa,
        "OPM": opm,
        "NPM": npm,
        "debt_ratio": debt_ratio,
        "current_ratio": current_ratio,
        "interest_coverage": interest_coverage,
        "dart_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _merge_with_existing(output_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if not output_path.exists():
        return new_df.copy()

    try:
        old = pd.read_csv(output_path, dtype={"code": str, "corp_code": str})
        old["code"] = old["code"].map(_norm_code6)
    except Exception:
        old = pd.DataFrame(columns=["code"])

    if new_df.empty:
        return old

    keep_old = old[~old["code"].isin(new_df["code"].tolist())].copy() if "code" in old.columns else old
    merged = pd.concat([keep_old, new_df], ignore_index=True, sort=False)
    if "code" in merged.columns:
        merged = merged.drop_duplicates("code", keep="last")
    return merged


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build DART fundamental snapshot")
    ap.add_argument("--api-key", default="", help="DART API key (optional if env/file exists)")
    ap.add_argument("--api-key-file", default="", help="Path to file containing DART API key")
    ap.add_argument("--codes-file", default="", help="CSV file with code/ticker column")
    ap.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"), help="As-of date YYYYMMDD")
    ap.add_argument("--bsns-year", type=int, default=0, help="Preferred business year")
    ap.add_argument("--max-codes", type=int, default=300, help="Max number of target codes")
    ap.add_argument("--sleep", type=float, default=0.15, help="Sleep seconds between API calls")
    ap.add_argument("--corp-refresh", action="store_true", help="Force refresh corp code map")
    ap.add_argument("--output", default=str(CACHE_DIR / "dart_fundamental_latest.csv"), help="Output CSV path")
    ap.add_argument("--meta-output", default=str(CACHE_DIR / "dart_fundamental_meta.json"), help="Meta JSON path")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    api_key = _resolve_api_key(args.api_key, args.api_key_file)
    if not api_key:
        print("[ERR] DART API key not found. Set DART_API_KEY or --api-key-file.")
        return 2

    codes, code_src = _load_target_codes(args.codes_file or None)
    if not codes:
        print("[ERR] No target codes found from codes-file/candidate/listing source.")
        return 2

    if args.max_codes > 0:
        codes = codes[: int(args.max_codes)]

    corp_map = _load_or_refresh_corp_map(api_key, force_refresh=bool(args.corp_refresh))
    corp_map = corp_map.drop_duplicates("code")

    code_to_corp = dict(zip(corp_map["code"], corp_map["corp_code"]))
    code_to_name = dict(zip(corp_map["code"], corp_map["corp_name"]))

    mapped_codes = [c for c in codes if c in code_to_corp]
    unmapped = [c for c in codes if c not in code_to_corp]

    as_of_ymd = str(args.as_of)
    try:
        as_of_year = int(as_of_ymd[:4])
    except Exception:
        as_of_year = datetime.now().year

    years = []
    if int(args.bsns_year or 0) > 0:
        years.append(int(args.bsns_year))
    for y in [as_of_year - 1, as_of_year - 2, as_of_year - 3]:
        if y > 0 and y not in years:
            years.append(y)

    print(f"[DART] code_source={code_src} requested={len(codes)} mapped={len(mapped_codes)} unmapped={len(unmapped)}")
    print(f"[DART] years={years} max_codes={args.max_codes} sleep={args.sleep}")

    session = requests.Session()
    rows_out: list[dict] = []
    no_data = 0
    errors = 0

    for i, code in enumerate(mapped_codes, start=1):
        corp_code = code_to_corp.get(code, "")
        corp_name = code_to_name.get(code, "")
        try:
            payload = _fetch_with_year_fallback(session, api_key, corp_code, years, sleep_sec=float(args.sleep))
            if payload is None:
                no_data += 1
            else:
                fs_year, fs_div, rows, _, _ = payload
                rows_out.append(
                    _build_metric_row(
                        code=code,
                        corp_code=corp_code,
                        corp_name=corp_name,
                        fs_year=fs_year,
                        fs_div=fs_div,
                        rows=rows,
                        as_of_ymd=as_of_ymd,
                    )
                )
        except Exception as e:
            errors += 1
            print(f"[WARN] code={code} corp={corp_code} err={type(e).__name__}: {e}")

        if i % 20 == 0 or i == len(mapped_codes):
            print(f"[DART] progress {i}/{len(mapped_codes)} rows={len(rows_out)} no_data={no_data} errors={errors}")

    new_df = pd.DataFrame(rows_out)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    merged = _merge_with_existing(out_path, new_df)
    if not merged.empty and "code" in merged.columns:
        merged["code"] = merged["code"].map(_norm_code6)
        merged = merged.sort_values("code").reset_index(drop=True)

    merged.to_csv(out_path, index=False, encoding="utf-8-sig")

    dated_path = CACHE_DIR / f"dart_fundamental_{as_of_ymd}.csv"
    merged.to_csv(dated_path, index=False, encoding="utf-8-sig")

    meta = {
        "as_of_ymd": as_of_ymd,
        "code_source": code_src,
        "requested_codes": int(len(codes)),
        "mapped_codes": int(len(mapped_codes)),
        "unmapped_codes": int(len(unmapped)),
        "rows_fetched_this_run": int(len(new_df)),
        "rows_output_total": int(len(merged)),
        "no_data": int(no_data),
        "errors": int(errors),
        "years": years,
        "output": str(out_path),
        "dated_output": str(dated_path),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    meta_path = Path(args.meta_output)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[DART] done rows_fetched={0} rows_total={1} no_data={2} errors={3}".format(
            len(new_df), len(merged), no_data, errors
        )
    )
    print(f"[DART] output={out_path}")
    print(f"[DART] meta={meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

