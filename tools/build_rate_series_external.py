from __future__ import annotations

import json
import io
import os
import csv
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import logging
ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
LOGS.mkdir(parents=True, exist_ok=True)

FRED_SERIES = [
    "FEDFUNDS",      # Fed funds rate
    "DGS10",         # US 10Y
    "DGS2",          # US 2Y
    "DTB3",          # US 3M T-bill
    "CPIAUCSL",      # US CPI index
    "CPILFESL",      # US Core CPI index
    "UNRATE",        # US unemployment
    "NAPM",          # ISM manufacturing PMI
    "NAPMNONMFG",    # ISM non-manufacturing PMI
    "DCOILWTICO",    # WTI spot
    "DCOILBRENTEU",  # Brent spot
    "VIXCLS",        # VIX
    "BAMLH0A0HYM2",  # HY OAS spread
    "SP500",         # S&P500 index
    "NASDAQCOM",     # Nasdaq Composite index
    "NASDAQSOX",     # PHLX Semiconductor index (SOX)
    "DCOPPER",       # Copper spot ($/lb)
    "DTWEXBGS",      # Broad dollar index (trade-weighted)
    "DEXCHUS",       # USD/CNY (중국 위안화 → CNY/KRW 파생)
    "MANEXIMCN",     # China Caixin Manufacturing PMI
    "M2SL",          # US M2 money stock
    "WPU101",        # US PPI iron and steel proxy
]
FRED_FALLBACKS: Dict[str, List[str]] = {
    # FRED does not expose DCOPPER through the observations endpoint; use the
    # official IMF/FRED global copper price series as the copper proxy.
    "DCOPPER": ["PCOPPUSDM"],
}

CRITICAL_ARCHIVE_REUSE_SERIES = {"VIXCLS", "BAMLH0A0HYM2"}

SERIES_MAX_AGE_DAYS: Dict[str, int] = {
    # FRED dollar/CNY daily series can publish later than equity/rate series.
    # Keep this grace limited to these two forecast macro inputs.
    "DTWEXBGS": 7,
    "DEXCHUS": 7,
    # These are monthly macro inputs. PCOPPUSDM is used as the official proxy
    # for DCOPPER, and both it and M2SL can publish with a longer release lag
    # than the generic monthly 45-day freshness window.
    "DCOPPER": 75,
    "M2SL": 75,
}

INDICATOR_SOURCE_MAP: List[Dict[str, str]] = [
    {"indicator": "US CPI", "series_id": "CPIAUCSL", "source": "FRED", "update_cycle": "monthly", "purpose": "regime helper / rate direction"},
    {"indicator": "US Core CPI", "series_id": "CPILFESL", "source": "FRED", "update_cycle": "monthly", "purpose": "regime helper / rate direction"},
    {"indicator": "US Unemployment", "series_id": "UNRATE", "source": "FRED", "update_cycle": "monthly", "purpose": "growth assessment"},
    {"indicator": "US ISM Manufacturing", "series_id": "NAPM", "source": "FRED", "update_cycle": "monthly", "purpose": "leading signal"},
    {"indicator": "US ISM Service", "series_id": "NAPMNONMFG", "source": "FRED", "update_cycle": "monthly", "purpose": "leading signal"},
    {"indicator": "WTI Spot", "series_id": "DCOILWTICO", "source": "FRED", "update_cycle": "daily", "purpose": "energy shock monitor"},
    {"indicator": "Brent Spot", "series_id": "DCOILBRENTEU", "source": "FRED", "update_cycle": "daily", "purpose": "energy shock monitor"},
    {"indicator": "VIX", "series_id": "VIXCLS", "source": "Yahoo/FRED", "update_cycle": "daily", "purpose": "risk filter"},
    {"indicator": "HY Spread", "series_id": "BAMLH0A0HYM2", "source": "FRED", "update_cycle": "daily", "purpose": "credit risk"},
    {"indicator": "KR Base Rate", "series_id": "BOK_BASE_RATE", "source": "BOK API(ECOS)", "update_cycle": "monthly", "purpose": "domestic demand sensitivity"},
    {"indicator": "KR CPI", "series_id": "KR_CPI_INDEX", "source": "KOSIS(ECOS fallback)", "update_cycle": "monthly", "purpose": "inflation assessment"},
    {"indicator": "KR Export", "series_id": "KR_EXPORT_VALUE", "source": "Customs/KDI(ECOS fallback)", "update_cycle": "monthly", "purpose": "export momentum"},
    {"indicator": "S&P 500", "series_id": "SP500", "source": "FRED", "update_cycle": "daily", "purpose": "global risk appetite / KOSPI correlation"},
    {"indicator": "Nasdaq Composite", "series_id": "NASDAQCOM", "source": "FRED", "update_cycle": "daily", "purpose": "growth stock risk appetite / Korea tech coupling"},
    {"indicator": "SOX", "series_id": "NASDAQSOX", "source": "FRED(Nasdaq)", "update_cycle": "daily", "purpose": "semiconductor sector leading/linked indicator"},
    {"indicator": "Copper Spot", "series_id": "DCOPPER", "source": "FRED", "update_cycle": "daily", "purpose": "global growth leading indicator"},
    {"indicator": "USD Broad Index", "series_id": "DTWEXBGS", "source": "FRED", "update_cycle": "daily", "purpose": "dollar strength / EM capital flow"},
    {"indicator": "USD/CNY", "series_id": "DEXCHUS", "source": "FRED", "update_cycle": "daily", "purpose": "CNY/KRW derived — China export competition"},
    {"indicator": "China Mfg PMI", "series_id": "MANEXIMCN", "source": "FRED(Caixin)", "update_cycle": "monthly", "purpose": "China demand — Korea export 25%+"},
    {"indicator": "US M2 Money Stock", "series_id": "M2SL", "source": "FRED", "update_cycle": "monthly", "purpose": "liquidity expansion/contraction"},
    {"indicator": "US Iron and Steel PPI", "series_id": "WPU101", "source": "FRED(BLS)", "update_cycle": "monthly", "purpose": "steel price proxy for construction/shipbuilding/auto"},
    {"indicator": "Put/Call Ratio", "series_id": "CBOE_PUT_CALL_TOTAL", "source": "CBOE Daily Market Statistics", "update_cycle": "daily", "purpose": "sentiment/fear reference"},
    {"indicator": "CNN Fear & Greed", "series_id": "CNN_FEAR_GREED_OFFICIAL_NOT_CONFIGURED", "source": "not_configured", "update_cycle": "daily", "purpose": "sentiment composite reference"},
    {"indicator": "BDI", "series_id": "BDI_INVESTING_WEB", "source": "Investing.com web fallback", "update_cycle": "daily", "purpose": "global dry bulk trade leading indicator"},
    {"indicator": "KR KTB 3Y", "series_id": "KR_KTB3Y", "source": "BOK API(ECOS)", "update_cycle": "daily", "purpose": "domestic rate environment / credit spread"},
]

KOSIS_CPI_CANDIDATES: List[Dict[str, str]] = [
    {
        "orgId": "101",
        "tblId": "DT_1J20001",
        "itmId": "T10",
        "objL1": "0",
        "prdSe": "M",
        "newIndex": "1",
    },
    {"orgId": "101", "tblId": "DT_1J15001", "itmId": "ALL", "objL1": "ALL", "prdSe": "M", "newIndex": "1"},
    {"orgId": "101", "tblId": "DT_1J15002", "itmId": "ALL", "objL1": "ALL", "prdSe": "M", "newIndex": "1"},
    {"orgId": "101", "tblId": "DT_1J22001", "itmId": "ALL", "objL1": "ALL", "prdSe": "M", "newIndex": "1"},
    {"orgId": "101", "tblId": "DT_1J22003", "itmId": "ALL", "objL1": "ALL", "prdSe": "M", "newIndex": "1"},
]

PUBLIC_EXPORT_ENDPOINTS: List[str] = [
    "http://apis.data.go.kr/1220000/nationtrade/getNationtradeList",
]

US_TREASURY_SERIES_COLUMN_MAP: Dict[str, str] = {
    "DGS10": "10 Yr",
    "DGS2": "2 Yr",
    "DTB3": "3 Mo",
}




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _today_ymd() -> str:
    return date.today().strftime("%Y%m%d")


def _derive_d_from_fills() -> str:
    p = ROOT / "paper" / "fills.csv"
    if not p.exists():
        return ""
    try:
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        try:
            with p.open("r", encoding="cp949", newline="") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            return ""
    buy_dates: List[str] = []
    any_dates: List[str] = []
    for r in rows:
        action = str(r.get("action") or r.get("side") or r.get("type") or "").upper()
        raw_dt = str(r.get("ymd") or r.get("date") or r.get("datetime") or r.get("timestamp") or "")
        d8 = "".join(ch for ch in raw_dt if ch.isdigit())[:8]
        if len(d8) != 8:
            continue
        any_dates.append(d8)
        if action == "BUY":
            buy_dates.append(d8)
    return max(buy_dates or any_dates or [""])


def _as_date(v: Any) -> Optional[pd.Timestamp]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s_digits = "".join(ch for ch in s if ch.isdigit())
    if len(s_digits) >= 8:
        try:
            return pd.Timestamp(datetime.strptime(s_digits[:8], "%Y%m%d").date())
        except Exception:
            pass
    if len(s_digits) >= 6:
        try:
            return pd.Timestamp(datetime.strptime(s_digits[:6] + "01", "%Y%m%d").date())
        except Exception:
            pass
    try:
        x = pd.to_datetime(s, errors="coerce")
        if pd.isna(x):
            return None
        return pd.Timestamp(x)
    except Exception:
        return None


def _env(*names: str) -> str:
    for n in names:
        v = str(os.environ.get(n, "")).replace("\ufeff", "").strip()
        if v:
            return v

    key_files = {
        "FRED_API_KEY": [ROOT / "_cache" / "fred_api_key.txt"],
        "ECOS_API_KEY": [ROOT / "_cache" / "ecos_api_key.txt", ROOT / "_cache" / "bok_api_key.txt"],
        "BOK_API_KEY": [ROOT / "_cache" / "bok_api_key.txt", ROOT / "_cache" / "ecos_api_key.txt"],
        "KOSIS_API_KEY": [ROOT / "_cache" / "kosis_api_key.txt"],
        "PUBLIC_DATA_API_KEY": [
            ROOT / "_cache" / "public_data_api_key.txt",
            ROOT / "_cache" / "data_go_kr_api_key.txt",
        ],
        "DATA_GO_KR_API_KEY": [
            ROOT / "_cache" / "data_go_kr_api_key.txt",
            ROOT / "_cache" / "public_data_api_key.txt",
        ],
    }

    for n in names:
        for p in key_files.get(n, []):
            try:
                if p.exists():
                    val = p.read_text(encoding="utf-8").replace("\ufeff", "").strip()
                    if val:
                        return val
            except Exception:
                pass
    return ""


def _fetch_fred_series(api_key: str, series_id: str, start_ymd: str, end_ymd: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": f"{start_ymd[:4]}-{start_ymd[4:6]}-{start_ymd[6:8]}",
        "observation_end": f"{end_ymd[:4]}-{end_ymd[4:6]}-{end_ymd[6:8]}",
        "sort_order": "asc",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    obj = r.json()
    rows = obj.get("observations") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    out: List[Dict[str, Any]] = []
    for it in rows:
        d = str((it or {}).get("date", "")).strip()
        v = str((it or {}).get("value", "")).strip()
        if not d or v in {"", "."}:
            continue
        try:
            vv = float(v)
        except Exception:
            continue
        dt = _as_date(d)
        if dt is None or pd.isna(dt):
            continue
        out.append(
            {
                "date": dt.strftime("%Y%m%d"),
                "value": vv,
                "source": f"fred:{series_id}",
                "series_id": series_id,
            }
        )
    if not out:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    return pd.DataFrame(out).sort_values("date").drop_duplicates(["date"], keep="last")


def _fetch_ecos_series(
    api_key: str,
    stat_code: str,
    cycle: str,
    start_period: str,
    end_period: str,
    item_code: str,
    series_id: str,
) -> pd.DataFrame:
    # /StatisticSearch/{KEY}/json/kr/1/10000/{STAT}/{CYCLE}/{START}/{END}/{ITEM}
    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/kr/1/10000/"
        f"{stat_code}/{cycle}/{start_period}/{end_period}/{item_code}"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    obj = r.json()
    sec = obj.get("StatisticSearch") if isinstance(obj, dict) else {}
    rows = sec.get("row") if isinstance(sec, dict) else None
    if not isinstance(rows, list):
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    out: List[Dict[str, Any]] = []
    for it in rows:
        d = (it or {}).get("TIME")
        v = (it or {}).get("DATA_VALUE")
        dt = _as_date(d)
        try:
            vv = float(v)
        except Exception:
            continue
        if dt is None or pd.isna(dt):
            continue
        out.append(
            {
                "date": dt.strftime("%Y%m%d"),
                "value": vv,
                "source": f"ecos:{stat_code}/{item_code}",
                "series_id": series_id,
            }
        )

    if not out:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    return pd.DataFrame(out).sort_values("date").drop_duplicates(["date"], keep="last")


def _fetch_us_treasury_daily_series(start_ymd: str, end_ymd: str) -> Dict[str, pd.DataFrame]:
    start_year = int(start_ymd[:4])
    end_year = int(end_ymd[:4])
    out_map: Dict[str, pd.DataFrame] = {}

    for year in range(start_year, end_year + 1):
        url = (
            "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
            f"daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
        )
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        raw = pd.read_csv(io.StringIO(r.text))
        if "Date" not in raw.columns:
            continue
        raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce")
        raw = raw.dropna(subset=["Date"])
        raw["date"] = raw["Date"].dt.strftime("%Y%m%d")
        raw = raw[(raw["date"] >= start_ymd) & (raw["date"] <= end_ymd)]
        if raw.empty:
            continue

        for sid, col in US_TREASURY_SERIES_COLUMN_MAP.items():
            if col not in raw.columns:
                continue
            df_sid = raw[["date", col]].copy()
            df_sid[col] = pd.to_numeric(df_sid[col], errors="coerce")
            df_sid = df_sid.dropna(subset=[col])
            if df_sid.empty:
                continue
            df_sid = df_sid.rename(columns={col: "value"})
            df_sid["source"] = f"us_treasury_daily:{col.replace(' ', '').lower()}"
            df_sid["series_id"] = sid
            if sid in out_map and not out_map[sid].empty:
                out_map[sid] = pd.concat([out_map[sid], df_sid], ignore_index=True)
            else:
                out_map[sid] = df_sid

    for sid in list(out_map.keys()):
        df_sid = out_map[sid]
        out_map[sid] = (
            df_sid.sort_values("date")
            .drop_duplicates(["date"], keep="last")
            .reset_index(drop=True)
        )
    return out_map



def _kosis_date_to_ymd(v: Any) -> Optional[str]:
    d = _as_date(v)
    if d is None or pd.isna(d):
        return None
    return d.strftime("%Y%m%d")


def _fetch_kosis_cpi_series(api_key: str, start_ym: str, end_ym: str, series_id: str = "KR_CPI_INDEX") -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
    rows_out: List[Dict[str, Any]] = []
    errors: List[str] = []

    for cand in KOSIS_CPI_CANDIDATES:
        params = {
            "method": "getList",
            "apiKey": api_key,
            "format": "json",
            "jsonVD": "Y",
            "vwCd": str(cand.get("vwCd") or "MT_ZTITLE"),
            "orgId": cand.get("orgId", "101"),
            "tblId": cand.get("tblId", ""),
            "prdSe": str(cand.get("prdSe") or "M"),
            "startPrdDe": start_ym,
            "endPrdDe": end_ym,
            "newIndex": str(cand.get("newIndex") or "1"),
        }
        params["itmId"] = str(cand.get("itmId") or "ALL")
        params["prdInterval"] = str(cand.get("prdInterval") or "1")
        params["newEstPrdCnt"] = str(cand.get("newEstPrdCnt") or "3")
        for i in range(1, 9):
            key = f"objL{i}"
            default_val = "" if i > 1 else "ALL"
            params[key] = str(cand.get(key) if cand.get(key) is not None else default_val)

        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            obj = r.json()
        except Exception as e:
            errors.append(f"{cand.get('tblId')}: {type(e).__name__}: {e}")
            continue

        if isinstance(obj, dict) and obj.get("err"):
            errors.append(f"{cand.get('tblId')}: {obj.get('err')}:{obj.get('errMsg')}")
            continue
        if not isinstance(obj, list):
            errors.append(f"{cand.get('tblId')}: unexpected_response")
            continue

        for it in obj:
            if not isinstance(it, dict):
                continue
            d = it.get("PRD_DE") or it.get("prdDe") or it.get("TIME")
            ymd = _kosis_date_to_ymd(d)
            vv = _safe_num(it.get("DT") or it.get("dt") or it.get("DATA_VALUE"))
            if ymd is None or vv is None:
                continue
            rows_out.append(
                {
                    "date": ymd,
                    "value": vv,
                    "source": f"kosis:{cand.get('orgId')}/{cand.get('tblId')}",
                    "series_id": series_id,
                }
            )

        if rows_out:
            break

    if not rows_out:
        if errors:
            raise RuntimeError("; ".join(errors[:5]))
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    return pd.DataFrame(rows_out).sort_values("date").drop_duplicates(["date"], keep="last")


def _extract_items_from_obj(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [it for it in obj if isinstance(it, dict)]
    if not isinstance(obj, dict):
        return []

    direct = obj.get("item")
    if isinstance(direct, list):
        return [it for it in direct if isinstance(it, dict)]
    if isinstance(direct, dict):
        return [direct]

    for k in ("response", "body", "items", "data", "list", "rows"):
        if k in obj:
            nested = _extract_items_from_obj(obj.get(k))
            if nested:
                return nested
    return []


def _extract_month_from_row(row: Dict[str, Any]) -> Optional[str]:
    for k in ("strtYymm", "endYymm", "yearMonth", "YEAR_MONTH", "YYMM", "yymm", "TRD_YM", "ym"):
        v = row.get(k)
        s = "".join(ch for ch in str(v or "") if ch.isdigit())
        if len(s) >= 6:
            return s[:6]

    y = None
    m = None
    for k in ("year", "YEAR", "yy"):
        sv = "".join(ch for ch in str(row.get(k) or "") if ch.isdigit())
        if len(sv) >= 6:
            return sv[:6]
        if len(sv) == 4:
            y = sv
            break
    for k in ("month", "MONTH", "mm"):
        sv = "".join(ch for ch in str(row.get(k) or "") if ch.isdigit())
        if 1 <= len(sv) <= 2:
            m = sv.zfill(2)
            break
    if y and m:
        return y + m

    for v in row.values():
        sv = "".join(ch for ch in str(v or "") if ch.isdigit())
        if len(sv) == 6:
            return sv
    return None


def _extract_export_value(row: Dict[str, Any]) -> Optional[float]:
    for k in ("expDlr", "EXP_DLR", "expCnt", "expWgt", "export", "EXP", "EXPTM"):
        if k in row:
            v = _safe_num(row.get(k))
            if v is not None:
                return v
    return None


def _fetch_public_export_series(api_key: str, start_ym: str, end_ym: str, series_id: str = "KR_EXPORT_VALUE") -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    rows_out: List[Dict[str, Any]] = []
    errors: List[str] = []
    try:
        start_year = int(start_ym[:4])
        end_year = int(end_ym[:4])
        request_ranges = [
            (max(start_ym, f"{year}01"), min(end_ym, f"{year}12"))
            for year in range(start_year, end_year + 1)
        ]
    except Exception:
        request_ranges = [(start_ym, end_ym)]

    for url in PUBLIC_EXPORT_ENDPOINTS:
        before_count = len(rows_out)
        for req_start_ym, req_end_ym in request_ranges:
            params = {
                "serviceKey": api_key,
                "strtYymm": req_start_ym,
                "endYymm": req_end_ym,
                "pageNo": "1",
                "numOfRows": "999",
                "resultType": "json",
            }
            text = ""
            try:
                r = requests.get(url, params=params, timeout=20)
                text = r.text or ""
                r.raise_for_status()
            except Exception as e:
                errors.append(f"{url}: {req_start_ym}-{req_end_ym}: {type(e).__name__}: {e}")
                continue

            items: List[Dict[str, Any]] = []
            try:
                obj = r.json()
                items = _extract_items_from_obj(obj)
            except Exception:
                pass

            if not items and text:
                try:
                    root = ET.fromstring(text)
                    for node in root.findall(".//item"):
                        row = {str(c.tag): (c.text or "") for c in node}
                        if row:
                            items.append(row)
                except Exception as ex:
                    errors.append(f"{url}: {req_start_ym}-{req_end_ym}: parse_error:{type(ex).__name__}")

            for it in items:
                yymm = _extract_month_from_row(it)
                vv = _extract_export_value(it)
                if yymm is None or vv is None:
                    continue
                try:
                    month_no = int(yymm[4:6])
                except Exception:
                    continue
                if len(yymm) != 6 or month_no < 1 or month_no > 12 or yymm < req_start_ym or yymm > req_end_ym:
                    continue
                rows_out.append(
                    {
                        "date": f"{yymm}01",
                        "value": vv,
                        "source": "public_data:1220000/nationtrade/getNationtradeList",
                        "series_id": series_id,
                    }
                )

        if len(rows_out) > before_count:
            break

    if not rows_out:
        if errors:
            raise RuntimeError("; ".join(errors[:5]))
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])

    df = pd.DataFrame(rows_out)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    if df.empty:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    grouped = (
        df.groupby(["date", "series_id"], as_index=False)
        .agg({"value": "sum", "source": "first"})
        .sort_values("date")
    )
    return grouped[["date", "value", "source", "series_id"]]


def _fetch_cboe_put_call_ratio(as_of_ymd: str = "") -> pd.DataFrame:
    if as_of_ymd and len(str(as_of_ymd)) >= 8:
        d = str(as_of_ymd)[:8]
        url = f"https://www.cboe.com/us/options/market_statistics/daily/?dt={d[:4]}-{d[4:6]}-{d[6:8]}"
    else:
        url = "https://www.cboe.com/us/options/market_statistics/daily/"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    text = resp.text or ""
    date_match = re.search(r'"selectedDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"', text)
    value_match = re.search(
        r'"name"\s*:\s*"TOTAL PUT/CALL RATIO"\s*,\s*"value"\s*:\s*"([0-9.]+)"',
        text,
    )
    if not date_match or not value_match:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    d8 = date_match.group(1).replace("-", "")
    value = pd.to_numeric(value_match.group(1), errors="coerce")
    if pd.isna(value):
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    return pd.DataFrame(
        [
            {
                "date": d8,
                "value": float(value),
                "source": "cboe:daily_market_statistics_total_put_call_ratio",
                "series_id": "CBOE_PUT_CALL_TOTAL",
            }
        ]
    )


def _fetch_bdi_investing_web(as_of_ymd: str = "") -> pd.DataFrame:
    url = "https://www.investing.com/indices/baltic-dry-historical-data"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    text = resp.text or ""
    rows: List[Dict[str, Any]] = []
    pattern = re.compile(
        r'<time\s+dateTime="([^"]+)">[^<]+</time></td><td[^>]*>([0-9,]+(?:\.[0-9]+)?)</td>',
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        dt = pd.to_datetime(m.group(1), errors="coerce")
        if pd.isna(dt):
            continue
        d8 = pd.Timestamp(dt).strftime("%Y%m%d")
        if as_of_ymd and d8 > str(as_of_ymd)[:8]:
            continue
        val = pd.to_numeric(str(m.group(2)).replace(",", ""), errors="coerce")
        if pd.isna(val):
            continue
        rows.append(
            {
                "date": d8,
                "value": float(val),
                "source": "web_fallback:investing_baltic_dry_historical_data",
                "series_id": "BDI_INVESTING_WEB",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    out = pd.DataFrame(rows)
    out = out.sort_values("date").drop_duplicates(["date", "series_id"], keep="last")
    return out[["date", "value", "source", "series_id"]]


def _fetch_vkospi_pykrx(start_ymd: str, end_ymd: str) -> pd.DataFrame:
    try:
        from pykrx import stock
        # 1167 is the code for VKOSPI (KOSPI 200 Volatility Index)
        df = stock.get_index_ohlcv_by_date(start_ymd, end_ymd, "1167")
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "value", "source", "series_id"])
        
        # Use '종가' column
        if "종가" in df.columns:
            df["value"] = df["종가"]
        else:
            return pd.DataFrame(columns=["date", "value", "source", "series_id"])
        
        df = df.reset_index()
        # '날짜' column is generated by reset_index() for datetime index
        date_col = "날짜" if "날짜" in df.columns else df.columns[0]
        df["date"] = df[date_col].dt.strftime("%Y%m%d")
        df["source"] = "pykrx:index:1167"
        df["series_id"] = "VKOSPI"
        
        return df[["date", "value", "source", "series_id"]]
    except Exception as e:
        _log_print(f"[WARN] _fetch_vkospi_pykrx failed: {e}")
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])


def _safe_num(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return None
    if pd.isna(v):
        return None
    return v


def _series_stats(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "rows": 0,
            "latest_date": None,
            "latest_value": None,
            "latest_source": None,
            "prev_value": None,
            "chg_1": None,
            "chg_20": None,
            "chg_60": None,
        }

    x = df.sort_values("date").copy()
    vals = pd.to_numeric(x["value"], errors="coerce").dropna().reset_index(drop=True)
    latest_source = str(x["source"].iloc[-1]) if "source" in x.columns and len(x) else None
    if vals.empty:
        return {
            "rows": int(len(x)),
            "latest_date": str(x["date"].iloc[-1]),
            "latest_value": None,
            "latest_source": latest_source,
            "prev_value": None,
            "chg_1": None,
            "chg_20": None,
            "chg_60": None,
        }

    last = float(vals.iloc[-1])
    prev = float(vals.iloc[-2]) if len(vals) >= 2 else None
    chg1 = (last - prev) if prev is not None else None
    chg20 = (last - float(vals.iloc[-20])) if len(vals) >= 20 else None
    chg60 = (last - float(vals.iloc[-60])) if len(vals) >= 60 else None

    return {
        "rows": int(len(vals)),
        "latest_date": str(x["date"].iloc[-1]),
        "latest_value": last,
        "latest_source": latest_source,
        "prev_value": prev,
        "chg_1": chg1,
        "chg_20": chg20,
        "chg_60": chg60,
    }


def _derive_fx_context(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "available": False,
            "level": None,
            "level_band": "UNKNOWN",
            "volatility_band": "UNKNOWN",
            "daily_change": None,
            "daily_abs_change": None,
            "avg_abs_change_5": None,
            "avg_abs_change_20": None,
            "three_day_extreme": False,
            "market_bias": "NEUTRAL",
            "trading_action": "NO_FX_SIGNAL",
        }

    x = df.copy()
    x["date"] = x["date"].astype(str)
    x["value"] = pd.to_numeric(x["value"], errors="coerce")
    x = x.dropna(subset=["value"]).sort_values("date")
    if x.empty:
        return {
            "available": False,
            "level": None,
            "level_band": "UNKNOWN",
            "volatility_band": "UNKNOWN",
            "daily_change": None,
            "daily_abs_change": None,
            "avg_abs_change_5": None,
            "avg_abs_change_20": None,
            "three_day_extreme": False,
            "market_bias": "NEUTRAL",
            "trading_action": "NO_FX_SIGNAL",
        }

    level = _safe_num(x["value"].iloc[-1])
    x["diff_1"] = x["value"].diff()
    x["abs_diff_1"] = x["diff_1"].abs()
    daily_change = _safe_num(x["diff_1"].iloc[-1]) if len(x) >= 2 else None
    daily_abs_change = abs(daily_change) if daily_change is not None else None
    avg_abs_change_5 = _safe_num(x["abs_diff_1"].tail(5).mean())
    avg_abs_change_20 = _safe_num(x["abs_diff_1"].tail(20).mean())
    last3_abs = x["abs_diff_1"].tail(3).tolist()
    three_day_extreme = bool(last3_abs) and len(last3_abs) == 3 and all(float(v or 0.0) >= 10.0 for v in last3_abs)

    # 환율 추세 방향: MA20 vs MA60 기울기 (원화 약세 추세 = WEAKENING, 강세 추세 = STRENGTHENING)
    vals = pd.to_numeric(x["value"], errors="coerce")
    ma20_val = _safe_num(vals.tail(20).mean()) if len(vals) >= 20 else None
    ma60_val = _safe_num(vals.tail(60).mean()) if len(vals) >= 60 else None
    if ma20_val is not None and ma60_val is not None:
        trend_direction = "WEAKENING" if ma20_val > ma60_val else "STRENGTHENING"
    else:
        trend_direction = "UNKNOWN"

    # level_band: 2024~2026 현실 환율 기준으로 재조정
    # (구 기준 1200/1350/1450/1550 → 1300/1380/1450/1520)
    if level is None:
        level_band = "UNKNOWN"
    elif level < 1300.0:
        level_band = "OVER_STRONG"
    elif level < 1380.0:
        level_band = "STRONG"
    elif level < 1450.0:
        level_band = "FAIR"
    elif level <= 1520.0:
        level_band = "WEAK"
    else:
        level_band = "CRISIS"

    vol_base = avg_abs_change_20 if avg_abs_change_20 is not None else daily_abs_change
    if vol_base is None:
        volatility_band = "UNKNOWN"
    elif vol_base <= 3.0:
        volatility_band = "LOW"
    elif vol_base <= 7.0:
        volatility_band = "MEDIUM"
    elif vol_base <= 10.0:
        volatility_band = "HIGH"
    else:
        volatility_band = "EXTREME"

    if level_band in {"WEAK", "CRISIS"}:
        market_bias = "EXPORTERS_POSITIVE_DOMESTIC_NEGATIVE"
    elif level_band in {"OVER_STRONG", "STRONG"}:
        market_bias = "DOMESTIC_POSITIVE_EXPORTERS_NEGATIVE"
    else:
        market_bias = "BALANCED"

    if level_band == "CRISIS" or volatility_band == "EXTREME" or three_day_extreme:
        trading_action = "DEFENSIVE"
    elif level_band == "WEAK" or volatility_band == "HIGH":
        trading_action = "CAUTIOUS"
    elif level_band in {"OVER_STRONG", "STRONG"}:
        trading_action = "ROTATE_TO_DOMESTIC"
    else:
        trading_action = "BALANCED"

    return {
        "available": True,
        "level": level,
        "level_band": level_band,
        "volatility_band": volatility_band,
        "trend_direction": trend_direction,
        "ma20": ma20_val,
        "ma60": ma60_val,
        "daily_change": daily_change,
        "daily_abs_change": daily_abs_change,
        "avg_abs_change_5": avg_abs_change_5,
        "avg_abs_change_20": avg_abs_change_20,
        "three_day_extreme": three_day_extreme,
        "market_bias": market_bias,
        "trading_action": trading_action,
        "latest_date": str(x["date"].iloc[-1]),
    }


def _calc_yoy_from_index(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty:
        return None
    x = df.sort_values("date").copy()
    vals = pd.to_numeric(x["value"], errors="coerce").dropna().reset_index(drop=True)
    if len(vals) < 13:
        return None
    cur = float(vals.iloc[-1])
    old = float(vals.iloc[-13])
    if old == 0:
        return None
    return (cur / old - 1.0) * 100.0


def _combine_global_signal(us_signal: str, kr_signal: str) -> str:
    us = str(us_signal or "NEUTRAL").upper()
    kr = str(kr_signal or "NEUTRAL").upper()
    if us == "RISK_OFF":
        return "RISK_OFF"
    if us == "RISK_ON" and kr in {"POSITIVE", "RISK_ON"}:
        return "RISK_ON"
    if kr in {"NEGATIVE", "RISK_OFF"} and us != "RISK_ON":
        return "RISK_OFF"
    return "NEUTRAL"


def _cycle_to_max_age_days(cycle: str) -> int:
    c = str(cycle or "").strip().lower()
    if c == "daily":
        # Daily macro series can lag over weekends/holidays; keep a small grace window.
        return 5
    if c == "monthly":
        return 45
    return 14


def _series_age_days(latest_ymd: Optional[str], as_of_ymd: str, cycle: str = "") -> Optional[float]:
    if not latest_ymd:
        return None
    latest_dt = _as_date(latest_ymd)
    asof_dt = _as_date(as_of_ymd)
    if latest_dt is None or asof_dt is None or pd.isna(latest_dt) or pd.isna(asof_dt):
        return None
    cycle_norm = str(cycle or "").strip().lower()
    if cycle_norm == "monthly":
        latest_dt = (pd.Timestamp(latest_dt) + pd.offsets.MonthEnd(0)).to_pydatetime()
    if cycle_norm == "daily":
        latest_day = pd.Timestamp(latest_dt).normalize()
        asof_day = pd.Timestamp(asof_dt).normalize()
        if asof_day <= latest_day:
            return 0.0
        return float(len(pd.bdate_range(latest_day + pd.Timedelta(days=1), asof_day)))
    return float((asof_dt - latest_dt).days)


def _indicator_source_tier(series_id: str, configured_source: str, effective_source: Any, freshness: str, rows: int) -> Tuple[str, float, str]:
    sid = str(series_id or "")
    src = str(configured_source or "")
    eff = str(effective_source or "")
    fresh = str(freshness or "").upper()
    if src == "not_configured" or fresh == "NOT_CONFIGURED":
        return "MISSING", 0.0, "deferred_optional"
    if rows <= 0 or not eff or fresh in {"UNKNOWN", "STALE"}:
        return (
            "MISSING",
            0.0,
            "deferred_optional"
            if sid in {"CBOE_PUT_CALL_TOTAL", "NAPM", "NAPMNONMFG", "MANEXIMCN", "BDI_INVESTING_WEB"}
            else "required_guard",
        )
    if eff.startswith("web_fallback:") or "web fallback" in src.lower():
        return "WEB_FALLBACK", 0.35, "reference_only"
    if eff.startswith("fred_fallback:") or eff.startswith("proxy:") or "fallback" in src.lower() or "proxy" in src.lower():
        return "OFFICIAL_PROXY", 0.60, "guard_light"
    return "OFFICIAL", 1.0, "guard"


def _summarize_indicator_source_tiers(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    weights: List[float] = []
    for row in rows:
        tier = str(row.get("source_tier") or "MISSING")
        counts[tier] = int(counts.get(tier, 0)) + 1
        try:
            weights.append(float(row.get("confidence_weight") or 0.0))
        except Exception:
            weights.append(0.0)
    return {
        "counts": counts,
        "avg_confidence_weight": float(sum(weights) / len(weights)) if weights else 0.0,
        "total": int(len(rows)),
    }


def _build_indicator_mapping(
    feature_series: Dict[str, Dict[str, Any]],
    as_of_ymd: str,
    freshness_ref_ymd: str = "",
) -> List[Dict[str, Any]]:
    """지표 매핑과 신선도 판정을 만든다.

    [2026-08-21] 신선도 기준일을 `as_of_ymd` 에서 분리했다.

    `as_of_ymd` 는 `_derive_d_from_fills()` 로 만든 **마지막 매매일**이고,
    같은 값이 위쪽에서 `feature_df` 를 그 날짜까지 **자르는 데도** 쓰인다(:1768-1769).
    그래서 자른 지점을 기준으로 나이를 재는 자기 정합적 구조가 됐다 -
    무엇을 잘라내든 `age_days` 는 항상 0.0 이고 판정은 언제나 OK 다.

    실측(2026-08-21): FRED 는 오늘까지 줬는데(fetch DGS10 2497행) 저장은 2489행이었다.
    8행이 잘렸고, 잘린 지점(20260807)을 기준으로 재니 14일 낡은 데이터가 age=0.0 "OK" 였다.

    여기서는 **판정 기준일만** 오늘로 바꾼다. 절단 로직은 건드리지 않는다
    (point-in-time 재현에 정당한 용도가 있을 수 있어 별도 판단 대상이다).
    따라서 데이터는 그대로이고, 낡은 데이터가 낡았다고 보고될 뿐이다.
    비면 예전 동작(as_of_ymd 기준)으로 되돌아간다.
    상세: .agent/PLANS.md 2026-08-21 (21)
    """
    ref_ymd = str(freshness_ref_ymd or "").strip() or as_of_ymd
    rows: List[Dict[str, Any]] = []
    for m in INDICATOR_SOURCE_MAP:
        sid = str(m.get("series_id") or "")
        stats = feature_series.get(sid, {}) if isinstance(feature_series, dict) else {}
        latest_date = str(stats.get("latest_date")) if stats.get("latest_date") is not None else None
        cycle = str(m.get("update_cycle") or "")
        effective_source = stats.get("latest_source") if isinstance(stats, dict) else None
        configured_source = str(m.get("source") or "")
        if configured_source == "not_configured":
            tier, weight, score_role = _indicator_source_tier(sid, configured_source, None, "NOT_CONFIGURED", 0)
            rows.append(
                {
                    "indicator": m.get("indicator"),
                    "series_id": sid,
                    "source": configured_source,
                    "effective_source": None,
                    "source_tier": tier,
                    "confidence_weight": weight,
                    "score_role": score_role,
                    "update_cycle": cycle,
                    "purpose": m.get("purpose"),
                    "latest_date": None,
                    "age_days": None,
                    "max_age_days": _cycle_to_max_age_days(cycle),
                    "freshness": "NOT_CONFIGURED",
                    "rows": 0,
                }
            )
            continue
        if sid == "DCOPPER" and "PCOPPUSDM" in str(effective_source or ""):
            cycle = "monthly"
        age_days = _series_age_days(latest_date, ref_ymd, cycle)
        max_age_days = int(SERIES_MAX_AGE_DAYS.get(sid, _cycle_to_max_age_days(cycle)))
        freshness = "UNKNOWN"
        if age_days is not None:
            freshness = "OK" if age_days <= max_age_days else "STALE"
        tier, weight, score_role = _indicator_source_tier(sid, str(m.get("source") or ""), effective_source, freshness, int(stats.get("rows") or 0))
        rows.append(
            {
                "indicator": m.get("indicator"),
                "series_id": sid,
                "source": m.get("source"),
                "effective_source": effective_source,
                "source_tier": tier,
                "confidence_weight": weight,
                "score_role": score_role,
                "update_cycle": cycle,
                "purpose": m.get("purpose"),
                "latest_date": latest_date,
                "age_days": age_days,
                "max_age_days": max_age_days,
                "freshness": freshness,
                "rows": int(stats.get("rows") or 0),
            }
        )
    return rows


def _derive_macro_features(series_map: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    us_cpi_yoy = _calc_yoy_from_index(series_map.get("CPIAUCSL", pd.DataFrame()))
    us_core_cpi_yoy = _calc_yoy_from_index(series_map.get("CPILFESL", pd.DataFrame()))
    us_unemp = _safe_num(_series_stats(series_map.get("UNRATE", pd.DataFrame())).get("latest_value"))
    ism_manu_stats = _series_stats(series_map.get("NAPM", pd.DataFrame()))
    ism_service_stats = _series_stats(series_map.get("NAPMNONMFG", pd.DataFrame()))
    wti_stats = _series_stats(series_map.get("DCOILWTICO", pd.DataFrame()))
    brent_stats = _series_stats(series_map.get("DCOILBRENTEU", pd.DataFrame()))
    us_ism_manu = _safe_num(ism_manu_stats.get("latest_value"))
    us_ism_service_proxy = _safe_num(ism_service_stats.get("latest_value"))
    wti_spot = _safe_num(wti_stats.get("latest_value"))
    brent_spot = _safe_num(brent_stats.get("latest_value"))
    wti_chg_20 = _safe_num(wti_stats.get("chg_20"))
    brent_chg_20 = _safe_num(brent_stats.get("chg_20"))
    us_vix = _safe_num(_series_stats(series_map.get("VIXCLS", pd.DataFrame())).get("latest_value"))
    us_hy = _safe_num(_series_stats(series_map.get("BAMLH0A0HYM2", pd.DataFrame())).get("latest_value"))

    # US 10Y / 2Y 국채금리 (장단기 스프레드 및 금리 수준 반영)
    dgs10_stats = _series_stats(series_map.get("DGS10", pd.DataFrame()))
    dgs2_stats  = _series_stats(series_map.get("DGS2",  pd.DataFrame()))
    dgs10 = _safe_num(dgs10_stats.get("latest_value"))   # 10년 금리 (%)
    dgs2  = _safe_num(dgs2_stats.get("latest_value"))    # 2년 금리 (%)
    dgs10_chg_20 = _safe_num(dgs10_stats.get("chg_20")) # 20영업일 금리 변화 (pp)
    fedfunds_stats = _series_stats(series_map.get("FEDFUNDS", pd.DataFrame()))
    fedfunds = _safe_num(fedfunds_stats.get("latest_value"))
    fedfunds_chg_1 = _safe_num(fedfunds_stats.get("chg_1"))
    fedfunds_chg_20 = _safe_num(fedfunds_stats.get("chg_20"))
    if dgs10_chg_20 is None:
        rate_direction_20d = "UNKNOWN"
    elif dgs10_chg_20 >= 0.10:
        rate_direction_20d = "RISING"
    elif dgs10_chg_20 <= -0.10:
        rate_direction_20d = "FALLING"
    else:
        rate_direction_20d = "FLAT"

    # 장단기 스프레드: 양수=정상, 음수=역전(경기침체 선행 신호)
    term_spread = (dgs10 - dgs2) if (dgs10 is not None and dgs2 is not None) else None
    yield_curve_inverted = (term_spread is not None and term_spread < 0.0)

    # 금리 수준 과열: 10Y >= 4.5% → 주식 할인율 부담 (데이터 없으면 패널티 없음)
    rate_extreme = (dgs10 is not None and dgs10 >= 4.5)

    # 금리 급등: 20일 +0.5pp 이상 상승 → 유동성 긴축 신호
    rate_spike_20d = (dgs10_chg_20 is not None and dgs10_chg_20 >= 0.5)

    # S&P500 / Nasdaq / 구리 / 달러 인덱스
    sp500_stats  = _series_stats(series_map.get("SP500",    pd.DataFrame()))
    nasdaq_stats = _series_stats(series_map.get("NASDAQCOM", pd.DataFrame()))
    sox_stats    = _series_stats(series_map.get("NASDAQSOX", pd.DataFrame()))
    copper_stats = _series_stats(series_map.get("DCOPPER",  pd.DataFrame()))
    dxy_stats    = _series_stats(series_map.get("DTWEXBGS", pd.DataFrame()))
    m2_stats     = _series_stats(series_map.get("M2SL",     pd.DataFrame()))
    steel_stats  = _series_stats(series_map.get("WPU101",   pd.DataFrame()))
    put_call_stats = _series_stats(series_map.get("CBOE_PUT_CALL_TOTAL", pd.DataFrame()))
    bdi_stats = _series_stats(series_map.get("BDI_INVESTING_WEB", pd.DataFrame()))
    sp500_latest  = _safe_num(sp500_stats.get("latest_value"))
    nasdaq_latest = _safe_num(nasdaq_stats.get("latest_value"))
    sox_latest    = _safe_num(sox_stats.get("latest_value"))
    copper_latest = _safe_num(copper_stats.get("latest_value"))
    dxy_latest    = _safe_num(dxy_stats.get("latest_value"))
    m2_latest     = _safe_num(m2_stats.get("latest_value"))
    steel_latest  = _safe_num(steel_stats.get("latest_value"))
    put_call_ratio = _safe_num(put_call_stats.get("latest_value"))
    put_call_status = "OK" if put_call_ratio is not None else "OFFICIAL_SOURCE_NO_ROWS"
    bdi = _safe_num(bdi_stats.get("latest_value"))
    bdi_status = "WEB_FALLBACK" if bdi is not None else "WEB_FALLBACK_NO_ROWS"

    # P2 — CNY/KRW(파생) / 중국 PMI / 한국 국고채 3년
    dexchus_stats  = _series_stats(series_map.get("DEXCHUS",    pd.DataFrame()))
    cn_pmi_stats   = _series_stats(series_map.get("MANEXIMCN",  pd.DataFrame()))
    ktb3y_stats    = _series_stats(series_map.get("KR_KTB3Y",   pd.DataFrame()))
    usdcny         = _safe_num(dexchus_stats.get("latest_value"))  # USD/CNY
    cn_pmi         = _safe_num(cn_pmi_stats.get("latest_value"))   # 중국 PMI
    ktb3y          = _safe_num(ktb3y_stats.get("latest_value"))    # 한국 국고채 3년 (%)

    # 원유 % 기준 충격 계산 (절대값$/bbl 기준에서 % 기준으로 정규화)
    def _oil_chg_pct(stats: Dict[str, Any]) -> Optional[float]:
        """chg_20(절대값) / (latest - chg_20) = 20일 등락률(소수)"""
        latest = _safe_num(stats.get("latest_value"))
        chg20  = _safe_num(stats.get("chg_20"))
        if latest is None or chg20 is None:
            return None
        base = latest - chg20
        if abs(base) < 1e-9:
            return None
        return float(chg20 / base)

    wti_chg_20_pct   = _oil_chg_pct(wti_stats)    # WTI 20일 등락률 (소수, 부호 있음)
    brent_chg_20_pct = _oil_chg_pct(brent_stats)  # Brent 20일 등락률 (소수, 부호 있음)
    sp500_chg_20_pct  = _oil_chg_pct(sp500_stats)  # S&P500 20일 등락률
    nasdaq_chg_20_pct = _oil_chg_pct(nasdaq_stats) # Nasdaq 20일 등락률
    sox_chg_20_pct    = _oil_chg_pct(sox_stats)    # SOX 20일 등락률
    copper_chg_20_pct = _oil_chg_pct(copper_stats) # 구리 20일 등락률
    dxy_chg_20_pct    = _oil_chg_pct(dxy_stats)    # 달러 인덱스 20일 등락률
    m2_chg_20_pct     = _oil_chg_pct(m2_stats)     # M2 20개 관측치 등락률
    steel_chg_20_pct  = _oil_chg_pct(steel_stats)  # 철강 PPI 20개 관측치 등락률
    dexchus_chg_20_pct = _oil_chg_pct(dexchus_stats) # USD/CNY 20일 등락률 (양수=CNY 약세)

    # S&P500 급락: 20일 -8% 이하 → 글로벌 리스크 오프 (데이터 없으면 패널티 없음)
    sp500_declining = (sp500_chg_20_pct is not None and sp500_chg_20_pct < -0.08)
    # Nasdaq 급락: 20일 -10% 이하 → 성장주/반도체 위험선호 둔화
    nasdaq_declining = (nasdaq_chg_20_pct is not None and nasdaq_chg_20_pct < -0.10)
    # SOX 급락: 반도체 선행/연동 리스크. 현재 점수에는 직접 가산하지 않고 raw/reference로만 제공.
    sox_declining = (sox_chg_20_pct is not None and sox_chg_20_pct < -0.10)
    # 구리 급락: 20일 -10% 이하 → 글로벌 경기 둔화 선행 신호
    copper_collapsing = (copper_chg_20_pct is not None and copper_chg_20_pct < -0.10)
    m2_contracting = (m2_chg_20_pct is not None and m2_chg_20_pct < 0.0)
    steel_declining = (steel_chg_20_pct is not None and steel_chg_20_pct < -0.05)
    # 달러 급등: 20일 +3% 이상 → 신흥국(한국 포함) 자금 이탈 압력
    dollar_surging = (dxy_chg_20_pct is not None and dxy_chg_20_pct > 0.03)
    # CNY 급격 약세: USD/CNY 20일 +2% 이상 상승 → 한국 수출 경쟁력 압박 + 위안화 리스크
    cny_weakening = (dexchus_chg_20_pct is not None and dexchus_chg_20_pct > 0.02)
    # 중국 PMI 위축: 50 미만 → 한국 대중국 수출 수요 감소 (데이터 없으면 패널티 없음)
    china_pmi_contracting = (cn_pmi is not None and cn_pmi < 50.0)
    # 한국 국고채 3년 스프레드: KTB3Y - BOK_BASE_RATE (크레딧 환경)
    # base_rate는 아래 kr 섹션에서 정의되므로 여기서는 raw값만 준비
    ktb3y_chg_20 = _safe_num(ktb3y_stats.get("chg_20"))  # 20일 금리 변화 (pp)

    # 급등: WTI 또는 Brent 중 하나라도 20일 +15% 초과
    wti_shock   = (wti_chg_20_pct   is not None and wti_chg_20_pct   > 0.15)
    brent_shock = (brent_chg_20_pct is not None and brent_chg_20_pct > 0.15)
    oil_shock   = wti_shock or brent_shock  # 에너지 급등 통합 플래그

    # 급락: WTI 또는 Brent 20일 -15% 이하 → 수요 붕괴 신호
    wti_collapse   = (wti_chg_20_pct   is not None and wti_chg_20_pct   < -0.15)
    brent_collapse = (brent_chg_20_pct is not None and brent_chg_20_pct < -0.15)
    oil_collapse   = wti_collapse or brent_collapse  # 수요 붕괴 통합 플래그

    # US risk level (VIX + HY Spread 기반, WTI 에너지 충격 + 금리 역전 추가 반영)
    vix = us_vix if us_vix is not None else 20.0
    hy = us_hy if us_hy is not None else 4.0
    if vix > 35 or hy > 6.0 or (yield_curve_inverted and rate_extreme) or sp500_declining or nasdaq_declining:
        us_risk_level = "EXTREME"
        exposure_mult = 0.2
    elif vix > 28 or hy > 5.0 or oil_shock or (yield_curve_inverted and rate_spike_20d) or copper_collapsing:
        us_risk_level = "HIGH"
        exposure_mult = 0.5
    elif vix > 22 or hy > 4.0 or rate_extreme or yield_curve_inverted or oil_collapse:
        us_risk_level = "MODERATE"
        exposure_mult = 0.8
    else:
        us_risk_level = "LOW"
        exposure_mult = 1.0

    # 금리 급등 시 exposure 추가 하향 (기존 레벨 유지, 배수만 조정)
    if rate_spike_20d and exposure_mult > 0.5:
        exposure_mult = max(exposure_mult - 0.2, 0.5)

    us_components = {
        "cpi_stable": (us_cpi_yoy is not None and us_cpi_yoy < 3.0),
        "employment_goldilocks": (us_unemp is not None and 3.5 < us_unemp < 5.0),
        "vix_calm": (us_vix is not None and us_vix < 22.0),
        "spread_tight": (us_hy is not None and us_hy < 4.0),
        # 에너지: WTI+Brent 모두 급등 없을 때 안정 (데이터 없으면 True)
        "energy_stable": (not oil_shock),
        # 수요: 원유 급락(수요 붕괴) 없을 때 안정 (데이터 없으면 True)
        "oil_demand_stable": (not oil_collapse),
        # 국채금리 반영 (데이터 없으면 True로 패스 — 페널티 없음)
        "yield_not_inverted": (not yield_curve_inverted),
        "rate_not_extreme": (not rate_extreme),
        # 글로벌 경기·증시 (데이터 없으면 True로 패스)
        "global_equity_stable": (not sp500_declining),   # S&P500 급락 없음
        "nasdaq_stable": (not nasdaq_declining),          # Nasdaq 급락 없음
        "copper_not_collapsing": (not copper_collapsing), # 구리 급락 없음 (경기 둔화 선행)
    }
    us_score = int(sum(bool(v) for v in us_components.values()))
    # 컴포넌트 11개 기준: >=7 RISK_ON, <=4 RISK_OFF
    if us_score >= 7:
        us_signal = "RISK_ON"
    elif us_score <= 4:
        us_signal = "RISK_OFF"
    else:
        us_signal = "NEUTRAL"

    # KR features
    kr_base = _series_stats(series_map.get("BOK_BASE_RATE", pd.DataFrame()))
    kr_usd = _series_stats(series_map.get("KR_USDKRW", pd.DataFrame()))
    kr_exp = _series_stats(series_map.get("KR_EXPORT_VALUE", pd.DataFrame()))
    kr_cpi_yoy = _calc_yoy_from_index(series_map.get("KR_CPI_INDEX", pd.DataFrame()))
    kr_export_yoy = _calc_yoy_from_index(series_map.get("KR_EXPORT_VALUE", pd.DataFrame()))

    base_rate = _safe_num(kr_base.get("latest_value"))
    base_rate_chg = _safe_num(kr_base.get("chg_1"))
    usdkrw = _safe_num(kr_usd.get("latest_value"))

    # KTB3Y 스프레드: 국고채 3년 - 기준금리 (양수=정상, 0 이하=비정상 역전)
    ktb3y_spread = (ktb3y - base_rate) if (ktb3y is not None and base_rate is not None) else None
    # 국고채 금리 급등: 20일 +0.3pp 이상 → 시중금리 긴축 환경
    ktb3y_spiking = (ktb3y_chg_20 is not None and ktb3y_chg_20 >= 0.3)

    kr_components = {
        "inflation_stable":    (kr_cpi_yoy is not None and kr_cpi_yoy < 3.0),
        "fx_stable":           (usdkrw is not None and usdkrw < 1380.0),
        "export_positive":     (kr_export_yoy is not None and kr_export_yoy > 0.0),
        "rate_supportive":     (base_rate_chg is not None and base_rate_chg <= 0.0),
        # P1: 달러 급등 없으면 외국인 자금 유입 환경 양호
        "dollar_not_surging":  (not dollar_surging),
        # P2: CNY 급격 약세 없으면 대중국 수출 경쟁력 유지
        "cny_not_weakening":   (not cny_weakening),
        # P2: 중국 PMI 확장(>=50)이면 수출 수요 환경 양호 (데이터 없으면 True)
        "china_demand_ok":     (not china_pmi_contracting),
        # P2: 국내 시중금리 급등 없으면 내수 신용 환경 양호
        "ktb3y_not_spiking":   (not ktb3y_spiking),
    }
    kr_score = int(sum(bool(v) for v in kr_components.values()))
    # 컴포넌트 8개 기준: >=5 POSITIVE(62%), <=3 NEGATIVE(37%)
    if kr_score >= 5:
        kr_signal = "POSITIVE"
    elif kr_score <= 3:
        kr_signal = "NEGATIVE"
    else:
        kr_signal = "NEUTRAL"

    global_signal = _combine_global_signal(us_signal, kr_signal)

    return {
        "us": {
            "signal": us_signal,
            "score": us_score,
            "risk_level": us_risk_level,
            "exposure_multiplier": exposure_mult,
            "raw": {
                "cpi_yoy": us_cpi_yoy,
                "core_cpi_yoy": us_core_cpi_yoy,
                "unemployment": us_unemp,
                "ism_manufacturing": us_ism_manu,
                "ism_service_proxy": us_ism_service_proxy,
                "wti_spot": wti_spot,
                "brent_spot": brent_spot,
                "wti_chg_20": wti_chg_20,
                "brent_chg_20": brent_chg_20,
                "wti_chg_20_pct": wti_chg_20_pct,
                "brent_chg_20_pct": brent_chg_20_pct,
                "oil_shock": oil_shock,
                "oil_collapse": oil_collapse,
                "vix": us_vix,
                "hy_spread": us_hy,
                "us10y": dgs10,
                "us2y": dgs2,
                "fedfunds": fedfunds,
                "fedfunds_chg_1": fedfunds_chg_1,
                "fedfunds_chg_20": fedfunds_chg_20,
                "rate_direction_20d": rate_direction_20d,
                "term_spread_10y2y": term_spread,
                "yield_curve_inverted": yield_curve_inverted,
                "rate_extreme": rate_extreme,
                "rate_spike_20d": rate_spike_20d,
                "yield_curve_inverted": yield_curve_inverted,
                "rate_extreme": rate_extreme,
                "rate_spike_20d": rate_spike_20d,
                "dgs10_chg_20": dgs10_chg_20,
                "sp500": sp500_latest,
                "sp500_chg_20_pct": sp500_chg_20_pct,
                "sp500_declining": sp500_declining,
                "nasdaq": nasdaq_latest,
                "nasdaq_chg_20_pct": nasdaq_chg_20_pct,
                "nasdaq_declining": nasdaq_declining,
                "sox": sox_latest,
                "sox_series_id": "NASDAQSOX",
                "sox_chg_20_pct": sox_chg_20_pct,
                "sox_declining": sox_declining,
                "copper": copper_latest,
                "copper_chg_20_pct": copper_chg_20_pct,
                "copper_collapsing": copper_collapsing,
                "m2": m2_latest,
                "m2_series_id": "M2SL",
                "m2_chg_20_pct": m2_chg_20_pct,
                "m2_contracting": m2_contracting,
                "steel_ppi": steel_latest,
                "steel_proxy_series_id": "WPU101",
                "steel_chg_20_pct": steel_chg_20_pct,
                "steel_declining": steel_declining,
                "put_call_ratio": put_call_ratio,
                "put_call_source_status": put_call_status,
                "put_call_source": str(put_call_stats.get("latest_source") or ""),
                "put_call_as_of": str(put_call_stats.get("latest_date") or ""),
                "cnn_fear_greed": None,
                "cnn_fear_greed_source_status": "OFFICIAL_SOURCE_NOT_CONFIGURED",
                "bdi": bdi,
                "bdi_source_status": bdi_status,
                "bdi_source": str(bdi_stats.get("latest_source") or ""),
                "bdi_as_of": str(bdi_stats.get("latest_date") or ""),
            },
            "components": us_components,
        },
        "kr": {
            "signal": kr_signal,
            "score": kr_score,
            "raw": {
                "base_rate": base_rate,
                "base_rate_change": base_rate_chg,
                "usdkrw": usdkrw,
                "cpi_yoy": kr_cpi_yoy,
                "export_yoy": kr_export_yoy,
                "dxy": dxy_latest,
                "dxy_proxy_series_id": "DTWEXBGS",
                "dxy_proxy_source": "FRED trade-weighted broad dollar index",
                "dxy_chg_20_pct": dxy_chg_20_pct,
                "dollar_surging": dollar_surging,
                "usdcny": usdcny,
                "dexchus_chg_20_pct": dexchus_chg_20_pct,
                "cny_weakening": cny_weakening,
                "china_pmi": cn_pmi,
                "china_pmi_contracting": china_pmi_contracting,
                "ktb3y": ktb3y,
                "ktb3y_spread": ktb3y_spread,
                "ktb3y_chg_20": ktb3y_chg_20,
                "ktb3y_spiking": ktb3y_spiking,            },
            "components": kr_components,
        },
        "global": {
            "signal": global_signal,
            "exposure_multiplier": exposure_mult,
        },
    }

def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _series_frame_from_archived_stats(series_id: str, stats: Dict[str, Any], source_path: Path) -> pd.DataFrame:
    latest_date = str(stats.get("latest_date") or "").strip()
    latest_value = _safe_num(stats.get("latest_value"))
    if not latest_date or latest_value is None:
        return pd.DataFrame(columns=["date", "value", "source", "series_id"])
    rows: List[Dict[str, Any]] = [
        {
            "date": latest_date,
            "value": latest_value,
            "source": f"archive_reuse:{source_path.name}",
            "series_id": series_id,
        }
    ]
    prev_value = _safe_num(stats.get("prev_value"))
    if prev_value is not None:
        latest_dt = _as_date(latest_date)
        if latest_dt is not None and not pd.isna(latest_dt):
            rows.append(
                {
                    "date": (latest_dt - pd.Timedelta(days=1)).strftime("%Y%m%d"),
                    "value": prev_value,
                    "source": f"archive_reuse:{source_path.name}",
                    "series_id": series_id,
                }
            )
    return pd.DataFrame(rows).sort_values("date").drop_duplicates(["date"], keep="last")


def _reuse_missing_critical_archive_series(
    series_map: Dict[str, pd.DataFrame],
    status: Dict[str, Any],
    as_of_ymd: str,
    exclude_today: str,
) -> None:
    missing = [
        sid for sid in sorted(CRITICAL_ARCHIVE_REUSE_SERIES)
        if series_map.get(sid) is None or series_map.get(sid, pd.DataFrame()).empty
    ]
    if not missing:
        return
    archived_obj, archived_path = _latest_valid_macro_feature(exclude_today)
    archived_series = archived_obj.get("series") if isinstance(archived_obj.get("series"), dict) else {}
    reused: List[Dict[str, Any]] = []
    for sid in missing:
        stats = archived_series.get(sid) if isinstance(archived_series.get(sid), dict) else {}
        latest_date = str(stats.get("latest_date") or "").strip()
        age_days = _series_age_days(latest_date, as_of_ymd, "daily")
        max_age_days = int(SERIES_MAX_AGE_DAYS.get(sid, _cycle_to_max_age_days("daily")))
        if age_days is None or age_days > max_age_days or archived_path is None:
            reused.append({"series_id": sid, "reused": False, "latest_date": latest_date or None, "age_days": age_days})
            continue
        frame = _series_frame_from_archived_stats(sid, stats, archived_path)
        if frame.empty:
            reused.append({"series_id": sid, "reused": False, "latest_date": latest_date or None, "age_days": age_days})
            continue
        series_map[sid] = frame
        reused.append(
            {
                "series_id": sid,
                "reused": True,
                "source": str(archived_path),
                "latest_date": latest_date,
                "age_days": age_days,
                "max_age_days": max_age_days,
            }
        )
    status["critical_archive_reuse"] = reused


def _latest_valid_macro_feature(exclude_today: str) -> Tuple[Dict[str, Any], Optional[Path]]:
    candidates = []
    for path in sorted(LOGS.glob("macro_feature_external_*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.name == "macro_feature_external_latest.json":
            continue
        if exclude_today and path.stem.endswith(str(exclude_today)):
            continue
        candidates.append(path)
    for path in candidates:
        obj = _read_json(path)
        series = obj.get("series") if isinstance(obj.get("series"), dict) else {}
        signals = obj.get("signals") if isinstance(obj.get("signals"), dict) else {}
        if series and signals:
            nonzero = any(int((v or {}).get("rows") or 0) > 0 for v in series.values() if isinstance(v, dict))
            if nonzero:
                return obj, path
    return {}, None


def _required_bad_macro_freshness_count(obj: Dict[str, Any]) -> int:
    mapping = obj.get("indicator_source_mapping") if isinstance(obj.get("indicator_source_mapping"), list) else []
    required_bad = 0
    for item in mapping:
        if not isinstance(item, dict):
            continue
        if str(item.get("score_role") or "") != "required_guard":
            continue
        if str(item.get("freshness") or "").upper() in {"STALE", "UNKNOWN"}:
            required_bad += 1
    return required_bad


def _latest_fresh_macro_feature() -> Tuple[Dict[str, Any], Optional[Path]]:
    path = LOGS / "macro_feature_external_latest.json"
    obj = _read_json(path)
    series = obj.get("series") if isinstance(obj.get("series"), dict) else {}
    signals = obj.get("signals") if isinstance(obj.get("signals"), dict) else {}
    if not series or not signals:
        return {}, None
    nonzero = any(int((v or {}).get("rows") or 0) > 0 for v in series.values() if isinstance(v, dict))
    if not nonzero:
        return {}, None
    if _required_bad_macro_freshness_count(obj) > 0:
        return {}, None
    return obj, path


def _is_empty_fetch_snapshot(series_map: Dict[str, pd.DataFrame], feature_obj: Dict[str, Any]) -> bool:
    if any((df is not None and not df.empty) for df in series_map.values()):
        return False
    series = feature_obj.get("series") if isinstance(feature_obj.get("series"), dict) else {}
    return not any(int((v or {}).get("rows") or 0) > 0 for v in series.values() if isinstance(v, dict))


def main() -> int:
    today = _today_ymd()
    # [2026-09-13] 기준일을 **달력**으로 바꿨다. 사용자 승인.
    #   전에는 `_derive_d_from_fills()` - 마지막 체결일이었다. 08-24 이후 체결이
    #   없어서 거시 특징 전체가 20일 전에서 잘렸고(:1790-1792 절단), 그 잘린
    #   데이터를 macro_signal 이 STALE 로 읽어 진입 게이트가 REDUCE 로 붙들렸다.
    #   매매가 멈추면 거시가 낡아 보이고 그게 진입을 다시 줄이는 구조였다.
    #   2026-08-21 에 판정 기준일만 분리하고 절단은 '별도 판단 대상' 으로 남긴 자리다.
    #   실측 대조(절단 유지 -> 제거): stale 10->0, macro_critical_bad 2->0,
    #   global exposure_multiplier **0.8->0.5** (VIX 15.85->17.84, US10Y 4.70->4.95).
    #   완화가 아니다 - 노출은 오히려 줄어든다. 실제 값으로 바꾸는 변경이다.
    #   point-in-time 재현이 필요하면 PAPER_MACRO_ASOF_YMD 로 고정한다.
    as_of_ymd = str(os.environ.get("PAPER_MACRO_ASOF_YMD", "") or "").strip() or today
    start_day = (date.today() - timedelta(days=3650)).strftime("%Y%m%d")
    start_ym = start_day[:6]
    end_ym = today[:6]

    fred_key = _env("FRED_API_KEY")
    ecos_key = _env("ECOS_API_KEY", "BOK_API_KEY")
    kosis_key = _env("KOSIS_API_KEY")
    public_key = _env("PUBLIC_DATA_API_KEY", "DATA_GO_KR_API_KEY")

    all_frames: List[pd.DataFrame] = []
    status: Dict[str, Any] = {
        "as_of_ymd": as_of_ymd,
        "generated_for_ymd": today,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "keys_configured": {
            "fred": bool(fred_key),
            "ecos": bool(ecos_key),
            "kosis": bool(kosis_key),
            "public_data": bool(public_key),
        },
        "fetch": {},
    }

    # FRED multi-series
    if fred_key:
        fred_consecutive_failures = 0
        for sid in FRED_SERIES:
            try:
                df = _fetch_fred_series(fred_key, sid, start_day, today)
                status["fetch"][f"fred:{sid}"] = {"ok": True, "rows": int(len(df))}
                fred_consecutive_failures = 0
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                status["fetch"][f"fred:{sid}"] = {"ok": False, "error": msg}
                if any(token in msg.lower() for token in ("504", "time-out", "timeout", "timed out")):
                    fred_consecutive_failures += 1
                if fred_consecutive_failures >= 2:
                    status["fetch"]["fred_skip_remaining"] = {
                        "ok": False,
                        "reason": "consecutive_timeout_or_gateway_errors",
                        "after_series_id": sid,
                        "skipped_count": int(len(FRED_SERIES) - FRED_SERIES.index(sid) - 1),
                    }
                    break

        # FRED targeted fallbacks for unstable or optional series.
        for target_sid, fallback_ids in FRED_FALLBACKS.items():
            if status["fetch"].get("fred_skip_remaining"):
                break
            target_fetch = status["fetch"].get(f"fred:{target_sid}", {})
            already_ok = bool(target_fetch.get("ok")) and int(target_fetch.get("rows") or 0) > 0
            if already_ok:
                continue
            for fb_sid in fallback_ids:
                try:
                    df_fb = _fetch_fred_series(fred_key, fb_sid, start_day, today)
                    if df_fb.empty:
                        status["fetch"][f"fred_fallback:{target_sid}<={fb_sid}"] = {"ok": False, "rows": 0}
                        continue
                    df_fb = df_fb.copy()
                    df_fb["series_id"] = target_sid
                    df_fb["source"] = f"fred_fallback:{fb_sid}"
                    all_frames.append(df_fb)
                    status["fetch"][f"fred_fallback:{target_sid}<={fb_sid}"] = {"ok": True, "rows": int(len(df_fb))}
                    break
                except Exception as ex:
                    status["fetch"][f"fred_fallback:{target_sid}<={fb_sid}"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}
    else:
        status["fetch"]["fred"] = {"ok": False, "error": "api_key_missing"}

    # ECOS core series (always keep: base rate + USDKRW + KTB3Y)
    if ecos_key:
        ecos_defs_core: List[Tuple[str, str, str, str, str, str]] = [
            ("BOK_BASE_RATE", "722Y001", "M", start_ym, end_ym, "0101000"),
            ("KR_KTB3Y",      "817Y002", "D", start_day, today, "010190000"),  # 국고채(3년)
        ]
        for sid, stat, cyc, s, e, item in ecos_defs_core:
            try:
                df = _fetch_ecos_series(ecos_key, stat, cyc, s, e, item, sid)
                status["fetch"][f"ecos:{sid}"] = {"ok": True, "rows": int(len(df))}
                if not df.empty:
                    all_frames.append(df)
            except Exception as ex:
                status["fetch"][f"ecos:{sid}"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

        # USD/KRW: prefer previous-day close basis (731Y003/0000003), fallback to base rate quote (731Y001/0000001)
        fx_loaded = False
        fx_candidates: List[Tuple[str, str]] = [
            ("731Y003", "0000003"),  # 원/달러(종가 15:30)
            ("731Y001", "0000001"),  # 원/미국달러(매매기준율)
        ]
        for stat, item in fx_candidates:
            try:
                df_fx = _fetch_ecos_series(ecos_key, stat, "D", start_day, today, item, "KR_USDKRW")
                status["fetch"][f"ecos:KR_USDKRW:{stat}/{item}"] = {"ok": True, "rows": int(len(df_fx))}
                if not df_fx.empty:
                    all_frames.append(df_fx)
                    fx_loaded = True
                    break
            except Exception as ex:
                status["fetch"][f"ecos:KR_USDKRW:{stat}/{item}"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}
        if not fx_loaded:
            status["fetch"]["ecos:KR_USDKRW"] = {"ok": False, "error": "all_candidates_failed"}
    else:
        status["fetch"]["ecos"] = {"ok": False, "error": "api_key_missing"}

    # KR CPI: KOSIS first, ECOS fallback
    kosis_cpi_rows = 0
    ecos_cpi_rows = 0
    cpi_loaded = False
    if kosis_key:
        try:
            df_kosis_cpi = _fetch_kosis_cpi_series(kosis_key, start_ym, end_ym, "KR_CPI_INDEX")
            kosis_cpi_rows = int(len(df_kosis_cpi))
            status["fetch"]["kosis:KR_CPI_INDEX"] = {"ok": True, "rows": kosis_cpi_rows}
            if not df_kosis_cpi.empty:
                all_frames.append(df_kosis_cpi)
                cpi_loaded = True
        except Exception as ex:
            status["fetch"]["kosis:KR_CPI_INDEX"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}
    else:
        status["fetch"]["kosis:KR_CPI_INDEX"] = {"ok": False, "error": "api_key_missing"}

    if not cpi_loaded:
        if ecos_key:
            try:
                df_ecos_cpi = _fetch_ecos_series(ecos_key, "901Y009", "M", start_ym, end_ym, "0", "KR_CPI_INDEX")
                ecos_cpi_rows = int(len(df_ecos_cpi))
                status["fetch"]["ecos:KR_CPI_INDEX"] = {"ok": True, "rows": ecos_cpi_rows, "fallback": True}
                if not df_ecos_cpi.empty:
                    all_frames.append(df_ecos_cpi)
            except Exception as ex:
                status["fetch"]["ecos:KR_CPI_INDEX"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}", "fallback": True}
        else:
            status["fetch"]["ecos:KR_CPI_INDEX"] = {"ok": False, "error": "api_key_missing", "fallback": True}

    # KR Export: PublicData(Customs) first, ECOS fallback
    public_export_rows = 0
    public_export_used_rows = 0
    ecos_export_rows = 0
    export_loaded = False
    if public_key:
        try:
            df_public_export = _fetch_public_export_series(public_key, start_ym, end_ym, "KR_EXPORT_VALUE")
            public_export_rows = int(len(df_public_export))
            if not df_public_export.empty:
                public_stats = _series_stats(df_public_export)
                public_latest = str(public_stats.get("latest_date") or "")
                public_age = _series_age_days(public_latest, today, "monthly")
                public_max_age = _cycle_to_max_age_days("monthly")
                if public_age is not None and public_age <= public_max_age:
                    public_export_used_rows = int(len(df_public_export))
                    all_frames.append(df_public_export)
                    export_loaded = True
                status["fetch"]["public_data:KR_EXPORT_VALUE"] = {
                    "ok": bool(export_loaded),
                    "rows": public_export_rows,
                    "used_rows": public_export_used_rows,
                    "latest_date": public_latest,
                    "age_days": public_age,
                    "max_age_days": public_max_age,
                    "reason": "ok" if export_loaded else "stale_public_source_blocked",
                }
            else:
                status["fetch"]["public_data:KR_EXPORT_VALUE"] = {"ok": False, "rows": 0, "used_rows": 0, "reason": "no_rows"}
        except Exception as ex:
            status["fetch"]["public_data:KR_EXPORT_VALUE"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}
    else:
        status["fetch"]["public_data:KR_EXPORT_VALUE"] = {"ok": False, "error": "api_key_missing"}

    if not export_loaded:
        if ecos_key:
            try:
                df_ecos_export = _fetch_ecos_series(ecos_key, "403Y001", "M", start_ym, end_ym, "1", "KR_EXPORT_VALUE")
                ecos_export_rows = int(len(df_ecos_export))
                status["fetch"]["ecos:KR_EXPORT_VALUE"] = {"ok": True, "rows": ecos_export_rows, "fallback": True}
                if not df_ecos_export.empty:
                    all_frames.append(df_ecos_export)
            except Exception as ex:
                status["fetch"]["ecos:KR_EXPORT_VALUE"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}", "fallback": True}
        else:
            status["fetch"]["ecos:KR_EXPORT_VALUE"] = {"ok": False, "error": "api_key_missing", "fallback": True}

    # KR Export: final fallback from FRED proxy when public/ecos both unavailable.
    fred_export_rows = 0
    if not export_loaded and fred_key:
        try:
            df_fred_export = _fetch_fred_series(fred_key, "XTEXVA01KRM667S", start_day, today)
            if not df_fred_export.empty:
                df_fred_export = df_fred_export.copy()
                df_fred_export["series_id"] = "KR_EXPORT_VALUE"
                df_fred_export["source"] = "fred_fallback:XTEXVA01KRM667S"
                fred_stats = _series_stats(df_fred_export)
                fred_latest = str(fred_stats.get("latest_date") or "")
                fred_age = _series_age_days(fred_latest, today, "monthly")
                fred_max_age = _cycle_to_max_age_days("monthly")
                if fred_age is not None and fred_age <= fred_max_age:
                    fred_export_rows = int(len(df_fred_export))
                    all_frames.append(df_fred_export)
                    export_loaded = True
                status["fetch"]["fred_fallback:KR_EXPORT_VALUE<=XTEXVA01KRM667S"] = {
                    "ok": bool(export_loaded),
                    "rows": int(len(df_fred_export)),
                    "used_rows": int(fred_export_rows),
                    "fallback": True,
                    "latest_date": fred_latest,
                    "age_days": fred_age,
                    "max_age_days": fred_max_age,
                    "reason": "ok" if export_loaded else "stale_fallback_blocked",
                }
            else:
                status["fetch"]["fred_fallback:KR_EXPORT_VALUE<=XTEXVA01KRM667S"] = {"ok": False, "rows": 0, "fallback": True}
        except Exception as ex:
            status["fetch"]["fred_fallback:KR_EXPORT_VALUE<=XTEXVA01KRM667S"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}", "fallback": True}

    status["source_health"] = {
        "kr_cpi": {
            "active_source": "kosis" if cpi_loaded else "ecos_fallback",
            "kosis_rows": int(kosis_cpi_rows),
            "ecos_rows": int(ecos_cpi_rows),
        },
        "kr_export": {
            "active_source": (
                "public_data"
                if public_export_used_rows > 0
                else ("ecos_fallback" if ecos_export_rows > 0 else ("fred_fallback" if fred_export_rows > 0 else "ecos_fallback_or_pending"))
            ),
            "public_rows": int(public_export_rows),
            "public_used_rows": int(public_export_used_rows),
            "ecos_rows": int(ecos_export_rows),
            "fred_rows": int(fred_export_rows),
            "pending_external_source": bool((public_export_used_rows == 0) and (ecos_export_rows == 0) and (fred_export_rows == 0)),
            "pending_reason": "customs_source_pending" if ((public_export_rows == 0) and (ecos_export_rows == 0) and (fred_export_rows == 0)) else ("stale_public_source_blocked" if ((public_export_used_rows == 0) and (ecos_export_rows == 0) and (fred_export_rows == 0)) else "ok"),
        },
    }

    try:
        df_cboe_put_call = _fetch_cboe_put_call_ratio(as_of_ymd)
        if as_of_ymd and not df_cboe_put_call.empty:
            df_cboe_put_call = df_cboe_put_call[df_cboe_put_call["date"].astype(str) <= str(as_of_ymd)].copy()
        cboe_rows = int(len(df_cboe_put_call))
        if cboe_rows > 0:
            all_frames.append(df_cboe_put_call)
        status["fetch"]["cboe:CBOE_PUT_CALL_TOTAL"] = {
            "ok": bool(cboe_rows > 0),
            "rows": cboe_rows,
            "latest_date": str(df_cboe_put_call["date"].max()) if cboe_rows > 0 else None,
        }
    except Exception as ex:
        status["fetch"]["cboe:CBOE_PUT_CALL_TOTAL"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

    try:
        df_bdi = _fetch_bdi_investing_web(as_of_ymd)
        bdi_rows = int(len(df_bdi))
        if bdi_rows > 0:
            all_frames.append(df_bdi)
        status["fetch"]["web_fallback:BDI_INVESTING_WEB"] = {
            "ok": bool(bdi_rows > 0),
            "rows": bdi_rows,
            "latest_date": str(df_bdi["date"].max()) if bdi_rows > 0 else None,
        }
    except Exception as ex:
        status["fetch"]["web_fallback:BDI_INVESTING_WEB"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

    try:
        df_vkospi = _fetch_vkospi_pykrx(start_day, today)
        if as_of_ymd and not df_vkospi.empty:
            df_vkospi = df_vkospi[df_vkospi["date"].astype(str) <= str(as_of_ymd)].copy()
        vkospi_rows = int(len(df_vkospi))
        if vkospi_rows > 0:
            all_frames.append(df_vkospi)
        status["fetch"]["pykrx:VKOSPI"] = {
            "ok": bool(vkospi_rows > 0),
            "rows": vkospi_rows,
            "latest_date": str(df_vkospi["date"].max()) if vkospi_rows > 0 else None,
        }
    except Exception as ex:
        status["fetch"]["pykrx:VKOSPI"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}"}

    all_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(
        columns=["date", "value", "source", "series_id"]
    )
    if not all_df.empty:
        all_df["date"] = all_df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        all_df["value"] = pd.to_numeric(all_df["value"], errors="coerce")
        all_df = (
            all_df.dropna(subset=["value"])
            .query("date != ''")
            .sort_values(["series_id", "date"])
            .drop_duplicates(["series_id", "date"], keep="last")
        )
    feature_df = all_df.copy()
    if not feature_df.empty and as_of_ymd:
        feature_df = feature_df[feature_df["date"].astype(str) <= str(as_of_ymd)].copy()

    # Build per-series map
    series_map: Dict[str, pd.DataFrame] = {}
    if not feature_df.empty:
        for sid, g in feature_df.groupby("series_id"):
            series_map[str(sid)] = g[["date", "value", "source", "series_id"]].copy()

    # Service PMI fallback: if NAPMNONMFG is unavailable, reuse NAPM as a fallback.
    if ("NAPMNONMFG" not in series_map or series_map.get("NAPMNONMFG", pd.DataFrame()).empty) and ("NAPM" in series_map):
        nm_proxy = series_map.get("NAPM", pd.DataFrame()).copy()
        if not nm_proxy.empty:
            nm_proxy["series_id"] = "NAPMNONMFG"
            nm_proxy["source"] = "proxy:NAPM"
            series_map["NAPMNONMFG"] = nm_proxy
            status["fetch"]["proxy:NAPMNONMFG<=NAPM"] = {"ok": True, "rows": int(len(nm_proxy)), "fallback": True}

    # FRED daily market-rate lag fallback:
    # if DGS10/2Y/3M lags behind as_of_ymd, patch latest rows from US Treasury official daily curve CSV.
    try:
        treasury_map = _fetch_us_treasury_daily_series(start_day, today)
        for sid in ("DGS10", "DGS2", "DTB3"):
            base_df = series_map.get(sid, pd.DataFrame())
            tr_df = treasury_map.get(sid, pd.DataFrame())
            if tr_df is None or tr_df.empty:
                status["fetch"][f"us_treasury:{sid}"] = {"ok": False, "rows": 0}
                continue
            if as_of_ymd:
                tr_df = tr_df[tr_df["date"].astype(str) <= str(as_of_ymd)].copy()
            if tr_df.empty:
                status["fetch"][f"us_treasury:{sid}"] = {"ok": False, "rows": 0, "fallback": True, "reason": "no_rows_at_or_before_asof"}
                continue
            base_latest = str(base_df["date"].astype(str).max()) if (base_df is not None and not base_df.empty) else ""
            tr_latest = str(tr_df["date"].astype(str).max())
            if (not base_latest) or (tr_latest > base_latest):
                merged = pd.concat([base_df, tr_df], ignore_index=True) if (base_df is not None and not base_df.empty) else tr_df.copy()
                merged = merged.sort_values("date").drop_duplicates(["date"], keep="last").reset_index(drop=True)
                series_map[sid] = merged
                status["fetch"][f"us_treasury:{sid}"] = {
                    "ok": True,
                    "rows": int(len(tr_df)),
                    "fallback": True,
                    "latest_date": tr_latest,
                    "patched_over": base_latest or None,
                }
            else:
                status["fetch"][f"us_treasury:{sid}"] = {
                    "ok": True,
                    "rows": int(len(tr_df)),
                    "fallback": True,
                    "latest_date": tr_latest,
                    "patched_over": None,
                }
    except Exception as ex:
        status["fetch"]["us_treasury_daily"] = {"ok": False, "error": f"{type(ex).__name__}: {ex}", "fallback": True}

    _reuse_missing_critical_archive_series(series_map, status, as_of_ymd, today)

    # Preferred rate source for existing macro_signal compatibility
    selected = pd.DataFrame(columns=["date", "rate", "source"])
    for sid in ("BOK_BASE_RATE", "FEDFUNDS", "DGS10"):
        g = series_map.get(sid)
        if g is None or g.empty:
            continue
        x = g[["date", "value", "source"]].copy().rename(columns={"value": "rate"})
        selected = x.sort_values("date").reset_index(drop=True)
        status["selected_source"] = str(x["source"].iloc[-1])
        status["selected_rows"] = int(len(selected))
        status["selected_series_id"] = sid
        break

    if selected.empty:
        status["selected_source"] = "none"
        status["selected_rows"] = 0
        status["selected_series_id"] = "none"

    # External macro features snapshot
    feature_series = {sid: _series_stats(df) for sid, df in series_map.items()}
    feature_signals = _derive_macro_features(series_map)
    fx_context = _derive_fx_context(series_map.get("KR_USDKRW", pd.DataFrame()))
    # [2026-08-21] 신선도는 오늘(today) 기준으로 잰다. as_of_ymd(마지막 매매일)로 재면
    # 절단 지점과 기준점이 같아져 항상 age=0.0 "OK" 가 나온다. PLANS 2026-08-21 (21)
    indicator_mapping = _build_indicator_mapping(feature_series, as_of_ymd, freshness_ref_ymd=today)

    freshness_summary = {
        "ok": int(sum(1 for r in indicator_mapping if str(r.get("freshness")) == "OK")),
        "stale": int(sum(1 for r in indicator_mapping if str(r.get("freshness")) == "STALE")),
        "unknown": int(sum(1 for r in indicator_mapping if str(r.get("freshness")) == "UNKNOWN")),
        "total": int(len(indicator_mapping)),
    }
    status["indicator_source_mapping"] = indicator_mapping
    status["indicator_freshness_summary"] = freshness_summary
    status["indicator_source_tier_summary"] = _summarize_indicator_source_tiers(indicator_mapping)

    feature_obj: Dict[str, Any] = {
        "as_of_ymd": as_of_ymd,
        "generated_for_ymd": today,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signals": feature_signals,
        "fx_context": fx_context,
        "series": feature_series,
        "indicator_source_mapping": indicator_mapping,
        "indicator_freshness_summary": freshness_summary,
        "indicator_source_tier_summary": status["indicator_source_tier_summary"],
        "selected_rate_series_id": status.get("selected_series_id"),
        "selected_rate_source": status.get("selected_source"),
    }
    if _is_empty_fetch_snapshot(series_map, feature_obj):
        fallback_obj, fallback_path = _latest_fresh_macro_feature()
        fallback_reason = "all_external_fetch_failed_preserve_latest_fresh_macro_feature"
        if not (fallback_obj and fallback_path):
            fallback_obj, fallback_path = _latest_valid_macro_feature(today)
            fallback_reason = "all_external_fetch_failed"
        if fallback_obj and fallback_path:
            feature_obj = dict(fallback_obj)
            original_asof = str(feature_obj.get("as_of_ymd") or "")
            feature_obj["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            feature_obj["fallback_reused"] = {
                "enabled": True,
                "source": str(fallback_path),
                "reason": fallback_reason,
                "original_as_of_ymd": original_asof,
                "current_run_ymd": today,
            }
            status["fallback_reused"] = feature_obj["fallback_reused"]
            status["selected_source"] = str(feature_obj.get("selected_rate_source") or status.get("selected_source") or "reused_macro_feature")
            status["selected_rows"] = int(((feature_obj.get("series") or {}).get(str(feature_obj.get("selected_rate_series_id") or ""), {}) or {}).get("rows") or 0)
            status["selected_series_id"] = str(feature_obj.get("selected_rate_series_id") or status.get("selected_series_id") or "reused")
        else:
            feature_obj["fallback_reused"] = {
                "enabled": False,
                "reason": "no_valid_macro_feature_snapshot",
            }
            status["fallback_reused"] = feature_obj["fallback_reused"]

    out_all = LOGS / "rate_series_external_all_latest.csv"
    out_sel = LOGS / "rate_series_external_latest.csv"
    out_status = LOGS / f"rate_series_external_status_{today}.json"
    out_status_latest = LOGS / "rate_series_external_status_latest.json"
    out_feat = LOGS / f"macro_feature_external_{today}.json"
    out_feat_latest = LOGS / "macro_feature_external_latest.json"

    _write_csv(out_all, all_df)
    _write_csv(out_sel, selected)
    _write_json(out_status, status)
    _write_json(out_status_latest, status)
    _write_json(out_feat, feature_obj)
    _write_json(out_feat_latest, feature_obj)

    _log_print(f"[EXT_RATE] selected_source={status.get('selected_source')} rows={status.get('selected_rows')}")
    _log_print(f"[EXT_RATE] wrote={out_sel}")
    _log_print(f"[EXT_MACRO] global_signal={feature_signals.get('global', {}).get('signal')} wrote={out_feat_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




















