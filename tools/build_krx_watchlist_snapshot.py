# -*- coding: utf-8 -*-
"""
Build KRX watchlist snapshot from KIND pages.

Outputs:
- _cache/krx_watchlist_latest.csv
- _cache/krx_watchlist_YYYYMMDD.csv
- _cache/krx_watchlist_meta.json
"""

from __future__ import annotations

import argparse
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE_DIR = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
CACHE_DIR = BASE_DIR / "_cache"

ADMIN_URL = "https://kind.krx.co.kr/investwarn/adminissue.do"
ADMIN_REF = "https://kind.krx.co.kr/investwarn/adminissue.do?method=searchAdminIssueList"
INVEST_URL = "https://kind.krx.co.kr/investwarn/investattentwarnrisky.do"
INVEST_REF = "https://kind.krx.co.kr/investwarn/investattentwarnrisky.do?method=investattentwarnriskyMain"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
}


def _norm_code6(v: object) -> str:
    s = str(v).strip()
    s = "".join(ch for ch in s if ch.isalnum())
    if s.isdigit():
        return s.zfill(6)
    return s.upper()


def _pick_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    cmap = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        k = str(a).strip().lower()
        if k in cmap:
            return cmap[k]
    return None


def _to_dt(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(series.astype(str).str.strip(), errors="coerce")


def _read_html_xls(content: bytes) -> pd.DataFrame:
    txt = None
    for enc in ("euc-kr", "cp949", "utf-8"):
        try:
            txt = content.decode(enc, errors="ignore")
            break
        except Exception:
            continue
    if txt is None:
        return pd.DataFrame()

    try:
        tables = pd.read_html(io.StringIO(txt))
    except Exception:
        return pd.DataFrame()

    if not tables:
        return pd.DataFrame()
    return tables[0]


def _post_excel(session: requests.Session, url: str, referer: str, form: dict) -> bytes:
    headers = dict(HEADERS)
    headers["Referer"] = referer
    r = session.post(url, data=form, headers=headers, timeout=40)
    r.raise_for_status()
    return r.content


def _fetch_admin(session: requests.Session) -> tuple[pd.DataFrame, dict]:
    form = {
        "method": "searchAdminIssueSub",
        "forward": "adminissue_down",
        "currentPageSize": "3000",
        "pageIndex": "1",
        "searchMode": "1",
        "searchCodeType": "",
        "searchCorpName": "",
        "repIsuSrtCd": "",
        "marketType": "",
        "orderMode": "",
        "orderStat": "",
    }

    content = _post_excel(session, ADMIN_URL, ADMIN_REF, form)
    raw = _read_html_xls(content)
    if raw.empty:
        return pd.DataFrame(columns=["code", "name", "admin_reason", "admin_date"]), {"rows": 0}

    code_col = _pick_col(raw, ["종목코드", "code", "ticker"])
    name_col = _pick_col(raw, ["종목명", "name"])
    reason_col = _pick_col(raw, ["지정사유", "사유", "유형"])
    date_col = _pick_col(raw, ["지정일", "일자", "공시일"])

    if code_col is None:
        return pd.DataFrame(columns=["code", "name", "admin_reason", "admin_date"]), {"rows": 0}

    out = pd.DataFrame()
    out["code"] = raw[code_col].astype(str).map(_norm_code6)
    out = out[out["code"].str.match(r"^[0-9]{6}$", na=False)].copy()
    out["name"] = raw[name_col].astype(str).str.strip() if name_col else ""
    out["admin_reason"] = raw[reason_col].astype(str).str.strip() if reason_col else ""
    out["admin_date"] = _to_dt(raw[date_col]) if date_col else pd.NaT
    out = out.drop_duplicates("code", keep="first")

    return out, {"rows": int(len(raw)), "codes": int(len(out))}


def _fetch_invest(session: requests.Session, menu_index: str, forward_down: str, start_date: str, end_date: str) -> tuple[pd.DataFrame, dict]:
    form = {
        "method": "investattentwarnriskySub",
        "forward": forward_down,
        "menuIndex": str(menu_index),
        "currentPageSize": "3000",
        "pageIndex": "1",
        "searchCodeType": "",
        "searchCorpName": "",
        "repIsuSrtCd": "",
        "marketType": "",
        "etsIsuSrtCd": "",
        "startDate": start_date,
        "endDate": end_date,
        "searchFromDate": end_date,
        "orderMode": "",
        "orderStat": "",
    }

    content = _post_excel(session, INVEST_URL, INVEST_REF, form)
    raw = _read_html_xls(content)
    if raw.empty:
        return pd.DataFrame(columns=["code", "name", "kind_type", "disclose_date", "designate_date", "release_date"]), {"rows": 0}

    code_col = _pick_col(raw, ["종목코드", "code", "ticker"])
    name_col = _pick_col(raw, ["종목명", "name"])
    type_col = _pick_col(raw, ["유형", "구분"])
    disc_col = _pick_col(raw, ["공시일", "공시일자"])
    des_col = _pick_col(raw, ["지정일", "지정일자"])
    rel_col = _pick_col(raw, ["해제일", "해제일자"])

    if code_col is None:
        return pd.DataFrame(columns=["code", "name", "kind_type", "disclose_date", "designate_date", "release_date"]), {"rows": 0}

    out = pd.DataFrame()
    out["code"] = raw[code_col].astype(str).map(_norm_code6)
    out = out[out["code"].str.match(r"^[0-9]{6}$", na=False)].copy()
    out["name"] = raw[name_col].astype(str).str.strip() if name_col else ""
    out["kind_type"] = raw[type_col].astype(str).str.strip() if type_col else ""
    out["disclose_date"] = _to_dt(raw[disc_col]) if disc_col else pd.NaT
    out["designate_date"] = _to_dt(raw[des_col]) if des_col else pd.NaT
    out["release_date"] = _to_dt(raw[rel_col]) if rel_col else pd.NaT

    info = {
        "rows": int(len(raw)),
        "codes": int(out["code"].nunique()) if "code" in out.columns else 0,
        "truncated_3000": bool(len(raw) >= 3000),
    }
    return out, info


def _active_mask(df: pd.DataFrame, as_of_ts: pd.Timestamp, fallback_days: int = 0) -> pd.Series:
    if df.empty:
        return pd.Series(np.zeros(0, dtype=bool))

    des = _to_dt(df["designate_date"]) if "designate_date" in df.columns else pd.Series(pd.NaT, index=df.index)
    rel = _to_dt(df["release_date"]) if "release_date" in df.columns else pd.Series(pd.NaT, index=df.index)

    has_rel = int(rel.notna().sum()) > 0
    if has_rel:
        return des.notna() & (des <= as_of_ts) & (rel.isna() | (rel >= as_of_ts))

    if fallback_days > 0:
        lb = as_of_ts - pd.Timedelta(days=int(fallback_days))
        return des.notna() & (des >= lb) & (des <= as_of_ts)

    return des.notna() & (des <= as_of_ts)


def _fmt_dt(v: object) -> str:
    try:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _collapse_watch(
    admin_df: pd.DataFrame,
    caution_df: pd.DataFrame,
    warning_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    as_of_ts: pd.Timestamp,
    caution_hold_days: int,
) -> pd.DataFrame:
    admin_df = admin_df.copy()
    caution_df = caution_df.copy()
    warning_df = warning_df.copy()
    risk_df = risk_df.copy()

    if not admin_df.empty:
        admin_df["active"] = True
    if not caution_df.empty:
        caution_df["active"] = _active_mask(caution_df, as_of_ts, fallback_days=caution_hold_days)
    if not warning_df.empty:
        warning_df["active"] = _active_mask(warning_df, as_of_ts, fallback_days=0)
    if not risk_df.empty:
        risk_df["active"] = _active_mask(risk_df, as_of_ts, fallback_days=0)

    admin_act = admin_df[admin_df.get("active", False)].copy() if not admin_df.empty else pd.DataFrame(columns=["code"])
    caut_act = caution_df[caution_df.get("active", False)].copy() if not caution_df.empty else pd.DataFrame(columns=["code"])
    warn_act = warning_df[warning_df.get("active", False)].copy() if not warning_df.empty else pd.DataFrame(columns=["code"])
    risk_act = risk_df[risk_df.get("active", False)].copy() if not risk_df.empty else pd.DataFrame(columns=["code"])

    codes = sorted(
        set(admin_act.get("code", pd.Series(dtype=str)).dropna().tolist())
        | set(caut_act.get("code", pd.Series(dtype=str)).dropna().tolist())
        | set(warn_act.get("code", pd.Series(dtype=str)).dropna().tolist())
        | set(risk_act.get("code", pd.Series(dtype=str)).dropna().tolist())
    )

    if not codes:
        return pd.DataFrame(columns=["code", "name", "krx_admin", "krx_warning", "krx_risk", "krx_caution", "krx_watch_note"])

    out = pd.DataFrame({"code": codes})
    out["krx_admin"] = out["code"].isin(set(admin_act.get("code", [])))
    out["krx_warning"] = out["code"].isin(set(warn_act.get("code", [])))
    out["krx_risk"] = out["code"].isin(set(risk_act.get("code", [])))
    out["krx_caution"] = out["code"].isin(set(caut_act.get("code", [])))

    def _latest_name(df: pd.DataFrame, date_col: str) -> pd.Series:
        if df.empty or "code" not in df.columns:
            return pd.Series(dtype=str)
        z = df.copy()
        if date_col in z.columns:
            z = z.sort_values(date_col, ascending=False, na_position="last")
        z = z.drop_duplicates("code", keep="first")
        if "name" not in z.columns:
            return pd.Series(dtype=str)
        return z.set_index("code")["name"].astype(str)

    name_map = pd.Series(dtype=str)
    for df, dcol in [(admin_act, "admin_date"), (warn_act, "designate_date"), (risk_act, "designate_date"), (caut_act, "designate_date")]:
        nm = _latest_name(df, dcol)
        if nm.empty:
            continue
        if name_map.empty:
            name_map = nm
        else:
            name_map = name_map.combine_first(nm)

    out["name"] = out["code"].map(name_map).fillna("")

    note_map: dict[str, list[str]] = {c: [] for c in codes}

    if not admin_act.empty:
        z = admin_act.sort_values("admin_date", ascending=False, na_position="last").drop_duplicates("code")
        for _, r in z.iterrows():
            reason = str(r.get("admin_reason", "")).strip()
            d = _fmt_dt(r.get("admin_date"))
            msg = "ADMIN"
            if reason:
                msg += f":{reason}"
            if d:
                msg += f"({d})"
            note_map[str(r["code"])].append(msg)

    if not caut_act.empty:
        z = caut_act.sort_values("designate_date", ascending=False, na_position="last").drop_duplicates("code")
        for _, r in z.iterrows():
            typ = str(r.get("kind_type", "")).strip()
            d = _fmt_dt(r.get("designate_date"))
            msg = "CAUTION"
            if typ:
                msg += f":{typ}"
            if d:
                msg += f"({d})"
            note_map[str(r["code"])].append(msg)

    if not warn_act.empty:
        z = warn_act.sort_values("designate_date", ascending=False, na_position="last").drop_duplicates("code")
        for _, r in z.iterrows():
            sd = _fmt_dt(r.get("designate_date"))
            ed = _fmt_dt(r.get("release_date"))
            msg = "WARNING"
            if sd or ed:
                msg += f"({sd}~{ed})"
            note_map[str(r["code"])].append(msg)

    if not risk_act.empty:
        z = risk_act.sort_values("designate_date", ascending=False, na_position="last").drop_duplicates("code")
        for _, r in z.iterrows():
            sd = _fmt_dt(r.get("designate_date"))
            ed = _fmt_dt(r.get("release_date"))
            msg = "RISK"
            if sd or ed:
                msg += f"({sd}~{ed})"
            note_map[str(r["code"])].append(msg)

    out["krx_watch_note"] = out["code"].map(lambda x: " | ".join(note_map.get(x, [])))
    return out


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build KRX watchlist snapshot from KIND")
    ap.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"), help="As-of date YYYYMMDD")
    ap.add_argument("--caution-lookback-days", type=int, default=30, help="Fallback active window(days) for caution rows without release date")
    ap.add_argument("--search-lookback-days", type=int, default=365, help="KIND search period days")
    ap.add_argument("--output", default=str(CACHE_DIR / "krx_watchlist_latest.csv"), help="Output CSV path")
    ap.add_argument("--meta-output", default=str(CACHE_DIR / "krx_watchlist_meta.json"), help="Meta JSON path")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    as_of_ymd = str(args.as_of)
    try:
        as_of_ts = pd.to_datetime(as_of_ymd, format="%Y%m%d", errors="raise")
    except Exception:
        as_of_ts = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        as_of_ymd = as_of_ts.strftime("%Y%m%d")

    search_end = as_of_ts.strftime("%Y-%m-%d")
    search_start = (as_of_ts - timedelta(days=int(max(1, args.search_lookback_days)))).strftime("%Y-%m-%d")
    caution_search_days = int(max(args.caution_lookback_days * 4, 120))
    caution_search_start = (as_of_ts - timedelta(days=caution_search_days)).strftime("%Y-%m-%d")

    session = requests.Session()

    admin_df, admin_info = _fetch_admin(session)
    caut_df, caut_info = _fetch_invest(session, menu_index="1", forward_down="invstcautnisu_down", start_date=caution_search_start, end_date=search_end)
    warn_df, warn_info = _fetch_invest(session, menu_index="2", forward_down="invstwarnisu_down", start_date=search_start, end_date=search_end)
    risk_df, risk_info = _fetch_invest(session, menu_index="3", forward_down="invstriskisu_down", start_date=search_start, end_date=search_end)

    out = _collapse_watch(
        admin_df=admin_df,
        caution_df=caut_df,
        warning_df=warn_df,
        risk_df=risk_df,
        as_of_ts=as_of_ts,
        caution_hold_days=int(max(1, args.caution_lookback_days)),
    )

    out["as_of_ymd"] = as_of_ymd
    out["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    dated_path = CACHE_DIR / f"krx_watchlist_{as_of_ymd}.csv"
    out.to_csv(dated_path, index=False, encoding="utf-8-sig")

    meta = {
        "as_of_ymd": as_of_ymd,
        "search_start": search_start,
        "search_end": search_end,
        "caution_search_start": caution_search_start,
        "caution_search_days": caution_search_days,
        "caution_lookback_days": int(args.caution_lookback_days),
        "search_lookback_days": int(args.search_lookback_days),
        "fetch": {
            "admin": admin_info,
            "caution": caut_info,
            "warning": warn_info,
            "risk": risk_info,
        },
        "active": {
            "rows_total": int(len(out)),
            "admin_flags": int(out.get("krx_admin", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not out.empty else 0,
            "warning_flags": int(out.get("krx_warning", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not out.empty else 0,
            "risk_flags": int(out.get("krx_risk", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not out.empty else 0,
            "caution_flags": int(out.get("krx_caution", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not out.empty else 0,
        },
        "output": str(out_path),
        "dated_output": str(dated_path),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    meta_path = Path(args.meta_output)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[WATCH] done rows={rows} admin={admin} warning={warn} risk={risk} caution={caut}".format(
            rows=int(len(out)),
            admin=int(meta["active"]["admin_flags"]),
            warn=int(meta["active"]["warning_flags"]),
            risk=int(meta["active"]["risk_flags"]),
            caut=int(meta["active"]["caution_flags"]),
        )
    )
    print(f"[WATCH] output={out_path}")
    print(f"[WATCH] meta={meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
