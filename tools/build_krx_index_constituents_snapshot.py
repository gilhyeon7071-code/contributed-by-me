from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import logging


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

OUT_LATEST = CACHE / "krx_index_constituents_latest.csv"
META_LATEST = LOGS / "krx_index_constituents_meta_latest.json"
STATUS_LATEST = LOGS / "krx_index_constituents_status_latest.json"
LISTING = CACHE / "krx_listing.csv"

KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
}

INDEX_MARKET_CODES = {
    "KOSPI": "3",
    "KOSDAQ": "4",
}

TARGET_INDEXES = {
    "KOSPI200": {
        "market": "KOSPI",
        "aliases": ["코스피 200", "코스피200"],
        "size": 200,
    },
    "KOSDAQ150": {
        "market": "KOSDAQ",
        "aliases": ["코스닥 150", "코스닥150"],
        "size": 150,
    },
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
def _post_json(session: requests.Session, payload: dict[str, object]) -> dict:
    resp = session.post(KRX_URL, headers=HEADERS, data=payload, timeout=40)
    resp.raise_for_status()
    return resp.json()


def _bounded_krx_glob() -> list[Path]:
    # Bounded, non-recursive scan (mirrors p0_daily_check.py:_krx_clean_files())
    # so this does not also pick up unrelated backup/tmp copies via rglob.
    out: list[Path] = []
    seen: set[str] = set()
    for d in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*_clean.parquet"):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def _latest_clean_parquet() -> Path:
    files = _bounded_krx_glob()
    def _score(path: Path) -> tuple[int, float]:
        low = str(path).lower()
        name = path.name
        m = re.search(r"krx_daily_(\d{8})_(\d{8})_clean\.parquet$", name)
        end_date = int(m.group(2)) if m else 0
        manual_priority = 1 if "\\_krx_manual\\" in low else 0
        mtime = path.stat().st_mtime if path.exists() else 0.0
        return end_date * 10 + manual_priority, mtime

    files = sorted(files, key=_score, reverse=True)
    for path in files:
        low = str(path).lower()
        if "\\.venv\\" in low or "\\node_modules\\" in low or "\\__pycache__\\" in low:
            continue
        return path
    raise FileNotFoundError("latest krx clean parquet not found")


def _load_listing() -> pd.DataFrame:
    if not LISTING.exists():
        return pd.DataFrame(columns=["code", "name"])
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            df = pd.read_csv(LISTING, encoding=enc, dtype={"code": str})
            break
        except Exception:
            continue
    else:
        df = pd.read_csv(LISTING, dtype={"code": str})
    code_col = df["code"] if "code" in df.columns else pd.Series("", index=df.index)
    name_col = df["name"] if "name" in df.columns else pd.Series("", index=df.index)
    out = pd.DataFrame()
    out["code"] = code_col.astype(str).str.strip().str.zfill(6)
    out["name"] = name_col.astype(str).str.strip()
    out = out[(out["code"] != "")].drop_duplicates(["code"], keep="last")
    return out


def _load_local_market_base() -> tuple[pd.DataFrame, dict[str, object]]:
    path = _latest_clean_parquet()
    raw = pd.read_parquet(path)
    work = raw.copy()
    code_col = work["code"] if "code" in work.columns else pd.Series("", index=work.index)
    market_col = work["market"] if "market" in work.columns else pd.Series("", index=work.index)
    name_col = work["name"] if "name" in work.columns else pd.Series("", index=work.index)
    work["code"] = code_col.astype(str).str.strip().str.zfill(6)
    work["market"] = market_col.astype(str).str.strip().str.upper()
    work["name"] = name_col.astype(str).str.strip()
    for col in ["mktcap", "value", "shares", "close"]:
        col_ser = work[col] if col in work.columns else pd.Series(0, index=work.index)
        work[col] = (
            col_ser
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)

    if "date" in work.columns:
        work["date"] = work["date"].astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
        latest_date8 = str(work["date"].dropna().astype(str).iloc[-1]) if len(work["date"].dropna()) else datetime.now().strftime("%Y%m%d")
        work = work.sort_values(["code", "date"]).drop_duplicates(["code"], keep="last")
    else:
        latest_date8 = datetime.now().strftime("%Y%m%d")
        work = work.drop_duplicates(["code"], keep="last")

    listing = _load_listing()
    if len(listing):
        work = work.merge(listing, on="code", how="left", suffixes=("", "_listing"))
        work["name"] = work["name"].where(work["name"] != "", work["name_listing"].fillna("").astype(str).str.strip())
        if "name_listing" in work.columns:
            work = work.drop(columns=["name_listing"])

    work["ranking_value"] = work["mktcap"].where(work["mktcap"] > 0, work["value"])
    work = work[
        (work["code"].str.match(r"^[0-9]{6}$", na=False))
        & (work["market"].isin(["KOSPI", "KOSDAQ"]))
        & (work["ranking_value"] > 0)
    ].copy()
    work["liquidity_rank"] = work.groupby("market")["value"].rank(method="dense", ascending=False)

    meta = {
        "local_source_file": str(path),
        "local_source_date8": latest_date8,
        "local_rows": int(len(work)),
    }
    return work, meta


def _derive_local_constituents(base: pd.DataFrame, date8: str, index_key: str, market: str, size: int) -> pd.DataFrame:
    subset = base[base["market"] == market].copy()
    subset = subset.sort_values(["ranking_value", "value", "shares"], ascending=[False, False, False])
    subset = subset.head(int(size)).copy()
    out = subset[["code", "name", "close", "mktcap"]].copy().reset_index(drop=True)
    out["date8"] = date8
    out["index_key"] = index_key
    out["index_name"] = "코스피 200" if index_key == "KOSPI200" else "코스닥 150"
    out["index_market"] = market
    out["index_group_id"] = ""
    out["index_ticker_code"] = ""
    out["code"] = out["code"].astype(str).str.strip().str.zfill(6)
    out["name"] = out["name"].astype(str).str.strip()
    out["close"] = out["close"].astype(float)
    out["market_cap"] = out["mktcap"].astype(float)
    out["source"] = "local_derived"
    out = out.drop(columns=["mktcap"])
    return out


def _fetch_index_list(session: requests.Session, market_code: str) -> pd.DataFrame:
    data = _post_json(session, {"bld": "dbms/comm/finder/finder_equidx", "mktsel": market_code})
    return pd.DataFrame(data.get("block1", []))


def _resolve_target_index(row_df: pd.DataFrame, aliases: list[str]) -> pd.Series | None:
    if row_df.empty:
        return None
    work = row_df.copy()
    work["codeName"] = work.get("codeName", "").astype(str).str.strip()
    alias_set = {a.strip() for a in aliases}
    matched = work[work["codeName"].isin(alias_set)].copy()
    if matched.empty:
        alias_no_space = {a.replace(" ", "") for a in alias_set}
        matched = work[work["codeName"].str.replace(" ", "", regex=False).isin(alias_no_space)].copy()
    if matched.empty:
        return None
    return matched.iloc[0]


def _fetch_constituents(session: requests.Session, date8: str, group_id: str, ticker_code: str) -> tuple[pd.DataFrame, str]:
    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00601",
        "trdDd": date8,
        "indIdx": str(group_id),
        "indIdx2": str(ticker_code),
    }
    resp = session.post(KRX_URL, headers=HEADERS, data=payload, timeout=40)
    body = (resp.text or "").strip()
    if resp.status_code >= 400:
        if "LOGOUT" in body.upper():
            return pd.DataFrame(), "login_required"
        return pd.DataFrame(), f"http_{resp.status_code}"
    try:
        data = resp.json()
    except Exception:
        return pd.DataFrame(), "invalid_json"
    return pd.DataFrame(data.get("output", [])), "ok"


def _build_rows(date8: str) -> tuple[pd.DataFrame, dict[str, object], dict[str, object]]:
    base, local_meta = _load_local_market_base()
    session = requests.Session()

    rows: list[pd.DataFrame] = []
    status: dict[str, object] = {
        "date8": date8,
        "targets": {},
        "quality": "FAIL",
        "reason": "not_started",
        "policy": {
            "primary_source": "local_derived",
            "secondary_source": "krx_session",
        },
    }

    for index_key, spec in TARGET_INDEXES.items():
        market_name = str(spec["market"])
        local_rows = _derive_local_constituents(
            base=base,
            date8=date8,
            index_key=index_key,
            market=market_name,
            size=int(spec["size"]),
        )
        target_status: dict[str, object] = {
            "market": market_name,
            "aliases": list(spec["aliases"]),
            "index_found": False,
            "local_rows": int(len(local_rows)),
            "final_rows": int(len(local_rows)),
            "final_source": "local_derived",
            "constituent_rows": 0,
        }

        market_code = INDEX_MARKET_CODES[market_name]
        basics = _fetch_index_list(session, market_code)
        target = _resolve_target_index(basics, list(spec["aliases"]))

        if target is not None:
            target_status["index_found"] = True
            target_status["index_name"] = str(target.get("codeName", "")).strip()
            target_status["group_id"] = str(target.get("full_code", "")).strip()
            target_status["ticker_code"] = str(target.get("short_code", "")).strip()
            cons, cons_reason = _fetch_constituents(
                session=session,
                date8=date8,
                group_id=target_status["group_id"],
                ticker_code=target_status["ticker_code"],
            )
            target_status["constituent_rows"] = int(len(cons))
            target_status["constituent_reason"] = cons_reason

            if len(cons):
                krx_rows = pd.DataFrame({
                    "code": cons.get("ISU_SRT_CD", "").astype(str).str.strip().str.zfill(6),
                    "name": cons.get("ISU_ABBRV", "").astype(str).str.strip(),
                    "close": cons.get("TDD_CLSPRC", "").astype(str).str.strip(),
                    "market_cap": cons.get("MKTCAP", "").astype(str).str.strip(),
                }).reset_index(drop=True)
                krx_rows["date8"] = date8
                krx_rows["index_key"] = index_key
                krx_rows["index_name"] = target_status["index_name"]
                krx_rows["index_market"] = market_name
                krx_rows["index_group_id"] = target_status["group_id"]
                krx_rows["index_ticker_code"] = target_status["ticker_code"]
                krx_rows["source"] = "krx_session"
                local_rows = krx_rows
                target_status["final_rows"] = int(len(local_rows))
                target_status["final_source"] = "krx_session"

        rows.append(local_rows)
        status["targets"][index_key] = target_status

    if rows:
        df = pd.concat(rows, ignore_index=True)
        df = df[(df["code"] != "")].drop_duplicates(["index_key", "code"], keep="first").reset_index(drop=True)
        status["quality"] = "PASS" if len(df) else "FAIL"
        status["reason"] = "ok" if len(df) else "no_index_constituents"
        status["index_total"] = int(df["index_key"].nunique()) if len(df) else 0
        status["symbol_total"] = int(len(df))
        return df, status, local_meta

    status["index_total"] = 0
    status["symbol_total"] = 0
    status["reason"] = "no_index_constituents"
    return pd.DataFrame(), status, local_meta


def _load_latest_snapshot() -> tuple[pd.DataFrame, dict[str, object]]:
    candidates: list[Path] = []
    if OUT_LATEST.exists():
        candidates.append(OUT_LATEST)
    dated = sorted(CACHE.glob("krx_index_constituents_*.csv"), key=lambda p: p.name, reverse=True)
    for p in dated:
        if p not in candidates:
            candidates.append(p)

    required = {"code", "index_key", "date8"}
    for path in candidates:
        try:
            df = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty or not required.issubset(set(df.columns)):
            continue
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
        df = df[(df["code"].str.match(r"^[0-9]{6}$", na=False))].copy()
        if df.empty:
            continue
        meta = {
            "rows": int(len(df)),
            "source_path": str(path),
            "source_date8": str(df["date8"].dropna().astype(str).iloc[0]) if "date8" in df.columns and len(df["date8"].dropna()) else "",
        }
        return df.reset_index(drop=True), meta
    return pd.DataFrame(), {}


def main() -> int:
    CACHE.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    date8 = datetime.now().strftime("%Y%m%d")
    out_dated = CACHE / f"krx_index_constituents_{date8}.csv"
    meta_dated = LOGS / f"krx_index_constituents_meta_{date8}.json"
    status_dated = LOGS / f"krx_index_constituents_status_{date8}.json"

    prev_payload, prev_meta = _load_latest_snapshot()
    payload, status, local_meta = _build_rows(date8)

    if payload.empty and not prev_payload.empty:
        payload = prev_payload.copy()
        status["quality"] = "WARN"
        status["reason"] = "fallback_previous_latest"
        status["index_total"] = int(payload["index_key"].nunique()) if "index_key" in payload.columns else 0
        status["symbol_total"] = int(len(payload))
        status["fallback"] = {
            "used": True,
            "source": str(prev_meta.get("source_path", OUT_LATEST)),
            "source_date8": str(prev_meta.get("source_date8", "")),
            "rows": int(prev_meta.get("rows", len(payload))),
        }
    else:
        status["fallback"] = {"used": False}

    payload.to_csv(out_dated, index=False, encoding="utf-8-sig")
    payload.to_csv(OUT_LATEST, index=False, encoding="utf-8-sig")

    meta = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "date8": date8,
        "rows": int(len(payload)),
        "index_keys": sorted(payload["index_key"].dropna().astype(str).unique().tolist()) if not payload.empty else [],
        "status_file": str(status_dated),
        "out_file": str(out_dated),
        **local_meta,
    }

    meta_text = json.dumps(meta, ensure_ascii=False, indent=2)
    status_text = json.dumps(status, ensure_ascii=False, indent=2)

    meta_dated.write_text(meta_text + "\n", encoding="utf-8")
    META_LATEST.write_text(meta_text + "\n", encoding="utf-8")
    status_dated.write_text(status_text + "\n", encoding="utf-8")
    STATUS_LATEST.write_text(status_text + "\n", encoding="utf-8")

    _log_print(f"[KRX_INDEX] wrote {out_dated}")
    _log_print(status_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

