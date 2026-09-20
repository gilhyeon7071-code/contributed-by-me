from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import logging


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "_cache"
LOGS = ROOT / "2_Logs"

LISTING = CACHE / "krx_listing.csv"
SECTOR_SSOT = CACHE / "sector_ssot.csv"
SECTOR_MAP = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"
STATUS = LOGS / "krx_reference_cache_sync_latest.json"




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
def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _find_latest_sector_master() -> Path | None:
    files = sorted(
        CACHE.glob("krx_sector_master_*.csv"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )
    return files[0] if files else None


def _find_latest_krx_clean() -> Path | None:
    # Bounded, non-recursive scan (mirrors p0_daily_check.py:_krx_clean_files())
    # so this does not also pick up unrelated backup/tmp copies via a full rglob.
    candidates: list[Path] = []
    seen: set[str] = set()
    for d in (ROOT / "_krx_manual", ROOT / "krx_daily_archive", ROOT):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*_clean.parquet"):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(p)
    files = sorted(candidates, key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    for path in files:
        low = str(path).lower()
        if "\\.venv\\" in low or "\\node_modules\\" in low or "\\__pycache__\\" in low:
            continue
        return path
    return None


def _normalize_sector_master(df: pd.DataFrame) -> pd.DataFrame:
    cols = {str(c).lower(): str(c) for c in df.columns}
    code_col = cols.get("code")
    name_col = cols.get("name")
    sector_col = cols.get("krx_sector")
    industry_col = cols.get("industry")
    if not code_col or not name_col or not sector_col:
        raise ValueError("sector master missing required columns")

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["name"] = df[name_col].astype(str).str.strip()
    out["krx_sector"] = df[sector_col].astype(str).str.strip()
    out["industry"] = df[industry_col].astype(str).str.strip() if industry_col else ""
    out = out[(out["code"] != "") & (out["name"] != "")]
    out = out.drop_duplicates(["code"], keep="last")
    return out[["code", "industry", "krx_sector", "name"]]


def _build_listing_from_sector_source(df: pd.DataFrame) -> pd.DataFrame:
    out = df[["code", "name"]].copy()
    out["code"] = out["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["name"] = out["name"].astype(str).str.strip()
    out = out[(out["code"] != "") & (out["name"] != "")]
    out = out.drop_duplicates(["code"], keep="last")
    return out[["code", "name"]]


def _build_listing_from_clean(path: Path) -> pd.DataFrame:
    raw = pd.read_parquet(path, columns=None)
    cols = {str(c).lower(): str(c) for c in raw.columns}
    code_col = cols.get("code")
    name_col = cols.get("name")
    if not code_col or not name_col:
        raise ValueError("krx clean missing code/name columns")

    out = pd.DataFrame()
    out["code"] = raw[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["name"] = raw[name_col].astype(str).str.strip()
    if "date" in raw.columns:
        out["date"] = pd.to_datetime(raw["date"], errors="coerce")
        out = out.sort_values(["code", "date"]).drop_duplicates(["code"], keep="last")
        out = out.drop(columns=["date"])
    else:
        out = out.drop_duplicates(["code"], keep="last")
    out = out[(out["code"] != "") & (out["name"] != "")]
    return out[["code", "name"]]


def _normalize_sector_map(df: pd.DataFrame) -> pd.DataFrame:
    cols = {str(c).lower(): str(c) for c in df.columns}
    sector_col = cols.get("krx_sector")
    code_col = cols.get("sector_code")
    note_col = cols.get("note")
    if not sector_col or not code_col:
        raise ValueError("sector map missing required columns")

    out = pd.DataFrame()
    out["krx_sector"] = df[sector_col].astype(str).str.strip()
    out["sector_code"] = df[code_col].astype(str).str.strip().str.zfill(3)
    out["note"] = df[note_col].astype(str).str.strip() if note_col else ""
    out = out[(out["krx_sector"] != "") & (out["sector_code"] != "")]
    out = out.drop_duplicates(["krx_sector"], keep="last")
    return out[["krx_sector", "note", "sector_code"]]


def _sync_sector_map_from_ssot(sector_df: pd.DataFrame) -> dict[str, object]:
    if not SECTOR_MAP.exists():
        return {
            "updated": False,
            "source": "missing_map_file",
            "exists": False,
            "auto_supported": False,
            "missing_count": int(sector_df["krx_sector"].nunique()),
        }

    raw_map = _normalize_sector_map(_read_csv_any(SECTOR_MAP))
    ssot_sectors = (
        sector_df[["krx_sector"]]
        .copy()
        .dropna()
        .assign(krx_sector=lambda x: x["krx_sector"].astype(str).str.strip())
    )
    ssot_sectors = ssot_sectors[ssot_sectors["krx_sector"] != ""].drop_duplicates(["krx_sector"])

    merged = ssot_sectors.merge(raw_map, on="krx_sector", how="left")
    missing = merged[merged["sector_code"].isna() | (merged["sector_code"].astype(str).str.strip() == "")]
    if len(missing):
        return {
            "updated": False,
            "source": "existing_hotfix_map",
            "exists": True,
            "auto_supported": False,
            "missing_count": int(len(missing)),
            "missing_sample": missing["krx_sector"].head(10).tolist(),
            "last_write_time": datetime.fromtimestamp(SECTOR_MAP.stat().st_mtime).isoformat(timespec="seconds"),
        }

    merged["note"] = merged["note"].fillna("").astype(str).str.strip()
    merged["sector_code"] = merged["sector_code"].astype(str).str.strip().str.zfill(3)
    merged = merged[["krx_sector", "note", "sector_code"]].sort_values(["sector_code", "krx_sector"]).reset_index(drop=True)

    current = raw_map[["krx_sector", "note", "sector_code"]].sort_values(["sector_code", "krx_sector"]).reset_index(drop=True)
    changed = not merged.equals(current)
    if changed:
        merged.to_csv(SECTOR_MAP, index=False, encoding="utf-8-sig")

    return {
        "updated": bool(changed),
        "source": "existing_hotfix_map",
        "exists": True,
        "auto_supported": True,
        "missing_count": 0,
        "rows": int(len(merged)),
        "last_write_time": datetime.fromtimestamp(SECTOR_MAP.stat().st_mtime).isoformat(timespec="seconds"),
    }

def _check_mojibake(df: pd.DataFrame, columns: list[str], context: str):
    for col in columns:
        if col in df.columns:
            # Check for the Unicode Replacement Character (U+FFFD) which indicates decoding failure
            has_mojibake = df[col].astype(str).str.contains('\ufffd').any()
            if has_mojibake:
                _log_print(f"[FATAL] Mojibake (\ufffd) detected in column '{col}' for context '{context}'. FAIL-CLOSED triggered.")
                raise ValueError(f"Mojibake corruption detected in {context}")

def main() -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)

    status: dict[str, object] = {
        "updated_at": _now(),
        "sector_ssot": {"updated": False, "source": "", "rows": 0},
        "krx_listing": {"updated": False, "source": "", "rows": 0},
        "sector_map": {"updated": False, "source": "", "exists": SECTOR_MAP.exists(), "auto_supported": False},
    }

    latest_sector_master = _find_latest_sector_master()
    if latest_sector_master is not None:
        sector_df = _normalize_sector_master(_read_csv_any(latest_sector_master))
        need_sector_update = (not SECTOR_SSOT.exists()) or (
            latest_sector_master.stat().st_mtime > SECTOR_SSOT.stat().st_mtime
        )
        if need_sector_update:
            _check_mojibake(sector_df, ["name", "krx_sector", "note"], "sector_ssot")
            sector_df.to_csv(SECTOR_SSOT, index=False, encoding="utf-8-sig")
            status["sector_ssot"] = {
                "updated": True,
                "source": str(latest_sector_master),
                "rows": int(len(sector_df)),
            }
        else:
            status["sector_ssot"] = {
                "updated": False,
                "source": str(latest_sector_master),
                "rows": int(len(sector_df)),
            }

        listing_df = _build_listing_from_sector_source(sector_df)
        # Preserve pykrx-enriched names (preferred stocks, etc.) not covered by sector master.
        # Sector master is authoritative for overlapping codes; enriched-only codes are appended.
        _enriched_only_count = 0
        if LISTING.exists():
            try:
                _existing = pd.read_csv(LISTING, dtype={"code": str}, encoding="utf-8-sig")
                _existing["code"] = _existing["code"].astype(str).str.zfill(6)
                _enriched_only = _existing[~_existing["code"].isin(listing_df["code"])][["code", "name"]]
                _enriched_only_count = len(_enriched_only)
                if _enriched_only_count:
                    listing_df = pd.concat([listing_df, _enriched_only], ignore_index=True).drop_duplicates("code", keep="first")
            except Exception as _e:
                _log_print(f"[WARN] listing preserve 실패 (enriched 이름 유지 불가): {_e}")
        need_listing_update = (not LISTING.exists()) or (
            latest_sector_master.stat().st_mtime > LISTING.stat().st_mtime
        ) or (_enriched_only_count > 0)
        if need_listing_update:
            import os as _sync_os
            _check_mojibake(listing_df, ["name", "krx_sector"], "krx_listing")
            _tmp = LISTING.with_suffix(".csv.tmp")
            listing_df.to_csv(_tmp, index=False, encoding="utf-8-sig")
            _sync_os.replace(str(_tmp), str(LISTING))
            status["krx_listing"] = {
                "updated": True,
                "source": str(latest_sector_master),
                "rows": int(len(listing_df)),
                "enriched_preserved": _enriched_only_count,
            }
        else:
            status["krx_listing"] = {
                "updated": False,
                "source": str(latest_sector_master),
                "rows": int(len(listing_df)),
                "enriched_preserved": 0,
            }

        status["sector_map"] = _sync_sector_map_from_ssot(sector_df)
    else:
        latest_clean = _find_latest_krx_clean()
        if latest_clean is not None:
            listing_df = _build_listing_from_clean(latest_clean)
            need_listing_update = (not LISTING.exists()) or (
                latest_clean.stat().st_mtime > LISTING.stat().st_mtime
            )
            if need_listing_update:
                _check_mojibake(listing_df, ["name", "krx_sector"], "krx_listing_from_clean")
                listing_df.to_csv(LISTING, index=False, encoding="utf-8-sig")
            status["krx_listing"] = {
                "updated": bool(need_listing_update),
                "source": str(latest_clean),
                "rows": int(len(listing_df)),
            }
        if SECTOR_SSOT.exists():
            try:
                sector_df = _normalize_sector_master(_read_csv_any(SECTOR_SSOT))
                status["sector_map"] = _sync_sector_map_from_ssot(sector_df)
            except Exception:
                pass

    STATUS.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    _log_print(f"[KRX_REF_SYNC] wrote {STATUS}")
    _log_print(json.dumps(status, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

