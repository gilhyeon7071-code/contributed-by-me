from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCES = [
    ("candidates_final", LOG_DIR / "candidates_latest_data.with_final_score.csv"),
    ("candidates_sector", LOG_DIR / "candidates_latest_data.with_sector_score.csv"),
    ("sector_history", LOG_DIR / "sector_score_history.csv"),
    ("sector_snapshot_today", LOG_DIR / f"sector_score_snapshot_{datetime.now().strftime('%Y%m%d')}.csv"),
]

OUT_LATEST_JSON = LOG_DIR / "sector_mapping_quality_latest.json"
OUT_LATEST_CSV = LOG_DIR / "sector_mapping_quality_latest.csv"
PREFERRED_SHARE_OVERRIDE = ROOT / "_cache" / "sector_preferred_share_override.csv"


def _now() -> datetime:
    return datetime.now()


def _norm_code(v: Any) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except Exception:
            continue
    return pd.DataFrame()


def _is_missing_sector(v: Any) -> bool:
    s = str(v or "").strip()
    return s == "" or s.lower() in {"nan", "none", "null", "<na>", "na"}


def _looks_mojibake(v: Any) -> bool:
    s = str(v or "")
    if not s.strip() or _is_missing_sector(s):
        return False
    bad_char_values = (chr(0xFFFD), "?", chr(0x80), chr(0x9C), chr(0x9D), chr(0x5360))
    bad_chars = sum(s.count(ch) for ch in bad_char_values)
    if bad_chars > 0:
        return True
    artifact = sum(1 for ch in s if 0x80 <= ord(ch) <= 0xFF)
    hangul = sum(1 for ch in s if "\uac00" <= ch <= "\ud7a3")
    return artifact >= 2 and hangul == 0


def _sector_col(df: pd.DataFrame) -> str:
    for c in ("krx_sector", "sector", "sector_name"):
        if c in df.columns:
            return c
    return ""


def _sector_code_col(df: pd.DataFrame) -> str:
    for c in ("sector_code", "krx_sector_code"):
        if c in df.columns:
            return c
    return ""


def _load_source(name: str, path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty or "code" not in df.columns:
        return pd.DataFrame(columns=["source", "code", "krx_sector", "sector_code", "date8", "issue"])
    sec_col = _sector_col(df)
    code_col = _sector_code_col(df)
    out = pd.DataFrame()
    out["code"] = df["code"].map(_norm_code)
    out["source"] = name
    out["krx_sector"] = df[sec_col].astype(str).str.strip() if sec_col else ""
    out["sector_code"] = df[code_col].astype(str).str.strip() if code_col else ""
    out["date8"] = df["date8"].astype(str).str.strip() if "date8" in df.columns else (
        df["date_yyyymmdd"].astype(str).str.strip() if "date_yyyymmdd" in df.columns else ""
    )
    out = out[out["code"] != ""].copy()
    out["missing_sector"] = out["krx_sector"].map(_is_missing_sector)
    out["missing_sector_code"] = out["sector_code"].map(_is_missing_sector)
    out["mojibake_sector"] = out["krx_sector"].map(_looks_mojibake)
    out["issue"] = ""
    out.loc[out["missing_sector"], "issue"] = "missing_sector"
    out.loc[~out["missing_sector"] & out["mojibake_sector"], "issue"] = "mojibake_sector"
    out.loc[~out["missing_sector"] & ~out["mojibake_sector"] & out["missing_sector_code"], "issue"] = "missing_sector_code"
    out.loc[out["issue"] == "", "issue"] = "ok"
    return out


def _load_override() -> pd.DataFrame:
    if not PREFERRED_SHARE_OVERRIDE.exists():
        return pd.DataFrame(columns=["code", "krx_sector", "sector_code", "override_source"])
    df = _read_csv(PREFERRED_SHARE_OVERRIDE)
    if df.empty or "code" not in df.columns:
        return pd.DataFrame(columns=["code", "krx_sector", "sector_code", "override_source"])
    out = pd.DataFrame()
    out["code"] = df["code"].map(_norm_code)
    out["krx_sector"] = df["krx_sector"].astype(str).str.strip() if "krx_sector" in df.columns else ""
    out["sector_code"] = df["sector_code"].astype(str).str.strip() if "sector_code" in df.columns else ""
    out["override_source"] = df["source"].astype(str).str.strip() if "source" in df.columns else "override"
    out = out[(out["code"] != "") & (~out["krx_sector"].map(_is_missing_sector))].copy()
    return out.drop_duplicates("code", keep="last")


def _summarize_source(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"rows": 0, "codes": 0, "ok_codes": 0, "missing_sector": 0, "mojibake_sector": 0, "missing_sector_code": 0}
    by_code = df.drop_duplicates("code", keep="last")
    return {
        "rows": int(len(df)),
        "codes": int(by_code["code"].nunique()),
        "ok_codes": int((by_code["issue"] == "ok").sum()),
        "missing_sector": int((by_code["missing_sector"] == True).sum()),
        "mojibake_sector": int((by_code["mojibake_sector"] == True).sum()),
        "missing_sector_code": int(((by_code["missing_sector_code"] == True) & (by_code["missing_sector"] == False)).sum()),
    }


def main() -> int:
    ts = _now()
    ymd = ts.strftime("%Y%m%d")
    out_json = LOG_DIR / f"sector_mapping_quality_{ymd}.json"
    out_csv = LOG_DIR / f"sector_mapping_quality_{ymd}.csv"

    frames: List[pd.DataFrame] = []
    source_meta: Dict[str, Any] = {}
    for name, path in SOURCES:
        df = _load_source(name, path)
        frames.append(df)
        source_meta[name] = {
            "path": str(path),
            "exists": bool(path.exists()),
            **_summarize_source(df),
        }
    all_rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    priority = {name: i for i, (name, _) in enumerate(SOURCES)}
    consolidated = all_rows.copy()
    if not consolidated.empty:
        consolidated["_priority"] = consolidated["source"].map(priority).fillna(999)
        consolidated = consolidated.sort_values(["code", "_priority"]).drop_duplicates("code", keep="first")
    else:
        consolidated = pd.DataFrame(columns=["source", "code", "krx_sector", "sector_code", "date8", "issue"])

    overrides = _load_override()
    override_applied = 0
    if not consolidated.empty and not overrides.empty:
        o = overrides.set_index("code")
        for idx, row in consolidated.iterrows():
            code = str(row.get("code") or "")
            if code not in o.index:
                continue
            if str(row.get("issue") or "") == "ok":
                continue
            consolidated.at[idx, "krx_sector"] = str(o.at[code, "krx_sector"])
            consolidated.at[idx, "sector_code"] = str(o.at[code, "sector_code"])
            consolidated.at[idx, "source"] = f"{row.get('source')}_override"
            consolidated.at[idx, "missing_sector"] = False
            consolidated.at[idx, "missing_sector_code"] = False
            consolidated.at[idx, "mojibake_sector"] = _looks_mojibake(o.at[code, "krx_sector"])
            consolidated.at[idx, "issue"] = "mojibake_sector" if bool(consolidated.at[idx, "mojibake_sector"]) else "ok"
            override_applied += 1

    issue_rows = consolidated[consolidated["issue"] != "ok"].copy()
    sector_counts = (
        consolidated[consolidated["issue"] == "ok"]
        .groupby(["krx_sector", "sector_code"], dropna=False)["code"]
        .nunique()
        .reset_index(name="code_count")
        .sort_values(["code_count", "krx_sector"], ascending=[False, True])
    )
    detail = consolidated[["source", "date8", "code", "krx_sector", "sector_code", "issue"]].copy()
    detail = detail.fillna("")
    detail.to_csv(out_csv, index=False, encoding="utf-8-sig")
    detail.to_csv(OUT_LATEST_CSV, index=False, encoding="utf-8-sig")

    total_codes = int(consolidated["code"].nunique()) if not consolidated.empty else 0
    issue_count = int(len(issue_rows))
    missing_sector = int((consolidated["missing_sector"] == True).sum()) if "missing_sector" in consolidated.columns else 0
    mojibake_sector = int((consolidated["mojibake_sector"] == True).sum()) if "mojibake_sector" in consolidated.columns else 0
    missing_sector_code = int(((consolidated["missing_sector_code"] == True) & (consolidated["missing_sector"] == False)).sum()) if "missing_sector_code" in consolidated.columns else 0
    status = "PASS" if total_codes > 0 and issue_count == 0 else ("FAIL" if total_codes > 0 else "NO_DATA")
    payload = {
        "generated_at": ts.isoformat(timespec="seconds"),
        "status": status,
        "reason": "ok" if status == "PASS" else ("sector_mapping_issues_found" if status == "FAIL" else "no_source_rows"),
        "total_codes": total_codes,
        "ok_codes": int(total_codes - issue_count),
        "issue_codes": issue_count,
        "missing_sector": missing_sector,
        "mojibake_sector": mojibake_sector,
        "missing_sector_code": missing_sector_code,
        "source_summary": source_meta,
        "override": {
            "path": str(PREFERRED_SHARE_OVERRIDE),
            "exists": bool(PREFERRED_SHARE_OVERRIDE.exists()),
            "rows": int(len(overrides)),
            "applied": int(override_applied),
        },
        "sector_count": int(len(sector_counts)),
        "sector_counts_top": sector_counts.head(30).to_dict(orient="records"),
        "issues_top": issue_rows[["source", "date8", "code", "krx_sector", "sector_code", "issue"]].fillna("").head(100).to_dict(orient="records"),
        "output_csv": str(out_csv),
        "output_latest_csv": str(OUT_LATEST_CSV),
    }
    json_text = json.dumps(payload, ensure_ascii=True, indent=2, allow_nan=False)
    out_json.write_text(json_text, encoding="utf-8")
    OUT_LATEST_JSON.write_text(json_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
