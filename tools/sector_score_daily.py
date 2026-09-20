import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import logging
import requests

ROOTA = Path(__file__).resolve().parents[1]
LOGS = ROOTA / "2_Logs"
CACHE = ROOTA / "_cache"

FILTERED_CAND = LOGS / "candidates_latest_data.filtered.csv"
RAW_CAND = LOGS / "candidates_latest_data.csv"
CAND = FILTERED_CAND if FILTERED_CAND.exists() else RAW_CAND
SSOT_SECTOR = CACHE / "sector_ssot.csv"
MAP = CACHE / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"
PREFERRED_SHARE_OVERRIDE = CACHE / "sector_preferred_share_override.csv"

OUT = LOGS / "candidates_latest_data.with_sector_score.csv"
HIST = LOGS / "sector_score_history.csv"
STATUS_LATEST = LOGS / "sector_score_status_latest.json"

SNAP_COLS = [
    "date8", "code", "krx_sector", "sector_code", "sector_action", "sector_entry_allowed",
    "sector_entry_weight", "sector_score", "sector_strength", "sector_leader_code",
    "sector_leader_name", "sector_leader_coupling", "sector_leader_effect",
    "sector_reason", "sector_policy_reason", "leader_coupling_reason",
]
ALLOWED_SECTOR_CODES = {"005", "008", "009", "011", "012", "013", "015", "016", "017", "018", "019", "020", "022", "024", "025", "026"}
WAIT_ENTRY_THRESHOLD = 0.40
WAIT_PARTIAL_THRESHOLD = 0.20
WAIT_PARTIAL_WEIGHT = 0.50
SECTOR_SCORE_MULTIPLIER = {
    "BUY": 1.00,
    "WAIT": 0.35,
    "SELL": -0.30,
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
def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _extract_date8(cand: pd.DataFrame) -> str:
    if "date" in cand.columns:
        raw = cand["date"].dropna()
        if len(raw) > 0:
            try:
                return str(raw.iloc[0]).replace("-", "").replace("/", "")[:8]
            except Exception:
                pass
    return datetime.now().strftime("%Y%m%d")


def _resolve_window(cand: pd.DataFrame) -> tuple[str, str, str]:
    date8 = _extract_date8(cand)
    try:
        asof = datetime.strptime(date8, "%Y%m%d")
    except Exception:
        asof = datetime.now()
        date8 = asof.strftime("%Y%m%d")

    end_date = asof.strftime("%Y-%m-%d")
    start_date = (asof - timedelta(days=550)).strftime("%Y-%m-%d")
    return start_date, end_date, date8


def _load_candidates() -> pd.DataFrame:
    cand = pd.read_csv(CAND, dtype={"code": str})
    if RAW_CAND.exists():
        raw = pd.read_csv(RAW_CAND, dtype={"code": str})
        if len(cand) > 0 and len(raw) > 0 and CAND != RAW_CAND:
            merge_keys = [c for c in ["code", "date", "name"] if c in cand.columns and c in raw.columns]
            extra_cols = [c for c in raw.columns if c not in cand.columns]
            if merge_keys and extra_cols:
                cand = cand.merge(raw[merge_keys + extra_cols], on=merge_keys, how="left")
        if len(cand) > 0 or CAND == RAW_CAND:
            return cand
        if len(raw) > 0:
            _log_print(f"[SECTOR] fallback raw candidates: filtered empty -> {RAW_CAND}")
            return raw
    if len(cand) > 0 or CAND == RAW_CAND or not RAW_CAND.exists():
        return cand
    return cand


def _clean_text(v: object) -> str:
    s = str(v if v is not None else "").strip()
    if s.lower() in {"", "nan", "none", "<na>"}:
        return ""
    return s


def _normalize_sector_code(v: object) -> str:
    s = _clean_text(v)
    if not s:
        return ""
    if s.endswith(".0"):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(3)


def _load_preferred_share_override() -> pd.DataFrame:
    if not PREFERRED_SHARE_OVERRIDE.exists():
        return pd.DataFrame(columns=["code", "krx_sector", "sector_code"])
    ov = pd.read_csv(
        PREFERRED_SHARE_OVERRIDE,
        dtype={"code": str, "krx_sector": str, "sector_code": str},
    )
    for col in ["code", "krx_sector", "sector_code"]:
        if col not in ov.columns:
            return pd.DataFrame(columns=["code", "krx_sector", "sector_code"])
    ov = ov[["code", "krx_sector", "sector_code"]].copy()
    ov["code"] = ov["code"].astype(str).str.zfill(6)
    ov["krx_sector"] = ov["krx_sector"].map(_clean_text)
    ov["sector_code"] = ov["sector_code"].map(_normalize_sector_code)
    ov = ov[(ov["code"] != "") & (ov["krx_sector"] != "") & (ov["sector_code"] != "")]
    return ov.drop_duplicates(["code"], keep="last")


def _apply_preferred_share_override(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if df is None or len(df) == 0 or "code" not in df.columns:
        return df, 0

    ov = _load_preferred_share_override()
    if ov.empty:
        return df, 0

    out = df.copy()
    if "krx_sector" not in out.columns:
        out["krx_sector"] = ""
    if "sector_code" not in out.columns:
        out["sector_code"] = ""

    out["code"] = out["code"].astype(str).str.zfill(6)
    out["krx_sector"] = out["krx_sector"].map(_clean_text)
    out["sector_code"] = out["sector_code"].map(_normalize_sector_code)

    ov_map = {
        str(row["code"]).zfill(6): (str(row["krx_sector"]).strip(), str(row["sector_code"]).strip())
        for _, row in ov.iterrows()
    }
    applied = 0
    for idx, row in out.iterrows():
        code = str(row.get("code", "")).zfill(6)
        override = ov_map.get(code)
        if not override:
            continue
        current_sector = _clean_text(row.get("krx_sector", ""))
        current_code = _normalize_sector_code(row.get("sector_code", ""))
        if current_sector and current_code in ALLOWED_SECTOR_CODES:
            continue
        out.at[idx, "krx_sector"] = override[0]
        out.at[idx, "sector_code"] = override[1]
        applied += 1

    return out, applied


def _sector_override_status(applied: int) -> dict:
    return {
        "path": str(PREFERRED_SHARE_OVERRIDE),
        "exists": bool(PREFERRED_SHARE_OVERRIDE.exists()),
        "applied": int(applied),
    }


def _save_snapshot(df: pd.DataFrame, date8: str) -> None:
    df["date8"] = date8
    snap = df[[c for c in SNAP_COLS if c in df.columns]].copy()

    snap_path = LOGS / f"sector_score_snapshot_{date8}.csv"
    snap.to_csv(snap_path, index=False, encoding="utf-8-sig")
    _log_print(f"SNAP {snap_path} rows={len(snap)}")

    if HIST.exists():
        try:
            hist = pd.read_csv(HIST, dtype={"code": str, "date8": str})
            hist = hist[hist["date8"] != date8]
            hist = pd.concat([hist, snap], ignore_index=True)
        except Exception:
            hist = snap.copy()
    else:
        hist = snap.copy()

    hist, history_override_applied = _apply_preferred_share_override(hist)
    hist.to_csv(HIST, index=False, encoding="utf-8-sig")
    _log_print(f"HIST {HIST} rows={len(hist)} (+{len(snap)} for {date8}) preferred_override={history_override_applied}")


def _build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-mock", action="store_true", help="Force mock mode even when KRX key exists")
    return ap.parse_args()


def _load_krx_key(explicit_key: str) -> str:
    key = str(explicit_key or "").strip()
    if key:
        return key
    key_path = CACHE / "krx_api_key.txt"
    if not key_path.exists():
        return ""
    try:
        return str(key_path.read_text(encoding="utf-8")).strip()
    except Exception:
        return ""


def _write_status(date8: str, payload: dict) -> None:
    path = LOGS / f"sector_score_status_{date8}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text + "\n", encoding="utf-8")
    STATUS_LATEST.write_text(text + "\n", encoding="utf-8")


def _load_latest_valid_snapshot(current_date8: str) -> tuple[pd.DataFrame | None, str | None]:
    snaps = sorted(LOGS.glob("sector_score_snapshot_*.csv"), reverse=True)
    for path in snaps:
        snap_date8 = path.stem.replace("sector_score_snapshot_", "").strip()
        if not snap_date8 or snap_date8 >= current_date8:
            continue
        try:
            snap = pd.read_csv(path, dtype={"code": str, "sector_code": str, "date8": str})
        except Exception:
            continue
        if snap is None or len(snap) == 0 or "sector_code" not in snap.columns:
            continue
        work = snap.copy()
        work["sector_code"] = work["sector_code"].astype(str).str.strip()
        if "sector_action" in work.columns:
            work["sector_action"] = work["sector_action"].astype(str).str.strip().str.upper()
        if "sector_strength" in work.columns:
            work["sector_strength"] = pd.to_numeric(work["sector_strength"], errors="coerce").fillna(0.0)
        else:
            work["sector_strength"] = 0.0
        meaningful = work[
            (work["sector_code"] != "")
            & (
                (work["sector_action"] != "")
                | (work["sector_strength"] > 0)
            )
        ].copy()
        if len(meaningful) == 0:
            continue
        return meaningful, snap_date8
    return None, None


def _direct_sector_cache_exists(date8: str) -> bool:
    p = ROOTA / "_cache" / "krx_sector_daily" / f"krx_sector_{date8}.json"
    if not p.exists():
        return False
    try:
        rows = json.loads(p.read_text(encoding="utf-8"))
        return isinstance(rows, list) and len(rows) > 0
    except Exception:
        return False


def _fast_krx_sector_preflight(date8: str, timeout_sec: float, auth_key: str = "") -> tuple[bool, str]:
    try:
        from data.krx_api import KRXClient

        r = requests.get(
            KRXClient.DIRECT_INDEX_URL,
            params={"basDd": str(date8)},
            timeout=max(1.0, float(timeout_sec)),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020501",
                **({"AUTH_KEY": auth_key} if auth_key else {}),
            },
        )
        if int(r.status_code) >= 400:
            return False, f"http_status_{r.status_code}"
        payload = r.json()
        rows = payload.get("OutBlock_1") or []
        if not isinstance(rows, list) or len(rows) == 0:
            return False, "empty_payload"
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}:{e}"


def _apply_snapshot_fallback(df: pd.DataFrame, snap: pd.DataFrame, p0_regime: str) -> tuple[pd.DataFrame, list[str]]:
    if snap is None or len(snap) == 0 or "sector_code" not in snap.columns:
        return df, []

    snap_cols = [c for c in ["sector_code", "sector_action", "sector_strength"] if c in snap.columns]
    snap_work = snap[snap_cols].copy()
    snap_work["sector_code"] = snap_work["sector_code"].astype(str).str.strip()
    snap_work["sector_action"] = snap_work["sector_action"].astype(str).str.strip().str.upper()
    snap_work["sector_strength"] = pd.to_numeric(snap_work["sector_strength"], errors="coerce").fillna(0.0)
    snap_work = snap_work[snap_work["sector_code"] != ""].drop_duplicates(["sector_code"], keep="last")
    if len(snap_work) == 0:
        return df, []

    sig_map = {
        str(row["sector_code"]).strip(): (str(row["sector_action"]).strip().upper(), float(row["sector_strength"]))
        for _, row in snap_work.iterrows()
    }
    used_codes: set[str] = set()

    for i, row in df.iterrows():
        sector_code = str(row.get("sector_code", "") or "").strip()
        if sector_code in sig_map:
            act, strength = sig_map[sector_code]
            result = calc_sector_result(act, strength, p0_regime)
            df.at[i, "sector_action"] = act
            df.at[i, "sector_entry_allowed"] = bool(result.get("entry_allowed", False))
            df.at[i, "sector_entry_weight"] = float(result.get("entry_weight", 0.0))
            df.at[i, "sector_strength"] = strength
            df.at[i, "sector_score"] = float(result.get("sector_score", 0.0))
            df.at[i, "sector_reason"] = str(result.get("sector_reason", ""))
            df.at[i, "sector_policy_reason"] = str(result.get("sector_policy_reason", ""))
            used_codes.add(sector_code)

    return df, sorted(used_codes)


def _business_lag_days(candidate_date8: str, source_date8: str) -> int | None:
    if not candidate_date8 or not source_date8:
        return None
    try:
        cdt = pd.to_datetime(candidate_date8, format="%Y%m%d", errors="raise")
        sdt = pd.to_datetime(source_date8, format="%Y%m%d", errors="raise")
    except Exception:
        return None
    if sdt > cdt:
        return 0
    return max(0, len(pd.bdate_range(sdt, cdt)) - 1)


def _load_p0_regime() -> str:
    files = sorted(LOGS.glob("p0_daily_check_*.json"), key=lambda p: p.stat().st_mtime)
    for path in reversed(files):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        regime = str(obj.get("market_regime") or "").upper()
        if regime:
            return regime
    return "NORMAL"


def calc_sector_result(sector_action: str, strength: float, p0_regime: str) -> dict:
    act = str(sector_action or "").strip().upper()
    try:
        raw_strength = max(0.0, float(strength))
    except Exception:
        raw_strength = 0.0

    entry_weight = 0.0
    if act == "BUY":
        entry_allowed = True
        entry_weight = 1.0
    elif act == "WAIT":
        if raw_strength >= WAIT_ENTRY_THRESHOLD:
            entry_allowed = True
            entry_weight = 1.0
        elif raw_strength >= WAIT_PARTIAL_THRESHOLD:
            entry_allowed = True
            entry_weight = WAIT_PARTIAL_WEIGHT
        else:
            entry_allowed = False
    else:
        entry_allowed = False

    if not entry_allowed:
        sector_score = 0.0
    else:
        multiplier = float(SECTOR_SCORE_MULTIPLIER.get(act, 0.0))
        if p0_regime in ("BEAR", "CRASH") and multiplier < 0:
            multiplier *= 1.5
        sector_score = round(raw_strength * multiplier, 4)

    if act == "BUY":
        policy_reason = "BUY action allows full sector entry weight"
    elif act == "WAIT" and raw_strength >= WAIT_ENTRY_THRESHOLD:
        policy_reason = f"WAIT strength >= {WAIT_ENTRY_THRESHOLD:.2f} allows full sector entry weight"
    elif act == "WAIT" and raw_strength >= WAIT_PARTIAL_THRESHOLD:
        policy_reason = f"WAIT strength >= {WAIT_PARTIAL_THRESHOLD:.2f} allows partial sector entry weight"
    elif act == "WAIT":
        policy_reason = f"WAIT strength < {WAIT_PARTIAL_THRESHOLD:.2f} blocks sector entry"
    elif act == "SELL":
        policy_reason = "SELL action blocks sector entry"
    else:
        policy_reason = "missing or unsupported sector action blocks sector entry"

    multiplier_used = float(SECTOR_SCORE_MULTIPLIER.get(act, 0.0))
    if p0_regime in ("BEAR", "CRASH") and multiplier_used < 0:
        multiplier_used *= 1.5
    sector_reason = (
        f"action={act or 'NONE'};strength={raw_strength:.6f};"
        f"entry_allowed={str(bool(entry_allowed)).lower()};entry_weight={entry_weight:.2f};"
        f"multiplier={multiplier_used:.2f};p0_regime={str(p0_regime or '').upper() or 'UNKNOWN'};"
        f"sector_score={float(sector_score):.6f}"
    )

    return {
        "entry_allowed": bool(entry_allowed),
        "entry_weight": float(entry_weight),
        "sector_score": float(sector_score),
        "sector_reason": sector_reason,
        "sector_policy_reason": policy_reason,
    }


def _apply_sector_leader_coupling(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    out = df.copy()
    for col, default in (
        ("sector_leader_code", ""),
        ("sector_leader_name", ""),
        ("sector_leader_coupling", 0.0),
        ("sector_leader_effect", 0.0),
        ("leader_coupling_reason", ""),
    ):
        if col not in out.columns:
            out[col] = default
    if out.empty or "sector_code" not in out.columns:
        return out, {"status": "SKIP", "reason": "empty_or_missing_sector_code", "groups": 0}

    work = out.copy()
    work["sector_code"] = work["sector_code"].astype(str).str.strip()
    work = work[(work["sector_code"] != "") & (work["sector_code"].str.lower() != "nan")].copy()
    if work.empty:
        return out, {"status": "SKIP", "reason": "no_sector_rows", "groups": 0}

    if "market_cap" in work.columns:
        work["_leader_size"] = pd.to_numeric(work["market_cap"], errors="coerce").fillna(0.0)
    elif "value" in work.columns:
        work["_leader_size"] = pd.to_numeric(work["value"], errors="coerce").fillna(0.0)
    else:
        work["_leader_size"] = 0.0

    leaders = (
        work.sort_values(["sector_code", "_leader_size", "code"], ascending=[True, False, True])
            .groupby("sector_code", as_index=False, group_keys=False)
            .head(1)
    )
    leader_map = {str(r["sector_code"]): r for _, r in leaders.iterrows()}
    adjusted = 0
    for idx, row in out.iterrows():
        sector_code = str(row.get("sector_code", "") or "").strip()
        leader = leader_map.get(sector_code)
        if leader is None:
            continue
        ret1 = pd.to_numeric(pd.Series([leader.get("ret1_pct", 0.0)]), errors="coerce").fillna(0.0).iloc[0] / 100.0
        rs_slope = pd.to_numeric(pd.Series([leader.get("rs_slope", 1.0)]), errors="coerce").fillna(1.0).iloc[0]
        v_accel = pd.to_numeric(pd.Series([leader.get("v_accel", 1.0)]), errors="coerce").fillna(1.0).iloc[0]
        macd = str(leader.get("macd_golden", "")).strip().lower() in {"true", "1", "yes"}
        momentum = (
            max(-1.0, min(1.0, float(ret1) / 0.05)) * 0.35
            + max(-1.0, min(1.0, (float(rs_slope) - 1.0) / 0.35)) * 0.30
            + max(-1.0, min(1.0, (float(v_accel) - 1.0) / 2.0)) * 0.25
            + (0.10 if macd else 0.0)
        )
        momentum = max(-1.0, min(1.0, float(momentum)))
        effect = round(momentum * 0.08, 6)
        out.at[idx, "sector_leader_code"] = str(leader.get("code", "") or "").zfill(6)
        out.at[idx, "sector_leader_name"] = str(leader.get("name", "") or "")
        out.at[idx, "sector_leader_coupling"] = round(momentum, 6)
        out.at[idx, "sector_leader_effect"] = effect
        out.at[idx, "leader_coupling_reason"] = (
            f"leader={str(leader.get('code', '') or '').zfill(6)};"
            f"ret1={float(ret1):.6f};rs_slope={float(rs_slope):.6f};"
            f"v_accel={float(v_accel):.6f};macd_golden={str(bool(macd)).lower()};"
            f"coupling={momentum:.6f};effect={effect:.6f}"
        )
        has_sector_signal = str(row.get("sector_action", "") or "").strip() != "" or float(pd.to_numeric(pd.Series([row.get("sector_strength", 0.0)]), errors="coerce").fillna(0.0).iloc[0]) > 0
        if has_sector_signal:
            base = float(pd.to_numeric(pd.Series([row.get("sector_score", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            out.at[idx, "sector_score"] = round(max(-1.0, min(1.0, base + effect)), 6)
            if "sector_reason" in out.columns:
                prior = str(out.at[idx, "sector_reason"] or "").strip()
                after = float(out.at[idx, "sector_score"])
                detail = f"leader_effect={effect:.6f};sector_score_after_leader={after:.6f}"
                out.at[idx, "sector_reason"] = f"{prior};{detail}" if prior else detail
            adjusted += 1

    return out, {"status": "OK", "groups": int(len(leaders)), "adjusted_rows": int(adjusted)}


def main() -> int:
    args = _build_args()

    if not CAND.exists():
        raise SystemExit(f"FATAL missing {CAND}")
    if not SSOT_SECTOR.exists():
        raise SystemExit(f"FATAL missing {SSOT_SECTOR}")
    if not MAP.exists():
        raise SystemExit(f"FATAL missing {MAP}")

    cand = _load_candidates()
    cand["code"] = cand["code"].astype(str).str.zfill(6)

    ssot = pd.read_csv(SSOT_SECTOR, dtype={"code": str, "krx_sector": str})
    ssot["code"] = ssot["code"].astype(str).str.zfill(6)
    ssot["krx_sector"] = ssot["krx_sector"].astype(str).str.strip()

    mp = pd.read_csv(MAP, dtype={"krx_sector": str, "sector_code": str})
    mp["krx_sector"] = mp["krx_sector"].astype(str).str.strip()
    mp["sector_code"] = mp["sector_code"].map(_normalize_sector_code)

    df = cand.merge(ssot[["code", "krx_sector"]], on="code", how="left")
    df["krx_sector"] = df["krx_sector"].map(_clean_text)
    df = df.merge(mp[["krx_sector", "sector_code"]], on="krx_sector", how="left")
    df, sector_override_applied = _apply_preferred_share_override(df)
    sector_override_info = _sector_override_status(sector_override_applied)

    sc = df["sector_code"].map(_normalize_sector_code)
    df["sector_code"] = sc
    sector_codes = sorted([x for x in sc.unique().tolist() if x and x.lower() != "nan" and x in ALLOWED_SECTOR_CODES])

    df["sector_score"] = 0.0
    df["sector_action"] = ""
    df["sector_entry_allowed"] = False
    df["sector_entry_weight"] = 0.0
    df["sector_strength"] = 0.0
    df["sector_reason"] = ""
    df["sector_policy_reason"] = ""
    df["leader_coupling_reason"] = ""

    start_date, end_date, date8 = _resolve_window(cand)

    if not sector_codes:
        df, leader_info = _apply_sector_leader_coupling(df)
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        _log_print("WROTE", OUT, "note=no_sector_codes")
        _write_status(date8, {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "candidate_date8": date8,
            "sector_latest_date8": None,
            "lag_business_days": None,
            "freshness": "NO_SECTOR_CODES",
            "status": "PASS",
            "leader_coupling": leader_info,
            "sector_override": sector_override_info,
        })
        _save_snapshot(df, date8)
        return 0

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_dev" / "kospi_sector"))
    from data.krx_api import KRXClient
    from data.oecd_cli import OECDCLIClient
    from strategy.sector_signals import SectorSignalEngine

    krx_key = _load_krx_key(os.getenv("KRX_API_KEY", ""))
    force_mock = bool(args.force_mock or _env_flag("SECTOR_SCORE_FORCE_MOCK", False))
    use_mock = force_mock or (not krx_key)

    krx = KRXClient(auth_key=(None if use_mock else krx_key), mock=use_mock)
    oecd = OECDCLIClient(mock=force_mock)

    _log_print(f"[SECTOR] mode={'MOCK' if use_mock else 'REAL'} krx_key={'SET' if krx_key else 'EMPTY'} start={start_date} end={end_date}")

    p0_regime = _load_p0_regime()
    fallback_snap, fallback_date8 = _load_latest_valid_snapshot(date8)
    if (not use_mock) and fallback_snap is not None and fallback_date8 and (not _direct_sector_cache_exists(date8)):
        preflight_timeout = float(os.getenv("SECTOR_SCORE_KRX_PREFLIGHT_TIMEOUT_SEC", "5") or "5")
        preflight_ok, preflight_reason = _fast_krx_sector_preflight(date8, preflight_timeout, krx_key)
        if not preflight_ok:
            df, used_codes = _apply_snapshot_fallback(df, fallback_snap, p0_regime)
            df, leader_info = _apply_sector_leader_coupling(df)
            lag_business_days = _business_lag_days(date8, fallback_date8)
            status = "PASS" if lag_business_days is not None and lag_business_days <= 1 else "WARN"
            df.to_csv(OUT, index=False, encoding="utf-8-sig")
            _log_print("WROTE", OUT, f"note=fallback_prev_snapshot_fast_preflight reason={preflight_reason} source={fallback_date8}")
            _write_status(date8, {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "candidate_date8": date8,
                "sector_latest_date8": fallback_date8,
                "lag_business_days": lag_business_days,
                "freshness": "FALLBACK_PREV_VALID_API_PREFLIGHT_FAIL",
                "status": status,
                "sector_codes": used_codes,
                "rows": int(len(df)),
                "leader_coupling": leader_info,
                "sector_override": sector_override_info,
                "fallback_reason": preflight_reason,
                "preflight_timeout_sec": preflight_timeout,
            })
            _save_snapshot(df, date8)
            return 0

    sector_data = {}
    for code in sector_codes:
        try:
            s = krx.get_sector_index(code, start_date, end_date)
            if s is None or len(s) == 0 or ("close" not in s.columns):
                continue
            sector_data[code] = s[["close"]].rename(columns={"close": code})
        except Exception as e:
            _log_print(f"[SECTOR][WARN] code={code} fetch failed: {type(e).__name__}:{e}")

    if not sector_data:
        p0_regime = _load_p0_regime()
        fallback_snap, fallback_date8 = _load_latest_valid_snapshot(date8)
        if fallback_snap is not None and fallback_date8:
            df, used_codes = _apply_snapshot_fallback(df, fallback_snap, p0_regime)
            df, leader_info = _apply_sector_leader_coupling(df)
            lag_business_days = _business_lag_days(date8, fallback_date8)
            status = "PASS" if lag_business_days is not None and lag_business_days <= 1 else "WARN"
            df.to_csv(OUT, index=False, encoding="utf-8-sig")
            _log_print("WROTE", OUT, f"note=fallback_prev_snapshot source={fallback_date8}")
            _write_status(date8, {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "candidate_date8": date8,
                "sector_latest_date8": fallback_date8,
                "lag_business_days": lag_business_days,
                "freshness": "FALLBACK_PREV_VALID",
                "status": status,
                "sector_codes": used_codes,
                "rows": int(len(df)),
                "leader_coupling": leader_info,
                "sector_override": sector_override_info,
            })
            _save_snapshot(df, date8)
            return 0
        df, leader_info = _apply_sector_leader_coupling(df)
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        _log_print("WROTE", OUT, "note=no_sector_data")
        _write_status(date8, {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "candidate_date8": date8,
            "sector_latest_date8": None,
            "lag_business_days": None,
            "freshness": "NO_DATA",
            "status": "WARN",
            "leader_coupling": leader_info,
            "sector_override": sector_override_info,
        })
        _save_snapshot(df, date8)
        return 0

    prices = pd.concat([sector_data[c] for c in sorted(sector_data.keys())], axis=1).dropna(how="all")
    if prices is None or len(prices) == 0:
        p0_regime = _load_p0_regime()
        fallback_snap, fallback_date8 = _load_latest_valid_snapshot(date8)
        if fallback_snap is not None and fallback_date8:
            df, used_codes = _apply_snapshot_fallback(df, fallback_snap, p0_regime)
            df, leader_info = _apply_sector_leader_coupling(df)
            lag_business_days = _business_lag_days(date8, fallback_date8)
            status = "PASS" if lag_business_days is not None and lag_business_days <= 1 else "WARN"
            df.to_csv(OUT, index=False, encoding="utf-8-sig")
            _log_print("WROTE", OUT, f"note=fallback_prev_snapshot source={fallback_date8}")
            _write_status(date8, {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "candidate_date8": date8,
                "sector_latest_date8": fallback_date8,
                "lag_business_days": lag_business_days,
                "freshness": "FALLBACK_PREV_VALID",
                "status": status,
                "sector_codes": used_codes,
                "rows": int(len(df)),
                "leader_coupling": leader_info,
                "sector_override": sector_override_info,
            })
            _save_snapshot(df, date8)
            return 0
        df, leader_info = _apply_sector_leader_coupling(df)
        df.to_csv(OUT, index=False, encoding="utf-8-sig")
        _log_print("WROTE", OUT, "note=empty_prices_matrix")
        _write_status(date8, {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "candidate_date8": date8,
            "sector_latest_date8": None,
            "lag_business_days": None,
            "freshness": "EMPTY",
            "status": "WARN",
            "leader_coupling": leader_info,
            "sector_override": sector_override_info,
        })
        _save_snapshot(df, date8)
        return 0

    cli_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
    cli_df = oecd.get_cli(start=cli_start)
    cli_gate = cli_df.set_index("date")["risk_on"]
    cli_gate.index = pd.to_datetime(cli_gate.index)

    eng = SectorSignalEngine(prices=prices, cli_gate=cli_gate)
    latest_date = prices.index[-1]
    signals = eng.scan_signals(latest_date)
    p0_regime = _load_p0_regime()
    latest_date8 = pd.Timestamp(latest_date).strftime("%Y%m%d")
    lag_business_days = _business_lag_days(date8, latest_date8)
    freshness = "CURRENT"
    status = "PASS"
    if lag_business_days is None:
        freshness = "UNKNOWN"
        status = "WARN"
    elif lag_business_days == 1:
        freshness = "PREV_BDAY_OK"
        status = "PASS"
    elif lag_business_days >= 2:
        freshness = "STALE"
        status = "WARN"

    sig_map = {k: (v.action, float(v.strength)) for k, v in signals.items()}

    for i, row in df.iterrows():
        code = str(row["sector_code"]).strip()
        if code in sig_map:
            act, strength = sig_map[code]
            result = calc_sector_result(act, strength, p0_regime)
            df.at[i, "sector_action"] = act
            df.at[i, "sector_entry_allowed"] = bool(result.get("entry_allowed", False))
            df.at[i, "sector_entry_weight"] = float(result.get("entry_weight", 0.0))
            df.at[i, "sector_strength"] = strength
            df.at[i, "sector_score"] = float(result.get("sector_score", 0.0))
            df.at[i, "sector_reason"] = str(result.get("sector_reason", ""))
            df.at[i, "sector_policy_reason"] = str(result.get("sector_policy_reason", ""))

    df, leader_info = _apply_sector_leader_coupling(df)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")
    _log_print("WROTE", OUT, "sectors", sorted(sector_data.keys()), "asof", str(latest_date)[:10])
    _log_print(f"[SECTOR_STATUS] candidate_date={date8} source_latest={latest_date8} lag_bdays={lag_business_days} freshness={freshness}")
    _write_status(date8, {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "candidate_date8": date8,
        "sector_latest_date8": latest_date8,
        "lag_business_days": lag_business_days,
        "freshness": freshness,
        "status": status,
        "sector_codes": sorted(sector_data.keys()),
        "rows": int(len(df)),
        "leader_coupling": leader_info,
        "sector_override": sector_override_info,
    })

    _save_snapshot(df, date8)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
