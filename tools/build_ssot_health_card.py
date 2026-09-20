from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import stat
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

try:
    import pandas as pd
except Exception:
    pd = None

KST = dt.timezone(dt.timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from holiday_manager import HolidayManager

LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "config" / "ssot_health_card.json"

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"


def _now_kst() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(KST)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _norm_ymd(v: Any) -> str:
    d = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(d) < 8:
        return ""
    d = d[:8]
    try:
        dt.datetime.strptime(d, "%Y%m%d")
        return d
    except Exception:
        return ""


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _set_readonly(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)


def _clear_readonly(path: Path) -> None:
    if not path.exists():
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IWUSR)


def _latest_path(path_or_glob: str) -> Optional[Path]:
    raw = Path(path_or_glob)
    if "*" not in path_or_glob and "?" not in path_or_glob:
        return raw if raw.exists() else None
    parent = raw.parent
    if not parent.exists():
        return None
    files = list(parent.glob(raw.name))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def _extract_clean_end_ymd(path: Path) -> str:
    name = path.name
    marker = "krx_daily_"
    suffix = "_clean.parquet"
    if marker not in name or not name.endswith(suffix):
        return ""
    body = name.replace(marker, "", 1).replace(suffix, "")
    parts = body.split("_")
    if len(parts) != 2:
        return ""
    return _norm_ymd(parts[1])


def _latest_krx_clean_path(ds: Dict[str, Any], as_of_ymd: str) -> Optional[Path]:
    patterns: List[str] = []
    src = str(ds.get("path") or "").strip()
    if src:
        patterns.append(src)
    for item in ds.get("fallback_paths") or []:
        s = str(item or "").strip()
        if s:
            patterns.append(s)

    candidates: List[Tuple[str, float, Path]] = []
    for pattern in patterns:
        raw = Path(pattern)
        if "*" in pattern or "?" in pattern:
            parent = raw.parent
            files = list(parent.glob(raw.name)) if parent.exists() else []
        else:
            files = [raw] if raw.exists() else []
        for path in files:
            end_ymd = _extract_clean_end_ymd(path)
            if not end_ymd or (as_of_ymd and end_ymd > as_of_ymd):
                continue
            candidates.append((end_ymd, path.stat().st_mtime, path))
    if not candidates:
        return _latest_path(src) if src else None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def _load_frame(path: Path, kind: str) -> Tuple[Optional[Any], str]:
    if pd is None:
        return None, "pandas_unavailable"
    try:
        if kind == "csv":
            return pd.read_csv(path), ""
        if kind == "parquet":
            return pd.read_parquet(path), ""
        if kind == "xlsx":
            try:
                return pd.read_excel(path), ""
            except ImportError as e:
                return _load_xlsx_minimal(path, str(e))
        return None, f"unsupported_kind:{kind}"
    except Exception as e:
        return None, f"read_error:{e}"


def _xlsx_col_idx(cell_ref: str) -> int:
    letters = "".join(ch for ch in str(cell_ref or "") if ch.isalpha()).upper()
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return max(idx - 1, 0)


def _load_xlsx_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except Exception:
        return []
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    out: List[str] = []
    for si in root.findall("x:si", ns):
        parts = [t.text or "" for t in si.findall(".//x:t", ns)]
        out.append("".join(parts))
    return out


def _xlsx_cell_value(cell: Any, shared_strings: List[str]) -> Any:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    typ = str(cell.attrib.get("t") or "")
    if typ == "inlineStr":
        parts = [t.text or "" for t in cell.findall(".//x:t", ns)]
        return "".join(parts)
    val = cell.find("x:v", ns)
    if val is None or val.text is None:
        return ""
    raw = val.text
    if typ == "s":
        try:
            return shared_strings[int(raw)]
        except Exception:
            return raw
    return raw


def _load_xlsx_minimal(path: Path, original_error: str) -> Tuple[Optional[Any], str]:
    try:
        with zipfile.ZipFile(path) as zf:
            shared_strings = _load_xlsx_shared_strings(zf)
            sheet_name = "xl/worksheets/sheet1.xml"
            root = ET.fromstring(zf.read(sheet_name))
    except Exception as e:
        return None, f"read_error:{original_error}; xlsx_fallback_error:{e}"

    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: List[List[Any]] = []
    for row in root.findall(".//x:sheetData/x:row", ns):
        values: List[Any] = []
        for cell in row.findall("x:c", ns):
            idx = _xlsx_col_idx(str(cell.attrib.get("r") or ""))
            while len(values) <= idx:
                values.append("")
            values[idx] = _xlsx_cell_value(cell, shared_strings)
        rows.append(values)

    if not rows:
        return pd.DataFrame(), ""
    width = max(len(r) for r in rows)
    padded = [r + [""] * (width - len(r)) for r in rows]
    headers = [str(v).strip() for v in padded[0]]
    data = padded[1:]
    return pd.DataFrame(data, columns=headers), ""


def _status_worst(a: str, b: str) -> str:
    order = {PASS: 0, WARN: 1, FAIL: 2}
    return a if order.get(a, 2) >= order.get(b, 2) else b


def _as_of_ymd_from_ops() -> str:
    p0_files = sorted(LOG_DIR.glob("p0_daily_check_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if p0_files:
        p0 = _read_json(p0_files[0])
        y = _norm_ymd(((p0.get("prices") or {}).get("prev_weekday")) or p0.get("as_of_ymd"))
        if y:
            return y
    return _now_kst().strftime("%Y%m%d")


def _previous_trading_ymd(as_of_ymd: str) -> str:
    return HolidayManager().previous_trading_day(as_of_ymd)


def _calendar_prev_ymd(as_of_ymd: str) -> str:
    y = _norm_ymd(as_of_ymd)
    if not y:
        return ""
    return (dt.datetime.strptime(y, "%Y%m%d").date() - dt.timedelta(days=1)).strftime("%Y%m%d")


def _latest_p0_krx_effective() -> Dict[str, Any]:
    p0_files = sorted(LOG_DIR.glob("p0_daily_check_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not p0_files:
        return {}
    p0 = _read_json(p0_files[0])
    krx = p0.get("krx_clean") if isinstance(p0.get("krx_clean"), dict) else {}
    return {
        "effective_date_max": _norm_ymd(krx.get("effective_date_max")),
        "effective_reason": str(krx.get("effective_reason") or "").strip(),
        "path": str(p0_files[0]),
    }


def _recency_eval(path: Path, max_age_hours: float) -> Tuple[int, str]:
    age_sec = int((_now_kst() - dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).astimezone(KST)).total_seconds())
    if age_sec <= int(max_age_hours * 3600):
        return age_sec, PASS
    return age_sec, FAIL


def _schema_eval(
    df: Any,
    expected_cols: List[str],
    expected_dtypes: Dict[str, str],
    allow_added_columns: bool = False,
) -> Tuple[str, List[str]]:
    if df is None:
        return FAIL, ["dataframe_missing"]
    cols = [str(c) for c in list(df.columns)]
    colset = set(cols)
    diffs: List[str] = []
    status = PASS

    missing = [c for c in expected_cols if c not in colset]
    added = [c for c in cols if c not in set(expected_cols)] if expected_cols else []

    if missing:
        status = FAIL
        diffs.append("missing:" + ",".join(missing[:8]))
    if added and not allow_added_columns:
        status = _status_worst(status, WARN)
        diffs.append("added:" + ",".join(added[:8]))

    if expected_dtypes:
        for c, typ in expected_dtypes.items():
            if c not in colset:
                continue
            got = str(df[c].dtype)
            if got != str(typ):
                status = _status_worst(status, WARN)
                diffs.append(f"type:{c}({got}->{typ})")

    return status, diffs


def _max_date_from_cols(df: Any, date_cols: List[str]) -> str:
    if df is None or not date_cols:
        return ""
    mx = ""
    for c in date_cols:
        if c not in df.columns:
            continue
        s = df[c]
        try:
            for v in s.astype(str).tolist():
                y = _norm_ymd(v)
                if y and y > mx:
                    mx = y
        except Exception:
            continue
    return mx


def _ymd_from_filename(path: Path) -> str:
    for part in path.name.replace(".", "_").split("_"):
        ymd = _norm_ymd(part)
        if ymd:
            return ymd
    return ""


def _completeness_eval(df: Any, required_cols: List[str], min_pct: float) -> Tuple[float, str, Dict[str, float]]:
    if df is None:
        return 0.0, FAIL, {}
    n = int(len(df.index))
    if n <= 0:
        return 0.0, FAIL, {}

    miss_rates: Dict[str, float] = {}
    status = PASS
    valid_cols = 0
    for c in required_cols:
        if c not in df.columns:
            status = FAIL
            miss_rates[c] = 1.0
            continue
        valid_cols += 1
        miss = float(df[c].isna().mean())
        if str(df[c].dtype) == "object":
            try:
                blank = (df[c].astype(str).str.strip() == "").mean()
                miss = max(miss, float(blank))
            except Exception:
                pass
        miss_rates[c] = miss

    if valid_cols == 0 and required_cols:
        return 0.0, FAIL, miss_rates

    if required_cols:
        completeness = (1.0 - (sum(miss_rates.values()) / max(len(required_cols), 1))) * 100.0
    else:
        completeness = 100.0

    if completeness < min_pct:
        status = FAIL
    elif completeness < 100.0:
        status = _status_worst(status, WARN)

    return round(completeness, 2), status, miss_rates


def _fmt_age(sec: int) -> str:
    if sec < 0:
        sec = 0
    d = sec // 86400
    rem = sec % 86400
    h = rem // 3600
    m = (rem % 3600) // 60
    return f"{d}d {h:02d}:{m:02d}"


def _dataset_status(recency: str, schema: str, comp: str) -> str:
    s = PASS
    s = _status_worst(s, recency)
    s = _status_worst(s, schema)
    s = _status_worst(s, comp)
    return s


def _apply_row_filter(df: Any, ds: Dict[str, Any]) -> Any:
    if df is None or not isinstance(ds, dict):
        return df
    filt = ds.get("row_filter")
    if not isinstance(filt, dict):
        return df
    col = str(filt.get("column") or "").strip()
    if not col or col not in df.columns:
        return df
    mode = str(filt.get("mode") or "exclude").strip().lower()
    values = filt.get("values")
    if not isinstance(values, list) or not values:
        return df
    vals = {str(v).strip() for v in values}
    s = df[col].astype(str).str.strip()
    if mode == "include":
        return df[s.isin(vals)].copy()
    return df[~s.isin(vals)].copy()


def _new_orders_policy(overall: str) -> Tuple[str, str]:
    if overall == FAIL:
        return "NO", "dataset_fail"
    pending = _read_json(LOG_DIR / "pending_entry_status_latest.json")
    try:
        max_new = int(pending.get("max_new", 0) or 0)
    except Exception:
        max_new = 0
    try:
        entry_ready = int(pending.get("entry_ready", 0) or 0)
    except Exception:
        entry_ready = 0
    gate_action = str(pending.get("gate_action") or "").upper()
    if gate_action == "BLOCK" or (max_new <= 0 and entry_ready <= 0):
        return "NO", "pending_gate_block_or_no_room"
    return "YES", "ok"


def build_card(cfg: Dict[str, Any]) -> Dict[str, Any]:
    as_of = _as_of_ymd_from_ops()
    calendar_prev_ymd = _calendar_prev_ymd(as_of)
    expected_prev_trading_ymd = _previous_trading_ymd(as_of)
    p0_krx_effective = _latest_p0_krx_effective()
    datasets_cfg = cfg.get("datasets") if isinstance(cfg.get("datasets"), list) else []
    rows: List[Dict[str, Any]] = []
    overall = PASS

    for ds in datasets_cfg:
        if not isinstance(ds, dict):
            continue
        name = str(ds.get("name") or "unknown")
        kind = str(ds.get("kind") or "csv").lower()
        source = str(ds.get("path") or "")
        if name == "krx_clean":
            p = _latest_krx_clean_path(ds, as_of)
        else:
            p = _latest_path(source) if source else None

        if not p:
            row = {
                "name": name,
                "path": source,
                "status": FAIL,
                "recency_sec": None,
                "recency_status": FAIL,
                "schema_status": FAIL,
                "schema_diff": ["file_missing"],
                "completeness_pct": 0.0,
                "completeness_status": FAIL,
                "required_missing_rate": {},
                "max_date": None,
            }
            overall = _status_worst(overall, FAIL)
            rows.append(row)
            continue

        max_age_h = float(ds.get("max_age_hours", 24.0) or 24.0)
        rec_sec, rec_status = _recency_eval(p, max_age_h)

        df, read_err = _load_frame(p, kind)
        if read_err:
            schema_status = FAIL
            schema_diff = [read_err]
            comp_pct = 0.0
            comp_status = FAIL
            miss_rates = {}
            max_date = None
        else:
            df = _apply_row_filter(df, ds)
            expected_cols = [str(c) for c in (ds.get("expected_columns") or [])]
            expected_dtypes = {str(k): str(v) for k, v in (ds.get("expected_dtypes") or {}).items()}
            allow_added = bool(ds.get("allow_added_columns", False))
            schema_status, schema_diff = _schema_eval(df, expected_cols, expected_dtypes, allow_added_columns=allow_added)
            req_cols = [str(c) for c in (ds.get("required_columns") or [])]
            min_pct = float(ds.get("min_completeness_pct", 95.0) or 95.0)
            comp_pct, comp_status, miss_rates = _completeness_eval(df, req_cols, min_pct)
            date_cols = [str(c) for c in (ds.get("date_columns") or [])]
            max_date = _max_date_from_cols(df, date_cols) or None
            # [2026-08-31] 진입 정지 중에는 체결이 없어 orders 파일이 DIAG 행만 갖는다.
            #   row_filter 가 DIAG 를 걷어내면 0행이 되고 _completeness_eval 이 FAIL 을 준다
            #   -> new_orders=NO -> run_paper_daily PRECHECK 중단 -> full_auto rc=90.
            #   종전 예외는 파일명 날짜 == as_of 일 때만 걸려서, **월요일처럼 최신 파일이
            #   직전 거래일(금)인 날** 에는 안 걸렸다. 08-31 아침 배치가 그렇게 죽었다.
            #   "그 날 주문이 없었다" 는 데이터 불완전이 아니다. 직전 거래일까지 허용한다.
            #   파일이 진짜로 비었거나 깨진 경우는 schema_status != FAIL 조건이 계속 막는다.
            _ok_ymds = {_norm_ymd(as_of)}
            if expected_prev_trading_ymd:
                _ok_ymds.add(_norm_ymd(expected_prev_trading_ymd))
            if (
                name == "orders_exec"
                and df is not None
                and int(len(df.index)) == 0
                and schema_status != FAIL
                and _ymd_from_filename(p) in _ok_ymds
            ):
                comp_pct = 100.0
                comp_status = PASS
                max_date = _ymd_from_filename(p) or _norm_ymd(as_of)
                miss_rates = {c: 0.0 for c in req_cols}

        if (
            name == "krx_clean"
            and p0_krx_effective.get("effective_reason") == "skip_write_due_tolerated_zero_ohlc_deficit"
            and p0_krx_effective.get("effective_date_max")
        ):
            max_date = p0_krx_effective.get("effective_date_max")

        # If content date already matches ops as-of date, don't fail recency only by file mtime.
        if rec_status == FAIL and max_date and _norm_ymd(max_date) == _norm_ymd(as_of):
            rec_status = PASS
        # Date-aligned datasets can legitimately stay on the previous trading day during
        # holidays, weekends, or no-fill sessions; don't fail them by file mtime alone.
        if (
            rec_status == FAIL
            and name in {"krx_clean", "prices_paper", "live_fills", "orders_exec"}
            and max_date
            and expected_prev_trading_ymd
            and schema_status != FAIL
            and comp_status != FAIL
            and _norm_ymd(max_date) == expected_prev_trading_ymd
        ):
            rec_status = PASS
        # Price parquet may keep the previous KRX trading date during holidays/non-trading days.
        # If it matches the accepted KRX effective date, mtime-only recency must not block precheck.
        if (
            rec_status == FAIL
            and name == "prices_paper"
            and max_date
            and p0_krx_effective.get("effective_date_max")
            and _norm_ymd(max_date) == _norm_ymd(p0_krx_effective.get("effective_date_max"))
        ):
            rec_status = PASS

        row_status = _dataset_status(rec_status, schema_status, comp_status)
        overall = _status_worst(overall, row_status)

        rows.append(
            {
                "name": name,
                "path": str(p),
                "status": row_status,
                "recency_sec": rec_sec,
                "recency_status": rec_status,
                "schema_status": schema_status,
                "schema_diff": schema_diff,
                "completeness_pct": comp_pct,
                "completeness_status": comp_status,
                "required_missing_rate": miss_rates,
                "max_date": max_date,
                "as_of_ymd": as_of,
                "effective_source": (p0_krx_effective.get("path") if name == "krx_clean" and p0_krx_effective.get("effective_date_max") else None),
            }
        )

    fail_count = sum(1 for r in rows if r.get("status") == FAIL)
    warn_count = sum(1 for r in rows if r.get("status") == WARN)
    new_orders, new_orders_reason = _new_orders_policy(overall)

    return {
        "schema_version": "ssot_health_card_v1",
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "as_of_ymd": as_of,
        "calendar_prev_day": calendar_prev_ymd or None,
        "expected_prev_trading_day": expected_prev_trading_ymd or None,
        "source_calendar": "holiday_manager",
        "datasets": rows,
        "overall": {
            "status": overall,
            "fail_count": fail_count,
            "warn_count": warn_count,
            "dataset_count": len(rows),
            "new_orders": new_orders,
            "new_orders_reason": new_orders_reason,
        },
    }


def write_outputs(card: Dict[str, Any], out_dir: Path) -> Tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    json_latest = out_dir / "ssot_health_card_latest.json"
    txt_latest = out_dir / "ssot_health_card_latest.txt"
    json_dated = out_dir / f"ssot_health_card_{ts}.json"

    lines: List[str] = []
    for row in card.get("datasets", []):
        rec = _fmt_age(int(row.get("recency_sec") or 0)) if row.get("recency_sec") is not None else "-"
        schema = str(row.get("schema_status") or "-")
        if row.get("schema_diff"):
            head = str((row.get("schema_diff") or [""])[0])
            if head and head != "":
                schema = f"{schema}({head})"
        comp = row.get("completeness_pct")
        comp_txt = "-" if comp is None else f"{float(comp):.1f}%"
        lines.append(f"[{row.get('name')}] RECENCY {rec} | SCHEMA {schema} | COMPLETENESS {comp_txt}")

    lines.append(f"[overall] status={card.get('overall',{}).get('status')} fail={card.get('overall',{}).get('fail_count')} warn={card.get('overall',{}).get('warn_count')} new_orders={card.get('overall',{}).get('new_orders')}")

    _clear_readonly(json_latest)
    _clear_readonly(txt_latest)

    tmp_json = json_latest.with_suffix(".json.tmp")
    tmp_txt = txt_latest.with_suffix(".txt.tmp")

    with tmp_json.open("w", encoding="utf-8") as f:
        json.dump(card, f, ensure_ascii=False, indent=2)
    tmp_json.replace(json_latest)

    with tmp_txt.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    tmp_txt.replace(txt_latest)

    json_dated.write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")

    for p in (json_latest, txt_latest, json_dated):
        sha = _sha256_file(p)
        p.with_suffix(p.suffix + ".sha256").write_text(sha + "\n", encoding="utf-8")
        _set_readonly(p)

    return json_latest, txt_latest


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(CFG_PATH))
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    cfg = _read_json(Path(args.config))
    if not cfg:
        print(f"[FAIL] missing/invalid config: {args.config}")
        return 2

    card = build_card(cfg)
    json_path, txt_path = write_outputs(card, Path(args.out_dir))

    print(f"[OK] wrote: {json_path}")
    print(f"[OK] wrote: {txt_path}")
    print(f"[FINAL] ssot_health overall={card.get('overall',{}).get('status')} new_orders={card.get('overall',{}).get('new_orders')}")

    return 0 if card.get("overall", {}).get("status") != FAIL else 3


if __name__ == "__main__":
    raise SystemExit(main())

