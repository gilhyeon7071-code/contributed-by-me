"""D1 입력 + D2 모집단 실행.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.run_d1_d2 \
        --selection-date 20260930 --files <f1> <f2> <f3> --out-dir <경로>

- 출력 위치는 반드시 지정한다(기본값 없음)
- --format-test: 받은 날짜 검사를 끄고 산출물에 FORMAT_TEST 를 찍는다. 운영 폴더(data/)에는 쓸 수 없다
- 파일 쓰기 지점: out_dir 아래 csv/json 과 두 jsonl 로그뿐이다. 다른 곳에 쓰지 않는다
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from paper.strategies.kospi_mcap_quarterly_v2.src.krx_input import (
    build_input, check_receipt, classify_krx_file, load_kospi200, read_krx_csv, read_zone_identifier,
)
from paper.strategies.kospi_mcap_quarterly_v2.src.universe import build_universe

STRATEGY_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DATA_DIR = STRATEGY_ROOT / "data"
DEFAULT_THRESHOLDS = STRATEGY_ROOT / "config" / "thresholds_v1.json"
DEFAULT_INDEX_CSV = STRATEGY_ROOT.parents[2] / "2_Logs" / "index_daily_history.csv"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _last_ok(log_path: Path, key: str) -> Optional[Dict[str, int]]:
    """직전 정상 실행의 수. 날짜 없는 로그에서 읽는다(시험 실행은 제외)."""
    if not log_path.exists():
        return None
    last = None
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("status") == "OK" and not row.get("format_test"):
            last = row.get(key)
    return last


def run(selection_date: str, files: List[Path], out_dir: Path, *, index_csv: Path,
        thresholds_path: Path, format_test: bool) -> Dict[str, Any]:
    out_dir = out_dir.resolve()
    if format_test and (out_dir == PRODUCTION_DATA_DIR or PRODUCTION_DATA_DIR in out_dir.parents):
        raise SystemExit("--format-test 는 운영 폴더(data/)에 쓸 수 없어요")
    thresholds = json.loads(thresholds_path.read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)
    input_log = out_dir / "input_log.jsonl"
    universe_log = out_dir / "universe_log.jsonl"

    manifest: List[Dict[str, Any]] = []
    frames: Dict[str, Any] = {}
    reasons: List[str] = []
    for f in files:
        df = read_krx_csv(f)
        kind = classify_krx_file(df)
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        zone = read_zone_identifier(f)
        manifest.append({"path": str(f), "kind": kind, "rows": len(df), "bytes": f.stat().st_size,
                         "sha256": _sha256(f), "modified_at": mtime.isoformat(timespec="seconds"),
                         "referrer_url": zone.get("ReferrerUrl"), "host_url": zone.get("HostUrl")})
        if kind is None:
            reasons.append(f"INPUT_FILE_UNKNOWN:{f.name}")
            continue
        if kind in frames:
            reasons.append(f"INPUT_FILE_DUPLICATE_KIND:{kind}")
        frames[kind] = df
        if not format_test:
            r = check_receipt(mtime, selection_date, thresholds["receipt_min_hhmm"])
            if r:
                reasons.append(f"{r}:{f.name}")
    for kind in ("price", "basic", "flags"):
        if kind not in frames:
            reasons.append(f"INPUT_FILE_MISSING:{kind}")

    result: Dict[str, Any] = {"selection_date": selection_date, "format_test": format_test,
                              "run_at": datetime.now().isoformat(timespec="seconds"), "manifest": manifest}
    if reasons:
        result.update({"status": "STOP", "stage": "D1_files", "reasons": reasons})
        _write_json(out_dir / f"input_{selection_date}_check.json", result)
        _append_jsonl(input_log, {k: result[k] for k in ("selection_date", "status", "stage", "reasons", "format_test", "run_at")})
        return result

    input_df, d1 = build_input(frames["price"], frames["basic"], frames["flags"], thresholds=thresholds,
                               previous_rows=_last_ok(input_log, "rows"))
    series, idx = load_kospi200(index_csv, selection_date, index_code=thresholds["index_code"],
                                window=int(thresholds["index_window"]))
    d1_status = "OK" if d1["status"] == "OK" and idx["status"] == "OK" else "STOP"
    result.update({"status": d1_status, "stage": "D1", "d1": d1, "index": idx})
    _write_json(out_dir / f"input_{selection_date}_check.json", result)
    if not input_df.empty:
        input_df.to_csv(out_dir / f"input_{selection_date}.csv", index=False, encoding="utf-8-sig")
    series.to_csv(out_dir / f"kospi200_{selection_date}.csv", index=False, encoding="utf-8-sig")
    _append_jsonl(input_log, {"selection_date": selection_date, "status": d1_status, "stage": "D1",
                              "reasons": d1["reasons"] + idx["reasons"], "rows": d1.get("rows"),
                              "format_test": format_test, "run_at": result["run_at"]})
    if d1_status != "OK":
        return result

    universe, counts = build_universe(input_df, thresholds=thresholds,
                                      previous_counts=_last_ok(universe_log, "counts"))
    result.update({"status": counts["status"], "stage": "D2", "d2": counts})
    universe.to_csv(out_dir / f"universe_{selection_date}.csv", index=False, encoding="utf-8-sig")
    _write_json(out_dir / f"universe_{selection_date}_count.json", {**counts, "format_test": format_test})
    _write_json(out_dir / f"input_{selection_date}_check.json", result)
    _append_jsonl(universe_log, {"selection_date": selection_date, "status": counts["status"],
                                 "reasons": counts["reasons"], "counts": {k: v for k, v in counts.items() if isinstance(v, int)},
                                 "format_test": format_test, "run_at": result["run_at"]})
    return result


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selection-date", required=True)
    ap.add_argument("--files", nargs=3, required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--index-csv", type=Path, default=DEFAULT_INDEX_CSV)
    ap.add_argument("--thresholds", type=Path, default=DEFAULT_THRESHOLDS)
    ap.add_argument("--format-test", action="store_true")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    res = run(args.selection_date, args.files, args.out_dir, index_csv=args.index_csv,
              thresholds_path=args.thresholds, format_test=args.format_test)
    summary = {k: res.get(k) for k in ("selection_date", "status", "stage", "format_test")}
    summary["reasons"] = res.get("reasons") or (res.get("d2") or {}).get("reasons") or \
        ((res.get("d1") or {}).get("reasons", []) + (res.get("index") or {}).get("reasons", []))
    summary["counts"] = {k: v for k, v in (res.get("d2") or {}).items() if isinstance(v, int)}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if res.get("status") == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())
