# -*- coding: utf-8 -*-
"""
diag_oos0.py
Read-only diagnostic helper for OOS=0 issues.

What it does:
1) Finds latest result artifacts (json/csv/xlsx/parquet) related to sensitivity/walkforward/OOS
2) Summarizes split/segment counts if tabular
3) Prints OOS-related fields if json-like
4) Locates source code lines that mention OOS / split / walkforward / sensitivity

Outputs:
- Writes report under ./_diag/oos0_diag_YYYYMMDD_HHMMSS.txt
- Prints the report path to stdout (single line if success)
"""
from __future__ import annotations

import os
import sys
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Tuple

KEYWORDS_FILES = (
    "sensitivity", "walkforward", "walk_forward", "walk-forward",
    "oos", "outofsample", "out_of_sample", "is_val_oos", "isval", "isvaloos",
    "split", "fold", "cv", "validation", "val", "live"
)

KEYWORDS_CODE = (
    "OOS", "out_of_sample", "out-of-sample", "outofsample",
    "walkforward", "walk_forward", "walk-forward",
    "sensitivity", "split", "isval", "is_val", "val", "live"
)

TABULAR_EXTS = (".csv", ".tsv", ".xlsx", ".xls", ".parquet")
JSON_EXTS = (".json", ".jsonl")

SEARCH_DIR_HINTS = (
    "runs", "run", "reports", "report", "stats", "stat", "out", "output", "outputs",
    "data", "results", "artifacts", "_artifacts", "_reports"
)

def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_diag_dir(root: Path) -> Path:
    d = root / "_diag"
    d.mkdir(parents=True, exist_ok=True)
    return d

def is_keyword_hit(name: str, keywords: Iterable[str]) -> bool:
    n = name.lower()
    return any(k in n for k in keywords)

def pick_candidate_dirs(root: Path) -> List[Path]:
    cands: List[Path] = [root]
    for h in SEARCH_DIR_HINTS:
        p = root / h
        if p.exists() and p.is_dir():
            cands.append(p)
    return cands

def walk_files(base: Path) -> List[Path]:
    paths: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(base):
        # prune common heavy dirs
        prune = {".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".ruff_cache", "node_modules"}
        dirnames[:] = [d for d in dirnames if d.lower() not in prune]
        for fn in filenames:
            paths.append(Path(dirpath) / fn)
    return paths

def collect_artifacts(root: Path) -> List[Path]:
    candidates: List[Path] = []
    for base in pick_candidate_dirs(root):
        try:
            for p in walk_files(base):
                if p.is_file() and is_keyword_hit(p.name, KEYWORDS_FILES):
                    candidates.append(p.resolve())
        except Exception:
            continue
    # de-dup, preserve order
    uniq = list(dict.fromkeys(candidates).keys())
    return uniq

def sort_by_mtime(paths: List[Path]) -> List[Path]:
    return sorted(paths, key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)

def human_mtime(p: Path) -> str:
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "unknown"

def safe_str(x: Any, limit: int = 180) -> str:
    s = repr(x)
    return s if len(s) <= limit else (s[:limit] + "…")

def try_read_json(path: Path) -> Any:
    if path.suffix.lower() == ".jsonl":
        items = []
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception:
                    continue
                if i >= 199:
                    break
        return items
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return json.load(f)

def flatten_paths(obj: Any, prefix: str = "") -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            out.extend(flatten_paths(v, p))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:500]):  # cap
            p = f"{prefix}[{i}]"
            out.extend(flatten_paths(v, p))
    else:
        out.append((prefix, obj))
    return out

def extract_oos_related(flat: List[Tuple[str, Any]]) -> List[Tuple[str, Any]]:
    hits = []
    for p, v in flat:
        pl = p.lower()
        if ("oos" in pl) or ("out" in pl and "sample" in pl):
            hits.append((p, v))
    return hits[:2000]

def tabular_summary(path: Path) -> str:
    import pandas as pd  # local import

    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path, engine="python", encoding_errors="replace")
    elif ext == ".tsv":
        df = pd.read_csv(path, sep="\t", engine="python", encoding_errors="replace")
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
    elif ext == ".parquet":
        df = pd.read_parquet(path)
    else:
        return f"[SKIP] Not a supported tabular ext: {ext}"

    lines: List[str] = []
    lines.append(f"[TABULAR] rows={len(df):,} cols={len(df.columns):,}")
    cols = list(df.columns)
    lines.append("[COLUMNS] " + ", ".join([str(c) for c in cols[:80]]) + (" ..." if len(cols) > 80 else ""))

    # Likely split column
    split_cols = [c for c in cols if str(c).lower() in ("split", "set", "segment", "phase", "fold", "cv_split", "dataset")]
    if not split_cols:
        split_cols = [c for c in cols if any(k in str(c).lower() for k in ("split", "segment", "phase", "fold", "set", "dataset", "oos", "is_oos"))]

    if split_cols:
        c0 = split_cols[0]
        try:
            vc = df[c0].astype(str).value_counts(dropna=False).head(20)
            lines.append(f"[SPLIT_COUNTS] by '{c0}':")
            for k, v in vc.items():
                lines.append(f"  - {k}: {int(v):,}")
        except Exception as e:
            lines.append(f"[SPLIT_COUNTS] error: {e}")

    # Detect OOS rows
    oos_rows = None
    for c in cols:
        cl = str(c).lower()
        if cl in ("split", "set", "segment", "phase", "dataset"):
            try:
                ser = df[c].astype(str).str.upper()
                cnt = int((ser.str.contains("OOS") | ser.str.contains("OUT")).sum())
                if cnt > 0:
                    oos_rows = (c, cnt)
                    break
            except Exception:
                continue
        if cl in ("is_oos", "oos_flag"):
            try:
                cnt = int((df[c].fillna(0).astype(int) == 1).sum())
                if cnt > 0:
                    oos_rows = (c, cnt)
                    break
            except Exception:
                continue

    if oos_rows:
        lines.append(f"[OOS_ROWS_DETECTED] column='{oos_rows[0]}' oos_rows={oos_rows[1]:,}")
    else:
        lines.append("[OOS_ROWS_DETECTED] none (no obvious OOS markers found)")

    # metric/value style
    metric_cols = [c for c in cols if str(c).lower() in ("metric", "name", "key")]
    value_cols = [c for c in cols if str(c).lower() in ("value", "val", "score", "result")]
    if metric_cols and value_cols:
        mc, vc = metric_cols[0], value_cols[0]
        try:
            sub = df[[mc, vc]].copy()
            sub[mc] = sub[mc].astype(str)
            hits = sub[sub[mc].str.contains("OOS|OUT", case=False, na=False)].head(50)
            if len(hits) > 0:
                lines.append(f"[METRICS_OOS] sample (first 50) using '{mc}'/'{vc}':")
                for _, r in hits.iterrows():
                    lines.append(f"  - {r[mc]} = {safe_str(r[vc])}")
        except Exception:
            pass

    try:
        lines.append("[HEAD]")
        lines.append(df.head(5).to_string(index=False))
    except Exception:
        pass
    return "\n".join(lines)

def code_grep(root: Path) -> List[str]:
    lines: List[str] = []
    exts = {".py", ".cmd", ".bat", ".ps1", ".json"}
    for base in pick_candidate_dirs(root):
        try:
            for p in walk_files(base):
                if not p.is_file():
                    continue
                if p.suffix.lower() not in exts:
                    continue
                try:
                    if p.stat().st_size > 5_000_000:
                        continue
                except Exception:
                    continue
                try:
                    txt = p.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue
                low = txt.lower()
                hit_kw = None
                for kw in KEYWORDS_CODE:
                    if kw.lower() in low:
                        hit_kw = kw
                        break
                if not hit_kw:
                    continue
                for i, line in enumerate(txt.splitlines(), start=1):
                    if hit_kw.lower() in line.lower():
                        lines.append(f"{p.relative_to(root)}:{i}: {line.strip()}")
                        break
                if len(lines) >= 2500:
                    return lines
        except Exception:
            continue
    return lines

def main() -> int:
    root = Path(os.getcwd()).resolve()
    diag_dir = ensure_diag_dir(root)
    tag = now_tag()
    report_path = diag_dir / f"oos0_diag_{tag}.txt"

    out: List[str] = []
    out.append("=== OOS=0 DIAGNOSTIC REPORT ===")
    out.append(f"root: {root}")
    out.append(f"generated_at: {datetime.now().isoformat(sep=' ', timespec='seconds')}")
    out.append("")

    artifacts = sort_by_mtime(collect_artifacts(root))
    out.append(f"[ARTIFACTS_FOUND] count={len(artifacts)}")
    for p in artifacts[:40]:
        out.append(f"- {p.relative_to(root)} | mtime={human_mtime(p)} | size={p.stat().st_size:,} bytes")
    if len(artifacts) > 40:
        out.append(f"... (showing 40 of {len(artifacts)})")
    out.append("")

    latest = artifacts[0] if artifacts else None
    if latest is None:
        out.append("[LATEST_ARTIFACT] none")
    else:
        out.append(f"[LATEST_ARTIFACT] {latest.relative_to(root)}")
        out.append(f"mtime={human_mtime(latest)} size={latest.stat().st_size:,} bytes")
        out.append("")

        ext = latest.suffix.lower()
        try:
            if ext in JSON_EXTS:
                obj = try_read_json(latest)
                out.append("[LATEST_PARSE] JSON OK")
                flat = flatten_paths(obj)
                out.append(f"[JSON_FLAT] items={len(flat)} (capped traversal)")
                oos_hits = extract_oos_related(flat)
                out.append(f"[JSON_OOS_HITS] count={len(oos_hits)} (show up to 60)")
                for pth, val in oos_hits[:60]:
                    out.append(f"- {pth} = {safe_str(val)}")
            elif ext in TABULAR_EXTS:
                out.append("[LATEST_PARSE] TABULAR")
                out.append(tabular_summary(latest))
            else:
                out.append(f"[LATEST_PARSE] unsupported ext '{ext}' (no parsing)")
        except Exception as e:
            out.append("[LATEST_PARSE] ERROR")
            out.append(str(e))
            out.append(traceback.format_exc())

    out.append("")
    out.append("=== CODE POINTERS (first 200) ===")
    try:
        grep_lines = code_grep(root)
        for l in grep_lines[:200]:
            out.append(l)
        if len(grep_lines) > 200:
            out.append(f"... (showing 200 of {len(grep_lines)})")
    except Exception as e:
        out.append(f"[CODE POINTERS] error: {e}")

    report_path.write_text("\n".join(out), encoding="utf-8")
    print(str(report_path))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
