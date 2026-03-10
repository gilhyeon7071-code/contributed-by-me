# -*- coding: utf-8 -*-
"""
plan_10_5_grep.py
- Purpose: Find where candidate count / trade slots / capital are controlled in E:\1_Data project.
- Output: _diag\plan_10_5_grep.txt (file:line:text)
"""
import os, re
from collections import OrderedDict

ROOTS = [r"E:\1_Data"]
OUT_DIR = os.path.join(r"E:\1_Data", "_diag")
OUT_PATH = os.path.join(OUT_DIR, "plan_10_5_grep.txt")

PATTERNS = [
    r"\btop[_ ]?n\b",
    r"\bmax[_ ]?cands?\b",
    r"\bmax[_ ]?candidates?\b",
    r"\b(cands?|candidates?)\b",
    r"\bkept\b",
    r"\braw\b",
    r"\bhead\s*\(",
    r"\bnlargest\s*\(",
    r"\blimit\b",
    r"\bmax[_ ]?new\b",
    r"\bnew[_ ]?entries?\b",
    r"\bmax[_ ]?positions?\b",
    r"\bslots?\b",
    r"\bcapital\b",
    r"\b(initial[_ ]?cash|start[_ ]?cash)\b",
    r"\bequity\b",
    r"\bnotional\b",
    r"\bposition[_ ]?size\b",
    r"\bshares?\b",
    r"\bentry[_ ]?qty\b",
    r"\bportfolio\b",
]

LITERAL_HINTS = [
    "paper_engine_config.json",
    "candidates_latest_data.csv",
    "candidates_latest_meta.json",
    "paper\\prices\\ohlcv_paper.parquet",
    "paper\\trades.csv",
    "kill_switch",
    "crash_risk_off",
]

RX = re.compile("|".join(PATTERNS + [re.escape(x) for x in LITERAL_HINTS]), re.IGNORECASE)

INCLUDE_EXT = {".py", ".cmd", ".bat", ".json"}

def iter_files():
    for root in ROOTS:
        for dirpath, dirnames, filenames in os.walk(root):
            low = dirpath.lower()
            if "\\.git" in low or "\\__pycache__" in low or "\\_cache" in low:
                continue
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext not in INCLUDE_EXT:
                    continue
                yield os.path.join(dirpath, fn)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    hits = OrderedDict()
    for fp in iter_files():
        try:
            with open(fp, "r", encoding="utf-8", errors="replace") as f:
                for idx, line in enumerate(f, start=1):
                    if RX.search(line):
                        hits.setdefault(fp, []).append((idx, line.rstrip("\n")))
        except Exception as e:
            hits.setdefault(fp, []).append((0, f"[READ_ERROR] {e!r}"))

    out_lines = []
    out_lines.append(f"ROOTS={ROOTS}")
    out_lines.append(f"FILES_WITH_HITS={len(hits)}")
    out_lines.append("")
    for fp, items in hits.items():
        out_lines.append(f"=== {fp} ({len(items)} hits) ===")
        for ln, txt in items[:200]:
            out_lines.append(f"{fp}:{ln}: {txt}")
        out_lines.append("")
    with open(OUT_PATH, "w", encoding="utf-8") as w:
        w.write("\n".join(out_lines))
    print(f"[OK] wrote: {OUT_PATH}")
    print(f"[OK] files_with_hits={len(hits)}")

if __name__ == "__main__":
    raise SystemExit(main())
