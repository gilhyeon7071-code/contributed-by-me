# pf_calc_grep.py
# Scan python source under ROOT for PF/OOS/sensitivity related keywords and write matches to:
#   ROOT\_diag\pf_calc_grep_py.txt
#
# Usage:
#   python tools\pf_calc_grep.py --root project root
#
import argparse
import os
import re
from pathlib import Path
import logging

DEFAULT_KEYS = [
    "oos_pf",
    "OOS_DROP",
    "delta_oos_pf_pct",
    "profit factor",
    "profit_factor",
    "pf",
    "sensitivity_report",
    "sensitivity",
    "report_backtest",
    "split_policy",
]



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
def iter_py_files(root: Path):
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".py"):
                yield Path(dirpath) / fn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]), help="Project root (default: E:\\1_Data)")
    ap.add_argument("--out", default=None, help="Output file path (default: <root>\\_diag\\pf_calc_grep_py.txt)")
    ap.add_argument("--keys", nargs="*", default=DEFAULT_KEYS, help="Keywords to match (case-insensitive)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_path = Path(args.out).resolve() if args.out else (root / "_diag" / "pf_calc_grep_py.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    keys = [k for k in args.keys if k and str(k).strip()]
    rx = re.compile("|".join(re.escape(k) for k in keys), re.IGNORECASE)

    lines_out = []
    file_count = 0
    hit_count = 0

    for p in iter_py_files(root):
        file_count += 1
        try:
            txt = p.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as e:
            lines_out.append(f"{p}: [READ_ERROR] {e}")
            continue

        for i, line in enumerate(txt, 1):
            if rx.search(line):
                hit_count += 1
                lines_out.append(f"{p}:{i}: {line.strip()}")

    out_path.write_text("\n".join(lines_out), encoding="utf-8")
    _log_print(f"WROTE={out_path}")
    _log_print(f"FILES_SCANNED={file_count}")
    _log_print(f"HITS={hit_count}")

if __name__ == "__main__":
    main()


