# -*- coding: utf-8 -*-
"""
Fix report_backtest_v41_1.py encoding issues.

Problem confirmed:
- First line contains non-UTF-8 bytes (e.g., CP949/EUC-KR text) BEFORE the encoding cookie.
- tokenize.detect_encoding fails because it tries to decode the first line as UTF-8.

What this script does:
- Creates a timestamped backup of the target file.
- Decodes the file as: UTF-8 -> CP949 -> EUC-KR (strict).
- Normalizes line endings to LF.
- Removes any existing "coding:" cookie lines in the first 5 lines.
- Ensures the encoding cookie is placed at line 1 (or line 2 if a shebang exists).
- If the first line contains non-ASCII characters and is not a comment/docstring/import/def/class,
  it will be commented out to avoid syntax errors.
- Writes back as UTF-8 (no BOM).
"""

import argparse
import datetime as dt
import re
import shutil
from pathlib import Path

CODING_RE = re.compile(r"coding[:=]\s*([-\w.]+)", re.IGNORECASE)


def _decode_bytes(raw: bytes) -> tuple[str, str]:
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", raw, 0, 1, "Cannot decode as utf-8/cp949/euc-kr")


def _is_safe_noncomment_line(s: str) -> bool:
    t = s.lstrip()
    return (
        t.startswith("#")
        or t.startswith('"""')
        or t.startswith("'''")
        or t.startswith("import ")
        or t.startswith("from ")
        or t.startswith("def ")
        or t.startswith("class ")
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="report_backtest_v41_1.py", help="Target python file path")
    args = ap.parse_args()

    p = Path(args.path)
    if not p.is_file():
        print(f"[FATAL] not found: {p}")
        return 2

    raw = p.read_bytes()
    text, used_enc = _decode_bytes(raw)

    # Normalize newlines to LF
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    # Remove coding cookies in first 5 lines (avoid duplicates)
    for i in range(min(5, len(lines))):
        if CODING_RE.search(lines[i] or ""):
            lines[i] = ""

    # If first line has non-ASCII and is unsafe, comment it out
    if lines:
        first = lines[0]
        has_non_ascii = any(ord(ch) > 127 for ch in first)
        if has_non_ascii and not _is_safe_noncomment_line(first):
            lines[0] = "# " + first

    # Insert encoding cookie at top (keep shebang if present)
    cookie = "# -*- coding: utf-8 -*-"
    if lines and lines[0].startswith("#!"):
        if len(lines) < 2:
            lines.append(cookie)
        else:
            lines.insert(1, cookie)
    else:
        lines.insert(0, cookie)

    fixed = "\n".join(lines).rstrip("\n") + "\n"

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = p.with_suffix(p.suffix + f".bak_{ts}")
    shutil.copy2(p, bak)

    p.write_text(fixed, encoding="utf-8", newline="\n")

    print(f"[OK] decoded_as={used_enc}")
    print(f"[OK] backup={bak}")
    print(f"[OK] rewritten_utf8={p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
