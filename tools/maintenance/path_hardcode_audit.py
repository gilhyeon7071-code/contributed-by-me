from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
TOOLS = ROOT / "tools"
LOGS = ROOT / "2_Logs"
LOGS.mkdir(parents=True, exist_ok=True)

EXTS = {".py", ".ps1", ".cmd", ".bat", ".json", ".yaml", ".yml"}
ABS_PATTERNS = [
    re.compile(r"[A-Za-z]:\\"),
    re.compile(r"E:\\1_Data", re.IGNORECASE),
    re.compile(r"E:\\vibe", re.IGNORECASE),
    re.compile(r"C:\\Users\\jjtop", re.IGNORECASE),
]


def scan_file(path: Path) -> dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        try:
            text = path.read_text(encoding="cp949")
        except Exception:
            return {"file": str(path), "read_error": True, "hits": []}

    hits: list[dict[str, object]] = []
    for i, line in enumerate(text.splitlines(), start=1):
        line_hits = []
        for pat in ABS_PATTERNS:
            if pat.search(line):
                line_hits.append(pat.pattern)
        if line_hits:
            hits.append({"line": i, "text": line[:300], "patterns": line_hits})

    return {"file": str(path), "read_error": False, "hits": hits}


def main() -> int:
    files = [p for p in TOOLS.rglob("*") if p.is_file() and p.suffix.lower() in EXTS]
    reports = [scan_file(p) for p in files]

    files_with_hits = [r for r in reports if r.get("hits")]
    total_hits = sum(len(r["hits"]) for r in files_with_hits)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = {
        "run_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tools_root": str(TOOLS),
        "files_scanned": len(files),
        "files_with_hits": len(files_with_hits),
        "total_hit_lines": total_hits,
        "files": files_with_hits,
    }

    out_path = LOGS / f"tools_path_hardcode_audit_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (LOGS / "tools_path_hardcode_audit_latest.json").write_text(
        json.dumps({"last": str(out_path), "ts": ts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[PATH_AUDIT] scanned={len(files)} files_with_hits={len(files_with_hits)} hit_lines={total_hits}")
    print(f"[PATH_AUDIT] out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
