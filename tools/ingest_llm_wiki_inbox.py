from __future__ import annotations

import argparse
import hashlib
import re
import shutil
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = ROOT / "docs" / "llm_wiki"
INBOX_ROOT = WIKI_ROOT / "00_Inbox"
SOURCES_ROOT = WIKI_ROOT / "02_Sources"
INDEX_PATH = SOURCES_ROOT / "source_index_latest.md"

SOURCE_DIRS = {
    "raw_slack": "slack",
    "raw_meetings": "meetings",
    "raw_docs": "docs",
}

META_KEYS = ("title", "date", "channel", "tags")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_name(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return stem.strip("._") or "note"


def _read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, "unsupported text encoding")


def _extract_metadata(text: str, fallback_title: str) -> dict[str, str]:
    meta = {key: "" for key in META_KEYS}
    for raw_line in text.splitlines()[:40]:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# ") and not meta["title"]:
            meta["title"] = line[2:].strip()
            continue
        match = re.match(r"^([A-Za-z_ -]+):\s*(.+)$", line)
        if not match:
            continue
        key = match.group(1).strip().lower().replace(" ", "_")
        value = match.group(2).strip()
        if key in meta:
            meta[key] = value
    if not meta["title"]:
        meta["title"] = fallback_title
    return meta


def _write_index(rows: list[dict[str, str]]) -> None:
    SOURCES_ROOT.mkdir(parents=True, exist_ok=True)
    lines = [
        "# LLM Wiki Source Index",
        "",
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "| source_type | date | channel | title | tags | source_path | ingested_path | sha256 | bytes |",
        "|---|---|---|---|---|---|---|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| `{source_type}` | `{date}` | `{channel}` | `{title}` | `{tags}` | `{source_path}` | `{ingested_path}` | `{sha256}` | `{bytes}` |".format(
                **row
            )
        )
    lines.append("")
    lines.append("This index is read-only evidence for LLM retrieval.")
    lines.append("")
    INDEX_PATH.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def ingest(*, apply: bool) -> tuple[int, list[dict[str, str]]]:
    rows: list[dict[str, str]] = []
    ymd = datetime.now().strftime("%Y-%m-%d")

    for raw_dir_name, source_type in SOURCE_DIRS.items():
        raw_dir = INBOX_ROOT / raw_dir_name
        raw_dir.mkdir(parents=True, exist_ok=True)
        for source_path in sorted(raw_dir.glob("*.md")):
            text = _read_text(source_path)
            meta = _extract_metadata(text, source_path.stem)
            digest = _sha256(source_path)
            dest_dir = SOURCES_ROOT / source_type / ymd
            dest_name = f"{source_path.stem}.{digest[:12]}.md"
            dest_path = dest_dir / _safe_name(dest_name)
            rows.append(
                {
                    "source_type": source_type,
                    "title": meta["title"],
                    "date": meta["date"],
                    "channel": meta["channel"],
                    "tags": meta["tags"],
                    "source_path": str(source_path),
                    "ingested_path": str(dest_path),
                    "sha256": digest,
                    "bytes": str(len(text.encode("utf-8"))),
                }
            )
            if apply:
                dest_dir.mkdir(parents=True, exist_ok=True)
                if not dest_path.exists():
                    shutil.copy2(source_path, dest_path)

    if apply:
        _write_index(rows)
    return len(rows), rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest LLM Wiki inbox markdown files.")
    parser.add_argument("--apply", action="store_true", help="copy inbox files into 02_Sources")
    args = parser.parse_args()
    count, rows = ingest(apply=args.apply)
    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"[LLM_WIKI_INBOX] mode={mode} markdown_files={count}")
    for row in rows:
        print(f"{row['source_type']} {row['source_path']} -> {row['ingested_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
