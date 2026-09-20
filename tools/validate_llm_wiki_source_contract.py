from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = ROOT / "docs" / "llm_wiki"
INBOX_ROOT = WIKI_ROOT / "00_Inbox"
SOURCE_ROOTS = {
    "raw_slack": "slack",
    "raw_meetings": "meetings",
    "raw_docs": "docs",
}
META_KEYS = ("title", "date", "channel", "tags")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
        if key in meta:
            meta[key] = match.group(2).strip()
    if not meta["title"]:
        meta["title"] = fallback_title
    return meta


def _source_type(path: Path) -> str:
    try:
        rel = path.resolve().relative_to(INBOX_ROOT.resolve())
    except ValueError:
        return "unknown"
    if not rel.parts:
        return "unknown"
    if rel.parts[0] == "examples":
        return "example"
    return SOURCE_ROOTS.get(rel.parts[0], "unknown")


def validate_path(path: Path) -> dict[str, object]:
    result: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "status": "FAIL",
        "errors": [],
        "warnings": [],
        "source_type": _source_type(path),
        "metadata": {},
    }
    errors: list[str] = result["errors"]  # type: ignore[assignment]
    warnings: list[str] = result["warnings"]  # type: ignore[assignment]

    if not path.exists():
        errors.append("FILE_MISSING")
        return result
    if path.suffix.lower() != ".md":
        errors.append("NOT_MARKDOWN")
        return result

    text = _read_text(path)
    meta = _extract_metadata(text, path.stem)
    result["metadata"] = meta

    if result["source_type"] == "unknown":
        warnings.append("SOURCE_FOLDER_NOT_RECOGNIZED")
    if not meta["title"]:
        errors.append("TITLE_MISSING")
    if not meta["date"]:
        warnings.append("DATE_MISSING")
    elif not DATE_RE.match(meta["date"]):
        warnings.append("DATE_NOT_YYYY_MM_DD")
    if not meta["channel"]:
        warnings.append("CHANNEL_MISSING")
    if "Gate" in text or "STOP" in text or "LOCK" in text:
        warnings.append("POLICY_TERMS_PRESENT_REVIEW_REQUIRED")

    result["status"] = "PASS" if not errors else "FAIL"
    return result


def _discover_default_paths() -> list[Path]:
    paths: list[Path] = []
    for dirname in SOURCE_ROOTS:
        root = INBOX_ROOT / dirname
        if root.exists():
            paths.extend(sorted(root.glob("*.md")))
    return paths


def _expand_input_paths(raw_paths: list[str]) -> list[Path]:
    paths: list[Path] = []
    for raw_path in raw_paths:
        path = Path(raw_path)
        if path.is_dir():
            paths.extend(sorted(path.glob("*.md")))
        else:
            paths.append(path)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate LLM Wiki source Markdown contract.")
    parser.add_argument("paths", nargs="*", help="Markdown files to validate")
    parser.add_argument("--json", action="store_true", help="print JSON result")
    args = parser.parse_args()

    paths = _expand_input_paths(args.paths) if args.paths else _discover_default_paths()
    results = [validate_path(path) for path in paths]
    status = "PASS" if all(r["status"] == "PASS" for r in results) else "FAIL"
    payload = {
        "status": status,
        "files": len(results),
        "results": results,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"[LLM_WIKI_CONTRACT] status={status} files={len(results)}")
        for row in results:
            print(
                "{status} {path} source_type={source_type} warnings={warnings} errors={errors}".format(
                    **row
                )
            )
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
