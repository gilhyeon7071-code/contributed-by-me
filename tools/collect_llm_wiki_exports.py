from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = ROOT / "docs" / "llm_wiki"
IMPORT_ROOT = WIKI_ROOT / "00_Inbox" / "import_exports"
RAW_SLACK = WIKI_ROOT / "00_Inbox" / "raw_slack"
RAW_MEETINGS = WIKI_ROOT / "00_Inbox" / "raw_meetings"
RAW_DOCS = WIKI_ROOT / "00_Inbox" / "raw_docs"


def _read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, "unsupported text encoding")


def _safe_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return name.strip("._") or "note"


def _date_from_timestamp(value: Any) -> str:
    if value is None:
        return datetime.now().strftime("%Y-%m-%d")
    text = str(value)
    try:
        ts = float(text)
    except ValueError:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
            return text
        return datetime.now().strftime("%Y-%m-%d")
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def _extract_slack_messages(payload: Any) -> list[dict[str, str]]:
    if isinstance(payload, dict):
        messages = payload.get("messages", [])
        default_channel = str(payload.get("channel", "slack"))
    else:
        messages = payload
        default_channel = "slack"
    if not isinstance(messages, list):
        return []
    rows: list[dict[str, str]] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        rows.append(
            {
                "date": _date_from_timestamp(item.get("ts") or item.get("date")),
                "channel": str(item.get("channel") or default_channel),
                "user": str(item.get("user") or item.get("username") or "unknown"),
                "text": text,
            }
        )
    return rows


def _write_markdown(path: Path, text: str, apply: bool) -> None:
    if not apply:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _collect_slack(path: Path, apply: bool) -> list[Path]:
    payload = json.loads(_read_text(path))
    messages = _extract_slack_messages(payload)
    if not messages:
        return []
    first = messages[0]
    date = first["date"]
    channel = first["channel"]
    title = f"Slack Export {channel} {date}"
    out_name = f"{_safe_name(path.stem)}.{date}.md"
    out_path = RAW_SLACK / out_name
    lines = [
        f"# {title}",
        "",
        f"date: {date}",
        f"channel: {channel}",
        "tags: #slack #export",
        "",
        "## Content",
        "",
    ]
    for msg in messages:
        lines.append(f"- [{msg['date']}] {msg['user']}: {msg['text']}")
    lines.append("")
    _write_markdown(out_path, "\n".join(lines), apply)
    return [out_path]


def _collect_text(path: Path, target_dir: Path, source_tag: str, apply: bool) -> list[Path]:
    text = _read_text(path).strip()
    if not text:
        return []
    today = datetime.now().strftime("%Y-%m-%d")
    title = path.stem.replace("_", " ").strip().title()
    out_path = target_dir / f"{_safe_name(path.stem)}.{today}.md"
    lines = [
        f"# {title}",
        "",
        f"date: {today}",
        f"channel: {source_tag}",
        f"tags: #{source_tag} #export",
        "",
        "## Content",
        "",
        text,
        "",
    ]
    _write_markdown(out_path, "\n".join(lines), apply)
    return [out_path]


def collect(apply: bool) -> list[Path]:
    outputs: list[Path] = []
    slack_dir = IMPORT_ROOT / "slack_json"
    meetings_dir = IMPORT_ROOT / "meeting_text"
    docs_dir = IMPORT_ROOT / "doc_text"
    for folder in (slack_dir, meetings_dir, docs_dir):
        folder.mkdir(parents=True, exist_ok=True)
    for path in sorted(slack_dir.glob("*.json")):
        outputs.extend(_collect_slack(path, apply))
    for path in sorted(meetings_dir.glob("*.txt")) + sorted(meetings_dir.glob("*.md")):
        outputs.extend(_collect_text(path, RAW_MEETINGS, "meeting", apply))
    for path in sorted(docs_dir.glob("*.txt")) + sorted(docs_dir.glob("*.md")):
        outputs.extend(_collect_text(path, RAW_DOCS, "document", apply))
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect local exports into LLM Wiki raw inbox Markdown.")
    parser.add_argument("--apply", action="store_true", help="write Markdown notes into raw inbox folders")
    args = parser.parse_args()
    outputs = collect(apply=args.apply)
    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"[LLM_WIKI_COLLECT] mode={mode} outputs={len(outputs)}")
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

