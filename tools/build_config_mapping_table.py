# -*- coding: utf-8 -*-
"""Build a mapping table from paper_engine_config.json keys to code usages.

Outputs a Markdown file at E:\\1_Data\\.agent\\config_mapping_paper_engine.md.
Searches both paper_engine.py and the paper_engine/ package (excluding __pycache__).
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(r"E:\1_Data")
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
CODE_PATHS = [ROOT / "paper_engine.py"] + sorted(
    p for p in (ROOT / "paper_engine").glob("*.py") if p.is_file()
)
OUT_PATH = ROOT / ".agent" / "config_mapping_paper_engine.md"

# Last-segment matches require an explicit config access with that exact key
# (e.g. cfg.get("enabled", ...) or cfg["enabled"]). Broader substring matches
# produce too many false positives for common keys like "enabled".
CONFIG_VARS = ("cfg", "pol", "config", "params")


def access_patterns(last_segment: str, bare: bool = False) -> List[str]:
    patterns: List[str] = []
    for var in CONFIG_VARS:
        patterns.extend(
            [
                f'{var}.get("{last_segment}"',
                f"{var}.get('{last_segment}'",
                f'{var}["{last_segment}"]',
                f"{var}['{last_segment}']",
            ]
        )
    if bare:
        patterns.extend(
            [
                f'.get("{last_segment}"',
                f".get('{last_segment}'",
                f'["{last_segment}"]',
                f"['{last_segment}']",
            ]
        )
    return patterns


def is_nested_key_match(line: str, key: str) -> bool:
    """Catch multi-level config access like cfg.get("parent").get("child").

    Requires the immediate parent segment and a qualified access to the last
    segment to appear on the same line. Bare ``.get("child")`` is allowed here
    because the parent segment acts as a guard against unrelated accesses.
    """
    if "." not in key:
        return False
    parts = key.split(".")
    parent = parts[-2]
    last = parts[-1]
    return parent in line and any(p in line for p in access_patterns(last, bare=True))


def flatten_config(d: Dict[str, Any], prefix: str = "") -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.extend(flatten_config(v, key))
        else:
            out.append((key, v))
    return out


def summarize_value(v: Any) -> str:
    if isinstance(v, list):
        return f"list[{len(v)}]"
    if isinstance(v, bool):
        return f"bool({v})"
    if isinstance(v, (int, float)):
        return f"number({v})"
    if isinstance(v, str):
        snippet = v[:40].replace("|", "\\|")
        return f'str("{snippet}{"..." if len(v) > 40 else ""}")'
    return type(v).__name__


def relative_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def find_lines(paths: List[Path], key: str, last_segment: str) -> List[Tuple[str, int, str]]:
    results: List[Tuple[str, int, str]] = []
    seen: set[Tuple[str, int]] = set()

    for path in paths:
        if not path.exists():
            continue
        rel = relative_path(path)
        with path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if (rel, i) in seen:
                    continue
                full_match = key in line
                top_level_last_match = ("." not in key) and any(
                    p in line for p in access_patterns(last_segment)
                )
                nested_match = is_nested_key_match(line, key)
                if full_match or top_level_last_match or nested_match:
                    results.append((rel, i, line.rstrip()[:120]))
                    seen.add((rel, i))

    return results


def main() -> int:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    flat = flatten_config(cfg)

    rows: List[Dict[str, Any]] = []
    unused: List[str] = []

    for key, value in flat:
        last_segment = key.split(".")[-1]
        usages = find_lines(CODE_PATHS, key, last_segment)
        if not usages:
            unused.append(key)
        rows.append(
            {
                "key": key,
                "value_summary": summarize_value(value),
                "usage_count": len(usages),
                "locations": [f"{u[0]}:{u[1]}" for u in usages[:10]],
                "snippets": [u[2] for u in usages[:3]],
            }
        )

    lines: List[str] = [
        "# paper_engine_config.json → paper_engine code mapping",
        "",
        f"Generated: {datetime.now().isoformat()}",
        f"Config path: `{CONFIG_PATH}`",
        f"Code paths: `{'`, `'.join(relative_path(p) for p in CODE_PATHS)}`",
        f"Total config keys: {len(rows)}",
        f"Unused (no full-key or qualified last-segment match): {len(unused)}",
        "",
        "| config_key | value_summary | usage_count | locations |",
        "|---|---|---:|---|",
    ]

    for r in rows:
        loc_str = ", ".join(r["locations"]) if r["locations"] else "-"
        if len(r["locations"]) == 10:
            loc_str += " ..."
        key_escaped = r["key"].replace("|", "\\|")
        val_escaped = str(r["value_summary"]).replace("|", "\\|")
        lines.append(f"| `{key_escaped}` | {val_escaped} | {r['usage_count']} | {loc_str} |")

    if unused:
        lines.extend(["", "## Unused config keys", ""])
        for k in unused:
            lines.append(f"- `{k}`")

    # Keep a few code snippets visible for inspection.
    snippets_shown = 0
    for r in rows:
        if r["snippets"]:
            lines.extend(["", f"### `{r['key']}`", ""])
            for loc, snippet in zip(r["locations"][:3], r["snippets"]):
                code = snippet.replace("|", "\\|").replace("`", "'")
                lines.append(f"- `{loc}`: `{code}`")
            snippets_shown += 1
            if snippets_shown >= 30:
                break

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] wrote {OUT_PATH} (keys={len(rows)}, unused={len(unused)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
