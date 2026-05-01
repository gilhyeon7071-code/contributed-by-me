from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

DEFAULT_CONTEXT_FILES = [
    "AGENTS.md",
    "docs/03-operations/codex_llm_code_review_ci_checklist.md",
    "docs/03-operations/codex_llm_code_review_ci_design.md",
]

NON_OPERATIONAL_PREFIXES = (
    "docs/",
    "tools/ci/",
)


def run_git(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig")


def rel_path(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def collect_changed_files(diff_ref: str | None) -> list[str]:
    args = ["diff", "--name-only"]
    if diff_ref:
        args.append(diff_ref)
    code, stdout, stderr = run_git(args)
    if code != 0:
        raise SystemExit(f"git diff --name-only failed:\n{stderr}")
    return [line for line in stdout.splitlines() if line.strip()]


def collect_diff(diff_ref: str | None, max_chars: int) -> str:
    args = ["diff", "--no-ext-diff"]
    if diff_ref:
        args.append(diff_ref)
    code, stdout, stderr = run_git(args)
    if code != 0:
        raise SystemExit(f"git diff failed:\n{stderr}")
    if len(stdout) > max_chars:
        return stdout[:max_chars] + "\n\n[TRUNCATED: diff exceeded max chars]\n"
    return stdout


def is_roota_behavior_change(path: str) -> bool:
    if path == "AGENTS.md":
        return False
    if path.startswith(NON_OPERATIONAL_PREFIXES):
        return False
    return True


def context_files_for(changed_files: list[str]) -> list[str]:
    files = list(DEFAULT_CONTEXT_FILES)
    if any(is_roota_behavior_change(path) for path in changed_files):
        files.append(".agent/PLANS.md")
    if any(path.startswith("../vibe/buffett/") for path in changed_files):
        files.append("../vibe/buffett/PLANS.md")
    return files


def bounded_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[TRUNCATED: context file exceeded max chars]\n"


def build_context(diff_ref: str | None, max_diff_chars: int, max_context_file_chars: int) -> str:
    changed_files = collect_changed_files(diff_ref)
    diff_text = collect_diff(diff_ref, max_diff_chars)
    context_paths = context_files_for(changed_files)

    chunks: list[str] = [
        "# Codex LLM Review Context",
        "",
        "## Boundary",
        "",
        "Read-only review context. Do not auto-fix, execute operational batches, or write outputs.",
        "",
        "## Changed Files",
        "",
    ]

    if changed_files:
        chunks.extend(f"- `{path}`" for path in changed_files)
    else:
        chunks.append("- No changed files reported by git diff.")

    chunks.extend(["", "## Context Files", ""])
    for item in context_paths:
        path = (ROOT / item).resolve()
        if not path.exists():
            chunks.extend([f"### {item}", "", "[MISSING]", ""])
            continue
        chunks.extend(
            [
                f"### {rel_path(path)}",
                "",
                "```text",
                bounded_text(read_text(path), max_context_file_chars),
                "```",
                "",
            ]
        )

    chunks.extend(["## Unified Diff", "", "```diff", diff_text or "[EMPTY DIFF]", "```", ""])
    return "\n".join(chunks)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Build a read-only review context for Codex LLM code review CI."
    )
    parser.add_argument(
        "--diff-ref",
        help="Optional git diff ref, for example origin/main...HEAD. Defaults to working tree diff.",
    )
    parser.add_argument(
        "--max-diff-chars",
        type=int,
        default=60000,
        help="Maximum diff characters to include before truncation.",
    )
    parser.add_argument(
        "--max-context-file-chars",
        type=int,
        default=40000,
        help="Maximum characters per context file before truncation.",
    )
    args = parser.parse_args()

    print(build_context(args.diff_ref, args.max_diff_chars, args.max_context_file_chars))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
