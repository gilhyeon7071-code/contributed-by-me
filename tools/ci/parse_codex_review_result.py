from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


VALID_RESULTS = {"PASS", "WARN", "HARD_FAIL"}


def read_input(path: str | None) -> str:
    if path:
        target = Path(path)
        try:
            return target.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return target.read_text(encoding="utf-8-sig")
    return sys.stdin.read()


def extract_machine_result(text: str) -> dict[str, Any]:
    marker = re.search(r"Machine result\s*```json\s*(\{.*?\})\s*```", text, re.S)
    if not marker:
        marker = re.search(r"Machine result\s*(\{.*?\})", text, re.S)
    if not marker:
        raise ValueError("missing Machine result JSON block")

    data = json.loads(marker.group(1))
    if not isinstance(data, dict):
        raise ValueError("Machine result must be a JSON object")

    result = data.get("result")
    if result not in VALID_RESULTS:
        raise ValueError(f"invalid result: {result!r}")

    findings_count = data.get("findings_count")
    if not isinstance(findings_count, int) or findings_count < 0:
        raise ValueError("findings_count must be a non-negative integer")

    return data


def exit_code_for(result: str, hard_fail_blocks: bool) -> int:
    if hard_fail_blocks and result == "HARD_FAIL":
        return 2
    return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Parse Codex LLM review output and return an advisory CI result."
    )
    parser.add_argument(
        "review_output",
        nargs="?",
        help="Optional path to a review output file. Defaults to stdin.",
    )
    parser.add_argument(
        "--hard-fail-blocks",
        action="store_true",
        help="Exit non-zero when Machine result is HARD_FAIL.",
    )
    args = parser.parse_args()

    try:
        data = extract_machine_result(read_input(args.review_output))
    except Exception as exc:
        print(f"parse_error: {exc}", file=sys.stderr)
        return 3

    result = str(data["result"])
    findings_count = int(data["findings_count"])
    print(json.dumps({"result": result, "findings_count": findings_count}, ensure_ascii=False))
    return exit_code_for(result, args.hard_fail_blocks)


if __name__ == "__main__":
    raise SystemExit(main())
