# -*- coding: utf-8 -*-
"""Apply Korean stock term standard map to dashboard/report files.

Default map: E:/1_Data/docs/주식용어치환맵_v1.json
Default targets: E:/1_Data/docs/주식용어치환대상_v1.txt
"""

from __future__ import annotations

import argparse
import io
import json
import re
import tokenize
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


DEFAULT_MAP = Path(r"E:/1_Data/docs/주식용어치환맵_v1.json")
DEFAULT_TARGETS = Path(r"E:/1_Data/docs/주식용어치환대상_v1.txt")
DEFAULT_LOG_DIR = Path(r"E:/1_Data/2_Logs")


@dataclass
class Rule:
    mode: str
    src: str
    dst: str


@dataclass
class FileResult:
    path: str
    changed: bool
    total_replacements: int
    per_rule: list[dict[str, Any]]
    mode_used: str
    error: str | None = None


def _load_json(path: Path) -> dict[str, Any]:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    raise ValueError(f"cannot parse json: {path}")


def _read_targets(path: Path) -> list[Path]:
    out: list[Path] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip().lstrip("\ufeff")
        if not line or line.startswith("#"):
            continue
        out.append(Path(line))
    return out


def _read_text_fallback(path: Path) -> tuple[str, str]:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return path.read_text(encoding=enc), enc
        except Exception:
            continue
    raise UnicodeError(f"cannot decode: {path}")


def _apply_rules_with_counts(text: str, rules: Iterable[Rule]) -> tuple[str, list[int]]:
    cur = text
    counts: list[int] = []
    for r in rules:
        if not r.src:
            counts.append(0)
            continue
        if r.mode == "regex":
            nxt, n = re.subn(r.src, r.dst, cur)
        else:
            n = cur.count(r.src)
            nxt = cur.replace(r.src, r.dst)
        counts.append(int(n))
        cur = nxt
    return cur, counts


def _apply_rules_py_string_literals(text: str, rules: list[Rule]) -> tuple[str, list[int]]:
    reader = io.StringIO(text).readline
    tokens = list(tokenize.generate_tokens(reader))
    out_tokens: list[tokenize.TokenInfo] = []
    all_counts = [0] * len(rules)

    for tok in tokens:
        if tok.type == tokenize.STRING:
            replaced, counts = _apply_rules_with_counts(tok.string, rules)
            for i, n in enumerate(counts):
                all_counts[i] += int(n)
            tok = tok._replace(string=replaced)
        out_tokens.append(tok)

    return tokenize.untokenize(out_tokens), all_counts


def _counts_to_per_rule(rules: list[Rule], counts: list[int]) -> list[dict[str, Any]]:
    per_rule: list[dict[str, Any]] = []
    for idx, (r, n) in enumerate(zip(rules, counts), start=1):
        per_rule.append({"idx": idx, "mode": r.mode, "from": r.src, "to": r.dst, "count": int(n)})
    return per_rule


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--map", dest="map_path", default=str(DEFAULT_MAP))
    ap.add_argument("--targets", dest="targets_path", default=str(DEFAULT_TARGETS))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--log-dir", dest="log_dir", default=str(DEFAULT_LOG_DIR))
    ap.add_argument("--py-string-only", dest="py_string_only", action="store_true", default=True)
    ap.add_argument("--no-py-string-only", dest="py_string_only", action="store_false")
    args = ap.parse_args()

    map_path = Path(args.map_path)
    targets_path = Path(args.targets_path)
    log_dir = Path(args.log_dir)

    if not map_path.exists():
        raise FileNotFoundError(f"map missing: {map_path}")
    if not targets_path.exists():
        raise FileNotFoundError(f"targets missing: {targets_path}")

    spec = _load_json(map_path)
    rules_raw = spec.get("rules") or []
    rules: list[Rule] = []
    for it in rules_raw:
        if not isinstance(it, dict):
            continue
        rules.append(
            Rule(
                mode=str(it.get("mode") or "literal").strip().lower(),
                src=str(it.get("from") or ""),
                dst=str(it.get("to") or ""),
            )
        )

    targets = _read_targets(targets_path)
    results: list[FileResult] = []

    for path in targets:
        try:
            if not path.exists():
                results.append(
                    FileResult(
                        path=str(path),
                        changed=False,
                        total_replacements=0,
                        per_rule=[],
                        mode_used="NA",
                        error="missing file",
                    )
                )
                continue

            text, _enc = _read_text_fallback(path)

            if args.py_string_only and path.suffix.lower() == ".py":
                mode_used = "py_string_literal"
                new_text, counts = _apply_rules_py_string_literals(text, rules)
            else:
                mode_used = "text_all"
                new_text, counts = _apply_rules_with_counts(text, rules)

            per_rule = _counts_to_per_rule(rules, counts)
            total = int(sum(counts))
            changed = new_text != text

            if args.apply and changed:
                bak = path.with_suffix(path.suffix + f".bak_terms_{_timestamp()}")
                bak.write_text(text, encoding="utf-8")
                path.write_text(new_text, encoding="utf-8")

            results.append(
                FileResult(
                    path=str(path),
                    changed=bool(changed),
                    total_replacements=total,
                    per_rule=per_rule,
                    mode_used=mode_used,
                    error=None,
                )
            )
        except Exception as e:
            results.append(
                FileResult(
                    path=str(path),
                    changed=False,
                    total_replacements=0,
                    per_rule=[],
                    mode_used="ERR",
                    error=f"{type(e).__name__}: {e}",
                )
            )

    changed_files = sum(1 for r in results if r.changed)
    total_files = len(results)
    total_repl = sum(r.total_replacements for r in results)

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if args.apply else "DRY_RUN",
        "py_string_only": bool(args.py_string_only),
        "map": str(map_path),
        "targets": str(targets_path),
        "files_total": total_files,
        "files_changed": changed_files,
        "replacements_total": total_repl,
        "results": [r.__dict__ for r in results],
    }

    log_dir.mkdir(parents=True, exist_ok=True)
    out = log_dir / f"stock_terms_apply_{_timestamp()}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[TERMS] mode={summary['mode']} py_string_only={summary['py_string_only']} files={total_files} changed={changed_files} repl={total_repl}")
    print(f"[TERMS] log={out}")
    for r in results:
        state = "ERR" if r.error else ("CHANGED" if r.changed else "SAME")
        print(
            f"[{state}] {r.path} mode={r.mode_used} repl={r.total_replacements}"
            + (f" err={r.error}" if r.error else "")
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
