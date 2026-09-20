# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".txt",
    ".json",
    ".csv",
    ".html",
    ".htm",
    ".bat",
    ".cmd",
    ".ps1",
}
DEFAULT_EXCLUDE_PARTS = {
    ".git",
    "__pycache__",
    "backup",
    "_backup",
    "_bulk_backup_print_to_logging_20260406_143259",
}
DEFAULT_EXCLUDE_NAME_PREFIXES = (
    "backup_",
)

LATIN1_MOJIBAKE_CHARS = "".join(chr(x) for x in (
    0x00C4, 0x00C2, 0x00C6, 0x00C7, 0x00C8, 0x00C9, 0x00D6, 0x00D7,
    0x00D8, 0x00D9, 0x00DA, 0x00DB, 0x00DC, 0x00E0, 0x00E2, 0x00E3,
    0x00E4, 0x00E5, 0x00E6, 0x00E7, 0x00E9, 0x00EA,
    0xCC3C, 0xCC3D, 0xCC3E, 0xCC44, 0xCC45, 0xCC48, 0xCC4C, 0xCC54,
    0xCC55, 0xCC57, 0xCC58, 0xCC59, 0xCC60, 0xCC64, 0xCC66, 0xCC68,
    0xCC70, 0xCC79, 0xCC98, 0xCC99, 0xCC9C, 0xCCA0, 0xCCA9, 0xCCAB,
    0xCCAC, 0xCCAD, 0xCCB4, 0xCCB5, 0xCCB8, 0xCCBC,
))
CJK_MOJIBAKE_CHARS = "".join(chr(x) for x in (
    0x7344, 0xC33E, 0xCC8E, 0x24E9, 0xB2E9, 0x6028, 0xB6A3, 0xBFC4,
    0xC88E, 0xC1FD, 0xC575, 0xC744, 0xCFE0, 0x5937, 0x3474, 0xCD2F,
    0xA7AC, 0xC955, 0xB426, 0xD7A2, 0x82D1,
))

SUSPICIOUS_RE = re.compile(
    r"[\ufffd]|"
    r"[\x80-\x9f]|"
    rf"[{re.escape(LATIN1_MOJIBAKE_CHARS)}]{{2,}}|"
    rf"[{re.escape(CJK_MOJIBAKE_CHARS)}]{{2,}}|"
    r"\?{4,}"
)


def _read_text(path: Path) -> tuple[str, str] | tuple[None, str]:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return None, "decode_failed"


def _suspicious_score(text: str) -> int:
    score = 0
    score += text.count("\ufffd") * 8
    score += len(re.findall(r"[\x80-\x9f]", text)) * 4
    score += len(re.findall(r"\?{4,}", text)) * 4
    score += len(re.findall(rf"[{re.escape(LATIN1_MOJIBAKE_CHARS)}]", text))
    score += len(re.findall(rf"[{re.escape(CJK_MOJIBAKE_CHARS)}]", text))
    return score


def _hangul_count(text: str) -> int:
    return len(re.findall(r"[\uac00-\ud7a3]", text))


def _repair_candidates(text: str) -> list[dict[str, Any]]:
    candidates: list[tuple[str, str]] = []
    for name, enc, dec in (
        ("latin1_to_utf8", "latin-1", "utf-8"),
        ("cp1252_to_utf8", "cp1252", "utf-8"),
        ("cp949_to_utf8", "cp949", "utf-8"),
        ("latin1_to_cp949", "latin-1", "cp949"),
    ):
        try:
            repaired = text.encode(enc).decode(dec)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if repaired != text:
            candidates.append((name, repaired))

    original_score = _suspicious_score(text)
    original_hangul = _hangul_count(text)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for method, repaired in candidates:
        if repaired in seen:
            continue
        seen.add(repaired)
        repaired_score = _suspicious_score(repaired)
        repaired_hangul = _hangul_count(repaired)
        if repaired_score < original_score and repaired_hangul >= original_hangul:
            out.append(
                {
                    "method": method,
                    "text": repaired,
                    "score_before": original_score,
                    "score_after": repaired_score,
                    "hangul_before": original_hangul,
                    "hangul_after": repaired_hangul,
                }
            )
    out.sort(key=lambda x: (int(x["score_after"]), -int(x["hangul_after"])))
    return out


def _iter_files(paths: list[str], root: Path, recursive: bool) -> Iterable[Path]:
    if paths:
        for raw in paths:
            p = Path(raw)
            if not p.is_absolute():
                p = root / p
            if p.is_file():
                yield p
            elif p.is_dir():
                pattern = "**/*" if recursive else "*"
                for child in p.glob(pattern):
                    if child.is_file():
                        yield child
        return

    pattern = "**/*" if recursive else "*"
    for child in root.glob(pattern):
        if child.is_file():
            yield child


def _is_excluded(path: Path) -> bool:
    parts = set(path.parts)
    if parts & DEFAULT_EXCLUDE_PARTS:
        return True
    return any(part.startswith(DEFAULT_EXCLUDE_NAME_PREFIXES) for part in path.parts)


def _scan_file(path: Path) -> dict[str, Any]:
    text, enc = _read_text(path)
    if text is None:
        return {
            "path": str(path),
            "encoding": enc,
            "status": "DECODE_FAIL",
            "issue_count": 1,
            "issues": [{"line": 0, "text": "", "repairable": False, "reason": "decode_failed"}],
        }

    issues: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.split("\n"), start=1):
        if not SUSPICIOUS_RE.search(line):
            continue
        repairs = _repair_candidates(line)
        issues.append(
            {
                "line": line_no,
                "text": line.strip()[:300],
                "repairable": bool(repairs),
                "best_method": repairs[0]["method"] if repairs else "",
                "suggested": repairs[0]["text"].strip()[:300] if repairs else "",
                "score": _suspicious_score(line),
            }
        )

    return {
        "path": str(path),
        "encoding": enc,
        "status": "ISSUE" if issues else "PASS",
        "issue_count": len(issues),
        "repairable_count": sum(1 for x in issues if x.get("repairable")),
        "issues": issues,
    }


def _apply_repairs(path: Path, backup_dir: Path) -> dict[str, Any]:
    text, enc = _read_text(path)
    if text is None:
        return {"path": str(path), "applied": False, "reason": enc}

    lines = text.split("\n")
    changed = 0
    new_lines: list[str] = []
    for idx, line in enumerate(lines):
        body = line[:-1] if line.endswith("\r") else line
        suffix = "\r" if line.endswith("\r") else ""
        if SUSPICIOUS_RE.search(body):
            repairs = _repair_candidates(body)
            if repairs:
                body = str(repairs[0]["text"])
                changed += 1
        new_lines.append(body + suffix)

    if changed <= 0:
        return {"path": str(path), "applied": False, "reason": "no_repairable_lines"}

    new_text = "\n".join(new_lines)
    try:
        new_text.encode(enc)
    except UnicodeEncodeError as exc:
        return {
            "path": str(path),
            "applied": False,
            "reason": f"encoding_preserve_failed:{enc}:{exc.__class__.__name__}",
            "encoding_before": enc,
        }

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / path.name
    if backup_path.exists():
        backup_path = backup_dir / f"{path.stem}_{dt.datetime.now().strftime('%H%M%S')}{path.suffix}"
    shutil.copy2(path, backup_path)
    path.write_text(new_text, encoding=enc, newline="")
    return {
        "path": str(path),
        "applied": True,
        "changed_lines": changed,
        "backup": str(backup_path),
        "encoding_before": enc,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Scan and optionally repair mojibake-looking text only.")
    ap.add_argument("--root", default=str(ROOT))
    ap.add_argument("--path", action="append", default=[], help="File or directory to scan. Repeatable.")
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--apply", action="store_true", help="Repair only lines with reversible mojibake candidates.")
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    root = Path(args.root)
    explicit_apply_files: set[Path] = set()
    if args.apply:
        if not args.path:
            print("[MOJIBAKE] --apply requires one or more explicit --path file arguments")
            return 2
        for raw in args.path:
            p = Path(raw)
            if not p.is_absolute():
                p = root / p
            if not p.is_file():
                print(f"[MOJIBAKE] --apply is allowed only for explicit files: {p}")
                return 2
            explicit_apply_files.add(p.resolve())

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json) if args.out_json else LOG_DIR / f"mojibake_text_scan_{stamp}.json"
    latest_json = LOG_DIR / "mojibake_text_scan_latest.json"
    backup_dir = ROOT / "backup" / "mojibake_text_apply" / stamp

    files: list[Path] = []
    for p in _iter_files(list(args.path), root, bool(args.recursive)):
        if _is_excluded(p):
            continue
        if p.suffix.lower() not in TEXT_SUFFIXES:
            continue
        files.append(p)
    files = sorted(set(files), key=lambda x: str(x).lower())

    scans = [_scan_file(p) for p in files]
    apply_results: list[dict[str, Any]] = []
    if args.apply:
        for scan in scans:
            if scan.get("repairable_count", 0) <= 0:
                continue
            p = Path(str(scan.get("path", "")))
            if p.resolve() in explicit_apply_files:
                apply_results.append(_apply_repairs(p, backup_dir))
        scans_after = [_scan_file(p) for p in files]
    else:
        scans_after = scans

    issues = [x for x in scans if int(x.get("issue_count", 0) or 0) > 0]
    issues_after = [x for x in scans_after if int(x.get("issue_count", 0) or 0) > 0]
    payload = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "root": str(root),
        "paths": list(args.path),
        "recursive": bool(args.recursive),
        "apply": bool(args.apply),
        "files_scanned": len(files),
        "issue_file_count": len(issues),
        "issue_count": sum(int(x.get("issue_count", 0) or 0) for x in issues),
        "repairable_count": sum(int(x.get("repairable_count", 0) or 0) for x in issues),
        "post_apply_issue_file_count": len(issues_after),
        "post_apply_issue_count": sum(int(x.get("issue_count", 0) or 0) for x in issues_after),
        "apply_results": apply_results,
        "issues": issues_after if args.apply else issues,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out_json.write_text(text, encoding="utf-8")
    latest_json.write_text(text, encoding="utf-8")

    print(f"[MOJIBAKE] json={out_json}")
    print(f"[MOJIBAKE] latest={latest_json}")
    print(
        "[MOJIBAKE] files={files} issue_files={issue_files} issues={issues} repairable={repairable}".format(
            files=payload["files_scanned"],
            issue_files=payload["post_apply_issue_file_count"] if args.apply else payload["issue_file_count"],
            issues=payload["post_apply_issue_count"] if args.apply else payload["issue_count"],
            repairable=payload["repairable_count"],
        )
    )
    if args.apply:
        applied = sum(1 for x in apply_results if x.get("applied"))
        print(f"[MOJIBAKE] applied_files={applied} backup_dir={backup_dir}")
    final_issue_count = payload["post_apply_issue_count"] if args.apply else payload["issue_count"]
    return 1 if final_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
