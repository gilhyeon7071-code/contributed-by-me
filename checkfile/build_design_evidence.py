#!/usr/bin/env python3
"""Build design evidence JSON for phase-2 verification.

Priority (later precedence):
1) external design evidence file
2) dashboard_state.design_evidence / dashboard_state.design
3) template seed

Each section is tagged with evidence_source/_source for evidence-integrity gating.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Tuple


SECTIONS = [
    "process_isolation",
    "message_queue",
    "db_schema",
    "module_separation",
]


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def pick_section(
    section: str,
    template: Dict[str, Any],
    from_state: Dict[str, Any],
    external: Dict[str, Any],
) -> Tuple[Dict[str, Any], str]:
    candidates = [
        ("external_file", as_dict(external.get(section))),
        ("dashboard_state", as_dict(from_state.get(section))),
        ("template", as_dict(template.get(section))),
    ]

    for default_source, block in candidates:
        if not block:
            continue
        out = dict(block)
        source = str(out.get("evidence_source") or out.get("_source") or default_source).strip().lower()
        out["evidence_source"] = source
        out["_source"] = source
        return out, source

    return {}, "missing"


def build(args: argparse.Namespace) -> Dict[str, Any]:
    template = load_json(Path(args.template))
    state = load_json(Path(args.dashboard_state))
    external = load_json(Path(args.external))

    from_state = {}
    from_state.update(as_dict(state.get("design_evidence")))
    from_state.update(as_dict(state.get("design")))

    out: Dict[str, Any] = {}
    selected_sources: Dict[str, str] = {}

    for section in SECTIONS:
        block, src = pick_section(section, template, from_state, external)
        out[section] = block
        selected_sources[section] = src

    out["_meta"] = {
        "template": str(Path(args.template)),
        "dashboard_state": str(Path(args.dashboard_state)),
        "external": str(Path(args.external)),
        "selected_sources": selected_sources,
    }
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build design evidence JSON")
    p.add_argument("--out", required=True)
    p.add_argument("--template", default=r"E:\1_Data\checkfile\design_evidence_template.json")
    p.add_argument("--dashboard-state", default=r"E:\vibe\buffett\runs\dashboard_state_latest.json")
    p.add_argument("--external", default=r"E:\vibe\buffett\runs\design_evidence_latest.json")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    payload = build(args)
    save_json(Path(args.out), payload)
    print(f"design evidence written: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
