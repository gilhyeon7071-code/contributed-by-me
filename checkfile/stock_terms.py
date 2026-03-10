"""Shared stock-term glossary loader for report localization."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict


GLOSSARY_PATH = Path(__file__).with_name("stock_terms_glossary.json")


DEFAULT_STOCK_TERMS: Dict[str, Dict[str, str]] = {
    "status_label_map": {
        "PASSED": "통과",
        "FAILED": "실패",
        "WARNING": "경고",
        "SKIPPED": "건너뜀",
        "ERROR": "오류",
        "N/A": "해당없음",
    },
    "status_chip_map": {
        "PASSED": "chip-pass",
        "FAILED": "chip-fail",
        "WARNING": "chip-warn",
        "SKIPPED": "chip-skip",
        "ERROR": "chip-fail",
        "N/A": "chip-na",
    },
    "phase_title_map": {},
    "item_name_map": {},
    "message_map": {},
    "expected_map": {},
    "token_map": {},
    "api_provider_map": {},
    "message_prefix_map": {},
    "expected_post_replacements": {},
}


def _deep_update(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def load_stock_terms(path: str | None = None) -> Dict[str, Dict[str, str]]:
    terms: Dict[str, Dict[str, str]] = copy.deepcopy(DEFAULT_STOCK_TERMS)
    target = Path(path) if path else GLOSSARY_PATH
    if not target.exists():
        return terms

    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return terms

    if isinstance(raw, dict):
        _deep_update(terms, raw)
    return terms
