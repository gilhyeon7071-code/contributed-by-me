from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def append_audit_record(
    *,
    audit_root: Path,
    task: str,
    run_id: str,
    d_ymd: str,
    model: str,
    prompt_text: str,
    code_sha: str,
    input_path: Path,
    output_path: Path,
    validator_exit_code: int,
    golden_result: str,
    extra: dict[str, Any] | None = None,
) -> Path:
    audit_root.mkdir(parents=True, exist_ok=True)
    rec = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'task': task,
        'run_id': run_id,
        'D': d_ymd,
        'model': model,
        'prompt_sha': _sha256_text(prompt_text),
        'code_sha': code_sha,
        'input_ptr': str(input_path),
        'output_ptr': str(output_path),
        'input_sha': _sha256_file(input_path),
        'output_sha': _sha256_file(output_path),
        'validator_exit_code': int(validator_exit_code),
        'golden_result': golden_result,
    }
    if extra:
        rec['extra'] = extra

    out = audit_root / f'{task}_{d_ymd}.jsonl'
    with out.open('a', encoding='utf-8') as f:
        f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    return out
