from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from fill_idempotency import IdempotencyConflict, filter_new_fill_rows
# [2026-09-13] The 2026-08-07 package split stopped re-exporting these from
#   paper_engine/__init__; every reference below raised AttributeError at import
#   or first call. Import from the module that defines them instead.
from paper_engine.entry import read_csv_safe
from paper_engine.io import LEGACY_FILLS_HEADER
from paper_engine.positions import _append_rows_atomic

OUT_JSON = ROOT / "2_Logs" / "fill_idempotency_engine_append_smoke_latest.json"

def _write_report(payload: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

def main() -> int:
    tmp_root = Path(tempfile.mkdtemp(prefix="fill_engine_append_smoke_"))
    try:
        fills_path = tmp_root / "fills.csv"
        state_path = tmp_root / "fills_idempotency.json"

        existing = ["20260514T09:00:00", "005930", "BUY", 1, 70000, "PAPER_BUY_005930_20260514", "seed"]
        duplicate = list(existing)
        new_row = ["20260514T09:01:00", "000660", "BUY", 2, 120000, "PAPER_BUY_000660_20260514", "new"]
        conflict = list(existing)
        conflict[4] = 71000

        _append_rows_atomic(fills_path, [existing], LEGACY_FILLS_HEADER)
        seeded_df = read_csv_safe(fills_path)
        seeded_rows = seeded_df.to_dict("records") if seeded_df is not None else []

        accepted, first_report = filter_new_fill_rows(
            header=LEGACY_FILLS_HEADER,
            existing_rows=seeded_rows,
            new_rows=[duplicate, new_row],
            source="engine_append_smoke_first",
            state_path=state_path,
        )
        _append_rows_atomic(fills_path, accepted, LEGACY_FILLS_HEADER)

        after_first = read_csv_safe(fills_path)
        after_first_rows = int(len(after_first)) if after_first is not None else -1

        accepted_second, second_report = filter_new_fill_rows(
            header=LEGACY_FILLS_HEADER,
            existing_rows=after_first.to_dict("records") if after_first is not None else [],
            new_rows=[new_row],
            source="engine_append_smoke_duplicate",
            state_path=state_path,
        )

        conflict_detected = False
        try:
            filter_new_fill_rows(
                header=LEGACY_FILLS_HEADER,
                existing_rows=after_first.to_dict("records") if after_first is not None else [],
                new_rows=[conflict],
                source="engine_append_smoke_conflict",
                state_path=state_path,
            )
        except IdempotencyConflict:
            conflict_detected = True

        state = json.loads(state_path.read_text(encoding="utf-8"))
        ok = (
            len(accepted) == 1
            and len(accepted_second) == 0
            and after_first_rows == 2
            and int(first_report.get("duplicate_rows") or 0) == 1
            and int(second_report.get("duplicate_rows") or 0) == 1
            and conflict_detected
            and int(state.get("last_applied_seq") or 0) == 1
        )
        payload = {
            "status": "PASS" if ok else "FAIL",
            "tmp_root": str(tmp_root),
            "fills_path": str(fills_path),
            "state_path": str(state_path),
            "after_first_rows": after_first_rows,
            "first_report": first_report,
            "second_report": second_report,
            "conflict_detected": conflict_detected,
            "state": state,
        }
        _write_report(payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        if ok:
            return 0
        if not conflict_detected:
            return 3
        return 2
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

if __name__ == "__main__":
    raise SystemExit(main())
