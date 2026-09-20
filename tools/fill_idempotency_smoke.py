from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from fill_idempotency import IdempotencyConflict, filter_new_fill_rows


def main() -> int:
    tmp_root = Path(tempfile.mkdtemp(prefix="fill_idempotency_smoke_"))
    try:
        state_path = tmp_root / "fills_idempotency.json"
        header = ["datetime", "date", "code", "name", "side", "qty", "price", "fee", "slippage", "order_id", "note"]
        row = ["20260514T09:00:00", "20260514", "005930", "Samsung", "BUY", 1, 70000, 0, 0, "PAPER_BUY_005930_20260514", ""]

        first, first_report = filter_new_fill_rows(
            header=header,
            existing_rows=[],
            new_rows=[row],
            source="smoke_first",
            state_path=state_path,
        )
        second, second_report = filter_new_fill_rows(
            header=header,
            existing_rows=[],
            new_rows=[row],
            source="smoke_duplicate",
            state_path=state_path,
        )
        conflict_row = list(row)
        conflict_row[6] = 71000
        conflict_detected = False
        try:
            filter_new_fill_rows(
                header=header,
                existing_rows=[],
                new_rows=[conflict_row],
                source="smoke_conflict",
                state_path=state_path,
            )
        except IdempotencyConflict:
            conflict_detected = True

        state = json.loads(state_path.read_text(encoding="utf-8"))
        ok = (
            len(first) == 1
            and len(second) == 0
            and bool(conflict_detected)
            and int(state.get("dup_attempts") or 0) == 1
            and int(state.get("last_applied_seq") or 0) == 1
        )
        print(
            json.dumps(
                {
                    "status": "PASS" if ok else "FAIL",
                    "state_path": str(state_path),
                    "first": first_report,
                    "second": second_report,
                    "conflict_detected": conflict_detected,
                    "state": state,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if ok:
            return 0
        if not conflict_detected:
            return 3
        return 2
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
