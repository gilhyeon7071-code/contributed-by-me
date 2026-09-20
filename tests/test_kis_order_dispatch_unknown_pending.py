import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(r"E:\1_Data")
TOOLS = ROOT / "tools"
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import kis_order_dispatch_from_exec as dispatch


class _FakeClient:
    def __init__(self, rows=None, exc=None):
        self.rows = list(rows or [])
        self.exc = exc
        self.calls = []

    def inquire_daily_ccld(self, **kwargs):
        self.calls.append(kwargs)
        if self.exc:
            raise self.exc
        return {"ok": True, "rows": list(self.rows), "pages": 1}


class UnknownPendingDispatchTests(unittest.TestCase):
    def _row(self, status, ord_no="", code="005930"):
        return {
            "dispatch_ts": "2026-05-17T09:00:00",
            "exec_date": "20260517",
            "side": "BUY",
            "code": code,
            "qty": "10",
            "price": "0",
            "signal_date": "20260517",
            "source_row": "3",
            "dispatch_status": status,
            "ok": "False",
            "ord_no": ord_no,
            "org_no": "",
            "error": "",
            "precheck": "PASS",
        }

    def test_load_unresolved_pending_skips_when_latest_status_is_accepted(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "orders_20260517_broker_submit_prod.csv"
            pd.DataFrame(
                [
                    self._row("UNKNOWN_PENDING"),
                    self._row("ACCEPTED", ord_no="OD123"),
                ]
            ).to_csv(path, index=False, encoding="utf-8-sig")

            pending = dispatch._load_unresolved_pending_rows(path, "20260517")

            self.assertEqual(len(pending), 0)

    def test_load_unresolved_pending_returns_one_latest_row_per_key(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "orders_20260517_broker_submit_prod.csv"
            older = self._row("UNKNOWN_PENDING")
            older["error"] = "first timeout"
            newer = self._row("UNKNOWN_PENDING")
            newer["error"] = "second timeout"
            pd.DataFrame([older, newer]).to_csv(path, index=False, encoding="utf-8-sig")

            pending = dispatch._load_unresolved_pending_rows(path, "20260517")

            self.assertEqual(len(pending), 1)
            self.assertEqual(pending.iloc[0]["error"], "second timeout")

    def test_resolve_unknown_pending_marks_found_order_as_accepted(self):
        pending = pd.DataFrame([self._row("UNKNOWN_PENDING")])
        client = _FakeClient(
            rows=[
                {
                    "pdno": "005930",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "10",
                    "odno": "OD123",
                    "ord_gno_brno": "BR01",
                }
            ]
        )

        records = dispatch._resolve_unknown_pending_records(
            pending,
            client,
            now_ts="2026-05-17T09:01:00",
            max_pages=5,
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["dispatch_status"], "ACCEPTED")
        self.assertEqual(records[0]["ord_no"], "OD123")
        self.assertEqual(records[0]["broker_reconcile_status"], "FOUND")
        self.assertEqual(client.calls[0]["pdno"], "005930")

    def test_resolve_unknown_pending_query_failure_fails_closed(self):
        pending = pd.DataFrame([self._row("UNKNOWN_PENDING")])
        client = _FakeClient(exc=RuntimeError("query down"))

        records = dispatch._resolve_unknown_pending_records(
            pending,
            client,
            now_ts="2026-05-17T09:01:00",
            max_pages=5,
        )

        self.assertEqual(records[0]["dispatch_status"], "PENDING_BROKER_QUERY_FAIL")
        self.assertEqual(records[0]["precheck"], "FAIL")
        self.assertEqual(records[0]["broker_reconcile_status"], "QUERY_FAIL")


if __name__ == "__main__":
    unittest.main()
