"""부분 매도가 보유 원장에서 수량만 차감되는지 고정한다.

경위: 2026-09-09. 교정 매도(003230 27주 중 15주)가 체결됐는데
topn_reconcile 이 보유 행을 통째로 지웠다. 브로커는 12주를 들고 있는데
원장은 그 종목을 잃었고, 그래서 보유가 5가 아니라 4로 읽혀
빈자리가 1에서 2로 늘고 주문 파일이 매수 1건에서 2건으로 다시 만들어졌다.
**원장 오류가 그날의 매매 결정을 바꿨다.**

하네스 전략은 항상 전량 매도라 이 결함이 그동안 드러나지 않았다.
전량 매도에서 행이 사라지는 동작은 그대로 유지돼야 하므로 둘 다 고정한다.
"""
import unittest

import pandas as pd

POS_COLS = ["code", "entry_date", "qty", "entry_price", "held_days", "last_update"]


def _apply_sold(new_pos: pd.DataFrame, filled: dict) -> pd.DataFrame:
    """tools/topn_reconcile.py 의 매도 차감 블록과 같은 산식."""
    sold_qty = {c: int(q) for (c, s), (q, _px) in filled.items() if s == "SELL" and int(q) > 0}
    if sold_qty and len(new_pos):
        new_pos = new_pos.copy()
        new_pos["qty"] = pd.to_numeric(new_pos["qty"], errors="coerce").fillna(0).astype(int)
        for code, q in sold_qty.items():
            m = new_pos["code"].astype(str) == str(code)
            if bool(m.any()):
                before = int(new_pos.loc[m, "qty"].iloc[0])
                new_pos.loc[m, "qty"] = before - int(q)
        new_pos = new_pos[new_pos["qty"] > 0]
    return new_pos


class TopnReconcilePartialSellTests(unittest.TestCase):
    def _base(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"code": "003230", "entry_date": "20260908", "qty": 27,
                 "entry_price": 1283203.703, "held_days": 0, "last_update": "20260908"},
                {"code": "000370", "entry_date": "20260908", "qty": 2003,
                 "entry_price": 8171.0, "held_days": 0, "last_update": "20260908"},
            ],
            columns=POS_COLS,
        )

    def test_partial_sell_keeps_remainder(self) -> None:
        out = _apply_sold(self._base(), {("003230", "SELL"): (15, 1291000.0)})
        self.assertEqual(sorted(out["code"]), ["000370", "003230"])
        self.assertEqual(int(out.loc[out["code"] == "003230", "qty"].iloc[0]), 12)

    def test_full_sell_removes_row(self) -> None:
        out = _apply_sold(self._base(), {("003230", "SELL"): (27, 1291000.0)})
        self.assertEqual(list(out["code"]), ["000370"])

    def test_oversell_does_not_leave_negative(self) -> None:
        out = _apply_sold(self._base(), {("003230", "SELL"): (30, 1291000.0)})
        self.assertEqual(list(out["code"]), ["000370"])
        self.assertTrue((out["qty"] > 0).all())

    def test_sell_of_unheld_code_is_ignored(self) -> None:
        out = _apply_sold(self._base(), {("999999", "SELL"): (5, 1000.0)})
        self.assertEqual(sorted(out["code"]), ["000370", "003230"])
        self.assertEqual(int(out.loc[out["code"] == "003230", "qty"].iloc[0]), 27)


if __name__ == "__main__":
    unittest.main()
