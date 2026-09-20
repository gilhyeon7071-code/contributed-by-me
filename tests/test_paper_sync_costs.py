import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import paper_sync


class PaperSyncCostTests(unittest.TestCase):
    def test_load_config_falls_back_to_engine_costs(self) -> None:
        legacy = ROOT / "paper" / "__missing_paper_config_for_test__.json"
        engine = ROOT / "paper" / "paper_engine_config.json"
        old_legacy = paper_sync.CONFIG_PATH
        old_engine = paper_sync.ENGINE_CONFIG_PATH
        try:
            paper_sync.CONFIG_PATH = legacy
            paper_sync.ENGINE_CONFIG_PATH = engine
            cfg = paper_sync.load_config()
        finally:
            paper_sync.CONFIG_PATH = old_legacy
            paper_sync.ENGINE_CONFIG_PATH = old_engine

        # [2026-09-09] 기대값을 **하드코딩하지 않는다.**
        #   예전에는 fee 4e-05 / tax 0.0015 를 박아두고 있었는데, 2026-08-24 에 설정이
        #   브로커 실측(fee 0 / tax 0.002)에 맞춰 교정된 뒤로 이 테스트만 낡은 값을
        #   주장하고 있었다. 문서·상수·테스트가 같은 계보라 서로를 보증하지 못한다 - PLANS (261).
        #   이 테스트의 계약은 "로더가 엔진 설정을 그대로 읽어오는가" 이지
        #   "요율이 얼마인가" 가 아니다. 요율은 바깥(브로커)이 정한다.
        import json as _json
        raw = _json.loads(engine.read_text(encoding="utf-8-sig"))
        self.assertEqual(cfg.fee_rate, float(raw["fee_pct"]))
        self.assertEqual(cfg.slippage_rate, float(raw["slippage_pct"]))
        self.assertEqual(cfg.sell_tax_rate, float(raw["sell_tax_pct"]))

    def test_optimizer_default_fee_matches_live_config(self) -> None:
        """시뮬레이션 비용과 원장 비용이 갈라지지 않게 묶는다.

        2026-08-24 ~ 09-09 동안 optimize_params_v41_1.DEFAULT_FEE 는 0.00358 인데
        라이브 설정 왕복은 0.00400 이었다. **시뮬이 원장보다 0.042%p 싸게 계산했고**
        그 상태로 나온 최적화 결과를 원장 성과와 비교하면 시뮬이 유리하게 보인다.
        둘을 묶는 장치가 없어서 16일간 조용히 유지됐다.
        """
        import json as _json
        import optimize_params_v41_1 as opt

        raw = _json.loads(
            (ROOT / "paper" / "paper_engine_config.json").read_text(encoding="utf-8-sig")
        )
        live_roundtrip = (
            float(raw["fee_pct"]) * 2.0
            + float(raw["slippage_pct"]) * 2.0
            + float(raw["sell_tax_pct"])
        )
        self.assertAlmostEqual(
            opt.DEFAULT_FEE, live_roundtrip, places=6,
            msg="DEFAULT_FEE=%.5f vs 라이브 왕복=%.5f - 시뮬과 원장이 다른 비용을 본다"
                % (opt.DEFAULT_FEE, live_roundtrip),
        )

    def test_build_trades_applies_roundtrip_costs_and_sell_tax(self) -> None:
        fills = pd.DataFrame(
            [
                {"ts": "2026-04-01 09:00:00", "code": "000001", "name": "", "side": "BUY", "qty": 10, "price": 100.0, "order_id": "", "note": ""},
                {"ts": "2026-04-02 15:20:00", "code": "000001", "name": "", "side": "SELL", "qty": 10, "price": 110.0, "order_id": "", "note": ""},
            ]
        )
        cfg = paper_sync.Config(fee_rate=0.005, slippage_rate=0.001, sell_tax_rate=0.002)
        trades = paper_sync.build_trades_from_fills(fills, cfg)

        self.assertEqual(len(trades), 1)
        self.assertAlmostEqual(float(trades.loc[0, "gross_ret"]), 0.10, places=8)
        self.assertAlmostEqual(float(trades.loc[0, "net_ret"]), 0.0854, places=8)
        self.assertEqual(float(trades.loc[0, "sell_tax_rate"]), 0.002)


if __name__ == "__main__":
    unittest.main()
