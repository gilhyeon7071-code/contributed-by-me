"""계좌 점검 (읽기 전용) — 한투 모의계좌 실측과 전략 원장을 나란히 놓는다. 발주·취소 없음.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.check_account --out-dir <폴더> [--state-dir <전략 상태폴더>] [--probe-code 005930]

보는 것
  계좌: 보유 / dnca_tot_amt(예수금, 매도 미결제 제외) / prvs_rcdl_excc_amt(D+2) / nrcvb_buy_amt(미수 없는 매수가능)
        / ruse_psbl_amt(당일 매도대금 재사용) — max_buy_amt 는 증거금 포함이라 판정에 안 쓴다
  판정: 전략 자본(3,000만)을 계좌가 감당하나(nrcvb_buy_amt >= 자본), 원장에 없는 보유가 계좌에 있나,
        원장 보유가 계좌보다 많나(불가능한 상태)
쓰기: out-dir 의 check_account_YYYYMMDD_HHMMSS.json + 날짜 없는 check_account_log.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from paper.strategies.kospi_mcap_quarterly_v2.src import fills as F
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import ledger as L
from paper.strategies.kospi_mcap_quarterly_v2.src.run_execution_day import DEFAULT_STRATEGY_CFG, _next_trading_day_fn


def check(client: Any, cfg: Dict[str, Any], *, state_dir: Optional[Path], probe_code: str, now: datetime,
          next_trading_day=None) -> Dict[str, Any]:
    bal = K.fetch_balance(client)
    q = K.fetch_quote(client, probe_code, lambda: now)
    bp = K.fetch_buying_power(client, probe_code, q.get("ask1") or 0)
    capital = float(cfg["strategy_capital_krw"])
    rep: Dict[str, Any] = {"checked_at": now.isoformat(timespec="seconds"), "broker": bal, "buying_power": bp,
                           "status": "OK", "findings": []}
    if bp["nrcvb_buy_amt"] < capital:
        rep["findings"].append(f"BUYING_POWER_BELOW_CAPITAL:{bp['nrcvb_buy_amt']}<{capital:.0f}")
    positions: Dict[str, int] = {}
    if state_dir is not None:
        fl = F.load_fills(Path(state_dir) / "fills.jsonl")
        led = L.build_ledger(capital, fl, as_of=now.strftime("%Y%m%d"), closes=bal["prices"], cfg=cfg,
                             next_trading_day=next_trading_day or _next_trading_day_fn())
        rep["strategy_ledger"] = {k: led[k] for k in ("status", "nav", "cash_economic", "cash_settled", "receivable",
                                                      "payable", "market_value")}
        positions = {p["code"]: p["qty"] for p in led["positions"]}
        rep["latch_file_exists"] = (Path(state_dir) / "latch.json").exists()
    over = {c: (q_, bal["holdings"].get(c, 0)) for c, q_ in positions.items() if bal["holdings"].get(c, 0) < q_}
    extra = {c: q_ for c, q_ in bal["holdings"].items() if c not in positions}
    if over:
        rep["findings"].append(f"LEDGER_EXCEEDS_BROKER:{over}")
    if extra:
        rep["findings"].append(f"BROKER_HOLDINGS_NOT_IN_STRATEGY:{extra}")  # 공유 계좌면 설명이 필요한 항목
    rep["unsettled_sell_proceeds_estimate"] = bal["prvs_rcdl_excc_amt"] - bal["dnca_tot_amt"]
    if over or bp["nrcvb_buy_amt"] < capital:
        rep["status"] = "STOP"
    elif extra:
        rep["status"] = "WARN"
    return rep


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--state-dir", type=Path)
    ap.add_argument("--probe-code", default="005930")
    ap.add_argument("--strategy-cfg", type=Path, default=DEFAULT_STRATEGY_CFG)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    cfg = json.loads(args.strategy_cfg.read_text(encoding="utf-8"))
    now = datetime.now()
    rep = check(K.make_client(), cfg, state_dir=args.state_dir, probe_code=args.probe_code, now=now)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / f"check_account_{now:%Y%m%d_%H%M%S}.json").write_text(
        json.dumps(rep, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    with (args.out_dir / "check_account_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"checked_at": rep["checked_at"], "status": rep["status"], "findings": rep["findings"],
                             "dnca_tot_amt": rep["broker"]["dnca_tot_amt"],
                             "prvs_rcdl_excc_amt": rep["broker"]["prvs_rcdl_excc_amt"],
                             "nrcvb_buy_amt": rep["buying_power"]["nrcvb_buy_amt"],
                             "ruse_psbl_amt": rep["buying_power"]["ruse_psbl_amt"],
                             "holdings": rep["broker"]["holdings"]}, ensure_ascii=False) + "\n")
    print(json.dumps(rep, ensure_ascii=False, indent=2, default=str))
    return {"OK": 0, "WARN": 1}.get(rep["status"], 3)


if __name__ == "__main__":
    raise SystemExit(main())
