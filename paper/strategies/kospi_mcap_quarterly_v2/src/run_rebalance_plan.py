"""D1 → D2 → D3 → D6 → D7 한 번에. 목표 포트폴리오까지 만들고 주문은 만들지 않는다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.run_rebalance_plan \
        --selection-date 20260930 --files <f1> <f2> <f3> --out-dir <경로> [--format-test]

- D3 이후는 D1·D2 가 **쓴 파일을 다시 읽어서** 쓴다(칸 연결 G4 를 실제로 거친다)
- 쓰기 지점: out_dir 안 파일과 날짜 없는 target_log.jsonl 뿐 (D1·D2 쓰기는 run_d1_d2 참조)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from paper.strategies.kospi_mcap_quarterly_v2.src import run_d1_d2
from paper.strategies.kospi_mcap_quarterly_v2.src import targets as T

DEFAULT_STRATEGY_CFG = run_d1_d2.STRATEGY_ROOT / "config" / "strategy_v1.json"


def run(selection_date: str, files: List[Path], out_dir: Path, *, index_csv: Path, thresholds_path: Path,
        strategy_cfg_path: Path, format_test: bool) -> Dict[str, Any]:
    res = run_d1_d2.run(selection_date, files, out_dir, index_csv=index_csv,
                        thresholds_path=thresholds_path, format_test=format_test)
    if res.get("status") != "OK":
        return res
    out_dir = out_dir.resolve()
    cfg = json.loads(strategy_cfg_path.read_text(encoding="utf-8"))
    universe = pd.read_csv(out_dir / f"universe_{selection_date}.csv", dtype={"code": str})
    kospi200 = pd.read_csv(out_dir / f"kospi200_{selection_date}.csv", dtype={"date": str})

    regime = T.compute_regime(kospi200, selection_date, cfg)
    stage_out: Dict[str, Any] = {"d3": regime}
    status, stage, reasons = regime["status"], "D3", regime["reasons"]
    if status == "OK":
        selection, sel = T.select_top(universe, cfg)
        selection.to_csv(out_dir / f"selection_{selection_date}.csv", index=False, encoding="utf-8-sig")
        stage_out["d6"] = sel
        status, stage, reasons = sel["status"], "D6", sel["reasons"]
        if status == "OK":
            portfolio, summary = T.build_target_portfolio(selection, regime["exposure"], cfg)
            portfolio.to_csv(out_dir / f"target_portfolio_{selection_date}.csv", index=False, encoding="utf-8-sig")
            stage_out["d7"] = summary
            status, stage, reasons = summary["status"], "D7", summary["reasons"]

    res.update({"status": status, "stage": stage, **stage_out, "strategy_cfg": str(strategy_cfg_path)})
    (out_dir / f"target_{selection_date}_summary.json").write_text(
        json.dumps({k: res.get(k) for k in ("selection_date", "status", "stage", "format_test", "d3", "d6", "d7")},
                   ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    with (out_dir / "target_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"selection_date": selection_date, "status": status, "stage": stage, "reasons": reasons,
                             "exposure": regime.get("exposure"), "format_test": format_test,
                             "invested": (stage_out.get("d7") or {}).get("invested"),
                             "run_at": datetime.now().isoformat(timespec="seconds")}, ensure_ascii=False) + "\n")
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selection-date", required=True)
    ap.add_argument("--files", nargs=3, required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--index-csv", type=Path, default=run_d1_d2.DEFAULT_INDEX_CSV)
    ap.add_argument("--thresholds", type=Path, default=run_d1_d2.DEFAULT_THRESHOLDS)
    ap.add_argument("--strategy-cfg", type=Path, default=DEFAULT_STRATEGY_CFG)
    ap.add_argument("--format-test", action="store_true")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    res = run(args.selection_date, args.files, args.out_dir, index_csv=args.index_csv, thresholds_path=args.thresholds,
              strategy_cfg_path=args.strategy_cfg, format_test=args.format_test)
    print(json.dumps({k: res.get(k) for k in ("selection_date", "status", "stage", "format_test", "reasons", "d3", "d6", "d7")},
                     ensure_ascii=False, indent=2, default=str))
    return 0 if res.get("status") == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())
