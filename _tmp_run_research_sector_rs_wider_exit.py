# -*- coding: utf-8 -*-
"""어제 combo_sector_exit_test 로그의 'sector_rs_pos + wider_exit' 조합을
report_backtest_v41_1.py 연구 모드로 재현 (정확한 정의가 없어 추정 기반)."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(r"E:\1_Data")
OUT_DIR = ROOT / "2_Logs" / "research_sector_rs_wider_exit"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PARAM_PATH = OUT_DIR / "research_params.json"
CONTRACT_PATH = OUT_DIR / "research_selection_contract.json"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
CONFIG_BAK = OUT_DIR / "paper_engine_config.json.bak"

# stable_params_v41_1.json을 기반으로 파라미터 작성
STABLE_PARAMS = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"


def make_params() -> None:
    raw = json.loads(STABLE_PARAMS.read_text(encoding="utf-8-sig"))
    # 필요한 상위 구조 지원
    params = raw.get("params", raw) if isinstance(raw, dict) else raw
    payload = {"params": dict(params)}
    PARAM_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {PARAM_PATH}")


def make_contract() -> None:
    contract = {
        "selection_contract": {
            "numeric_filters": [
                {"field": "signal_rs", "op": ">", "value": 0.0}
            ],
            "exit_overrides": {}
        }
    }
    CONTRACT_PATH.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {CONTRACT_PATH}")


def backup_and_patch_config() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    shutil.copy2(CONFIG_PATH, CONFIG_BAK)
    print(f"[OK] backed up config to {CONFIG_BAK}")

    # 'wider_exit' 추정: TP 레벨을 더 넓게, trailing 완화
    sell_rules = config.setdefault("sell_rules", {})
    tp = sell_rules.setdefault("take_profit", {})
    tp["levels"] = [15, 30, 60]
    tp["ratios"] = [30, 40, 30]
    stop = sell_rules.setdefault("stop_loss", {})
    stop["trailing_stop_activation_profit_pct"] = 15
    stop["trailing_stop_pct"] = -15
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] patched config: tp={tp['levels']}, trail_act={stop['trailing_stop_activation_profit_pct']}, trail_pct={stop['trailing_stop_pct']}")


def restore_config() -> None:
    shutil.copy2(CONFIG_BAK, CONFIG_PATH)
    print(f"[OK] restored config from {CONFIG_BAK}")


def run_report() -> int:
    env = os.environ.copy()
    env["REPORT_RESEARCH_MODE"] = "1"
    env["REPORT_RESEARCH_PARAMS_PATH"] = str(PARAM_PATH)
    env["REPORT_RESEARCH_OUTPUT_DIR"] = str(OUT_DIR)
    env["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(CONTRACT_PATH)
    env["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    cmd = [sys.executable, str(ROOT / "report_backtest_v41_1.py")]
    print(f"[RUN] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env, text=True, capture_output=True)
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    return proc.returncode


def main() -> int:
    make_params()
    make_contract()
    backup_and_patch_config()
    try:
        rc = run_report()
    finally:
        restore_config()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
