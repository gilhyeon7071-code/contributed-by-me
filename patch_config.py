# -*- coding: utf-8 -*-
"""[무력화됨 2026-09-08] paper_engine_config.json 을 **직접 덮어쓰던** V5 패치 스크립트.

무엇이 문제였나
    이 스크립트는 `paper/paper_engine_config.json` 을 열어 그대로 다시 썼다.
    그 파일에는 `paper_engine_config.lock.json`(승인 해시)이 붙어 있다.
    직접 쓰면 승인 해시가 어긋나고 변경 이력(change log)도 남지 않는다.
    -> **잠금 계약 우회.** OBJECTIVE_LEDGER 의 O5 작업목록 3번.

지금 실행하면 무슨 일이 벌어지나 (2026-09-08 실측)
    의도한 값 중 대부분은 이미 설정에 반영돼 있는데 하나가 **어긋나 있다.**
        split_entry.first_ratio   현재 0.3   이 스크립트의 값 0.8
    즉 지금 돌리면 나중에 내려간 값을 조용히 0.8 로 되돌리고 잠금까지 깨뜨린다.
    그리고 `drawdown_manager.stages[].mdd_threshold` 4개는 **엔진이 읽지 않는 죽은 필드**다
    (GATE_THRESHOLD_INVENTORY.md §5). 거기 값을 써봐야 동작은 바뀌지 않는다.

휴면 확인 (2026-09-08)
    *.bat / *.ps1 / *.vbs / tools/*.py / tests/ 어디에서도 호출하지 않는다.
    참조는 문서 2개(GATE_THRESHOLD_INVENTORY.md, OBJECTIVE_LEDGER.md)뿐이고
    둘 다 **결함으로 기록한 것**이다.

그래서 지우지 않고 막았다
    지우면 "그런 절차가 있었다"는 사실까지 사라진다. 의도했던 값 목록은 아래에 그대로 남긴다.
    실행하면 쓰지 않고 멈추며, 같은 일을 하는 **정식 명령**을 출력한다.

정식 경로
    python tools/paper_engine_config_lock.py status
    python tools/paper_engine_config_lock.py set --set <dotted.key>=<json>
      -> 백업 + 변경로그 + 승인 해시 갱신을 함께 처리한다
"""
from __future__ import annotations

import json

CFG = "E:/1_Data/paper/paper_engine_config.json"

# 이 스크립트가 원래 쓰던 값들. **실행되지 않는다.** 기록으로 남긴다.
INTENDED = [
    ("production_risk_playbook.drawdown_role", "advisory"),
    ("entry_selection_policy.skip_same_code_day_already_buy", False),
    # 아래 4개는 엔진이 읽지 않는 죽은 필드다 (stages[].mdd 가 실효값)
    ("drawdown_manager.stages.0.mdd_threshold", 0.25),
    ("drawdown_manager.stages.1.mdd_threshold", 0.35),
    ("drawdown_manager.stages.2.mdd_threshold", 0.40),
    ("drawdown_manager.stages.3.mdd_threshold", 0.45),
    ("split_entry.first_ratio", 0.8),                      # 현재 0.3 - 되돌리면 안 된다
    ("risk_orchestration.c_default", 0.5),
    ("sell_rules.stop_loss.default_pct", -12.0),
    ("sell_rules.stop_loss.preemptive_close_pct", -8.0),
    ("sell_rules.stop_loss.asset_type_overrides.GROWTH", -12.0),
    ("sell_rules.stop_loss.asset_type_overrides.VALUE", -12.0),
    ("sell_rules.stop_loss.asset_type_overrides.THEME", -10.0),
    ("surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.first_ratio", 0.6),
    ("surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.first_ratio", 0.5),
    ("surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.first_ratio", 0.8),
    ("surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.first_ratio", 0.7),
    ("surge_exit_policy.stop_loss_pct", -0.08),
    ("surge_entry_policy.type_policy.type_overrides.PRICE_VOL_BREAKOUT.stop_loss_pct", -0.08),
    ("surge_entry_policy.type_policy.type_overrides.PRICE_RANGE_BREAKOUT.stop_loss_pct", -0.07),
    ("surge_entry_policy.type_policy.type_overrides.REG_SHORT_5D60.stop_loss_pct", -0.10),
    ("surge_entry_policy.type_policy.type_overrides.REG_MID_15D100.stop_loss_pct", -0.08),
    ("adaptive_entry_control.reduce_soft", 0.8),
    ("adaptive_entry_control.reduce_mid", 0.6),
    ("adaptive_entry_control.reduce_hard", 0.4),
    ("risk_orchestration.dd_taper.dd_stop", 0.25),
    ("crash_risk_off.mode", "REDUCE"),
    ("crash_risk_off.reduce_factor", 0.5),
]


def main() -> int:
    print("[STOP] 이 스크립트는 2026-09-08 에 무력화됐다. 설정을 쓰지 않는다.")
    print("       이유: %s 에 잠금(paper_engine_config.lock.json)이 붙어 있고," % CFG)
    print("             직접 쓰면 승인 해시가 어긋나며 변경 이력이 남지 않는다.")
    print()
    print("[주의] split_entry.first_ratio 가 현재 설정(0.3)과 다르다(이 스크립트 0.8).")
    print("       그대로 돌리면 나중에 내려간 값을 조용히 되돌린다.")
    print("       drawdown_manager.stages[].mdd_threshold 4개는 엔진이 읽지 않는 죽은 필드다.")
    print()
    print("[정식 경로] 같은 변경을 하려면 잠금 도구를 쓴다.")
    print("  python tools/paper_engine_config_lock.py status")
    for key, val in INTENDED:
        print("  python tools/paper_engine_config_lock.py set --set %s=%s" % (key, json.dumps(val)))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
