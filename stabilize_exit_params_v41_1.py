# -*- coding: utf-8 -*-
"""
Exit 정책 안정화(강제) - stop_loss 부호/범위 교정 포함

문제
- 출력이 stop_loss=0.02 로 나오면, 손절이 "손실"이 아니라 "이익"처럼 해석되어
  시뮬레이터가 비정상 동작할 수 있습니다. (손절은 음수여야 함)

정책
- take_profit, trail_pct: OFF(None) 강제
- stop_loss:
  - None 이거나, 0 이상이면 -> -0.05로 강제
  - 너무 큰 손절(예: -0.30 미만)이면 -> -0.30으로 캡(안전장치)

실행:
  python E:\1_Data\stabilize_exit_params_v41_1.py
"""

import json
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
STABLE = RC_DIR / "stable_params_v41_1.json"
BEST = RC_DIR / "best_params_v41_1.json"

DEFAULT_SL = -0.05
MIN_SL_CAP = -0.30  # 손절 하한(너무 과도한 값 방지)

def _backup(path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak_{ts}")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak

def _fix_stop_loss(val):
    if val is None:
        return DEFAULT_SL
    try:
        v = float(val)
    except Exception:
        return DEFAULT_SL
    # 손절은 음수여야 함
    if v >= 0:
        return DEFAULT_SL
    # 과도한 손절 캡
    if v < MIN_SL_CAP:
        return MIN_SL_CAP
    return v

def _patch(p: dict) -> dict:
    # TP/TRAIL OFF
    p["take_profit"] = None
    p["trail_pct"] = None

    # stop_loss 교정
    p["stop_loss"] = _fix_stop_loss(p.get("stop_loss"))

    # 사람이 보는 메타
    p["exit_policy"] = {
        "take_profit": None,
        "trail_pct": None,
        "stop_loss": p["stop_loss"],
        "note": "TP/TRAIL forced OFF; stop_loss forced negative with safe bounds"
    }
    return p

def patch_one(path: Path) -> None:
    if not path.exists():
        return
    _backup(path)
    p = json.loads(path.read_text(encoding="utf-8"))
    p = _patch(p)
    path.write_text(json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

    v = json.loads(path.read_text(encoding="utf-8"))
    print(f"[OK] stabilized: {path}")
    print(f"     take_profit={v.get('take_profit')} trail_pct={v.get('trail_pct')} stop_loss={v.get('stop_loss')}")

def main() -> int:
    RC_DIR.mkdir(parents=True, exist_ok=True)
    if not STABLE.exists():
        print(f"[SKIP] missing: {STABLE}")
        return 0
    patch_one(STABLE)
    patch_one(BEST)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
