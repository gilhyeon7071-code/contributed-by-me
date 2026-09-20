# -*- coding: utf-8 -*-
"""STOC v41.1 - Parameter Optimizer (stability-oriented).

Purpose:
- Validate v41.1 candidate selection parameters against historical KRX daily
  parquet data and write best/stable parameter artifacts.

Outputs:
- E:/1_Data/12_Risk_Controlled/best_params_v41_1.json
- E:/1_Data/12_Risk_Controlled/stable_params_v41_1.json
- E:/1_Data/12_Risk_Controlled/search_report_v41_1.csv

Notes:
- Exit/risk parameters are frozen by default for stability.
- The search scope is entry filters and ranking weights unless a key is
  explicitly listed in BOUNDS.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

from utils.price_history_contract import apply_price_history_contract
from utils.stable_params_gate import evaluate_stable_params, load_stable_quality_gate, compute_provenance_metadata
from generate_candidates_v41_1 import _relax_ladder, _risk_unit_linear
from generate_candidates_v41_1 import (_apply_sector_prefilter_union, _compute_rally_breadth_stats)
from strategy_core import select_candidates_core

# --------------------
# Paths and constants
# --------------------
BASE_DIR = Path(os.environ.get("STOC_BASE_DIR") or str(Path(__file__).resolve().parent))
OUT_DIR = BASE_DIR / "12_Risk_Controlled"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PAPER_ENGINE_CONFIG = BASE_DIR / "paper" / "paper_engine_config.json"
SYMBOL_PANEL_CSV = BASE_DIR / "2_Logs" / "backtest_symbol_panel_latest.csv"

# Data range note: use enough years to cover IS/VAL/OOS windows.
# -----------------------------------------------------------------------------
# Price cache (code -> slice in sorted arrays)
# -----------------------------------------------------------------------------
# [PERF 2026-07-25] Per-window price caches keyed by (w_start, w_end). The cache
# content depends only on the window df (param-independent), so it is built once
# per unique window and reused across all HPO combos instead of being rebuilt
# 7*N_ITER times. Correctness-preserving: identical window -> identical cache.
_WINDOW_CACHE: dict = {}

def _build_price_cache(df: pd.DataFrame) -> dict:
    """Build a memory-efficient cache for fast per-code OHLC access.

    The cache keeps one set of large arrays sorted by (code, date) and a mapping
    code -> (start, end) slice indices into those arrays.
    """
    need = ["code", "price_history_key", "date", "open", "high", "low", "close"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns for price cache: {missing}")

    tmp = df[need].copy()
    tmp["code"] = tmp["code"].astype(str)
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
    tmp = tmp.dropna(subset=["code", "date", "open", "high", "low", "close"])
    if tmp.empty:
        raise ValueError("price cache build: empty after dropna")

    code = tmp["price_history_key"].astype(str).to_numpy()
    date = tmp["date"].to_numpy(dtype="datetime64[ns]")
    o = tmp["open"].to_numpy(dtype=float)
    h = tmp["high"].to_numpy(dtype=float)
    l = tmp["low"].to_numpy(dtype=float)
    c = tmp["close"].to_numpy(dtype=float)

    # sort by code then date
    order = np.lexsort((date, code))
    code_s = code[order]
    date_s = date[order]
    o_s = o[order]
    h_s = h[order]
    l_s = l[order]
    c_s = c[order]

    # boundaries where code changes
    if len(code_s) == 0:
        raise ValueError("price cache build: empty after sort")
    cut = np.flatnonzero(code_s[1:] != code_s[:-1]) + 1
    bounds = np.concatenate(([0], cut, [len(code_s)]))
    slices = {code_s[bounds[i]]: (int(bounds[i]), int(bounds[i + 1])) for i in range(len(bounds) - 1)}

    return {
        "code_s": code_s,
        "date_s": date_s,
        "open_s": o_s,
        "high_s": h_s,
        "low_s": l_s,
        "close_s": c_s,
        "slices": slices,
    }

YEARS_BACK = 10

# Search iterations.
N_ITER = 40

# NOTE: COST/SLIPPAGE below were defined but never referenced anywhere in this
# file (dead constants) -- removed 2026-07-24 to avoid implying they applied.
# Only DEFAULT_FEE (below) affects the simulated P&L.

# Minimum trade-count guards.
MIN_TRADES_TOTAL = 120
MIN_TRADES_PER_WINDOW = 15
MIN_ACTIVE_DAYS = 15  # minimum trades per fold for trade-level metric

# Portfolio evaluation constants.
CAPITAL_TOTAL = 100_000_000.0  # fixed denominator for daily portfolio returns


def _load_regime_gross_exposure() -> dict[str, float]:
    """Read regime-specific gross exposure from the same config the live engine uses."""
    cfg_path = BASE_DIR / "paper" / "paper_engine_config.json"
    fallback = {
        "RALLY": 0.60,
        "NORMAL": 0.55,
        "BEAR": 0.35,
        "CRASH": 0.10,
        "BULL": 0.55,
    }
    if not cfg_path.exists():
        return fallback
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return fallback
    out = dict(fallback)
    overrides = cfg.get("regime_overrides") if isinstance(cfg.get("regime_overrides"), dict) else {}
    for regime in ("RALLY", "NORMAL", "BEAR", "CRASH"):
        sub = overrides.get(regime) if isinstance(overrides.get(regime), dict) else {}
        cap = sub.get("capital_budget_policy") if isinstance(sub.get("capital_budget_policy"), dict) else {}
        v = cap.get("gross_exposure_pct")
        if v is None:
            v = sub.get("max_gross_exposure_pct")
        if v is not None:
            try:
                out[regime] = float(v)
            except Exception:
                pass
    return out


REGIME_GROSS_EXPOSURE = _load_regime_gross_exposure()

# Stability penalty.
PF_STD_PENALTY = 0.35

# Time-aware HPO selection policy. These values do not lift any score/state;
# they block candidates whose weakest time fold is below the existing OOS gate.
HPO_MIN_FOLDS = 3
HPO_WORST_FOLD_PF_HURDLE = 0.75
HPO_RECENT_WEIGHT_HALF_LIFE = 2.0

# Stable promotion policy.
PROMOTION_MARGIN = 0.05  # Promote only when best improves over stable by 5%.
# [2026-08-15] Floor for the promotion margin, in score units. The margin is
# applied as `stable + max(PROMOTION_MARGIN*|stable|, PROMOTION_MARGIN_MIN_ABS)`
# instead of the old `stable * (1 + PROMOTION_MARGIN)`. The multiplicative form
# inverted for negative scores -- at stable=-0.50 the bar became -0.525, so a
# WORSE candidate was promoted -- and collapsed to no margin at all near zero.
PROMOTION_MARGIN_MIN_ABS = 0.02
STEP_TOWARD_ALPHA = 1.0  # Validation run: use best directly as stable.

# Search parameter bounds (entry filters plus weights).
BOUNDS = {
    "rs_lim": (-0.10, 1.00, 0.01),
    "v_accel_lim": (1.00, 8.00, 0.10),
    "stretch_max": (1.05, 1.30, 0.01),
    "value_min": (10e9, 200e9, 5e9),
    "near_52w_high_gap_max": (0.05, 0.25, 0.05),  # allow up to 25% below 52w high
    "atr_max": (0.03, 0.25, 0.005),
    "gap_limit": (0.00, 0.25, 0.01),
    "max_pos": (4, 16, 1),  # 옵티마이저 탐색 추가 (이전: FROZEN)
    "defense_bear_rs_slope_min": (-0.02, 0.03, 0.005),  # 2022 defense param
    "stop_loss": (-0.12, -0.04, 0.01),  # allow HPO to widen stop loss
    # rule_e filters frozen for this validation run
    # "defense_bear_disable_entry": (0.0, 1.0, 1.0),
    # "mkt_ret20_min": (-0.05, 0.10, 0.01),
    # "mkt_ret60_min": (-0.10, 0.10, 0.01),
    # "sector_rs_min": (-0.10, 0.20, 0.01),
}

# Frozen parameters: not searched directly.
FROZEN_KEYS = ["take_profit", "trail_pct", "hold"]  # stop_loss 제거 → BOUNDS로 이동

DEFAULT_FROZEN = {
    "take_profit": 0.15,
    "trail_pct": 0.08,
    "hold": 10,
}

# Trading fee fallback -- round-trip cost.
#
# [2026-09-09] 0.00358 -> 0.00400. **측정이 정했다. 선택이 아니다.**
#   0.00358 은 2026-07-23 설정(fee 0.00004 x2 + slip 0.001 x2 + tax 0.0015)의 역산이었다.
#   2026-08-24 에 실계좌에 맞춰 fee 0.0 / tax 0.002 로 교정됐는데(PLANS (261)),
#   이 상수만 남아 **시뮬레이션이 원장보다 왕복 0.042%p 싸게 계산**하고 있었다.
#   브로커 실측(tr_id TTTC8715R, 실화폐 매도 3건):
#     매도대금 88,730 / 수수료 0 / 제세금 175  -> 0.19723%
#     건별 재현 0.002 -> 176 (브로커 175 와 일치) / 0.0015 -> 132 (43 차이)
#   현행 라이브 설정 왕복 = 0.0 x2 + 0.001 x2 + 0.002 = **0.00400**
#
#   영향: 이 상수로 나온 과거 최적화 결과는 이제 더 비싼 세계에서 재평가된다.
#         기존 stable_params 의 점수·PF 와 직접 비교할 수 없다. **보고할 사실이지
#         고치지 말아야 할 이유가 아니다** - 틀린 비용으로 맞춘 기준선이 더 나쁘다.
DEFAULT_FEE = 0.00400

# 다시 벌어지지 않게 묶는다. 상수는 그대로 두되(시뮬 재현성) 라이브 설정과
# 어긋나면 경고를 낸다. 08-24 변경이 조용히 16일간 유지된 것이 이 장치가 없어서였다.
def _warn_if_fee_diverges_from_live() -> None:
    try:
        import json as _json
        cfg_p = BASE_DIR / "paper" / "paper_engine_config.json"
        if not cfg_p.exists():
            return
        c = _json.loads(cfg_p.read_text(encoding="utf-8-sig"))

        # 설정 파일이 **말하는** 값
        stated = (float(c.get("fee_pct", 0.0) or 0.0) * 2.0
                  + float(c.get("slippage_pct", 0.0) or 0.0) * 2.0
                  + float(c.get("sell_tax_pct", 0.0) or 0.0))

        # [2026-09-10] 엔진이 **실제로 쓰는** 값. 이 줄이 없어서 C7 을 놓쳤다.
        #   설정에 fee_pct: 0.0 이 정확히 들어 있는데 build_cost_profile 의
        #   `or 0.005` 가 그걸 삼켜, 엔진은 왕복 1.400% 를 청구하고 있었다.
        #   이 함수는 설정 파일만 읽어 0.400% 를 보고 "일치" 라고 판정했다.
        #   **감시는 설정값이 아니라 실효값을 봐야 한다.**
        effective = None
        try:
            import sys as _sys
            if str(BASE_DIR) not in _sys.path:
                _sys.path.insert(0, str(BASE_DIR))
            from pricing_engine import build_cost_profile as _bcp
            _cp = _bcp(c)
            effective = (float(_cp.fee_pct) * 2.0
                         + float(_cp.slippage_pct) * 2.0
                         + float(_cp.sell_tax_pct))
        except Exception as _pe:
            print("[COST_DRIFT] 실효값 계산 실패(설정값만 비교함): %s: %s"
                  % (type(_pe).__name__, _pe))

        live = stated if effective is None else effective

        # (1) 설정이 말하는 값과 엔진이 쓰는 값이 다르면 - 접근자가 값을 삼킨 것이다
        if effective is not None and abs(effective - stated) > 1e-9:
            print("[COST_DRIFT] **설정 왕복=%.5f 인데 엔진 실효=%.5f 다.** "
                  "설정 파일이 아니라 접근자(pricing_engine.build_cost_profile)가 값을 바꾸고 있다 "
                  "-> 설정을 고쳐도 안 먹는다" % (stated, effective))

        # (2) 시뮬 상수와 라이브가 다르면 - 원래 목적
        if abs(live - DEFAULT_FEE) > 1e-6:
            print("[COST_DRIFT] DEFAULT_FEE=%.5f 인데 라이브 실효 왕복=%.5f 다. "
                  "시뮬과 원장이 다른 비용을 본다 -> 둘을 맞춰라" % (DEFAULT_FEE, live))
    except Exception as _e:
        print("[COST_DRIFT] 확인 실패: %s: %s" % (type(_e).__name__, _e))


_warn_if_fee_diverges_from_live()


# --- IS/VAL/OOS split policy (for selection scoring) ---
SPLIT_POLICY_PATH = BASE_DIR / '12_Risk_Controlled' / 'split_policy_v41_1.json'
DEFAULT_TRAIN_END = datetime(2023, 12, 31)
DEFAULT_VAL_END   = datetime(2024, 12, 31)

def _load_split_policy():
    train_end = DEFAULT_TRAIN_END
    val_end = DEFAULT_VAL_END
    try:
        if SPLIT_POLICY_PATH.exists():
            j = json.load(open(SPLIT_POLICY_PATH, 'r', encoding='utf-8'))
            te = j.get('train_end')
            ve = j.get('val_end')
            if te:
                train_end = datetime.strptime(str(te)[:10], '%Y-%m-%d')
            if ve:
                val_end = datetime.strptime(str(ve)[:10], '%Y-%m-%d')
    except Exception as exc:
        # [2026-08-15] Announce the fallback. These two dates decide which folds
        # are IS/VAL and which are reserved as the OOS holdout, so silently
        # reverting to the defaults changes what "out-of-sample" means.
        print(
            f"[WARN] split policy unreadable ({type(exc).__name__}: {exc}); "
            f"using defaults train_end={DEFAULT_TRAIN_END:%Y-%m-%d} "
            f"val_end={DEFAULT_VAL_END:%Y-%m-%d}. path={SPLIT_POLICY_PATH}"
        )
    return train_end, val_end

TRAIN_END, VAL_END = _load_split_policy()


# ?쒕뜡 ?먯깋 ?쒕뱶: ?ы쁽???뺣낫(遺덉븞??媛먯냼)
RNG_SEED = 42


# ============================================================================
# [SYNC 2026-07-27] Production behaviour is read from the SAME config the live
# engine uses, so the simulator cannot silently diverge again.
#   - entry timing  : paper_engine_config.json -> entry_timing_mode
#   - exit ladder   : paper_engine_config.json -> sell_rules.take_profit / stop_loss
# Research overrides: set the module globals directly after import.
# ============================================================================
def _load_production_exec_policy() -> dict:
    """Read live entry/exit policy. Falls back to the pre-sync behaviour."""
    cfg_path = BASE_DIR / "paper" / "paper_engine_config.json"
    out = {"entry_mode": "next_open", "tp_plan": [], "trail_pct": None, "trail_activation": 0.12}
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        # [2026-08-15] Do not fail over in silence. The whole point of this
        # loader is that the simulator cannot diverge from production; falling
        # back to next_open with no TP ladder IS a divergence, and a quiet one
        # produced a certified-but-invalid stable once already.
        print(
            f"[WARN] production exec policy unreadable ({type(exc).__name__}: {exc}); "
            f"falling back to entry={out['entry_mode']} tp=[] trail=None. "
            f"path={cfg_path}"
        )
        return out
    mode = str(cfg.get("entry_timing_mode") or "").strip().lower()
    if mode in ("same_close", "next_open"):
        out["entry_mode"] = mode
    sr = cfg.get("sell_rules") if isinstance(cfg.get("sell_rules"), dict) else {}
    tp = sr.get("take_profit") if isinstance(sr.get("take_profit"), dict) else {}
    st = sr.get("stop_loss") if isinstance(sr.get("stop_loss"), dict) else {}
    lv = [float(x) / 100.0 for x in (tp.get("levels") or []) if x is not None]
    rt = [float(x) / 100.0 for x in (tp.get("ratios") or []) if x is not None]
    if lv and rt:
        m = min(len(lv), len(rt))
        ssum = sum(rt[:m])
        if ssum > 0:
            out["tp_plan"] = sorted([(lv[i], rt[i] / ssum) for i in range(m)], key=lambda z: z[0])
    if "trailing_stop_pct" in st:
        try:
            out["trail_pct"] = float(st["trailing_stop_pct"]) / 100.0
        except Exception:
            pass
    if "trailing_stop_activation_profit_pct" in st:
        try:
            out["trail_activation"] = float(st["trailing_stop_activation_profit_pct"]) / 100.0
        except Exception:
            pass
    return out


_PROD_EXEC = _load_production_exec_policy()
_ENTRY_MODE = _PROD_EXEC["entry_mode"]          # next_open | same_close
_PTP_PLAN = _PROD_EXEC["tp_plan"]               # [] disables the ladder
_PTP_TRAIL_PCT = _PROD_EXEC["trail_pct"]
_PTP_TRAIL_ACTIVATION = _PROD_EXEC["trail_activation"]
_PROD_CAND = True                               # replicate production candidate mechanisms
print(f"[SYNC] production exec policy: entry={_ENTRY_MODE} tp_levels={[round(x[0],3) for x in _PTP_PLAN]} "
      f"trail={_PTP_TRAIL_PCT} act={_PTP_TRAIL_ACTIVATION} prod_cand={_PROD_CAND}")


# --------------------
# Helpers
# --------------------

def _jload(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _jsave(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _persist_promoted_stable(path: Path, obj: dict, promoted: bool) -> bool:
    """Write the approved stable artifact only after an actual promotion."""
    if not promoted:
        return False
    _jsave(path, obj)
    return True


def _load_paper_gap_policy() -> tuple[float, float]:
    if not PAPER_ENGINE_CONFIG.exists():
        return 0.0, 0.0
    try:
        j = _jload(PAPER_ENGINE_CONFIG)
        gu = _safe_float(j.get("gap_up_max_pct", 0.0), 0.0)
        gd = _safe_float(j.get("entry_gap_down_stop_pct", 0.0), 0.0)
        return float(gu), float(gd)
    except Exception:
        return 0.0, 0.0


GAP_POLICY_KEYS = ("gap_up_max_pct", "entry_gap_down_stop_pct")


def _norm_gap_value(v, default: float = 0.0) -> float:
    x = _safe_float(v, default)
    if not np.isfinite(x):
        return float(default)
    return max(0.0, float(x))


def _apply_gap_policy_schema(obj: dict, fallback: dict | None = None) -> dict:
    fb = fallback or {}
    out = dict(obj)
    out["gap_up_max_pct"] = _norm_gap_value(out.get("gap_up_max_pct", fb.get("gap_up_max_pct", 0.0)))
    out["entry_gap_down_stop_pct"] = _norm_gap_value(out.get("entry_gap_down_stop_pct", fb.get("entry_gap_down_stop_pct", 0.0)))
    return out


def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def _round_grid(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    return round(round(x / step) * step, 10)


def _clip_grid(x: float, lo: float, hi: float, step: float) -> float:
    x = max(lo, min(hi, float(x)))
    return _round_grid(x, step)


def _safe_float(v, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


@dataclass
class WindowResult:
    """Per-window evaluation result.

    Field name is **n_trades** (not n). Some older scripts used `n`, but this
    optimizer uses `n_trades` everywhere (reporting, penalties, splits).
    We keep a read-only alias property `n` for backward compatibility.
    """

    start: str
    end: str
    n_trades: int
    pf: float
    mean_ret: float
    split: str
    year: int
    daily_rets: List[float] = field(default_factory=list)
    avg_exposure_pct: float = 0.0

    @property
    def n(self) -> int:
        return int(self.n_trades)


def _recent_weighted(values: List[float], half_life: float = HPO_RECENT_WEIGHT_HALF_LIFE) -> float:
    clean = [float(v) for v in values if np.isfinite(float(v))]
    if not clean:
        return 0.0
    hl = max(float(half_life), 1e-9)
    n = len(clean)
    weights = np.asarray([0.5 ** ((n - 1 - i) / hl) for i in range(n)], dtype=float)
    vals = np.asarray(clean, dtype=float)
    return float(np.average(vals, weights=weights))


def _trade_pf(trade_rets: List[float]) -> float:
    """Profit factor computed from per-trade final returns.

    PF = sum(positive trade returns) / sum(abs(negative trade returns)).
    This aligns the optimizer with report_backtest_v41_1.py's trade-level
    metric instead of the daily portfolio return approximation.
    """
    gross_profit = sum(float(r) for r in trade_rets if float(r) > 0.0)
    gross_loss = sum(abs(float(r)) for r in trade_rets if float(r) < 0.0)
    if gross_loss <= 0.0:
        return 1.0 if gross_profit > 0.0 else 0.0
    return float(gross_profit / gross_loss)


def _count_active_days(daily_rets: List[float], eps: float = 1e-12) -> int:
    # Trade-level mode: active days proxy kept for fold-selection compatibility.
    return int(sum(1 for r in daily_rets if abs(float(r)) > eps))


def _fold_selection_metrics(results: List[WindowResult], base_score: float) -> dict:
    ordered = sorted(results, key=lambda r: (str(r.end), int(r.year)))
    excluded_low_trade = [
        r for r in ordered
        if int(r.n_trades) < int(MIN_TRADES_PER_WINDOW)
    ]
    excluded_low_active = [
        r for r in ordered
        if int(r.n_trades) >= int(MIN_TRADES_PER_WINDOW)
        and int(r.n_trades) < int(MIN_ACTIVE_DAYS)
    ]
    eligible = [
        r for r in ordered
        if int(r.n_trades) >= int(MIN_TRADES_PER_WINDOW)
        and int(r.n_trades) >= int(MIN_ACTIVE_DAYS)
        and np.isfinite(float(r.pf))
    ]
    # Reserve EVERY OOS fold as a true holdout. OOS folds stay in `windows` and
    # are scored by utils.stable_params_gate (min_oos_pf), but they must not
    # influence HPO selection (avg / recent / worst).
    #
    # Slicing off only the last fold is not sufficient: VAL_END=2024-12-31 with
    # 1-year rolling windows produces TWO OOS folds, so `eligible[:-1]` still let
    # the optimizer tune directly on the earlier one -- and that fold supplied
    # ~43% of the gate's n_trades-weighted oos_pf, keeping the circular argument
    # alive. Partitioning on `split` instead of on position also makes the
    # reserved set independent of which folds happen to clear
    # MIN_TRADES_PER_WINDOW for a given combo.
    holdout_rows = [r for r in eligible if str(r.split).strip().upper() == "OOS"]
    eligible_for_selection = [r for r in eligible if str(r.split).strip().upper() != "OOS"]
    pfs = [float(r.pf) for r in eligible_for_selection]
    n_folds = int(len(pfs))
    avg_pf = float(np.mean(pfs)) if pfs else 0.0
    worst_row = min(eligible_for_selection, key=lambda r: float(r.pf)) if eligible_for_selection else None
    worst_pf = float(worst_row.pf) if worst_row is not None else 0.0
    recent_pf = _recent_weighted(pfs)
    worst_pass = bool(n_folds >= HPO_MIN_FOLDS and worst_pf >= HPO_WORST_FOLD_PF_HURDLE)

    if n_folds < HPO_MIN_FOLDS:
        hypertime_score = -1e9 + float(n_folds)
        reason = "insufficient_folds"
    elif not worst_pass:
        hypertime_score = -1e8 + worst_pf
        reason = "worst_fold_below_hurdle"
    else:
        hypertime_score = float(base_score) + 0.01 * recent_pf
        reason = "pass"

    return {
        "avg": avg_pf,
        "avg_pf": avg_pf,
        "worst_fold": worst_pf,
        "worst_fold_pf": worst_pf,
        "recent_weighted": recent_pf,
        "recent_weighted_pf": recent_pf,
        "n_folds": n_folds,
        "n_folds_total": int(len(ordered)),
        # Holdout bookkeeping: folds reserved out of HPO selection and left to
        # the stable quality gate. n_folds_holdout==0 means NO out-of-sample
        # evidence was reserved for this run -- do not read the gate's oos_pf as
        # out-of-sample in that case.
        "holdout_split": "OOS",
        "n_folds_holdout": int(len(holdout_rows)),
        "holdout_windows": [
            {
                "start": str(r.start),
                "end": str(r.end),
                "n_trades": int(r.n_trades),
                "pf": float(r.pf),
                "split": str(r.split),
            }
            for r in holdout_rows
        ],
        "excluded_low_trade_folds": int(len(excluded_low_trade)),
        "excluded_low_active_folds": int(len(excluded_low_active)),
        "min_trades_per_fold": int(MIN_TRADES_PER_WINDOW),
        "min_active_days": int(MIN_ACTIVE_DAYS),
        "worst_fold_window": (
            {
                "start": str(worst_row.start),
                "end": str(worst_row.end),
                "n_trades": int(worst_row.n_trades),
                "pf": float(worst_row.pf),
                "split": str(worst_row.split),
                "year": int(worst_row.year),
            }
            if worst_row is not None
            else None
        ),
        "worst_fold_hurdle": float(HPO_WORST_FOLD_PF_HURDLE),
        "min_folds": int(HPO_MIN_FOLDS),
        "worst_fold_pass": worst_pass,
        "hypertime_score": float(hypertime_score),
        "hypertime_reason": reason,
    }


# --------------------
# Data load and factors
# --------------------

def _bounded_krx_glob(base_dir: Path, pattern: str) -> List[Path]:
    """Bounded, non-recursive KRX parquet discovery.

    Mirrors p0_daily_check.py:_krx_clean_files() so HPO/backtest scripts see
    the same canonical source set instead of an unrestricted base_dir.rglob()
    that also sweeps in backup/, tmp/, and other unrelated subtrees.
    """
    out: List[Path] = []
    seen: set[str] = set()
    for d, _prio in _krx_source_dirs(base_dir):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob(pattern):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return sorted(out)


# [2026-08-29] R1 배선. generate_candidates_v41_1.py 와 동일한 결함이 여기에도
#   그대로 복사돼 있었다: 탐색 경로에 Raw 가 없고, `_clean` 을 찾으면 거기서 반환해
#   `_clean` 접미사가 없는 Raw 파일은 영원히 안 읽힌다.
#   생산만 고치고 옵티마이저를 놔두면 HPO 는 계속 결손 패널로 돈다.
#   Raw 는 최하위 우선순위 -> 겹치는 (code,date) 는 종전 값 유지, 빈 날짜만 채운다.
def _krx_source_dirs(base_dir: Path) -> List[tuple]:
    return [
        (base_dir / "Raw", 0),
        (base_dir, 1),
        (base_dir / "krx_daily_archive", 2),
        (base_dir / "_krx_manual", 3),
    ]


def _krx_src_priority(base_dir: Path, path: Path) -> int:
    for d, prio in _krx_source_dirs(base_dir):
        if path.parent == d:
            return prio
    return 0


def find_parquets(base_dir: Path) -> List[Path]:
    clean = _bounded_krx_glob(base_dir, "krx_daily_*_clean.parquet")
    if not clean:
        return _bounded_krx_glob(base_dir, "krx_daily_*.parquet")
    seen = {str(p.resolve()) for p in clean}
    raw_dir = base_dir / "Raw"
    for p in _bounded_krx_glob(base_dir, "krx_daily_*.parquet"):
        if p.parent != raw_dir:
            continue
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            clean.append(p)
    return sorted(clean)


def load_data(base_dir: Path) -> pd.DataFrame:
    files = find_parquets(base_dir)
    if not files:
        raise FileNotFoundError(f"KRX parquet ?뚯씪???놁뒿?덈떎: {base_dir}")

    dfs = []
    for p in files:
        df = pd.read_parquet(p)
        df["_src_priority"] = _krx_src_priority(base_dir, p)
        try:
            df["_src_mtime"] = p.stat().st_mtime
        except OSError:
            df["_src_mtime"] = 0.0
        dfs.append(df)

    sanitized: List[pd.DataFrame] = []
    for frame in dfs:
        if not isinstance(frame, pd.DataFrame):
            continue
        if frame.empty:
            continue
        work = frame.dropna(axis=1, how="all")
        work = work.dropna(axis=0, how="all")
        if work.empty:
            continue
        # concat 전 파일별 날짜 정규화: ISO("2023-01-02")와 YYYYMMDD("20230102") 혼용 대응
        if "date" in work.columns:
            d = work["date"].astype(str).str.strip()
            # YYYYMMDD 8자리 숫자 → 하이픈 삽입 후 파싱
            is_compact = d.str.match(r"^\d{8}$")
            if is_compact.any():
                d = d.where(~is_compact, d.str[:4] + "-" + d.str[4:6] + "-" + d.str[6:8])
            work = work.copy()
            work["date"] = pd.to_datetime(d, errors="coerce")
        sanitized.append(work)

    if not sanitized:
        raise RuntimeError("parquet read succeeded but all files are empty")
    df = pd.concat(sanitized, ignore_index=True)

    # dtype normalize (날짜는 파일별 정규화 완료, 이중 변환 방지)
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["code"] = df["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df.loc[df[c] == 0, c] = np.nan

    need = ["date", "code", "open", "high", "low", "close", "value"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"?꾩닔 而щ읆 ?꾨씫: {missing}")

    df = df.dropna(subset=["date", "code", "open", "high", "low", "close", "value"]).copy()

    # Multiple source files (krx_daily_archive / _krx_manual) can carry the
    # same (code, date) row. Keep the row from the most recently written
    # source file so manual/patched corrections win over stale archive rows.
    df = df.sort_values(["_src_priority", "_src_mtime"]).drop_duplicates(subset=["code", "date"], keep="last")
    df = df.drop(columns=["_src_priority", "_src_mtime"])

    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    df, integrity = apply_price_history_contract(df)
    print(f"[PRICE_HISTORY_INTEGRITY] {integrity['log_line']}")

    # Merge symbol panel for sector / market-cap analysis and defensive filters.
    panel = _load_symbol_panel()
    if not panel.empty:
        before = len(df)
        df = df.merge(panel, on=["date", "code"], how="left")
        after = len(df)
        if before != after:
            print(f"[WARN] symbol_panel merge changed row count {before}->{after}; dropping duplicates")
            df = df.drop_duplicates(["date", "code"], keep="first")
        missing_sector = df.get("sector_code", pd.Series()).isna().mean()
        missing_mcap = df.get("market_cap", pd.Series()).isna().mean()
        print(f"[OPTIMIZER] merged symbol_panel rows={len(df)} sector_code_missing={missing_sector:.2%} market_cap_missing={missing_mcap:.2%}")

    return df


def _load_regime_dayret_thresholds() -> tuple[float, float]:
    """Read rally/crash day-return thresholds from live engine config (decimal)."""
    cfg_path = BASE_DIR / "paper" / "paper_engine_config.json"
    rally_min = 0.025
    crash_max = -0.025
    if not cfg_path.exists():
        return rally_min, crash_max
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        pol = cfg.get("regime_entry_policy") if isinstance(cfg.get("regime_entry_policy"), dict) else {}
        rally_min = float(pol.get("rally_day_ret_min", rally_min))
        crash_max = float(pol.get("crash_day_ret_max", crash_max))
    except Exception:
        pass
    return rally_min, crash_max


_RALLY_DAY_RET_MIN, _CRASH_DAY_RET_MAX = _load_regime_dayret_thresholds()


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    # Market proxy: use each row's KOSPI/KOSDAQ peer group when available,
    # with the old all-market average as a compatibility fallback.
    if "market" not in df.columns:
        df["market"] = ""
    df["market"] = df["market"].fillna("").astype(str).str.upper().str.strip()
    overall_idx = df.groupby("date", as_index=True)["close"].mean().sort_index()
    overall_ret20 = overall_idx.pct_change(20)
    overall_ret60 = overall_idx.pct_change(60)
    overall_bull = overall_idx > overall_idx.rolling(60).mean()
    overall_day_ret = overall_idx.pct_change(1)
    market_idx = (
        df[df["market"].ne("")]
        .groupby(["market", "date"], as_index=False)["close"]
        .mean()
        .sort_values(["market", "date"])
    )
    if not market_idx.empty:
        market_idx["m_ret20"] = market_idx.groupby("market")["close"].pct_change(20)
        market_idx["market_ma60"] = market_idx.groupby("market")["close"].transform(lambda x: x.rolling(60).mean())
        market_idx["market_is_bull"] = market_idx["close"] > market_idx["market_ma60"]
        df = df.merge(
            market_idx[["market", "date", "m_ret20", "market_is_bull"]],
            on=["market", "date"],
            how="left",
            sort=False,
        )
    else:
        df["m_ret20"] = np.nan
        df["market_is_bull"] = pd.NA
    df["m_ret20"] = df["m_ret20"].fillna(df["date"].map(overall_ret20))
    df["m_ret60"] = df["date"].map(overall_ret60)
    # Shared strategy_core expects production-style column names.
    df["mkt_ret20"] = df["m_ret20"]
    df["mkt_ret60"] = df["m_ret60"]
    df["market_is_bull"] = df["market_is_bull"].where(df["market_is_bull"].notna(), df["date"].map(overall_bull))
    df["market_day_ret"] = df["date"].map(overall_day_ret)

    def _classify_regime(row):
        bull = bool(row["market_is_bull"]) if pd.notna(row["market_is_bull"]) else True
        day_ret = float(row["market_day_ret"]) if pd.notna(row["market_day_ret"]) else 0.0
        if bull:
            if day_ret >= _RALLY_DAY_RET_MIN:
                return "RALLY"
            return "NORMAL"
        if day_ret <= _CRASH_DAY_RET_MAX:
            return "CRASH"
        return "BEAR"

    df["market_regime"] = df.apply(_classify_regime, axis=1)
    df["ret20"] = df.groupby("price_history_key", sort=False)["close"].pct_change(20)
    df["rs"] = df["ret20"] - df["m_ret20"]
    df["rs_slope"] = df.groupby("price_history_key", sort=False)["rs"].diff(5)

    # Sector-relative strength (research filter; sector_code comes from symbol panel)
    if "sector_code" in df.columns:
        df["sector_code"] = df["sector_code"].astype(str).str.strip()
        sector_idx = (
            df[df["sector_code"].ne("") & df["sector_code"].notna()]
            .groupby(["sector_code", "date"], as_index=False)["close"]
            .mean()
            .sort_values(["sector_code", "date"])
        )
        if not sector_idx.empty:
            sector_idx["sector_ret20"] = sector_idx.groupby("sector_code")["close"].pct_change(20)
            df = df.merge(
                sector_idx[["sector_code", "date", "sector_ret20"]],
                on=["sector_code", "date"],
                how="left",
                sort=False,
            )
            df["sector_rs"] = df["sector_ret20"] - df["m_ret20"]
        else:
            df["sector_ret20"] = np.nan
            df["sector_rs"] = np.nan
    else:
        df["sector_ret20"] = np.nan
        df["sector_rs"] = np.nan

    df["ma5"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(5).mean())
    df["stretch"] = df["close"] / (df["ma5"] + 1e-9)

    df["v_ma5"] = df.groupby("price_history_key", sort=False)["value"].transform(lambda x: x.rolling(5).mean())
    df["v_accel"] = df["value"] / (df.groupby("price_history_key", sort=False)["v_ma5"].shift(1) + 1e-9)

    # 2026-07-24: added for junk_risk simulation (price-derived pump-pattern component).
    df["ret1_pct"] = df.groupby("price_history_key", sort=False)["close"].pct_change(1) * 100.0

    # Auxiliary operational filters (aligned with candidate generator)
    delta = df.groupby("price_history_key", sort=False)["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.groupby(df["price_history_key"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(df["price_history_key"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs14 = avg_gain / (avg_loss + 1e-9)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs14))

    ema12 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("price_history_key", sort=False)["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    prev_macd = df.groupby("price_history_key", sort=False)["macd_line"].shift(1)
    prev_sig = df.groupby("price_history_key", sort=False)["macd_signal"].shift(1)
    df["macd_golden"] = (df["macd_line"] > df["macd_signal"]) & (prev_macd <= prev_sig)

    df["vol_close_corr20"] = (
        df.groupby("price_history_key", group_keys=False)[["close", "value"]]
        .apply(lambda g: g["close"].rolling(20, min_periods=20).corr(g["value"]))
        .reset_index(level=0, drop=True)
    )
    df["high_52w"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(252, min_periods=60).max())
    df["high_52w_gap"] = ((df["high_52w"] - df["close"]) / (df["high_52w"] + 1e-9)).clip(lower=0.0)
    df["ma200"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(200, min_periods=100).mean())
    first_date = df.groupby("code")["date"].transform("min")
    df["listing_days"] = (df["date"] - first_date).dt.days

    # ATR14% (robust)
    prev_close = df.groupby("price_history_key", sort=False)["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr14"] = tr.groupby(df["price_history_key"]).transform(lambda x: x.rolling(14).mean())
    df["atr14_pct"] = df["atr14"] / (df["close"] + 1e-9)

    # next-day open for entry; gap for backtest filter
    df["n_open"] = df.groupby("price_history_key", sort=False)["open"].shift(-1)
    df["gap_next"] = (df["n_open"] / (df["close"] + 1e-9) - 1.0).abs()

    return df


# --------------------
# Backtest helpers
# --------------------

def _score_day(df_day: pd.DataFrame, w: dict) -> pd.Series:
    # percentile ranks among passing candidates (same day)
    r_rs = df_day["rs"].rank(pct=True)
    r_slope = df_day["rs_slope"].rank(pct=True)
    r_va = df_day["v_accel"].rank(pct=True)

    w_rs = _safe_float(w.get("w_rs"), 0.2)
    w_slope = _safe_float(w.get("w_rs_slope"), 0.55)
    w_va = _safe_float(w.get("w_v_accel"), 0.25)

    s = (r_rs * w_rs) + (r_slope * w_slope) + (r_va * w_va)
    return s


# _relax_ladder_operational() removed 2026-07-24 -- was an independently
# diverged copy of generate_candidates_v41_1.py's _relax_ladder() (different
# formulas, missing L7-L9). Consolidated to import the real one instead so
# this simulation can't silently drift from production again.


def _junk_risk_score_price_only(sig: pd.DataFrame) -> pd.Series:
    """Price-data-only junk_risk score (0-100 scale), added 2026-07-24.

    Mirrors generate_candidates_v41_1.py's _apply_junk_risk_overlay() weight
    formula (liq*0.35 + pump*0.40 + young*0.10 + fund*0.15), but the
    fundamental component is fixed at 0.0 -- the optimizer's 10-year
    backtest window has no matching fundamental history (ROE/OPM/debt_ratio
    are only cached ~92 days), so it cannot be simulated here. Per decision,
    the original weight split is kept as-is (NOT renormalized to the
    remaining 85%), so the max reachable score in this simulation is 85, not
    100 -- production's max is 100 when fundamental data is present.
    """
    liq_r = _risk_unit_linear(sig.get("value", np.nan), low=10_000_000_000.0, high=80_000_000_000.0, invert=True)
    v_r = _risk_unit_linear(sig.get("v_accel", np.nan), low=1.8, high=4.2)
    stretch_r = _risk_unit_linear(sig.get("stretch", np.nan), low=1.08, high=1.30)
    ret_r = _risk_unit_linear(sig.get("ret1_pct", np.nan), low=8.0, high=30.0)
    rsi_r = _risk_unit_linear(sig.get("rsi14", np.nan), low=65.0, high=85.0)
    pump_r = pd.concat([v_r, stretch_r, ret_r, rsi_r], axis=1).mean(axis=1, skipna=True).fillna(0.0).clip(0.0, 1.0)
    young_r = _risk_unit_linear(sig.get("listing_days", np.nan), low=120.0, high=720.0, invert=True).fillna(0.0)
    junk_r = (liq_r.fillna(0.0) * 0.35 + pump_r * 0.40 + young_r * 0.10).clip(0.0, 0.85)
    return junk_r * 100.0


def _load_symbol_panel() -> pd.DataFrame:
    """Load latest symbol panel (date x code) with sector and market-cap snapshots."""
    if not SYMBOL_PANEL_CSV.exists():
        return pd.DataFrame(columns=["date", "code", "sector", "sector_code", "market_cap", "listed_shares"])
    panel = pd.read_csv(SYMBOL_PANEL_CSV, dtype={"code": str}, encoding="utf-8-sig", low_memory=False)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.normalize()
    panel["code"] = panel["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    sector_map_path = BASE_DIR / "_cache" / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"
    if "sector" in panel.columns and sector_map_path.exists():
        sm = pd.read_csv(sector_map_path, dtype={"krx_sector": str, "sector_code": str}, encoding="utf-8-sig")
        sm["krx_sector"] = sm["krx_sector"].astype(str).str.strip()
        sm["sector_code"] = sm["sector_code"].astype(str).str.strip()
        panel["sector"] = panel["sector"].astype(str).str.strip()
        panel = panel.merge(sm[["krx_sector", "sector_code"]], left_on="sector", right_on="krx_sector", how="left")
        panel = panel.drop(columns=["krx_sector"], errors="ignore")
    for c in ["sector", "sector_code", "market_cap", "listed_shares"]:
        if c not in panel.columns:
            panel[c] = pd.NA
    for c in ["market_cap", "listed_shares"]:
        panel[c] = pd.to_numeric(panel[c], errors="coerce")
    return panel[["date", "code", "sector", "sector_code", "market_cap", "listed_shares"]].drop_duplicates(["date", "code"], keep="last")


def _rule_e_param(params: dict, key: str, disabled: float = -1.0) -> float:
    """Read a rule_e threshold without the `or` falsy trap.

    0.0 is a meaningful threshold for mkt_ret20_min / mkt_ret60_min ("market
    return must be positive"), so `float(params.get(k, d) or d)` would silently
    convert it back into the -1.0 disabled sentinel. Missing / None / non-finite
    all mean "disabled".
    """
    v = params.get(key, disabled)
    if v is None:
        return float(disabled)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return float(disabled)
    return f if np.isfinite(f) else float(disabled)


def _select_day_candidates_operational(day_df: pd.DataFrame, p: dict) -> pd.DataFrame:
    """Optimizer candidate filter — delegated to the shared strategy core.

    The shared core expects production-style column names (mkt_ret20, mkt_ret60).
    compute_factors() adds these aliases alongside the legacy m_ret20/m_ret60.
    """
    return select_candidates_core(day_df, p)


def simulate_window(df: pd.DataFrame, params: dict) -> dict:
    """Simulate a single window.

    - Signals are generated from factor columns on df (window-limited).
    - Entry is next trading day's OPEN (D+1) for each signal day.
    - Exit is within HOLD_DAYS with stop/TP/trailing rules.
    - Max concurrent positions enforced.
    """
    global _WINDOW_CACHE

    empty_out = {"trade_rets": [], "trades": []}

    if df is None or df.empty:
        return empty_out

    # Window bounds (hard clip: do not use prices beyond this window)
    w_start = pd.to_datetime(df["date"].min())
    w_end = pd.to_datetime(df["date"].max())
    if pd.isna(w_start) or pd.isna(w_end):
        return empty_out

    # Build the per-window cache once (expensive) and reuse across all parameter
    # trials. Keyed by (w_start, w_end): the cache depends only on the window df,
    # so identical windows across combos share one build. [PERF 2026-07-25]
    ck = (np.datetime64(w_start), np.datetime64(w_end))
    pc = _WINDOW_CACHE.get(ck)
    if pc is None:
        pc = _build_price_cache(df)
        _WINDOW_CACHE[ck] = pc
    slices = pc["slices"]
    date_s = pc["date_s"]
    o_s = pc["open_s"]
    h_s = pc["high_s"]
    l_s = pc["low_s"]
    c_s = pc["close_s"]

    # thresholds
    rs_lim = float(params.get("rs_lim", 1.0))
    v_accel_lim = float(params.get("v_accel_lim", 1.0))
    v_accel_max = float(params.get("v_accel_max", 5.0))
    stretch_max = float(params.get("stretch_max", 9.9))
    value_min = float(params.get("value_min", 0.0))
    atr_max = float(params.get("atr_max", 9.9))
    rsi_max = float(params.get("rsi_max", 70.0))
    require_macd = float(params.get("require_macd_golden", 0.0) or 0.0) >= 0.5
    vol_close_corr_min = float(params.get("vol_close_corr_min", 0.0))
    near_52w_high_gap_max = float(params.get("near_52w_high_gap_max", 0.25))
    min_listing_days = float(params.get("min_listing_days", 126.0))
    _urlv = params.get("use_relax_ladder")
    use_relax_ladder = float(_urlv if _urlv is not None else 1.0) >= 0.5
    gap_up_max_pct = float(params.get("gap_up_max_pct", 0.0) or 0.0)
    entry_gap_down_stop_pct = float(params.get("entry_gap_down_stop_pct", 0.0) or 0.0)
    gap_limit = float(params.get("gap_limit", 0.0) or 0.0)

    # weights for daily ranking score
    w_rs = float(params.get("w_rs", 0.2))
    w_sl = float(params.get("w_rs_slope", 0.55))
    w_va = float(params.get("w_v_accel", 0.25))

    # Frozen/default values come from stable params when present.
    hold_days = int(params.get("hold", params.get("hold_days", DEFAULT_FROZEN.get("hold", 10))))
    max_pos = int(params.get("max_pos", params.get("max_positions", DEFAULT_FROZEN.get("max_pos", 20))))
    fee = float(params.get("fee", DEFAULT_FEE))

    _sl_raw = params.get("stop_loss", DEFAULT_FROZEN.get("stop_loss", -0.05))
    stop_loss = float(_sl_raw) if _sl_raw is not None else None
    take_profit = params.get("take_profit", DEFAULT_FROZEN.get("take_profit", None))
    trail_pct = params.get("trail_pct", DEFAULT_FROZEN.get("trail_pct", None))

    # signal filter (window-limited)
    req_cols = ["date", "code", "rs", "rs_slope", "stretch", "v_accel", "value", "atr14_pct", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days", "macd_golden", "ret1_pct"]
    for c in req_cols:
        if c not in df.columns:
            return empty_out

    p0 = {
        "rs_lim": rs_lim,
        "v_accel_lim": v_accel_lim,
        "v_accel_max": v_accel_max,
        "defense_bear_rs_slope_min": float(params.get("defense_bear_rs_slope_min", 0.0)),
        "defense_bear_disable_entry": float(params.get("defense_bear_disable_entry", 0.0)),
        "stretch_max": stretch_max,
        "value_min": value_min,
        "atr_max": atr_max,
        "rsi_max": rsi_max,
        "require_macd_golden": 1.0 if require_macd else 0.0,
        "vol_close_corr_min": vol_close_corr_min,
        "near_52w_high_gap_max": near_52w_high_gap_max,
        "min_listing_days": min_listing_days,
        "sector_blacklist": str(params.get("sector_blacklist", "")),
        "sector_max_per_day": int(params.get("sector_max_per_day", 0) or 0),
        "min_market_cap": float(params.get("min_market_cap", 0.0) or 0.0),
        "require_above_ma200": 1.0 if float(params.get("require_above_ma200", 0.0) or 0.0) >= 0.5 else 0.0,
        # [2026-08-15] rule_e was wired into the day filter
        # (_select_day_candidates_operational, "Market/sector defense filters")
        # but never reached it: p0 omitted these three keys, so `p.get(k, -1.0)`
        # fell through to the -1.0 disabled sentinel on every level of the
        # ladder. main() puts real values in `base`; they died here. Production
        # got the same filters wired on 2026-08-14, so until now the optimizer
        # was the more permissive of the two.
        #
        # Read with an explicit None check, never `x or default`: 0.0 is a valid
        # threshold for mkt_ret20_min / mkt_ret60_min and `or` would silently
        # turn it back into the disabled sentinel.
        "mkt_ret20_min": _rule_e_param(params, "mkt_ret20_min"),
        "mkt_ret60_min": _rule_e_param(params, "mkt_ret60_min"),
        "sector_rs_min": _rule_e_param(params, "sector_rs_min"),
    }
    ladder = _relax_ladder(p0) if use_relax_ladder else [("L0", p0)]

    selected_daily = []
    for _, gday in df.groupby("date", sort=True):
        chosen = pd.DataFrame()
        chosen_lv = "NONE"
        p_chosen = ladder[-1][1]
        for _lv, p_try in ladder:
            cand = _select_day_candidates_operational(gday, p_try)
            if not cand.empty:
                chosen = cand
                chosen_lv = _lv
                p_chosen = p_try
                break

        # [SYNC] production candidate mechanisms (the two replicable ones;
        # fundamental overlay / watchlist exclusion are data-blocked pre-2026)
        if _PROD_CAND:
            try:
                lvnum = int(str(chosen_lv).upper().replace("L", "")[:1])
            except Exception:
                lvnum = -1
            if lvnum >= 5 and len(chosen) < 5:
                _mx, _p5, _p10 = _compute_rally_breadth_stats(gday.get("ret1_pct"))
                if (_mx >= 25.0) and (_p10 >= 0.08):
                    p2 = dict(p_chosen)
                    p2["near_52w_high_gap_max"] = max(float(p2.get("near_52w_high_gap_max", 0.25)), 0.12)
                    p2["v_accel_lim"] = max(float(p2.get("v_accel_lim", 1.2)) * 0.90, 1.05)
                    p2["value_min"] = max(float(p2.get("value_min", 1e9)) * 0.70, 5_000_000_000.0)
                    cand2 = _select_day_candidates_operational(gday, p2)
                    if len(cand2) > len(chosen):
                        chosen = cand2
            # [2026-08-15] The sector-union fallback is NOT simulated any more.
            #
            # `_apply_sector_prefilter_union()` only ever appends rows tagged
            # candidate_origin="SECTOR_PREFILTER_UNION" / natural_pass=False; the
            # natural candidates it passes through are returned unchanged. In
            # production those appended rows are observe-only -- entry.py's
            # _sector_fallback_observe_only_mask() drops every one of them before
            # the entry pool is built -- so counting them as tradeable here made
            # the simulator strictly more permissive than the thing it is meant
            # to model.
            #
            # This call previously raised KeyError('sector_code_x') and the bare
            # `except Exception: pass` below swallowed it, so the fallback
            # silently contributed nothing. That broken state is exactly what the
            # live stable_params_v41_1.json was tuned and certified under: 229
            # trades across 7 folds, against 2354 once the KeyError was fixed on
            # 2026-08-14. Reproduced 7/7 by re-raising the KeyError.
            #
            # Dropping the call is equivalent to calling it and filtering the
            # union rows back out, minus the per-day cost, and it no longer
            # depends on an exception to stay correct. Note the coupling: this
            # matches production only while generate_candidates_v41_1.py keeps
            # candidate_origin in keep_cols. If that column is dropped again the
            # observe-only mask goes dead, production starts trading union rows,
            # and this exclusion becomes too strict.
            pass

        if not chosen.empty:
            selected_daily.append(chosen)

    if not selected_daily:
        return empty_out

    sig = pd.concat(selected_daily, ignore_index=True)
    sig["code"] = sig["code"].astype(str)
    sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
    sig = sig.dropna(subset=["date", "code"])
    if sig.empty:
        return empty_out

    # Per-day score (fast: avoid groupby.apply)
    g = sig.groupby("date", sort=False)
    sig["score"] = (
        g["rs"].rank(pct=True) * w_rs
        + g["rs_slope"].rank(pct=True) * w_sl
        + g["v_accel"].rank(pct=True) * w_va
    )

    # junk_risk (price-only simulation, 2026-07-24) -- soft penalty + hard
    # exclude, matching generate_candidates_v41_1.py's _apply_junk_risk_overlay()
    # defaults/mechanics. Fundamental component fixed at 0 (see function docstring).
    _jre = params.get("junk_risk_enable")
    junk_enable = float(_jre if _jre is not None else 1.0) >= 0.5
    if junk_enable:
        junk_score = _junk_risk_score_price_only(sig)
        _jpm = params.get("junk_penalty_max")
        pen_max = min(max(float(_jpm if _jpm is not None else 0.18), 0.0), 0.35)
        sig["score"] = sig["score"] * (1.0 - (junk_score / 100.0) * pen_max)

        _jhe = params.get("junk_hard_exclude")
        hard_exclude = float(_jhe if _jhe is not None else 1.0) >= 0.5
        if hard_exclude:
            _jht = params.get("junk_hard_threshold")
            hard_th = min(max(float(_jht if _jht is not None else 88.0), 60.0), 99.0)
            sig = sig[junk_score < hard_th].copy()

    # Sector max-per-day defensive cap (added 2026-08-10)
    sector_max = int(params.get("sector_max_per_day", 0) or 0)
    if sector_max > 0 and "sector_code" in sig.columns and not sig.empty:
        sig = sig.sort_values(["date", "score"], ascending=[True, False], kind="mergesort")
        sig = sig.groupby(["date", "sector_code"], as_index=False, group_keys=False).head(sector_max)

    sig = sig.sort_values(["date", "score"], ascending=[True, False], kind="mergesort")
    sig_dates = sig["date"].to_numpy(dtype="datetime64[ns]")
    sig_codes = sig["code"].to_numpy(dtype=object)
    sig_history_keys = sig["price_history_key"].to_numpy(dtype=object)

    rets: list[float] = []
    trades: list[dict] = []  # per-trade metadata for daily portfolio return reconstruction
    active: list[tuple[np.datetime64, str]] = []  # (exit_date, code)

    i = 0
    n = len(sig)
    w_start64 = np.datetime64(w_start.to_datetime64())
    w_end64 = np.datetime64(w_end.to_datetime64())

    while i < n:
        d = sig_dates[i]

        # expire finished positions once per day (avoid per-signal filtering)
        if active:
            active = [t for t in active if t[0] > d]
        if len(active) >= max_pos:
            # skip all signals for this day
            j = i + 1
            while j < n and sig_dates[j] == d:
                j += 1
            i = j
            continue

        active_codes = {t[1] for t in active}

        # process signals of the day in score order until slots filled
        j = i
        while j < n and sig_dates[j] == d:
            if len(active) >= max_pos:
                break

            code = str(sig_codes[j])
            history_key = str(sig_history_keys[j])
            j += 1

            if code in active_codes:
                continue

            sl = slices.get(history_key)
            if not sl:
                continue
            a, b = sl

            # Clip this code's available prices to window range [w_start, w_end]
            dates_code = date_s[a:b]
            if len(dates_code) < 2:
                continue

            # locate window bounds within this code slice
            lo = int(a + np.searchsorted(dates_code, w_start64, side="left"))
            hi_excl = int(a + np.searchsorted(dates_code, w_end64, side="right"))
            if hi_excl - lo < 2:
                continue

            dates_w = date_s[lo:hi_excl]

            # signal day must exist in this clipped slice
            idx = int(np.searchsorted(dates_w, d))
            if idx >= len(dates_w) or dates_w[idx] != d:
                continue

            # [SYNC] entry timing follows production (entry_timing_mode)
            entry_i = (lo + idx) if _ENTRY_MODE == "same_close" else (lo + idx + 1)
            if entry_i >= hi_excl:
                continue

            entry_p = float(c_s[entry_i]) if _ENTRY_MODE == "same_close" else float(o_s[entry_i])
            if not np.isfinite(entry_p) or entry_p <= 0:
                continue

            sig_close = float(c_s[lo + idx])
            # same_close has no overnight gap, so the gap filters do not apply
            if _ENTRY_MODE != "same_close" and np.isfinite(sig_close) and sig_close > 0:
                gap = (entry_p - sig_close) / sig_close
                if gap_up_max_pct > 0 and gap > gap_up_max_pct:
                    continue
                if entry_gap_down_stop_pct > 0 and gap <= -abs(entry_gap_down_stop_pct):
                    continue
                if gap_up_max_pct <= 0 and entry_gap_down_stop_pct <= 0 and gap_limit > 0 and abs(gap) > gap_limit:
                    continue

            # simulate exit within hold_days, clipped to window end
            end_i = min(entry_i + hold_days - 1, hi_excl - 1)

            exit_p = float(c_s[end_i])
            exit_d = date_s[end_i]

            # stop/tp/trailing
            if stop_loss is not None:
                stop_p = entry_p * (1.0 + float(stop_loss))
            else:
                stop_p = None
            if take_profit is not None:
                tp_p = entry_p * (1.0 + float(take_profit))
            else:
                tp_p = None

            max_high = entry_p
            if trail_pct is not None:
                trail_p = entry_p * (1.0 - float(trail_pct))
            else:
                trail_p = None

            # [SYNC] production partial-TP ladder + activated trailing.
            # Mirrors report_backtest_v41_1.py / paper_engine ordering:
            #   STOP -> TRAIL -> staged TP -> time exit
            if _PTP_PLAN:
                max_close = entry_p
                remaining = 1.0
                realized = 0.0
                taken = set()
                _scan0 = entry_i + 1 if _ENTRY_MODE == "same_close" else entry_i
                for k in range(_scan0, end_i + 1):
                    hi = float(h_s[k]); lo_p = float(l_s[k]); cl = float(c_s[k])
                    if np.isfinite(cl) and cl > max_close:
                        max_close = cl
                    t_px = None
                    if (_PTP_TRAIL_PCT is not None) and max_close > 0 and \
                       ((max_close / (entry_p + 1e-9) - 1.0) >= float(_PTP_TRAIL_ACTIVATION)):
                        t_px = max_close * (1.0 + float(_PTP_TRAIL_PCT))
                    if stop_p is not None and np.isfinite(lo_p) and lo_p <= stop_p:
                        realized += remaining * ((stop_p / entry_p) - 1.0)
                        remaining = 0.0; exit_d = date_s[k]; break
                    if t_px is not None and np.isfinite(lo_p) and lo_p <= t_px:
                        realized += remaining * ((t_px / entry_p) - 1.0)
                        remaining = 0.0; exit_d = date_s[k]; break
                    for i_tp, (lv, rt) in enumerate(_PTP_PLAN):
                        if i_tp in taken:
                            continue
                        px = entry_p * (1.0 + float(lv))
                        if np.isfinite(hi) and hi >= px:
                            w = min(remaining, max(0.0, float(rt)))
                            taken.add(i_tp)
                            if w <= 0:
                                continue
                            realized += w * ((px / entry_p) - 1.0)
                            remaining -= w
                            exit_d = date_s[k]
                            if remaining <= 1e-12:
                                remaining = 0.0
                                break
                    if remaining <= 0.0:
                        break
                if remaining > 0.0:
                    realized += remaining * ((float(c_s[end_i]) / entry_p) - 1.0)
                    exit_d = date_s[end_i]
                pnl_pct = realized - fee
                if np.isfinite(pnl_pct):
                    rets.append(float(pnl_pct))
                    trades.append({
                        "entry_d": np.datetime64(date_s[entry_i]),
                        "exit_d": np.datetime64(exit_d),
                        "pnl_pct": float(pnl_pct),
                    })
                active.append((exit_d, code))
                active_codes.add(code)
                continue

            _scan0 = entry_i + 1 if _ENTRY_MODE == "same_close" else entry_i
            for k in range(_scan0, end_i + 1):
                hi = float(h_s[k])
                lo_p = float(l_s[k])

                if np.isfinite(hi) and hi > max_high:
                    max_high = hi
                    if trail_p is not None:
                        trail_p = max_high * (1.0 - float(trail_pct))

                # STOP (gap-aware using LOW)
                if stop_p is not None and np.isfinite(lo_p) and lo_p <= stop_p:
                    exit_p = float(stop_p)
                    exit_d = date_s[k]
                    break

                # TAKE PROFIT (gap-aware using HIGH)
                if tp_p is not None and np.isfinite(hi) and hi >= tp_p:
                    exit_p = float(tp_p)
                    exit_d = date_s[k]
                    break

                # TRAIL (gap-aware using LOW)
                if trail_p is not None and np.isfinite(lo_p) and lo_p <= trail_p:
                    exit_p = float(trail_p)
                    exit_d = date_s[k]
                    break

            pnl_pct = (exit_p / entry_p) - 1.0 - fee
            if np.isfinite(pnl_pct):
                rets.append(float(pnl_pct))
                trades.append({
                    "entry_d": np.datetime64(date_s[entry_i]),
                    "exit_d": np.datetime64(exit_d),
                    "pnl_pct": float(pnl_pct),
                })

            active.append((exit_d, code))
            active_codes.add(code)

        # advance to next day
        while j < n and sig_dates[j] == d:
            j += 1
        i = j

    return {"trade_rets": rets, "trades": trades}


def _regime_for_date(d: np.datetime64, regime_ser: pd.Series) -> str:
    """Look up market_regime for a given date from a date-indexed series."""
    try:
        val = regime_ser.get(d, None)
        if val is not None:
            return str(val).upper()
    except Exception:
        pass
    return "NORMAL"


def _compute_daily_portfolio_returns(
    trades: List[dict],
    wdf: pd.DataFrame,
    params: dict,
) -> Tuple[List[float], float]:
    """Convert per-trade final returns into daily portfolio returns.

    Approximation: each trade's final pnl_pct is spread evenly over its
    holding days. The portfolio holds up to `max_pos` equal-weight positions.
    Cash portion earns 0. Regime-specific gross exposure is applied per day.

    Vectorized over dates for performance.

    Returns (daily_rets, avg_exposure_pct).
    """
    if not trades or wdf is None or wdf.empty:
        return [], 0.0

    max_pos = int(params.get("max_pos", params.get("max_positions", DEFAULT_FROZEN.get("max_pos", 20))))
    max_pos = max(1, max_pos)

    # Unique dates in this window, sorted
    all_dates = pd.Series(wdf["date"].unique()).sort_values().reset_index(drop=True)
    n_days = int(len(all_dates))
    if n_days == 0:
        return [], 0.0

    date_to_idx = {d: i for i, d in enumerate(all_dates)}

    # Accumulate per-trade daily contribution along holding window.
    # all_dates comes from wdf["date"], so index distance = trading days.
    daily_pnl = np.zeros(n_days, dtype=float)
    for t in trades:
        entry_d = pd.Timestamp(t["entry_d"]).normalize()
        exit_d = pd.Timestamp(t["exit_d"]).normalize()
        s = date_to_idx.get(entry_d)
        e = date_to_idx.get(exit_d)
        if s is None or e is None or e < s:
            continue
        hold_days = max(1, int(e - s) + 1)
        contrib = float(t["pnl_pct"]) / float(hold_days)
        daily_pnl[s : e + 1] += contrib

    # Equal-weight up to max_pos; cash earns 0
    portfolio_pnl = daily_pnl / float(max_pos)

    # Regime-specific gross exposure per date
    if "market_regime" in wdf.columns:
        date_regime = wdf.groupby("date")["market_regime"].first()
        regimes = [str(date_regime.get(d, "NORMAL")).upper() for d in all_dates]
    else:
        regimes = ["NORMAL"] * n_days
    exposures = np.array([float(REGIME_GROSS_EXPOSURE.get(r, REGIME_GROSS_EXPOSURE["NORMAL"])) for r in regimes])

    daily_rets = (portfolio_pnl * exposures).tolist()
    avg_exposure = float(np.mean(exposures)) * 100.0
    return daily_rets, avg_exposure


def build_windows(df: pd.DataFrame, years_back: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    end = df["date"].max().normalize()
    windows: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

    for k in range(years_back):
        w_end = end - pd.DateOffset(years=k)
        w_start = w_end - pd.DateOffset(years=1) + pd.DateOffset(days=1)
        windows.append((w_start, w_end))

    # Sort in chronological order.
    windows = sorted(windows, key=lambda x: x[0])

    # Clip to actual data range and remove empty windows.
    min_d = df["date"].min().normalize()
    out = []
    for s, e in windows:
        s2 = max(s, min_d)
        e2 = min(e, end)
        if s2 >= e2:
            continue
        out.append((s2, e2))
    return out


def eval_params(df: pd.DataFrame, windows: List[Tuple[pd.Timestamp, pd.Timestamp]], params: dict, label: str = "") -> Tuple[float, List[WindowResult]]:
    results: List[WindowResult] = []
    total_n = 0

    for idx, (s, e) in enumerate(windows, start=1):
        wdf = df[(df["date"] >= s) & (df["date"] <= e)].copy()
        # [PERF 2026-07-25] No per-window cache reset needed: simulate_window now
        # keys its price cache by (w_start, w_end), so each window uses its own
        # cache (built once, reused across all combos). The old
        # `_PRICE_CACHE = None` reset forced a rebuild every window *and* every
        # combo (7*N_ITER redundant builds).
        sim_out = simulate_window(wdf, params)
        rets_list = sim_out["trade_rets"]
        trades = sim_out["trades"]
        rets = np.asarray(rets_list, dtype="float64")
        if rets.size:
            rets = rets[np.isfinite(rets)]
        n = int(rets.size)
        total_n += n

        # Trade-level PF (aligned with report_backtest_v41_1.py).
        active_days = n
        if n < 5 or active_days < MIN_ACTIVE_DAYS:
            pf = 0.0
            mean_ret = -1.0
        else:
            pf = _trade_pf(rets_list)
            mean_ret = float(np.mean(rets)) if rets.size else -1.0

        if label:
            print(f"[EVAL {label}] window {idx}/{len(windows)} {s.date()}~{e.date()} n={n} active_days={active_days} pf={pf:.4f}")

        split = "IS" if e <= TRAIN_END else ("VAL" if e <= VAL_END else "OOS")
        # [2026-08-29] R6 배선. _compute_daily_portfolio_returns() 는 2026-07 부터
        #   존재했지만 호출된 적이 없고 여기서 [] / 0.0 을 하드코딩하고 있었다.
        #   그 결과 windows[].daily_rets 가 비어 CAGR·MDD·지수대비 환산이 구조적으로 불가능했고,
        #   A→B→C 검증(PLANS 131/132)이 "복리 상한" 이라는 대용치를 쓸 수밖에 없었다.
        #   계산은 이미 있었다 - 결과에 담기지 않았을 뿐이다.
        try:
            _daily_rets, _avg_exposure = _compute_daily_portfolio_returns(trades, wdf, params)
        except Exception as _exc:
            print(f"[R6_WARN] daily-return 계산 실패 window {idx}: {type(_exc).__name__}: {_exc}")
            _daily_rets, _avg_exposure = [], 0.0
        results.append(WindowResult(start=s.strftime("%Y-%m-%d"), end=e.strftime("%Y-%m-%d"), n_trades=n, pf=pf, mean_ret=mean_ret, split=split, year=int(e.year), daily_rets=_daily_rets, avg_exposure_pct=_avg_exposure))
    # Avoid score-distribution collapse: no-trade only is hard fail (still a
    # legitimate data/logic problem, not a "selective trading" case).
    if total_n <= 0:
        return -1e9, results
    # NOTE: trade-volume penalties (penalty_total_n, penalty_n, penalty_oos_n)
    # were removed from base_score on 2026-07-08. Empirically, pushing the
    # search toward higher trade counts (via loosened filters) improved a
    # single weak year (2022) but degraded 4 of 7 rolling windows when
    # cross-validated (see PLANS.md same-date entry). Trade quality (pf) and
    # the worst-fold hurdle already gate reliability; a parameter set that
    # legitimately trades less in adverse regimes should not be scored down
    # for that alone. MIN_TRADES_TOTAL/MIN_TRADES_PER_WINDOW are kept as
    # named constants (still used by _fold_selection_metrics to exclude
    # too-thin folds from the worst-fold hurdle) but no longer feed a score
    # penalty here.
    penalty_total_n = 0.0
    low_windows = sum(1 for r in results if r.n_trades < MIN_TRADES_PER_WINDOW)
    penalty_n = 0.0

    # [2026-08-15] Sentinel folds must not enter any aggregate.
    # The loop above marks a too-thin fold with pf=0.0 / mean_ret=-1.0. Those are
    # MISSING-DATA markers, not measurements: -1.0 is not a -100% return. Feeding
    # them to np.mean let a single n=4 fold swing base_score by more than the
    # whole score was worth (measured 2026-08-15: is_mean -0.24564 vs +0.00580
    # over the three real IS folds). They stay in `results`, and therefore in the
    # stable json `windows`, so the thin fold remains visible -- but every mean /
    # std / split aggregate below is computed on scored folds only.
    # `n_trades >= MIN_TRADES_PER_WINDOW` is exactly the complement of the
    # sentinel condition in the loop (n < 5 or active_days < MIN_ACTIVE_DAYS,
    # both 15) and matches the `eligible` set in _fold_selection_metrics().
    def _scored(arr):
        return [
            x for x in arr
            if int(x.n_trades) >= MIN_TRADES_PER_WINDOW and np.isfinite(float(x.pf))
        ]

    scored_pfs = [float(x.pf) for x in _scored(results)]
    scored_means = [float(x.mean_ret) for x in _scored(results)]
    mean_pf = float(np.mean(scored_pfs)) if scored_pfs else 0.0
    std_pf = float(np.std(scored_pfs)) if scored_pfs else 0.0
    mean_ret = float(np.mean(scored_means)) if scored_means else -1.0

    # split aggregation (IS/VAL/OOS) for scoring / reporting.
    # The split lists stay complete so is_n/val_n/oos_n keep counting every
    # window; only the pf/mean_ret averages drop the sentinel folds.
    is_res = [r for r in results if r.split == "IS"]
    val_res = [r for r in results if r.split == "VAL"]
    oos_res = [r for r in results if r.split == "OOS"]

    def _avg_pf(arr):
        vals = [float(x.pf) for x in _scored(arr)]
        return float(np.mean(vals)) if vals else 0.0

    def _avg_mean(arr):
        vals = [float(x.mean_ret) for x in _scored(arr)]
        return float(np.mean(vals)) if vals else 0.0

    def _sum_n(arr):
        return int(np.sum([x.n_trades for x in arr])) if arr else 0

    is_pf = _avg_pf(is_res); val_pf = _avg_pf(val_res); oos_pf = _avg_pf(oos_res)
    is_mean = _avg_mean(is_res); val_mean = _avg_mean(val_res); oos_mean = _avg_mean(oos_res)
    is_n = _sum_n(is_res); val_n = _sum_n(val_res); oos_n = _sum_n(oos_res)

    pf_cap = 5.0

    def _cap_pf(x: float) -> float:
        try:
            x = float(x)
        except Exception:
            return 0.0
        if not np.isfinite(x):
            return pf_cap
        return max(0.0, min(x, pf_cap))
    # penalty_oos_n removed alongside penalty_n/penalty_total_n above (2026-07-08);
    # oos_n==0 is still visible in reporting/diagnostics, just no longer scored down.
    penalty_oos_n = 0.0
    # [2026-08-15] OOS is OUT of the objective.
    # Reserving the OOS folds inside _fold_selection_metrics() was not enough:
    # that only cleaned the `0.01 * recent_weighted` term, while OOS entered here
    # with pf x1.6 and mean_ret x40.0 -- 68.6% of base_score's term magnitude.
    # With those coefficients live, HPO tuned directly on the windows the stable
    # quality gate then re-scored, so "OOS PF" was never out-of-sample.
    #
    # The OOS weight is redistributed across IS/VAL rather than dropped, keeping
    # the totals unchanged (pf 2.4, mean_ret 52.0) so the score scale -- and with
    # it the gate's min_stable_score and the promotion margin -- stays comparable
    # to previous runs. VAL carries 2x IS: it is the more recent evidence, but it
    # holds fewer trades (60 vs 96 as of 2026-08-15), so a larger ratio would
    # hand the smaller sample the louder vote.
    #
    # oos_pf / oos_mean / oos_n stay computed above for reporting and land in the
    # stable json; they simply do not steer the search any more.
    base_score = (
        (_cap_pf(val_pf) * 1.6 + _cap_pf(is_pf) * 0.8)
        + (val_mean * 35.0 + is_mean * 17.0)
        - (PF_STD_PENALTY * std_pf)
        - penalty_n
        - penalty_total_n
        - penalty_oos_n
    )
    metrics = _fold_selection_metrics(results, float(base_score))
    return float(metrics["hypertime_score"]), results


def _enforce_v_accel_band(p: dict, margin: float = 0.10) -> dict:
    """Keep v_accel_lim strictly below v_accel_max.

    The day filter asks for `v_accel > v_accel_lim AND v_accel <= v_accel_max`,
    so v_accel_lim >= v_accel_max is an empty set, not a strict filter. BOUNDS
    samples v_accel_lim from [1.00, 8.00] while v_accel_max is fixed at 5.0 and
    is not searched, so 37% of the sampling range produced a structurally dead
    L0. The live stable landed exactly there (v_accel_lim 6.6 > v_accel_max 5.0):
    ladder levels L0-L2 could never return a candidate and L3 was the real
    starting level, which is invisible in the relax_level diagnostics.

    Clamping rather than rejecting keeps the sampler's proposal density intact;
    the clamped value is still a legitimate point in the feasible region.
    """
    out = dict(p)
    try:
        lim = float(out.get("v_accel_lim"))
        mx = float(out.get("v_accel_max", 5.0))
    except (TypeError, ValueError):
        return out
    if not (np.isfinite(lim) and np.isfinite(mx)):
        return out
    if lim >= mx:
        out["v_accel_lim"] = max(float(mx) - float(margin), 0.0)
    return out


def sample_params(rng: np.random.Generator, base: dict) -> dict:
    p = dict(base)

    for k, (lo, hi, step) in BOUNDS.items():
        v = rng.uniform(lo, hi)
        p[k] = _clip_grid(v, lo, hi, step)

    # weights: dirichlet then round, normalize
    w = rng.dirichlet([2.0, 3.0, 2.0])
    w_rs = float(w[0])
    w_slope = float(w[1])
    w_va = float(w[2])

    # round and renormalize
    w_rs = _round_grid(w_rs, 0.01)
    w_slope = _round_grid(w_slope, 0.01)
    w_va = _round_grid(w_va, 0.01)
    s = w_rs + w_slope + w_va
    if s <= 0:
        w_rs, w_slope, w_va = 0.2, 0.55, 0.25
        s = 1.0
    p["w_rs"] = w_rs / s
    p["w_rs_slope"] = w_slope / s
    p["w_v_accel"] = w_va / s

    p = _enforce_v_accel_band(p)
    return p


def step_toward(stable: dict, best: dict, alpha: float) -> dict:
    out = dict(stable)
    for k in best.keys():
        if k in FROZEN_KEYS:
            continue
        if k in ["as_of", "source", "best_score", "windows"]:
            continue

        if k in BOUNDS:
            lo, hi, step = BOUNDS[k]
            sv = _safe_float(stable.get(k), best.get(k))
            bv = _safe_float(best.get(k), stable.get(k))
            nv = sv + (bv - sv) * alpha
            out[k] = _clip_grid(nv, lo, hi, step)
        elif k.startswith("w_"):
            # Normalize weights after moving toward best.
            out[k] = _safe_float(stable.get(k), best.get(k)) + (
                _safe_float(best.get(k), stable.get(k)) - _safe_float(stable.get(k), best.get(k))
            ) * alpha

    # normalize weights
    w_rs = _safe_float(out.get("w_rs"), 0.2)
    w_slope = _safe_float(out.get("w_rs_slope"), 0.55)
    w_va = _safe_float(out.get("w_v_accel"), 0.25)
    s = w_rs + w_slope + w_va
    if s <= 0:
        w_rs, w_slope, w_va = 0.2, 0.55, 0.25
        s = 1.0
    out["w_rs"] = w_rs / s
    out["w_rs_slope"] = w_slope / s
    out["w_v_accel"] = w_va / s

    return out


def main() -> int:
    print("[OPT] loading data...")
    df = load_data(BASE_DIR)

    # Exclude pre-2020 data if present.
    df = df[df["date"] >= pd.Timestamp("2020-01-01")].copy()

    df = compute_factors(df)

    # [PERF 2026-07-25] Per-window price caches are built lazily inside
    # simulate_window (keyed by window bounds, reused across combos). No global
    # full-df cache build here — the old one was discarded by eval_params anyway.
    print(f"[OPT] price universe codes={df['price_history_key'].nunique()} rows={len(df)}")
    df = df.dropna(subset=["rs", "rs_slope", "v_accel", "stretch", "atr14_pct", "gap_next", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days"]).copy()

    if df.empty:
        raise RuntimeError("?⑺꽣 怨꾩궛 ???좏슚 ?곗씠?곌? ?놁뒿?덈떎.")

    windows = build_windows(df, YEARS_BACK)
    if not windows:
        raise RuntimeError("?덈룄??援ъ꽦???ㅽ뙣?덉뒿?덈떎.")

    stable_path = OUT_DIR / "stable_params_v41_1.json"
    best_path = OUT_DIR / "best_params_v41_1.json"
    report_path = OUT_DIR / "search_report_v41_1.csv"

    prev_stable = _jload(stable_path)

    # base params = prev stable or defaults
    gu_cfg, gd_cfg = _load_paper_gap_policy()
    base = {
        "rs_lim": _safe_float(prev_stable.get("rs_lim"), 1.7),
        "v_accel_lim": _safe_float(prev_stable.get("v_accel_lim"), 2.5),
        "v_accel_max": _safe_float(prev_stable.get("v_accel_max"), 5.0),
        "defense_bear_rs_slope_min": _safe_float(prev_stable.get("defense_bear_rs_slope_min"), 0.0),
        "defense_bear_disable_entry": 1.0,  # rule_e: skip bear/crash entries
        "stretch_max": _safe_float(prev_stable.get("stretch_max"), 1.19),
        "value_min": _safe_float(prev_stable.get("value_min"), 105e9),
        "atr_max": _safe_float(prev_stable.get("atr_max"), 0.20),
        "gap_limit": _safe_float(prev_stable.get("gap_limit"), 0.15),
        "gap_up_max_pct": _safe_float(prev_stable.get("gap_up_max_pct"), gu_cfg),
        "entry_gap_down_stop_pct": _safe_float(prev_stable.get("entry_gap_down_stop_pct"), gd_cfg),
        "rsi_max": _safe_float(prev_stable.get("rsi_max"), 70.0),
        "require_macd_golden": 1.0 if _safe_float(prev_stable.get("require_macd_golden"), 0.0) >= 0.5 else 0.0,
        "vol_close_corr_min": _safe_float(prev_stable.get("vol_close_corr_min"), 0.0),
        "near_52w_high_gap_max": _safe_float(prev_stable.get("near_52w_high_gap_max"), 0.25),
        "min_listing_days": _safe_float(prev_stable.get("min_listing_days"), 126.0),
        "use_relax_ladder": 1.0 if _safe_float(prev_stable.get("use_relax_ladder"), 1.0) >= 0.5 else 0.0,
        "w_rs": _safe_float(prev_stable.get("w_rs"), 0.20),
        "w_rs_slope": _safe_float(prev_stable.get("w_rs_slope"), 0.55),
        "w_v_accel": _safe_float(prev_stable.get("w_v_accel"), 0.25),
        "sector_blacklist": str(prev_stable.get("sector_blacklist", "005,024")),
        "sector_max_per_day": int(prev_stable.get("sector_max_per_day", 2) or 2),
        "min_market_cap": float(prev_stable.get("min_market_cap", 1e11) or 1e11),
        "require_above_ma200": 1.0 if _safe_float(prev_stable.get("require_above_ma200"), 1.0) >= 0.5 else 0.0,
        "mkt_ret20_min": 0.0,  # rule_e: market 20d return > 0
        "mkt_ret60_min": 0.0,  # rule_e: market 60d return > 0
        "sector_rs_min": 0.05,  # rule_e: sector rs > 0.05
    }
    base = _apply_gap_policy_schema(base, {"gap_up_max_pct": gu_cfg, "entry_gap_down_stop_pct": gd_cfg})

    # BOUNDS에 새로 추가된 키는 prev_stable 값으로 워밍스타트 (없으면 범위 중간값)
    for k, (lo, hi, step) in BOUNDS.items():
        if k not in base:
            base[k] = _clip_grid(
                _safe_float(prev_stable.get(k), (lo + hi) / 2.0),
                lo, hi, step,
            )

    # frozen: keep from prev stable if exists, else defaults
    for k in FROZEN_KEYS:
        if k in prev_stable:
            base[k] = prev_stable[k]
        else:
            base[k] = DEFAULT_FROZEN[k]

    # [2026-08-15] The previous stable can itself carry the empty-set band (the
    # live one did: v_accel_lim 6.6 vs v_accel_max 5.0). Repair the baseline too,
    # loudly -- silently searching around a dead L0 is how it went unnoticed.
    _va_before = base.get("v_accel_lim")
    base = _enforce_v_accel_band(base)
    if base.get("v_accel_lim") != _va_before:
        print(
            f"[FIX] v_accel_lim {_va_before} >= v_accel_max "
            f"{base.get('v_accel_max')}: ladder levels L0-L2 were an empty set. "
            f"clamped to {base.get('v_accel_lim')}"
        )

    rng = np.random.default_rng(RNG_SEED)

    # Evaluate current stable baseline (for promotion decision)
    stable_score, stable_windows = eval_params(df, windows, base, label="STABLE_BASELINE")

    best_score = -1e18
    best_params = None
    best_windows = None
    best_selection = None

    rows = []

    for i in range(N_ITER):
        p = sample_params(rng, base)

        # ensure frozen exactly
        for k in FROZEN_KEYS:
            p[k] = base[k]

        score, win_res = eval_params(df, windows, p, label=f"ITER_{i+1}")

        # flatten quick metrics
        n_total = int(sum(r.n_trades for r in win_res))
        mean_pf = float(np.mean([r.pf for r in win_res])) if win_res else 0.0
        std_pf = float(np.std([r.pf for r in win_res])) if win_res else 0.0
        mean_ret = float(np.mean([r.mean_ret for r in win_res])) if win_res else -1.0

        # split metrics (IS/VAL/OOS) derived from yearly windows
        is_res = [r for r in win_res if r.split == "IS"]
        val_res = [r for r in win_res if r.split == "VAL"]
        oos_res = [r for r in win_res if r.split == "OOS"]
        is_n = int(sum(r.n_trades for r in is_res)) if is_res else 0
        val_n = int(sum(r.n_trades for r in val_res)) if val_res else 0
        oos_n = int(sum(r.n_trades for r in oos_res)) if oos_res else 0
        is_pf = float(np.mean([r.pf for r in is_res])) if is_res else 0.0
        val_pf = float(np.mean([r.pf for r in val_res])) if val_res else 0.0
        oos_pf = float(np.mean([r.pf for r in oos_res])) if oos_res else 0.0
        is_ret = float(np.mean([r.mean_ret for r in is_res])) if is_res else 0.0
        val_ret = float(np.mean([r.mean_ret for r in val_res])) if val_res else 0.0
        oos_ret = float(np.mean([r.mean_ret for r in oos_res])) if oos_res else 0.0
        fold_selection = _fold_selection_metrics(win_res, 0.0)
        fold_selection["hypertime_score"] = float(score)


        rows.append({
            "iter": i + 1,
            "score": score,
            "hypertime_score": fold_selection["hypertime_score"],
            "hypertime_reason": fold_selection["hypertime_reason"],
            "avg": fold_selection["avg"],
            "worst_fold": fold_selection["worst_fold"],
            "recent_weighted": fold_selection["recent_weighted"],
            "worst_fold_hurdle": fold_selection["worst_fold_hurdle"],
            "worst_fold_pass": fold_selection["worst_fold_pass"],
            "n_folds": fold_selection["n_folds"],
            "n_total": n_total,
            "mean_pf": mean_pf,
            "std_pf": std_pf,
            "mean_ret": mean_ret,
            "is_n": is_n, "val_n": val_n, "oos_n": oos_n,
            "is_pf": is_pf, "val_pf": val_pf, "oos_pf": oos_pf,
            "is_ret": is_ret, "val_ret": val_ret, "oos_ret": oos_ret,
            **{k: p.get(k) for k in [
                "rs_lim", "v_accel_lim", "v_accel_max", "defense_bear_rs_slope_min", "defense_bear_disable_entry", "stretch_max", "value_min", "atr_max", "gap_limit", "gap_up_max_pct", "entry_gap_down_stop_pct",
                "rsi_max", "require_macd_golden", "vol_close_corr_min", "near_52w_high_gap_max", "min_listing_days", "use_relax_ladder",
                "w_rs", "w_rs_slope", "w_v_accel",
                "mkt_ret20_min", "mkt_ret60_min", "sector_rs_min",
            ]},
        })

        if score > best_score:
            best_score = score
            best_params = dict(p)
            best_windows = win_res
            best_selection = dict(fold_selection)

        if (i + 1) % 20 == 0:
            print(f"[OPT] {i+1}/{N_ITER} best_score={best_score:.4f} stable_score={stable_score:.4f}")

    # Save report
    pd.DataFrame(rows).to_csv(report_path, index=False, encoding="utf-8-sig")

    # best json
    best_obj = _apply_gap_policy_schema(dict(best_params or base), base)
    if best_selection is not None:
        best_selection["hypertime_score"] = float(best_score)
    best_obj.update({
        "as_of": _today_str(),
        "source": "best",
        "best_score": float(best_score),
        "selection_metrics": best_selection or {},
        "windows": [r.__dict__ for r in (best_windows or [])],
    })
    _jsave(best_path, best_obj)

    # stable promotion decision
    new_stable = dict(base)
    promoted = False
    recovery_from_invalid_stable = False

    best_is_selectable = bool(
        best_params is not None
        and best_selection is not None
        and best_selection.get("hypertime_reason") == "pass"
        and best_selection.get("worst_fold_pass") is True
        and float(best_score) > -1e8
    )

    if best_params is not None and best_is_selectable:
        if stable_score <= -1e8:
            # Recover path: if previous stable is clearly invalid, seed from best candidate.
            new_stable = dict(best_params)
            promoted = True
            recovery_from_invalid_stable = True
        elif best_score > stable_score + max(
            PROMOTION_MARGIN * abs(float(stable_score)), PROMOTION_MARGIN_MIN_ABS
        ):
            # normal promotion path. Additive margin: identical to the old
            # `stable*(1+PROMOTION_MARGIN)` for positive scores, but it keeps
            # pointing the right way when stable_score is negative or near zero.
            new_stable = step_toward(base, best_params, STEP_TOWARD_ALPHA)
            promoted = True

    # stable json
    new_score, new_windows = eval_params(df, windows, new_stable, label="NEW_STABLE")
    new_selection = _fold_selection_metrics(new_windows, 0.0)
    new_selection["hypertime_score"] = float(new_score)
    stable_obj = _apply_gap_policy_schema(dict(new_stable), base)
    stable_obj.update({
        "as_of": _today_str(),
        "source": "stable",
        "best_score": float(new_score),
        "promoted": bool(promoted),
        # [2026-08-20] certified / operational 분리. 승격은 두 가지를 동시에 뜻했으나
        # 이제 분리한다. 기계가 인증(certified)을, 사람이 가동(operational)을 정한다.
        # 승격 시점에는 둘 다 참이며, 이후 재현 불가 등이 드러나면 사람이
        # certified=false / cert_reason 을 기록하되 operational 은 별도로 판단한다.
        "certified": bool(promoted),
        "operational": bool(promoted),
        "cert_reason": "",
        "selection_metrics": new_selection,
        "windows": [r.__dict__ for r in new_windows],
        "meta": {
            "years_back": YEARS_BACK,
            "n_iter": N_ITER,
            "min_trades_total": MIN_TRADES_TOTAL,
            "min_trades_per_window": MIN_TRADES_PER_WINDOW,
            "rng_seed": RNG_SEED,
            "split_policy": {"train_end": TRAIN_END.strftime("%Y-%m-%d"), "val_end": VAL_END.strftime("%Y-%m-%d")},
            "promotion_margin": PROMOTION_MARGIN,
            "step_alpha": STEP_TOWARD_ALPHA,
            "recovery_from_invalid_stable": bool(recovery_from_invalid_stable),
            "best_is_selectable": bool(best_is_selectable),
            "schema_gap_policy_keys": list(GAP_POLICY_KEYS),
            "hpo_selection_policy": {
                "type": "time_aware_worst_fold_hurdle",
                "min_folds": int(HPO_MIN_FOLDS),
                "worst_fold_pf_hurdle": float(HPO_WORST_FOLD_PF_HURDLE),
                "recent_weight_half_life": float(HPO_RECENT_WEIGHT_HALF_LIFE),
            },
            "provenance": compute_provenance_metadata(BASE_DIR, PAPER_ENGINE_CONFIG),
        }
    })
    # Official quality gate — authoritative barrier layered ON TOP of this
    # optimizer's internal promotion criteria. Without this, the optimizer could
    # auto-overwrite stable_params_v41_1.json using only its own logic, bypassing
    # utils/stable_params_gate.py (the same gate report_backtest_v41_1.py and
    # generate_candidates_v41_1.py trust). Fail-closed: any error verifying the
    # gate blocks the write rather than silently promoting.
    try:
        _stable_gate_cfg = load_stable_quality_gate(PAPER_ENGINE_CONFIG)
        stable_gate_status = evaluate_stable_params(stable_obj, _stable_gate_cfg)
    except Exception as exc:  # noqa: BLE001 - gate error must never allow a write
        stable_gate_status = {
            "ok": False,
            "reason": f"gate_error:{exc}",
            "reasons": [f"gate_error:{exc}"],
        }
    gate_ok = bool(stable_gate_status.get("ok"))
    stable_obj.setdefault("meta", {})["stable_quality_gate"] = stable_gate_status

    write_allowed = bool(promoted and gate_ok)
    if promoted and not gate_ok:
        print(f"[GATE] promotion BLOCKED by official quality gate: {stable_gate_status.get('reason')}")

    stable_written = _persist_promoted_stable(stable_path, stable_obj, write_allowed)

    print("\n[OK] saved:")
    print(f" - {best_path}")
    if stable_written:
        print(f" - {stable_path}")
    else:
        print(f" - stable preserved (not promoted): {stable_path}")
    print(f" - {report_path}")
    print(
        f"[STABLE] promoted={promoted} gate_ok={gate_ok} written={stable_written} "
        f"gate_reason={stable_gate_status.get('reason')} "
        f"stable_score={new_score:.4f} (prev {stable_score:.4f})"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
