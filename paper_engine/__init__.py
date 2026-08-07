"""paper_engine package: split-out modules for the legacy paper_engine.py engine."""

from __future__ import annotations

# Re-export the public config loader so existing callers can use:
#   from paper_engine.config import load_config
from paper_engine.config import load_config
from paper_engine.drawdown import (
    DrawdownAction,
    _ddm_forced_sell_ratio_pct,
    _ddm_select_action,
)
from paper_engine.exit import (
    _calc_surge_exit_severity,
    _evaluate_hold_close_drop_guard,
    _resolve_hold_close_drop_guard_cfg,
    _resolve_surge_exit_ratio_pct,
)
from paper_engine.guards import compute_adaptive_kill_cap
from paper_engine.positions import _revalidate_pending_signals

__all__ = [
    "load_config",
    "_revalidate_pending_signals",
    "DrawdownAction",
    "_ddm_forced_sell_ratio_pct",
    "_ddm_select_action",
    "_calc_surge_exit_severity",
    "_evaluate_hold_close_drop_guard",
    "_resolve_hold_close_drop_guard_cfg",
    "_resolve_surge_exit_ratio_pct",
    "compute_adaptive_kill_cap",
]
