from __future__ import annotations

import csv
import json
import logging
import math
import os
import re
import sys
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ???????????????????????
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("paper_engine")

# ???????????????????????????????????????????????????????????????????????????????????????????????import
from utils.common import (
    norm_code,
    now_ymd,
    latest_file,
    read_csv_safe,
    read_json,
)

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
RISK_DIR = BASE_DIR / "12_Risk_Controlled"
PAPER_DIR = BASE_DIR / "paper"

FILLS = PAPER_DIR / "fills.csv"
TRADES = PAPER_DIR / "trades.csv"
STATE_PATH = PAPER_DIR / "paper_state.json"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
PENDING_SIGNALS_PATH = LOG_DIR / "pending_entry_signals_latest.csv"
OPS_ALERT_LATEST_PATH = LOG_DIR / "market_ops_alert_latest.json"
PENDING_STATUS_LATEST_PATH = LOG_DIR / "pending_entry_status_latest.json"

# Legacy schema (?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥???????????????????????????????????????????????????????????paper ?????????????????????????????????????
LEGACY_FILLS_HEADER = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
LEGACY_TRADES_HEADER = ["trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "pnl_krw", "exit_reason", "note"]

# v41.1 schema (?????????????????????????????????? ?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥??????????????????????????????????????????????????????????????????????????
V411_FILLS_HEADER = ["ts","date","code","name","side","qty","price","fee","slippage","order_id","note"]
V411_TRADES_HEADER = ["trade_id","entry_ts","exit_ts","code","name","side","qty","entry_price","exit_price",
                      "gross_ret","net_ret","fee","slippage","stop_hit","take_profit_hit","trail_hit","note"]

DEFAULT_CONFIG: Dict[str, Any] = {
    "max_new_trades_per_day": 3,
    "fixed_qty": 1,
    "max_hold_days": 10,                 # exit ???????????????????????????????????????濾????????????????곕춴???????븐뼐?????????嶺뚮∥?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????TIME??????
    "allow_same_code_reentry": False,
    "gap_up_max_pct": 0.0,               # 0 = disabled; >0 = T?????????? ????T+1??? ?????????????????????????????????????????????????????????????⑤벡??????????????????????????????????????
    "entry_gap_down_stop_pct": 0.0,      # 0 = disabled; >0 = T?? ?? T+1?? ???(??) ??

    # ??????????????????????????꾩룆梨띰쭕?뚢뵾??????????????嶺뚮죭?댁젘??????????????????????釉먮폁???????????????????살몝?????????????????????????????????????????????????????????????????????????????????????????????? ??????????????????????????????????????????
    "fee_pct": 0.005,
    "slippage_pct": 0.001,

    # tax (optional, default 0)
    "sell_tax_pct": 0.0,

    # ?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥???????????????????????????????????????????????????????????
    "candidates_latest_data": str(LOG_DIR / "candidates_latest_data.csv"),

    # parquet ?????
    "parquet_root": str(BASE_DIR),
    "parquet_top_n_recent": 120,
    "parquet_max_open_files": 30,
    "max_gross_exposure_pct": 1.0,       # 0~1 (or 0~100); cap on open+new notional / capital_total
    "max_daily_new_exposure_pct": 1.0,   # 0~1 (or 0~100); cap on same-day newly deployed notional
    "max_per_sector": 0,
    "max_per_symbol_exposure_pct": 0.0,
    # Adaptive operations policy:
    # avoid permanent all-stop by scaling entry allowance from live risk metrics.
    "adaptive_entry_control": {
        "enabled": True,
        "kill_switch_override_block": True,   # allow dynamic REDUCE even if kill_switch.mode=BLOCK
        "dd_ratio_soft": 1.00,                # |dd| / limit threshold
        "dd_ratio_mid": 1.10,
        "dd_ratio_hard": 1.25,
        "reduce_soft": 0.50,                  # applied to base max_new
        "reduce_mid": 0.30,
        "reduce_hard": 0.15,
        "probe_min_new": 1,                   # never zero for non-hard-data kill_switch day
        "relief_after_streak_days": 3,        # if blocked/reduced for N consecutive days
        "relief_min_new": 1,                  # keep small participation alive
        "dynamic_relax_l5_factor": 0.10,      # L5 cap scales with base max_new
        "kill_switch_block_fallback_reduce": True, # if ks mode=BLOCK and adaptive unavailable, keep minimal probe
    },
    # Regime policy:
    # - NORMAL: default behavior
    # - RALLY: allow small probe even under kill_switch BLOCK (non-hard-data only)
    # - CRASH: optional hard block (disabled by default; kill_switch remains hard guard)
    "regime_entry_policy": {
        "enabled": True,
        "rally_day_ret_min": 0.025,
        "crash_day_ret_max": -0.025,
        "allow_rally_on_macro_volatile": True,
        "allow_rally_when_macro_risk_off": True,
        "rally_probe_under_kill_switch_block": True,
        "rally_probe_max_new": 1,
        "rally_max_per_sector": 1,
        "rally_max_gross_exposure_pct": 0.60,
        "rally_max_daily_new_exposure_pct": 0.15,
        "rally_gap_up_max_pct": 0.015,
        "rally_entry_gap_down_stop_pct": 0.03,
        "crash_force_block": True,
        "macro_hard_block_enabled": False,
    },
    "market_ops_policy": {
        "enabled": True,
        "slo_normal": 0.10,
        "slo_rally_base": 0.20,
        "slo_rally_per_ret": 2.0,
        "slo_rally_cap": 0.50,
        "slo_crash_base": 0.05,
        "slo_crash_per_ret": 0.8,
        "slo_crash_cap": 0.20,
        "probe_rally_min": 1,
        "probe_rally_ratio_base": 0.10,
        "probe_rally_ratio_per_ret": 2.0,
        "probe_rally_ratio_cap": 0.35,
        "probe_crash_min": 0,
        "probe_crash_ratio_base": 0.03,
        "probe_crash_ratio_per_ret": 0.8,
        "probe_crash_ratio_cap": 0.10,
        "universe_shrink_min_candidates": 8,
        "universe_shrink_min_price_codes": 200,
        "universe_shrink_disable_signal_cap": True,
        "universe_shrink_disable_sector_cap": True,
        "universe_shrink_probe_uplift": 0.05,
        "carryover_no_next_day_enabled": True,
        "carryover_max_age_days": 2,
    },
}

@dataclass
class ColMap:
    date: str
    code: str
    open: str
    high: str
    low: str
    close: str
    name: Optional[str] = None

def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


# norm_code -> utils.common.norm_code????????
# ymd_now -> utils.common.now_ymd????????

def ensure_dirs() -> None:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def calc_qty(entry_price: float, cfg: Dict[str, Any], fee_pct: float, slip_pct: float) -> int:
    """Return order quantity based on a single sizing policy.

    Supported modes (cfg['sizing_mode']):
      - 'fixed_qty'      : use cfg['fixed_qty']
      - 'fixed_cash'     : use cfg['cash_per_trade'] (KRW)
      - 'capital_slots'  : use cfg['capital_total'] / cfg['max_positions'] (KRW)

    If qty < cfg['min_qty'] or entry_price<=0, returns 0 (skip).
    """
    try:
        mode = str(cfg.get("sizing_mode", "fixed_qty")).strip().lower()
    except Exception:
        mode = "fixed_qty"

    min_qty = int(cfg.get("min_qty", 1) or 1)

    if entry_price <= 0:
        return 0

    if mode == "fixed_qty":
        qty = int(cfg.get("fixed_qty", 1) or 1)
        return qty if qty >= min_qty else 0

    if mode == "fixed_cash":
        cash = float(cfg.get("cash_per_trade", 0) or 0)
    elif mode == "capital_slots":
        cap = float(cfg.get("capital_total", 0) or 0)
        slots = float(cfg.get("max_positions", 0) or 0)
        cash = (cap / slots) if (cap > 0 and slots > 0) else 0.0
    else:
        # unknown -> fallback to fixed_qty
        qty = int(cfg.get("fixed_qty", 1) or 1)
        return qty if qty >= min_qty else 0

    if cash <= 0:
        return 0

    # Conservative: ignore fees/slippage in qty calc for simplicity & reproducibility.
    qty = int(cash // float(entry_price))
    return qty if qty >= min_qty else 0


def load_config() -> Dict[str, Any]:
    """
    - ??????????????????????????????????????????????????????????????DEFAULT_CONFIG ??????????????????????諛몃마嶺뚮?????????????硫λ젒????????????????????遺얘턁??????얜Ŧ堉??????⑤뜪?????????????????????????癲????????????????????????????????????
    - ????????????????????????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥???????????????????????????????????????????????????????????????????????????????????DEFAULT_CONFIG?????????????????????????????????⑤벡????????????????????????KeyError ?????????????????????꾩룆梨띰쭕?뚢뵾??????????????嶺뚮죭?댁젘??????????????????????釉먮폁???????????????????살몝???????????????????????????????????????????????????????????????????????????????????)
    """
    ensure_dirs()
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[CONFIG] wrote default: {CONFIG_PATH}")
        return dict(DEFAULT_CONFIG)

    loaded = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(loaded)

    # Keep defaults in-memory only; do not mutate config on runtime.
    # Config file changes must go through tools/paper_engine_config_lock.py.
    missing = [k for k in DEFAULT_CONFIG.keys() if k not in loaded]
    if missing:
        print(f"[CONFIG] missing keys defaulted in-memory only: {missing}")
    return cfg

def read_header(path: Path) -> Optional[List[str]]:
    if not path.exists():
        return None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                r = csv.reader(f)
                return next(r, None)
        except Exception:
            continue

    return None

def detect_schema() -> str:
    hf = read_header(FILLS)
    ht = read_header(TRADES)
    if hf is None and ht is None:
        return "legacy"

    if hf == LEGACY_FILLS_HEADER and ht == LEGACY_TRADES_HEADER:
        return "legacy"
    if hf == V411_FILLS_HEADER and ht == V411_TRADES_HEADER:
        return "v41.1"

    raise SystemExit(
        "[FATAL] paper schema mismatch.\n"
        f"  fills_header={hf}\n"
        f"  trades_header={ht}\n"
        "Fix: make both legacy OR both v41.1 consistently."
    )

def ensure_csv(path: Path, header: List[str]) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(header)

def load_state() -> Dict[str, Any]:
    """
    paper_state.json
      - open_positions: ?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥?????????????????????????????????????????????????????????????????????????????????꾩룆梨띰쭕???????????
      - next_trade_seq: ????????????????trade_id ?????????????????????????밸븶筌믩끃??獄???????멥렑???????????????????耀붾굝?????臾먮뼁?????쇨덫?????????
      - processed_signals: "CODE:YYYYMMDD" (signal_date) ???????????????????????????????????????釉먮폁????????꿔꺂???癰귥옖留???????????????????????????????????????????????????????????????꾩룆梨띰쭕?뚢뵾??????????????嶺뚮죭?댁젘??????????????????????釉먮폁???????????????????살몝??????????????????????????????????????????????????????????????????????????????????? ??
    """
    if not STATE_PATH.exists():
        return {"open_positions": [], "next_trade_seq": 1, "processed_signals": []}
    try:
        st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if "processed_signals" not in st:
            st["processed_signals"] = []
        if "open_positions" not in st:
            st["open_positions"] = []
        if "next_trade_seq" not in st:
            st["next_trade_seq"] = 1
        return st
    except Exception:
        return {"open_positions": [], "next_trade_seq": 1, "processed_signals": []}

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    # atomic-ish on Windows: replace existing file
    tmp.replace(STATE_PATH)

# latest_json_in -> utils.common.latest_file????????

def load_latest_p0_risk_off(log_dir: Path) -> tuple[bool, list[str]]:
    """Return (enabled, reasons) from the latest p0_daily_check_*.json in log_dir.

    If missing or unreadable, returns (False, []).
    """
    try:
        p0p = latest_file(log_dir, 'p0_daily_check_*.json')  # ??utils.common ????
        if not p0p:
            return False, []
        obj = json.loads(p0p.read_text(encoding='utf-8'))
        ro = obj.get('risk_off') if isinstance(obj.get('risk_off'), dict) else {}
        enabled = bool(ro.get('enabled'))
        reasons = ro.get('reasons') if isinstance(ro.get('reasons'), list) else []
        reasons = [str(x) for x in reasons]
        return enabled, reasons
    except Exception:
        return False, []



def _is_hard_block_reason(reason: str) -> bool:
    r = str(reason or "")
    return (
        r.startswith("cand_latest_date(")
        or r.startswith("krx_clean_universe_degraded(")
        or r.startswith("prices_date_max(")
        or r.startswith("krx_clean_date_max(")
    )


def load_latest_p0_snapshot(log_dir: Path) -> Dict[str, Any]:
    """Load the latest p0_daily_check snapshot for adaptive entry policy."""
    out: Dict[str, Any] = {
        "path": None,
        "as_of_ymd": None,
        "risk_off_enabled": False,
        "risk_off_reasons": [],
        "kill_switch": {},
        "crash_risk_off": {},
    }
    try:
        p0p = latest_file(log_dir, "p0_daily_check_*.json")
        if not p0p:
            return out
        obj = json.loads(p0p.read_text(encoding="utf-8"))
        ro = obj.get("risk_off") if isinstance(obj.get("risk_off"), dict) else {}
        out["path"] = str(p0p)
        out["as_of_ymd"] = str(obj.get("as_of_ymd") or "") or None
        out["risk_off_enabled"] = bool(ro.get("enabled"))
        rr = ro.get("reasons") if isinstance(ro.get("reasons"), list) else []
        out["risk_off_reasons"] = [str(x) for x in rr]
        ks = obj.get("kill_switch") if isinstance(obj.get("kill_switch"), dict) else {}
        out["kill_switch"] = ks
        cro = obj.get("crash_risk_off") if isinstance(obj.get("crash_risk_off"), dict) else {}
        out["crash_risk_off"] = cro
        return out
    except Exception:
        return out



def load_latest_macro_snapshot(log_dir: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": None,
        "as_of_ymd": None,
        "regime": None,
        "risk_on": None,
        "crash_prob": None,
        "market_metrics": {},
    }
    try:
        p = latest_file(log_dir, "macro_signal_latest.json")
        if not p:
            return out
        obj = json.loads(p.read_text(encoding="utf-8"))
        out["path"] = str(p)
        out["as_of_ymd"] = str(obj.get("as_of_ymd") or "") or None
        out["regime"] = str(obj.get("regime") or "") or None
        out["risk_on"] = bool(obj.get("risk_on")) if ("risk_on" in obj) else None
        out["crash_prob"] = obj.get("crash_prob")
        mm = obj.get("market_metrics") if isinstance(obj.get("market_metrics"), dict) else {}
        out["market_metrics"] = mm
        return out
    except Exception:
        return out


def resolve_market_regime(cfg: Dict[str, Any], p0_snapshot: Dict[str, Any], macro_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    pol = cfg.get("regime_entry_policy") if isinstance(cfg, dict) else {}
    if not isinstance(pol, dict):
        pol = {}

    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "regime": "NORMAL",
        "day_ret": None,
        "macro_regime": str(macro_snapshot.get("regime") or "").upper(),
        "macro_risk_on": macro_snapshot.get("risk_on"),
        "reasons": [],
    }
    if not enabled:
        out["reasons"].append("policy_disabled")
        return out

    def _f(v: Any, d: float) -> float:
        try:
            return float(v)
        except Exception:
            return float(d)

    rally_day_ret_min = _f(pol.get("rally_day_ret_min", 0.025), 0.025)
    crash_day_ret_max = _f(pol.get("crash_day_ret_max", -0.025), -0.025)
    allow_rally_on_volatile = bool(pol.get("allow_rally_on_macro_volatile", True))
    allow_rally_when_macro_risk_off = bool(pol.get("allow_rally_when_macro_risk_off", True))

    day_ret = None
    # Prefer macro ret1 (market-level) over p0 crash fallback proxy.
    try:
        mm = macro_snapshot.get("market_metrics") if isinstance(macro_snapshot.get("market_metrics"), dict) else {}
        if "ret1" in mm:
            day_ret = float(mm.get("ret1"))
    except Exception:
        day_ret = None
    if day_ret is None:
        try:
            cro = p0_snapshot.get("crash_risk_off") if isinstance(p0_snapshot.get("crash_risk_off"), dict) else {}
            metrics = cro.get("metrics") if isinstance(cro.get("metrics"), dict) else {}
            if "day_ret" in metrics:
                day_ret = float(metrics.get("day_ret"))
        except Exception:
            day_ret = None
    out["day_ret"] = day_ret

    macro_regime = out["macro_regime"]
    macro_risk_on = out["macro_risk_on"]

    is_macro_crash = macro_regime in {"CRASH", "RATE_HIKE_FEAR"}
    is_macro_volatile = macro_regime == "VOLATILE"

    if is_macro_crash or (day_ret is not None and day_ret <= crash_day_ret_max):
        out["regime"] = "CRASH"
        out["reasons"].append("macro_or_dayret_crash")
        return out

    if day_ret is not None and day_ret >= rally_day_ret_min:
        if macro_risk_on is False and (not allow_rally_when_macro_risk_off):
            out["reasons"].append("macro_risk_off_blocks_rally")
        elif is_macro_volatile and not allow_rally_on_volatile:
            out["reasons"].append("macro_volatile_blocks_rally")
        else:
            out["regime"] = "RALLY"
            out["reasons"].append("dayret_rally")
            return out

    out["reasons"].append("normal_by_default")
    return out

def count_kill_switch_streak_days(log_dir: Path, max_scan_days: int = 30) -> int:
    """Count consecutive days where risk_off was enabled by kill_switch (non-hard-data reasons)."""
    files = sorted(log_dir.glob("p0_daily_check_*.json"), key=lambda p: p.name, reverse=True)
    seen_days: set[str] = set()
    streak = 0
    for p in files:
        m = re.search(r"p0_daily_check_(\d{8})_", p.name)
        if not m:
            continue
        ymd = m.group(1)
        if ymd in seen_days:
            continue
        seen_days.add(ymd)
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            break
        ro = obj.get("risk_off") if isinstance(obj.get("risk_off"), dict) else {}
        enabled = bool(ro.get("enabled"))
        reasons = ro.get("reasons") if isinstance(ro.get("reasons"), list) else []
        reasons = [str(x) for x in reasons]
        has_kill = "kill_switch" in reasons
        hard = any(_is_hard_block_reason(r) for r in reasons)
        if enabled and has_kill and not hard:
            streak += 1
            if streak >= int(max_scan_days):
                break
            continue
        break
    return int(streak)


def compute_adaptive_kill_cap(base_max_new: int, cfg: Dict[str, Any], p0_snapshot: Dict[str, Any], streak_days: int) -> Optional[Tuple[int, str]]:
    aec = cfg.get("adaptive_entry_control", {}) if isinstance(cfg, dict) else {}
    if not isinstance(aec, dict) or not bool(aec.get("enabled", False)):
        return None

    def _f(v: Any, d: float) -> float:
        try:
            return float(v)
        except Exception:
            return float(d)

    def _i(v: Any, d: int) -> int:
        try:
            return int(v)
        except Exception:
            return int(d)

    ks = p0_snapshot.get("kill_switch") if isinstance(p0_snapshot, dict) else {}
    metrics = ks.get("metrics") if isinstance(ks, dict) and isinstance(ks.get("metrics"), dict) else {}
    limits = ks.get("limits") if isinstance(ks, dict) and isinstance(ks.get("limits"), dict) else {}

    dd = abs(_f(metrics.get("max_drawdown_pct"), 0.0))
    dd_lim = abs(_f(limits.get("max_drawdown_pct"), 0.25))
    if dd_lim <= 0:
        return None
    dd_ratio = dd / dd_lim

    r_soft = _f(aec.get("dd_ratio_soft"), 1.00)
    r_mid = _f(aec.get("dd_ratio_mid"), 1.10)
    r_hard = _f(aec.get("dd_ratio_hard"), 1.25)
    f_soft = _f(aec.get("reduce_soft"), 0.50)
    f_mid = _f(aec.get("reduce_mid"), 0.30)
    f_hard = _f(aec.get("reduce_hard"), 0.15)
    probe_min = max(1, _i(aec.get("probe_min_new"), 1))
    relief_after = max(1, _i(aec.get("relief_after_streak_days"), 3))
    relief_min = max(1, _i(aec.get("relief_min_new"), 1))

    if dd_ratio >= r_hard:
        factor = f_hard
        band = "HARD"
    elif dd_ratio >= r_mid:
        factor = f_mid
        band = "MID"
    elif dd_ratio >= r_soft:
        factor = f_soft
        band = "SOFT"
    else:
        factor = min(1.0, max(f_soft, 0.0))
        band = "BELOW_SOFT"

    base = max(0, int(base_max_new))
    if base <= 0:
        return 0, "base_max_new=0"
    cap = int(math.floor(base * max(0.0, min(1.0, factor))))
    cap = max(probe_min, cap)
    if int(streak_days) >= relief_after:
        cap = max(cap, relief_min)
    cap = min(base, cap)
    detail = f"dd_ratio={dd_ratio:.3f} band={band} factor={factor:.2f} streak={int(streak_days)}"
    return int(cap), detail


def load_candidates_chosen_level(log_dir: Path) -> Optional[str]:
    """Return chosen_level from candidates_latest_meta.json, if available."""
    p = log_dir / "candidates_latest_meta.json"
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:
            return None
    if not isinstance(obj, dict):
        return None
    lv = obj.get("chosen_level")
    if lv is None:
        return None
    s = str(lv).strip().upper()
    return s if s else None

_RELAX_LEVEL_RE = re.compile(r"^L(\d+)$")

def parse_relax_level_num(chosen_level: Optional[str]) -> Optional[int]:
    if not chosen_level:
        return None
    m = _RELAX_LEVEL_RE.match(str(chosen_level).strip().upper())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _ops_policy(cfg: Dict[str, Any]) -> Dict[str, Any]:
    op = cfg.get("market_ops_policy", {}) if isinstance(cfg, dict) else {}
    return op if isinstance(op, dict) else {}


def _to_float(v: Any, d: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _to_int(v: Any, d: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(d)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _load_pending_signals(max_age_days: int) -> pd.DataFrame:
    if not PENDING_SIGNALS_PATH.exists():
        return pd.DataFrame()
    try:
        p = pd.read_csv(PENDING_SIGNALS_PATH)
    except Exception:
        return pd.DataFrame()
    if p.empty:
        return p
    if "signal_date" not in p.columns or "code" not in p.columns:
        return pd.DataFrame()

    p["signal_date"] = p["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    p["code"] = p["code"].astype(str).str.zfill(6)
    p = p[(p["signal_date"].str.len() == 8) & (p["code"].str.len() == 6)].copy()

    if max_age_days > 0:
        today = datetime.strptime(now_ymd(), "%Y%m%d")
        min_day = (today - timedelta(days=max_age_days)).strftime("%Y%m%d")
        p = p[p["signal_date"] >= min_day].copy()

    if p.empty:
        return p
    p = p.sort_values(["signal_date", "code"], ascending=[False, True]).drop_duplicates(["code", "signal_date"], keep="first")
    return p


def _save_pending_signals(rows: List[Dict[str, Any]], max_age_days: int) -> None:
    if not rows:
        return
    cur = _load_pending_signals(max_age_days=max_age_days)
    add = pd.DataFrame(rows)
    if add.empty:
        return
    all_df = pd.concat([cur, add], ignore_index=True)
    all_df["signal_date"] = all_df["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    all_df["code"] = all_df["code"].astype(str).str.zfill(6)
    all_df = all_df[(all_df["signal_date"].str.len() == 8) & (all_df["code"].str.len() == 6)].copy()

    if max_age_days > 0:
        today = datetime.strptime(now_ymd(), "%Y%m%d")
        min_day = (today - timedelta(days=max_age_days)).strftime("%Y%m%d")
        all_df = all_df[all_df["signal_date"] >= min_day].copy()

    if all_df.empty:
        try:
            if PENDING_SIGNALS_PATH.exists():
                PENDING_SIGNALS_PATH.unlink()
        except Exception:
            pass
        return

    all_df = all_df.sort_values(["signal_date", "code"], ascending=[False, True]).drop_duplicates(["code", "signal_date"], keep="first")
    all_df.to_csv(PENDING_SIGNALS_PATH, index=False, encoding="utf-8-sig")


def compute_dynamic_probe_floor(base_max_new: int, market_regime: str, regime_info: Dict[str, Any], cfg: Dict[str, Any], risk_off_hard: bool, universe_shrink: bool) -> int:
    op = _ops_policy(cfg)
    if not bool(op.get("enabled", False)):
        return 0
    if risk_off_hard:
        return 0

    day_ret_abs = abs(_to_float((regime_info or {}).get("day_ret"), 0.0))
    base = max(0, int(base_max_new))
    if base <= 0:
        return 0

    reg = str(market_regime or "NORMAL").upper()
    if reg == "RALLY":
        min_n = max(1, _to_int(op.get("probe_rally_min", 1), 1))
        ratio = _to_float(op.get("probe_rally_ratio_base", 0.10), 0.10) + day_ret_abs * _to_float(op.get("probe_rally_ratio_per_ret", 2.0), 2.0)
        ratio = min(_to_float(op.get("probe_rally_ratio_cap", 0.35), 0.35), ratio)
        if universe_shrink:
            ratio += _to_float(op.get("universe_shrink_probe_uplift", 0.05), 0.05)
        ratio = _clamp01(ratio)
        return min(base, max(min_n, int(math.ceil(base * ratio))))

    if reg == "CRASH":
        min_n = max(0, _to_int(op.get("probe_crash_min", 0), 0))
        ratio = _to_float(op.get("probe_crash_ratio_base", 0.03), 0.03) + day_ret_abs * _to_float(op.get("probe_crash_ratio_per_ret", 0.8), 0.8)
        ratio = min(_to_float(op.get("probe_crash_ratio_cap", 0.10), 0.10), ratio)
        ratio = _clamp01(ratio)
        return min(base, max(min_n, int(math.ceil(base * ratio))))

    return 0


def compute_participation_slo(market_regime: str, regime_info: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    op = _ops_policy(cfg)
    if not bool(op.get("enabled", False)):
        return 0.0
    reg = str(market_regime or "NORMAL").upper()
    day_ret_abs = abs(_to_float((regime_info or {}).get("day_ret"), 0.0))

    if reg == "RALLY":
        x = _to_float(op.get("slo_rally_base", 0.20), 0.20) + day_ret_abs * _to_float(op.get("slo_rally_per_ret", 2.0), 2.0)
        return _clamp01(min(_to_float(op.get("slo_rally_cap", 0.50), 0.50), x))
    if reg == "CRASH":
        x = _to_float(op.get("slo_crash_base", 0.05), 0.05) + day_ret_abs * _to_float(op.get("slo_crash_per_ret", 0.8), 0.8)
        return _clamp01(min(_to_float(op.get("slo_crash_cap", 0.20), 0.20), x))
    return _clamp01(_to_float(op.get("slo_normal", 0.10), 0.10))


def _write_ops_alert(payload: Dict[str, Any]) -> None:
    try:
        OPS_ALERT_LATEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _write_pending_status(payload: Dict[str, Any]) -> None:
    try:
        PENDING_STATUS_LATEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
def read_latest_stable_params() -> Dict[str, Any]:
    p = latest_file(RISK_DIR, "stable_params_v*.json")  # ??utils.common ????
    if not p:
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _max_date8_from_candidates(x: pd.DataFrame) -> str:
    if "date_yyyymmdd" in x.columns:
        ss = x["date_yyyymmdd"].astype(str)
    elif "date" in x.columns:
        ss = x["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    elif "signal_date" in x.columns:
        ss = x["signal_date"].astype(str)
    else:
        return ""
    ss = ss.str.replace(r"[^0-9]", "", regex=True).str[:8]
    ss = ss[ss.str.len() == 8]
    return str(ss.max()) if len(ss) else ""


def pick_candidates(cfg: Dict[str, Any]) -> pd.DataFrame:
    cpath = Path(cfg["candidates_latest_data"])
    if not cpath.exists():
        raise SystemExit(f"[FATAL] missing candidates file: {cpath}")
    df = pd.read_csv(cpath)

    # Prefer enriched sidecars in priority order.
    # This keeps base candidates immutable while allowing fail-soft fallbacks.
    d_base = _max_date8_from_candidates(df)
    sidecar_suffixes = [
        ".with_final_score.csv",
        ".with_news_score.csv",
        ".with_sector_score.csv",
    ]
    sidecar_paths: List[Path] = []
    for suffix in sidecar_suffixes:
        sidecar_paths.append(cpath.with_name(cpath.stem + suffix))
        canonical = LOG_DIR / ("candidates_latest_data" + suffix)
        if canonical not in sidecar_paths:
            sidecar_paths.append(canonical)

    for sidecar in sidecar_paths:
        if not sidecar.exists():
            continue
        try:
            sdf = pd.read_csv(sidecar)
        except Exception as e:
            print(f"[CAND] sidecar read failed: {sidecar.name} {type(e).__name__}: {e}")
            continue
        if "code" not in sdf.columns:
            print(f"[CAND] sidecar ignored (no code column): {sidecar.name}")
            continue
        d_side = _max_date8_from_candidates(sdf)
        if d_base and d_side and d_base != d_side:
            print(f"[CAND] sidecar stale: {sidecar.name} base={d_base} sidecar={d_side}")
            continue
        df = sdf
        print(f"[CAND] using sidecar: {sidecar.name} (date={d_side or d_base or 'n/a'})")
        break

    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] candidates has no 'code' column: cols={df.columns.tolist()}")

    if "date_yyyymmdd" in df.columns:
        df["signal_date"] = df["date_yyyymmdd"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    elif "date" in df.columns:
        df["signal_date"] = df["date"].astype(str).str.replace("-", "").str[:8]
    elif "signal_date" in df.columns:
        df["signal_date"] = df["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    else:
        df["signal_date"] = now_ymd()

    if "name" not in df.columns:
        df["name"] = ""

    df["code"] = df["code"].astype(str).str.zfill(6)

    op = _ops_policy(cfg)
    if bool(op.get("enabled", False)) and bool(op.get("carryover_no_next_day_enabled", True)):
        max_age = max(1, _to_int(op.get("carryover_max_age_days", 2), 2))
        pending = _load_pending_signals(max_age_days=max_age)
        if len(pending) > 0:
            pending["_carryover"] = 1
            if "_carryover" not in df.columns:
                df["_carryover"] = 0
            for c in df.columns:
                if c not in pending.columns:
                    pending[c] = pd.NA
            for c in pending.columns:
                if c not in df.columns:
                    df[c] = pd.NA
            df = pd.concat([df, pending[df.columns]], ignore_index=True)
            print(f"[CAND] carryover merged: +{len(pending)} rows from {PENDING_SIGNALS_PATH.name}")

    if len(df) > 0:
        if "_carryover" not in df.columns:
            df["_carryover"] = 0
        score_col = "final_score" if "final_score" in df.columns else ("score" if "score" in df.columns else None)
        if score_col:
            df["_score_dedup"] = pd.to_numeric(df[score_col], errors="coerce").fillna(-1e18)
            df = df.sort_values(["signal_date", "code", "_carryover", "_score_dedup"], ascending=[False, True, True, False])
        else:
            df = df.sort_values(["signal_date", "code", "_carryover"], ascending=[False, True, True])
        df = df.drop_duplicates(["code", "signal_date"], keep="first").copy()

    return df

def discover_recent_parquets(cfg: Dict[str, Any]) -> List[Path]:
    root = Path(cfg["parquet_root"]).resolve()
    top_n = int(cfg.get("parquet_top_n_recent", 120))

    cand: List[Tuple[float, Path]] = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.lower().endswith(".parquet"):
                continue
            p = Path(dirpath) / fn
            try:
                mt = p.stat().st_mtime
            except Exception:
                continue
            cand.append((mt, p))
            if len(cand) > top_n * 4:
                cand.sort(key=lambda x: x[0], reverse=True)
                cand = cand[:top_n]
    cand.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in cand[:top_n]]

def infer_colmap(cols: List[str]) -> Optional[ColMap]:
    lc = {c.lower(): c for c in cols}

    def pick(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in lc:
                return lc[k]
        return None

    date = pick(["date", "dt", "trade_date", "yyyymmdd", "ymd"])
    code = pick(["code", "ticker", "symbol"])
    o = pick(["open", "???"])
    h = pick(["high", "???????"])
    l = pick(["low", "?????????????"])
    c = pick(["close", "??????????"])
    name = pick(["name", "name_kor"])
    if not (date and code and o and h and l and c):
        return None
    return ColMap(date=date, code=code, open=o, high=h, low=l, close=c, name=name)

def load_prices_for_codes(cfg: Dict[str, Any], codes: List[str]) -> pd.DataFrame:
    """???????????????????????????????????????????(Parquet ??????????????????????????.

    ?????????????????????????????????????????
    - PyArrow??????????????????????????????????????????????????????????????????????????? ???????????????????????????????????????????????????⑤벡?????????됰Ŧ???????????꿔꺂?????????????????????????????????(I/O 50% ?????????????
    - ?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥???????????????????????????????????????????????????????????????????????????????????????????????????⑤벡?????????됰Ŧ???????????꿔꺂?????????????????????????????????????????????????????????????????????????????????????????????????????ш끽維뽳쭩?뱀땡???얩맪?????????
    """
    import pyarrow.parquet as pq

    recent = discover_recent_parquets(cfg)
    if not recent:
        raise SystemExit("[FATAL] no parquet found under parquet_root")

    max_open = int(cfg.get("parquet_max_open_files", 30))
    frames: List[pd.DataFrame] = []
    opened = 0

    for p in recent:
        if opened >= max_open:
            break
        try:
            # ????????????????????? ????????????????????????????????????????????????????????????????????????????????????ш끽維뽳쭩?뱀땡???얩맪?????????- ????????????????????????????????????????????
            pf = pq.ParquetFile(str(p))
            schema_cols = list(pf.schema_arrow.names)

            cm = infer_colmap(schema_cols)
            if not cm:
                continue

            # ?????????????????????熬곣뫖利당춯??쎾퐲???????????????????꿔꺂?㏘틠??怨몄젦????????????????????????거??????????????????????泥???????????????????????????????????????????????????????????????????????????????????????????????????⑤벡?????????됰Ŧ???????????꿔꺂??????????????????????????????????????????????????????????????????????諛몃마嶺뚮?????????????硫λ젒????????????????????遺얘턁??????얜Ŧ堉??????⑤뜪?????????????????????????癲?????????????????????????????1???????????????????????????????ш끽維뽳쭩?뱀땡???얩맪?????????
            cols = [cm.date, cm.code, cm.open, cm.high, cm.low, cm.close] + ([cm.name] if cm.name else [])
            df = pd.read_parquet(p, columns=cols, engine="pyarrow")

            ren = {cm.date: "date", cm.code: "code", cm.open: "open", cm.high: "high", cm.low: "low", cm.close: "close"}
            if cm.name:
                ren[cm.name] = "name"
            df = df.rename(columns=ren)
            frames.append(df)
            opened += 1
        except Exception as e:
            # ????????????????????????????????????????산뭐???????
            print(f"[WARN] parquet load skip: {p.name} - {type(e).__name__}")
            continue

    if not frames:
        raise SystemExit("[FATAL] parquet found but none matched OHLC schema (need date/code/open/high/low/close)")

    px = pd.concat(frames, ignore_index=True)
    px["code"] = px["code"].astype(str).str.zfill(6)

    if pd.api.types.is_datetime64_any_dtype(px["date"]):
        px["date"] = px["date"].dt.strftime("%Y%m%d")
    else:
        px["date"] = px["date"].astype(str).str.replace("-", "").str[:8]

    for col in ["open", "high", "low", "close"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")

    codes_set = set(norm_code(c) for c in codes if norm_code(c))
    if codes_set:
        px = px[px["code"].isin(codes_set)]
    else:
        # candidates may be empty on some days; keep all codes to avoid fatal
        print("[WARN] empty codes list; using all codes in price table")
    px = px.dropna(subset=["date", "open", "high", "low", "close"])

    # ?????????????????? 0??OHLC(?????????????????????????????????????????????⑤벡?????????????????????????????????????????????????????????⑤벡?????????됰Ŧ???????????꿔꺂??????????????)?????????????????
    px = px[(px["open"] > 0) & (px["high"] > 0) & (px["low"] > 0) & (px["close"] > 0)]

    if px.empty:
        raise SystemExit("[FATAL] price table empty after code filtering")

    # ?????????????????????????????⑤벡???????(code,date) ???????????????????釉먮폁????????꿔꺂???癰귥옖留????????????????????????????????????????????????????????? close ???????????????????????????????????????????????????????????????????????close) ???????????????
    px = px.sort_values(["code", "date", "close"]).drop_duplicates(["code", "date"], keep="last")
    px = px.sort_values(["code", "date"]).reset_index(drop=True)
    return px

def next_trading_date(px: pd.DataFrame, code: str, after_ymd: str) -> Optional[str]:
    # ???????????????????????????????⑤벡?????????? close>0 (?????????????????????????????????????????????⑤벡?????????????????????????????????????????????????????????⑤벡?????????됰Ŧ???????????꿔꺂?????????????? 0???????????????????????????????????????袁⑸즴筌?씛彛???돗??????????????癲ル슢二??곸젞???????????????????????됰Ŧ?????????????????????대첐?????????????????????????????????????????????????????????????????????????????????????산뭐??????? ??????????????????????????????
    d = px.loc[(px["code"] == code) & (px["date"] > after_ymd) & (px["close"] > 0), "date"]
    if d.empty:
        return None
    return str(d.iloc[0])

def get_ohlc(px: pd.DataFrame, code: str, day: str) -> Optional[Dict[str, float]]:
    r = px[(px["code"] == code) & (px["date"] == day)]
    if r.empty:
        return None
    x = r.iloc[0]
    return {"open": float(x["open"]), "high": float(x["high"]), "low": float(x["low"]), "close": float(x["close"])}

def calc_net_ret(entry: float, exit: float, fee_pct: float, slip_pct: float, sell_tax_pct: float = 0.0) -> float:
    # legacy SSOT (matches paper/trades.csv pnl_pct)
    # gross - ((entry+exit)/entry) * (fee+slip)
    if entry <= 0:
        return 0.0
    gross = (exit - entry) / entry
    cost = ((entry + exit) / entry) * (fee_pct + slip_pct)
    return gross - cost - float(sell_tax_pct or 0.0)

def append_rows(path: Path, rows: List[List[Any]]) -> None:
    # Best-effort durability: reduce chance of torn/partial rows on crash.
    # (Not a perfect transactional guarantee, but materially safer than buffered append.)
    import os
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r)
        f.flush()
        os.fsync(f.fileno())

def maybe_run_pnl_report() -> None:
    pnl = BASE_DIR / "paper_pnl_report.py"
    if pnl.exists():
        try:
            subprocess.run([sys.executable, str(pnl)], cwd=str(BASE_DIR))
        except Exception:
            pass

# read_csv_safe -> utils.common.read_csv_safe????????

def _sig_float(x: Any) -> str:
    try:
        v = float(x)
        return f"{v:.8f}"
    except Exception:
        return str(x)

def _legacy_trade_sig(row: pd.Series) -> str:
    # trade_id?????????????????????????????????????????룸챷援???????????????????????????????????????????????????????????????????????????????????????
    return "|".join([
        str(row.get("code", "")),
        str(row.get("entry_date", "")),
        _sig_float(row.get("entry_price", "")),
        str(row.get("exit_date", "")),
        _sig_float(row.get("exit_price", "")),
        _sig_float(row.get("pnl_pct", "")),
        str(row.get("exit_reason", "")),
        str(row.get("note", "")),
    ])

_SIGNAL_RE = re.compile(r"signal_date=(\d{8})")

def _extract_signal_date(note: Any) -> Optional[str]:
    if note is None:
        return None
    s = str(note)
    m = _SIGNAL_RE.search(s)
    return m.group(1) if m else None

def main() -> int:
    cfg = load_config()
    schema = detect_schema()

    # ensure files exist with detected schema
    if schema == "legacy":
        ensure_csv(FILLS, LEGACY_FILLS_HEADER)
        ensure_csv(TRADES, LEGACY_TRADES_HEADER)
    else:
        ensure_csv(FILLS, V411_FILLS_HEADER)
        ensure_csv(TRADES, V411_TRADES_HEADER)

    stable = read_latest_stable_params()
    stop_loss = float(stable.get("stop_loss", -0.05))
    take_profit = stable.get("take_profit", None)
    trail_pct = stable.get("trail_pct", None)

    fee_pct = float(cfg.get("fee_pct", 0.005))
    slip_pct = float(cfg.get("slippage_pct", 0.001))
    sell_tax_pct = float(cfg.get("sell_tax_pct", 0.0) or 0.0)

    p0_snapshot = load_latest_p0_snapshot(LOG_DIR)
    macro_snapshot = load_latest_macro_snapshot(LOG_DIR)
    regime_info = resolve_market_regime(cfg, p0_snapshot, macro_snapshot)
    market_regime = str(regime_info.get("regime") or "NORMAL").upper()
    regime_policy = cfg.get("regime_entry_policy", {}) if isinstance(cfg, dict) else {}
    if not isinstance(regime_policy, dict):
        regime_policy = {}
    ops_policy = _ops_policy(cfg)
    ops_enabled = bool(ops_policy.get("enabled", False))
    print(f"[REGIME] regime={market_regime} info={regime_info}")
    risk_off_enabled = bool(p0_snapshot.get("risk_off_enabled", False))
    risk_off_reasons = list(p0_snapshot.get("risk_off_reasons") or [])
    chosen_level = load_candidates_chosen_level(LOG_DIR)
    chosen_level_num = parse_relax_level_num(chosen_level)

    base_max_new = int(cfg.get("max_new_trades_per_day", 3))
    max_new = base_max_new

    aec = cfg.get("adaptive_entry_control", {}) if isinstance(cfg, dict) else {}
    adaptive_enabled = bool(isinstance(aec, dict) and aec.get("enabled", False))

    ks_cfg = cfg.get("kill_switch", {}) if isinstance(cfg, dict) else {}
    ks_mode = str(ks_cfg.get("mode", "REDUCE")).upper()
    try:
        ks_reduce_factor = float(ks_cfg.get("reduce_factor", 0.5) or 0.5)
    except Exception:
        ks_reduce_factor = 0.5
    try:
        ks_min_new = int(ks_cfg.get("min_new_trades_per_day", 1) or 1)
    except Exception:
        ks_min_new = 1

    crash_cfg = cfg.get("crash_risk_off", {}) if isinstance(cfg, dict) else {}
    crash_mode = str(crash_cfg.get("mode", "BLOCK")).upper()
    try:
        crash_reduce_factor = float(crash_cfg.get("reduce_factor", 0.5) or 0.5)
    except Exception:
        crash_reduce_factor = 0.5
    try:
        crash_min_new = int(crash_cfg.get("min_new_trades_per_day", 1) or 1)
    except Exception:
        crash_min_new = 1

    kill_streak_days = count_kill_switch_streak_days(LOG_DIR, max_scan_days=30)
    risk_off_hard = False
    risk_off_has_kill = False
    if risk_off_enabled:
        reasons = list(risk_off_reasons or [])
        msg = "; ".join(reasons) if reasons else "(no reasons)"

        hard_reasons = [r for r in reasons if _is_hard_block_reason(r)]
        risk_off_hard = bool(hard_reasons)
        if hard_reasons:
            max_new = 0
            print(f"[PAPER_ENGINE] risk_off=True -> BLOCK new entries (hard-data). reasons={msg}")
        elif any("kill_switch" in str(r) for r in reasons):
            risk_off_has_kill = True
            adaptive_used = False
            if adaptive_enabled and bool(aec.get("kill_switch_override_block", True)):
                cap_info = compute_adaptive_kill_cap(base_max_new, cfg, p0_snapshot, kill_streak_days)
                if cap_info is not None:
                    cap, detail = cap_info
                    max_new = max(0, int(cap))
                    adaptive_used = True
                    print(f"[PAPER_ENGINE] risk_off=True -> ADAPTIVE REDUCE max_new={max_new} ({detail}). reasons={msg}")
            if not adaptive_used:
                if ks_mode == "REDUCE":
                    max_new = max(ks_min_new, int(math.floor(base_max_new * ks_reduce_factor)))
                    print(f"[PAPER_ENGINE] risk_off=True -> REDUCE new entries to max_new={max_new}. reasons={msg}")
                else:
                    if bool(aec.get("kill_switch_block_fallback_reduce", True)):
                        max_new = max(1, ks_min_new)
                        print(f"[PAPER_ENGINE] risk_off=True -> SOFT REDUCE fallback max_new={max_new} (kill_switch mode=BLOCK). reasons={msg}")
                    else:
                        max_new = 0
                        print(f"[PAPER_ENGINE] risk_off=True -> BLOCK new entries. reasons={msg}")
        elif any("crash_risk_off" in str(r) for r in reasons):
            if crash_mode == "REDUCE":
                max_new = max(crash_min_new, int(math.floor(base_max_new * crash_reduce_factor)))
                print(f"[PAPER_ENGINE] risk_off=True -> REDUCE new entries to max_new={max_new}. reasons={msg}")
            else:
                max_new = 0
                print(f"[PAPER_ENGINE] risk_off=True -> BLOCK new entries. reasons={msg}")
        else:
            max_new = 0
            print(f"[PAPER_ENGINE] risk_off=True -> BLOCK new entries. reasons={msg}")
    # Regime policy override (operations matrix)
    if bool(regime_policy.get("enabled", False)):
        if market_regime == "CRASH" and bool(regime_policy.get("macro_hard_block_enabled", False)) and bool(regime_policy.get("crash_force_block", True)):
            if max_new != 0:
                print("[REGIME] CRASH -> force BLOCK new entries")
            max_new = 0
        elif market_regime == "RALLY":
            if (
                bool(regime_policy.get("rally_probe_under_kill_switch_block", True))
                and risk_off_enabled
                and risk_off_has_kill
                and (not risk_off_hard)
                and max_new <= 0
            ):
                try:
                    rally_probe_max_new = int(regime_policy.get("rally_probe_max_new", 1) or 1)
                except Exception:
                    rally_probe_max_new = 1
                rally_probe_max_new = max(1, rally_probe_max_new)
                max_new = min(base_max_new, rally_probe_max_new)
                print(f"[REGIME] RALLY -> probe reopen max_new={max_new} under kill_switch block")

    # Relax-ladder safety cap: tighten max_new when candidate filters were overly relaxed.
    if chosen_level_num is not None:
        if chosen_level_num >= 6:
            max_new = 0
            print(f"[PAPER_ENGINE] chosen_level={chosen_level} -> BLOCK new entries (max_new=0)")
        elif chosen_level_num >= 5:
            try:
                l5_factor = float((aec or {}).get("dynamic_relax_l5_factor", 0.10) or 0.10)
            except Exception:
                l5_factor = 0.10
            l5_factor = max(0.0, min(1.0, l5_factor))
            l5_cap = 0 if base_max_new <= 0 else max(1, int(math.floor(base_max_new * l5_factor)))
            max_new = min(max_new, l5_cap)
            print(f"[PAPER_ENGINE] chosen_level={chosen_level} -> DYNAMIC CAP new entries to max_new={max_new} (l5_factor={l5_factor:.2f})")
        elif chosen_level_num >= 4:
            half_cap = 0 if base_max_new <= 0 else max(1, int(math.floor(base_max_new * 0.5)))
            max_new = min(max_new, half_cap)
            print(f"[PAPER_ENGINE] chosen_level={chosen_level} -> CAP new entries to max_new={max_new}")
    max_hold_days = int(cfg.get("max_hold_days", 10))
    sizing_mode = str(cfg.get("sizing_mode", "fixed_qty"))
    allow_reentry = bool(cfg.get("allow_same_code_reentry", False))
    max_positions = int(cfg.get("max_positions", 0) or 0)

    def _pct01(v: Any, default: float) -> float:
        try:
            x = float(v)
        except Exception:
            x = float(default)
        if x > 1.0 and x <= 100.0:
            x = x / 100.0
        return max(0.0, min(1.0, x))

    capital_total = float(cfg.get("capital_total", 0) or 0)
    max_gross_exposure_pct = _pct01(cfg.get("max_gross_exposure_pct", 1.0), 1.0)
    max_daily_new_exposure_pct = _pct01(cfg.get("max_daily_new_exposure_pct", 1.0), 1.0)
    max_per_sector_runtime = int(cfg.get("max_per_sector", 0) or 0)
    max_per_symbol_exposure_pct = _pct01(cfg.get("max_per_symbol_exposure_pct", 0.0), 0.0)

    gap_up_max_pct_runtime = float(cfg.get("gap_up_max_pct", 0.0) or 0.0)
    entry_gap_down_stop_pct_runtime = float(cfg.get("entry_gap_down_stop_pct", 0.0) or 0.0)

    if bool(regime_policy.get("enabled", False)) and market_regime == "RALLY":
        max_gross_exposure_pct = min(
            max_gross_exposure_pct,
            _pct01(regime_policy.get("rally_max_gross_exposure_pct", max_gross_exposure_pct), max_gross_exposure_pct),
        )
        max_daily_new_exposure_pct = min(
            max_daily_new_exposure_pct,
            _pct01(regime_policy.get("rally_max_daily_new_exposure_pct", max_daily_new_exposure_pct), max_daily_new_exposure_pct),
        )
        try:
            rally_mps = int(regime_policy.get("rally_max_per_sector", max_per_sector_runtime) or max_per_sector_runtime)
            if rally_mps > 0:
                if max_per_sector_runtime > 0:
                    max_per_sector_runtime = min(max_per_sector_runtime, rally_mps)
                else:
                    max_per_sector_runtime = rally_mps
        except Exception:
            pass

        try:
            rally_gap_up = float(regime_policy.get("rally_gap_up_max_pct", gap_up_max_pct_runtime) or gap_up_max_pct_runtime)
            if rally_gap_up > 0:
                if gap_up_max_pct_runtime > 0:
                    gap_up_max_pct_runtime = min(gap_up_max_pct_runtime, rally_gap_up)
                else:
                    gap_up_max_pct_runtime = rally_gap_up
        except Exception:
            pass

        try:
            rally_gap_down = float(regime_policy.get("rally_entry_gap_down_stop_pct", entry_gap_down_stop_pct_runtime) or entry_gap_down_stop_pct_runtime)
            if rally_gap_down > 0:
                entry_gap_down_stop_pct_runtime = max(entry_gap_down_stop_pct_runtime, abs(rally_gap_down))
        except Exception:
            pass

        print(
            f"[REGIME] RALLY caps -> gross={max_gross_exposure_pct:.3f} daily={max_daily_new_exposure_pct:.3f} "
            f"max_per_sector={max_per_sector_runtime} gap_up={gap_up_max_pct_runtime:.3f} "
            f"gap_down_stop={entry_gap_down_stop_pct_runtime:.3f}"
        )

    cdf = pick_candidates(cfg)
    rank_col = "final_score" if "final_score" in cdf.columns else ("score" if "score" in cdf.columns else None)
    if rank_col:
        print(f"[CAND] ranking key: {rank_col}")

    cap_n_runtime = int(cfg.get("cap_signal_top_n", 0) or 0)
    max_per_sector = int(max_per_sector_runtime or 0)

    raw_cand_count = len(cdf)
    shrink_min_candidates = max(1, _to_int(ops_policy.get("universe_shrink_min_candidates", 8), 8))
    universe_shrink_candidates = raw_cand_count <= shrink_min_candidates if raw_cand_count > 0 else False
    if ops_enabled and universe_shrink_candidates:
        print(f"[OPS] universe_shrink detected: candidates={raw_cand_count} <= {shrink_min_candidates}")
        if cap_n_runtime > 0 and bool(ops_policy.get("universe_shrink_disable_signal_cap", True)):
            print(f"[OPS] universe_shrink -> disable cap_signal_top_n (was {cap_n_runtime})")
            cap_n_runtime = 0
        if max_per_sector > 0 and bool(ops_policy.get("universe_shrink_disable_sector_cap", True)):
            print(f"[OPS] universe_shrink -> disable max_per_sector cap (was {max_per_sector})")
            max_per_sector = 0

    probe_floor = compute_dynamic_probe_floor(
        base_max_new=base_max_new,
        market_regime=market_regime,
        regime_info=regime_info,
        cfg=cfg,
        risk_off_hard=risk_off_hard,
        universe_shrink=universe_shrink_candidates,
    )
    if probe_floor > 0 and max_new < probe_floor:
        old_max_new = max_new
        max_new = min(base_max_new, probe_floor)
        print(f"[OPS] dynamic_probe_floor applied: {old_max_new}->{max_new} (regime={market_regime})")

    # === CAP: limit entries per signal_date (tail-risk control) ===
    if cap_n_runtime > 0 and ("signal_date" in cdf.columns):
        if rank_col:
            cdf["_score_n"] = pd.to_numeric(cdf[rank_col], errors="coerce")
            tie_col = "trading_value" if "trading_value" in cdf.columns else ("value" if "value" in cdf.columns else None)
            if tie_col:
                cdf["_tie_n"] = pd.to_numeric(cdf[tie_col], errors="coerce")
                cdf = cdf.sort_values(["signal_date", "_score_n", "_tie_n"], ascending=[True, False, False])
            else:
                cdf = cdf.sort_values(["signal_date", "_score_n"], ascending=[True, False])

            cdf["_rk_sig"] = cdf.groupby("signal_date").cumcount() + 1
            before = len(cdf)
            cdf = cdf[cdf["_rk_sig"] <= cap_n_runtime].copy()
            after = len(cdf)
            print(f"[CAP] cap_signal_top_n={cap_n_runtime} applied: {before}->{after}")
        else:
            print("[CAP] ranking column missing (need final_score or score); cap_signal_top_n ignored")

    # === CAP: limit entries per sector per signal_date ===
    if max_per_sector > 0:
        sector_col = "sector_code" if "sector_code" in cdf.columns else None
        if not sector_col:
            print("[CAP] max_per_sector set but sector_code column missing; ignored")
        else:
            cdf["_sector_key"] = cdf[sector_col].astype(str).str.strip()
            miss = (cdf["_sector_key"] == "") | (cdf["_sector_key"].str.lower() == "nan")
            cdf.loc[miss, "_sector_key"] = "__NA__" + cdf.loc[miss, "code"].astype(str).str.zfill(6)

            if rank_col:
                cdf["_score_sec"] = pd.to_numeric(cdf[rank_col], errors="coerce")
                cdf = cdf.sort_values(["signal_date", "_sector_key", "_score_sec"], ascending=[True, True, False])

            cdf["_rk_sector"] = cdf.groupby(["signal_date", "_sector_key"]).cumcount() + 1
            before = len(cdf)
            cdf = cdf[cdf["_rk_sector"] <= max_per_sector].copy()
            after = len(cdf)
            print(f"[CAP] max_per_sector={max_per_sector} applied: {before}->{after}")

    # Prioritize deferred carryover signals so queued entries are consumed on next trading day.
    if len(cdf) > 0:
        if "_carryover" not in cdf.columns:
            cdf["_carryover"] = 0
        cdf["_carryover"] = pd.to_numeric(cdf["_carryover"], errors="coerce").fillna(0).astype(int)
        if rank_col:
            cdf["_rank_runtime"] = pd.to_numeric(cdf[rank_col], errors="coerce")
            cdf = cdf.sort_values(["_carryover", "signal_date", "_rank_runtime"], ascending=[False, True, False])
        else:
            cdf = cdf.sort_values(["_carryover", "signal_date", "code"], ascending=[False, True, True])
    state = load_state()

    # ---- ????????????????????????釉먮폁????????꿔꺂???癰귥옖留???????????????????????????????????????????????????????????????꾩룆梨띰쭕?뚢뵾??????????????嶺뚮죭?댁젘??????????????????????釉먮폁???????????????????살몝??????????????????????????????????????????????????????????????????????????????????? ???????????????????釉먮폁????????꿔꺂???癰귥옖留????????????????????????????????????????????????????----
    processed_signals = set(str(x) for x in (state.get("processed_signals") or []))

    existing_fill_order_ids: set[str] = set()
    existing_trade_sigs: set[str] = set()

    if schema == "legacy":
        df_fills = read_csv_safe(FILLS)
        if df_fills is not None and "order_id" in df_fills.columns:
            existing_fill_order_ids = set(df_fills["order_id"].astype(str).tolist())

        df_trades = read_csv_safe(TRADES)
        if df_trades is not None and len(df_trades) > 0:
            # trades ???????????????????????processed_signals????????????????????????????⑤벡????????????????????????????????????????????????????????????????????????????????????????쎛?????????????????????釉먮폁????????꿔꺂???癰귥옖留???????????????????????????????????????????????????????????????????????????????꾩룆梨띰쭕?뚢뵾??????????????嶺뚮죭?댁젘??????????????????????釉먮폁???????????????????살몝???????????????????????????????????????????????????????????????????????????????????)
            if "code" in df_trades.columns and "note" in df_trades.columns:
                for _, rr in df_trades.iterrows():
                    code = str(rr.get("code", "")).zfill(6)
                    sd = _extract_signal_date(rr.get("note"))
                    if sd:
                        processed_signals.add(f"{code}:{sd}")

            # trade signature set
            try:
                for _, rr in df_trades.iterrows():
                    existing_trade_sigs.add(_legacy_trade_sig(rr))
            except Exception:
                pass

    open_pos: List[Dict[str, Any]] = state.get("open_positions", []) or []
    open_codes = set(norm_code(p.get("code", "")) for p in open_pos if norm_code(p.get("code", "")))

    current_open_notional = 0.0
    open_notional_by_code: Dict[str, float] = {}
