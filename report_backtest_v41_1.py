# -*- coding: utf-8 -*-
"""v41.1 historical backtest report (operational-rule aligned)."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from utils.stable_params_gate import evaluate_stable_params, load_stable_quality_gate
from utils.price_history_contract import apply_price_history_contract
from generate_candidates_v41_1 import _relax_ladder

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
STABLE_PARAMS = RC_DIR / "stable_params_v41_1.json"


def _env_flag(name: str) -> bool:
    return str(os.environ.get(name, "0")).strip().lower() in {"1", "true", "yes", "y"}


def _load_param_file(path: Path) -> Dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(raw, dict) and isinstance(raw.get("params"), dict):
        return dict(raw["params"])
    if isinstance(raw, dict):
        return dict(raw)
    raise ValueError(f"invalid param file payload: {path}")


def _load_symbol_panel() -> pd.DataFrame:
    """Load latest symbol panel (date x code) with sector and market-cap snapshots.

    The panel is produced by tools/build_backtest_symbol_panel_csv.py from
    krx_daily parquet files plus CACHE_DIR sector mappings and the latest
    _krx_manual market-cap snapshot.  Coverage is >97% for both sector and
    market_cap, so it is sufficient for backtest analysis and defensive filters.
    """
    if not SYMBOL_PANEL_CSV.exists():
        return pd.DataFrame(columns=["date", "code", "sector", "sector_code", "market_cap", "listed_shares"])

    panel = pd.read_csv(SYMBOL_PANEL_CSV, dtype={"code": str}, encoding="utf-8-sig")
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.normalize()
    panel["code"] = panel["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)

    # Map krx_sector name -> sector_code using the same SSOT mapping as production.
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


def _report_research_mode() -> bool:
    return _env_flag("REPORT_RESEARCH_MODE")


def _report_param_path() -> Path:
    if not _report_research_mode():
        return STABLE_PARAMS
    raw = str(os.environ.get("REPORT_RESEARCH_PARAMS_PATH", "")).strip()
    if not raw:
        raise RuntimeError("REPORT_RESEARCH_MODE=1 requires REPORT_RESEARCH_PARAMS_PATH")
    return Path(raw)


def _report_output_dir() -> Path:
    if not _report_research_mode():
        return RC_DIR
    raw = str(os.environ.get("REPORT_RESEARCH_OUTPUT_DIR", "")).strip()
    if not raw:
        raise RuntimeError("REPORT_RESEARCH_MODE=1 requires REPORT_RESEARCH_OUTPUT_DIR")
    return Path(raw)

def _load_research_selection_contract() -> Optional[Dict[str, Any]]:
    if not _report_research_mode():
        return None
    raw = str(os.environ.get("REPORT_RESEARCH_SELECTION_CONTRACT_PATH", "")).strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.exists():
        raise FileNotFoundError(f"missing selection contract: {path}")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid selection contract payload: {path}")
    contract = payload.get("selection_contract", payload)
    if not isinstance(contract, dict):
        raise ValueError(f"invalid selection_contract object: {path}")
    out: Dict[str, Any] = {}
    if contract.get("required_signals") is not None:
        out["required_signals"] = [str(x) for x in contract.get("required_signals", [])]
    if contract.get("inverse_signals") is not None:
        out["inverse_signals"] = [str(x) for x in contract.get("inverse_signals", [])]
    if contract.get("regimes") is not None:
        out["regimes"] = [str(x).upper() for x in contract.get("regimes", [])]
    if contract.get("signal_date_start") is not None:
        out["signal_date_start"] = str(contract.get("signal_date_start"))
    if contract.get("signal_date_end") is not None:
        out["signal_date_end"] = str(contract.get("signal_date_end"))
    if contract.get("numeric_filters") is not None:
        filters = contract.get("numeric_filters", [])
        if not isinstance(filters, list):
            raise ValueError("selection_contract.numeric_filters must be a list")
        out["numeric_filters"] = [dict(x) for x in filters]
    if contract.get("entry_trigger") is not None:
        trigger = contract.get("entry_trigger", {})
        if not isinstance(trigger, dict):
            raise ValueError("selection_contract.entry_trigger must be an object")
        out["entry_trigger"] = dict(trigger)
    if contract.get("exit_overrides") is not None:
        overrides = contract.get("exit_overrides", {})
        if not isinstance(overrides, dict):
            raise ValueError("selection_contract.exit_overrides must be an object")
        out["exit_overrides"] = dict(overrides)
    out["contract_path"] = str(path)
    return out

PAPER_ENGINE_CONFIG = BASE_DIR / "paper" / "paper_engine_config.json"

CACHE_LISTING = BASE_DIR / "_cache" / "krx_listing.csv"
DART_FUND = BASE_DIR / "_cache" / "dart_fundamental_latest.csv"
PARQUET_GLOB_1 = "krx_daily_*_clean.parquet"
PARQUET_GLOB_2 = "krx_daily_*.parquet"
SYMBOL_PANEL_CSV = BASE_DIR / "2_Logs" / "backtest_symbol_panel_latest.csv"

TRAIN_END = pd.Timestamp("2023-12-31")
VAL_END = pd.Timestamp("2024-12-31")

DEFAULT_SL = -0.05
MIN_SL_CAP = -0.30

DEFAULT_REPORT_PARAMS = {
    "rs_lim": 1.70,
    "v_accel_lim": 2.50,
    "v_accel_max": 5.00,
    "defense_bear_rs_slope_min": 0.0,
    "defense_bear_disable_entry": 0.0,
    "stretch_max": 1.19,
    "value_min": 1_000_000_000.0,
    "atr_max": 0.12,
    "gap_limit": None,
    "rsi_max": 70.0,
    "require_macd_golden": 1.0,
    "vol_close_corr_min": 0.0,
    "near_52w_high_gap_max": 0.25,
    "min_listing_days": 126.0,
    "sector_blacklist": "",  # comma-separated sector codes (e.g. "005,024")
    "sector_max_per_day": 0,  # 0 = no limit
    "min_market_cap": 0.0,  # 0 = no minimum
    "require_above_ma200": 0.0,  # 1.0 => require close > ma200
    "use_relax_ladder": 1.0,
    "hold": 10,
    "max_pos": 20,
    "w_rs": 0.20,
    "w_rs_slope": 0.55,
    "w_v_accel": 0.25,
    "stop_loss": None,
    "take_profit": None,
    "trail_pct": None,
}


def _fix_stop_loss(val):
    if val is None:
        return DEFAULT_SL
    try:
        v = float(val)
    except Exception:
        return DEFAULT_SL
    if v >= 0:
        return DEFAULT_SL
    if v < MIN_SL_CAP:
        return MIN_SL_CAP
    return v


def _parse_mixed_date_series(s: pd.Series) -> pd.Series:
    if np.issubdtype(s.dtype, np.datetime64):
        return pd.to_datetime(s, errors="coerce")
    d = s.astype(str).str.strip()
    dt = pd.to_datetime(d, errors="coerce", format="mixed")
    mask = dt.isna() & d.str.match(r"^\d{8}$", na=False)
    if mask.any():
        dt2 = pd.to_datetime(d[mask], errors="coerce", format="%Y%m%d")
        dt.loc[mask] = dt2
    return dt


def _load_paper_gap_policy() -> tuple[float, float]:
    if not PAPER_ENGINE_CONFIG.exists():
        return 0.0, 0.0
    try:
        j = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
        gu = float(j.get("gap_up_max_pct", 0.0) or 0.0)
        gd = float(j.get("entry_gap_down_stop_pct", 0.0) or 0.0)
        return gu, gd
    except Exception:
        return 0.0, 0.0


def _load_paper_exit_policy() -> tuple[list[float], list[float], float, float]:
    tp_levels = [0.20, 0.50, 1.00]
    tp_ratios = [0.30, 0.30, 0.40]
    trailing_activation = 0.20
    trailing_pct = -0.15
    if not PAPER_ENGINE_CONFIG.exists():
        return tp_levels, tp_ratios, trailing_activation, trailing_pct
    try:
        j = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
        sell_rules = j.get("sell_rules") if isinstance(j.get("sell_rules"), dict) else {}
        tp = sell_rules.get("take_profit") if isinstance(sell_rules.get("take_profit"), dict) else {}
        stop = sell_rules.get("stop_loss") if isinstance(sell_rules.get("stop_loss"), dict) else {}
        lv_raw = tp.get("levels") if isinstance(tp.get("levels"), list) else []
        rt_raw = tp.get("ratios") if isinstance(tp.get("ratios"), list) else []
        if lv_raw:
            tp_levels = [float(x) / 100.0 for x in lv_raw if x is not None]
        if rt_raw:
            tp_ratios = [float(x) / 100.0 for x in rt_raw if x is not None]
        if "trailing_stop_activation_profit_pct" in stop:
            trailing_activation = float(stop.get("trailing_stop_activation_profit_pct")) / 100.0
        if "trailing_stop_pct" in stop:
            trailing_pct = float(stop.get("trailing_stop_pct")) / 100.0
    except Exception:
        return tp_levels, tp_ratios, trailing_activation, trailing_pct
    return tp_levels, tp_ratios, trailing_activation, trailing_pct


def _load_paper_sell_rules() -> Dict[str, Any]:
    if not PAPER_ENGINE_CONFIG.exists():
        return {}
    try:
        j = json.loads(PAPER_ENGINE_CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {}
    sell_rules = j.get("sell_rules")
    return sell_rules if isinstance(sell_rules, dict) else {}


def _load_stable_quality_gate() -> Dict[str, Any]:
    return load_stable_quality_gate(PAPER_ENGINE_CONFIG)


def _stable_gate_status(p: Dict[str, Any], gate: Dict[str, Any]) -> Dict[str, Any]:
    return evaluate_stable_params(p, gate)


def _load_fundamental_map() -> Dict[str, Dict[str, Any]]:
    if not DART_FUND.exists():
        return {}
    try:
        f = pd.read_csv(DART_FUND, dtype={"code": str}, encoding="utf-8")
    except Exception:
        try:
            f = pd.read_csv(DART_FUND, dtype={"code": str}, encoding="cp949")
        except Exception:
            return {}
    if f.empty or "code" not in f.columns:
        return {}
    f["code"] = f["code"].astype(str).str.zfill(6)
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in f.iterrows():
        out[str(r["code"])] = {
            "debt_ratio": pd.to_numeric(r.get("debt_ratio"), errors="coerce"),
            "roe": pd.to_numeric(r.get("ROE"), errors="coerce"),
            "revenue_growth": pd.to_numeric(r.get("revenue_growth"), errors="coerce"),
            "opm": pd.to_numeric(r.get("OPM"), errors="coerce"),
            "npm": pd.to_numeric(r.get("NPM"), errors="coerce"),
        }
    return out


def _calc_ma(values: List[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    arr = values[-period:]
    return float(np.mean(arr)) if arr else None


def _calc_rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    arr = np.asarray(values, dtype=float)
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / (avg_loss + 1e-12)
    return float(100.0 - (100.0 / (1.0 + rs)))


@dataclass
class Params:
    rs_lim: float
    v_accel_lim: float
    stretch_max: float
    value_min: float
    v_accel_max: float = 5.0
    defense_bear_rs_slope_min: float = 0.0
    defense_bear_disable_entry: bool = False
    atr_max: Optional[float] = None
    gap_limit: Optional[float] = None
    gap_up_max_pct: float = 0.0
    entry_gap_down_stop_pct: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trail_pct: Optional[float] = None
    hold: int = 10
    max_pos: int = 20
    w_rs: float = 0.2
    w_rs_slope: float = 0.55
    w_v_accel: float = 0.25
    rsi_max: float = 70.0
    require_macd_golden: bool = False
    vol_close_corr_min: float = 0.0
    near_52w_high_gap_max: float = 0.25
    min_listing_days: float = 126.0
    sector_blacklist: str = ""
    sector_max_per_day: int = 0
    min_market_cap: float = 0.0
    require_above_ma200: bool = False
    mkt_ret20_min: float = -1.0
    mkt_ret60_min: float = -1.0
    sector_rs_min: float = -1.0
    use_relax_ladder: bool = True
    cost: float = 0.005
    slippage: float = 0.001
    tp_levels: Tuple[float, ...] = (0.20, 0.50, 1.00)
    tp_ratios: Tuple[float, ...] = (0.30, 0.30, 0.40)
    trailing_activation_profit_pct: float = 0.20
    entry_enabled: bool = True
    entry_block_reason: str = ""
    param_gate: Optional[Dict[str, Any]] = None


def load_params() -> Params:
    param_path = _report_param_path()
    research_mode = _report_research_mode()
    if not param_path.exists():
        raise FileNotFoundError(f"missing: {param_path}")

    stable_raw = _load_param_file(param_path)
    if research_mode and _env_flag("REPORT_RESEARCH_DISABLE_RELAX"):
        stable_raw["use_relax_ladder"] = 0.0
    gate = _load_stable_quality_gate()
    gate_status = _stable_gate_status(stable_raw, gate)
    allow_unapproved_fallback = str(os.environ.get("REPORT_ALLOW_UNAPPROVED_FALLBACK", "0")).strip() == "1"
    if gate_status["ok"]:
        p = stable_raw
        param_source = "stable"
        entry_enabled = True
        entry_block_reason = ""
    elif allow_unapproved_fallback:
        p = dict(DEFAULT_REPORT_PARAMS)
        if research_mode:
            p.update(stable_raw)
        param_source = "research_unapproved_fallback" if research_mode else "default_fallback_research"
        entry_enabled = True
        entry_block_reason = "research_fallback_unapproved" if research_mode else ""
    else:
        p = stable_raw
        param_source = "stable_rejected_blocked"
        entry_enabled = False
        entry_block_reason = str(gate_status["reason"])

    def g(k, default=None):
        return p.get(k, default)

    gu_cfg, gd_cfg = _load_paper_gap_policy()
    tp_levels_cfg, tp_ratios_cfg, trail_activation_cfg, trail_pct_cfg = _load_paper_exit_policy()

    params = Params(
        rs_lim=float(g("rs_lim", 1.7)),
        v_accel_lim=float(g("v_accel_lim", 2.5)),
        stretch_max=float(g("stretch_max", 1.19)),
        v_accel_max=float(g("v_accel_max", 5.0)),
        defense_bear_rs_slope_min=float(g("defense_bear_rs_slope_min", 0.0)),
        defense_bear_disable_entry=(float(g("defense_bear_disable_entry", 0.0) or 0.0) >= 0.5),
        value_min=float(g("value_min", 105_000_000_000.0)),
        atr_max=(float(g("atr_max")) if g("atr_max") is not None else None),
        gap_limit=(float(g("gap_limit")) if g("gap_limit") is not None else None),
        gap_up_max_pct=float(g("gap_up_max_pct", gu_cfg)),
        entry_gap_down_stop_pct=float(g("entry_gap_down_stop_pct", gd_cfg)),
        stop_loss=(float(g("stop_loss")) if g("stop_loss") is not None else None),
        take_profit=(float(g("take_profit")) if g("take_profit") is not None else None),
        trail_pct=(float(g("trail_pct")) if g("trail_pct") is not None else None),
        hold=int(g("hold", 10)),
        max_pos=int(g("max_pos", 20)),
        w_rs=float(g("w_rs", 0.2)),
        w_rs_slope=float(g("w_rs_slope", 0.55)),
        w_v_accel=float(g("w_v_accel", 0.25)),
        rsi_max=float(g("rsi_max", 70.0)),
        require_macd_golden=(float(g("require_macd_golden", 0.0) or 0.0) >= 0.5),
        vol_close_corr_min=float(g("vol_close_corr_min", 0.0)),
        near_52w_high_gap_max=float(g("near_52w_high_gap_max", 0.25)),
        min_listing_days=float(g("min_listing_days", 126.0)),
        sector_blacklist=str(g("sector_blacklist", "") or ""),
        sector_max_per_day=int(g("sector_max_per_day", 0) or 0),
        min_market_cap=float(g("min_market_cap", 0.0) or 0.0),
        require_above_ma200=(float(g("require_above_ma200", 0.0) or 0.0) >= 0.5),
        mkt_ret20_min=float(g("mkt_ret20_min", -1.0)),
        mkt_ret60_min=float(g("mkt_ret60_min", -1.0)),
        sector_rs_min=float(g("sector_rs_min", -1.0)),
        use_relax_ladder=(float(g("use_relax_ladder", 1.0)) >= 0.5),
        tp_levels=tuple(tp_levels_cfg),
        tp_ratios=tuple(tp_ratios_cfg),
        trailing_activation_profit_pct=float(trail_activation_cfg),
        entry_enabled=entry_enabled,
        entry_block_reason=entry_block_reason,
        param_gate={
            "param_source": param_source,
            "stable_path": str(param_path),
            "research_mode": bool(research_mode),
            "research_params_path": str(param_path) if research_mode else "",
            "research_output_dir": str(_report_output_dir()) if research_mode else "",
            "research_disable_relax": bool(_env_flag("REPORT_RESEARCH_DISABLE_RELAX")) if research_mode else False,
            "official_use_allowed": bool((not research_mode) and gate_status["ok"]),
            "research_selection_contract_path": str(os.environ.get("REPORT_RESEARCH_SELECTION_CONTRACT_PATH", "")).strip() if research_mode else "",
            "stable_gate_ok": bool(gate_status["ok"]),
            "stable_gate_reason": str(gate_status["reason"]),
            "stable_promoted": bool(gate_status["promoted"]),
            "stable_best_score": float(gate_status["best_score"]),
            "stable_oos_n_total": int(gate_status["oos_n_total"]),
            "stable_oos_pf_weighted": float(gate_status["oos_pf_weighted"]),
            "stable_gate_thresholds": gate_status["thresholds"],
            "entry_enabled": bool(entry_enabled),
            "entry_block_reason": str(entry_block_reason),
            "allow_unapproved_fallback": bool(allow_unapproved_fallback),
        },
    )

    if params.take_profit is None and params.tp_levels:
        params.take_profit = float(params.tp_levels[0])
    if params.trail_pct is None:
        params.trail_pct = float(trail_pct_cfg)
    params.stop_loss = _fix_stop_loss(params.stop_loss)

    print(
        "[PARAM] exit_policy aligned: "
        f"tp_levels={list(params.tp_levels)} tp_ratios={list(params.tp_ratios)} "
        f"trail_activation={params.trailing_activation_profit_pct} trail_pct={params.trail_pct} "
        f"stop_loss={params.stop_loss}"
    )
    print(
        "[PARAM_GATE] "
        f"source={param_source} stable_ok={gate_status['ok']} "
        f"reason={gate_status['reason']}"
    )
    return params


def _params_payload(params: Params) -> Dict[str, Any]:
    out = dict(params.__dict__)
    out.pop("param_gate", None)
    return out


def _bounded_krx_glob(pattern: str) -> List[Path]:
    """Bounded, non-recursive KRX parquet discovery.

    Mirrors p0_daily_check.py:_krx_clean_files() so this report sees the same
    canonical source set instead of an unrestricted BASE_DIR.rglob() that
    also sweeps in backup/, tmp/, and other unrelated subtrees.
    """
    out: List[Path] = []
    seen: set[str] = set()
    for d in (BASE_DIR / "_krx_manual", BASE_DIR / "krx_daily_archive", BASE_DIR):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob(pattern):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return sorted(out)


def find_parquets() -> List[Path]:
    files = _bounded_krx_glob(PARQUET_GLOB_1)
    if files:
        return files
    return _bounded_krx_glob(PARQUET_GLOB_2)


def load_data() -> pd.DataFrame:
    files = find_parquets()
    if not files:
        raise FileNotFoundError(f"no parquet files under {BASE_DIR}")

    use_cols = ["date", "code", "market", "open", "high", "low", "close", "volume", "value"]
    mandatory_cols = ["date", "code", "open", "high", "low", "close", "volume", "value"]
    dfs: List[pd.DataFrame] = []
    for f in files:
        schema_cols = set(pq.read_schema(f).names)
        missing_mandatory = [c for c in mandatory_cols if c not in schema_cols]
        if missing_mandatory:
            raise ValueError(f"parquet missing mandatory cols={missing_mandatory} file={f}")

        read_cols = [c for c in use_cols if c in schema_cols]
        part = pd.read_parquet(f, columns=read_cols)
        if "market" not in part.columns:
            part["market"] = "KRX"
        part["_src_priority"] = 2 if f.parent == BASE_DIR / "_krx_manual" else (1 if f.parent == BASE_DIR / "krx_daily_archive" else 0)
        try:
            part["_src_mtime"] = f.stat().st_mtime
        except OSError:
            part["_src_mtime"] = 0.0
        dfs.append(part)

    df = pd.concat(dfs, ignore_index=True)

    df["date"] = _parse_mixed_date_series(df["date"])
    df = df.dropna(subset=["date"]).copy()
    df["code"] = df["code"].astype(str).str.zfill(6)

    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df.loc[df[c] == 0, c] = np.nan

    df = df.dropna(subset=["close", "open", "high", "low", "value"])

    # Multiple source files (krx_daily_archive / _krx_manual) can carry the
    # same (code, date) row. Keep the row from the most recently written
    # source file so manual/patched corrections win over stale archive rows.
    df = df.sort_values(["_src_priority", "_src_mtime"]).drop_duplicates(subset=["code", "date"], keep="last")
    df = df.drop(columns=["_src_priority", "_src_mtime"])

    df = df.sort_values(["code", "date"])
    df, integrity = apply_price_history_contract(df)
    print(f"[PRICE_HISTORY_INTEGRITY] {integrity['log_line']}")

    # Merge symbol panel for sector / market-cap analysis and defensive filters.
    panel = _load_symbol_panel()
    if not panel.empty:
        before = len(df)
        df = df.merge(panel, on=["date", "code"], how="left")
        after = len(df)
        if before != after:
            print(f"[WARN] symbol_panel merge changed row count {before}->{after}; dropping panel duplicates")
            df = df.drop_duplicates(["date", "code"], keep="first")
        missing_sector = df.get("sector_code", pd.Series()).isna().mean()
        missing_mcap = df.get("market_cap", pd.Series()).isna().mean()
        print(f"[REPORT] merged symbol_panel rows={len(df)} sector_code_missing={missing_sector:.2%} market_cap_missing={missing_mcap:.2%}")
    return df


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    # Market proxy: use each row's KOSPI/KOSDAQ peer group when available,
    # with the old all-market average as a compatibility fallback.
    if "market" not in df.columns:
        df["market"] = ""
    df["market"] = df["market"].fillna("").astype(str).str.upper().str.strip()
    overall_idx = df.groupby("date", as_index=True)["close"].mean().sort_index()
    overall_ret_20 = overall_idx.pct_change(20)
    overall_ret_60 = overall_idx.pct_change(60)
    overall_vol_20 = overall_idx.pct_change().rolling(20, min_periods=20).std()
    stock_ma60 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    df["ma60"] = stock_ma60
    stock_ma200 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(200, min_periods=100).mean())
    df["ma200"] = stock_ma200
    stock_above_ma60 = (df["close"] > stock_ma60).where(stock_ma60.notna())
    overall_breadth_60 = stock_above_ma60.groupby(df["date"]).mean().sort_index()
    df["mkt_ret20"] = df["date"].map(overall_ret_20)
    df["mkt_ret60"] = df["date"].map(overall_ret_60)
    df["mkt_vol20"] = df["date"].map(overall_vol_20)
    df["mkt_breadth"] = df["date"].map(overall_breadth_60)

    # Sector-relative strength (sector_code from symbol panel)
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
            df["sector_rs"] = df["sector_ret20"] - df["mkt_ret20"]
        else:
            df["sector_ret20"] = np.nan
            df["sector_rs"] = np.nan
    else:
        df["sector_ret20"] = np.nan
        df["sector_rs"] = np.nan

    market_idx = (
        df[df["market"].ne("")]
        .groupby(["market", "date"], as_index=False)["close"]
        .mean()
        .sort_values(["market", "date"])
    )
    if not market_idx.empty:
        market_idx["m_ret_20"] = market_idx.groupby("market")["close"].pct_change(20)
        df = df.merge(
            market_idx[["market", "date", "m_ret_20"]],
            on=["market", "date"],
            how="left",
            sort=False,
        )
    else:
        df["m_ret_20"] = np.nan
    df["m_ret_20"] = df["m_ret_20"].fillna(df["date"].map(overall_ret_20))
    df["ret1_pct"] = df.groupby("price_history_key", sort=False)["close"].pct_change(1) * 100.0
    df["ret_20"] = df.groupby("price_history_key", sort=False)["close"].pct_change(20)
    df["rs"] = df["ret_20"] / (df["m_ret_20"] + 1e-9)
    df["rs_slope"] = df.groupby("price_history_key", sort=False)["rs"].diff(5)
    df["ma5"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(5, min_periods=5).mean())
    df["stretch"] = df["close"] / (df["ma5"] + 1e-9)
    df["v_ma5"] = df.groupby("price_history_key", sort=False)["value"].transform(lambda x: x.rolling(5, min_periods=5).mean())
    df["v_accel"] = df["value"] / (df.groupby("price_history_key", sort=False)["v_ma5"].shift(1) + 1e-9)

    delta = df.groupby("price_history_key", sort=False)["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.groupby(df["price_history_key"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(df["price_history_key"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs = avg_gain / (avg_loss + 1e-9)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs))

    ema12 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("price_history_key", sort=False)["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    prev_macd = df.groupby("price_history_key", sort=False)["macd_line"].shift(1)
    prev_sig = df.groupby("price_history_key", sort=False)["macd_signal"].shift(1)
    df["macd_golden"] = (df["macd_line"] > df["macd_signal"]) & (prev_macd <= prev_sig)

    df["vol_close_corr20"] = (
        df.groupby("price_history_key", group_keys=False)[["close", "volume"]]
        .apply(lambda g: g["close"].rolling(20, min_periods=20).corr(g["volume"]))
        .reset_index(level=0, drop=True)
    )
    df["high_52w"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(252, min_periods=60).max())
    df["high_52w_gap"] = ((df["high_52w"] - df["close"]) / (df["high_52w"] + 1e-9)).clip(lower=0.0)
    first_date = df.groupby("code")["date"].transform("min")
    df["listing_days"] = (df["date"] - first_date).dt.days

    prev_close = df.groupby("price_history_key", sort=False)["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["tr"] = tr
    df["atr14"] = df.groupby("price_history_key", sort=False)["tr"].transform(lambda x: x.rolling(14, min_periods=14).mean())
    df["atr_pct"] = df["atr14"] / (df["close"] + 1e-9)

    for col in ["open", "high", "low", "close"]:
        df[f"n_{col}"] = df.groupby("price_history_key", sort=False)[col].shift(-1)
    return df


def _params_to_filter_dict(params: Params) -> Dict[str, Any]:
    return {
        "rs_lim": float(params.rs_lim),
        "v_accel_lim": float(params.v_accel_lim),
        "v_accel_max": float(params.v_accel_max),
        "defense_bear_rs_slope_min": float(params.defense_bear_rs_slope_min),
        "defense_bear_disable_entry": 1.0 if bool(params.defense_bear_disable_entry) else 0.0,
        "stretch_max": float(params.stretch_max),
        "value_min": float(params.value_min),
        "atr_max": float(params.atr_max) if params.atr_max is not None else 9.9,
        "rsi_max": float(params.rsi_max),
        "require_macd_golden": 1.0 if bool(params.require_macd_golden) else 0.0,
        "vol_close_corr_min": float(params.vol_close_corr_min),
        "near_52w_high_gap_max": float(params.near_52w_high_gap_max),
        "min_listing_days": float(params.min_listing_days),
        "sector_blacklist": str(getattr(params, "sector_blacklist", "")),
        "sector_max_per_day": int(getattr(params, "sector_max_per_day", 0) or 0),
        "min_market_cap": float(getattr(params, "min_market_cap", 0.0) or 0.0),
        "require_above_ma200": 1.0 if bool(getattr(params, "require_above_ma200", False)) else 0.0,
        "mkt_ret20_min": float(getattr(params, "mkt_ret20_min", -1.0)),
        "mkt_ret60_min": float(getattr(params, "mkt_ret60_min", -1.0)),
        "sector_rs_min": float(getattr(params, "sector_rs_min", -1.0)),
    }


# _relax_ladder() body removed 2026-07-24 -- was an independently diverged
# copy of generate_candidates_v41_1.py's _relax_ladder() (different formulas,
# missing L7-L9). Consolidated to the imported real one (see import above)
# so this simulation can't silently drift from production again.


CORE_SIGNAL_NAMES: Tuple[str, ...] = (
    "rs", "v_accel", "stretch", "value", "atr", "rsi", "volcorr", "high52", "listing",
)


def _select_candidates(
    day_df: pd.DataFrame,
    p: Dict[str, float],
    required_signals: Optional[Tuple[str, ...]] = None,
    inverse_signals: Optional[Tuple[str, ...]] = None,
) -> pd.DataFrame:
    required = CORE_SIGNAL_NAMES if required_signals is None else tuple(required_signals)
    inverse = tuple(inverse_signals or ())
    unknown = sorted(set(required).difference(CORE_SIGNAL_NAMES))
    unknown_inverse = sorted(set(inverse).difference(CORE_SIGNAL_NAMES))
    if unknown:
        raise ValueError(f"unknown required_signals: {unknown}")
    if unknown_inverse:
        raise ValueError(f"unknown inverse_signals: {unknown_inverse}")
    clauses = {
        "rs": day_df["rs"] > float(p["rs_lim"]),
        "v_accel": day_df["v_accel"] > float(p["v_accel_lim"]),
        "v_accel_max": day_df["v_accel"] <= float(p.get("v_accel_max", 5.0)),
        "stretch": day_df["stretch"] < float(p["stretch_max"]),
        "value": day_df["value"] > float(p["value_min"]),
        "atr": day_df["atr_pct"] < float(p["atr_max"]),
        "rsi": day_df["rsi14"] < float(p["rsi_max"]),
        "volcorr": day_df["vol_close_corr20"] >= float(p["vol_close_corr_min"]),
        "high52": day_df["high_52w_gap"] <= float(p["near_52w_high_gap_max"]),
        "listing": day_df["listing_days"] >= float(p["min_listing_days"]),
    }
    inverse_clauses = {
        "rs": day_df["rs"] <= float(p["rs_lim"]),
        "v_accel": day_df["v_accel"] <= float(p["v_accel_lim"]),
        "v_accel_max": day_df["v_accel"] > float(p.get("v_accel_max", 5.0)),
        "stretch": day_df["stretch"] >= float(p["stretch_max"]),
        "value": day_df["value"] <= float(p["value_min"]),
        "atr": day_df["atr_pct"] >= float(p["atr_max"]),
        "rsi": day_df["rsi14"] >= float(p["rsi_max"]),
        "volcorr": day_df["vol_close_corr20"] < float(p["vol_close_corr_min"]),
        "high52": day_df["high_52w_gap"] > float(p["near_52w_high_gap_max"]),
        "listing": day_df["listing_days"] < float(p["min_listing_days"]),
    }
    inverse_set = set(inverse)
    cond = pd.Series(True, index=day_df.index)
    for signal in required:
        cond &= inverse_clauses[signal] if signal in inverse_set else clauses[signal]
    # hard ceiling on acceleration (proposal v_accel <= 5.0)
    cond = cond & (day_df["v_accel"] <= float(p.get("v_accel_max", 5.0)))
    # 2022 defense: skip or require positive rs_slope in bear/stress regime
    if "market_regime" in day_df.columns:
        is_bear = day_df["market_regime"].astype(str).str.upper().isin({"BEAR", "STRESS"})
        if float(p.get("defense_bear_disable_entry") or 0.0) >= 0.5:
            cond = cond & (~is_bear)
        else:
            bear_defense = float(p.get("defense_bear_rs_slope_min") or 0.0)
            if bear_defense != 0.0 and "rs_slope" in day_df.columns:
                cond = cond & (~is_bear | (day_df["rs_slope"] >= bear_defense))
    if float(p.get("require_macd_golden", 0.0) or 0.0) >= 0.5:
        cond = cond & (day_df["macd_golden"] == True)
    if float(p.get("require_above_ma200", 0.0) or 0.0) >= 0.5 and "ma200" in day_df.columns and "close" in day_df.columns:
        cond = cond & (day_df["close"] > day_df["ma200"])
    # Market/sector defense filters (aligned with HPO rule_e)
    if "mkt_ret20" in day_df.columns:
        cond = cond & (day_df["mkt_ret20"] > float(p.get("mkt_ret20_min", -1.0)))
    if "mkt_ret60" in day_df.columns:
        cond = cond & (day_df["mkt_ret60"] > float(p.get("mkt_ret60_min", -1.0)))
    if "sector_rs" in day_df.columns:
        cond = cond & (day_df["sector_rs"] > float(p.get("sector_rs_min", -1.0)))

    candidates = day_df[cond].copy()

    # Sector / market-cap defensive filters (added 2026-08-10)
    blacklist_raw = str(p.get("sector_blacklist", "")).strip()
    if blacklist_raw and "sector_code" in candidates.columns:
        blacklist = {int(float(x.strip())) for x in blacklist_raw.split(",") if x.strip()}
        if blacklist:
            sc_num = pd.to_numeric(candidates["sector_code"], errors="coerce")
            candidates = candidates[~sc_num.isin(blacklist)].copy()

    min_mcap = float(p.get("min_market_cap", 0.0) or 0.0)
    if min_mcap > 0.0 and "market_cap" in candidates.columns:
        candidates = candidates[candidates["market_cap"] >= min_mcap].copy()

    return candidates



def _assign_report_research_regime(factors: pd.DataFrame) -> pd.Series:
    """Research-only ex-ante regime aligned with optimize_regime_strategy_signal_requirements.py."""
    d = factors.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.normalize()
    idx = d.groupby("date")["close"].mean().sort_index()
    ret20 = idx.pct_change(20)
    ma60 = idx.rolling(60, min_periods=60).mean()
    vol20 = idx.pct_change().rolling(20, min_periods=20).std()

    def _safe_breadth(g: pd.DataFrame) -> float:
        close = pd.to_numeric(g["close"], errors="coerce")
        ma = pd.to_numeric(g["ma60"], errors="coerce") if "ma60" in g.columns else pd.Series(np.nan, index=g.index)
        valid = close.notna() & ma.notna()
        if not bool(valid.any()):
            return float("nan")
        return float((close[valid] > ma[valid]).mean())

    breadth = d.groupby("date").apply(_safe_breadth, include_groups=False)
    out = pd.DataFrame({"ret20": ret20, "ma60": ma60, "vol20": vol20, "breadth": breadth})
    out["above_ma60"] = idx > out["ma60"]
    regime = np.full(len(out), "TRANSITION", dtype=object)
    stress = (out["ret20"] <= -0.10) | ((out["vol20"] >= 0.04) & (out["ret20"] < 0))
    bull = (~stress) & out["above_ma60"] & (out["ret20"] >= 0.05) & (out["breadth"] >= 0.55)
    bear = (~stress) & (~out["above_ma60"]) & (out["ret20"] <= -0.02) & (out["breadth"] <= 0.45)
    sideways = (~stress) & (out["ret20"].abs() <= 0.05) & out["breadth"].between(0.40, 0.60, inclusive="both")
    regime[stress.fillna(False).to_numpy()] = "STRESS"
    regime[bull.fillna(False).to_numpy()] = "BULL"
    regime[bear.fillna(False).to_numpy()] = "BEAR"
    regime[sideways.fillna(False).to_numpy()] = "SIDEWAYS"
    return pd.Series(regime, index=out.index, name="market_regime_research")

def load_listing() -> Optional[pd.DataFrame]:
    if not CACHE_LISTING.exists():
        return None
    m = pd.read_csv(CACHE_LISTING, dtype={"code": str})
    m["code"] = m["code"].astype(str).str.zfill(6)
    if "name" not in m.columns:
        return None
    return m[["code", "name"]].drop_duplicates("code")


def score_day(day_df: pd.DataFrame, params: Params, rank_base_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    base = rank_base_df if isinstance(rank_base_df, pd.DataFrame) and not rank_base_df.empty else day_df
    rs_r = base["rs"].rank(pct=True)
    slope_r = base["rs_slope"].rank(pct=True)
    v_r = base["v_accel"].rank(pct=True)
    score_map = pd.DataFrame(
        {
            "code": base["code"].astype(str),
            "_score_rs": rs_r,
            "_score_slope": slope_r,
            "_score_v": v_r,
        }
    ).drop_duplicates("code", keep="last")
    out = day_df.copy()
    out["code"] = out["code"].astype(str)
    out = out.merge(score_map, on="code", how="left")
    out["score"] = (
        out["_score_rs"] * params.w_rs
        + out["_score_slope"] * params.w_rs_slope
        + out["_score_v"] * params.w_v_accel
    )
    out = out.drop(columns=["_score_rs", "_score_slope", "_score_v"], errors="ignore")
    return out


def prepare_simulation_context(df: pd.DataFrame) -> Dict[str, Any]:
    cols = [
        "date", "code", "price_history_key", "market", "open", "high", "low", "close", "value",
        "rs", "rs_slope", "stretch", "v_accel", "atr_pct", "rsi14", "macd_golden",
        "vol_close_corr20", "high_52w_gap", "listing_days", "ret1_pct", "mkt_ret20", "mkt_ret60", "mkt_vol20", "mkt_breadth", "n_open", "n_high", "n_low", "n_close",
    ]
    for extra in ["sector", "sector_code", "market_cap", "listed_shares", "ma200"]:
        if extra in df.columns:
            cols.append(extra)
    if "market_regime" not in df.columns:
        regime_map = _assign_report_research_regime(df)
        df = df.copy()
        df["market_regime"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize().map(regime_map)
    if "market_regime" in df.columns:
        cols.append("market_regime")
    sig = df[cols].copy()
    sig = sig.dropna(subset=["n_open", "n_high", "n_low", "n_close", "rs", "rs_slope", "stretch", "v_accel", "value", "atr_pct", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days"])
    sell_rules = _load_paper_sell_rules()
    market_close = df.groupby("date", sort=True)["close"].mean().sort_index()
    return {
        "df_id": id(df),
        "sig": sig,
        "df_code": {c: g for c, g in df.groupby("price_history_key", sort=False)},
        "fundamental_cfg": sell_rules.get("fundamental_risk", {}) if isinstance(sell_rules.get("fundamental_risk"), dict) else {},
        "technical_cfg": sell_rules.get("technical", {}) if isinstance(sell_rules.get("technical"), dict) else {},
        "market_cfg": sell_rules.get("market_risk", {}) if isinstance(sell_rules.get("market_risk"), dict) else {},
        "fundamentals_map": _load_fundamental_map(),
        "market_day_ret": market_close.pct_change(),
        "market_ret_20": market_close.pct_change(20),
    }


def _apply_selection_numeric_filters(sig: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
    if not filters:
        return sig
    field_map = {
        "signal_ret1_pct": "ret1_pct",
        "signal_rs": "rs",
        "signal_v_accel": "v_accel",
        "signal_stretch": "stretch",
        "signal_atr_pct": "atr_pct",
        "signal_high_52w_gap": "high_52w_gap",
        "signal_value": "value",
        "mkt_ret20": "mkt_ret20",
        "mkt_ret60": "mkt_ret60",
        "mkt_vol20": "mkt_vol20",
        "mkt_breadth": "mkt_breadth",
    }
    out = sig
    for item in filters:
        if not isinstance(item, dict):
            raise ValueError("selection_contract.numeric_filters entries must be objects")
        raw_field = str(item.get("field", "")).strip()
        op = str(item.get("op", "")).strip()
        if raw_field == "entry_gap_pct":
            if "n_open" not in out.columns or "close" not in out.columns:
                raise ValueError("selection_contract numeric filter entry_gap_pct requires n_open and close")
            series = (pd.to_numeric(out["n_open"], errors="coerce") - pd.to_numeric(out["close"], errors="coerce")) / (pd.to_numeric(out["close"], errors="coerce") + 1e-9)
        else:
            field = field_map.get(raw_field, raw_field)
            if field not in out.columns:
                raise ValueError(f"unknown selection_contract numeric filter field: {raw_field}")
            series = pd.to_numeric(out[field], errors="coerce")
        value = float(item.get("value"))
        if op == "<=":
            mask = series <= value
        elif op == "<":
            mask = series < value
        elif op == ">=":
            mask = series >= value
        elif op == ">":
            mask = series > value
        elif op in {"==", "="}:
            mask = series == value
        elif op == "!=":
            mask = series != value
        else:
            raise ValueError(f"unsupported selection_contract numeric filter op: {op}")
        out = out[mask.fillna(False)].copy()
        if out.empty:
            return out
    return out
def simulate_trades(
    df: pd.DataFrame,
    params: Params,
    selection_contract: Optional[Dict[str, Any]] = None,
    simulation_context: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if not bool(params.entry_enabled):
        print(f"[REPORT_ENTRY_BLOCKED] reason={params.entry_block_reason}")
        return pd.DataFrame(columns=["entry_date", "exit_date", "code", "market", "sector", "sector_code", "market_cap", "listed_shares", "entry_px", "exit_px", "ret", "exit_reason", "signal_date", "score", "relax_level"])
    context_data = simulation_context if isinstance(simulation_context, dict) and simulation_context.get("df_id") == id(df) else prepare_simulation_context(df)
    sig = context_data["sig"]
    contract = dict(selection_contract or {})
    required_raw = contract.get("required_signals")
    required_signals = None if required_raw is None else tuple(str(x) for x in required_raw)
    inverse_raw = contract.get("inverse_signals") or []
    inverse_signals = tuple(str(x) for x in inverse_raw)
    allowed_regimes = {str(x).upper() for x in contract.get("regimes", []) if str(x).strip()}
    if allowed_regimes:
        if "market_regime" not in sig.columns:
            raise ValueError("selection_contract.regimes requires market_regime")
        sig = sig[sig["market_regime"].astype(str).str.upper().isin(allowed_regimes)].copy()
    if contract.get("signal_date_start") is not None:
        sig = sig[sig["date"] >= pd.Timestamp(contract["signal_date_start"])].copy()
    if contract.get("signal_date_end") is not None:
        sig = sig[sig["date"] <= pd.Timestamp(contract["signal_date_end"])].copy()
    numeric_filters = contract.get("numeric_filters") or []
    if numeric_filters:
        sig = _apply_selection_numeric_filters(sig, list(numeric_filters))
    if sig.empty:
        return pd.DataFrame(columns=["entry_date", "exit_date", "code", "market", "sector", "sector_code", "market_cap", "listed_shares", "entry_px", "exit_px", "ret", "exit_reason", "signal_date", "score", "relax_level"])

    base_filter = _params_to_filter_dict(params)
    ladder = _relax_ladder(base_filter) if bool(params.use_relax_ladder) else [("L0", base_filter)]

    out_rows = []
    active: Dict[str, pd.Timestamp] = {}
    df_code = context_data["df_code"]
    fundamental_cfg = context_data["fundamental_cfg"]
    technical_cfg = context_data["technical_cfg"]
    market_cfg = context_data["market_cfg"]
    fundamentals_map = context_data["fundamentals_map"]
    market_day_ret = context_data["market_day_ret"]
    market_ret_20 = context_data["market_ret_20"]

    entry_trigger = contract.get("entry_trigger") if isinstance(contract.get("entry_trigger"), dict) else {}
    entry_trigger_type = str(entry_trigger.get("type", "next_open")).strip().lower() or "next_open"
    if entry_trigger_type not in {"next_open", "breakout_prior_high"}:
        raise ValueError(f"unsupported selection_contract.entry_trigger.type: {entry_trigger_type}")
    exit_overrides = contract.get("exit_overrides") if isinstance(contract.get("exit_overrides"), dict) else {}
    effective_hold = max(1, int(exit_overrides.get("hold", params.hold) or params.hold))
    effective_stop_loss = params.stop_loss
    if "stop_loss" in exit_overrides:
        raw_stop = exit_overrides.get("stop_loss")
        effective_stop_loss = None if raw_stop is None else _fix_stop_loss(raw_stop)
    disable_paper_exit_rules = bool(exit_overrides.get("disable_paper_exit_rules", False))
    effective_tp_levels: Tuple[float, ...] = tuple(params.tp_levels or tuple())
    effective_tp_ratios: Tuple[float, ...] = tuple(params.tp_ratios or tuple())
    effective_trail_pct = params.trail_pct
    effective_trailing_activation = float(params.trailing_activation_profit_pct or 0.20)
    if disable_paper_exit_rules:
        effective_tp_levels = tuple()
        effective_tp_ratios = tuple()
        effective_trail_pct = None
        fundamental_cfg = {}
        technical_cfg = {}
        market_cfg = {}

    for d, g in sig.groupby("date", sort=True):
        for c in list(active.keys()):
            if active[c] < d:
                del active[c]

        selected = pd.DataFrame()
        chosen_level = "NONE"
        for level, p_try in ladder:
            cand = _select_candidates(g, p_try, required_signals=required_signals, inverse_signals=inverse_signals)
            if not cand.empty:
                selected = cand
                chosen_level = str(level)
                break

        if selected.empty:
            continue

        day = score_day(selected.copy(), params, rank_base_df=g).sort_values("score", ascending=False)

        # Sector max-per-day defensive cap (post-score, per day)
        sector_max = int(params.sector_max_per_day or 0)
        if sector_max > 0 and "sector_code" in day.columns and not day.empty:
            day = day.groupby("sector_code", as_index=False, group_keys=False).head(sector_max)

        slots = max(params.max_pos - len(active), 0)
        if slots <= 0:
            continue

        for _, r in day.iterrows():
            code = r["code"]
            if code in active:
                continue
            if slots <= 0:
                break

            prev_close = float(r["close"])
            history_key = str(r["price_history_key"])
            cdf = df_code.get(history_key)
            if cdf is None:
                continue

            entry_trigger_px = np.nan
            entry_signal_basis_px = np.nan
            entry_trigger_reason = "NEXT_OPEN"
            entry_trigger_window_days = 0
            apply_gap_filters = bool(entry_trigger.get("apply_gap_filters", entry_trigger_type == "next_open"))

            if entry_trigger_type == "breakout_prior_high":
                trigger_window_days = max(1, int(entry_trigger.get("lookahead_days", effective_hold) or effective_hold))
                trigger_basis = str(entry_trigger.get("price_basis", "signal_high")).strip().lower()
                if trigger_basis in {"signal_close", "close"}:
                    basis_px = float(r["close"])
                elif trigger_basis in {"signal_open", "open"}:
                    basis_px = float(r["open"])
                else:
                    basis_px = float(r["high"])
                trigger_buffer_pct = float(entry_trigger.get("buffer_pct", 0.0) or 0.0)
                trigger_px_raw = basis_px * (1.0 + trigger_buffer_pct)
                trigger_window = cdf[cdf["date"] > d].head(trigger_window_days).copy()
                if trigger_window.empty:
                    continue
                trigger_high = pd.to_numeric(trigger_window["high"], errors="coerce")
                hit_window = trigger_window[trigger_high >= trigger_px_raw]
                if hit_window.empty:
                    continue
                entry_day = hit_window.iloc[0]
                entry_day_dt = pd.to_datetime(entry_day["date"])
                entry_open_raw = float(entry_day["open"])
                entry_px_raw = entry_open_raw if entry_open_raw > trigger_px_raw else trigger_px_raw
                entry_px = entry_px_raw * (1.0 + params.slippage)
                gap = (entry_px_raw - prev_close) / (prev_close + 1e-9)
                entry_trigger_px = float(trigger_px_raw)
                entry_signal_basis_px = float(basis_px)
                entry_trigger_reason = "BREAKOUT_GAP" if entry_open_raw > trigger_px_raw else "BREAKOUT_TOUCH"
                entry_trigger_window_days = int(trigger_window_days)
                future = cdf[cdf["date"] >= entry_day_dt].head(effective_hold).copy()
            else:
                entry_open = float(r["n_open"])
                entry_px = entry_open * (1.0 + params.slippage)
                gap = (entry_open - prev_close) / (prev_close + 1e-9)
                future = cdf[cdf["date"] > d].head(effective_hold).copy()

            if apply_gap_filters:
                gu = float(params.gap_up_max_pct or 0.0)
                gd = float(params.entry_gap_down_stop_pct or 0.0)
                if gu > 0 and gap > gu:
                    continue
                if gd > 0 and gap <= -abs(gd):
                    continue

                # backward-compat fallback for legacy stable params
                if gu <= 0 and gd <= 0 and params.gap_limit is not None:
                    if abs(gap) > float(params.gap_limit):
                        continue

            if future.empty:
                continue

            entry_day = future.iloc[0]
            entry_day_open = float(entry_day["open"])
            entry_day_high = float(entry_day["high"])
            entry_day_low = float(entry_day["low"])
            entry_day_close = float(entry_day["close"])
            entry_day_open_to_close_ret = (entry_day_close / (entry_day_open + 1e-9)) - 1.0
            entry_day_high_from_open_pct = (entry_day_high / (entry_day_open + 1e-9)) - 1.0
            entry_day_low_from_open_pct = (entry_day_low / (entry_day_open + 1e-9)) - 1.0
            mfe_1d = entry_day_high_from_open_pct
            mae_1d = entry_day_low_from_open_pct
            followthrough_1d = bool(entry_day_close > entry_day_open and entry_day_close > prev_close)
            setup_tags: list[str] = []
            if float(r.get("atr_pct", 0.0) or 0.0) <= 0.05192369520951628 and float(r.get("stretch", 0.0) or 0.0) <= 1.0719612229679145:
                setup_tags.append("low_stop_risk")
            if float(r.get("high_52w_gap", 0.0) or 0.0) >= 0.005 and float(r.get("high_52w_gap", 0.0) or 0.0) <= 0.0373:
                setup_tags.append("breakout_proxy")
            if float(r.get("rs", 0.0) or 0.0) >= 3.5265 and float(r.get("stretch", 0.0) or 0.0) <= 1.0719612229679145:
                setup_tags.append("pullback_proxy")
            if float(r.get("value", 0.0) or 0.0) >= 159314523500 and float(r.get("atr_pct", 0.0) or 0.0) <= 0.05192369520951628:
                setup_tags.append("liquidity_quality_proxy")
            setup_family = "|".join(setup_tags) if setup_tags else "current_unclassified"

            exit_reason = "HOLD"
            exit_px = float(future.iloc[-1]["close"]) * (1.0 - params.slippage)
            stop = effective_stop_loss
            stop_px = entry_px * (1.0 + stop) if stop is not None else None

            tp_levels = [float(x) for x in list(effective_tp_levels or []) if x is not None and float(x) > 0]
            tp_ratios = [float(x) for x in list(effective_tp_ratios or []) if x is not None and float(x) > 0]
            tp_plan: List[Tuple[float, float]] = []
            if tp_levels and tp_ratios:
                m = min(len(tp_levels), len(tp_ratios))
                base_levels = tp_levels[:m]
                base_ratios = tp_ratios[:m]
                ratio_sum = sum(base_ratios)
                if ratio_sum > 0:
                    for lv, rt in zip(base_levels, base_ratios):
                        tp_plan.append((float(lv), float(rt) / float(ratio_sum)))
                tp_plan.sort(key=lambda x: x[0])

            trailing_pct = float(effective_trail_pct) if effective_trail_pct is not None else None
            trailing_activation = float(effective_trailing_activation or 0.20)
            max_close = entry_px
            remaining = 1.0
            realized_ret = 0.0
            tp_taken: set[int] = set()

            for idx_fr, (_, fr) in enumerate(future.iterrows()):
                o = float(fr["open"])
                h = float(fr["high"])
                l = float(fr["low"])
                c = float(fr["close"])
                d_cur = pd.to_datetime(fr["date"])

                trail_px = None
                if trailing_pct is not None and max_close > 0 and ((max_close / (entry_px + 1e-9) - 1.0) >= trailing_activation):
                    trail_px = max_close * (1.0 + trailing_pct)

                # 1) STOP/STOP_GAP
                if stop_px is not None and remaining > 0:
                    if o <= stop_px:
                        exit_reason = "STOP_GAP"
                        exit_px = o * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break
                    if l <= stop_px:
                        exit_reason = "STOP"
                        exit_px = stop_px * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break

                # 2) TRAIL/TRAIL_GAP
                if trail_px is not None and remaining > 0:
                    if o <= trail_px:
                        exit_reason = "TRAIL_GAP"
                        exit_px = o * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break
                    if l <= trail_px:
                        exit_reason = "TRAIL"
                        exit_px = trail_px * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break

                # 3) FUNDAMENTAL
                if remaining > 0 and bool(fundamental_cfg.get("enabled", False)):
                    frow = fundamentals_map.get(str(code).zfill(6), {})
                    debt_ratio = pd.to_numeric(frow.get("debt_ratio"), errors="coerce")
                    roe = pd.to_numeric(frow.get("roe"), errors="coerce")
                    revenue_growth = pd.to_numeric(frow.get("revenue_growth"), errors="coerce")
                    opm = pd.to_numeric(frow.get("opm"), errors="coerce")

                    critical_debt = float(fundamental_cfg.get("critical_debt_ratio", 200.0) or 200.0)
                    critical_roe = float(fundamental_cfg.get("critical_roe", 0.0) or 0.0)
                    critical_rev = float(fundamental_cfg.get("critical_revenue_growth_yoy", -20.0) or -20.0)
                    warning_opm = float(fundamental_cfg.get("warning_operating_margin", 5.0) or 5.0)

                    is_critical = (
                        (pd.notna(debt_ratio) and float(debt_ratio) >= critical_debt)
                        or (pd.notna(roe) and float(roe) <= critical_roe)
                        or (pd.notna(revenue_growth) and float(revenue_growth) <= critical_rev)
                    )
                    is_warning = pd.notna(opm) and float(opm) <= warning_opm
                    if is_critical:
                        exit_reason = "FUNDAMENTAL_CRITICAL"
                        exit_px = c * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break
                    if is_warning:
                        exit_reason = "FUNDAMENTAL_WARNING"
                        exit_px = c * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break

                # 4) TP 단계 익절
                if remaining > 0 and tp_plan:
                    for idx_tp, (tp_lv, tp_ratio) in enumerate(tp_plan):
                        if idx_tp in tp_taken:
                            continue
                        tp_px = entry_px * (1.0 + tp_lv)
                        if h >= tp_px:
                            exec_px = tp_px * (1.0 - params.slippage)
                            take_w = min(remaining, max(0.0, tp_ratio))
                            if take_w <= 0:
                                tp_taken.add(idx_tp)
                                continue
                            realized_ret += take_w * ((exec_px / (entry_px + 1e-9)) - 1.0)
                            remaining -= take_w
                            tp_taken.add(idx_tp)
                            exit_reason = f"TP_L{int(round(tp_lv * 100))}"
                            if remaining <= 1e-12:
                                remaining = 0.0
                                exit_px = exec_px
                                break
                    if remaining <= 0.0:
                        break

                # 5) TECHNICAL
                if remaining > 0 and bool(technical_cfg.get("enabled", False)):
                    hist = cdf[cdf["date"] <= d_cur]["close"].dropna().astype(float).tolist()
                    prev_hist = hist[:-1]
                    ma_periods = technical_cfg.get("ma_periods", [20, 60, 120]) if isinstance(technical_cfg.get("ma_periods"), list) else [20, 60, 120]
                    ma20 = _calc_ma(hist, int(ma_periods[0])) if len(ma_periods) >= 1 else None
                    ma60 = _calc_ma(hist, int(ma_periods[1])) if len(ma_periods) >= 2 else None
                    ma120 = _calc_ma(hist, int(ma_periods[2])) if len(ma_periods) >= 3 else None
                    prev_ma20 = _calc_ma(prev_hist, int(ma_periods[0])) if len(ma_periods) >= 1 else None
                    prev_ma60 = _calc_ma(prev_hist, int(ma_periods[1])) if len(ma_periods) >= 2 else None
                    rsi = _calc_rsi(hist, 14)
                    current_profit_pct = ((c - entry_px) / (entry_px + 1e-9)) * 100.0
                    tech_score = 0
                    if ma20 is not None and c < ma20:
                        tech_score += 10
                    if ma60 is not None and c < ma60:
                        tech_score += 20
                    if ma120 is not None and c < ma120:
                        tech_score += 30
                    if (
                        ma20 is not None and ma60 is not None
                        and prev_ma20 is not None and prev_ma60 is not None
                        and ma20 < ma60 and prev_ma20 >= prev_ma60
                    ):
                        tech_score += 25
                    if rsi is not None and rsi >= float(technical_cfg.get("rsi_overbought", 75) or 75) and current_profit_pct > 0:
                        tech_score += 20

                    high_thr = int(technical_cfg.get("high_score_threshold", 60) or 60)
                    mid_thr = int(technical_cfg.get("mid_score_threshold", 40) or 40)
                    if tech_score >= high_thr or tech_score >= mid_thr:
                        exit_reason = "TECHNICAL"
                        exit_px = c * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break

                # 6) MARKET_RISK (price proxy; historical vix/fx series unavailable)
                if remaining > 0 and bool(market_cfg.get("enabled", False)):
                    day_ret = pd.to_numeric(market_day_ret.get(d_cur), errors="coerce")
                    ret20 = pd.to_numeric(market_ret_20.get(d_cur), errors="coerce")
                    score_m = 0
                    if pd.notna(day_ret):
                        if float(day_ret) <= -0.05:
                            score_m += 40
                        elif float(day_ret) <= -0.03:
                            score_m += 25
                        if abs(float(day_ret)) >= 0.02:
                            score_m += 15
                    if pd.notna(ret20) and float(ret20) < 0:
                        score_m += 20
                    if pd.notna(ret20) and float(ret20) <= -0.05:
                        score_m += 10
                    if score_m >= 60:
                        exit_reason = "MARKET_RISK"
                        exit_px = c * (1.0 - params.slippage)
                        realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                        remaining = 0.0
                        break

                # 7) TIME
                if idx_fr >= (int(effective_hold) - 1):
                    exit_reason = "TIME"
                    exit_px = c * (1.0 - params.slippage)
                    realized_ret += remaining * ((exit_px / (entry_px + 1e-9)) - 1.0)
                    remaining = 0.0
                    break

                max_close = max(max_close, c)

            if remaining > 0:
                hold_exit_px = float(future.iloc[-1]["close"]) * (1.0 - params.slippage)
                realized_ret += remaining * ((hold_exit_px / (entry_px + 1e-9)) - 1.0)
                exit_px = hold_exit_px
                if tp_taken and exit_reason.startswith("TP_L"):
                    exit_reason = "TP_PARTIAL_HOLD"

            ret = float(realized_ret) - params.cost
            exit_date = pd.to_datetime(future.iloc[-1]["date"])
            active[code] = exit_date

            out_rows.append(
                {
                    "signal_date": pd.to_datetime(d).date().isoformat(),
                    "entry_date": pd.to_datetime(future.iloc[0]["date"]).date().isoformat(),
                    "exit_date": exit_date.date().isoformat(),
                    "code": code,
                    "market": r.get("market", ""),
                    "market_regime": r.get("market_regime", ""),
                    "sector": r.get("sector", ""),
                    "sector_code": r.get("sector_code", ""),
                    "market_cap": float(r.get("market_cap", 0.0) or 0.0),
                    "listed_shares": float(r.get("listed_shares", 0.0) or 0.0),
                    "entry_px": round(entry_px, 6),
                    "exit_px": round(exit_px, 6),
                    "ret": float(ret),
                    "exit_reason": exit_reason,
                    "score": float(r["score"]),
                    "relax_level": chosen_level,
                    "entry_gap_pct": float(gap),
                    "entry_trigger_type": entry_trigger_type,
                    "entry_trigger_px": float(entry_trigger_px) if pd.notna(entry_trigger_px) else np.nan,
                    "entry_signal_basis_px": float(entry_signal_basis_px) if pd.notna(entry_signal_basis_px) else np.nan,
                    "entry_trigger_reason": entry_trigger_reason,
                    "entry_trigger_window_days": int(entry_trigger_window_days),
                    "entry_day_open_to_close_ret": float(entry_day_open_to_close_ret),
                    "entry_day_high_from_open_pct": float(entry_day_high_from_open_pct),
                    "entry_day_low_from_open_pct": float(entry_day_low_from_open_pct),
                    "mfe_1d": float(mfe_1d),
                    "mae_1d": float(mae_1d),
                    "followthrough_1d": int(followthrough_1d),
                    "setup_family": setup_family,
                    "signal_ret1_pct": float(r.get("ret1_pct", 0.0) or 0.0),
                    "signal_rs": float(r.get("rs", 0.0) or 0.0),
                    "signal_v_accel": float(r.get("v_accel", 0.0) or 0.0),
                    "signal_stretch": float(r.get("stretch", 0.0) or 0.0),
                    "signal_atr_pct": float(r.get("atr_pct", 0.0) or 0.0),
                    "signal_high_52w_gap": float(r.get("high_52w_gap", 0.0) or 0.0),
                    "signal_value": float(r.get("value", 0.0) or 0.0),
                }
            )
            slots -= 1

    return pd.DataFrame(out_rows)


def summarize(rets: np.ndarray) -> Dict:
    if rets.size == 0:
        return {"n": 0, "win_rate": 0.0, "mean": 0.0, "median": 0.0, "p05": 0.0, "p95": 0.0, "pf": 0.0}
    wins = rets[rets > 0]
    losses = rets[rets < 0]
    pf = float(wins.sum() / (abs(losses.sum()) + 1e-9)) if losses.size else float("inf")
    return {
        "n": int(rets.size),
        "win_rate": float((rets > 0).mean()),
        "mean": float(rets.mean()),
        "median": float(np.median(rets)),
        "p05": float(np.percentile(rets, 5)),
        "p95": float(np.percentile(rets, 95)),
        "pf": pf if np.isfinite(pf) else 999.0,
    }


def main() -> int:
    params = load_params()
    print("[REPORT] loading data ...")
    df = load_data()
    print(f"[REPORT] rows={len(df):,} codes={df['code'].nunique():,} dates={df['date'].nunique():,}")
    print("[REPORT] compute factors ...")
    df = compute_factors(df)
    listing = load_listing()
    selection_contract = _load_research_selection_contract()
    if selection_contract:
        print(f"[REPORT_SELECTION_CONTRACT] {selection_contract}")
    print("[REPORT] simulate trades ...")
    trades = simulate_trades(df, params, selection_contract=selection_contract)

    if not trades.empty and listing is not None:
        trades = trades.merge(listing, on="code", how="left")
        cols = [
            "signal_date", "entry_date", "exit_date", "code", "name", "market", "sector", "sector_code", "market_cap", "listed_shares",
            "entry_px", "exit_px", "ret", "exit_reason", "score", "relax_level",
            "entry_gap_pct", "entry_trigger_type", "entry_trigger_px", "entry_signal_basis_px", "entry_trigger_reason", "entry_trigger_window_days",
            "entry_day_open_to_close_ret", "entry_day_high_from_open_pct", "entry_day_low_from_open_pct",
            "mfe_1d", "mae_1d", "followthrough_1d", "setup_family",
            "signal_ret1_pct", "signal_rs", "signal_v_accel", "signal_stretch", "signal_atr_pct", "signal_high_52w_gap", "signal_value",
        ]
        trades = trades[cols]

    out_dir = _report_output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_trades = out_dir / "report_backtest_trades_v41_1.csv"
    trades.to_csv(out_trades, index=False, encoding="utf-8-sig")

    if trades.empty:
        splits_df = pd.DataFrame([{"split": "TRAIN", "n": 0}, {"split": "VAL", "n": 0}, {"split": "OOS", "n": 0}])
        yearly_df = pd.DataFrame(columns=["year", "n", "win_rate", "mean", "median", "p05", "p95", "pf"])
        summary = {
            "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": _params_payload(params),
            "param_gate": params.param_gate,
            "selection_contract": selection_contract or {},
            "splits": {},
            "yearly": [],
            "exit_reason_counts": {},
            "model_scope": {
                "stop_gap": True,
                "tp_trailing": True,
                "fundamental_risk": True,
                "technical_risk": True,
                "market_risk_proxy": True,
                "sector_rebalance": False,
                "ddm_liquidate": False,
            },
        }
    else:
        entry_dt = pd.to_datetime(trades["entry_date"])
        split = np.where(entry_dt <= TRAIN_END, "TRAIN", np.where(entry_dt <= VAL_END, "VAL", "OOS"))
        trades["split"] = split

        splits = []
        for sp in ["TRAIN", "VAL", "OOS"]:
            r = trades.loc[trades["split"] == sp, "ret"].to_numpy(dtype=float)
            s = summarize(r)
            s["split"] = sp
            splits.append(s)
        splits_df = pd.DataFrame(splits)

        years = pd.to_datetime(trades["entry_date"]).dt.year
        trades["year"] = years
        ys = []
        for y in sorted(trades["year"].unique()):
            r = trades.loc[trades["year"] == y, "ret"].to_numpy(dtype=float)
            s = summarize(r)
            s["year"] = int(y)
            ys.append(s)
        yearly_df = pd.DataFrame(ys)[["year", "n", "win_rate", "mean", "median", "p05", "p95", "pf"]]

        summary = {
            "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": _params_payload(params),
            "param_gate": params.param_gate,
            "selection_contract": selection_contract or {},
            "splits": {row["split"]: {k: row[k] for k in row.index if k != "split"} for _, row in splits_df.iterrows()},
            "yearly": yearly_df.to_dict(orient="records"),
            "trades_file": str(out_trades),
            "exit_reason_counts": {str(k): int(v) for k, v in trades["exit_reason"].astype(str).value_counts().to_dict().items()},
            "model_scope": {
                "stop_gap": True,
                "tp_trailing": True,
                "fundamental_risk": True,
                "technical_risk": True,
                "market_risk_proxy": True,
                "sector_rebalance": False,
                "ddm_liquidate": False,
            },
        }

    out_splits = out_dir / "report_backtest_splits_v41_1.csv"
    out_yearly = out_dir / "report_backtest_yearly_v41_1.csv"
    out_summary = out_dir / "report_backtest_summary_v41_1.json"
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    splits_df.to_csv(out_splits, index=False, encoding="utf-8-sig")
    yearly_df.to_csv(out_yearly, index=False, encoding="utf-8-sig")

    print("[OK] saved:")
    print(f" - {out_summary}")
    print(f" - {out_splits}")
    print(f" - {out_yearly}")
    print(f" - {out_trades}")

    if not splits_df.empty:
        print("\n[SUMMARY] splits")
        print(splits_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
