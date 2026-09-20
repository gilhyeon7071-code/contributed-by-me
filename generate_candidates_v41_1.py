# -*- coding: utf-8 -*-
"""
STOC Candidate Generator (v41.1) - Candidate 0媛?諛⑹?(?먮룞 ?꾪솕 ?ы븿)

?낅젰
- <BASE_DIR> ?섏쐞 ?대뵒?? krx_daily_*_clean.parquet
- <BASE_DIR>\\_cache\\krx_listing.csv (code,name) (?놁뼱???숈옉)
- <BASE_DIR>\\12_Risk_Controlled\\stable_params_v41_1.json (?놁쑝硫?湲곕낯媛??ъ슜)

異쒕젰
- <BASE_DIR>\\2_Logs\\candidates_v41_1_YYYYMMDD.csv
- <BASE_DIR>\\2_Logs\\candidates_latest.csv                 (理쒖떊 ?뚯씪紐??ъ씤??
- <BASE_DIR>\\2_Logs\\candidates_latest_data.csv            (??긽 CSV ?곗씠??
- <BASE_DIR>\\2_Logs\\candidates_latest_meta.json           (?ъ슜 ?뚮씪誘명꽣/?꾪솕 ?④퀎/DIAG)

?ㅽ뻾
  python E:\\1_Data\\generate_candidates_v41_1.py
"""

import json
import logging
import os
import importlib.util
import subprocess
import sys
from pathlib import Path
import hashlib as _hashlib

def _sha256_file(_p: Path) -> str:
    h=_hashlib.sha256()
    with _p.open('rb') as f:
        for ch in iter(lambda: f.read(1024*1024), b''):
            h.update(ch)
    return h.hexdigest()

from datetime import datetime
import numpy as np
import pandas as pd
import re

def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(path)


def _atomic_write_json(path: Path, payload: object) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# 濡쒓퉭 ?ㅼ젙
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("generate_candidates")

# [NEW] Add FileHandler for strategy dashboard real-time tailing
try:
    from pathlib import Path
    _strategy_log_path = Path('E:/vibe/buffett/logs/strategy_decision.log')
    _strategy_log_path.parent.mkdir(parents=True, exist_ok=True)
    _fh = logging.FileHandler(_strategy_log_path, encoding='utf-8')
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(_fh)
except Exception as e:
    logger.warning("Failed to setup strategy_decision.log handler: %s", e)

# ROOT?뺤쓽 ?붿냼??utils import 媛???⑸룄濡?sys.path 異붽?
ROOT = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 怨듯넻 ?좏떥由ы떚 紐⑤뱢 import
from utils.common import read_json, write_json
from utils.stable_params_gate import evaluate_stable_params, load_stable_quality_gate
from utils.price_history_contract import apply_price_history_contract
from utils.gate_audit import log_gate_event
from utils.pipeline_audit import log_pipeline_event, count_rows

BASE_DIR = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
LOG_DIR = BASE_DIR / "2_Logs"
CACHE_DIR = BASE_DIR / "_cache"
RISK_DIR = BASE_DIR / "12_Risk_Controlled"
PAPER_ENGINE_CONFIG = BASE_DIR / "paper" / "paper_engine_config.json"

LOG_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
RISK_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PARAMS = {
    "rs_lim": 0.05,
    "v_accel_lim": 2.50,
    "v_accel_max": 5.00,     # upper bound: exclude extreme acceleration
    "defense_bear_rs_slope_min": 0.0,  # bear/crash regime: min rs_slope (0 = neutral)
    "defense_bear_disable_entry": 0.0,  # 1.0 => skip all entries in bear/crash/stress regime
    "stretch_max": 1.19,
    "value_min": 1_000_000_000.0,
    "atr_max": 0.12,     # ATR14/close (pct)
    "rsi_max": 70.0,     # avoid overbought entries
    "require_macd_golden": 0.0,  # 1.0 => require MACD golden cross
    "vol_close_corr_min": 0.0,   # rolling corr(close, volume)
    "near_52w_high_gap_max": 0.25,  # within 25% of 52w high (relaxed from 5%)
    "min_listing_days": 126.0,   # about 6 months
    # rule_e market/sector defense (2026-08-14). -1.0 => disabled (no-op).
    # Ported from optimize_params_v41_1.py::_select_day_candidates_operational so that
    # stable_params_v41_1.json values are actually honored in production.
    "mkt_ret20_min": -1.0,   # market 20d return must exceed this
    "mkt_ret60_min": -1.0,   # market 60d return must exceed this
    "sector_rs_min": -1.0,   # (sector 20d return - market 20d return) must exceed this
    "w_rs": 0.20,
    "w_rs_slope": 0.55,
    "w_v_accel": 0.25,
    "w_tech_score": 0.75,
    "w_fundamental_score": 0.25,
    "company_analyzer_enable": 0.0,  # 2026-07-24: disabled -- 98% rank-redundant with local fundamental blend (spearman=0.982), momentum/forward_value sub-factors non-functional, never tuned by optimizer
    "company_analyzer_blend": 0.70,
    "exclude_administrative": 1.0,
    "exclude_investment_warning": 1.0,
    "exclude_investment_risk": 1.0,
    "watch_penalty_caution": 0.10,
    "junk_risk_enable": 1.0,
    "junk_penalty_max": 0.18,
    "junk_hard_exclude": 1.0,
    "junk_hard_threshold": 88.0,
}

PARAM_EXPORT_KEYS = [
    "rs_lim",
    "v_accel_lim",
    "v_accel_max",
    "defense_bear_rs_slope_min",
    "defense_bear_disable_entry",
    "stretch_max",
    "value_min",
    "atr_max",
    "rsi_max",
    "require_macd_golden",
    "vol_close_corr_min",
    "near_52w_high_gap_max",
    "min_listing_days",
    "mkt_ret20_min",
    "mkt_ret60_min",
    "sector_rs_min",
    "w_rs",
    "w_rs_slope",
    "w_v_accel",
    "w_tech_score",
    "w_fundamental_score",
    "company_analyzer_enable",
    "company_analyzer_blend",
    "exclude_administrative",
    "exclude_investment_warning",
    "exclude_investment_risk",
    "watch_penalty_caution",
    "junk_risk_enable",
    "junk_penalty_max",
    "junk_hard_exclude",
    "junk_hard_threshold",
]

LOOKBACK_DAYS = 320
TOP_N = 10
ALLOWED_SECTOR_CODES = {"005", "008", "009", "011", "012", "013", "015", "016", "017", "018", "019", "020", "022", "024", "025", "026"}
SECTOR_SSOT_PATH = CACHE_DIR / "sector_ssot.csv"
SECTOR_MAP_PATH = CACHE_DIR / "krx_sector_to_sector_code_SSOT_v1_hotfix.csv"

# _jload -> utils.common.read_json濡??대룞??(read_json(p) or {} ?ъ슜)
# _jsave -> utils.common.write_json濡??대룞??

def _load_listing_map() -> pd.DataFrame:
    p = CACHE_DIR / "krx_listing.csv"
    if not p.exists():
        return pd.DataFrame(columns=["code", "name"])
    df = pd.read_csv(p, dtype={"code": str}, encoding="utf-8-sig")
    if "code" not in df.columns or "name" not in df.columns:
        return pd.DataFrame(columns=["code", "name"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    return df[["code", "name"]].drop_duplicates("code")

# _find_parquets -> utils.common.find_parquets濡??대룞??

# [2026-08-29] R1 배선. 생산은 Raw/ 를 한 번도 읽은 적이 없다.
#   원인 두 가지가 겹쳐 있었다:
#     (1) 탐색 디렉터리 목록에 Raw 가 없었다
#     (2) Raw 의 파일은 `_clean` 접미사가 없는데, _find_krx_parquets 가 _clean 을
#         하나라도 찾으면 거기서 반환해 비-clean 패턴은 아예 시도하지 않는다
#   그 결과 Raw/krx_daily_20221001_20251224.parquet (3,158,510행, 코스닥 203만)
#   이 통째로 빠져 2022-10~2024-12 유니버스가 결손됐다.
#   연구용 tools/load_merged_panel.py 는 이미 이 파일을 읽고 있었다 = 생산만의 결손.
#
#   Raw 는 **가장 낮은 우선순위**로 넣는다. 겹치는 (code,date) 는 종전 그대로
#   archive/_krx_manual 값이 남고, Raw 는 아무도 안 가진 날짜만 채운다.
#   -> 기존 행은 한 줄도 바뀌지 않고 결손만 메워진다.
_KRX_SOURCE_DIRS = (
    (BASE_DIR / "Raw", 0),
    (BASE_DIR, 1),
    (BASE_DIR / "krx_daily_archive", 2),
    (BASE_DIR / "_krx_manual", 3),
)


def _krx_src_priority(path: Path) -> int:
    parent = path.parent
    for directory, prio in _KRX_SOURCE_DIRS:
        if parent == directory:
            return prio
    return 0


def _bounded_krx_glob(pattern: str) -> list[Path]:
    """Return only canonical, non-recursive KRX parquet sources."""
    out: list[Path] = []
    seen: set[str] = set()
    for directory, _prio in _KRX_SOURCE_DIRS:
        if not directory.exists() or not directory.is_dir():
            continue
        for path in directory.glob(pattern):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(path)
    return sorted(out)


def _find_krx_parquets() -> list[Path]:
    files = _bounded_krx_glob("krx_daily_*_clean.parquet")
    if not files:
        return _bounded_krx_glob("krx_daily_*.parquet")
    # _clean 을 찾았더라도 Raw 의 비-clean 광역 백필은 별도로 합친다.
    seen = {str(p.resolve()) for p in files}
    for path in _bounded_krx_glob("krx_daily_*.parquet"):
        if _krx_src_priority(path) != 0 or path.parent != (BASE_DIR / "Raw"):
            continue
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            files.append(path)
    return sorted(files)


def _map_macro_to_candidate_regime(macro_regime: str, fallback_is_bull: bool = False) -> str:
    regime = str(macro_regime or "").strip().upper()
    if regime in {"BULL", "RALLY", "RECOVERY"}:
        return "BULL"
    if regime in {"BEAR", "CRASH", "RISK_OFF"}:
        return "BEAR"
    if regime in {"CORRECTION", "PULLBACK", "VOLATILE"}:
        return "CORRECTION"
    if regime in {"SIDEWAYS", "NORMAL"}:
        return "SIDEWAYS"
    return "CORRECTION" if bool(fallback_is_bull) else "BEAR"


def _should_fail_closed_on_factor_guard(factor_guard: dict) -> tuple[bool, str]:
    if not isinstance(factor_guard, dict):
        return False, ""
    lookahead = factor_guard.get("lookahead")
    if not isinstance(lookahead, dict):
        return False, ""
    try:
        negative_shift_detected = int(float(lookahead.get("negative_shift_detected", 0) or 0))
    except Exception:
        negative_shift_detected = 0
    if negative_shift_detected <= 0:
        return False, ""
    terms = lookahead.get("negative_shift_terms")
    if isinstance(terms, (list, tuple)):
        terms_text = ",".join(str(x) for x in terms if str(x).strip())
    else:
        terms_text = str(terms or "").strip()
    reason = "negative_shift_detected"
    if terms_text:
        reason = f"{reason}:{terms_text}"
    return True, reason


def _load_data(*, max_gap_sessions: int = 0) -> pd.DataFrame:
    files = _find_krx_parquets()
    if not files:
        print("[ERR] parquet files not found (krx_daily_*_clean.parquet).")
        return pd.DataFrame()
    files = sorted(files)
    print(f"[LOAD] parquet files={len(files)}")
    parts = []
    skipped = []

    for f in files:
        try:
            df = pd.read_parquet(f)
        except Exception as e:
            skipped.append((str(f), f"read_error:{e.__class__.__name__}"))
            continue

        mandatory_cols = ["date", "code", "open", "high", "low", "close", "volume", "value"]
        missing_mandatory = [c for c in mandatory_cols if c not in df.columns]
        if missing_mandatory:
            raise ValueError(f"parquet missing mandatory cols={missing_mandatory} file={f}")

        if "date" not in df.columns:
            skipped.append((str(f), "missing_date"))
            continue

        df = df.copy()
        df["date"] = _parse_date_col(df["date"])
        n_ok = int(pd.Series(df["date"]).notna().sum())
        if n_ok == 0:
            skipped.append((str(f), "date_parse_all_na"))
            continue

        df = df.loc[pd.Series(df["date"]).notna()].copy()
        df["_src_priority"] = _krx_src_priority(f)
        try:
            df["_src_mtime"] = f.stat().st_mtime
        except OSError:
            df["_src_mtime"] = 0.0
        parts.append(df)

    if skipped:
        print("[LOAD_SKIP]", len(skipped))
        for path, reason in skipped[:20]:
            print("  -", reason, path)
        if len(skipped) > 20:
            print("  ...")

    if not parts:
        print("[ERR] no usable parquet parts after date-parse.")
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)

    if "code" not in df.columns:
        print("[ERR] parquet data missing code column.")
        return pd.DataFrame()

    rows_loaded = int(len(df))
    df["code"] = df["code"].astype(str).str.zfill(6)
    for column in ["open", "high", "low", "close", "volume", "value"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df.loc[df[column] == 0, column] = np.nan

    df = df.dropna(subset=["close", "open", "high", "low", "value"])
    rows_before_dedupe = int(len(df))
    df = (
        df.sort_values(["_src_priority", "_src_mtime"])
        .drop_duplicates(subset=["code", "date"], keep="last")
        .drop(columns=["_src_priority", "_src_mtime"])
    )
    print(
        f"[LOAD_DEDUPE] rows_loaded={rows_loaded} required_rows={rows_before_dedupe} "
        f"rows_after={len(df)} removed={rows_before_dedupe - len(df)}"
    )
    df, integrity = apply_price_history_contract(df, max_gap_sessions=max_gap_sessions)
    print(f"[PRICE_HISTORY_INTEGRITY] {integrity['log_line']}")
    return df


def _normalize_params(p: dict) -> dict:
    x = dict(DEFAULT_PARAMS)
    x.update({k: p.get(k, DEFAULT_PARAMS.get(k)) for k in DEFAULT_PARAMS.keys()})

    # numeric cast + bounds (defensive)
    def fnum(v, default):
        try:
            return float(v)
        except Exception:
            return float(default)

    x["rs_lim"] = min(max(fnum(x["rs_lim"], DEFAULT_PARAMS["rs_lim"]), -0.10), 1.00)
    x["v_accel_lim"] = min(max(fnum(x["v_accel_lim"], DEFAULT_PARAMS["v_accel_lim"]), 1.20), 20.0)
    x["stretch_max"] = min(max(fnum(x["stretch_max"], DEFAULT_PARAMS["stretch_max"]), 1.05), 1.30)
    x["value_min"] = min(max(fnum(x["value_min"], DEFAULT_PARAMS["value_min"]), 1_000_000_000.0), 500_000_000_000.0)
    x["atr_max"] = min(max(fnum(x["atr_max"], DEFAULT_PARAMS["atr_max"]), 0.08), 0.40)
    x["rsi_max"] = min(max(fnum(x["rsi_max"], DEFAULT_PARAMS["rsi_max"]), 50.0), 90.0)
    x["require_macd_golden"] = 1.0 if fnum(x["require_macd_golden"], DEFAULT_PARAMS["require_macd_golden"]) >= 0.5 else 0.0
    x["vol_close_corr_min"] = min(max(fnum(x["vol_close_corr_min"], DEFAULT_PARAMS["vol_close_corr_min"]), -1.0), 1.0)
    x["near_52w_high_gap_max"] = min(max(fnum(x["near_52w_high_gap_max"], DEFAULT_PARAMS["near_52w_high_gap_max"]), 0.0), 0.50)
    x["min_listing_days"] = min(max(fnum(x["min_listing_days"], DEFAULT_PARAMS["min_listing_days"]), 0.0), 1000.0)

    # rule_e thresholds: clamp to [-1.0, 1.0]. -1.0 is the "disabled" sentinel, so the
    # lower bound must stay exactly -1.0 and 0.0 must survive as a real threshold.
    for _k in ("mkt_ret20_min", "mkt_ret60_min", "sector_rs_min"):
        x[_k] = min(max(fnum(x[_k], DEFAULT_PARAMS[_k]), -1.0), 1.0)

    # technical score weights normalize
    w_rs = fnum(x["w_rs"], DEFAULT_PARAMS["w_rs"])
    w_sl = fnum(x["w_rs_slope"], DEFAULT_PARAMS["w_rs_slope"])
    w_va = fnum(x["w_v_accel"], DEFAULT_PARAMS["w_v_accel"])
    s = w_rs + w_sl + w_va
    if s <= 0:
        w_rs, w_sl, w_va = DEFAULT_PARAMS["w_rs"], DEFAULT_PARAMS["w_rs_slope"], DEFAULT_PARAMS["w_v_accel"]
        s = w_rs + w_sl + w_va
    x["w_rs"], x["w_rs_slope"], x["w_v_accel"] = w_rs / s, w_sl / s, w_va / s

    # final blend weights normalize
    w_tech = fnum(x.get("w_tech_score", DEFAULT_PARAMS["w_tech_score"]), DEFAULT_PARAMS["w_tech_score"])
    w_fund = fnum(x.get("w_fundamental_score", DEFAULT_PARAMS["w_fundamental_score"]), DEFAULT_PARAMS["w_fundamental_score"])
    ws = w_tech + w_fund
    if ws <= 0:
        w_tech = DEFAULT_PARAMS["w_tech_score"]
        w_fund = DEFAULT_PARAMS["w_fundamental_score"]
        ws = w_tech + w_fund
    x["w_tech_score"] = w_tech / ws
    x["w_fundamental_score"] = w_fund / ws

    x["company_analyzer_enable"] = 1.0 if fnum(x.get("company_analyzer_enable", 1.0), 1.0) >= 0.5 else 0.0
    x["company_analyzer_blend"] = min(max(fnum(x.get("company_analyzer_blend", 0.70), 0.70), 0.0), 1.0)

    x["exclude_administrative"] = 1.0 if fnum(x.get("exclude_administrative", 1.0), 1.0) >= 0.5 else 0.0
    x["exclude_investment_warning"] = 1.0 if fnum(x.get("exclude_investment_warning", 1.0), 1.0) >= 0.5 else 0.0
    x["exclude_investment_risk"] = 1.0 if fnum(x.get("exclude_investment_risk", 1.0), 1.0) >= 0.5 else 0.0
    x["watch_penalty_caution"] = min(max(fnum(x.get("watch_penalty_caution", 0.10), 0.10), 0.0), 0.9)

    x["junk_risk_enable"] = 1.0 if fnum(x.get("junk_risk_enable", 1.0), 1.0) >= 0.5 else 0.0
    x["junk_penalty_max"] = min(max(fnum(x.get("junk_penalty_max", 0.18), 0.18), 0.0), 0.35)
    x["junk_hard_exclude"] = 1.0 if fnum(x.get("junk_hard_exclude", 1.0), 1.0) >= 0.5 else 0.0
    x["junk_hard_threshold"] = min(max(fnum(x.get("junk_hard_threshold", 88.0), 88.0), 60.0), 99.0)

    return x

def _parse_date_col(s):
    """
    Robust date parser for YYYYMMDD-like values.
    - Accepts int/str like 20260218, "20260218", "2026-02-18"
    - Returns pandas datetime64[ns] (NaT on failure)
    """
    import pandas as pd
    x = pd.Series(s)
    if pd.api.types.is_datetime64_any_dtype(x):
        try:
            return x.dt.normalize()
        except Exception:
            return x
    xs = x.astype(str).str.strip()
    xs = xs.str.replace("-", "", regex=False).str.replace("/", "", regex=False).str.replace(".", "", regex=False)
    xs = xs.str.extract(r"(\d{8})", expand=False)
    return pd.to_datetime(xs, format="%Y%m%d", errors="coerce")
def _compute_factors(df: pd.DataFrame):
    df = df.copy()
    # sanitize
    df["date"] = _parse_date_col(df["date"])

    for c in ["open", "high", "low", "close", "value"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] == 0, c] = np.nan
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df.loc[df["volume"] == 0, "volume"] = np.nan
    else:
        # Fallback volume proxy when raw volume is missing.
        df["volume"] = df["value"] / (df["close"] + 1e-9)

    # market may not exist
    if "market" not in df.columns:
        df["market"] = ""

    df = df.dropna(subset=["date", "code", "close"]).copy()
    df = df.sort_values(["code", "date"])

    # Market proxy: use each row's KOSPI/KOSDAQ peer group when available,
    # with the old all-market average as a compatibility fallback.
    df["market"] = df["market"].fillna("").astype(str).str.upper().str.strip()

    # [2026-08-21] 해결된 시장 라벨을 **별도 컬럼**으로만 붙인다. `market` 은 건드리지 않는다.
    #
    # KRX clean parquet 의 market 은 2026-05 부터 절반이 "UNKNOWN" 이다(수집 경로가 값을 버렸다).
    # 마스터(`2_Logs/market_master_latest.json`)로 100% 복구되지만, `market` 을 덮어쓰면
    # 아래 peer-group 지표(m_ret_20 / market_is_bull)가 같이 바뀐다. 실측으로 최신일 기준
    # 516종목의 market_is_bull 판정이 뒤집히고 KOSPI 그룹 bull 이 True -> False 가 된다.
    # 그건 데이터 보강이 아니라 매매 동작 변경이므로 승인 없이 하지 않는다.
    # 지금은 관측 가능하게만 만들어 둔다. 소비자는 아직 없다.
    # 상세: .agent/PLANS.md 2026-08-21 (11)
    try:
        _mm_path = BASE_DIR / "2_Logs" / "market_master_latest.json"
        _mm = {}
        if _mm_path.exists():
            _mm_doc = json.loads(_mm_path.read_text(encoding="utf-8-sig"))
            _mm_codes = _mm_doc.get("codes")
            if isinstance(_mm_codes, dict):
                _mm = {
                    str(k).zfill(6): str(v).upper()
                    for k, v in _mm_codes.items()
                    if str(v).upper() in ("KOSPI", "KOSDAQ")
                }
        if _mm:
            _cur = df["market"]
            _resolved = df["code"].astype(str).str.zfill(6).map(_mm)
            df["market_resolved"] = _cur.where(_cur.isin(["KOSPI", "KOSDAQ"]), _resolved).fillna("")
        else:
            df["market_resolved"] = df["market"]
    except Exception as _mm_exc:
        print(f"[MARKET_MASTER] resolve skipped: {type(_mm_exc).__name__}: {_mm_exc}")
        df["market_resolved"] = df["market"]
    overall_idx = df.groupby("date", as_index=True)["close"].mean().sort_index()
    overall_ret_20 = overall_idx.pct_change(20)
    overall_ret_60 = overall_idx.pct_change(60)
    overall_bull = overall_idx > overall_idx.rolling(60).mean()

    # [2026-08-21] peer-group 기준 컬럼. 기본은 예전대로 `market` 이다.
    #
    # `market` 은 2026-05 부터 절반이 "UNKNOWN" 이라, 그 47% 가 존재하지 않는 시장의
    # 평균을 벤치마크로 쓰고 있다. 그런데 아래에서 `rs = ret_20 - m_ret_20` 을 만들고
    # rs 는 이 시스템의 주 신호다(w_rs=0.29, rs_lim 하드 게이트).
    # 즉 **주 신호가 절반의 종목에서 엉터리 기준으로 측정된다.**
    #
    # `market_resolved` 로 바꾸면 패널 전체(과거 포함)가 한 번에 일관되게 라벨링된다.
    # 마스터에서 매번 계산하므로 과거 parquet 을 손댈 필요가 없다.
    # 실측 영향(2026-08-20, 2,580종목): rs 게이트 통과 1,512 -> 1,416,
    # 판정이 뒤집히는 종목 260(신규통과 82 / 탈락 178). **후보가 줄어드는 방향이다.**
    #
    # 그래서 기본 OFF 다. 켜는 시점을 골라 후보 수 변화를 바로 대조할 수 있게 한다.
    # `CAND_USE_MARKET_RESOLVED=1` 로 켠다. 상세: .agent/PLANS.md 2026-08-21 (12)
    _peer_col = "market"
    if str(os.environ.get("CAND_USE_MARKET_RESOLVED", "0")).strip().lower() in ("1", "true", "yes", "on"):
        if "market_resolved" in df.columns:
            _peer_col = "market_resolved"
        else:
            print("[MARKET_MASTER] CAND_USE_MARKET_RESOLVED=1 이지만 market_resolved 컬럼이 없어 market 을 쓴다")
    print(f"[MARKET_MASTER] peer_group_col={_peer_col}")

    market_idx = (
        df[df[_peer_col].ne("")]
        .groupby([_peer_col, "date"], as_index=False)["close"]
        .mean()
        .sort_values([_peer_col, "date"])
    )
    if not market_idx.empty:
        market_idx["m_ret_20"] = market_idx.groupby(_peer_col)["close"].pct_change(20)
        market_idx["market_ma60"] = market_idx.groupby(_peer_col)["close"].transform(lambda x: x.rolling(60).mean())
        market_idx["market_is_bull"] = market_idx["close"] > market_idx["market_ma60"]
        df = df.merge(
            market_idx[[_peer_col, "date", "m_ret_20", "market_is_bull"]],
            on=[_peer_col, "date"],
            how="left",
            sort=False,
        )
    else:
        df["m_ret_20"] = np.nan
        df["market_is_bull"] = pd.NA

    df["m_ret_20"] = df["m_ret_20"].fillna(df["date"].map(overall_ret_20))
    df["market_is_bull"] = df["market_is_bull"].where(df["market_is_bull"].notna(), df["date"].map(overall_bull))
    df["market_regime"] = np.where(df["market_is_bull"].fillna(True).astype(bool), "BULL", "BEAR")

    # RS / slope
    df["ret_20"] = df.groupby("price_history_key", sort=False)["close"].pct_change(20)
    df["rs"] = df["ret_20"] - df["m_ret_20"]
    df["rs_slope"] = df.groupby("price_history_key", sort=False)["rs"].diff(5)

    # MA5 stretch
    df["ma5"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(5).mean())
    df["stretch"] = df["close"] / (df["ma5"] + 1e-9)

    df["ma60"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(60).mean())
    df["ma60_support_bounce"] = ((df["low"] <= df["ma60"]) & (df["close"] > df["ma60"])).fillna(False).astype(int)



    # value accel
    df["v_ma5"] = df.groupby("price_history_key", sort=False)["value"].transform(lambda x: x.rolling(5).mean())
    df["v_accel"] = df["value"] / (df.groupby("price_history_key", sort=False)["v_ma5"].shift(1) + 1e-9)

    # RSI(14): overbought guard
    delta = df.groupby("price_history_key", sort=False)["close"].diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    avg_up = up.groupby(df["price_history_key"]).transform(lambda x: x.ewm(alpha=1/14, adjust=False, min_periods=14).mean())
    avg_down = down.groupby(df["price_history_key"]).transform(lambda x: x.ewm(alpha=1/14, adjust=False, min_periods=14).mean())
    rs = avg_up / (avg_down + 1e-9)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs))

    # MACD(12,26,9): optional trend turn confirmation
    ema12 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("price_history_key", sort=False)["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    prev_macd = df.groupby("price_history_key", sort=False)["macd_line"].shift(1)
    prev_sig = df.groupby("price_history_key", sort=False)["macd_signal"].shift(1)
    df["macd_golden"] = (df["macd_line"] > df["macd_signal"]) & (prev_macd <= prev_sig)

    # Rolling correlation(close, volume): confirm price move with volume participation
    df["vol_close_corr20"] = (
        df.groupby("price_history_key", sort=False)["close"]
          .transform(lambda x: x.rolling(20, min_periods=20).corr(df.loc[x.index, "volume"]))
    )

    # 52-week high proximity (252 trading days)
    df["high_52w"] = df.groupby("price_history_key", sort=False)["close"].transform(lambda x: x.rolling(252, min_periods=20).max())
    df["high_52w_gap"] = ((df["high_52w"] - df["close"]) / (df["high_52w"] + 1e-9)).clip(lower=0.0)

    # Listing age guard: exclude short-history names
    first_date = df.groupby("code")["date"].transform("min")
    df["listing_days"] = (df["date"] - first_date).dt.days

    # ATR14 pct
    prev_close = df.groupby("price_history_key", sort=False)["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)

    df["tr"] = tr
    df["atr14"] = df.groupby("price_history_key", sort=False)["tr"].transform(lambda x: x.rolling(14, min_periods=14).mean())
    df["atr14_pct"] = df["atr14"] / (df["close"] + 1e-9)
    df["ret1_pct"] = df.groupby("price_history_key", sort=False)["close"].pct_change(1) * 100.0

    # --- rule_e market / sector defense factors (2026-08-14) ---
    # Definitions are copied from optimize_params_v41_1.py::compute_factors(), which is the
    # code that produced stable_params_v41_1.json. Do NOT switch these to the all-market
    # index without also changing the optimizer: report_backtest_v41_1.py uses the
    # all-market index for mkt_ret20 and therefore already disagrees with the optimizer.
    #   mkt_ret20 = per-market (KOSPI/KOSDAQ) 20d return, all-market fallback  [optimizer m_ret20]
    #   mkt_ret60 = all-market 60d return                                      [optimizer m_ret60]
    #   sector_rs = sector 20d return - mkt_ret20                              [optimizer sector_rs]
    df["mkt_ret20"] = df["m_ret_20"]
    df["mkt_ret60"] = df["date"].map(overall_ret_60)

    if "sector_code" not in df.columns:
        # Unrestricted map: the optimizer builds its sector index from the full symbol panel,
        # not from ALLOWED_SECTOR_CODES, so the sector index composition must match.
        _sc_map = _build_sector_code_map(restrict_allowed=False)
        if _sc_map.empty:
            df["sector_code"] = ""
        else:
            # Join on a temporary zero-filled key: df["code"] must not be rewritten here,
            # since everything downstream keys off its existing form.
            df["_sc_key"] = df["code"].astype(str).str.zfill(6)
            df = df.merge(
                _sc_map.rename(columns={"code": "_sc_key"}),
                on="_sc_key",
                how="left",
                sort=False,
            ).drop(columns=["_sc_key"], errors="ignore")

    df["sector_code"] = df["sector_code"].fillna("").astype(str).str.strip()
    _sector_rows = df[df["sector_code"].ne("")]
    if _sector_rows.empty:
        df["sector_ret20"] = np.nan
        df["sector_rs"] = np.nan
    else:
        sector_idx = (
            _sector_rows
            .groupby(["sector_code", "date"], as_index=False)["close"]
            .mean()
            .sort_values(["sector_code", "date"])
        )
        sector_idx["sector_ret20"] = sector_idx.groupby("sector_code")["close"].pct_change(20)
        df = df.merge(
            sector_idx[["sector_code", "date", "sector_ret20"]],
            on=["sector_code", "date"],
            how="left",
            sort=False,
        )
        df["sector_rs"] = df["sector_ret20"] - df["mkt_ret20"]

    # latest date + bull flag
    # --- AS_OF selection: prefer latest date with sufficient universe size ---
    MIN_UNI = 2000
    if df is None or df.empty:
        latest_dt = pd.Timestamp.now().normalize()
        df.attrs["as_of_select"] = {"min_uni": int(MIN_UNI), "latest_raw_max": None, "codes_today_raw": 0, "latest_selected": None, "codes_today_selected": 0}
    else:
        df["date8"] = df["date"].astype(str).str.replace("-", "").str[:8]
        df["code"] = df["code"].astype(str).str.strip().str.upper()
        df["date"] = pd.to_datetime(df["date8"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"]).reset_index(drop=True)
        g = df.groupby("date8")["code"].nunique()
        latest_raw_max8 = str(df["date8"].max())
        elig = g[g >= MIN_UNI]
        latest_selected8 = (str(elig.index.max()) if len(elig) else latest_raw_max8)
        latest_dt = pd.to_datetime(latest_selected8, format="%Y%m%d")
        df.attrs["as_of_select"] = {
            "min_uni": int(MIN_UNI),
            "latest_raw_max": latest_raw_max8,
            "codes_today_raw": int(g.get(latest_raw_max8, 0)),
            "latest_selected": latest_selected8,
            "codes_today_selected": int(g.get(latest_selected8, 0)),
        }
        print(f"[AS_OF_SELECT] MIN_UNI={MIN_UNI} latest_raw_max={latest_raw_max8} codes_today_raw={int(g.get(latest_raw_max8,0))} latest_selected={latest_selected8} codes_today_selected={int(g.get(latest_selected8,0))}")
        # --- as_of_select snapshot (auto, fixed) ---
        as_of_select = {
            'MIN_UNI': int(MIN_UNI),
            'latest_raw_max': str(latest_raw_max8),
            'codes_today_raw': int(g.get(latest_raw_max8, 0)),
            'latest_selected': str(latest_selected8),
            'codes_today_selected': int(g.get(latest_selected8, 0)),
        }
        df.attrs['as_of_select'] = as_of_select
    latest_market_bull = market_idx[market_idx["date"].eq(latest_dt)] if not market_idx.empty else pd.DataFrame()
    # [2026-08-21] market_idx 는 _peer_col 로 묶여 있다. "market" 을 박아두면 스위치를 켠 순간 KeyError 다.
    kospi_latest = latest_market_bull[latest_market_bull[_peer_col].eq("KOSPI")] if not latest_market_bull.empty else pd.DataFrame()
    if not kospi_latest.empty:
        val = kospi_latest["market_is_bull"].iloc[-1]
    else:
        val = overall_bull.loc[latest_dt] if latest_dt in overall_bull.index else pd.NA
    if pd.isna(val):
        is_bull = True
    else:
        is_bull = bool(val)

    return df, latest_dt, is_bull


def _rule_e_threshold(p: dict, key: str):
    """Return the rule_e threshold for `key`, or None when the filter is disabled.

    -1.0 is the disabled sentinel. 0.0 is a real threshold and must not be swallowed,
    so this never uses `float(p.get(key) or default)`.
    """
    raw = p.get(key, -1.0)
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(val) or val <= -1.0:
        return None
    return val


def _diag_counts(today: pd.DataFrame, p: dict, n_all: int) -> dict:
    return {
        "rows_today": int(len(today)),
        "rs_pass": int((today["rs"] > float(p["rs_lim"])).sum()),
        "v_accel_pass": int((today["v_accel"] > float(p["v_accel_lim"])).sum()),
        # NOTE: v_accel_max is intentionally NOT enforced in production
        # _select_candidates (HEAD 2026-08-08 behavior). Keep it out of the
        # diagnostic counts so the log does not claim a filter is active when it
        # is not.
        "stretch_pass": int((today["stretch"] < float(p["stretch_max"])).sum()),
        "value_pass": int((today["value"] > float(p["value_min"])).sum()),
        "atr_pass": int((today["atr14_pct"] < float(p["atr_max"])).sum()),
        "rsi_pass": int((today["rsi14"] < float(p["rsi_max"])).sum()),
        "macd_pass": int((today["macd_golden"] == True).sum()),
        "volcorr_pass": int((today["vol_close_corr20"] >= float(p["vol_close_corr_min"])).sum()),
        "high52_pass": int((today["high_52w_gap"] <= float(p["near_52w_high_gap_max"])).sum()),
        "listing_pass": int((today["listing_days"] >= float(p["min_listing_days"])).sum()),
        **_rule_e_diag_counts(today, p),
        "all_pass": int(n_all),
    }


def _rule_e_diag_counts(today: pd.DataFrame, p: dict) -> dict:
    """Per-gate pass counts for rule_e, so a 0-candidate day is attributable."""
    out = {}
    for col, key, label in (
        ("mkt_ret20", "mkt_ret20_min", "mkt_ret20_pass"),
        ("mkt_ret60", "mkt_ret60_min", "mkt_ret60_pass"),
        ("sector_rs", "sector_rs_min", "sector_rs_pass"),
    ):
        thr = _rule_e_threshold(p, key)
        if thr is None or col not in today.columns:
            out[label] = int(len(today))  # gate disabled => everything passes
        else:
            out[label] = int((today[col] > thr).sum())
    return out

def _select_candidates(today: pd.DataFrame, p: dict) -> pd.DataFrame:
    require_macd = float((p.get("require_macd_golden") if p.get("require_macd_golden") is not None else 0.0)) >= 0.5
    cond = (
        (today["rs"] > float(p["rs_lim"]))
        & (today["v_accel"] > float(p["v_accel_lim"]))
        & (today["stretch"] < float(p["stretch_max"]))
        & (today["value"] > float(p["value_min"]))
        & (today["atr14_pct"] < float(p["atr_max"]))
        & (today["rsi14"] < float(p["rsi_max"]))
        & (today["vol_close_corr20"] >= float(p["vol_close_corr_min"]))
        & (today["high_52w_gap"] <= float(p["near_52w_high_gap_max"]))
        & (today["listing_days"] >= float(p["min_listing_days"]))
    )
    if require_macd:
        cond = cond & (today["macd_golden"] == True)

    # rule_e market/sector defense (restored 2026-08-15).
    #
    # These three gates were wired into production on 2026-08-14 as a deliberate
    # policy: "do not trade this market today". They were lost on 2026-08-15 when
    # _select_candidates was rolled back to the 08-08 revision to undo an
    # unrelated regression -- a shared candidate core had introduced v_accel_max
    # and the bear gate into production, and v_accel_lim 6.6 > v_accel_max 5.0
    # made the filter an empty set. Only v_accel_max and the bear gate were meant
    # to go; rule_e is restored here on its own.
    #
    # Semantics match _apply_sector_prefilter_union()'s soft gate and
    # report_backtest_v41_1.py:778-783 exactly: strict >, so NaN is excluded, and
    # -1.0 (via _rule_e_threshold) means disabled. Without this the union fallback
    # was stricter than the main path it exists to widen.
    #
    # rule_e is intentionally NOT relaxed by _relax_ladder(): it is a market-level
    # gate, not a per-name threshold.
    for _col, _key in (
        ("mkt_ret20", "mkt_ret20_min"),
        ("mkt_ret60", "mkt_ret60_min"),
        ("sector_rs", "sector_rs_min"),
    ):
        _thr = _rule_e_threshold(p, _key)
        if _thr is None:
            continue
        if _col not in today.columns:
            # Do not fail silently. A missing factor column would disable an active
            # policy gate without a trace -- the exact failure mode that cost the
            # 2026-08 audit weeks.
            print(f"[WARN] rule_e {_key} active but column '{_col}' missing; gate NOT applied")
            continue
        cond = cond & (pd.to_numeric(today[_col], errors="coerce") > _thr)

    return today[cond].copy()




_COMPANY_ANALYZER_OBJ = None
_COMPANY_ANALYZER_INIT = False
_DART_REFRESH_INIT = False
_KRX_WATCH_REFRESH_INIT = False


def _norm_code6(v: object) -> str:
    s = str(v).strip()
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    if s.isdigit():
        return s.zfill(6)
    return s.upper()


def _read_csv_if_exists(path: Path, dtype: dict | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=(dtype or {}))
    except Exception:
        return pd.DataFrame()


def _first_existing(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None



def _maybe_refresh_dart_snapshot(as_of_ymd: str) -> None:
    global _DART_REFRESH_INIT
    if _DART_REFRESH_INIT:
        return
    _DART_REFRESH_INIT = True

    if str(os.environ.get("FUND_DART_REFRESH", "0")).strip() != "1":
        return

    tool = BASE_DIR / "tools" / "build_dart_fundamental_snapshot.py"
    if not tool.exists():
        print(f"[WARN] DART refresh skipped: tool missing ({tool})")
        return

    key = str(os.environ.get("DART_API_KEY", "")).strip()
    key_file_env = str(os.environ.get("DART_API_KEY_FILE", "")).strip()
    default_key_file = CACHE_DIR / "dart_api_key.txt"

    cmd = [sys.executable, str(tool), "--as-of", str(as_of_ymd), "--max-codes", "300"]
    if key_file_env:
        cmd.extend(["--api-key-file", key_file_env])
    elif (not key) and default_key_file.exists():
        cmd.extend(["--api-key-file", str(default_key_file)])

    if not key and ("--api-key-file" not in cmd):
        print("[FUND] DART refresh skipped: missing API key")
        return

    codes_hint = LOG_DIR / "candidates_latest_data.csv"
    if codes_hint.exists():
        cmd.extend(["--codes-file", str(codes_hint)])

    env = os.environ.copy()
    if key:
        env["DART_API_KEY"] = key

    try:
        print("[FUND] DART refresh requested (FUND_DART_REFRESH=1)")
        cp = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=900)
        if cp.returncode != 0:
            stderr_tail = "\n".join((cp.stderr or "").splitlines()[-6:])
            print(f"[WARN] DART refresh failed: rc={cp.returncode}")
            if stderr_tail:
                print(stderr_tail)
        else:
            out_tail = "\n".join((cp.stdout or "").splitlines()[-6:])
            if out_tail:
                print(out_tail)
    except Exception as e:
        print(f"[WARN] DART refresh exception: {type(e).__name__}: {e}")

def _load_pykrx_fundamental_snapshot(as_of_ymd: str) -> pd.DataFrame:
    cache_candidates = [
        CACHE_DIR / f"pykrx_fundamental_{as_of_ymd}.csv",
        CACHE_DIR / "pykrx_fundamental_latest.csv",
        LOG_DIR / "pykrx_fundamental_latest.csv",
    ]

    src = _first_existing(cache_candidates)
    if src is not None:
        df = _read_csv_if_exists(src, dtype={"code": str})
        if not df.empty:
            if "code" not in df.columns:
                idx = next((c for c in ["ticker", "종목코드", "code"] if c in df.columns), None)
                if idx:
                    df = df.rename(columns={idx: "code"})
            if "code" in df.columns:
                df["code"] = df["code"].map(_norm_code6)
                keep = [c for c in ["code", "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap", "listed_shares"] if c in df.columns]
                if "code" in keep:
                    return df[keep].drop_duplicates("code")

    if str(os.environ.get("FUND_PYKRX_REFRESH", "0")).strip() == "0":
        return pd.DataFrame(columns=["code", "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap", "listed_shares"])

    try:
        from pykrx import stock as pykrx_stock  # type: ignore

        df = pykrx_stock.get_market_fundamental_by_ticker(as_of_ymd, market="ALL")
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap", "listed_shares"])

        z = df.reset_index().copy()
        code_col = next((c for c in ["티커", "종목코드", "Ticker", "ticker", "code"] if c in z.columns), None)
        if code_col is None:
            code_col = z.columns[0]
        z = z.rename(columns={code_col: "code"})
        z["code"] = z["code"].map(_norm_code6)

        col_alias = {
            "BPS": ["BPS", "bps"],
            "PER": ["PER", "per"],
            "PBR": ["PBR", "pbr"],
            "EPS": ["EPS", "eps"],
            "DIV": ["DIV", "div", "DVD_YLD"],
            "DPS": ["DPS", "dps"],
        }
        out = pd.DataFrame({"code": z["code"]})
        for k, aliases in col_alias.items():
            c = next((a for a in aliases if a in z.columns), None)
            if c is not None:
                out[k] = pd.to_numeric(z[c], errors="coerce")

        # Add market-cap context for cross-sectional sizing/capacity analysis.
        try:
            mc_raw = pykrx_stock.get_market_cap_by_ticker(as_of_ymd, market="ALL")
            if mc_raw is not None and (not mc_raw.empty):
                mc = mc_raw.reset_index().copy()
                mc_code_col = next((cc for cc in ["티커", "종목코드", "Ticker", "ticker", "code"] if cc in mc.columns), None)
                if mc_code_col is None:
                    mc_code_col = mc.columns[0]
                mc = mc.rename(columns={mc_code_col: "code"})
                mc["code"] = mc["code"].map(_norm_code6)

                cap_col = next((cc for cc in ["시가총액", "market_cap", "MarketCap"] if cc in mc.columns), None)
                sh_col = next((cc for cc in ["상장주식수", "listed_shares", "ListedShares"] if cc in mc.columns), None)

                mc_out = pd.DataFrame({"code": mc["code"]})
                if cap_col is not None:
                    mc_out["market_cap"] = pd.to_numeric(mc[cap_col], errors="coerce")
                if sh_col is not None:
                    mc_out["listed_shares"] = pd.to_numeric(mc[sh_col], errors="coerce")

                mc_keep = [cc for cc in ["code", "market_cap", "listed_shares"] if cc in mc_out.columns]
                if len(mc_keep) >= 2:
                    out = out.merge(mc_out[mc_keep].drop_duplicates("code"), on="code", how="left")
        except Exception as e:
            print(f"[WARN] pykrx market cap fetch failed: {type(e).__name__}: {e}")

        out = out.drop_duplicates("code")
        if not out.empty:
            save_p = CACHE_DIR / "pykrx_fundamental_latest.csv"
            try:
                out.to_csv(save_p, index=False, encoding="utf-8-sig")
            except Exception:
                pass
        return out
    except Exception as e:
        print(f"[WARN] pykrx fundamental fetch failed: {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["code", "PER", "PBR", "BPS", "EPS", "DIV", "DPS", "market_cap", "listed_shares"])


def _load_dart_fundamental_snapshot(as_of_ymd: str) -> pd.DataFrame:
    _maybe_refresh_dart_snapshot(as_of_ymd)

    paths = [
        CACHE_DIR / f"dart_fundamental_{as_of_ymd}.csv",
        CACHE_DIR / "dart_fundamental_latest.csv",
        LOG_DIR / "dart_fundamental_latest.csv",
    ]
    src = _first_existing(paths)
    if src is None:
        return pd.DataFrame(columns=["code"])

    df = _read_csv_if_exists(src, dtype={"code": str, "ticker": str, "종목코드": str})
    if df.empty:
        return pd.DataFrame(columns=["code"])

    code_col = next((c for c in ["code", "ticker", "종목코드"] if c in df.columns), None)
    if code_col is None:
        return pd.DataFrame(columns=["code"])

    z = df.copy()
    z = z.rename(columns={code_col: "code"})
    z["code"] = z["code"].map(_norm_code6)

    rename_map = {
        "revenue_growth": ["revenue_growth", "sales_growth", "매출성장률"],
        "op_growth": ["op_growth", "operating_profit_growth", "영업이익성장률"],
        "np_growth": ["np_growth", "net_income_growth", "순이익성장률"],
        "ROE": ["ROE", "roe"],
        "ROA": ["ROA", "roa"],
        "OPM": ["OPM", "operating_margin", "영업이익률"],
        "NPM": ["NPM", "net_margin", "순이익률"],
        "debt_ratio": ["debt_ratio", "부채비율"],
        "current_ratio": ["current_ratio", "유동비율"],
        "interest_coverage": ["interest_coverage", "이자보상배율"],
        "PSR": ["PSR", "psr"],
        "EV_EBITDA": ["EV_EBITDA", "EV/EBITDA", "ev_ebitda"],
    }

    out = pd.DataFrame({"code": z["code"]})
    for dst, aliases in rename_map.items():
        c = next((a for a in aliases if a in z.columns), None)
        if c is not None:
            out[dst] = pd.to_numeric(z[c], errors="coerce")

    out = out.drop_duplicates("code")
    return out


def _merge_external_fundamental(today: pd.DataFrame, as_of_ymd: str) -> tuple[pd.DataFrame, dict]:
    out = today.copy()
    info = {
        "as_of_ymd": str(as_of_ymd),
        "pykrx_rows": 0,
        "dart_rows": 0,
        "joined_cols": [],
    }

    pyf = _load_pykrx_fundamental_snapshot(as_of_ymd)
    if not pyf.empty and "code" in pyf.columns:
        info["pykrx_rows"] = int(len(pyf))
        out = out.merge(pyf, on="code", how="left")
        info["joined_cols"].extend([c for c in pyf.columns if c != "code"])

    df_dart = _load_dart_fundamental_snapshot(as_of_ymd)
    if not df_dart.empty and "code" in df_dart.columns:
        info["dart_rows"] = int(len(df_dart))
        overlap = [c for c in df_dart.columns if c != "code" and c in out.columns]
        if overlap:
            df_dart = df_dart.rename(columns={c: f"{c}_dart" for c in overlap})
        out = out.merge(df_dart, on="code", how="left")
        info["joined_cols"].extend([c for c in df_dart.columns if c != "code"])

    info["joined_cols"] = sorted(set(info["joined_cols"]))
    return out, info



def _maybe_refresh_krx_watchlist(as_of_ymd: str) -> None:
    global _KRX_WATCH_REFRESH_INIT
    if _KRX_WATCH_REFRESH_INIT:
        return
    _KRX_WATCH_REFRESH_INIT = True

    if str(os.environ.get("FUND_KRX_WATCH_REFRESH", "1")).strip() == "0":
        return

    tool = BASE_DIR / "tools" / "build_krx_watchlist_snapshot.py"
    if not tool.exists():
        print(f"[WARN] KRX watch refresh skipped: tool missing ({tool})")
        return

    cmd = [
        sys.executable,
        str(tool),
        "--as-of",
        str(as_of_ymd),
        "--caution-lookback-days",
        str(os.environ.get("WATCH_CAUTION_LOOKBACK_DAYS", "30")),
        "--search-lookback-days",
        str(os.environ.get("WATCH_SEARCH_LOOKBACK_DAYS", "365")),
    ]

    try:
        print("[WATCH] KRX watch refresh requested")
        cp = subprocess.run(cmd, env=os.environ.copy(), text=True, capture_output=True, timeout=900)
        if cp.returncode != 0:
            stderr_tail = "\n".join((cp.stderr or "").splitlines()[-6:])
            print(f"[WARN] KRX watch refresh failed: rc={cp.returncode}")
            if stderr_tail:
                print(stderr_tail)
        else:
            out_tail = "\n".join((cp.stdout or "").splitlines()[-6:])
            if out_tail:
                print(out_tail)
    except Exception as e:
        print(f"[WARN] KRX watch refresh exception: {type(e).__name__}: {e}")

def _load_krx_watchlist_snapshot(as_of_ymd: str) -> pd.DataFrame:
    _maybe_refresh_krx_watchlist(as_of_ymd)

    candidates = [
        CACHE_DIR / f"krx_watchlist_{as_of_ymd}.csv",
        CACHE_DIR / "krx_watchlist_latest.csv",
        LOG_DIR / "krx_watchlist_latest.csv",
    ]
    src = _first_existing(candidates)
    if src is None:
        return pd.DataFrame(columns=["code", "krx_admin", "krx_warning", "krx_risk", "krx_caution", "krx_watch_note"])

    df = _read_csv_if_exists(src)
    if df.empty:
        return pd.DataFrame(columns=["code", "krx_admin", "krx_warning", "krx_risk", "krx_caution", "krx_watch_note"])

    code_col = next((c for c in ["code", "ticker", "종목코드"] if c in df.columns), None)
    if code_col is None:
        return pd.DataFrame(columns=["code", "krx_admin", "krx_warning", "krx_risk", "krx_caution", "krx_watch_note"])

    z = df.copy()
    z["code"] = z[code_col].map(_norm_code6)

    def _bool_col(aliases: list[str]) -> pd.Series:
        c = next((a for a in aliases if a in z.columns), None)
        if c is None:
            return pd.Series(np.zeros(len(z), dtype=bool), index=z.index)
        s = z[c]
        if s.dtype == bool:
            return s.fillna(False)
        ss = s.astype(str).str.strip().str.upper()
        return ss.isin(["1", "TRUE", "Y", "YES", "관리", "투자경고", "투자위험", "주의"])

    z["krx_admin"] = _bool_col(["krx_admin", "is_admin", "administrative"])
    z["krx_warning"] = _bool_col(["krx_warning", "is_warning", "investment_warning"])
    z["krx_risk"] = _bool_col(["krx_risk", "is_risk", "investment_risk"])
    z["krx_caution"] = _bool_col(["krx_caution", "is_caution", "investment_caution"])

    if "category" in z.columns:
        cat = z["category"].astype(str)
        z["krx_admin"] = z["krx_admin"] | cat.str.contains("관리|ADMIN", case=False, regex=True)
        z["krx_warning"] = z["krx_warning"] | cat.str.contains("경고|WARNING", case=False, regex=True)
        z["krx_risk"] = z["krx_risk"] | cat.str.contains("위험|RISK", case=False, regex=True)
        z["krx_caution"] = z["krx_caution"] | cat.str.contains("주의|CAUTION", case=False, regex=True)

    note_col = next((c for c in ["note", "reason", "사유"] if c in z.columns), None)
    if note_col is not None:
        z["krx_watch_note"] = z[note_col].astype(str)
    else:
        z["krx_watch_note"] = ""

    keep = ["code", "krx_admin", "krx_warning", "krx_risk", "krx_caution", "krx_watch_note"]
    return z[keep].drop_duplicates("code")


def _attach_krx_watch_flags(today: pd.DataFrame, as_of_ymd: str) -> tuple[pd.DataFrame, dict]:
    out = today.copy()
    w = _load_krx_watchlist_snapshot(as_of_ymd)
    info = {
        "watch_rows": int(len(w)),
        "admin_flags": 0,
        "warning_flags": 0,
        "risk_flags": 0,
        "caution_flags": 0,
    }

    if w.empty:
        out["krx_admin"] = False
        out["krx_warning"] = False
        out["krx_risk"] = False
        out["krx_caution"] = False
        out["krx_watch_note"] = ""
        return out, info

    out = out.merge(w, on="code", how="left")
    for c in ["krx_admin", "krx_warning", "krx_risk", "krx_caution"]:
        if c not in out.columns:
            out[c] = False
        out[c] = out[c].fillna(False).astype(bool)
    if "krx_watch_note" not in out.columns:
        out["krx_watch_note"] = ""
    out["krx_watch_note"] = out["krx_watch_note"].fillna("").astype(str)

    info["admin_flags"] = int(out["krx_admin"].sum())
    info["warning_flags"] = int(out["krx_warning"].sum())
    info["risk_flags"] = int(out["krx_risk"].sum())
    info["caution_flags"] = int(out["krx_caution"].sum())
    return out, info


def _apply_krx_watch_hard_filter(candidates: pd.DataFrame, p: dict) -> tuple[pd.DataFrame, dict]:
    out = candidates.copy()
    if out.empty:
        return out, {"removed_admin": 0, "removed_warning": 0, "removed_risk": 0, "before": 0, "after": 0}

    ex_admin = float((p.get("exclude_administrative") if p.get("exclude_administrative") is not None else 1.0)) >= 0.5
    ex_warn = float((p.get("exclude_investment_warning") if p.get("exclude_investment_warning") is not None else 1.0)) >= 0.5
    ex_risk = float((p.get("exclude_investment_risk") if p.get("exclude_investment_risk") is not None else 1.0)) >= 0.5

    for c in ["krx_admin", "krx_warning", "krx_risk"]:
        if c not in out.columns:
            out[c] = False
        out[c] = out[c].fillna(False).astype(bool)

    removed_admin = int(out["krx_admin"].sum()) if ex_admin else 0
    removed_warning = int(out["krx_warning"].sum()) if ex_warn else 0
    removed_risk = int(out["krx_risk"].sum()) if ex_risk else 0

    mask = pd.Series(np.zeros(len(out), dtype=bool), index=out.index)
    if ex_admin:
        mask = mask | out["krx_admin"]
    if ex_warn:
        mask = mask | out["krx_warning"]
    if ex_risk:
        mask = mask | out["krx_risk"]

    before = int(len(out))
    out = out[~mask].copy()
    after = int(len(out))

    info = {
        "before": before,
        "after": after,
        "removed_admin": removed_admin,
        "removed_warning": removed_warning,
        "removed_risk": removed_risk,
    }
    return out, info


def _apply_krx_watch_soft_penalty(candidates: pd.DataFrame, p: dict) -> tuple[pd.DataFrame, dict]:
    out = candidates.copy()
    if out.empty or "final_score" not in out.columns:
        return out, {"penalty": 0.0, "penalized_rows": 0}

    if "krx_caution" not in out.columns:
        out["krx_caution"] = False

    pen = float((p.get("watch_penalty_caution") if p.get("watch_penalty_caution") is not None else 0.10))
    pen = min(max(pen, 0.0), 0.9)
    m = out["krx_caution"].fillna(False).astype(bool)
    if int(m.sum()) > 0 and pen > 0:
        out.loc[m, "final_score"] = pd.to_numeric(out.loc[m, "final_score"], errors="coerce") * (1.0 - pen)
    return out, {"penalty": float(pen), "penalized_rows": int(m.sum())}




def _risk_unit_linear(s: pd.Series, low: float, high: float, invert: bool = False) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if high <= low:
        out = pd.Series(np.zeros(len(x), dtype=float), index=x.index)
        out[x.isna()] = np.nan
        return out
    if invert:
        r = (high - x) / (high - low + 1e-9)
    else:
        r = (x - low) / (high - low + 1e-9)
    return r.clip(0.0, 1.0)


def _junk_risk_grade(score_100: float) -> str:
    try:
        s = float(score_100)
    except Exception:
        s = 0.0
    if s >= 85:
        return "EXTREME"
    if s >= 70:
        return "HIGH"
    if s >= 50:
        return "WARN"
    if s >= 30:
        return "MID"
    return "LOW"


def _apply_junk_risk_overlay(candidates: pd.DataFrame, p: dict) -> tuple[pd.DataFrame, dict]:
    out = candidates.copy()
    info = {
        "enabled": False,
        "mode": "disabled",
        "fallback_applied": False,
        "fallback_reason": "",
        "hard_exclude": False,
        "hard_threshold": None,
        "penalty_max": 0.0,
        "before": int(len(out)),
        "after": int(len(out)),
        "removed": 0,
        "penalized_rows": 0,
        "high_risk_rows": 0,
        "extreme_risk_rows": 0,
        "korean_special_rows": 0,
        "mean_score": 0.0,
    }

    def _apply_junk_fallback(reason: str, mode: str) -> tuple[pd.DataFrame, dict]:
        fb = out.copy()
        fb["junk_risk_score"] = 0.0
        fb["junk_risk_grade"] = "FALLBACK"
        fb["junk_flags"] = ""
        fb["junk_penalty"] = 0.0
        info["mode"] = mode
        info["fallback_applied"] = True
        info["fallback_reason"] = reason
        info["after"] = int(len(fb))
        return fb, info

    if out.empty or ("final_score" not in out.columns):
        return _apply_junk_fallback("missing_final_score", "fallback_missing_final_score")

    enabled = float((p.get("junk_risk_enable") if p.get("junk_risk_enable") is not None else 1.0)) >= 0.5
    info["enabled"] = bool(enabled)

    if "krx_caution" not in out.columns:
        out["krx_caution"] = False

    if not enabled:
        out["junk_risk_score"] = 0.0
        out["junk_risk_grade"] = "OFF"
        out["junk_flags"] = ""
        out["junk_penalty"] = 0.0
        return out, info

    info["mode"] = "normal"

    def _aligned_risk(values, *, low: float, high: float, invert: bool = False) -> pd.Series:
        ser = _risk_unit_linear(values, low=low, high=high, invert=invert)
        if not isinstance(ser, pd.Series):
            ser = pd.Series(ser, index=out.index)
        if len(ser) != len(out) or not ser.index.equals(out.index):
            ser = ser.reindex(out.index)
        return pd.to_numeric(ser, errors="coerce").fillna(0.0).clip(0.0, 1.0)

    try:
        # Liquidity risk (lower traded value => higher risk)
        liq_r = _aligned_risk(out.get("value", np.nan), low=10_000_000_000.0, high=80_000_000_000.0, invert=True)

        # Pump/operation-like microstructure pattern
        v_r = _aligned_risk(out.get("v_accel", np.nan), low=1.8, high=4.2)
        stretch_r = _aligned_risk(out.get("stretch", np.nan), low=1.08, high=1.30)
        ret_r = _aligned_risk(out.get("ret1_pct", np.nan), low=8.0, high=30.0)
        rsi_r = _aligned_risk(out.get("rsi14", np.nan), low=65.0, high=85.0)
        pump_r = pd.concat([v_r, stretch_r, ret_r, rsi_r], axis=1).mean(axis=1, skipna=True).fillna(0.0).clip(0.0, 1.0)

        # Young listing risk
        young_r = _aligned_risk(out.get("listing_days", np.nan), low=120.0, high=720.0, invert=True)

        # Fundamental fragility risk (if columns exist)
        roe_r = _aligned_risk(out.get("ROE", np.nan), low=5.0, high=25.0, invert=True)
        opm_r = _aligned_risk(out.get("OPM", np.nan), low=3.0, high=18.0, invert=True)
        debt_r = _aligned_risk(out.get("debt_ratio", np.nan), low=180.0, high=1200.0, invert=False)
        fund_r = pd.concat([roe_r, opm_r, debt_r], axis=1).mean(axis=1, skipna=True).fillna(0.0).clip(0.0, 1.0)
    except Exception as exc:
        return _apply_junk_fallback(type(exc).__name__, "fallback_exception")

    junk_r = (liq_r.fillna(0.0) * 0.35 + pump_r * 0.40 + young_r * 0.10 + fund_r * 0.15).clip(0.0, 1.0)
    score100 = (junk_r * 100.0).round(2)

    out["junk_risk_score"] = score100
    out["junk_risk_grade"] = out["junk_risk_score"].apply(_junk_risk_grade)

    # Human-readable flags
    flags = []
    korean_special_rows = 0
    for idx in out.index:
        f = []
        if float(liq_r.loc[idx]) >= 0.55:
            f.append("low_liquidity")
        if float(pump_r.loc[idx]) >= 0.55:
            f.append("pump_pattern")
        if float(young_r.loc[idx]) >= 0.60:
            f.append("young_listing")
        if float(fund_r.loc[idx]) >= 0.55:
            f.append("weak_fundamental")
        if bool(out.loc[idx, "krx_caution"]):
            f.append("krx_caution")
        market = str(out.loc[idx, "market"] if "market" in out.columns else "" or "").strip().upper()
        market_cap = float(pd.to_numeric(pd.Series([out.loc[idx, "market_cap"] if "market_cap" in out.columns else np.nan]), errors="coerce").fillna(0.0).iloc[0])
        ret1 = float(pd.to_numeric(pd.Series([out.loc[idx, "ret1_pct"] if "ret1_pct" in out.columns else np.nan]), errors="coerce").fillna(0.0).iloc[0])
        stretch = float(pd.to_numeric(pd.Series([out.loc[idx, "stretch"] if "stretch" in out.columns else np.nan]), errors="coerce").fillna(0.0).iloc[0])
        watch_note = str(out.loc[idx, "krx_watch_note"] if "krx_watch_note" in out.columns else "" or "")
        special = False
        if ret1 >= 29.0 or stretch >= 1.24:
            f.append("limit_up_like")
            special = True
        if ("과열" in watch_note) or ("怨쇱뿴" in watch_note):
            f.append("krx_overheat_note")
            special = True
        if market == "KOSDAQ" and 0.0 < market_cap <= 300_000_000_000.0:
            f.append("kosdaq_smallcap_spec")
            special = True
        if special:
            korean_special_rows += 1
        flags.append("|".join(f))
    out["junk_flags"] = flags
    info["korean_special_rows"] = int(korean_special_rows)

    info["mean_score"] = float(pd.to_numeric(out["junk_risk_score"], errors="coerce").fillna(0.0).mean())
    info["high_risk_rows"] = int((pd.to_numeric(out["junk_risk_score"], errors="coerce") >= 70.0).sum())
    info["extreme_risk_rows"] = int((pd.to_numeric(out["junk_risk_score"], errors="coerce") >= 85.0).sum())

    # Soft penalty on final_score
    pen_max = float((p.get("junk_penalty_max") if p.get("junk_penalty_max") is not None else 0.18))
    pen_max = min(max(pen_max, 0.0), 0.35)
    info["penalty_max"] = float(pen_max)

    pen_ser = (pd.to_numeric(out["junk_risk_score"], errors="coerce").fillna(0.0) / 100.0) * pen_max
    pen_ser = pen_ser.clip(0.0, pen_max)
    out["junk_penalty"] = pen_ser.round(4)
    m_pen = pen_ser > 0
    info["penalized_rows"] = int(m_pen.sum())
    if int(m_pen.sum()) > 0:
        out.loc[m_pen, "final_score"] = pd.to_numeric(out.loc[m_pen, "final_score"], errors="coerce") * (1.0 - pen_ser.loc[m_pen])

    # Optional hard exclude for extreme junk/manipulation risk
    hard_ex = float((p.get("junk_hard_exclude") if p.get("junk_hard_exclude") is not None else 1.0)) >= 0.5
    hard_th = float((p.get("junk_hard_threshold") if p.get("junk_hard_threshold") is not None else 88.0))
    hard_th = min(max(hard_th, 60.0), 99.0)
    info["hard_exclude"] = bool(hard_ex)
    info["hard_threshold"] = float(hard_th)

    if hard_ex:
        hard_threshold_ser = pd.Series(float(hard_th), index=out.index, dtype=float)
        if "market_cap" in out.columns:
            market_cap_ser = pd.to_numeric(out["market_cap"], errors="coerce").fillna(0.0)
            hard_threshold_ser.loc[market_cap_ser >= 1_000_000_000_000.0] = np.maximum(
                hard_threshold_ser.loc[market_cap_ser >= 1_000_000_000_000.0],
                95.0,
            )
        m_hard = pd.to_numeric(out["junk_risk_score"], errors="coerce").fillna(0.0) >= hard_threshold_ser
        before = int(len(out))
        out = out.loc[~m_hard].copy()
        info["before"] = before
        info["after"] = int(len(out))
        info["removed"] = int(before - len(out))

    return out, info


def _get_company_analyzer(enabled: bool = True):
    global _COMPANY_ANALYZER_OBJ, _COMPANY_ANALYZER_INIT
    if not enabled:
        return None
    if _COMPANY_ANALYZER_INIT:
        return _COMPANY_ANALYZER_OBJ

    _COMPANY_ANALYZER_INIT = True
    candidate_paths = []
    env_path = str(os.environ.get("COMPANY_ANALYZER_FILE", "")).strip()
    if env_path:
        candidate_paths.append(Path(env_path))
    candidate_paths.append(BASE_DIR / "company_analyzer.py")
    candidate_paths.append(Path(r"C:\Users\jjtop\OneDrive\Desktop\claude code\기업분석\company_analyzer.py"))

    for p in candidate_paths:
        try:
            if not p.exists():
                continue
            spec = importlib.util.spec_from_file_location("ext_company_analyzer", str(p))
            if spec is None or spec.loader is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if hasattr(mod, "CompanyAnalyzer"):
                _COMPANY_ANALYZER_OBJ = mod.CompanyAnalyzer()
                print(f"[FUND] company_analyzer loaded: {p}")
                return _COMPANY_ANALYZER_OBJ
        except Exception as e:
            print(f"[WARN] company_analyzer load failed ({p}): {type(e).__name__}: {e}")
            continue
    return None
def _resolve_col_alias(df: pd.DataFrame, aliases: list[str]) -> str | None:
    cmap = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        k = str(a).strip().lower()
        if k in cmap:
            return cmap[k]
    return None


def _safe_rank01(s: pd.Series, higher_is_better: bool = True) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if int(x.notna().sum()) <= 1:
        out = pd.Series(np.full(len(x), 0.5), index=s.index, dtype=float)
        out[x.isna()] = np.nan
        return out
    r = x.rank(pct=True, method="average")
    if not higher_is_better:
        r = 1.0 - r
    return r.clip(0.0, 1.0)


def _component_rank(df: pd.DataFrame, specs: list[tuple[str, bool]]) -> tuple[pd.Series, list[str]]:
    mats = []
    used = []
    for col, hib in specs:
        if col not in df.columns:
            continue
        r = _safe_rank01(df[col], higher_is_better=hib)
        if int(r.notna().sum()) <= 0:
            continue
        mats.append(r.rename(col))
        used.append(col)

    if not mats:
        return pd.Series(np.full(len(df), 0.5), index=df.index, dtype=float), used

    m = pd.concat(mats, axis=1)
    comp = m.mean(axis=1, skipna=True)
    med = float(comp.median()) if int(comp.notna().sum()) > 0 else 0.5
    comp = comp.fillna(med).fillna(0.5).clip(0.0, 1.0)
    return comp, used


def _fundamental_grade(score_100: float) -> str:
    try:
        s = float(score_100)
    except Exception:
        s = 50.0
    if s >= 90:
        return "S"
    if s >= 80:
        return "A"
    if s >= 65:
        return "B"
    if s >= 50:
        return "C"
    if s >= 35:
        return "D"
    return "F"


def _apply_fundamental_overlay(candidates: pd.DataFrame, p: dict, market_regime: str) -> pd.DataFrame:
    out = candidates.copy()

    def m(aliases: list[str], higher_is_better: bool) -> tuple[str, bool] | None:
        c = _resolve_col_alias(out, aliases)
        return (c, higher_is_better) if c else None

    value_specs = [
        m(["PER"], False),
        m(["PBR"], False),
        m(["PSR"], False),
        m(["EV_EBITDA", "EV/EBITDA"], False),
    ]
    quality_specs = [
        m(["ROE"], True),
        m(["ROA"], True),
        m(["OPM", "operating_margin"], True),
        m(["NPM", "net_margin"], True),
    ]
    growth_specs = [
        m(["revenue_growth", "sales_growth"], True),
        m(["op_growth", "operating_profit_growth"], True),
        m(["np_growth", "net_income_growth", "earnings_growth"], True),
    ]
    stability_specs = [
        m(["debt_ratio"], False),
        m(["current_ratio"], True),
        m(["interest_coverage"], True),
    ]
    supply_specs = [
        m(["foreign_net_20d", "foreign_net"], True),
        m(["institution_net_20d", "institution_net"], True),
    ]

    value_specs = [x for x in value_specs if x is not None]
    quality_specs = [x for x in quality_specs if x is not None]
    growth_specs = [x for x in growth_specs if x is not None]
    stability_specs = [x for x in stability_specs if x is not None]
    supply_specs = [x for x in supply_specs if x is not None]

    value_r, value_used = _component_rank(out, value_specs)
    quality_r, quality_used = _component_rank(out, quality_specs)
    growth_r, growth_used = _component_rank(out, growth_specs)
    stability_r, stability_used = _component_rank(out, stability_specs)
    supply_r, supply_used = _component_rank(out, supply_specs)

    regime_weights = {
        "BULL": {"value": 0.10, "quality": 0.15, "growth": 0.40, "stability": 0.10, "supply": 0.25},
        "BEAR": {"value": 0.15, "quality": 0.20, "growth": 0.05, "stability": 0.50, "supply": 0.10},
        "CORRECTION": {"value": 0.25, "quality": 0.25, "growth": 0.15, "stability": 0.25, "supply": 0.10},
        "SIDEWAYS": {"value": 0.20, "quality": 0.20, "growth": 0.10, "stability": 0.40, "supply": 0.10},
    }
    default_regime = str(market_regime or "SIDEWAYS").upper()
    if default_regime not in regime_weights:
        default_regime = "SIDEWAYS"
    if "market_regime" in out.columns:
        regime_s = out["market_regime"].fillna(default_regime).astype(str).str.upper()
        regime_s = regime_s.where(regime_s.isin(regime_weights.keys()), default_regime)
    else:
        regime_s = pd.Series(default_regime, index=out.index)

    fund_rank = pd.Series(0.0, index=out.index, dtype=float)
    components = {
        "value": value_r,
        "quality": quality_r,
        "growth": growth_r,
        "stability": stability_r,
        "supply": supply_r,
    }
    for comp_name, comp_rank in components.items():
        weight_s = regime_s.map({k: v[comp_name] for k, v in regime_weights.items()}).astype(float)
        fund_rank = fund_rank + comp_rank * weight_s
    fund_rank = fund_rank.clip(0.0, 1.0)

    out["fundamental_score"] = (fund_rank * 100.0).round(2)
    out["fundamental_grade"] = out["fundamental_score"].apply(_fundamental_grade)
    out["fundamental_metric_count"] = int(
        len(value_used) + len(quality_used) + len(growth_used) + len(stability_used) + len(supply_used)
    )

    _ca_enable_raw = p.get("company_analyzer_enable")
    use_ca = float(_ca_enable_raw if _ca_enable_raw is not None else 1.0) >= 0.5
    analyzer = _get_company_analyzer(enabled=use_ca)
    if analyzer is not None and (len(out) > 0):
        blend = min(max(float((p.get("company_analyzer_blend") if p.get("company_analyzer_blend") is not None else 0.70)), 0.0), 1.0)
        fd_alias = {
            "PER": ["PER"],
            "PBR": ["PBR"],
            "PSR": ["PSR"],
            "EV_EBITDA": ["EV_EBITDA", "EV/EBITDA"],
            "ROE": ["ROE"],
            "ROA": ["ROA"],
            "OPM": ["OPM", "operating_margin"],
            "NPM": ["NPM", "net_margin"],
            "revenue_growth": ["revenue_growth", "sales_growth"],
            "op_growth": ["op_growth", "operating_profit_growth"],
            "np_growth": ["np_growth", "net_income_growth", "earnings_growth"],
            "debt_ratio": ["debt_ratio"],
            "current_ratio": ["current_ratio"],
            "interest_coverage": ["interest_coverage"],
            "foreign_net_20d": ["foreign_net_20d", "foreign_net"],
            "institution_net_20d": ["institution_net_20d", "institution_net"],
        }
        fd_cols = {}
        for k, aliases in fd_alias.items():
            c = _resolve_col_alias(out, aliases)
            if c:
                fd_cols[k] = c

        ca_scores = []
        for _, rr in out.iterrows():
            fd = {}
            for k, c in fd_cols.items():
                v = pd.to_numeric(pd.Series([rr.get(c)]), errors="coerce").iloc[0]
                if pd.notna(v):
                    fd[k] = float(v)
            try:
                sc = analyzer.analyze(
                    code=str(rr.get("code", "")),
                    name=str(rr.get("name", rr.get("code", ""))),
                    fundamental_data=fd,
                    price_df=None,
                    regime=str(regime_s.loc[rr.name]),
                )
                ca_scores.append(float(getattr(sc, "total_score", np.nan)))
            except Exception:
                ca_scores.append(np.nan)

        ca_ser = pd.Series(ca_scores, index=out.index, dtype=float)
        base_ser = pd.to_numeric(out["fundamental_score"], errors="coerce").fillna(50.0)
        ca_ser = ca_ser.where(ca_ser.notna(), base_ser)
        out["fundamental_score"] = (base_ser * (1.0 - blend) + ca_ser * blend).round(2)
        out["fundamental_grade"] = out["fundamental_score"].apply(_fundamental_grade)

    w_tech = float(p.get("w_tech_score", DEFAULT_PARAMS["w_tech_score"]))
    w_fund = float(p.get("w_fundamental_score", DEFAULT_PARAMS["w_fundamental_score"]))
    ws = w_tech + w_fund
    if ws <= 0:
        w_tech = DEFAULT_PARAMS["w_tech_score"]
        w_fund = DEFAULT_PARAMS["w_fundamental_score"]
        ws = w_tech + w_fund
    w_tech = w_tech / ws
    w_fund = w_fund / ws

    tech_score = pd.to_numeric(out.get("score", np.nan), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    fund_score = (pd.to_numeric(out.get("fundamental_score", np.nan), errors="coerce").fillna(50.0) / 100.0).clip(0.0, 1.0)
    out["final_score"] = (tech_score * w_tech + fund_score * w_fund).clip(0.0, 1.0)
    return out
def _build_sector_code_map(restrict_allowed: bool = True) -> pd.DataFrame:
    if (not SECTOR_SSOT_PATH.exists()) or (not SECTOR_MAP_PATH.exists()):
        return pd.DataFrame(columns=["code", "sector_code"])
    try:
        ssot = pd.read_csv(SECTOR_SSOT_PATH, dtype={"code": str, "krx_sector": str}, encoding="utf-8-sig")
        mp = pd.read_csv(SECTOR_MAP_PATH, dtype={"krx_sector": str, "sector_code": str}, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=["code", "sector_code"])

    ssot["code"] = ssot["code"].astype(str).str.zfill(6)
    ssot["krx_sector"] = ssot["krx_sector"].astype(str).str.strip()
    mp["krx_sector"] = mp["krx_sector"].astype(str).str.strip()
    mp["sector_code"] = mp["sector_code"].astype(str).str.strip()

    out = ssot.merge(mp[["krx_sector", "sector_code"]], on="krx_sector", how="left")
    out["sector_code"] = out["sector_code"].astype(str).str.strip()
    if restrict_allowed:
        out = out[out["sector_code"].isin(ALLOWED_SECTOR_CODES)].copy()
    else:
        out = out[out["sector_code"].ne("") & out["sector_code"].ne("nan")].copy()
    return out[["code", "sector_code"]].drop_duplicates("code")


def _compute_rally_breadth_stats(ret1_pct: pd.Series) -> tuple[float, float, float]:
    """Shared cross-section return-breadth stats for broad-rally signals.

    Returns (max_ret, pct_ge_5, pct_ge_10); all 0.0 if input is missing/empty.
    Callers each apply their own broad_rally threshold combination on top of
    these shared stats -- 2026-07-24: _apply_sector_prefilter_union() and
    _apply_rally_safety_override() previously computed this independently
    with copy-pasted (and silently divergeable) code; thresholds are kept
    call-site-specific on purpose (different stakes: display-only fallback
    pool sizing vs. real entry-gate relaxation), only the stat computation
    itself is now shared.
    """
    ret = pd.to_numeric(ret1_pct, errors="coerce").dropna()
    if ret.empty:
        return 0.0, 0.0, 0.0
    return float(ret.max()), float((ret >= 5.0).mean()), float((ret >= 10.0).mean())


def _apply_sector_prefilter_union(today: pd.DataFrame, candidates: pd.DataFrame, p: dict, chosen_level: str = "L0") -> tuple[pd.DataFrame, dict]:
    enabled = str(os.environ.get("SECTOR_PREFILTER_ENABLE", "1")).strip() != "0"

    try:
        level_num = int(str(chosen_level).upper().replace("L", "")[:1])
    except Exception:
        level_num = 0

    max_ret, pct_ge_5, pct_ge_10 = _compute_rally_breadth_stats(today.get("ret1_pct"))
    broad_rally = (max_ret >= 20.0 and pct_ge_5 >= 0.12) or (pct_ge_10 >= 0.10)

    env_min = str(os.environ.get("SECTOR_PREFILTER_MIN_TOTAL", "")).strip()
    if env_min:
        min_total = max(int(float(env_min)), 1)
        min_source = "env"
    else:
        if level_num >= 5 and broad_rally:
            min_total = 16
        elif level_num >= 5:
            min_total = 12
        elif broad_rally:
            min_total = 14
        else:
            min_total = 10
        min_source = "auto"

    env_top = str(os.environ.get("SECTOR_PREFILTER_TOP_PER_SECTOR", "")).strip()
    if env_top:
        top_per_sector = max(int(float(env_top)), 1)
    else:
        top_per_sector = 3 if (broad_rally or level_num >= 5) else 2

    env_max_add = str(os.environ.get("SECTOR_PREFILTER_MAX_ADD", "")).strip()
    if env_max_add:
        max_add = max(int(float(env_max_add)), 1)
    else:
        max_add = max(min_total * 2, 20)

    info = {
        "enabled": bool(enabled),
        "chosen_level": str(chosen_level),
        "broad_rally": bool(broad_rally),
        "max_ret_pct": float(max_ret),
        "pct_ge_10": float(pct_ge_10),
        "min_total": int(min_total),
        "min_source": str(min_source),
        "top_per_sector": int(top_per_sector),
        "max_add": int(max_add),
        "before": int(len(candidates)),
        "added": 0,
        "after": int(len(candidates)),
        "reason": "",
    }

    if not enabled:
        info["reason"] = "disabled"
        return candidates, info
    if len(candidates) >= min_total:
        info["reason"] = "enough_candidates"
        return candidates, info

    sc_map = _build_sector_code_map()
    if sc_map.empty:
        info["reason"] = "sector_map_missing"
        return candidates, info

    pool = today.copy()
    pool["code"] = pool["code"].astype(str).str.zfill(6)
    # _compute_factors() now attaches an unrestricted sector_code for rule_e's sector index.
    # Drop it before merging so this restricted (ALLOWED_SECTOR_CODES) map stays authoritative
    # here and the merge does not split into sector_code_x / sector_code_y.
    pool = pool.drop(columns=["sector_code"], errors="ignore")
    pool = pool.merge(sc_map, on="code", how="left")
    pool["sector_code"] = pool["sector_code"].astype(str).str.strip()
    pool = pool[pool["sector_code"].isin(ALLOWED_SECTOR_CODES)].copy()

    base_codes = set(candidates["code"].astype(str).str.zfill(6).tolist()) if not candidates.empty else set()
    if base_codes:
        pool = pool[~pool["code"].isin(base_codes)].copy()
    if pool.empty:
        info["reason"] = "no_pool_after_dedupe"
        return candidates, info

    if broad_rally:
        value_soft = max(float(p.get("value_min", 1e9)) * 0.35, 3_000_000_000.0)
        atr_soft = min(max(float(p.get("atr_max", 0.12)) * 1.6, 0.18), 0.33)
        rsi_soft = min(max(float(p.get("rsi_max", 70.0)) + 8.0, 70.0), 82.0)
    else:
        value_soft = max(float(p.get("value_min", 1e9)) * 0.5, 5_000_000_000.0)
        atr_soft = min(max(float(p.get("atr_max", 0.12)) * 1.3, 0.16), 0.30)
        rsi_soft = min(max(float(p.get("rsi_max", 70.0)) + 5.0, 70.0), 80.0)
    listing_soft = max(float(p.get("min_listing_days", 126.0)), 126.0)

    gate = (
        (pd.to_numeric(pool["value"], errors="coerce") >= value_soft)
        & (pd.to_numeric(pool["atr14_pct"], errors="coerce") <= atr_soft)
        & (pd.to_numeric(pool["rsi14"], errors="coerce") <= rsi_soft)
        & (pd.to_numeric(pool["listing_days"], errors="coerce") >= listing_soft)
    )
    # rule_e is a market-level "do not trade today" gate, so the union fallback must respect
    # it too. Without this the fallback re-injects names on exactly the days rule_e closed.
    # candidate_origin is kept in keep_cols (line 2211) so the observe-only mask can use it,
    # but end-to-end observe-only enforcement still needs runtime verification.
    for _col, _key in (
        ("mkt_ret20", "mkt_ret20_min"),
        ("mkt_ret60", "mkt_ret60_min"),
        ("sector_rs", "sector_rs_min"),
    ):
        _thr = _rule_e_threshold(p, _key)
        if _thr is not None and _col in pool.columns:
            gate = gate & (pd.to_numeric(pool[_col], errors="coerce") > _thr)
    pool = pool[gate].copy()
    if pool.empty:
        info["reason"] = "no_pool_after_soft_gate"
        return candidates, info

    pool["_r_rs"] = pd.to_numeric(pool["rs"], errors="coerce").rank(pct=True)
    pool["_r_sl"] = pd.to_numeric(pool["rs_slope"], errors="coerce").rank(pct=True)
    pool["_r_va"] = pd.to_numeric(pool["v_accel"], errors="coerce").rank(pct=True)
    pool["_r_liq"] = pd.to_numeric(pool["value"], errors="coerce").rank(pct=True)
    pool["_seed"] = (0.45 * pool["_r_rs"]) + (0.20 * pool["_r_sl"]) + (0.25 * pool["_r_va"]) + (0.10 * pool["_r_liq"])

    need = max(min_total - len(candidates), 0)
    take = min(max_add, max(need, 0))
    if take <= 0:
        info["reason"] = "no_need"
        return candidates, info

    primary = (
        pool.sort_values(["sector_code", "_seed"], ascending=[True, False])
            .groupby("sector_code", as_index=False, group_keys=False)
            .head(top_per_sector)
            .copy()
    )
    if primary.empty:
        info["reason"] = "seed_empty"
        return candidates, info

    selected = primary.sort_values("_seed", ascending=False).head(take).copy()
    if len(selected) < take:
        remain = pool[~pool["code"].isin(set(selected["code"].astype(str).tolist()))].copy()
        extra = remain.sort_values("_seed", ascending=False).head(take - len(selected)).copy()
        if not extra.empty:
            selected = pd.concat([selected, extra], ignore_index=True)

    selected = selected.drop(columns=["_r_rs", "_r_sl", "_r_va", "_r_liq", "_seed"], errors="ignore")
    selected["candidate_origin"] = "SECTOR_PREFILTER_UNION"
    selected["natural_pass"] = False
    selected["observe_only"] = True
    selected["observe_only_reason"] = "sector_prefilter_union_fallback"

    if candidates.empty:
        out = selected.copy()
    else:
        out = pd.concat([candidates, selected], ignore_index=True)
        out["code"] = out["code"].astype(str).str.zfill(6)
        out = out.drop_duplicates("code", keep="first")

    info["added"] = int(len(out) - len(candidates))
    info["after"] = int(len(out))
    info["reason"] = "applied" if info["added"] > 0 else "no_add"
    return out, info


def _apply_rally_safety_override(today: pd.DataFrame, p: dict, cand: pd.DataFrame, chosen_level: str) -> tuple[pd.DataFrame, dict, bool, dict]:
    """On broad rally days, prevent sample collapse at high relax levels.
    Applies only when chosen level is L5/L6 and candidate count is too small.
    """
    try:
        level_num = int(str(chosen_level).upper().replace("L", "")[:1])
    except Exception:
        level_num = -1

    info = {"applied": False, "reason": ""}
    if level_num < 5:
        info["reason"] = "level_lt_L5"
        return cand, p, False, info

    # broad rally signal from cross-section returns (ret1_pct is in percent scale)
    if pd.to_numeric(today.get("ret1_pct"), errors="coerce").dropna().empty:
        info["reason"] = "ret_missing"
        return cand, p, False, info

    max_ret, _pct_ge_5, pct_ge_10 = _compute_rally_breadth_stats(today.get("ret1_pct"))
    broad_rally = (max_ret >= 25.0) and (pct_ge_10 >= 0.08)
    info["broad_rally"] = broad_rally
    info["max_ret_pct"] = max_ret
    info["pct_ge_10"] = pct_ge_10

    min_floor = 5
    if (not broad_rally) or (len(cand) >= min_floor):
        info["reason"] = "no_override_needed"
        return cand, p, False, info

    p2 = dict(p)
    # Controlled relax only for rally over-constraining factors.
    p2["near_52w_high_gap_max"] = max(float(p2.get("near_52w_high_gap_max", 0.05)), 0.12)
    p2["v_accel_lim"] = max(float(p2.get("v_accel_lim", 1.2)) * 0.90, 1.05)
    p2["value_min"] = max(float(p2.get("value_min", 1e9)) * 0.70, 5_000_000_000.0)

    cand2 = _select_candidates(today, p2)
    if len(cand2) > len(cand):
        info["applied"] = True
        info["reason"] = "rally_floor_recover"
        info["before"] = int(len(cand))
        info["after"] = int(len(cand2))
        return cand2, p2, True, info

    info["reason"] = "override_no_gain"
    return cand, p, False, info

# [2026-09-10] 완화 방향 **선언**. B 수리.
#
#   왜 선언이 필요한가: 종전에는 방향이 각 수식 안에 숨어 있었다.
#     rs_lim  = max(rs_lim * 0.95, -0.10)   <- 음수가 되면 방향이 뒤집힌다
#   수식만 보면 "완화" 처럼 보이는데 실제로는 조이고 있었고, 6주간 안 보였다.
#   방향을 데이터로 꺼내면 **틀렸을 때 검사가 잡을 수 있다.**
#
#   "loosen" 은 그 파라미터가 **느슨해지는 방향**이다.
#     down = 값이 내려가야 더 많이 통과한다  (게이트가 `x > lim` 또는 `x >= lim`)
#     up   = 값이 올라가야 더 많이 통과한다  (게이트가 `x < max` 또는 `x <= max`)
RELAX_DIRECTION = {
    "rs_lim": "down",                  # rs > rs_lim
    "v_accel_lim": "down",             # v_accel > v_accel_lim
    "value_min": "down",               # value > value_min
    "min_listing_days": "down",        # listing_days >= min_listing_days
    "vol_close_corr_min": "down",      # vol_close_corr20 >= vol_close_corr_min
    "atr_max": "up",                   # atr14_pct < atr_max
    "stretch_max": "up",               # stretch < stretch_max
    "rsi_max": "up",                   # rsi14 < rsi_max
    "near_52w_high_gap_max": "up",     # high_52w_gap <= near_52w_high_gap_max
}


def _assert_ladder_monotone(ladder: list) -> list:
    """사다리가 선언된 방향으로 **단조**인지 검사한다. 아니면 즉시 실패한다.

    완화 단계는 이전 단계보다 느슨하거나 같아야 한다. 더 엄격해지면 사다리가 아니다.
    (2026-07-28 rs 재정의 때 바닥값만 고치고 계수를 안 고쳐 L0 -0.0400 -> L6 -0.0278 로
     **31% 더 엄격**해졌다. 2026-09-10 까지 6주간 아무도 못 봤다 - 검사가 없었기 때문이다)
    """
    viol = []
    for i in range(1, len(ladder)):
        pname, prev = ladder[i - 1]
        cname, cur = ladder[i]
        for k, direction in RELAX_DIRECTION.items():
            if k not in prev or k not in cur:
                continue
            try:
                a, b = float(prev[k]), float(cur[k])
            except Exception:
                continue
            if direction == "down" and b > a + 1e-12:
                viol.append("%s->%s %s %.6f -> %.6f (내려가야 하는데 올라감)" % (pname, cname, k, a, b))
            elif direction == "up" and b < a - 1e-12:
                viol.append("%s->%s %s %.6f -> %.6f (올라가야 하는데 내려감)" % (pname, cname, k, a, b))
    if viol:
        raise RuntimeError(
            "[RELAX_LADDER] 완화 사다리가 단조가 아니다. 완화 단계가 오히려 조이고 있다:\n  "
            + "\n  ".join(viol)
        )
    return ladder


def _relax_rs_lim(value: float, frac: float, floor: float = -0.10) -> float:
    """rs_lim 을 **바닥 쪽으로** 이동시킨다. 부호와 무관하게 항상 느슨해진다.

    게이트는 `rs > rs_lim` 이라 rs_lim 이 **내려가야** 완화다.

    [2026-09-10] 종전 `max(rs_lim * 0.95, -0.10)` 는 rs_lim 이 음수면 반대로 갔다.
      -0.04 * 0.95 = -0.038 -> 0 쪽으로 **올라간다** = 더 엄격해진다.
      실측: L0 -0.0400 -> L6 -0.0278 로 완화 사다리를 오를수록 31% 더 엄격했다.
      바닥값 max(..., -0.10) 은 max() 가 늘 덜 음수인 쪽을 골라 **영원히 못 닿았다.**
      바닥을 -0.10 으로 내려둔 것 자체가 "아래로 가야 한다" 는 의도의 증거다.

      계보: 이 사다리는 rs 가 **비율**(rs_lim=1.6, 양수)이던 시절 코드다.
      2026-07-28 에 rs 를 차이값으로 재정의하며 5곳을 고쳤는데,
      사다리는 **바닥값만 고치고 곱셈 계수는 그대로 뒀다** = 반쪽 수리.

    구현: 남은 거리(value - floor)의 frac 만큼 바닥 쪽으로 옮긴다.
      옛 양수 값에서도 거의 같은 결과다 - 1.6, frac 0.05 -> 1.515 (종전 1.52).
    """
    v = float(value)
    f = float(floor)
    if v <= f:
        return f
    return max(v - (v - f) * float(frac), f)


def _relax_ladder(p0: dict) -> list[dict]:
    """?꾨낫 0媛?諛⑹?: ?먯쭊 ?꾪솕(遺덉븞??諛⑹??⑹쑝濡??④퀎 ?쒗븳)"""
    p0 = dict(p0)

    ladder = []
    ladder.append(("L0", dict(p0)))

    # L1: ATR ?곹븳 ?꾪솕 + v_accel ?꾪솕
    p1 = dict(p0)
    p1["atr_max"] = max(float(p1["atr_max"]), 0.10)
    p1["v_accel_lim"] = max(float(p1["v_accel_lim"]) * 0.90, 1.20)
    ladder.append(("L1", p1))

    # L2: ATR ???꾪솕 + RS ?쎄컙 ?꾪솕
    p2 = dict(p1)
    p2["atr_max"] = max(float(p2["atr_max"]), 0.12)
    p2["v_accel_lim"] = max(float(p2["v_accel_lim"]) * 0.90, 1.20)
    p2["rs_lim"] = _relax_rs_lim(p2["rs_lim"], 0.05)
    ladder.append(("L2", p2))

    # L3: stretch/value ?꾪솕(留덉?留??④퀎)
    p3 = dict(p2)
    p3["atr_max"] = max(float(p3["atr_max"]), 0.15)
    p3["v_accel_lim"] = max(float(p3["v_accel_lim"]) * 0.90, 1.20)
    p3["rs_lim"] = _relax_rs_lim(p3["rs_lim"], 0.05)
    p3["stretch_max"] = min(float(p3["stretch_max"]) + 0.03, 1.30)
    p3["value_min"] = max(float(p3["value_min"]) * 0.85, 1_000_000_000.0)
    ladder.append(("L3", p3))

    # L4: value_min 異붽? ?꾪솕 + stretch ?쎄컙 ?꾪솕
    p4 = dict(p3)
    p4["atr_max"] = max(float(p4["atr_max"]), 0.18)
    p4["v_accel_lim"] = max(float(p4["v_accel_lim"]) * 0.90, 1.15)
    p4["rs_lim"] = _relax_rs_lim(p4["rs_lim"], 0.07)
    p4["stretch_max"] = min(float(p4["stretch_max"]) + 0.03, 1.35)
    p4["value_min"] = max(float(p4["value_min"]) * 0.70, 1_000_000_000.0)
    ladder.append(("L4", p4))

    # L5: value_min ???꾪솕(?댁쁺??諛붾떏) + ATR 異붽? ?꾪솕
    p5 = dict(p4)
    p5["atr_max"] = max(float(p5["atr_max"]), 0.22)
    p5["v_accel_lim"] = max(float(p5["v_accel_lim"]) * 0.88, 1.10)
    p5["rs_lim"] = _relax_rs_lim(p5["rs_lim"], 0.08)
    p5["stretch_max"] = min(float(p5["stretch_max"]) + 0.04, 1.40)
    p5["value_min"] = max(float(p5["value_min"]) * 0.55, 1_000_000_000.0)
    ladder.append(("L5", p5))

    # L6: 留덉?留??꾪솕(洹몃옒??0媛쒕㈃ NONE ?좎?)
    p6 = dict(p5)
    p6["atr_max"] = max(float(p6["atr_max"]), 0.25)
    p6["v_accel_lim"] = max(float(p6["v_accel_lim"]) * 0.85, 1.05)
    p6["rs_lim"] = _relax_rs_lim(p6["rs_lim"], 0.10)
    p6["stretch_max"] = min(float(p6["stretch_max"]) + 0.05, 1.45)
    p6["value_min"] = max(float(p6["value_min"]) * 0.40, 1_000_000_000.0)
    ladder.append(("L6", p6))

    # L7-L9: late-stage recovery only for sample collapse.
    p7 = dict(p6)
    p7["near_52w_high_gap_max"] = max(float(p7.get("near_52w_high_gap_max", 0.05)), 0.65)
    p7["rsi_max"] = max(float(p7.get("rsi_max", 70.0)), 80.0)
    p7["vol_close_corr_min"] = min(float(p7.get("vol_close_corr_min", 0.0)), -0.30)
    p7["disparity20_max"] = max(float(p7.get("disparity20_max", 1.06)), 1.25)
    p7["disparity60_max"] = max(float(p7.get("disparity60_max", 1.12)), 1.36)
    p7["disparity200_max"] = max(float(p7.get("disparity200_max", 1.30)), 1.50)
    ladder.append(("L7", p7))

    p8 = dict(p7)
    p8["near_52w_high_gap_max"] = max(float(p8.get("near_52w_high_gap_max", 0.05)), 0.70)
    p8["rsi_max"] = max(float(p8.get("rsi_max", 70.0)), 85.0)
    p8["vol_close_corr_min"] = min(float(p8.get("vol_close_corr_min", 0.0)), -0.50)
    p8["disparity20_max"] = max(float(p8.get("disparity20_max", 1.06)), 1.25)
    p8["disparity60_max"] = max(float(p8.get("disparity60_max", 1.12)), 1.36)
    p8["disparity200_max"] = max(float(p8.get("disparity200_max", 1.30)), 1.50)
    ladder.append(("L8", p8))

    p9 = dict(p8)
    p9["near_52w_high_gap_max"] = max(float(p9.get("near_52w_high_gap_max", 0.05)), 0.80)
    p9["rsi_max"] = max(float(p9.get("rsi_max", 70.0)), 90.0)
    p9["vol_close_corr_min"] = min(float(p9.get("vol_close_corr_min", 0.0)), -1.00)
    p9["disparity20_max"] = max(float(p9.get("disparity20_max", 1.06)), 1.30)
    p9["disparity60_max"] = max(float(p9.get("disparity60_max", 1.12)), 1.42)
    p9["disparity200_max"] = max(float(p9.get("disparity200_max", 1.30)), 1.60)
    ladder.append(("L9", p9))

    # [2026-09-10] 만든 직후 검사한다. 여기서 막으면 잘못된 사다리가 생산에 안 나간다.
    return _assert_ladder_monotone(ladder)


def main() -> int:
    research_mode = os.environ.get("CANDIDATE_RESEARCH_MODE", "0") == "1"
    research_params_path_s = os.environ.get("CANDIDATE_RESEARCH_PARAMS_PATH", "").strip()
    research_output_dir_s = os.environ.get("CANDIDATE_RESEARCH_OUTPUT_DIR", "").strip()
    research_allow_unapproved = os.environ.get("CANDIDATE_RESEARCH_ALLOW_UNAPPROVED", "0") == "1"
    research_disable_relax = os.environ.get("CANDIDATE_RESEARCH_DISABLE_RELAX", "0") == "1"
    research_disable_sector_union = os.environ.get("CANDIDATE_RESEARCH_DISABLE_SECTOR_UNION", "0") == "1"
    output_log_dir = Path(research_output_dir_s) if research_output_dir_s else LOG_DIR
    output_log_dir.mkdir(parents=True, exist_ok=True)
    _audit_ymd = datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="generate_candidates",
        batch_label="[6.25/9]",
        event="START",
        date=_audit_ymd,
        input_files={
            "stable_params": str(RISK_DIR / "stable_params_v41_1.json"),
        },
    )

    stable_path = RISK_DIR / "stable_params_v41_1.json"
    if research_mode and research_params_path_s:
        stable_path = Path(research_params_path_s)
        raw_payload = read_json(stable_path) or {}
        raw_params = raw_payload.get("params", raw_payload) if isinstance(raw_payload, dict) else {}
    else:
        raw_params = read_json(stable_path) or {}
    try:
        stable_gate_cfg = load_stable_quality_gate(PAPER_ENGINE_CONFIG)
        stable_gate_status = evaluate_stable_params(raw_params, stable_gate_cfg)
    except Exception as exc:
        stable_gate_status = {
            "ok": False,
            "reason": f"quality_gate_load_failed:{type(exc).__name__}",
            "reasons": [f"quality_gate_load_failed:{type(exc).__name__}"],
            "promoted": bool(raw_params.get("promoted", False)),
        }
    params = _normalize_params(raw_params)
    print(
        f"[PARAM_GATE] ok={stable_gate_status.get('ok', False)} "
        f"reason={stable_gate_status.get('reason', 'unknown')}"
    )

    print(f"[PARAM] source=stable rs_lim={params['rs_lim']} v_accel_lim={params['v_accel_lim']} stretch_max={params['stretch_max']} value_min={params['value_min']} atr_max={params['atr_max']}")
    print(f"[PARAM] weights w_rs={params['w_rs']:.2f} w_rs_slope={params['w_rs_slope']:.2f} w_v_accel={params['w_v_accel']:.2f} w_tech={params['w_tech_score']:.2f} w_fund={params['w_fundamental_score']:.2f} ca_blend={params['company_analyzer_blend']:.2f}")
    print(f"[PARAM] watch exclude_admin={int(params['exclude_administrative'])} exclude_warning={int(params['exclude_investment_warning'])} exclude_risk={int(params['exclude_investment_risk'])} caution_penalty={params['watch_penalty_caution']:.2f}")
    print(f"[PARAM] junk enable={int(params['junk_risk_enable'])} hard_exclude={int(params['junk_hard_exclude'])} hard_th={params['junk_hard_threshold']:.1f} penalty_max={params['junk_penalty_max']:.2f}")

    def _rule_e_fmt(key: str) -> str:
        thr = _rule_e_threshold(params, key)
        return "OFF" if thr is None else f"{thr:+.4f}"

    print(
        f"[PARAM] rule_e mkt_ret20_min={_rule_e_fmt('mkt_ret20_min')} "
        f"mkt_ret60_min={_rule_e_fmt('mkt_ret60_min')} "
        f"sector_rs_min={_rule_e_fmt('sector_rs_min')} (not relaxed by ladder)"
    )

    df_raw = _load_data()
    if df_raw.empty:
        return 2

    listing = _load_listing_map()

    df, latest_dt, is_bull = _compute_factors(df_raw)

    _log_candidate_stable_gate(stable_gate_status, as_of_ymd=latest_dt.strftime("%Y%m%d"))


    # NOTE: as_of_select is produced inside _compute_factors() and stored on df.attrs
    as_of_select = ((getattr(df, "attrs", {}) or {}).get("as_of_select", {}) or {})
    today = df[df["date"] == latest_dt].copy()
    if today.empty:
        print(f"[WARN] no data for {latest_dt}")
        return 0

    # name column
    if "name" not in today.columns:
        today["name"] = np.nan

    # name merge
    if not listing.empty:
        today = today.merge(listing, on="code", how="left", suffixes=("", "_m"))
        if "name_m" in today.columns:
            today["name"] = today["name"].fillna(today["name_m"])
            today = today.drop(columns=["name_m"], errors="ignore")
    as_of_ymd = latest_dt.strftime("%Y%m%d")
    today, ext_fund_info = _merge_external_fundamental(today, as_of_ymd)
    today, watch_attach_info = _attach_krx_watch_flags(today, as_of_ymd)

    print(
        f"[FUND_JOIN] pykrx_rows={ext_fund_info.get('pykrx_rows',0)} "
        f"dart_rows={ext_fund_info.get('dart_rows',0)} joined_cols={len(ext_fund_info.get('joined_cols',[]))}"
    )
    print(
        f"[WATCH] rows={watch_attach_info.get('watch_rows',0)} "
        f"admin={watch_attach_info.get('admin_flags',0)} "
        f"warning={watch_attach_info.get('warning_flags',0)} "
        f"risk={watch_attach_info.get('risk_flags',0)} "
        f"caution={watch_attach_info.get('caution_flags',0)}"
    )

    watch_hard_info = {"before": 0, "after": 0, "removed_admin": 0, "removed_warning": 0, "removed_risk": 0}
    watch_soft_info = {"penalty": 0.0, "penalized_rows": 0}
    junk_info = {"enabled": False, "before": 0, "after": 0, "removed": 0, "penalized_rows": 0, "mean_score": 0.0}

    attempts = []
    chosen_level = None
    chosen_params = None
    candidates = pd.DataFrame()

    ladder = [("L0_RESEARCH", dict(params))] if (research_mode and research_disable_relax) else _relax_ladder(params)
    if not (research_mode and research_disable_relax):
        _relax_max_raw = str(os.environ.get("CAND_RELAX_MAX_LEVEL", "9")).strip()
        try:
            relax_max_level = max(0, int(float(_relax_max_raw)))
        except Exception:
            relax_max_level = 6
        ladder = [(level, p) for level, p in ladder if int(str(level).lstrip("L")) <= relax_max_level]
    for level, p in ladder:
        cand = _select_candidates(today, p)
        d = _diag_counts(today, p, len(cand))
        attempts.append({"level": level, "params": {k: float(p[k]) for k in PARAM_EXPORT_KEYS}, "diag": d})
        if (bool(stable_gate_status.get("ok", False)) or (research_mode and research_allow_unapproved)) and not cand.empty:
            chosen_level = level
            chosen_params = p
            candidates = cand
            # [2026-09-10] A 수리. **게이트를 통과한 후보임을 여기서 못박는다.**
            #   종전에는 표시가 없었다. 그래서 keep_cols 의
            #   `if c not in top.columns: top[c] = np.nan` 에 걸려 natural_pass 가
            #   NaN 이 됐고, 폴백 행에만 False 가 박혀 있었다.
            #   결과: 소비자마다 "이 행이 매매 가능한가" 를 원자료에서 다시 유도했고
            #   판정 조건의 절반(natural_pass)이 죽은 채로 우연히 맞고 있었다.
            candidates = candidates.copy()
            candidates["natural_pass"] = True
            if "candidate_origin" not in candidates.columns:
                candidates["candidate_origin"] = "GATE_PASS"
            else:
                candidates["candidate_origin"] = candidates["candidate_origin"].fillna("GATE_PASS")
            candidates["observe_only"] = False
            candidates["observe_only_reason"] = ""
            break

    rally_override = {"applied": False, "reason": "not_checked"}
    if chosen_level is not None and chosen_params is not None and (not candidates.empty):
        candidates2, params2, ov_applied, ov_info = _apply_rally_safety_override(today, chosen_params, candidates, chosen_level)
        rally_override = ov_info
        if ov_applied:
            candidates = candidates2
            chosen_params = params2
            attempts.append({
                "level": f"{chosen_level}_RALLY",
                "params": {k: float(chosen_params[k]) for k in PARAM_EXPORT_KEYS},
                "diag": _diag_counts(today, chosen_params, len(candidates)),
            })

    if chosen_level is None:
        # no candidates even after relax
        chosen_level = "NONE"
        chosen_params = params

    if research_mode and research_disable_sector_union:
        sector_union_info = {
            "enabled": False,
            "reason": "research_mode_disabled",
            "before": int(len(candidates)),
            "added": 0,
            "after": int(len(candidates)),
        }
    else:
        candidates, sector_union_info = _apply_sector_prefilter_union(today, candidates, chosen_params, chosen_level=chosen_level)
    candidates, watch_hard_info = _apply_krx_watch_hard_filter(candidates, chosen_params)

    meta = {
        "research_mode": {
            "enabled": bool(research_mode),
            "params_path": str(stable_path) if research_mode else None,
            "output_dir": str(output_log_dir) if research_mode else None,
            "allow_unapproved": bool(research_allow_unapproved),
            "disable_relax": bool(research_disable_relax),
            "disable_sector_union": bool(research_disable_sector_union),
            "official_use_allowed": False if research_mode else bool(stable_gate_status.get("ok", False)),
            # [2026-08-20] 파라미터 자격 라벨. official_use_allowed 는 "가동 가능"만
            # 뜻하므로, 인증 여부를 별도로 실어 소비자가 구분할 수 있게 한다.
            "param_certified": bool(stable_gate_status.get("certified", False)),
            "param_operational": bool(stable_gate_status.get("operational", False)),
            "param_cert_reason": str(stable_gate_status.get("cert_reason", "") or ""),
        },
        "stable_param_gate": stable_gate_status,
        "stable_params": {
            "path": str(stable_path),
            "file_mtime": (datetime.fromtimestamp(stable_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if stable_path.exists() else None),
            "sha256": (_sha256_file(stable_path) if stable_path.exists() else None),
        },

        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_date": latest_dt.strftime("%Y-%m-%d"),
        "market_regime": "BULL" if is_bull else "BEAR",
        "chosen_level": chosen_level,
        "chosen_params": {k: float(chosen_params[k]) for k in PARAM_EXPORT_KEYS},
        "attempts": attempts,
        "rally_override": rally_override,
        "sector_prefilter_union": sector_union_info,
        "external_fundamental": ext_fund_info,
        "watch_attach": watch_attach_info,
        "watch_hard_filter": watch_hard_info,
        "watch_soft_penalty": watch_soft_info,
    }
    out_path = output_log_dir / f"candidates_v41_1_{latest_dt.strftime('%Y%m%d')}.csv"
    latest_ptr = output_log_dir / "candidates_latest.csv"
    latest_data_path = output_log_dir / "candidates_latest_data.csv"
    latest_meta_path = output_log_dir / "candidates_latest_meta.json"

    # [CR] meta paths SSOT (寃쎈줈??臾몄옄?대줈 怨좎젙 湲곕줉)
    meta["paths"] = {
        "log_dir": str(output_log_dir),
        "versioned_csv": str(out_path),
        "latest_ptr": str(latest_ptr),
        "latest_data_csv": str(latest_data_path),
        "latest_meta_json": str(latest_meta_path),
        "csv": str(latest_data_path),  # standard_check expects paths.csv (alias of latest_data_csv)
    }
    out_path = output_log_dir / f"candidates_v41_1_{latest_dt.strftime('%Y%m%d')}.csv"
    latest_ptr = output_log_dir / "candidates_latest.csv"
    latest_data_path = output_log_dir / "candidates_latest_data.csv"
    latest_meta_path = output_log_dir / "candidates_latest_meta.json"

    if candidates.empty:
        # empty output (schema fixed)
        cols = ["no","date","code","name","market","market_regime","close","value","market_cap","listed_shares","rs","rs_slope","stretch","v_accel","atr14_pct","rsi14","macd_golden","vol_close_corr20","high_52w_gap","listing_days","score","fundamental_score","fundamental_grade","final_score","junk_risk_score","junk_risk_grade","junk_flags","krx_admin","krx_warning","krx_risk","krx_caution","relax_level"]
        _atomic_write_csv(out_path, pd.DataFrame(columns=cols))
        _atomic_write_csv(latest_data_path, pd.DataFrame(columns=cols))
        _atomic_write_text(latest_ptr, out_path.name, encoding="utf-8")
        print("[FIX17] latest_data updated empty-current (no candidates) -> candidates_latest_data.csv")
        meta['as_of_select'] = dict(as_of_select)
        meta['as_of_select']['src'] = 'generate_candidates_v41_1.py'  # standard_check expects as_of_select.src
        _atomic_write_json(latest_meta_path, meta)

        # print
        diag0 = attempts[-1]["diag"] if attempts else {}
        print(f"\n[AS_OF] {latest_dt.date()} (BULL={is_bull})")
        print("[WARN] no candidates passed filters")
        print(f"[DIAG] {diag0}")
        print(f"[OUT] {out_path}")
        print(f"[META] {latest_meta_path}")
        log_pipeline_event(
            stage="generate_candidates",
            batch_label="[6.25/9]",
            event="END",
            date=_audit_ymd,
            output_files={
                "candidates_latest_data": {"path": str(latest_data_path), "rows": count_rows(latest_data_path)},
                "versioned_csv": {"path": str(out_path), "rows": 0},
            },
            metrics={
                "chosen_level": chosen_level,
                "candidates_empty": True,
            },
            status="PASS",
        )
        return 0

    # score
    w_rs = float(chosen_params["w_rs"])
    w_sl = float(chosen_params["w_rs_slope"])
    w_va = float(chosen_params["w_v_accel"])

    candidates["score"] = (
        candidates["rs"].rank(pct=True) * w_rs
        + candidates["rs_slope"].rank(pct=True) * w_sl
        + candidates["v_accel"].rank(pct=True) * w_va
    )
    candidates["relax_level"] = chosen_level

    market_regime = "BULL" if bool(is_bull) else "BEAR"
    candidates = _apply_fundamental_overlay(candidates, chosen_params, market_regime=market_regime)
    candidates, junk_info = _apply_junk_risk_overlay(candidates, chosen_params)
    candidates, watch_soft_info = _apply_krx_watch_soft_penalty(candidates, chosen_params)

    top_n = max(int(float(os.environ.get("CANDIDATE_TOP_N", "20"))), 1)
    top = candidates.sort_values("final_score", ascending=False).head(top_n).copy()
    top.insert(0, "no", range(1, len(top) + 1))

    # [2026-09-10] A 수리. **매매 가능 여부를 여기서 한 번 정해 내려보낸다.**
    #   소비자가 candidate_origin/natural_pass 를 각자 다시 조합하지 않게 한다.
    #   지금 규칙은 종전 entry.py 의 판정과 같다 -
    #     섹터 폴백(SECTOR_PREFILTER_UNION)이고 자연통과가 아니면 매매 불가.
    #   규칙을 바꾸는 것이 아니라 **정하는 자리를 옮기는 것**이다.
    _origin = top["candidate_origin"].astype(str).str.strip().str.upper() \
        if "candidate_origin" in top.columns else pd.Series("", index=top.index)
    _natural = top["natural_pass"].map(lambda v: str(v).strip().lower() in {"1", "true", "t", "y", "yes"}) \
        if "natural_pass" in top.columns else pd.Series(False, index=top.index)
    _fallback = _origin.eq("SECTOR_PREFILTER_UNION") & (~_natural)
    top["tradable"] = ~_fallback
    top["tradable_reason"] = np.where(_fallback, "SECTOR_FALLBACK_OBSERVE_ONLY", "GATE_PASS")

    # columns order
    keep_cols = ["no","date","code","name","market","market_regime","close","value","market_cap","listed_shares","ret1_pct","rs","rs_slope","stretch","v_accel","atr14_pct","rsi14","macd_golden","vol_close_corr20","high_52w_gap","listing_days","ma60_support_bounce","trend_smoothness","price_vol_divergence","score","fundamental_score","fundamental_grade","final_score","junk_risk_score","junk_risk_grade","junk_flags","krx_admin","krx_warning","krx_risk","krx_caution","relax_level","candidate_origin","natural_pass","observe_only","observe_only_reason","tradable","tradable_reason"]
    for c in keep_cols:
        if c not in top.columns:
            top[c] = np.nan
    top = top[keep_cols]

    _atomic_write_csv(out_path, top)
    _atomic_write_text(latest_ptr, out_path.name, encoding="utf-8")
    _atomic_write_csv(latest_data_path, top)
    meta['watch_soft_penalty'] = watch_soft_info
    meta['junk_risk'] = junk_info
    meta['as_of_select'] = as_of_select
    meta['as_of_select']['src'] = 'generate_candidates_v41_1.py'  # standard_check expects as_of_select.src
    _atomic_write_json(latest_meta_path, meta)

    _log_candidate_summary(today, chosen_level, chosen_params, top, as_of_ymd)

    # print summary
    diag_chosen = attempts[-1]["diag"] if attempts else {}
    print(f"\n[AS_OF] {latest_dt.date()} (BULL={is_bull})")
    if chosen_level != "L0":
        print(f"[RELAX] applied level={chosen_level}")
    print(f"[TOP] v41.1 Top {top_n}:")
    print("-" * 110)
    print(top[["no","date","code","name","market","close","score","fundamental_score","junk_risk_score","final_score","krx_admin","krx_warning","krx_risk","krx_caution","relax_level"]].to_string(index=False))
    print("-" * 110)
    print(f"[OUT] {out_path}")
    print(f"[META] {latest_meta_path}")
    print(f"[DIAG] {diag_chosen}")
    log_pipeline_event(
        stage="generate_candidates",
        batch_label="[6.25/9]",
        event="END",
        date=_audit_ymd,
        output_files={
            "candidates_latest_data": {"path": str(latest_data_path), "rows": int(len(top))},
            "versioned_csv": {"path": str(out_path), "rows": int(len(top))},
        },
        metrics={
            "chosen_level": chosen_level,
            "candidates_empty": False,
        },
        status="PASS",
    )
    return 0


def _log_candidate_stable_gate(stable_gate_status: dict, as_of_ymd: str) -> None:
    ok = bool(stable_gate_status.get("ok", False))
    log_gate_event(
        gate_layer="candidate",
        gate_name="stable_params_quality_gate",
        decision="PASS" if ok else "BLOCK",
        reason_code="ok" if ok else "stable_gate_failed",
        reason_detail=str(stable_gate_status.get("reason", "")),
        source_file="generate_candidates_v41_1.py",
        source_key="evaluate_stable_params",
        recoverable=not ok,
        recovery_hint="HPO 또는 gate 임계값 조정",
        date=as_of_ymd,
        extra={k: v for k, v in stable_gate_status.items() if k not in {"reasons", "thresholds"}},
    )


def _log_candidate_summary(
    today: pd.DataFrame,
    chosen_level: str,
    chosen_params: dict,
    top: pd.DataFrame,
    as_of_ymd: str,
) -> None:
    if top.empty:
        log_gate_event(
            gate_layer="candidate",
            gate_name="candidate_pool_empty",
            decision="BLOCK",
            reason_code="no_candidates_after_filters",
            reason_detail=f"level={chosen_level}, today_rows={len(today)}",
            source_file="generate_candidates_v41_1.py",
            source_key="main",
            recoverable=True,
            recovery_hint="필터/릴랙스 레벨 조정",
            date=as_of_ymd,
            extra={"chosen_level": chosen_level, "today_rows": int(len(today))},
        )
        return
    for _, r in top.iterrows():
        code = str(r.get("code", "")).zfill(6)
        log_gate_event(
            code=code,
            gate_layer="candidate",
            gate_name="candidate_selected",
            decision="PASS",
            source_file="generate_candidates_v41_1.py",
            source_key=f"level={chosen_level}",
            date=as_of_ymd,
            extra={
                "level": chosen_level,
                "final_score": float(r.get("final_score", 0)),
                "rs": float(r.get("rs", 0)),
                "value": float(r.get("value", 0)),
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
