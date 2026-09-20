from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import logging


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = Path(os.getenv("TRVAL_CONFIG_PATH", str(ROOT / "config" / "trading_stage_validation.json")))
KST = dt.timezone(dt.timedelta(hours=9))




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            if not isinstance(obj, dict):
                raise SystemExit(f"[TRVAL_CONFIG] invalid root type (dict required): {path}")
            return obj
        except json.JSONDecodeError as e:
            raise SystemExit(f"[TRVAL_CONFIG] invalid JSON: {path} ({e})")
        except UnicodeDecodeError:
            continue
    raise SystemExit(f"[TRVAL_CONFIG] unreadable config encoding: {path}")


CFG = _load_config(CONFIG_PATH)


def _cfg_get(path: str, default: Any = None) -> Any:
    cur: Any = CFG
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _parse_bool_strict(name: str, raw: Any) -> bool:
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    raise SystemExit(f"[TRVAL_CONFIG] invalid bool for {name}: {raw}")


def _env_or_cfg(name: str, cfg_path: str, default: Any = None) -> Any:
    v = os.getenv(name, None)
    if v is not None and str(v).strip() != "":
        return v
    return _cfg_get(cfg_path, default)


def _env_or_cfg_float(name: str, cfg_path: str, default: float) -> float:
    raw = _env_or_cfg(name, cfg_path, default)
    try:
        return float(raw)
    except Exception:
        raise SystemExit(f"[TRVAL_CONFIG] invalid float for {name}/{cfg_path}: {raw}")


def _env_or_cfg_int(name: str, cfg_path: str, default: int) -> int:
    raw = _env_or_cfg(name, cfg_path, default)
    try:
        return int(raw)
    except Exception:
        raise SystemExit(f"[TRVAL_CONFIG] invalid int for {name}/{cfg_path}: {raw}")


def _env_or_cfg_bool(name: str, cfg_path: str, default: bool) -> bool:
    raw = _env_or_cfg(name, cfg_path, default)
    return _parse_bool_strict(f"{name}/{cfg_path}", raw)


LOG_DIR = Path(_env_or_cfg("TRVAL_LOG_DIR", "paths.log_dir", str(ROOT / "2_Logs"))).expanduser()

PASS = "PASS"
FAIL = "FAIL"
NE = "NOT_EVALUABLE"


# Freshness window for auxiliary state logs (p0/gate/after_close)
RISK_STATE_FRESH_DAYS = _env_or_cfg_float("TRVAL_RISK_STATE_FRESH_DAYS", "thresholds.risk_state_fresh_days", 7.0)

# Warm-up relaxed thresholds (override via env if needed)
TRVAL_PAPER_FRESH_MAX_DAYS = _env_or_cfg_float("TRVAL_PAPER_FRESH_MAX_DAYS", "thresholds.paper_fresh_max_days", 5.0)
TRVAL_PAPER_MIN_TRADES = _env_or_cfg_int("TRVAL_PAPER_MIN_TRADES", "thresholds.paper_min_trades", 9)
TRVAL_PAPER_MAX_DD_PCT = _env_or_cfg_float("TRVAL_PAPER_MAX_DD_PCT", "thresholds.paper_max_drawdown_pct", -80.0)
TRVAL_PAPER_LIVE_MAX_DD_PCT = _env_or_cfg_float("TRVAL_PAPER_LIVE_MAX_DD_PCT", "thresholds.paper_live_max_drawdown_pct", -20.0)
TRVAL_PAPER_WARMUP_TRADES = _env_or_cfg_int("TRVAL_PAPER_WARMUP_TRADES", "thresholds.paper_warmup_trades", 50)
TRVAL_PAPER_RET_DIVERGENCE_MAX = _env_or_cfg_float("TRVAL_PAPER_RET_DIVERGENCE_MAX", "thresholds.paper_return_divergence_max", 0.30)
TRVAL_LIVE_E2E_FRESH_MAX_DAYS = _env_or_cfg_float("TRVAL_LIVE_E2E_FRESH_MAX_DAYS", "thresholds.live_e2e_fresh_max_days", 7.0)
TRVAL_PAPER_RISK_OFF_SOFT_WARMUP = _env_or_cfg_bool("TRVAL_PAPER_RISK_OFF_SOFT_WARMUP", "policy.risk_off_soft_warmup", True)
TRVAL_REQUIRE_PENDING_CLEAR = _env_or_cfg_bool("TRVAL_REQUIRE_PENDING_CLEAR", "policy.require_pending_clear", True)
TRVAL_STRICT_DATE_CONSISTENCY = _env_or_cfg_bool("TRVAL_STRICT_DATE_CONSISTENCY", "policy.strict_date_consistency", True)
TRVAL_TRANSITION_REQUIRE_RET_DIVERGENCE = _env_or_cfg_bool(
    "TRVAL_TRANSITION_REQUIRE_RET_DIVERGENCE",
    "policy.require_return_divergence",
    True,
)
TRVAL_ASOF_YMD = ""

def _normalize_ymd(s: Any) -> str:
    t = "".join(ch for ch in str(s or "") if ch.isdigit())
    if len(t) < 8:
        return ""
    cand = t[:8]
    try:
        dt.datetime.strptime(cand, "%Y%m%d")
    except Exception:
        return ""
    return cand


def _ymd_to_date(ymd: str) -> Optional[dt.date]:
    y = _normalize_ymd(ymd)
    if len(y) != 8:
        return None
    try:
        return dt.datetime.strptime(y, "%Y%m%d").date()
    except Exception:
        return None


def _now_kst() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(KST)


def _resolve_oper_start_ymd() -> str:
    raw = _env_or_cfg("PAPER_OPER_START_YMD", "scope.paper_oper_start_ymd", "")
    ymd = _normalize_ymd(raw)
    if len(ymd) == 8:
        return ymd
    if TRVAL_STRICT_DATE_CONSISTENCY:
        raise SystemExit("[TRVAL_CONFIG] PAPER_OPER_START_YMD(scope.paper_oper_start_ymd) is required (YYYYMMDD)")
    return ""


def _resolve_oper_scope_meta(pnl: Dict[str, Any], fb: Dict[str, Any]) -> Dict[str, Any]:
    scope = _as_dict(_as_dict(pnl.get("meta")).get("operational_scope")) if pnl else {}
    pnl_start = _normalize_ymd(scope.get("start_ymd"))
    fb_start = _normalize_ymd((_as_dict(fb.get("thresholds")).get("oper_start_ymd")) if fb else "")
    env_start = _resolve_oper_start_ymd()

    effective = pnl_start or fb_start or env_start
    mismatch = bool(pnl_start and fb_start and pnl_start != fb_start)

    out = {
        "enabled": bool(scope.get("enabled", bool(effective))),
        "start_ymd": effective or None,
        "source_priority": ["paper_pnl_summary.meta.operational_scope", "live_vs_bt_feedback.thresholds.oper_start_ymd", "env:PAPER_OPER_START_YMD"],
        "pnl_scope_start_ymd": pnl_start or None,
        "feedback_oper_start_ymd": fb_start or None,
        "env_oper_start_ymd": env_start or None,
        "applied": bool(scope.get("applied", False)),
        "filter_col": scope.get("filter_col"),
        "rows_before": scope.get("rows_before"),
        "rows_after": scope.get("rows_after"),
        "scope_mismatch": mismatch,
    }
    return out



def _read_json(path: Path, *, fail_on_parse: bool = False) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parse_errors: List[str] = []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception as e:
            parse_errors.append(f"{enc}:{type(e).__name__}")
            continue
    if fail_on_parse:
        raise SystemExit(f"[TRVAL_CONFIG] invalid JSON input: {path} ({', '.join(parse_errors)})")
    return {}


def _ws_status_path() -> Path:
    # kis_realtime_ws.py writes kis_ws_status_latest{_worker}.json, and
    # kis_ws_multiplexer.py always assigns a worker id, so the unsuffixed
    # name stops being updated. Pick whichever worker file is newest.
    # _latest_by_glob is not usable here: it ranks on as_of_ymd, which these
    # status payloads do not carry.
    files = [p for p in LOG_DIR.glob("kis_ws_status_latest*.json") if p.is_file()]
    if not files:
        return LOG_DIR / "kis_ws_status_latest.json"
    return max(files, key=lambda p: p.stat().st_mtime)


def _latest_by_glob(pattern: str) -> Optional[Path]:
    files = list(LOG_DIR.glob(pattern))
    if not files:
        return None

    def _name_date8(p: Path) -> str:
        m = re.findall(r"(20\d{6})", p.name)
        if not m:
            return ""
        y = _normalize_ymd(m[-1])
        return y

    candidates: List[Tuple[str, float, Path]] = []
    asof_lim = _normalize_ymd(TRVAL_ASOF_YMD)
    for p in files:
        name_d8 = _name_date8(p)
        body_d8 = ""
        if p.suffix.lower() == ".json":
            obj = _read_json(p, fail_on_parse=False)
            body_d8 = _normalize_ymd(obj.get("as_of_ymd") or obj.get("as_of") or obj.get("input_asof_ymd"))
        if TRVAL_STRICT_DATE_CONSISTENCY and name_d8 and body_d8 and name_d8 != body_d8:
            continue
        rank_d8 = body_d8 or name_d8
        if TRVAL_STRICT_DATE_CONSISTENCY and not rank_d8:
            # fail-closed: strict mode에서는 날짜가 없는 파일을 latest 후보에서 제외
            continue
        if asof_lim and rank_d8 and rank_d8 > asof_lim:
            continue
        candidates.append((rank_d8, float(p.stat().st_mtime), p))

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def _age_days(path: Path) -> float:
    if not path.exists():
        return float("nan")
    ts = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).astimezone(KST)
    return (_now_kst() - ts).total_seconds() / 86400.0


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _as_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _safe_float(v: Any) -> float:
    try:
        f = float(v)
    except Exception:
        return float("nan")
    if math.isnan(f) or math.isinf(f):
        return float("nan")
    return f


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _is_finite(v: float) -> bool:
    return not (math.isnan(v) or math.isinf(v))


def _fmt_num(v: Any) -> str:
    f = _safe_float(v)
    return "-" if not _is_finite(f) else f"{f:.4g}"


def _fmt_pct(v: Any) -> str:
    f = _safe_float(v)
    return "-" if not _is_finite(f) else f"{(f * 100.0):.2f}%"


def _make_item(
    name: str,
    status: str,
    metric: str,
    threshold: str,
    issue: str,
    action: str,
    required: bool = True,
) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "metric": metric,
        "threshold": threshold,
        "issue": issue,
        "action": action,
        "required": required,
    }


def _judge(items: List[Dict[str, Any]], stage: str) -> Tuple[str, Dict[str, int]]:
    req = [x for x in items if bool(x.get("required", True))]
    pass_n = sum(1 for x in req if x.get("status") == PASS)
    fail_n = sum(1 for x in req if x.get("status") == FAIL)
    ne_n = sum(1 for x in req if x.get("status") == NE)
    total = len(req)

    if fail_n > 0:
        judgment = "보류"
    elif ne_n > 0:
        judgment = "조건부"
    else:
        judgment = "운영가능"

    if stage == "live":
        canary_exec = next((x for x in items if x.get("name") == "canary_execute_mode"), None)
        if canary_exec and canary_exec.get("status") == NE and judgment == "운영가능":
            judgment = "실주문대기"

    return judgment, {"total": total, "pass_n": pass_n, "fail_n": fail_n, "not_evaluable_n": ne_n}


def _resolve_paper_risk_off(p_pnl: Path, pnl: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve risk_off state from freshest reliable source.

    Priority by freshness among known sources:
    - p0_daily_check_*.json (risk_off.enabled)
    - gate_daily_*.json (snapshot.flags.risk_off_enabled or gate1 status)
    - after_close_summary_last.json (p0.risk_off or risk_off)
    - embedded paper_pnl_summary_last.after_close.risk_off
    """
    candidates: List[Dict[str, Any]] = []

    # 1) p0 latest
    p0_path = _latest_by_glob("p0_daily_check_*.json")
    p0 = _read_json(p0_path) if p0_path else {}
    p0_val = (_as_dict(p0.get("risk_off")).get("enabled") if p0 else None)
    if isinstance(p0_val, bool) and p0_path:
        candidates.append({"source": "p0_daily_check", "value": p0_val, "path": p0_path, "age_days": _age_days(p0_path)})

    # 2) gate latest
    gate_path = _latest_by_glob("gate_daily_*.json")
    gate = _read_json(gate_path) if gate_path else {}
    gate_val: Optional[bool] = None
    if gate:
        flags = _as_dict(_as_dict(gate.get("snapshot")).get("flags"))
        if isinstance(flags.get("risk_off_enabled"), bool):
            gate_val = bool(flags.get("risk_off_enabled"))
        else:
            g1 = ((gate.get("gate1") or {}).get("status"))
            if isinstance(g1, str):
                gate_val = (g1.upper() != "PASS")
    if isinstance(gate_val, bool) and gate_path:
        candidates.append({"source": "gate_daily", "value": gate_val, "path": gate_path, "age_days": _age_days(gate_path)})

    # 3) after_close_summary_last
    ac_path = LOG_DIR / "after_close_summary_last.json"
    ac = _read_json(ac_path)
    ac_val: Optional[bool] = None
    if ac:
        v1 = _as_dict(ac.get("p0")).get("risk_off")
        v2 = ac.get("risk_off")
        if isinstance(v1, bool):
            ac_val = v1
        elif isinstance(v2, bool):
            ac_val = v2
    if isinstance(ac_val, bool) and ac_path.exists():
        candidates.append({"source": "after_close_summary_last", "value": ac_val, "path": ac_path, "age_days": _age_days(ac_path)})

    # 4) embedded in pnl summary
    emb_val = (_as_dict(pnl.get("after_close")).get("risk_off") if pnl else None)
    if isinstance(emb_val, bool) and p_pnl.exists():
        candidates.append({"source": "paper_pnl_summary_embedded", "value": emb_val, "path": p_pnl, "age_days": _age_days(p_pnl)})

    if not candidates:
        return {
            "known": False,
            "value": None,
            "source": None,
            "age_days": float("nan"),
            "inputs": [],
            "p0_path": str(p0_path) if p0_path else None,
            "gate_path": str(gate_path) if gate_path else None,
            "after_close_path": str(ac_path),
        }

    # Prefer fresh candidates only. If every source is stale, do not silently
    # decide PASS/FAIL from stale state; surface as data gap (NOT_EVALUABLE).
    fresh = [c for c in candidates if _is_finite(_safe_float(c.get("age_days"))) and float(c.get("age_days", 9999.0)) <= RISK_STATE_FRESH_DAYS]
    if not fresh:
        return {
            "known": False,
            "value": None,
            "source": None,
            "age_days": float("nan"),
            "inputs": [
                {
                    "source": str(c.get("source")),
                    "value": bool(c.get("value")) if isinstance(c.get("value"), bool) else None,
                    "age_days": float(c.get("age_days", float("nan"))),
                    "path": str(c.get("path")),
                }
                for c in sorted(candidates, key=lambda x: float(x.get("age_days", 9999.0)))
            ],
            "stale_only": True,
            "p0_path": str(p0_path) if p0_path else None,
            "gate_path": str(gate_path) if gate_path else None,
            "after_close_path": str(ac_path),
        }
    pool = sorted(fresh, key=lambda c: float(c.get("age_days", 9999.0)))
    chosen = pool[0]

    return {
        "known": True,
        "value": bool(chosen.get("value")),
        "source": str(chosen.get("source")),
        "age_days": float(chosen.get("age_days", float("nan"))),
        "inputs": [
            {
                "source": str(c.get("source")),
                "value": bool(c.get("value")) if isinstance(c.get("value"), bool) else None,
                "age_days": float(c.get("age_days", float("nan"))),
                "path": str(c.get("path")),
            }
            for c in sorted(candidates, key=lambda x: float(x.get("age_days", 9999.0)))
        ],
        "p0_path": str(p0_path) if p0_path else None,
        "gate_path": str(gate_path) if gate_path else None,
        "after_close_path": str(ac_path),
    }


def _build_paper_stage() -> Dict[str, Any]:
    p_pnl = LOG_DIR / "paper_pnl_summary_last.json"
    p_fb = LOG_DIR / "live_vs_bt_feedback_latest.json"
    p_pending = LOG_DIR / "pending_entry_status_latest.json"
    p_pending_signals = LOG_DIR / "pending_entry_signals_latest.csv"

    pnl = _read_json(p_pnl, fail_on_parse=p_pnl.exists())
    fb = _read_json(p_fb, fail_on_parse=p_fb.exists())
    pending = _read_json(p_pending, fail_on_parse=p_pending.exists())
    oper_scope = _resolve_oper_scope_meta(pnl=pnl, fb=fb)
    alignment = _as_dict(_as_dict(fb.get("optimize")).get("alignment")) if fb else {}
    alignment_snapshot = {
        "mode": alignment.get("mode"),
        "ready": alignment.get("ready"),
        "reason": alignment.get("reason"),
        "window_start": alignment.get("window_start"),
        "window_end": alignment.get("window_end"),
        "live_n": alignment.get("live_n"),
        "bt_n": alignment.get("bt_n"),
        "ret_col_live": alignment.get("ret_col_live"),
        "ret_col_bt": alignment.get("ret_col_bt"),
        "live_path": alignment.get("live_path"),
        "bt_path": alignment.get("bt_path"),
        "oper_start_ymd": alignment.get("oper_start_ymd"),
        "live_sample_rows": alignment.get("live_sample_rows") or [],
        "bt_sample_rows": alignment.get("bt_sample_rows") or [],
        "feedback_loop_state": alignment.get("feedback_loop_state"),
        "feedback_loop_reason": alignment.get("feedback_loop_reason"),
        "bt_live_coverage_ratio": alignment.get("bt_live_coverage_ratio"),
        "monthly_counts": alignment.get("monthly_counts") or [],
    }

    items: List[Dict[str, Any]] = []

    age = _age_days(p_pnl)
    if _is_finite(age):
        st = PASS if age <= TRVAL_PAPER_FRESH_MAX_DAYS else FAIL
        items.append(
            _make_item(
                "paper_freshness",
                st,
                f"age_days={_fmt_num(age)}",
                f"<={TRVAL_PAPER_FRESH_MAX_DAYS:g}",
                "최근 가상매매 결과 파일 최신성",
                "run_paper_daily.bat 재실행",
            )
        )
    else:
        items.append(_make_item("paper_freshness", NE, "age_days=-", f"<={TRVAL_PAPER_FRESH_MAX_DAYS:g}", "가상매매 요약 파일 없음", "run_paper_daily.bat 실행"))

    trades = _safe_int(pnl.get("trades_used", 0)) if pnl else 0
    if pnl:
        items.append(
            _make_item(
                "paper_trade_count",
                PASS if trades >= TRVAL_PAPER_MIN_TRADES else FAIL,
                f"trades_used={trades}",
                f">={TRVAL_PAPER_MIN_TRADES}",
                "가상매매 표본 거래 수",
                f"표본 {TRVAL_PAPER_MIN_TRADES}회 이상 누적",
            )
        )
    else:
        items.append(_make_item("paper_trade_count", NE, "trades_used=-", f">={TRVAL_PAPER_MIN_TRADES}", "가상매매 요약 파일 없음", "run_paper_daily.bat 실행"))

    mdd = _safe_float(((pnl.get("equity") or {}).get("max_drawdown_pct")) if pnl else float("nan"))
    if _is_finite(mdd):
        if trades < TRVAL_PAPER_WARMUP_TRADES:
            items.append(
                _make_item(
                    "paper_mdd",
                    NE,
                    f"max_drawdown={_fmt_pct(mdd)}, trades_used={trades}",
                    f"trades>={TRVAL_PAPER_WARMUP_TRADES} then >={TRVAL_PAPER_MAX_DD_PCT:.2f}%",
                    "가상매매 최대 낙폭(워밍업)",
                    "표본 확장 후 MDD 재검증",
                    required=False,
                )
            )
        else:
            items.append(
                _make_item(
                    "paper_mdd",
                    PASS if mdd >= (TRVAL_PAPER_MAX_DD_PCT / 100.0) else FAIL,
                    f"max_drawdown={_fmt_pct(mdd)}",
                    f"paper 참고 기준 >={TRVAL_PAPER_MAX_DD_PCT:.2f}%",
                    "가상매매 최대 낙폭(참고 기준)",
                    "포지션 축소/손절 강화",
                )
            )
            items.append(
                _make_item(
                    "paper_mdd_live_transition",
                    PASS if mdd >= (TRVAL_PAPER_LIVE_MAX_DD_PCT / 100.0) else FAIL,
                    f"max_drawdown={_fmt_pct(mdd)}, trades_used={trades}",
                    f"실전 전환 기준 >={TRVAL_PAPER_LIVE_MAX_DD_PCT:.2f}%",
                    "가상매매 최대 낙폭(실전 전환 기준)",
                    "DDM/사이징/손절 정책 재검증",
                    required=False,
                )
            )
    else:
        items.append(_make_item("paper_mdd", NE, "max_drawdown=-", f">={TRVAL_PAPER_MAX_DD_PCT:.2f}%", "낙폭 계산값 없음", "paper_pnl_summary 생성"))
        items.append(_make_item("paper_mdd_live_transition", NE, "max_drawdown=-", f">={TRVAL_PAPER_LIVE_MAX_DD_PCT:.2f}%", "실전 전환 낙폭 계산값 없음", "paper_pnl_summary 생성", required=False))

    risk_state = _resolve_paper_risk_off(p_pnl=p_pnl, pnl=pnl)
    if bool(risk_state.get("known")):
        risk_off = bool(risk_state.get("value"))
        src = str(risk_state.get("source") or "-")
        src_age = _fmt_num(risk_state.get("age_days"))
        risk_off_soft_pass = bool(
            risk_off
            and trades < TRVAL_PAPER_WARMUP_TRADES
            and TRVAL_PAPER_RISK_OFF_SOFT_WARMUP
        )
        items.append(
            _make_item(
                "paper_risk_off_state",
                PASS if (not risk_off or risk_off_soft_pass) else FAIL,
                f"risk_off={risk_off} (src={src}, age_days={src_age})",
                f"False (or warmup<{TRVAL_PAPER_WARMUP_TRADES})",
                "현재 리스크오프 상태",
                "kill-switch 원인 해소 후 재검증",
            )
        )
    else:
        items.append(_make_item("paper_risk_off_state", NE, "risk_off=-", "False", "리스크 상태 데이터 없음", "after_close_summary 생성"))

    align_ready = bool(((fb.get("comparison") or {}).get("alignment_ready")) if fb else False)
    align_reason = str(alignment.get("reason") or "")
    align_bt_n = _safe_int(alignment.get("bt_n", 0))
    feedback_loop_state = str(alignment.get("feedback_loop_state") or "").upper()
    feedback_loop_reason = str(alignment.get("feedback_loop_reason") or "")
    bt_live_coverage_ratio = alignment.get("bt_live_coverage_ratio")
    comparable = _as_dict(alignment.get("real_validation_comparable"))
    strict_min_shared = _safe_int(_as_dict(fb.get("thresholds")).get("min_shared_trades", 0)) if fb else 0
    align_metric = (
        f"alignment_ready={align_ready} (relaxed_used=True, bt_n={align_bt_n}, strict_min_shared={strict_min_shared})"
        if align_reason == "bt_window_relaxed_min_shared"
        else f"alignment_ready={align_ready}"
    )
    if feedback_loop_state:
        if feedback_loop_state in {"COMPARABLE_SERIES_AVAILABLE", "ALIGNED_COMPARABLE"} and comparable:
            align_metric = (
                f"{align_metric}, feedback_loop_state={feedback_loop_state}, "
                f"basis={comparable.get('mode') or '-'}, "
                f"common_days={_safe_int(comparable.get('common_days', 0))}, "
                f"abs_diff={_fmt_pct(comparable.get('abs_diff'))}"
            )
        else:
            align_metric = (
                f"{align_metric}, feedback_loop_state={feedback_loop_state}, "
                f"feedback_loop_reason={feedback_loop_reason or '-'}, "
                f"bt_live_coverage_ratio={_fmt_num(bt_live_coverage_ratio)}"
            )
    align_action = (
        "BT/live 모델 정렬 재구성"
        if feedback_loop_state == "DIVERGED"
        else "BT comparable series 기준 유지 및 모니터링"
        if feedback_loop_state == "ALIGNED_COMPARABLE"
        else "BT comparable series 검토 후 트리거 연결 여부 결정"
        if feedback_loop_state == "COMPARABLE_SERIES_AVAILABLE"
        else "BT 비교 소스 정렬 및 comparable series 생성"
        if feedback_loop_state == "SOURCE_MISMATCH"
        else "백테스트/라이브 비교 윈도우 재구성"
    )
    relaxed_alignment_used = bool(align_reason == "bt_window_relaxed_min_shared")
    relaxed_alignment_insufficient = bool(
        align_reason == "bt_window_relaxed_min_shared"
        and strict_min_shared > 0
        and align_bt_n < strict_min_shared
    )
    if fb:
        if trades < TRVAL_PAPER_WARMUP_TRADES:
            items.append(
                _make_item(
                    "paper_bt_alignment",
                    NE,
                    f"alignment_ready={align_ready}, trades_used={trades}",
                    f"trades>={TRVAL_PAPER_WARMUP_TRADES} then True",
                    "백테스트 대비 정렬비교 준비 상태(워밍업)",
                    "표본 확장 후 정렬비교 재검증",
                    required=False,
                )
            )
        else:
            items.append(
                _make_item(
                    "paper_bt_alignment",
                    PASS if align_ready else FAIL,
                    align_metric,
                    "True",
                    "백테스트 대비 정렬비교 준비 상태",
                    align_action,
                    required=False,
                )
            )
            if relaxed_alignment_used:
                items.append(
                    _make_item(
                        "paper_bt_alignment_live_transition",
                        NE if relaxed_alignment_insufficient else PASS,
                        f"bt_n={align_bt_n}, strict_min_shared={strict_min_shared}, relaxed_used=True",
                        f"bt_n>={strict_min_shared}",
                        "백테스트 정렬 비교(실전 전환 기준)",
                        "3월 이후 백테스트 표본 10건 이상 확보 또는 전환정책 재검토",
                        required=False,
                    )
                )
    else:
        items.append(_make_item("paper_bt_alignment", NE, "alignment_ready=-", "True", "비교 피드백 파일 없음", "run_live_vs_bt_paper_daily.bat 실행", required=False))

    abs_diff = _safe_float(((fb.get("divergence") or {}).get("abs_diff")) if fb else float("nan"))
    if _is_finite(abs_diff):
        items.append(
            _make_item(
                "paper_return_divergence",
                PASS if abs_diff <= TRVAL_PAPER_RET_DIVERGENCE_MAX else FAIL,
                f"abs_diff={_fmt_pct(abs_diff)}",
                f"<={TRVAL_PAPER_RET_DIVERGENCE_MAX * 100.0:.2f}%",
                "가상매매 vs 백테스트 수익률 괴리",
                "전략/비용/체결 가정 재보정",
                required=False,
            )
        )
    else:
        items.append(
            _make_item(
                "paper_return_divergence",
                NE,
                "abs_diff=-",
                f"<={TRVAL_PAPER_RET_DIVERGENCE_MAX * 100.0:.2f}%",
                "괴리 계산 데이터 부족",
                "비교 표본 확장",
                required=False,
            )
        )

    # Quality gate should reflect OOS quality metrics, not alignment readiness.
    optimize_obj = _as_dict(fb.get("optimize")) if fb else {}
    quality_gate_ok = bool(((optimize_obj.get("quality_gate") or {}).get("ok"))) if fb else False
    optimize_gate_ok = quality_gate_ok if fb else False
    if fb:
        if trades < TRVAL_PAPER_WARMUP_TRADES:
            items.append(
                _make_item(
                    "paper_quality_gate",
                    NE,
                    f"optimize_gate_ok={optimize_gate_ok}, trades_used={trades}",
                    f"trades>={TRVAL_PAPER_WARMUP_TRADES} then True",
                    "가상매매 품질 게이트(워밍업)",
                    "표본 확장 후 품질게이트 재검증",
                    required=False,
                )
            )
        else:
            items.append(
                _make_item(
                    "paper_quality_gate",
                    PASS if optimize_gate_ok else FAIL,
                    f"optimize_gate_ok={optimize_gate_ok}",
                    "True",
                    "가상매매 품질 게이트",
                    "oos 품질 기준 재검토/전략 보정",
                    required=False,
                )
            )
    else:
        items.append(_make_item("paper_quality_gate", NE, "optimize_gate_ok=-", "True", "품질 게이트 데이터 없음", "live_vs_bt_feedback 생성", required=False))

    pending_queue_len = _safe_int(pending.get("pending_queue_len", 0)) if pending else 0
    pending_signal_rows = _safe_int(pending.get("pending_signal_rows", 0)) if pending else 0
    pending_queue_len_raw = _safe_int(pending.get("pending_queue_len_raw", pending_queue_len), pending_queue_len) if pending else 0
    pending_signal_rows_raw = _safe_int(pending.get("pending_signal_rows_raw", pending_signal_rows), pending_signal_rows) if pending else 0
    pending_queue_len_eff = max(pending_queue_len, pending_queue_len_raw)
    pending_signal_rows_eff = max(pending_signal_rows, pending_signal_rows_raw)
    pending_as_of = _normalize_ymd(pending.get("as_of")) if pending else ""
    pending_signal_rows_meta = _read_pending_signal_rows(p_pending_signals)
    pending_signal_dates = [str(x.get("signal_date") or "") for x in pending_signal_rows_meta if str(x.get("signal_date") or "")]
    pending_as_of_date = _ymd_to_date(pending_as_of)
    older_pending_rows = sum(
        1
        for ymd in pending_signal_dates
        if pending_as_of_date is not None and (_ymd_to_date(ymd) is not None and _ymd_to_date(ymd) < pending_as_of_date)
    )
    lifecycle_status = str(((pending.get("entry_exit_lifecycle") or {}).get("status")) or "-") if pending else "-"
    state_machine_status = str(((pending.get("state_machine_summary") or {}).get("status")) or "-") if pending else "-"
    carry_reason_counts = _as_dict(pending.get("carry_reason_counts")) if pending else {}
    carry_reason_text = ", ".join(f"{k}={v}" for k, v in sorted(carry_reason_counts.items())) or "-"
    tolerated_same_day_no_next = bool(
        pending
        and pending_queue_len_eff > 0
        and pending_signal_rows_eff == pending_queue_len_eff
        and older_pending_rows == 0
        and bool(pending.get("deferred_all_no_next_day"))
        and _safe_int(pending.get("no_next_day", 0)) == pending_signal_rows_eff
    )
    allowed_retry_reasons = {
        "NO_NEXT_DAY",
        "ENTRY_DAY_WAIT",
        "SAME_DAY_PRICE_MISSING",
        "CLOSE_AUCTION_UNFILLED",
        "CLOSE_CUTOFF_RECHECK",
        "NEXT_OPEN_UNFILLED",
        "SPLIT_ENTRY_2ND",
    }
    tolerated_retry_pending = bool(
        pending
        and pending_queue_len_eff > 0
        and pending_signal_rows_eff == pending_queue_len_eff
        and older_pending_rows == 0
        and pending_signal_rows_meta
        and all(str(x.get("carry_origin_reason") or "").strip().upper() in allowed_retry_reasons for x in pending_signal_rows_meta)
    )
    tolerated_pending_queue = bool(tolerated_same_day_no_next or tolerated_retry_pending)
    if pending:
        items.append(
            _make_item(
                "paper_pending_queue_clear",
                PASS if (pending_queue_len_eff == 0 and pending_signal_rows_eff == 0) or tolerated_pending_queue else FAIL,
                f"pending_queue_len={pending_queue_len_eff}, pending_signal_rows={pending_signal_rows_eff}, older_pending_rows={older_pending_rows}, carry_reason_counts={carry_reason_text}",
                "pending_queue_len=0 and pending_signal_rows=0 (당일 정상 재시도 대기 예외)",
                "미해결 진입 대기 신호",
                "pending/carryover 원인 해소 후 재검증",
                required=TRVAL_REQUIRE_PENDING_CLEAR,
            )
        )
        items.append(
            _make_item(
                "paper_lifecycle_ready",
                PASS if (lifecycle_status == PASS and state_machine_status == PASS) or (tolerated_pending_queue and lifecycle_status == "CAUTION" and state_machine_status == "CAUTION") else FAIL,
                f"entry_exit_lifecycle={lifecycle_status}, state_machine={state_machine_status}, tolerated_pending_queue={tolerated_pending_queue}",
                "PASS / PASS (당일 정상 재시도 대기 CAUTION 예외)",
                "진입·상태머신 lifecycle 안정성",
                "pending 상태 및 lifecycle 경고 해소",
            )
        )
    else:
        items.append(_make_item("paper_pending_queue_clear", NE, "pending_queue_len=-", "0", "pending 상태 파일 없음", "paper_engine.py 재실행", required=TRVAL_REQUIRE_PENDING_CLEAR))
        items.append(_make_item("paper_lifecycle_ready", NE, "lifecycle=-", "PASS / PASS", "lifecycle 상태 파일 없음", "paper_engine.py 재실행"))

    judgment, counts = _judge(items, stage="paper")
    key_actions = [x["action"] for x in items if x["status"] in {FAIL, NE}][:4]

    return {
        "stage": "paper",
        "title": "2단계 가상매매 검증",
        "judgment": judgment,
        "counts": counts,
        "items": items,
        "key_actions": key_actions,
        "sources": {
            "paper_pnl_summary": str(p_pnl),
            "live_vs_bt_feedback": str(p_fb),
            "pending_entry_status": str(p_pending),
            "pending_entry_signals": str(p_pending_signals),
            "after_close_summary_last": str(LOG_DIR / "after_close_summary_last.json"),
            "p0_daily_check_latest": str(_latest_by_glob("p0_daily_check_*.json") or ""),
            "gate_daily_latest": str(_latest_by_glob("gate_daily_*.json") or ""),
            "risk_state_fresh_days": RISK_STATE_FRESH_DAYS,
            "risk_state_inputs": risk_state.get("inputs", []),
            "operational_scope": oper_scope,
            "alignment_snapshot": alignment_snapshot,
        },
    }


def _build_live_stage() -> Dict[str, Any]:
    p_e2e = LOG_DIR / "kis_intraday_e2e_latest.json"
    p_fault = LOG_DIR / "kis_fault_injection_latest.json"
    p_canary = LOG_DIR / "kis_live_canary_first_latest.json"
    p_realtime_ws = LOG_DIR / "kis_realtime_status_latest.json"
    p_ws_direct = _ws_status_path()
    p_ws = p_ws_direct if p_ws_direct.exists() else p_realtime_ws

    e2e = _read_json(p_e2e, fail_on_parse=p_e2e.exists())
    fault = _read_json(p_fault, fail_on_parse=p_fault.exists())
    canary = _read_json(p_canary, fail_on_parse=p_canary.exists())
    ws = _read_json(p_ws, fail_on_parse=p_ws.exists())
    if ws and "ws" not in ws and ("status" in ws or "ts" in ws):
        ws = {"ws": ws}
    e2e_runs = _as_list(e2e.get("runs"))
    e2e_last_run = e2e_runs[-1] if e2e_runs and isinstance(e2e_runs[-1], dict) else {}
    e2e_steps = _as_list(e2e_last_run.get("steps"))
    canary_steps = _as_list(canary.get("steps"))
    fault_results = _as_list(fault.get("results"))

    e2e_snapshot = {
        "generated_at": e2e.get("generated_at"),
        "mode": e2e.get("mode"),
        "mock": e2e.get("mock"),
        "iterations_done": e2e.get("iterations_done"),
        "ok": e2e.get("ok"),
        "pass_n": e2e.get("pass_n"),
        "fail_n": e2e.get("fail_n"),
        "steps": [
            {
                "name": step.get("name"),
                "ok": step.get("ok"),
                "returncode": step.get("returncode"),
                "duration_sec": step.get("duration_sec"),
                "stdout_tail": step.get("stdout_tail"),
                "stderr_tail": step.get("stderr_tail"),
                "error": step.get("error"),
            }
            for step in e2e_steps
            if isinstance(step, dict)
        ],
    }
    canary_snapshot = {
        "generated_at": canary.get("generated_at"),
        "mode": canary.get("mode"),
        "mock": canary.get("mock"),
        "execute": canary.get("execute"),
        "ok": canary.get("ok"),
        "steps": [
            {
                "name": step.get("name"),
                "ok": step.get("ok"),
                "returncode": step.get("returncode"),
                "duration_sec": step.get("duration_sec"),
                "stdout_tail": step.get("stdout_tail"),
                "stderr_tail": step.get("stderr_tail"),
                "error": step.get("error"),
            }
            for step in canary_steps
            if isinstance(step, dict)
        ],
    }
    fault_snapshot = {
        "generated_at": fault.get("generated_at"),
        "ok": fault.get("ok"),
        "pass_n": fault.get("pass_n"),
        "fail_n": fault.get("fail_n"),
        "results": [
            {
                "name": row.get("name"),
                "passed": row.get("passed"),
                "expected": row.get("expected"),
                "actual": row.get("actual"),
                "detail": row.get("detail"),
            }
            for row in fault_results
            if isinstance(row, dict)
        ],
    }

    items: List[Dict[str, Any]] = []

    age_e2e = _age_days(p_e2e)
    if _is_finite(age_e2e):
        items.append(
            _make_item(
                "live_e2e_freshness",
                PASS if age_e2e <= TRVAL_LIVE_E2E_FRESH_MAX_DAYS else FAIL,
                f"age_days={_fmt_num(age_e2e)}",
                f"<={TRVAL_LIVE_E2E_FRESH_MAX_DAYS:g}",
                "실전 E2E 최신성",
                "run_kis_intraday_e2e.bat 재실행",
            )
        )
    else:
        items.append(_make_item("live_e2e_freshness", NE, "age_days=-", f"<={TRVAL_LIVE_E2E_FRESH_MAX_DAYS:g}", "E2E 결과 없음", "run_kis_intraday_e2e.bat 실행"))

    age_ws = _age_days(p_ws)
    ws_status = str((ws or {}).get("ws", {}).get("status", "")).upper()
    ws_ok_statuses = {"OK", "DONE", "CONNECTED", "STREAMING"}
    ws_fresh_ok = _is_finite(age_ws) and age_ws <= TRVAL_LIVE_E2E_FRESH_MAX_DAYS
    ws_ready = bool(ws_fresh_ok and ws_status in ws_ok_statuses)
    if ws:
        items.append(
            _make_item(
                "live_ws_status",
                PASS if ws_ready else FAIL,
                f"status={ws_status or '-'}, age_days={_fmt_num(age_ws) if _is_finite(age_ws) else '-'}",
                f"status in {sorted(ws_ok_statuses)} and age<={TRVAL_LIVE_E2E_FRESH_MAX_DAYS:g}",
                "실시간 WS 상태/신선도",
                "kis_realtime_ws 재실행 및 KIS 인증/연결 복구",
            )
        )
    else:
        items.append(
            _make_item(
                "live_ws_status",
                NE,
                "status=-",
                f"status in {sorted(ws_ok_statuses)} and age<={TRVAL_LIVE_E2E_FRESH_MAX_DAYS:g}",
                "실시간 WS 상태 파일 없음",
                "kis_status_monitor 또는 kis_realtime_ws 실행",
            )
        )

    if e2e:
        ok = bool(e2e.get("ok", False))
        items.append(
            _make_item(
                "live_e2e_ok",
                PASS if ok else FAIL,
                f"ok={ok}, pass_n={e2e.get('pass_n','-')}, fail_n={e2e.get('fail_n','-')}",
                "ok=True",
                "장중 E2E 시나리오",
                "E2E 실패 단계 원인 수정",
            )
        )
    else:
        items.append(_make_item("live_e2e_ok", NE, "ok=-", "ok=True", "E2E 결과 없음", "run_kis_intraday_e2e.bat 실행"))

    if fault:
        ok = bool(fault.get("ok", False))
        items.append(
            _make_item(
                "live_fault_injection",
                PASS if ok else FAIL,
                f"ok={ok}, pass_n={fault.get('pass_n','-')}, fail_n={fault.get('fail_n','-')}",
                "ok=True",
                "장애주입 시나리오",
                "fault 시나리오 실패 케이스 수정",
            )
        )
    else:
        items.append(_make_item("live_fault_injection", NE, "ok=-", "ok=True", "fault 결과 없음", "run_kis_fault_injection.bat 실행"))

    if canary:
        ok = bool(canary.get("ok", False))
        items.append(
            _make_item(
                "live_canary_gate",
                PASS if ok else FAIL,
                f"ok={ok}, mode={canary.get('mode','-')}, mock={canary.get('mock','-')}",
                "ok=True",
                "소액 실전 canary 게이트",
                "canary step 실패 항목 수정",
            )
        )

        execute = canary.get("execute", None)
        items.append(
            _make_item(
                "canary_execute_mode",
                PASS if execute is True else NE,
                f"execute={execute}",
                "True",
                "실주문 실행 여부",
                "실주문 전환 시 CANARY_EXECUTE=1 설정",
                required=False,
            )
        )

        is_mock = str(canary.get("mock", "")).lower() == "true"

        pre_prod = next((x for x in _as_list(canary.get("steps")) if isinstance(x, dict) and str(x.get("name")) == "preflight_healthcheck_prod"), None)
        if isinstance(pre_prod, dict):
            pre_ok_prod = bool(pre_prod.get("ok", False))
            items.append(
                _make_item(
                    "live_preflight_health_prod",
                    PASS if pre_ok_prod else FAIL,
                    f"ok={pre_ok_prod}, returncode={pre_prod.get('returncode','-')}",
                    "ok=True",
                    "실전(PROD) 상태 체크",
                    "시장데이터 조회를 위한 실전 API 환경",
                )
            )
        else:
            items.append(_make_item("live_preflight_health_prod", NE, "ok=-", "ok=True", "canary preflight(prod) 기록 없음", "canary 재실행"))

        if is_mock:
            pre_mock = next((x for x in _as_list(canary.get("steps")) if isinstance(x, dict) and str(x.get("name")) == "preflight_healthcheck_mock"), None)
            if isinstance(pre_mock, dict):
                pre_ok_mock = bool(pre_mock.get("ok", False))
                items.append(
                    _make_item(
                        "live_preflight_health_mock",
                        PASS if pre_ok_mock else FAIL,
                        f"ok={pre_ok_mock}, returncode={pre_mock.get('returncode','-')}",
                        "ok=True",
                        "모의(MOCK) 상태 체크",
                        "주문 체결을 위한 모의 API 환경",
                    )
                )
            else:
                items.append(_make_item("live_preflight_health_mock", NE, "ok=-", "ok=True", "canary preflight(mock) 기록 없음", "canary 재실행"))
        else:
            items.append(_make_item("live_preflight_health_mock", NE, "ok=NA", "ok=NA", "실전 모드 (모의 검사 불필요)", "canary 재실행", required=False))
    else:
        items.append(_make_item("live_canary_gate", NE, "ok=-", "ok=True", "canary 결과 없음", "run_live_canary_first_test.bat 실행"))
        items.append(_make_item("canary_execute_mode", NE, "execute=-", "True", "실주문 실행 여부 확인 불가", "canary 결과 생성", required=False))
        items.append(_make_item("live_preflight_health_prod", NE, "ok=-", "ok=True", "preflight(prod) 기록 없음", "canary 재실행"))
        items.append(_make_item("live_preflight_health_mock", NE, "ok=-", "ok=True", "preflight(mock) 기록 없음", "canary 재실행", required=False))

    judgment, counts = _judge(items, stage="live")
    key_actions = [x["action"] for x in items if x["status"] in {FAIL, NE}][:4]

    return {
        "stage": "live",
        "title": "3단계 실전매매 검증",
        "judgment": judgment,
        "counts": counts,
        "items": items,
        "key_actions": key_actions,
        "sources": {
            "e2e": str(p_e2e),
            "fault": str(p_fault),
            "canary": str(p_canary),
            "ws": str(p_ws),
            "ws_realtime_status": str(p_realtime_ws),
            "ws_direct_status": str(p_ws_direct),
            "e2e_snapshot": e2e_snapshot,
            "canary_snapshot": canary_snapshot,
            "fault_snapshot": fault_snapshot,
            "ws_snapshot": ws,
        },
    }


def _item_status(stage: Dict[str, Any], name: str, default: str = NE) -> str:
    for it in (stage.get("items") or []):
        if str(it.get("name")) == name:
            return str(it.get("status") or default)
    return default


def _item_detail(stage: Dict[str, Any], name: str) -> Dict[str, Any]:
    for it in (stage.get("items") or []):
        if str(it.get("name")) == name:
            return it if isinstance(it, dict) else {}
    return {}


def _read_pending_signal_dates(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                ymd = _normalize_ymd(row.get("signal_date"))
                if ymd:
                    out.append(ymd)
    except Exception:
        return []
    return out


def _read_pending_signal_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    out: List[Dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                signal_date = _normalize_ymd(row.get("signal_date"))
                if not signal_date:
                    continue
                out.append(
                    {
                        "signal_date": signal_date,
                        "carry_origin_reason": str(row.get("carry_origin_reason") or "").strip().upper(),
                        "entry_day_override": _normalize_ymd(row.get("entry_day_override")),
                    }
                )
    except Exception:
        return []
    return out


def _classify_transition_blocker(
    name: str,
    paper: Dict[str, Any],
    live: Dict[str, Any],
    req_align: bool,
    req_quality: bool,
    req_divergence: bool,
    req_execute_effective: bool,
    warmup_active: bool,
) -> Dict[str, Any]:
    source_stage = paper if name.startswith("paper_") or name == "paper_judgment_operational" else live
    source_item = _item_detail(source_stage, name)
    source_status = str(source_item.get("status", _item_status(source_stage, name)))
    metric = str(source_item.get("metric", "-"))
    issue = str(source_item.get("issue", "-"))
    action = str(source_item.get("action", "-"))

    blocker_type = "validation_fail"
    label = "실제실패"
    summary = "실제 기준 미충족"

    if name in {"paper_bt_alignment", "paper_bt_alignment_live_transition", "paper_quality_gate", "paper_return_divergence"} and source_status == NE and warmup_active:
        blocker_type = "warmup"
        label = "워밍업"
        summary = "표본/정렬 준비 부족으로 아직 판정 확정 불가"
    elif name == "canary_execute_mode" and req_execute_effective and source_status == NE:
        blocker_type = "policy_lock"
        label = "정책잠금"
        summary = "실주문 실행 플래그가 꺼져 있어 엄격 전환정책상 차단"
    elif source_status == NE:
        blocker_type = "data_gap"
        label = "데이터부족"
        summary = "필수 근거 데이터 부족으로 판정 불가"

    return {
        "name": name,
        "blocker_type": blocker_type,
        "blocker_label": label,
        "summary": summary,
        "source_status": source_status,
        "issue": issue,
        "metric": metric,
        "action": action,
        "required_by_policy": (
            (name == "paper_bt_alignment" and req_align)
            or (name == "paper_bt_alignment_live_transition" and req_align)
            or (name == "paper_quality_gate" and req_quality)
            or (name == "paper_return_divergence" and req_divergence)
            or (name == "canary_execute_mode" and req_execute_effective)
            or name not in {"paper_bt_alignment", "paper_bt_alignment_live_transition", "paper_quality_gate", "paper_return_divergence", "canary_execute_mode"}
        ),
    }


def _build_transition_gate(paper: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    req_align = _env_or_cfg_bool("TRVAL_TRANSITION_REQUIRE_ALIGNMENT", "policy.require_alignment", True)
    req_quality = _env_or_cfg_bool("TRVAL_TRANSITION_REQUIRE_QUALITY", "policy.require_quality_gate", True)
    req_divergence = _env_or_cfg_bool(
        "TRVAL_TRANSITION_REQUIRE_RET_DIVERGENCE",
        "policy.require_return_divergence",
        TRVAL_TRANSITION_REQUIRE_RET_DIVERGENCE,
    )
    req_execute = _env_or_cfg_bool("TRVAL_TRANSITION_REQUIRE_CANARY_EXECUTE", "policy.require_canary_execute", True)

    checks: List[Dict[str, Any]] = []
    paper_items = {str(x.get("name")): x for x in (paper.get("items") or []) if isinstance(x, dict)}
    paper_trade_metric = str((paper_items.get("paper_trade_count") or {}).get("metric", ""))
    m = re.search(r"trades_used=(\d+)", paper_trade_metric)
    paper_trades = int(m.group(1)) if m else 0
    warmup_active = paper_trades < TRVAL_PAPER_WARMUP_TRADES
    align_required_effective = bool(req_align and (not warmup_active))
    quality_required_effective = bool(req_quality and (not warmup_active))
    divergence_required_effective = bool(req_divergence and (not warmup_active))
    live_j = str(live.get("judgment", "보류"))
    # [2026-09-09 사용자 결정] **WARMUP 에서는 카나리아 실주문을 필수로 하지 않는다.**
    #   이전에는 req_execute 만 warmup 완화 대상에서 빠져 있었다. 그래서 표본이 모자란
    #   단계에서도 실주문 카나리아가 없으면 게이트가 policy_lock 으로 막혔다.
    #   결정의 취지는 게이트를 없애자는 것이 아니라 **단계를 분리하자**는 것이다:
    #     WARMUP   표본을 모으는 단계. align/quality/divergence 와 함께 카나리아도 완화
    #     CANARY   표본이 찼고, 이제 실주문 카나리아를 요구하는 단계
    #     NORMAL   전부 통과. 실전 전환 가능
    #   아래 transition_stage 가 그 셋을 산출물에 명시한다. 완화는 WARMUP 에서만이고
    #   워밍업을 벗어나면 카나리아는 그대로 필수다.
    execute_required_for_gate = bool(req_execute and (not warmup_active))

    paper_j = str(paper.get("judgment", "보류"))
    checks.append(
        _make_item(
            "paper_judgment_operational",
            PASS if paper_j == "운영가능" else FAIL,
            f"paper_judgment={paper_j}",
            "운영가능",
            "가상매매 단계 판정",
            "paper 게이트 항목 보강",
        )
    )

    def add_gate(
        name: str,
        stage: Dict[str, Any],
        issue: str,
        action: str,
        required: bool = True,
        passthrough_status_when_optional: bool = False,
    ) -> None:
        st = _item_status(stage, name)
        if not required:
            gate_status = st if passthrough_status_when_optional else PASS
        else:
            gate_status = PASS if st == PASS else FAIL
        checks.append(
            _make_item(
                name,
                gate_status,
                f"status={st}",
                "PASS",
                issue,
                action,
                required=required,
            )
        )

    add_gate("paper_freshness", paper, "가상매매 최신성", "run_paper_daily.bat 재실행")
    add_gate("paper_trade_count", paper, "가상매매 표본 거래 수", f"표본 {TRVAL_PAPER_MIN_TRADES}회 이상 누적")
    add_gate(
        "paper_mdd_live_transition",
        paper,
        "가상매매 최대 낙폭(실전 전환 기준)",
        "DDM/사이징/손절 정책 재검증",
        required=not warmup_active,
        passthrough_status_when_optional=True,
    )
    add_gate("paper_risk_off_state", paper, "리스크오프 상태", "kill-switch 원인 해소 후 재검증")
    if req_align:
        add_gate(
            "paper_bt_alignment_live_transition" if "paper_bt_alignment_live_transition" in paper_items else "paper_bt_alignment",
            paper,
            "백테스트 정렬 비교",
            "run_live_vs_bt_paper_daily.bat 재실행",
            required=align_required_effective,
            passthrough_status_when_optional=True,
        )
    if req_quality:
        add_gate(
            "paper_quality_gate",
            paper,
            "가상매매 품질 게이트",
            "oos 품질 기준 보강",
            required=quality_required_effective,
            # [2026-09-09] False -> True. 완화된 게이트를 PASS 로 덮으면 화면에는
            #   "판정한 적 없는 항목"이 초록으로 보인다. 나머지 3개(trade_count/alignment/
            #   canary_execute)는 이미 실제 상태를 그대로 보여준다. 여기만 달랐다.
            #   required 가 아닌 항목은 ready/blockers 집계에 들어가지 않으므로 판정은 불변이고,
            #   바뀌는 것은 **무엇이 아직 판정되지 않았는지 보이는가** 뿐이다.
            passthrough_status_when_optional=True,
        )
    if req_divergence:
        add_gate(
            "paper_return_divergence",
            paper,
            "가상매매 vs 백테스트 괴리",
            "비교 표본/체결 가정 재검증",
            required=divergence_required_effective,
            passthrough_status_when_optional=True,  # [2026-09-09] 위와 같은 이유
        )

    add_gate("live_canary_gate", live, "canary 게이트", "run_live_canary_first_test.bat 재실행")
    add_gate("live_ws_status", live, "실시간 WS 상태/신선도", "kis_realtime_ws 재실행 및 KIS 인증/연결 복구")
    add_gate("live_preflight_health_prod", live, "실전(PROD) 상태 체크", "시장데이터 조회를 위한 실전 API 환경")
    add_gate("live_preflight_health_mock", live, "모의(MOCK) 상태 체크", "주문 체결을 위한 모의 API 환경", required=False)
    add_gate(
        "canary_execute_mode",
        live,
        "실주문 실행 여부",
        "실전 전환 시 CANARY_EXECUTE=1 설정",
        required=execute_required_for_gate,
        passthrough_status_when_optional=True,
    )

    req = [x for x in checks if bool(x.get("required", True))]
    pass_n = sum(1 for x in req if x.get("status") == PASS)
    fail_n = sum(1 for x in req if x.get("status") == FAIL)
    total = len(req)

    ready = fail_n == 0
    blockers = [str(x.get("name")) for x in req if x.get("status") != PASS]
    blocker_details = [
        _classify_transition_blocker(
            name,
            paper,
            live,
            req_align=align_required_effective,
            req_quality=quality_required_effective,
            req_divergence=divergence_required_effective,
            req_execute_effective=execute_required_for_gate,
            warmup_active=warmup_active,
        )
        for name in blockers
    ]
    blocker_summary = {
        "warmup_n": sum(1 for x in blocker_details if x.get("blocker_type") == "warmup"),
        "policy_lock_n": sum(1 for x in blocker_details if x.get("blocker_type") == "policy_lock"),
        "validation_fail_n": sum(1 for x in blocker_details if x.get("blocker_type") == "validation_fail"),
        "data_gap_n": sum(1 for x in blocker_details if x.get("blocker_type") == "data_gap"),
    }

    next_step = "go_live"
    if not ready:
        if any(str(b).startswith("paper_") for b in blockers) or "paper_judgment_operational" in blockers:
            next_step = "paper_fix"
        elif "canary_execute_mode" in blockers:
            next_step = "live_canary"
        else:
            next_step = "live_fix"
    elif live_j == "실주문대기":
        next_step = "live_canary"

    # [2026-09-09 사용자 결정] WARMUP 단계에서 next_step 이 go_live 로 나오면 안 된다.
    #   워밍업은 완화가 걸린 단계라 "막는 것이 없다"(ready)가 곧 "가도 된다"가 아니다.
    #   표본을 채우는 것이 다음 할 일이고, 그 다음이 CANARY, 그 다음이 정상운영이다.
    #   상태를 분리하기로 한 이상 다음 걸음도 단계를 따라가야 한다.
    if warmup_active and ready:
        next_step = "warmup_accumulate"

    # [2026-09-09 사용자 결정] 상태를 WARMUP -> CANARY -> NORMAL 로 **명시한다.**
    #   이전에는 이 구분이 policy_profile 문자열 안에만 암묵적으로 있었고, 읽는 쪽은
    #   "지금 어느 단계인가" 를 그 문자열을 해석해서 알아내야 했다.
    #   WARMUP  표본 부족(paper_trades < TRVAL_PAPER_WARMUP_TRADES). 완화가 적용되는 유일한 단계
    #   CANARY  워밍업을 벗어났고 실주문 카나리아가 아직 확인되지 않았다
    #   NORMAL  전부 통과. 실전 전환 가능
    canary_item = next((c for c in checks if str(c.get("name")) == "canary_execute_mode"), None)
    canary_ok = bool(canary_item and str(canary_item.get("status")) == "PASS")
    if warmup_active:
        transition_stage = "WARMUP"
    elif ready:
        transition_stage = "NORMAL"
    else:
        transition_stage = "CANARY" if not canary_ok else "NORMAL"

    return {
        "paper_to_live": {
            "status": "PASS" if ready else "HOLD",
            "ready": bool(ready),
            "transition_stage": transition_stage,
            "warmup_active": bool(warmup_active),
            "paper_trades": int(paper_trades),
            "warmup_threshold": int(TRVAL_PAPER_WARMUP_TRADES),
            "next_step": next_step,
            "counts": {"total": total, "pass_n": pass_n, "fail_n": fail_n},
            "blockers": blockers,
            "blocker_details": blocker_details,
            "blocker_summary": blocker_summary,
            "policy_profile": (
                "WARMUP_RELAXED_TO_50_CANARY_REQUIRED"
                if (warmup_active and execute_required_for_gate)
                else "WARMUP_RELAXED_TO_50_CANARY_OPTIONAL"
                if warmup_active
                else "CANARY_REQUIRED"
                if execute_required_for_gate
                else "CANARY_OPTIONAL"
            ),
            "policy": {
                "require_alignment": align_required_effective,
                "require_quality_gate": quality_required_effective,
                "require_return_divergence": divergence_required_effective,
                "require_canary_execute": execute_required_for_gate,
                "warmup_trades": TRVAL_PAPER_WARMUP_TRADES,
                "paper_min_trades": TRVAL_PAPER_MIN_TRADES,
                "paper_fresh_max_days": TRVAL_PAPER_FRESH_MAX_DAYS,
                "paper_reference_max_drawdown_pct": TRVAL_PAPER_MAX_DD_PCT,
                "paper_live_max_drawdown_pct": TRVAL_PAPER_LIVE_MAX_DD_PCT,
            },
            "policy_notes": [
                (
                    f"paper_bt_alignment은 표본 {TRVAL_PAPER_WARMUP_TRADES}회 미만에서는 참고 항목으로 두고, "
                    "이후부터 실전 전환 필수항목으로 적용"
                ),
                (
                    f"paper_quality_gate는 표본 {TRVAL_PAPER_WARMUP_TRADES}회 미만에서는 참고 항목으로 두고, "
                    "이후부터 최종 품질 게이트로 적용"
                ),
                (
                    f"paper_return_divergence는 표본 {TRVAL_PAPER_WARMUP_TRADES}회 미만에서는 참고 항목으로 두고, "
                    "이후부터 전환 차단 기준으로 적용"
                ),
                "canary_execute_mode는 실전 전환 필수 항목이며 미충족 시 차단",
            ],
            "checks": checks,
        }
    }


def _overall(paper: Dict[str, Any], live: Dict[str, Any], transition_gate: Dict[str, Any]) -> Dict[str, Any]:
    pj = str(paper.get("judgment", "보류"))
    lj = str(live.get("judgment", "보류"))
    p2l = ((transition_gate.get("paper_to_live") or {}) if transition_gate else {})
    p2l_ready = bool(p2l.get("ready", False))
    p2l_next = str(p2l.get("next_step", "-"))

    if pj == "보류":
        return {"judgment": "가상매매 보류", "message": "가상매매 게이트 미통과. 실전 전환 불가", "next_step": "paper_fix"}
    if pj in {"조건부"}:
        return {"judgment": "가상매매 보강", "message": "가상매매 조건부 상태. 부족 항목 보강 필요", "next_step": "paper_recheck"}
    if lj in {"보류"}:
        return {"judgment": "실전준비 보류", "message": "실전 게이트 미통과. canary/e2e/fault 보강 필요", "next_step": "live_fix"}
    if lj in {"실주문대기"}:
        return {"judgment": "실전준비 조건부", "message": "가상매매 운영 중. 실주문 실행 canary 확인 대기", "next_step": "live_canary"}
    if not p2l_ready:
        if p2l_next == "paper_fix":
            return {"judgment": "가상매매 보강", "message": "Paper→Live 전환 기준 미충족. 가상매매 보강 필요", "next_step": "paper_fix"}
        if p2l_next == "live_canary":
            return {"judgment": "실전준비 조건부", "message": "실전 실행 전 canary 실행모드 확인 필요", "next_step": "live_canary"}
        return {"judgment": "실전준비 보류", "message": "Paper→Live 전환 기준 미충족", "next_step": "live_fix"}

    return {"judgment": "실전환진행", "message": "전체 신호·소스 체인 정상, 실전 진행 기준 충족", "next_step": "go_live"}


def build_report() -> Dict[str, Any]:
    paper = _build_paper_stage()
    live = _build_live_stage()
    transition_gate = _build_transition_gate(paper, live)
    overall = _overall(paper, live, transition_gate)
    op_scope = (((paper.get("sources") or {}).get("operational_scope")) or {})
    return {
        "generated_at": _now_kst().strftime("%Y-%m-%d %H:%M:%S%z"),
        "meta": {"operational_scope": op_scope},
        "paper": paper,
        "live": live,
        "transition_gate": transition_gate,
        "overall": overall,
    }


def _render_md(rep: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# 가상/실전 검증 리포트 ({rep.get('generated_at')})")
    lines.append("")
    ov = rep.get("overall", {})
    lines.append(f"- overall: **{ov.get('judgment','-')}**")
    lines.append(f"- message: {ov.get('message','-')}")
    lines.append(f"- next_step: `{ov.get('next_step','-')}`")
    m_scope = (((rep.get("meta") or {}).get("operational_scope")) or {})
    if m_scope:
        lines.append(
            f"- operational_scope: start_ymd={m_scope.get('start_ymd','-')}, "
            f"pnl_start={m_scope.get('pnl_scope_start_ymd','-')}, "
            f"feedback_start={m_scope.get('feedback_oper_start_ymd','-')}, "
            f"mismatch={m_scope.get('scope_mismatch','-')}"
        )
    p2l = (((rep.get("transition_gate") or {}).get("paper_to_live")) or {})
    if p2l:
        c = (p2l.get("counts") or {})
        lines.append(
            f"- paper_to_live: status={p2l.get('status','-')}, ready={p2l.get('ready','-')}, "
            f"next_step={p2l.get('next_step','-')}, pass={c.get('pass_n',0)}/{c.get('total',0)}"
        )

    for key in ("paper", "live"):
        st = rep.get(key, {})
        cnt = st.get("counts", {})
        lines.append("")
        lines.append(f"## {st.get('title', key)}")
        lines.append("")
        lines.append(f"- judgment: **{st.get('judgment','-')}**")
        lines.append(
            f"- required checks: total={cnt.get('total',0)}, pass={cnt.get('pass_n',0)}, fail={cnt.get('fail_n',0)}, ne={cnt.get('not_evaluable_n',0)}"
        )
        lines.append("")
        lines.append("| 항목 | 상태 | 지표 | 기준 | 이슈 | 조치 |")
        lines.append("|---|---|---|---|---|---|")
        for it in st.get("items", []):
            lines.append(
                f"| {it.get('name')} | {it.get('status')} | {it.get('metric')} | {it.get('threshold')} | {it.get('issue')} | {it.get('action')} |"
            )
        acts = st.get("key_actions", []) or []
        if acts:
            lines.append("")
            lines.append("- 우선 조치")
            for a in acts:
                lines.append(f"  - {a}")

    return "\n".join(lines) + "\n"


def _status_class(status: str) -> str:
    s = str(status)
    if s == PASS:
        return "pass"
    if s == FAIL:
        return "fail"
    return "warn"


def _render_stage_table(stage: Dict[str, Any]) -> str:
    rows = []
    for it in stage.get("items", []):
        cls = _status_class(str(it.get("status", NE)))
        rows.append(
            "<tr>"
            f"<td>{it.get('name','-')}</td>"
            f"<td><span class='chip chip-{cls}'>{it.get('status','-')}</span></td>"
            f"<td>{it.get('metric','-')}</td>"
            f"<td>{it.get('threshold','-')}</td>"
            f"<td>{it.get('issue','-')}</td>"
            f"<td>{it.get('action','-')}</td>"
            "</tr>"
        )
    cnt = stage.get("counts", {})
    return (
        "<section class='card'>"
        f"<h2>{stage.get('title','-')}</h2>"
        f"<p class='judge'>판정: <b>{stage.get('judgment','-')}</b> | total={cnt.get('total',0)} pass={cnt.get('pass_n',0)} fail={cnt.get('fail_n',0)} ne={cnt.get('not_evaluable_n',0)}</p>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>항목</th><th>상태</th><th>지표</th><th>기준</th><th>이슈</th><th>조치</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_html(rep: Dict[str, Any]) -> str:
    ov = rep.get("overall", {})
    paper = rep.get("paper", {})
    live = rep.get("live", {})
    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>가상/실전 검증 리포트</title>
<style>
:root {{ --bg:#eef2f7; --card:#fff; --line:#d8dee8; --ink:#1f2d3d; --muted:#5f7185; --pass:#22b35f; --fail:#dc3545; --warn:#d59f00; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Segoe UI","Malgun Gothic",sans-serif; }}
.container {{ max-width:1320px; margin:14px auto 24px; padding:0 12px; }}
.hero {{ background:linear-gradient(135deg,#2d435a,#3a8ed0); color:#fff; border-radius:12px; padding:18px 22px; margin-bottom:12px; }}
.hero h1 {{ margin:0 0 6px; font-size:36px; }}
.hero p {{ margin:0; opacity:.95; }}
.summary {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:10px; margin-bottom:12px; }}
.metric {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }}
.metric .k {{ font-size:13px; color:var(--muted); margin-bottom:6px; }}
.metric .v {{ font-size:26px; font-weight:700; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }}
.card h2 {{ margin:0 0 6px; font-size:24px; }}
.judge {{ margin:0 0 10px; color:#314a62; }}
.table-wrap {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; word-break:break-word; }}
th {{ background:#f6f9fd; color:#38506b; }}
.chip {{ display:inline-block; min-width:90px; text-align:center; border-radius:999px; padding:3px 8px; color:#fff; font-size:12px; font-weight:700; }}
.chip-pass {{ background:var(--pass); }} .chip-fail {{ background:var(--fail); }} .chip-warn {{ background:var(--warn); color:#111; }}
</style>
</head>
<body>
<main class='container'>
<section class='hero'>
  <h1>가상/실전 검증 보고서</h1>
  <p>생성: {rep.get('generated_at','-')}</p>
</section>
<section class='summary'>
  <div class='metric'><div class='k'>전체 판정</div><div class='v'>{ov.get('judgment','-')}</div></div>
  <div class='metric'><div class='k'>다음 단계</div><div class='v'>{ov.get('next_step','-')}</div></div>
  <div class='metric'><div class='k'>가상매매</div><div class='v'>{paper.get('judgment','-')}</div></div>
  <div class='metric'><div class='k'>실전매매</div><div class='v'>{live.get('judgment','-')}</div></div>
  <div class='metric'><div class='k'>Paper→Live</div><div class='v'>{(((rep.get('transition_gate') or {}).get('paper_to_live') or {}).get('status','-'))}</div></div>
</section>
{_render_stage_table(paper)}
{_render_stage_table(live)}
</main>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Build paper/live validation report from latest logs")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    ap.add_argument("--out-html", default="")
    args = ap.parse_args()

    global TRVAL_ASOF_YMD
    TRVAL_ASOF_YMD = _normalize_ymd(_env_or_cfg("TRVAL_ASOF_YMD", "scope.asof_ymd", ""))
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rep = build_report()

    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"trading_stage_validation_{stamp}.json")
    out_md = Path(args.out_md) if args.out_md else (LOG_DIR / f"trading_stage_validation_{stamp}.md")
    out_html = Path(args.out_html) if args.out_html else (LOG_DIR / f"trading_stage_validation_{stamp}.html")

    latest_json = LOG_DIR / "trading_stage_validation_latest.json"
    latest_md = LOG_DIR / "trading_stage_validation_latest.md"
    latest_html = LOG_DIR / "trading_stage_validation_latest.html"

    out_json.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    latest_json.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    md = _render_md(rep)
    out_md.write_text(md, encoding="utf-8-sig")
    latest_md.write_text(md, encoding="utf-8-sig")

    html = _render_html(rep)
    out_html.write_text(html, encoding="utf-8")
    latest_html.write_text(html, encoding="utf-8")

    _log_print(f"[TRVAL] overall={rep.get('overall',{}).get('judgment','-')} next={rep.get('overall',{}).get('next_step','-')}")
    _log_print(f"[TRVAL] json={out_json}")
    _log_print(f"[TRVAL] md={out_md}")
    _log_print(f"[TRVAL] html={out_html}")
    _log_print(f"[TRVAL] latest_json={latest_json}")
    _log_print(f"[TRVAL] latest_md={latest_md}")
    _log_print(f"[TRVAL] latest_html={latest_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
