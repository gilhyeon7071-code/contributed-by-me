from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRICES_PATH = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
PARAMS_PATH = ROOT / "paper" / "surge_params.json"
OUT_PROPOSE = ROOT / "2_Logs" / "surge_param_proposal_latest.json"
OUT_APPLY = ROOT / "2_Logs" / "surge_param_apply_latest.json"
OUT_ML_RETRAIN = ROOT / "2_Logs" / "surge_ml_retrain_latest.json"
ML_STATUS_PATH = ROOT / "2_Logs" / "surge_ml_train_status_latest.json"
BT_REPORT_PATH = ROOT / "2_Logs" / "surge_backtest_report_latest.json"
DEFAULT_EVAL_START_YMD = "20260301"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _load_prices() -> pd.DataFrame:
    px = pd.read_parquet(PRICES_PATH).copy()
    px["date"] = px["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.slice(0, 8)
    px["code"] = px["code"].astype(str).str.zfill(6)
    for c in ["open", "high", "low", "close", "volume"]:
        px[c] = pd.to_numeric(px[c], errors="coerce")
    px = px.dropna(subset=["open", "high", "low", "close", "volume"]).sort_values(["code", "date"]).copy()
    return px


def _build_base(px: pd.DataFrame) -> pd.DataFrame:
    g = px.groupby("code", group_keys=False)
    out = px.copy()
    out["ret1"] = g["close"].pct_change(1)
    out["ret5"] = g["close"].pct_change(5)
    out["ret15"] = g["close"].pct_change(15)
    out["range_pct"] = (out["high"] - out["low"]) / out["close"].replace(0, pd.NA)
    out["vol_ma20"] = g["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    out["vol_ratio20"] = out["volume"] / out["vol_ma20"].replace(0, pd.NA)
    out["close_shift_1"] = g["close"].shift(1)
    out["max_close_15_prev"] = g["close"].rolling(15, min_periods=15).max().shift(1).reset_index(level=0, drop=True)
    out["fwd_ret5"] = g["close"].shift(-5) / out["close"] - 1.0
    return out.dropna(
        subset=["ret1", "ret5", "ret15", "range_pct", "vol_ratio20", "close_shift_1", "max_close_15_prev", "fwd_ret5"]
    ).copy()


def _score_rule(
    df: pd.DataFrame,
    pct_min: float,
    rvol20_min: float,
    range_min: float,
    reg_short_5d_min: float,
    reg_mid_15d_min: float,
) -> pd.DataFrame:
    d = df.copy()
    cond_price = d["ret1"] >= float(pct_min)
    cond_vol = d["vol_ratio20"] >= float(rvol20_min)
    cond_range = d["range_pct"] >= float(range_min)
    cond_highest = d["close"] >= d["max_close_15_prev"]
    cond_reg_short = (d["ret5"] >= float(reg_short_5d_min)) & cond_highest
    cond_reg_mid = (d["ret15"] >= float(reg_mid_15d_min)) & cond_highest
    cond_limit_near = d["ret1"] >= 0.29
    d["sig"] = ((cond_price & (cond_vol | cond_range)) | cond_reg_short | cond_reg_mid | cond_limit_near).astype(int)
    return d


def _summary(d: pd.DataFrame) -> Dict[str, float]:
    s = d[d["sig"] == 1].copy()
    if len(s) == 0:
        return {"signals": 0.0, "win_rate": 0.0, "avg_ret5": 0.0, "median_ret5": 0.0, "std_ret5": 0.0}
    r = pd.to_numeric(s["fwd_ret5"], errors="coerce").fillna(0.0)
    return {
        "signals": float(len(s)),
        "win_rate": float((r > 0).mean()),
        "avg_ret5": float(r.mean()),
        "median_ret5": float(r.median()),
        "std_ret5": float(r.std(ddof=0)),
    }


def _objective(stats: Dict[str, float]) -> float:
    win = float(stats.get("win_rate", 0.0))
    sig = float(stats.get("signals", 0.0))
    avg = float(stats.get("avg_ret5", 0.0))
    size = math.log(sig + 1.0)
    if avg <= 0.0:
        return avg * size
    if win <= 0.5:
        return (win - 0.5) * size * avg
    return (win - 0.5) * size * avg


def _status(stats: Dict[str, float]) -> str:
    win = float(stats.get("win_rate", 0.0))
    med = float(stats.get("median_ret5", 0.0))
    sig = float(stats.get("signals", 0.0))
    if win >= 0.55 and med >= 0.0 and sig >= 30:
        return "PASS"
    if win >= 0.48 and sig >= 15:
        return "WARN"
    return "FAIL"


def _metrics_diff(cur: Dict[str, float], nxt: Dict[str, float]) -> Dict[str, float]:
    keys = sorted(set(cur.keys()) | set(nxt.keys()))
    out: Dict[str, float] = {}
    for k in keys:
        out[k] = float(nxt.get(k, 0.0)) - float(cur.get(k, 0.0))
    return out


def _walk_forward_splits(dates: List[str], folds: int = 3) -> List[Tuple[List[str], List[str]]]:
    if len(dates) < 12:
        return [(dates[: max(1, int(len(dates) * 0.7))], dates[max(1, int(len(dates) * 0.7)) :])]
    step = max(2, len(dates) // (folds + 1))
    out: List[Tuple[List[str], List[str]]] = []
    for i in range(1, folds + 1):
        cut = min(len(dates) - 2, i * step)
        tr = dates[:cut]
        va = dates[cut : min(len(dates), cut + step)]
        if len(tr) >= 5 and len(va) >= 2:
            out.append((tr, va))
    return out or [(dates[: max(1, int(len(dates) * 0.7))], dates[max(1, int(len(dates) * 0.7)) :])]


def _grid() -> List[Tuple[float, float, float]]:
    pct = [0.03, 0.05, 0.07, 0.09, 0.11, 0.13]
    rvol = [1.0, 1.5, 2.0, 2.5, 3.0]
    rng = [0.04, 0.06, 0.08, 0.10, 0.12]
    out: List[Tuple[float, float, float]] = []
    for a in pct:
        for b in rvol:
            for c in rng:
                out.append((a, b, c))
    return out


def _check_ml_retrain() -> Dict[str, Any]:
    ml = _load_json(ML_STATUS_PATH)
    bt = _load_json(BT_REPORT_PATH)
    auc = _safe_float(ml.get("valid_auc"), -1.0)
    wf = ml.get("walk_forward") if isinstance(ml.get("walk_forward"), dict) else {}
    folds = wf.get("folds") if isinstance(wf.get("folds"), list) else []
    latest_fold = folds[-1] if folds and isinstance(folds[-1], dict) else {}
    recent_auc = _safe_float(latest_fold.get("valid_auc"), auc)
    pos_rate = _safe_float(ml.get("valid_pos_rate"), -1.0)
    fallback = bool((((bt.get("ml_topq_fallback") or {}) if isinstance(bt.get("ml_topq_fallback"), dict) else {}).get("used")))
    reasons: List[str] = []
    if recent_auc < 0.65:
        reasons.append(f"recent_fold_auc_below_0.65:{recent_auc:.4f}")
    if auc >= 0 and recent_auc >= 0 and (auc - recent_auc) > 0.08:
        reasons.append(f"auc_drift:{auc:.3f}->{recent_auc:.3f}")
    if pos_rate >= 0 and pos_rate < 0.03:
        reasons.append("valid_pos_rate_below_0.03")
    if fallback:
        reasons.append("ml_topq_fallback_used")
    return {
        "required": bool(reasons),
        "reasons": reasons,
        "valid_auc": auc,
        "recent_fold_auc": recent_auc,
        "auc_drift": (float(auc - recent_auc) if auc >= 0 and recent_auc >= 0 else None),
        "valid_pos_rate": pos_rate,
        "ml_topq_fallback_used": fallback,
        "suggest_label_thr_ret5": 0.10 if (pos_rate >= 0 and pos_rate < 0.03) else 0.20,
    }


def propose() -> int:
    ts = _now()
    if not PRICES_PATH.exists():
        OUT_PROPOSE.write_text(
            json.dumps({"ts": ts, "status": "MISSING_PRICES", "path": str(PRICES_PATH)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    px = _load_prices()
    df = _build_base(px)
    eval_start_ymd = str(os.getenv("SURGE_PARAM_EVAL_START_YMD", DEFAULT_EVAL_START_YMD) or DEFAULT_EVAL_START_YMD).strip()
    if eval_start_ymd:
        df = df[df["date"].astype(str) >= eval_start_ymd].copy()
    dates = sorted(df["date"].astype(str).unique().tolist())
    if len(dates) >= 20:
        holdout_cut = max(1, int(len(dates) * 0.8))
        tune_dates = dates[:holdout_cut]
        holdout_dates = dates[holdout_cut:]
    else:
        tune_dates = dates
        holdout_dates = []
    splits = _walk_forward_splits(tune_dates, folds=3)
    grid = _grid()

    current = _load_json(PARAMS_PATH)
    cur_params = {
        "pct_min": _safe_float(current.get("pct_min"), 0.07),
        "rvol20_min": _safe_float(current.get("rvol20_min"), 2.0),
        "range_min": _safe_float(current.get("range_min"), 0.08),
        "reg_short_5d_min": _safe_float(current.get("reg_short_5d_min"), 0.60),
        "reg_mid_15d_min": _safe_float(current.get("reg_mid_15d_min"), 1.00),
    }

    best: Dict[str, Any] = {"objective": -10**18}
    for pct_min, rvol20_min, range_min in grid:
        fold_scores: List[float] = []
        fold_stats: List[Dict[str, Any]] = []
        for tr_dates, va_dates in splits:
            tr = df[df["date"].isin(tr_dates)].copy()
            va = df[df["date"].isin(va_dates)].copy()
            if len(tr) == 0 or len(va) == 0:
                continue
            tr_scored = _score_rule(
                tr,
                pct_min=pct_min,
                rvol20_min=rvol20_min,
                range_min=range_min,
                reg_short_5d_min=float(cur_params["reg_short_5d_min"]),
                reg_mid_15d_min=float(cur_params["reg_mid_15d_min"]),
            )
            va_scored = _score_rule(
                va,
                pct_min=pct_min,
                rvol20_min=rvol20_min,
                range_min=range_min,
                reg_short_5d_min=float(cur_params["reg_short_5d_min"]),
                reg_mid_15d_min=float(cur_params["reg_mid_15d_min"]),
            )
            tr_stats = _summary(tr_scored)
            va_stats = _summary(va_scored)
            overfit_penalty = abs(float(tr_stats["win_rate"]) - float(va_stats["win_rate"])) * 0.1 + abs(
                float(tr_stats["avg_ret5"]) - float(va_stats["avg_ret5"])
            )
            fold_scores.append(_objective(va_stats) - float(overfit_penalty))
            fold_stats.append({"train": tr_stats, "valid": va_stats, "overfit_penalty": float(overfit_penalty)})
        if not fold_scores:
            continue
        obj = float(sum(fold_scores) / len(fold_scores))
        if obj > float(best["objective"]):
            best = {
                "objective": obj,
                "pct_min": pct_min,
                "rvol20_min": rvol20_min,
                "range_min": range_min,
                "fold_stats": fold_stats,
            }

    if not best or float(best.get("objective", -10**18)) <= -10**17:
        OUT_PROPOSE.write_text(
            json.dumps({"ts": ts, "status": "NO_CANDIDATE", "rows_eval": int(len(df))}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 2

    current_scored = _score_rule(
        df,
        pct_min=float(cur_params["pct_min"]),
        rvol20_min=float(cur_params["rvol20_min"]),
        range_min=float(cur_params["range_min"]),
        reg_short_5d_min=float(cur_params["reg_short_5d_min"]),
        reg_mid_15d_min=float(cur_params["reg_mid_15d_min"]),
    )
    current_metrics = _summary(current_scored)

    all_scored = _score_rule(
        df,
        pct_min=float(best["pct_min"]),
        rvol20_min=float(best["rvol20_min"]),
        range_min=float(best["range_min"]),
        reg_short_5d_min=float(cur_params["reg_short_5d_min"]),
        reg_mid_15d_min=float(cur_params["reg_mid_15d_min"]),
    )
    proposed_metrics = _summary(all_scored)
    overall_status = _status(proposed_metrics)
    holdout_metrics: Dict[str, float] = {}
    holdout_status = "NA"
    if holdout_dates:
        holdout_df = df[df["date"].isin(holdout_dates)].copy()
        holdout_scored = _score_rule(
            holdout_df,
            pct_min=float(best["pct_min"]),
            rvol20_min=float(best["rvol20_min"]),
            range_min=float(best["range_min"]),
            reg_short_5d_min=float(cur_params["reg_short_5d_min"]),
            reg_mid_15d_min=float(cur_params["reg_mid_15d_min"]),
        )
        holdout_metrics = _summary(holdout_scored)
        holdout_status = _status(holdout_metrics)
    ml_retrain = _check_ml_retrain()
    proposal = {
        "ts": ts,
        "status": "OK",
        "mode": "propose",
        "objective_name": "avg_ret5<=0 ? avg_ret5*log(signals+1) : (win_rate-0.5)*log(signals+1)*avg_ret5",
        "walk_forward_folds": int(len(splits)),
        "eval_start_ymd": eval_start_ymd,
        "eval_end_ymd": (str(dates[-1]) if dates else ""),
        "rows_eval": int(len(df)),
        "grid_size": int(len(grid)),
        "current_params": cur_params,
        "proposed_params": {
            "pct_min": float(best["pct_min"]),
            "rvol20_min": float(best["rvol20_min"]),
            "range_min": float(best["range_min"]),
            "reg_short_5d_min": float(cur_params["reg_short_5d_min"]),
            "reg_mid_15d_min": float(cur_params["reg_mid_15d_min"]),
        },
        "metrics_current": current_metrics,
        "metrics_proposed": proposed_metrics,
        "metrics_diff": _metrics_diff(current_metrics, proposed_metrics),
        "metrics_holdout": holdout_metrics,
        "holdout_status": holdout_status,
        "metrics": proposed_metrics,
        "param_status": overall_status,
        "fold_stats": best.get("fold_stats", []),
        "ml_retrain": ml_retrain,
        "apply_recommended": (overall_status == "FAIL" or holdout_status == "FAIL") or bool(ml_retrain.get("required")),
    }
    OUT_PROPOSE.write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


def _run_cmd(args: List[str]) -> Dict[str, Any]:
    try:
        cp = subprocess.run(args, capture_output=True, text=True, cwd=str(ROOT))
        return {
            "ok": cp.returncode == 0,
            "returncode": int(cp.returncode),
            "cmd": args,
            "stdout": (cp.stdout or "")[:2000],
            "stderr": (cp.stderr or "")[:2000],
        }
    except Exception as e:
        return {"ok": False, "returncode": -1, "cmd": args, "error": str(e)}


def _python_candidates() -> List[str]:
    forced = str(os.getenv("SURGE_ML_TRAIN_PY", "") or "").strip()
    cands = [
        forced,
        sys.executable,
        "python",
        "py",
        str(ROOT / "_runtime" / "python312-embed" / "python.exe"),
        str(ROOT / ".venv" / "Scripts" / "python.exe"),
        str(Path("E:/vibe/buffett/.venv/Scripts/python.exe")),
        str(ROOT / "tools" / "python_exec_proxy.cmd"),
        str(Path("C:/Users/jjtop/AppData/Local/Programs/Python/Python312/python.exe")),
        str(Path("C:/Users/jjtop/AppData/Local/Programs/Python/Python314/python.exe")),
    ]
    uniq: List[str] = []
    for c in cands:
        if c and c not in uniq:
            uniq.append(c)
    return uniq


def _pick_python_for_ml_train() -> Tuple[str | None, List[Dict[str, Any]]]:
    checks: List[Dict[str, Any]] = []
    for py in _python_candidates():
        has_sep = ("\\" in py) or ("/" in py) or py.lower().endswith(".exe") or py.lower().endswith(".cmd")
        if has_sep:
            try:
                exists = Path(py).exists()
            except Exception as e:
                checks.append({"python": py, "ok": False, "reason": "path_access_error", "error": str(e)})
                continue
            if not exists:
                checks.append({"python": py, "ok": False, "reason": "missing"})
                continue
        else:
            resolved = shutil.which(py)
            if not resolved:
                checks.append({"python": py, "ok": False, "reason": "not_in_path"})
                continue
        try:
            cp = subprocess.run(
                [py, "-c", "import pandas,numpy; print('OK')"],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
            )
            ok = cp.returncode == 0 and "OK" in (cp.stdout or "")
            checks.append(
                {
                    "python": py,
                    "ok": ok,
                    "returncode": int(cp.returncode),
                    "stdout": (cp.stdout or "")[:200],
                    "stderr": (cp.stderr or "")[:200],
                }
            )
            if ok:
                return py, checks
        except Exception as e:
            checks.append({"python": py, "ok": False, "error": str(e)})
    return None, checks


def _parse_label_grid(default_label: float) -> List[float]:
    raw = str(os.getenv("SURGE_ML_LABEL_GRID", "") or "").strip()
    if not raw:
        base = [0.10, 0.12, 0.15, 0.20]
    else:
        base = []
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                base.append(float(tok))
            except Exception:
                continue
    if not base:
        base = [float(default_label)]
    uniq: List[float] = []
    for v in base:
        vv = max(0.01, min(0.50, float(v)))
        if vv not in uniq:
            uniq.append(vv)
    return uniq


def _run_ml_label_grid(py_exec: str, env: Dict[str, str], py_checks: List[Dict[str, Any]], default_label: float) -> Dict[str, Any]:
    grid = _parse_label_grid(default_label)
    results: List[Dict[str, Any]] = []
    best_auc = -1.0
    best_label = float(default_label)
    best_row: Dict[str, Any] = {}
    min_pos_rate = _safe_float(os.getenv("SURGE_ML_MIN_VALID_POS_RATE"), 0.03)
    best_pass_auc = -1.0
    best_pass_label = float(default_label)
    best_pass_row: Dict[str, Any] = {}
    for thr in grid:
        run_env = dict(env)
        run_env["SURGE_ML_LABEL_RET5_MIN"] = str(float(thr))
        cp = subprocess.run(
            [py_exec, str(ROOT / "tools" / "surge_ml_train.py")],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            env=run_env,
        )
        st = _load_json(ML_STATUS_PATH)
        auc = _safe_float(st.get("valid_auc"), -1.0)
        row = {
            "label_thr_ret5": float(thr),
            "ok": bool(cp.returncode == 0),
            "returncode": int(cp.returncode),
            "valid_auc": auc,
            "valid_pos_rate": _safe_float(st.get("valid_pos_rate"), -1.0),
            "trainer": st.get("trainer"),
            "stdout": (cp.stdout or "")[:500],
            "stderr": (cp.stderr or "")[:500],
        }
        results.append(row)
        if row["ok"] and auc > best_auc:
            best_auc = auc
            best_label = float(thr)
            best_row = row
        if row["ok"] and float(row["valid_pos_rate"]) >= min_pos_rate and auc > best_pass_auc:
            best_pass_auc = auc
            best_pass_label = float(thr)
            best_pass_row = row
    selected_by = "auc_with_min_pos_rate" if best_pass_row else "auc_only_no_pos_rate_candidate"
    if best_pass_row:
        best_auc = best_pass_auc
        best_label = best_pass_label
        best_row = best_pass_row
    return {
        "grid": grid,
        "results": results,
        "min_valid_pos_rate": float(min_pos_rate),
        "selection": selected_by,
        "best_label_thr_ret5": float(best_label),
        "best_valid_auc": float(best_auc),
        "best_result": best_row,
        "python_checks": py_checks,
    }


def _run_ml_retrain(cand: Dict[str, Any], existing: Dict[str, Any], enabled: bool = True) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
    ml_info = cand.get("ml_retrain") if isinstance(cand.get("ml_retrain"), dict) else {}
    ml_triggered = False
    ml_train_result: Dict[str, Any] = {"skipped": True, "reason": "not_required_or_disabled"}
    param_updates: Dict[str, Any] = {}
    if not enabled or not bool(ml_info.get("required")):
        return ml_triggered, ml_train_result, param_updates

    ml_triggered = True
    env = os.environ.copy()
    suggest_label = _safe_float(ml_info.get("suggest_label_thr_ret5"), 0.20)
    fixed_label = existing.get("ml_label_thr_ret5")
    if isinstance(fixed_label, (int, float)):
        suggest_label = float(fixed_label)
    env["SURGE_ML_LABEL_RET5_MIN"] = str(suggest_label)
    fixed_grid = existing.get("ml_label_grid")
    if isinstance(fixed_grid, str) and fixed_grid.strip():
        env["SURGE_ML_LABEL_GRID"] = fixed_grid.strip()
    elif isinstance(fixed_label, (int, float)):
        env["SURGE_ML_LABEL_GRID"] = str(float(fixed_label))
    py_exec, py_checks = _pick_python_for_ml_train()
    if not py_exec:
        ml_train_result = {
            "skipped": False,
            "ok": False,
            "returncode": -1,
            "reason": "no_python_for_ml_train",
            "python_checks": py_checks,
        }
        return ml_triggered, ml_train_result, param_updates

    try:
        use_grid = str(os.getenv("SURGE_ML_AUTO_LABEL_GRID", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
        if use_grid:
            grid_out = _run_ml_label_grid(py_exec, env, py_checks, suggest_label)
            best_label = _safe_float(grid_out.get("best_label_thr_ret5"), suggest_label)
            env2 = dict(env)
            env2["SURGE_ML_LABEL_RET5_MIN"] = str(best_label)
            cp = subprocess.run(
                [py_exec, str(ROOT / "tools" / "surge_ml_train.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
                env=env2,
            )
            ml_train_result = {
                "skipped": False,
                "ok": cp.returncode == 0,
                "returncode": int(cp.returncode),
                "cmd": [py_exec, str(ROOT / "tools" / "surge_ml_train.py")],
                "label_thr_ret5": env2.get("SURGE_ML_LABEL_RET5_MIN"),
                "auto_label_grid": grid_out,
                "python_checks": py_checks,
                "stdout": (cp.stdout or "")[:2000],
                "stderr": (cp.stderr or "")[:2000],
            }
            if cp.returncode == 0:
                param_updates["ml_label_thr_ret5"] = float(best_label)
                param_updates["_ml_label_applied_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        else:
            cp = subprocess.run(
                [py_exec, str(ROOT / "tools" / "surge_ml_train.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
                env=env,
            )
            ml_train_result = {
                "skipped": False,
                "ok": cp.returncode == 0,
                "returncode": int(cp.returncode),
                "cmd": [py_exec, str(ROOT / "tools" / "surge_ml_train.py")],
                "label_thr_ret5": env.get("SURGE_ML_LABEL_RET5_MIN"),
                "python_checks": py_checks,
                "stdout": (cp.stdout or "")[:2000],
                "stderr": (cp.stderr or "")[:2000],
            }
    except Exception as e:
        ml_train_result = {"skipped": False, "ok": False, "returncode": -1, "error": str(e)}
    return ml_triggered, ml_train_result, param_updates


def apply(trigger_ml_retrain: bool = True) -> int:
    if not OUT_PROPOSE.exists():
        return 2
    ts = _now()
    cand = _load_json(OUT_PROPOSE)
    proposed = cand.get("proposed_params") if isinstance(cand.get("proposed_params"), dict) else {}
    if not proposed:
        return 3

    existing = _load_json(PARAMS_PATH)
    before = {
        "pct_min": _safe_float(existing.get("pct_min"), 0.07),
        "rvol20_min": _safe_float(existing.get("rvol20_min"), 2.0),
        "range_min": _safe_float(existing.get("range_min"), 0.08),
    }
    proposal_status = str(cand.get("status") or "UNKNOWN")
    param_status = str(cand.get("param_status") or "UNKNOWN")
    holdout_status = str(cand.get("holdout_status") or "UNKNOWN")
    ml_info = cand.get("ml_retrain") if isinstance(cand.get("ml_retrain"), dict) else {}
    block_reasons: List[str] = []
    if proposal_status != "OK":
        block_reasons.append(f"proposal_status_not_ok:{proposal_status}")
    if param_status != "PASS":
        block_reasons.append(f"param_status_not_pass:{param_status}")
    if holdout_status != "PASS":
        block_reasons.append(f"holdout_status_not_pass:{holdout_status}")
    if block_reasons:
        apply_log = {
            "ts": ts,
            "status": "BLOCKED",
            "reason": "apply_guard_blocked",
            "block_reasons": block_reasons,
            "before_params": before,
            "proposed_params": proposed,
            "proposal_path": str(OUT_PROPOSE),
            "metrics_current": cand.get("metrics_current"),
            "metrics_proposed": cand.get("metrics_proposed"),
            "metrics_diff": cand.get("metrics_diff"),
            "param_status": param_status,
            "holdout_status": holdout_status,
            "ml_retrain_required": bool(ml_info.get("required")),
            "ml_retrain_triggered": False,
            "ml_train_result": {"skipped": True, "reason": "apply_guard_blocked"},
        }
        OUT_APPLY.write_text(json.dumps(apply_log, ensure_ascii=False, indent=2), encoding="utf-8")
        return 4

    out = dict(existing)
    out["pct_min"] = _safe_float(proposed.get("pct_min"), _safe_float(existing.get("pct_min"), 0.07))
    out["rvol20_min"] = _safe_float(proposed.get("rvol20_min"), _safe_float(existing.get("rvol20_min"), 2.0))
    out["range_min"] = _safe_float(proposed.get("range_min"), _safe_float(existing.get("range_min"), 0.08))
    out["_last_validated"] = datetime.now().strftime("%Y-%m-%d")
    out["_param_status"] = str(cand.get("param_status") or "UNKNOWN")
    out["_source"] = "surge_param_validator"
    PARAMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PARAMS_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    ml_triggered, ml_train_result, param_updates = _run_ml_retrain(cand, existing, enabled=trigger_ml_retrain)
    out.update(param_updates)

    apply_log = {
        "ts": ts,
        "status": "OK",
        "before_params": before,
        "after_params": {
            "pct_min": _safe_float(out.get("pct_min"), 0.07),
            "rvol20_min": _safe_float(out.get("rvol20_min"), 2.0),
            "range_min": _safe_float(out.get("range_min"), 0.08),
        },
        "proposal_path": str(OUT_PROPOSE),
        "metrics_current": cand.get("metrics_current"),
        "metrics_proposed": cand.get("metrics_proposed"),
        "metrics_diff": cand.get("metrics_diff"),
        "param_status": cand.get("param_status"),
        "ml_retrain_required": bool(ml_info.get("required")),
        "ml_retrain_triggered": ml_triggered,
        "ml_train_result": ml_train_result,
    }
    PARAMS_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_APPLY.write_text(json.dumps(apply_log, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


def retrain_ml() -> int:
    if not OUT_PROPOSE.exists():
        return 2
    ts = _now()
    cand = _load_json(OUT_PROPOSE)
    existing = _load_json(PARAMS_PATH)
    proposal_status = str(cand.get("status") or "UNKNOWN")
    ml_info = cand.get("ml_retrain") if isinstance(cand.get("ml_retrain"), dict) else {}
    if proposal_status != "OK":
        log = {
            "ts": ts,
            "status": "BLOCKED",
            "reason": f"proposal_status_not_ok:{proposal_status}",
            "proposal_path": str(OUT_PROPOSE),
            "ml_retrain_required": bool(ml_info.get("required")),
            "ml_retrain_triggered": False,
            "ml_train_result": {"skipped": True, "reason": "proposal_status_not_ok"},
        }
        OUT_ML_RETRAIN.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
        return 4
    if not bool(ml_info.get("required")):
        log = {
            "ts": ts,
            "status": "SKIPPED",
            "reason": "ml_retrain_not_required",
            "proposal_path": str(OUT_PROPOSE),
            "ml_retrain_required": False,
            "ml_retrain_triggered": False,
            "ml_train_result": {"skipped": True, "reason": "not_required"},
        }
        OUT_ML_RETRAIN.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    before_params = {
        "pct_min": _safe_float(existing.get("pct_min"), 0.07),
        "rvol20_min": _safe_float(existing.get("rvol20_min"), 2.0),
        "range_min": _safe_float(existing.get("range_min"), 0.08),
        "ml_label_thr_ret5": _safe_float(existing.get("ml_label_thr_ret5"), -1.0),
    }
    ml_triggered, ml_train_result, param_updates = _run_ml_retrain(cand, existing, enabled=True)
    ok = bool(ml_train_result.get("ok"))
    log = {
        "ts": ts,
        "status": "OK" if ok else "FAIL",
        "mode": "retrain-ml",
        "proposal_path": str(OUT_PROPOSE),
        "before_params": before_params,
        "params_written": False,
        "param_updates_skipped": param_updates,
        "param_status": cand.get("param_status"),
        "holdout_status": cand.get("holdout_status"),
        "ml_retrain_required": bool(ml_info.get("required")),
        "ml_retrain_triggered": ml_triggered,
        "ml_train_result": ml_train_result,
    }
    OUT_ML_RETRAIN.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0 if ok else 5


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate and propose/apply surge parameters.")
    ap.add_argument("--mode", choices=["propose", "apply", "retrain-ml"], default="propose")
    ap.add_argument("--no-ml-retrain", action="store_true")
    args = ap.parse_args()
    if args.mode == "propose":
        return propose()
    if args.mode == "retrain-ml":
        return retrain_ml()
    return apply(trigger_ml_retrain=not bool(args.no_ml_retrain))


if __name__ == "__main__":
    raise SystemExit(main())
