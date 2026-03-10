# -*- coding: utf-8 -*-
"""
v41.1 파라미터 민감도(±20%) 리포트

- 목적: stable_params_v41_1.json 기준으로 핵심 파라미터를 ±20% 흔들었을 때
        IS/VAL/OOS 성과(특히 OOS)가 얼마나 붕괴/유지되는지 자동 리포트 생성

입력:
- <BASE_DIR>\optimize_params_v41_1.py
- <BASE_DIR>\12_Risk_Controlled\stable_params_v41_1.json
- <BASE_DIR>\12_Risk_Controlled\split_policy_v41_1.json   (존재 시)
출력:
- <BASE_DIR>\12_Risk_Controlled\sensitivity_report_v41_1.csv
- <BASE_DIR>\12_Risk_Controlled\sensitivity_report_v41_1.json
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


FACTORS = [0.8, 0.9, 1.0, 1.1, 1.2]

# 민감도 대상(최소 범위): "필터/진입/리스크" 핵심만
PARAM_KEYS = [
    "rs_lim",
    "v_accel_lim",
    "stretch_max",
    "value_min",
    "atr_max",
    "gap_limit",
    "stop_loss",
    "hold_days",
    "max_positions",
]

# int로 처리해야 하는 키
INT_KEYS = {"hold_days", "max_positions"}


def _jload(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _jsave(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _get_attr(obj: Any, key: str, default=None):
    # dataclass/obj
    if hasattr(obj, key):
        return getattr(obj, key)
    # dict
    if isinstance(obj, dict) and key in obj:
        return obj.get(key)
    return default


def _split_of(win: Any) -> str:
    s = _get_attr(win, "split", None)
    if s is None:
        # fallback: year-based split policy may embed in 'notes' or similar
        s = _get_attr(win, "tag", None)
    return str(s or "UNK").upper()


def _pf_of(win: Any) -> float:
    v = _get_attr(win, "pf", None)
    if v is None:
        v = _get_attr(win, "profit", None)
    v = _safe_float(v)
    return float(v) if v is not None else 0.0


def _ntrades_of(win: Any) -> int:
    for k in ("n_trades", "trades", "n", "count"):
        v = _get_attr(win, k, None)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return 0


def _aggregate_by_split(windows: List[Any]) -> Dict[str, Dict[str, float]]:
    agg: Dict[str, Dict[str, float]] = {}
    for w in windows or []:
        sp = _split_of(w)
        agg.setdefault(sp, {"pf": 0.0, "n": 0})
        agg[sp]["pf"] += _pf_of(w)
        agg[sp]["n"] += _ntrades_of(w)
    return agg


def _pct_change(base: float, cur: float) -> float:
    if base == 0:
        return 0.0 if cur == 0 else (100.0 if cur > 0 else -100.0)
    return (cur - base) / abs(base) * 100.0


def _try_build_price_cache(opt_mod: Any, df: pd.DataFrame) -> None:
    # v6 계열에서 price_cache가 필요할 수 있어, 있으면 만든 뒤 모듈 전역에 심어줌
    candidates = ["build_price_cache", "make_price_cache", "build_cache", "make_cache"]
    for fn_name in candidates:
        fn = getattr(opt_mod, fn_name, None)
        if callable(fn):
            try:
                pc = fn(df)
                # 전역 변수 이름 추정
                for var in ["PRICE_CACHE", "price_cache", "_PRICE_CACHE", "PRICE_CACHE_BY_CODE"]:
                    try:
                        setattr(opt_mod, var, pc)
                    except Exception:
                        pass
                return
            except Exception:
                # cache 빌드가 실패해도 eval이 내부에서 처리할 수 있으니 패스
                return


def _call_eval(opt_mod: Any, df: pd.DataFrame, windows: List[Any], params: dict) -> Tuple[float, List[Any]]:
    # eval_params 시그니처가 바뀌어도 최대한 맞춰 호출
    ev = getattr(opt_mod, "eval_params", None)
    if not callable(ev):
        raise RuntimeError("optimize_params_v41_1.py 에 eval_params() 가 없습니다.")
    try:
        return ev(df, windows, params)
    except TypeError:
        # 혹시 (df, params, windows) 형태면 재시도
        return ev(df, params, windows)


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir / "12_Risk_Controlled"

    stable_path = out_dir / "stable_params_v41_1.json"
    split_policy_path = out_dir / "split_policy_v41_1.json"

    if not stable_path.exists():
        print(f"[FATAL] missing: {stable_path}")
        return 2

    # import optimizer module (same folder)
    try:
        import importlib
        sys.path.insert(0, str(base_dir))
        opt = importlib.import_module("optimize_params_v41_1")
    except Exception as e:
        print("[FATAL] cannot import optimize_params_v41_1.py:", repr(e))
        print("        - BASE_DIR에 optimize_params_v41_1.py 가 있어야 합니다.")
        return 3

    stable = _jload(stable_path)

    # params dict extraction: (v6) might be { "params": {...} } or direct dict
    params_base = stable.get("params") if isinstance(stable, dict) and isinstance(stable.get("params"), dict) else stable

    # load data + windows (reuse existing functions)
    ld = getattr(opt, "load_data", None)
    bw = getattr(opt, "build_windows", None)
    if not callable(ld) or not callable(bw):
        print("[FATAL] optimize_params_v41_1.py must provide load_data() and build_windows()")
        return 4

    print("[SENS] loading data...")
    df = ld(base_dir)
    # [FIX] PREPROCESS DF WITH FACTORS BEFORE EVAL
    # compute factors + dropna so eval sees rs/rs_slope/v_accel/stretch/atr14_pct/gap_next
    df = opt.compute_factors(df)
    df = df.dropna(subset=['rs','rs_slope','v_accel','stretch','atr14_pct','gap_next']).copy()

    # windows range
    years_back = int(getattr(opt, "YEARS_BACK", 2))
    windows = bw(df, years_back)

    # price cache if available
    _try_build_price_cache(opt, df)

    # baseline evaluation
    base_score, base_windows = _call_eval(opt, df, windows, dict(params_base))
    base_agg = _aggregate_by_split(base_windows)
    base_is_pf = float(base_agg.get("IS", {}).get("pf", 0.0))
    base_val_pf = float(base_agg.get("VAL", {}).get("pf", 0.0))
    base_oos_pf = float(base_agg.get("OOS", {}).get("pf", 0.0))

    rows = []
    rows.append({
        "param": "__BASELINE__",
        "factor": 1.0,
        "value": "",
        "base_value": "",
        "score": float(base_score),
        "delta_score_pct": 0.0,
        "is_pf": base_is_pf,
        "val_pf": base_val_pf,
        "oos_pf": base_oos_pf,
        "is_n": int(base_agg.get("IS", {}).get("n", 0)),
        "val_n": int(base_agg.get("VAL", {}).get("n", 0)),
        "oos_n": int(base_agg.get("OOS", {}).get("n", 0)),
        "flag": "",
    })

    # per-parameter sensitivity
    for key in PARAM_KEYS:
        b = params_base.get(key, None)
        b_f = _safe_float(b)
        if b_f is None:
            continue

        for f in FACTORS:
            if f == 1.0:
                continue
            v = b_f * f
            if key in INT_KEYS:
                v = int(max(1, round(v)))

            p = dict(params_base)
            p[key] = v

            score, win_res = _call_eval(opt, df, windows, p)
            agg = _aggregate_by_split(win_res)
            is_pf = float(agg.get("IS", {}).get("pf", 0.0))
            val_pf = float(agg.get("VAL", {}).get("pf", 0.0))
            oos_pf = float(agg.get("OOS", {}).get("pf", 0.0))
            oos_n = int(agg.get("OOS", {}).get("n", 0))
            d_score = _pct_change(float(base_score), float(score))
            d_oos = _pct_change(base_oos_pf, oos_pf)

            # 붕괴 플래그: OOS -20% 이하 또는 score -20% 이하
            flag = ""
            if oos_n < 5:
                flag = "INSUFFICIENT_DATA"
            elif d_oos <= -20.0:
                flag = "OOS_DROP"
            elif d_score <= -20.0:
                flag = "SCORE_DROP"
            rows.append({
                "param": key,
                "factor": float(f),
                "value": v,
                "base_value": b_f if key not in INT_KEYS else int(b_f),
                "score": float(score),
                "delta_score_pct": float(d_score),
                "is_pf": is_pf,
                "val_pf": val_pf,
                "oos_pf": oos_pf,
                "is_n": int(agg.get("IS", {}).get("n", 0)),
                "val_n": int(agg.get("VAL", {}).get("n", 0)),
                "oos_n": int(agg.get("OOS", {}).get("n", 0)),
                "delta_oos_pf_pct": float(d_oos),
                "flag": flag,
            })

    out_csv = out_dir / "sensitivity_report_v41_1.csv"
    out_json = out_dir / "sensitivity_report_v41_1.json"

    rdf = pd.DataFrame(rows)

    # 정렬: baseline -> param -> factor
    rdf["__order"] = rdf.apply(lambda r: (0 if r["param"] == "__BASELINE__" else 1, str(r["param"]), float(r["factor"]) if r["factor"] != "" else 1.0), axis=1)
    rdf = rdf.sort_values("__order").drop(columns=["__order"])

    rdf.to_csv(out_csv, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stable_params_path": str(stable_path),
        "split_policy_path": str(split_policy_path) if split_policy_path.exists() else "",
        "baseline": {
            "score": float(base_score),
            "is_pf": base_is_pf,
            "val_pf": base_val_pf,
            "oos_pf": base_oos_pf,
        },
        "flags": {
            "INSUFFICIENT_DATA": int((rdf.get("flag") == "INSUFFICIENT_DATA").sum()) if "flag" in rdf.columns else 0,
            "OOS_DROP": int((rdf.get("flag") == "OOS_DROP").sum()) if "flag" in rdf.columns else 0,
            "SCORE_DROP": int((rdf.get("flag") == "SCORE_DROP").sum()) if "flag" in rdf.columns else 0,
        },
        "outputs": {
            "csv": str(out_csv),
            "json": str(out_json),
        },
    }
    _jsave(out_json, summary)

    print("[OK] saved:")
    print(" -", out_csv)
    print(" -", out_json)
    print(f"[BASELINE] score={base_score:.4f}  IS_pf={base_is_pf:.4f}  VAL_pf={base_val_pf:.4f}  OOS_pf={base_oos_pf:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
