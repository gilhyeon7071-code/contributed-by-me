"""Build read-only companion diagnostics for general candidate logic.

This tool observes the latest candidate output and writes companion JSON/CSV
files. It does not change candidate eligibility, gates, sizing, orders, fills,
ledger, or stats.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _num(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _text(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _pct_summary(df: pd.DataFrame, col: str) -> dict[str, Any]:
    if col not in df.columns or df.empty:
        return {"count": 0}
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return {"count": 0}
    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "median": float(s.median()),
        "max": float(s.max()),
        "mean": float(s.mean()),
    }


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = ""
    return out


def _spac_like(name: str) -> bool:
    lowered = str(name or "").lower()
    return ("spac" in lowered) or ("스팩" in str(name or ""))


def build_bucket_rows(candidates: pd.DataFrame, meta: dict[str, Any]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if candidates.empty:
        return pd.DataFrame(), []

    out = candidates.copy()
    out["code"] = out["code"].astype(str).str.zfill(6)
    out["_market_regime"] = _text(out, "market_regime").str.upper()
    out["_relax_level"] = _text(out, "relax_level").str.upper()
    out["_candidate_origin"] = _text(out, "candidate_origin").str.upper()
    out["_final_score_n"] = _num(out.get("final_score", pd.Series(index=out.index)), 0.0)
    out["_fund_score_n"] = _num(out.get("fundamental_score", pd.Series(index=out.index)), 0.0)
    out["_rs_n"] = _num(out.get("rs", pd.Series(index=out.index)), 0.0)
    out["_rs_slope_n"] = _num(out.get("rs_slope", pd.Series(index=out.index)), 0.0)
    out["_atr_n"] = _num(out.get("atr14_pct", pd.Series(index=out.index)), 0.0)
    out["_rsi_n"] = _num(out.get("rsi14", pd.Series(index=out.index)), 0.0)
    out["_high52_n"] = _num(out.get("high_52w_gap", pd.Series(index=out.index)), 999.0)
    out["_ret1_n"] = _num(out.get("ret1_pct", pd.Series(index=out.index)), 0.0)
    out["_volcorr_n"] = _num(out.get("vol_close_corr20", pd.Series(index=out.index)), 0.0)

    global_regime = str(meta.get("market_regime") or "").upper()
    if not global_regime:
        global_regime = str(out["_market_regime"].mode().iloc[0]) if not out.empty else "UNKNOWN"

    def classify(row: pd.Series) -> pd.Series:
        regime = str(row["_market_regime"] or global_regime).upper()
        relax = str(row["_relax_level"] or "").upper()
        origin = str(row["_candidate_origin"] or "").upper()
        observe_only = relax == "NONE" or origin in {"SECTOR_PREFILTER_UNION", "SECTOR_FALLBACK"}

        bull_momentum = (
            row["_rs_n"] >= 1.0
            and row["_rs_slope_n"] > 0.0
            and row["_volcorr_n"] >= 0.0
            and row["_high52_n"] <= 0.20
        )
        quality_ok = row["_fund_score_n"] >= 55.0 and row["_atr_n"] <= 0.18 and row["_rsi_n"] <= 70.0
        bear_survivor = (
            regime == "BEAR"
            and row["_rs_n"] >= 1.0
            and row["_fund_score_n"] >= 50.0
            and row["_atr_n"] <= 0.18
            and row["_ret1_n"] > -15.0
        )
        rally_recovery = row["_ret1_n"] >= 5.0 and row["_rs_slope_n"] > 0.0

        if rally_recovery:
            bucket = "RALLY_RECOVERY"
        elif regime == "BEAR" and bear_survivor:
            bucket = "BEAR_SURVIVOR_OBSERVE"
        elif regime in {"SIDEWAYS", "CORRECTION", "NORMAL"} and quality_ok:
            bucket = "SIDEWAYS_SELECTIVE"
        elif bull_momentum:
            bucket = "BULL_MOMENTUM"
        else:
            bucket = "UNCLASSIFIED_OBSERVE"

        if observe_only and not bucket.endswith("_OBSERVE"):
            bucket = f"{bucket}_OBSERVE"
        # [2026-09-10] 이름을 `observe_only` -> `observe_only_calc` 로 바꿨다.
        #   입력 CSV(candidates_latest_data.csv)에 이미 생산 값 `observe_only` 가 있어서
        #   아래 pd.concat 이 **같은 이름의 컬럼을 둘** 만들었다.
        #   그러면 group["observe_only"] 가 Series 가 아니라 DataFrame 이 되고
        #   int(...sum()) 이 TypeError 로 죽는다. 이 단계가 **매일 rc=1 로 실패했다.**
        #   생산 값과 도구 계산값을 **다른 이름으로 둘 다** 내보내 서로 대조할 수 있게 한다
        #   (`project_1data_observe_only_mask_dead`: 내보낸 CSV 전파가 미검증이었다).
        return pd.Series({"regime_bucket": bucket, "observe_only_calc": bool(observe_only)})

    out = pd.concat([out, out.apply(classify, axis=1)], axis=1)

    summary_rows: list[dict[str, Any]] = []
    for bucket, group in out.groupby("regime_bucket", dropna=False):
        summary_rows.append(
            {
                "regime_bucket": str(bucket),
                "rows": int(len(group)),
                "observe_only_rows": int(group["observe_only_calc"].astype(bool).sum()),
                # 생산 값과의 불일치를 함께 낸다. 둘이 다르면 마스크 전파에 문제가 있는 것이다
                "observe_only_mismatch": int(
                    (group["observe_only_calc"].astype(bool)
                     != _text(group, "observe_only").str.upper().isin({"TRUE", "1", "Y", "YES"})).sum()
                ) if "observe_only" in group.columns else None,
                "final_score": _pct_summary(group, "final_score"),
                "fundamental_score": _pct_summary(group, "fundamental_score"),
                "rs": _pct_summary(group, "rs"),
                "ret1_pct": _pct_summary(group, "ret1_pct"),
            }
        )
    return out, summary_rows


def build_quality_rows(bucket_rows: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if bucket_rows.empty:
        return pd.DataFrame(), []

    out = bucket_rows.copy()
    out["code"] = out["code"].astype(str).str.zfill(6)
    for col, default in {
        "final_score": 0.0,
        "fundamental_score": 0.0,
        "rs": 0.0,
        "rs_slope": 0.0,
        "ret1_pct": 0.0,
        "atr14_pct": 0.0,
        "rsi14": 0.0,
        "junk_risk_score": 0.0,
    }.items():
        out[f"_{col}"] = _num(out.get(col, pd.Series(index=out.index)), default)
    for col in ("junk_flags", "name", "relax_level"):
        out[f"_{col}"] = _text(out, col)

    def classify(row: pd.Series) -> pd.Series:
        flags: list[str] = []
        junk_flags = row["_junk_flags"].lower()
        if _spac_like(row["_name"]):
            flags.append("spac_or_spac_like")
        if "krx_caution" in junk_flags:
            flags.append("krx_caution")
        if "weak_fundamental" in junk_flags:
            flags.append("weak_fundamental")
        if row["_fundamental_score"] < 50.0:
            flags.append("fundamental_below_50")
        if row["_rs_slope"] <= 0.0:
            flags.append("rs_slope_nonpositive")
        if row["_ret1_pct"] <= -10.0:
            flags.append("deep_daily_drop")
        if row["_final_score"] < 0.50:
            flags.append("final_score_below_0p50")
        if row["_rs"] < 1.0:
            flags.append("rs_below_1")

        promotion_status = (
            "OBSERVE_ONLY_NO_BUY_PROMOTION"
            if row["_relax_level"].upper() == "NONE"
            else "NON_NONE_REVIEW_REQUIRED"
        )
        no_core_flags = not any(
            flag in flags
            for flag in [
                "weak_fundamental",
                "krx_caution",
                "spac_or_spac_like",
                "fundamental_below_50",
                "rs_slope_nonpositive",
                "deep_daily_drop",
            ]
        )

        if "spac_or_spac_like" in flags or "krx_caution" in flags:
            quality = "LOW_OR_SEPARATE_OBSERVE"
        elif (
            row["_fundamental_score"] >= 55.0
            and row["_final_score"] >= 0.60
            and row["_rs"] >= 3.0
            and row["_rs_slope"] > 0
            and row["_ret1_pct"] > -10.0
            and row["_atr14_pct"] <= 0.16
            and no_core_flags
        ):
            quality = "STRICT_STRONG_OBSERVE"
        elif (
            row["_fundamental_score"] >= 53.0
            and row["_final_score"] >= 0.54
            and row["_rs"] >= 1.0
            and row["_rs_slope"] > 0
            and row["_atr14_pct"] <= 0.18
            and row["_ret1_pct"] > -12.0
        ):
            quality = "MEDIUM_WATCH_OBSERVE"
        elif row["_fundamental_score"] >= 50.0 and row["_rs"] >= 1.0 and row["_atr14_pct"] <= 0.18:
            quality = "WEAK_WATCH_OBSERVE"
        else:
            quality = "LOW_OR_SEPARATE_OBSERVE"

        return pd.Series(
            {
                "observe_quality": quality,
                "promotion_status": promotion_status,
                "diagnostic_flags": "|".join(flags),
            }
        )

    out = pd.concat([out, out.apply(classify, axis=1)], axis=1)

    summary_rows: list[dict[str, Any]] = []
    for quality, group in out.groupby("observe_quality", dropna=False):
        summary_rows.append(
            {
                "observe_quality": str(quality),
                "rows": int(len(group)),
                "codes": group["code"].astype(str).str.zfill(6).tolist(),
                "names": group["name"].fillna("").astype(str).tolist(),
                "median_final_score": float(pd.to_numeric(group["final_score"], errors="coerce").median()),
                "median_fundamental_score": float(pd.to_numeric(group["fundamental_score"], errors="coerce").median()),
            }
        )
    return out, summary_rows


def write_outputs(root: Path) -> dict[str, Any]:
    log_dir = root / "2_Logs"
    candidate_path = log_dir / "candidates_latest_data.csv"
    meta_path = log_dir / "candidates_latest_meta.json"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    generated_at = datetime.now().isoformat(timespec="seconds")

    meta = _read_json(meta_path)
    candidates = pd.read_csv(candidate_path, dtype={"code": str}) if candidate_path.exists() else pd.DataFrame()

    bucket_rows, bucket_summary = build_bucket_rows(candidates, meta)
    bucket_cols = [
        "no",
        "date",
        "code",
        "name",
        "market",
        "market_regime",
        "regime_bucket",
        "observe_only",
        "observe_only_calc",
        "relax_level",
        "candidate_origin",
        "execution_pool",
        "final_score",
        "score",
        "fundamental_score",
        "fundamental_grade",
        "rs",
        "rs_slope",
        "v_accel",
        "atr14_pct",
        "rsi14",
        "vol_close_corr20",
        "high_52w_gap",
        "ret1_pct",
        "value",
        "junk_risk_score",
        "junk_risk_grade",
        "junk_flags",
    ]
    bucket_rows = _ensure_cols(bucket_rows, bucket_cols)

    quality_rows, quality_summary = build_quality_rows(bucket_rows)
    quality_cols = [
        "no",
        "date",
        "code",
        "name",
        "market",
        "market_regime",
        "regime_bucket",
        "observe_quality",
        "promotion_status",
        "diagnostic_flags",
        "relax_level",
        "final_score",
        "fundamental_score",
        "fundamental_grade",
        "rs",
        "rs_slope",
        "ret1_pct",
        "atr14_pct",
        "rsi14",
        "junk_risk_score",
        "junk_risk_grade",
        "junk_flags",
    ]
    quality_rows = _ensure_cols(quality_rows, quality_cols)

    bucket_json = log_dir / f"general_logic_regime_bucket_diagnostic_{ts}.json"
    bucket_csv = log_dir / f"general_logic_regime_bucket_diagnostic_{ts}.csv"
    bucket_latest_json = log_dir / "general_logic_regime_bucket_diagnostic_latest.json"
    bucket_latest_csv = log_dir / "general_logic_regime_bucket_diagnostic_latest.csv"
    quality_json = log_dir / f"bear_survivor_observe_quality_diagnostic_{ts}.json"
    quality_csv = log_dir / f"bear_survivor_observe_quality_diagnostic_{ts}.csv"
    quality_latest_json = log_dir / "bear_survivor_observe_quality_diagnostic_latest.json"
    quality_latest_csv = log_dir / "bear_survivor_observe_quality_diagnostic_latest.csv"
    combined_json = log_dir / f"general_logic_observe_companion_{ts}.json"
    combined_latest_json = log_dir / "general_logic_observe_companion_latest.json"

    bucket_rows[bucket_cols].to_csv(bucket_csv, index=False, encoding="utf-8-sig")
    bucket_rows[bucket_cols].to_csv(bucket_latest_csv, index=False, encoding="utf-8-sig")
    quality_rows[quality_cols].to_csv(quality_csv, index=False, encoding="utf-8-sig")
    quality_rows[quality_cols].to_csv(quality_latest_csv, index=False, encoding="utf-8-sig")

    bucket_report = {
        "generated_at": generated_at,
        "scope": "read_only_general_logic_regime_bucket_diagnostic",
        "source_candidates": str(candidate_path),
        "source_meta": str(meta_path),
        "candidate_file_exists": candidate_path.exists(),
        "candidate_rows": int(len(candidates)),
        "latest_date": meta.get("latest_date"),
        "meta_market_regime": meta.get("market_regime"),
        "chosen_level": meta.get("chosen_level"),
        "sector_prefilter_union": meta.get("sector_prefilter_union"),
        "bucket_summary": bucket_summary,
        "policy_effect": "read_only_no_buy_eligibility_change",
        "limitations": [
            "Buckets are observational labels only.",
            "No order eligibility, gate meaning, sizing, fills, ledger, or stats were changed.",
            "Current candidates_latest_data may contain sector union rows with relax_level=NONE; these remain observe-only in this diagnostic.",
        ],
    }
    quality_report = {
        "generated_at": generated_at,
        "scope": "read_only_bear_survivor_observe_quality_diagnostic",
        "source": str(bucket_latest_csv),
        "source_exists": bucket_latest_csv.exists(),
        "rows": int(len(quality_rows)),
        "all_promotion_status": (
            sorted(quality_rows["promotion_status"].dropna().unique().tolist()) if not quality_rows.empty else []
        ),
        "quality_summary": quality_summary,
        "policy_effect": "read_only_no_buy_eligibility_change",
        "limitations": [
            "This is an observation quality split, not a trade recommendation.",
            "All relax_level=NONE rows remain no-buy-promotion.",
            "Thresholds are diagnostic labels only and are not wired into candidate generation or paper_engine.",
        ],
    }
    combined_report = {
        "generated_at": generated_at,
        "scope": "read_only_general_logic_observe_companion",
        "policy_effect": "read_only_no_buy_eligibility_change",
        "outputs": {
            "bucket_json": str(bucket_json),
            "bucket_csv": str(bucket_csv),
            "bucket_latest_json": str(bucket_latest_json),
            "bucket_latest_csv": str(bucket_latest_csv),
            "quality_json": str(quality_json),
            "quality_csv": str(quality_csv),
            "quality_latest_json": str(quality_latest_json),
            "quality_latest_csv": str(quality_latest_csv),
            "combined_json": str(combined_json),
            "combined_latest_json": str(combined_latest_json),
        },
        "bucket": bucket_report,
        "quality": quality_report,
    }

    for path, payload in [
        (bucket_json, bucket_report),
        (bucket_latest_json, bucket_report),
        (quality_json, quality_report),
        (quality_latest_json, quality_report),
        (combined_json, combined_report),
        (combined_latest_json, combined_report),
    ]:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return combined_report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    args = parser.parse_args()
    report = write_outputs(Path(args.root))
    print(
        json.dumps(
            {
                "generated_at": report.get("generated_at"),
                "policy_effect": report.get("policy_effect"),
                "candidate_rows": (report.get("bucket") or {}).get("candidate_rows"),
                "bucket_summary": (report.get("bucket") or {}).get("bucket_summary"),
                "quality_summary": (report.get("quality") or {}).get("quality_summary"),
                "combined_latest_json": (report.get("outputs") or {}).get("combined_latest_json"),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
