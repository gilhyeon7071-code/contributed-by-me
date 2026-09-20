from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
SHADOW_CSV = LOGS / "news_multisource_shadow_score_review_latest.csv"
OUT_CSV = LOGS / "news_multisource_shadow_score_impact_sim_latest.csv"
OUT_JSON = LOGS / "news_multisource_shadow_score_impact_sim_latest.json"

KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _num(value: object, default: float = 0.0) -> float:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return float(default)
        return float(parsed)
    except Exception:
        return float(default)


def _norm_code(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _min_entry_score(row: pd.Series) -> float:
    # Mirrors the practical paper-engine floor family without changing policy.
    strategy = str(row.get("strategy_type") or "").strip().upper()
    if strategy == "SURGE":
        return 0.015
    return 0.020


def main() -> int:
    status = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "mode": "read_only_multisource_shadow_score_impact_sim",
        "candidate_input": str(CANDIDATES_CSV),
        "shadow_input": str(SHADOW_CSV),
        "output_csv": str(OUT_CSV),
        "score_effect": False,
        "trading_effect": False,
        "quality": "FAIL",
        "reason": "",
    }
    if not CANDIDATES_CSV.exists():
        status["reason"] = "candidate_input_missing"
        OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1
    if not SHADOW_CSV.exists():
        status["reason"] = "shadow_input_missing"
        OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    cand = _read_csv(CANDIDATES_CSV)
    shadow = _read_csv(SHADOW_CSV)
    if cand.empty or shadow.empty or "code" not in cand.columns or "code" not in shadow.columns:
        status["reason"] = "input_empty_or_missing_code"
        OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    cand = cand.copy()
    shadow = shadow.copy()
    cand["code"] = cand["code"].map(_norm_code)
    shadow["code"] = shadow["code"].map(_norm_code)
    cand = cand[cand["code"] != ""].drop_duplicates("code", keep="first")
    shadow = shadow[shadow["code"] != ""].drop_duplicates("code", keep="first")

    current_rank = cand[["code", "final_score"]].copy()
    current_rank["final_score"] = pd.to_numeric(current_rank["final_score"], errors="coerce").fillna(0.0)
    current_rank = current_rank.sort_values(["final_score", "code"], ascending=[False, True]).reset_index(drop=True)
    current_rank["rank_before"] = current_rank.index + 1

    merged = cand.merge(
        shadow[["code", "shadow_news_score", "shadow_action", "shadow_score_reason"]],
        on="code",
        how="left",
    )
    merged["shadow_news_score"] = pd.to_numeric(merged.get("shadow_news_score"), errors="coerce").fillna(0.0)
    merged["shadow_action"] = merged.get("shadow_action", "").fillna("").astype(str)
    merged["current_news_score"] = pd.to_numeric(merged.get("news_score"), errors="coerce").fillna(0.0)
    merged["current_final_score"] = pd.to_numeric(merged.get("final_score"), errors="coerce").fillna(0.0)
    merged["news_weight"] = pd.to_numeric(merged.get("final_score_w_news"), errors="coerce").fillna(0.0)
    merged["shadow_news_score_applied"] = merged[["current_news_score", "shadow_news_score"]].max(axis=1)
    merged["news_score_delta"] = (merged["shadow_news_score_applied"] - merged["current_news_score"]).clip(lower=0.0)
    merged["final_score_delta_sim"] = (merged["news_score_delta"] * merged["news_weight"]).round(6)
    merged["final_score_shadow_sim"] = (merged["current_final_score"] + merged["final_score_delta_sim"]).clip(0.0, 1.0).round(6)

    simulated_rank = merged[["code", "final_score_shadow_sim"]].copy()
    simulated_rank = simulated_rank.sort_values(["final_score_shadow_sim", "code"], ascending=[False, True]).reset_index(drop=True)
    simulated_rank["rank_after"] = simulated_rank.index + 1

    out = merged.merge(current_rank[["code", "rank_before"]], on="code", how="left")
    out = out.merge(simulated_rank[["code", "rank_after"]], on="code", how="left")
    out["rank_delta"] = pd.to_numeric(out["rank_before"], errors="coerce") - pd.to_numeric(out["rank_after"], errors="coerce")
    out["min_entry_score_sim"] = out.apply(_min_entry_score, axis=1)
    out["crosses_min_entry_score_sim"] = (
        (out["current_final_score"] < out["min_entry_score_sim"])
        & (out["final_score_shadow_sim"] >= out["min_entry_score_sim"])
    )
    out["score_only_entry_effect"] = out.apply(
        lambda r: "SCORE_ONLY_CROSSES_MIN_ENTRY"
        if bool(r["crosses_min_entry_score_sim"])
        else (
            "SCORE_ONLY_ALREADY_ABOVE_MIN"
            if _num(r.get("current_final_score"), 0.0) >= _num(r.get("min_entry_score_sim"), 0.0)
            else "SCORE_ONLY_STILL_BELOW_MIN"
        ),
        axis=1,
    )
    out["sim_score_effect"] = False
    out["sim_trading_effect"] = False

    cols = [
        "code",
        "name",
        "current_news_score",
        "shadow_news_score",
        "shadow_news_score_applied",
        "news_weight",
        "current_final_score",
        "final_score_delta_sim",
        "final_score_shadow_sim",
        "rank_before",
        "rank_after",
        "rank_delta",
        "min_entry_score_sim",
        "score_only_entry_effect",
        "sector_entry_allowed",
        "execution_pool",
        "shadow_action",
        "shadow_score_reason",
        "sim_score_effect",
        "sim_trading_effect",
    ]
    cols = [c for c in cols if c in out.columns]
    out = out[cols].sort_values(["final_score_delta_sim", "rank_delta", "final_score_shadow_sim"], ascending=[False, False, False])
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    changed = out[pd.to_numeric(out["final_score_delta_sim"], errors="coerce").fillna(0.0) > 0].copy()
    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "rows": int(len(out)),
            "changed_rows": int(len(changed)),
            "max_final_score_delta": float(changed["final_score_delta_sim"].max()) if len(changed) else 0.0,
            "score_only_crosses_min_entry_rows": int((out["score_only_entry_effect"] == "SCORE_ONLY_CROSSES_MIN_ENTRY").sum()),
            "top_changes": changed[
                [
                    "code",
                    "name",
                    "current_news_score",
                    "shadow_news_score",
                    "current_final_score",
                    "final_score_delta_sim",
                    "final_score_shadow_sim",
                    "rank_before",
                    "rank_after",
                    "rank_delta",
                    "score_only_entry_effect",
                ]
            ].to_dict(orient="records"),
            "score_effect": False,
            "trading_effect": False,
        }
    )
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[NEWS_MULTI_SHADOW_SIM] quality=PASS rows={len(out)} changed={len(changed)} latest={OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
