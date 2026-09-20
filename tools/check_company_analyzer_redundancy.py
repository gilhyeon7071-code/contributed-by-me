"""Read-only diagnostic: does company_analyzer.total_score add information beyond the
simpler local 5-factor rank blend (base_ser) already computed in _apply_fundamental_overlay?

Does not modify generate_candidates_v41_1.py or company_analyzer.py. No score/threshold/
Gate/order/fill/ledger/config change.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import company_analyzer as ca_mod  # noqa: E402

FUND_FILE = ROOT / "_cache" / "dart_fundamental_20260723.csv"

REGIME_WEIGHTS_LOCAL = {
    "BULL": {"value": 0.10, "quality": 0.15, "growth": 0.40, "stability": 0.10, "supply": 0.25},
}


def _rank01(s: pd.Series, higher_is_better: bool) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    r = x.rank(pct=True, ascending=higher_is_better)
    return r.fillna(0.5)


def main() -> int:
    df = pd.read_csv(FUND_FILE, dtype=str)
    for c in ["revenue_growth", "op_growth", "np_growth", "ROE", "ROA", "OPM", "NPM", "debt_ratio", "current_ratio", "interest_coverage"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["ROE", "OPM", "debt_ratio"]).reset_index(drop=True)
    print(f"[DATA] rows={len(df)} file={FUND_FILE.name}")

    # base_ser: local 5-factor rank blend (value factor skipped -- no PER/PBR in DART snapshot,
    # matches production behavior when those columns are absent: _component_rank drops missing specs)
    quality_r = pd.concat([_rank01(df["ROE"], True), _rank01(df["ROA"], True), _rank01(df["OPM"], True), _rank01(df["NPM"], True)], axis=1).mean(axis=1)
    growth_r = pd.concat([_rank01(df["revenue_growth"], True), _rank01(df["op_growth"], True), _rank01(df["np_growth"], True)], axis=1).mean(axis=1)
    stability_r = pd.concat([_rank01(df["debt_ratio"], False), _rank01(df["current_ratio"], True), _rank01(df["interest_coverage"], True)], axis=1).mean(axis=1)

    w = REGIME_WEIGHTS_LOCAL["BULL"]
    w_sum_no_value_no_supply = w["quality"] + w["growth"] + w["stability"]
    base_ser = (
        quality_r * (w["quality"] / w_sum_no_value_no_supply)
        + growth_r * (w["growth"] / w_sum_no_value_no_supply)
        + stability_r * (w["stability"] / w_sum_no_value_no_supply)
    )

    analyzer = ca_mod.CompanyAnalyzer()
    ca_scores = []
    for _, row in df.iterrows():
        fd = {
            "ROE": row["ROE"], "ROA": row["ROA"], "OPM": row["OPM"], "NPM": row["NPM"],
            "revenue_growth": row["revenue_growth"], "op_growth": row["op_growth"], "np_growth": row["np_growth"],
            "debt_ratio": row["debt_ratio"], "current_ratio": row["current_ratio"], "interest_coverage": row["interest_coverage"],
        }
        fd = {k: v for k, v in fd.items() if pd.notna(v)}
        sc = analyzer.analyze(code=str(row["code"]), name=str(row.get("corp_name", "")), fundamental_data=fd, price_df=None, regime="BULL")
        ca_scores.append(sc.total_score)

    ca_ser = pd.Series(ca_scores, index=df.index)

    spearman = base_ser.corr(ca_ser, method="spearman")
    pearson = base_ser.corr(ca_ser, method="pearson")
    print(f"[RESULT] base_ser vs company_analyzer.total_score  spearman={spearman:.4f} pearson={pearson:.4f}")
    print(f"[RESULT] base_ser stats: mean={base_ser.mean():.4f} std={base_ser.std():.4f}")
    print(f"[RESULT] ca_ser   stats: mean={ca_ser.mean():.4f} std={ca_ser.std():.4f}")
    top10_base = set(df.loc[base_ser.sort_values(ascending=False).head(10).index, "code"])
    top10_ca = set(df.loc[ca_ser.sort_values(ascending=False).head(10).index, "code"])
    overlap = len(top10_base & top10_ca)
    print(f"[RESULT] TOP10 overlap: {overlap}/10 codes in common")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
