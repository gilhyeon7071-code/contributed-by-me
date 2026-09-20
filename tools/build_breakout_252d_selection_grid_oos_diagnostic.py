"""Read-only OOS distribution diagnostic for the predeclared breakout grid."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TRADES = LOG_DIR / "breakout_252d_selection_grid_execution_trades_latest.csv"
OUT_CSV = LOG_DIR / "breakout_252d_selection_grid_oos_diagnostic_latest.csv"
OUT_JSON = LOG_DIR / "breakout_252d_selection_grid_oos_diagnostic_latest.json"
OOS_PERIOD = "P4_202604_202606"


def main() -> int:
    trades = pd.read_csv(TRADES)
    oos = trades.loc[trades["period"].eq(OOS_PERIOD)].copy()
    rows: list[dict[str, object]] = []
    for (horizon, variant), group in oos.groupby(["horizon", "selection_variant"], sort=True):
        ordered = group.sort_values("net_return", ascending=False)
        trimmed = ordered.iloc[5:]
        rows.append(
            {
                "horizon": horizon,
                "selection_variant": variant,
                "oos_period": OOS_PERIOD,
                "trades": int(len(group)),
                "avg_net": float(group["net_return"].mean()),
                "median_net": float(group["net_return"].median()),
                "win_rate": float(group["net_return"].gt(0).mean()),
                "top5_net_sum": float(ordered.head(5)["net_return"].sum()),
                "avg_net_excluding_top5": float(trimmed["net_return"].mean()) if len(trimmed) else None,
                "top_return": float(ordered["net_return"].iloc[0]),
                "bottom_return": float(ordered["net_return"].iloc[-1]),
            }
        )
    diagnostic = pd.DataFrame(rows)
    diagnostic.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(TRADES),
        "oos_period": OOS_PERIOD,
        "method": "net-return distribution and top-five-trimmed mean; no strategy parameters changed",
        "rows": diagnostic.to_dict(orient="records"),
        "operational_change": False,
        "broker_order": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "variants": len(diagnostic)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
