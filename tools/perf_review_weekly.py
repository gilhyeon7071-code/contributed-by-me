from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _load_json(path: Path) -> Dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly performance review report")
    ap.add_argument("--lookback-days", type=int, default=7)
    args = ap.parse_args()

    files = sorted(LOG_DIR.glob("paper_pnl_summary_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        print("[STOP] no pnl summary files")
        return 2

    cutoff = dt.datetime.now() - dt.timedelta(days=max(1, int(args.lookback_days)))
    picked: List[Path] = [p for p in files if dt.datetime.fromtimestamp(p.stat().st_mtime) >= cutoff]
    if not picked:
        picked = files[:7]

    rows: List[Dict[str, object]] = []
    for p in picked:
        j = _load_json(p)
        eq = j.get("equity", {}) if isinstance(j, dict) else {}
        rows.append(
            {
                "file": str(p),
                "generated_at": str(j.get("generated_at", "")),
                "last_exit_date": str(eq.get("last_exit_date", "")),
                "last_day_ret": float(eq.get("last_day_ret", 0.0) or 0.0),
                "max_drawdown_pct": float(eq.get("max_drawdown_pct", 0.0) or 0.0),
                "end_equity": float(eq.get("end_equity", 1.0) or 1.0),
            }
        )

    n = len(rows)
    avg_day = sum(float(r["last_day_ret"]) for r in rows) / n if n else 0.0
    worst_dd = min(float(r["max_drawdown_pct"]) for r in rows) if n else 0.0
    latest_equity = float(rows[0]["end_equity"]) if n else 1.0

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "lookback_days": int(args.lookback_days),
        "samples": n,
        "avg_last_day_ret": avg_day,
        "worst_max_drawdown_pct": worst_dd,
        "latest_end_equity": latest_equity,
        "rows": rows,
    }

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"perf_review_weekly_{ts}.json"
    out_md = LOG_DIR / f"perf_review_weekly_{ts}.md"
    out_latest = LOG_DIR / "perf_review_weekly_latest.json"

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md = [
        f"# Weekly Performance Review ({ts})",
        "",
        f"- lookback_days: {args.lookback_days}",
        f"- samples: {n}",
        f"- avg_last_day_ret: {avg_day:.6f}",
        f"- worst_max_drawdown_pct: {worst_dd:.6f}",
        f"- latest_end_equity: {latest_equity:.6f}",
        "",
        "## Rows",
        json.dumps(rows, ensure_ascii=False, indent=2),
    ]
    out_md.write_text("\n".join(md), encoding="utf-8")

    print(f"[OK] json={out_json}")
    print(f"[OK] md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
