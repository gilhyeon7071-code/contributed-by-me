from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"


def _latest_orders_exec() -> Path:
    files = sorted(PAPER_DIR.glob("orders_*_exec.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError("no orders_*_exec.xlsx")
    return files[0]


def main() -> int:
    ap = argparse.ArgumentParser(description="Canary live-test guarded runner")
    ap.add_argument("--orders-path", default="")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--max-orders", type=int, default=1)
    ap.add_argument("--max-total-qty", type=int, default=10)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm", default="", help="must be CANARY to apply")
    args = ap.parse_args()

    orders_path = Path(args.orders_path) if args.orders_path else _latest_orders_exec()
    if not orders_path.exists():
        print(f"[STOP] orders file not found: {orders_path}")
        return 2

    df = pd.read_excel(orders_path, dtype=str)
    need = ["side", "code", "fill_qty"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"[STOP] missing columns: {miss}")
        return 2

    x = df.copy()
    x["side"] = x["side"].astype(str).str.upper().str.strip()
    x["qty"] = x["fill_qty"].astype(str).str.replace(",", "", regex=False).astype(float).fillna(0).astype(int)
    elig = x[(x["side"].isin(["BUY", "SELL"])) & (x["qty"] > 0)].copy()

    if len(elig) > int(args.max_orders):
        print(f"[STOP] canary guard: eligible rows {len(elig)} > max_orders {args.max_orders}")
        return 2

    total_qty = int(elig["qty"].sum()) if len(elig) else 0
    if total_qty > int(args.max_total_qty):
        print(f"[STOP] canary guard: total_qty {total_qty} > max_total_qty {args.max_total_qty}")
        return 2

    cmd = [
        sys.executable,
        str(ROOT / "tools" / "kis_order_dispatch_from_exec.py"),
        "--orders-path",
        str(orders_path),
        "--mock",
        str(args.mock),
        "--max-orders",
        str(args.max_orders),
    ]

    if args.apply:
        if str(args.confirm).strip().upper() != "CANARY":
            print("[STOP] apply requires --confirm CANARY")
            return 2
        cmd.append("--apply")

    print("[RUN]", " ".join(cmd))
    rc = subprocess.call(cmd, cwd=str(ROOT))
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
