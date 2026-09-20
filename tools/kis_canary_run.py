from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
logger = logging.getLogger("kis_canary_run")


def _latest_orders_exec() -> Path:
    files = sorted(PAPER_DIR.glob("orders_*_exec.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError("no orders_*_exec.xlsx")
    return files[0]


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

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
        logger.error("[STOP] orders file not found: %s", orders_path)
        return 2

    df = pd.read_excel(orders_path, dtype=str)
    need = ["side", "code", "fill_qty"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        logger.error("[STOP] missing columns: %s", miss)
        return 2

    x = df.copy()
    x["side"] = x["side"].astype(str).str.upper().str.strip()
    x["qty"] = x["fill_qty"].astype(str).str.replace(",", "", regex=False).astype(float).fillna(0).astype(int)
    # Canary gate evaluates entry chain only; SELL liquidation rows can be large and are handled separately.
    elig = x[(x["side"] == "BUY") & (x["qty"] > 0)].copy()

    max_orders = int(args.max_orders)
    if len(elig) > max_orders:
        logger.warning(
            "[GUARD] eligible rows %s > max_orders %s, limiting canary scope to first %s rows",
            len(elig),
            max_orders,
            max_orders,
        )
        elig = elig.head(max_orders).copy()

    total_qty = int(elig["qty"].sum()) if len(elig) else 0
    if total_qty > int(args.max_total_qty):
        logger.error("[STOP] canary guard: total_qty %s > max_total_qty %s", total_qty, args.max_total_qty)
        return 2

    cmd = [
        sys.executable,
        str(ROOT / "tools" / "kis_order_dispatch_from_exec.py"),
        "--orders-path",
        str(orders_path),
        "--mock",
        str(args.mock),
        "--max-orders",
        str(max_orders),
    ]

    if args.apply:
        if str(args.confirm).strip().upper() != "CANARY":
            logger.error("[STOP] apply requires --confirm CANARY")
            return 2
        cmd.append("--apply")

    logger.info("[RUN] %s", " ".join(cmd))
    rc = subprocess.call(cmd, cwd=str(ROOT))
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())

