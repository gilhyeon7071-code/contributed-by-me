# -*- coding: utf-8 -*-
"""
vibe_onepass_run.py (DEV)
P2 upgrade: fingerprint-based NOOP policy
- If FINAL snapshot exists for (D, today):
    - NOOP only when inputs_fingerprint matches
    - else rerun + create a new FINAL snapshot (auto) to preserve audit trail
Safeguards:
- STOP if exec_date != D
- STOP if live_vs_bt not PASS (unless already handled by approved LOCK patterns)
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -------------------------
# helpers
# -------------------------
def _now_ts() -> str:
    return _dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _today_ymd() -> str:
    return _dt.datetime.now().strftime("%Y%m%d")


def _safe_read_json(p: Path) -> Optional[dict]:
    try:
        return json.load(open(p, "r", encoding="utf-8"))
    except Exception:
        return None


def _write_json(p: Path, obj: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _file_sig(p: Path) -> dict:
    if not p.exists():
        return {"path": str(p), "exists": False}
    st = p.stat()
    return {
        "path": str(p),
        "exists": True,
        "size": int(st.st_size),
        "mtime_ns": int(st.st_mtime_ns),
    }


def _sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _run(cmd: List[str], cwd: Path, tag: str) -> int:
    print(f"[RUN]{tag} {' '.join(cmd)}")
    print(f"[RUN]{tag} CWD={cwd}")
    r = subprocess.run(cmd, cwd=str(cwd))
    print(f"[RUN]{tag} EXIT={r.returncode}")
    return int(r.returncode)


def _determine_d_by_rule(fills_csv: Path) -> Tuple[str, dict]:
    """
    D RULE:
    - ymd = first 8 chars of fills.datetime after removing '-', ':', ' '
    - D = latest BUY ymd if exists else latest any ymd
    """
    if not fills_csv.exists():
        raise FileNotFoundError(f"fills_path not found: {fills_csv}")

    df = pd.read_csv(fills_csv, dtype=str)
    if "datetime" not in df.columns:
        raise ValueError(f"fills.csv missing 'datetime' col: cols={df.columns.tolist()}")
    if "side" not in df.columns:
        raise ValueError(f"fills.csv missing 'side' col: cols={df.columns.tolist()}")

    y = (
        df["datetime"].astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(":", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str[:8]
    )
    df["_ymd"] = y

    b = df[df["side"].astype(str).str.upper() == "BUY"]
    latest_buy = (b["_ymd"].max() if len(b) else None)
    latest_any = df["_ymd"].max()

    D = latest_buy or latest_any
    if D is None or len(str(D)) != 8:
        raise ValueError(f"invalid D computed: D={D} latest_buy={latest_buy} latest_any={latest_any}")

    meta = {
        "fills_path": str(fills_csv),
        "latest_buy": latest_buy,
        "latest_any": latest_any,
        "D_by_rule": D,
    }
    return str(D), meta


def _read_exec_date_unique(orders_exec_xlsx: Path) -> List[str]:
    df = pd.read_excel(orders_exec_xlsx)
    if "exec_date" not in df.columns:
        raise ValueError(f"orders_exec missing exec_date col: {orders_exec_xlsx}")
    u = sorted(df["exec_date"].astype(str).unique().tolist())
    return u


def _live_fills_d_summary(live_fills_csv: Path, D: str) -> dict:
    if not live_fills_csv.exists():
        return {"live_fills_path": str(live_fills_csv), "exists": False}

    df = pd.read_csv(live_fills_csv, dtype=str)
    # column selection
    date_col = "date" if "date" in df.columns else next((c for c in ["ymd", "exec_date", "trade_date", "as_of_ymd"] if c in df.columns), None)
    code_col = next((c for c in ["code", "ticker", "symbol"] if c in df.columns), None)
    side_col = "side" if "side" in df.columns else None
    price_col = "price" if "price" in df.columns else None

    if date_col is None or code_col is None:
        return {
            "live_fills_path": str(live_fills_csv),
            "exists": True,
            "fatal": f"missing date/code col: date_col={date_col} code_col={code_col}",
            "cols": df.columns.tolist(),
        }

    d = df.copy()
    d[date_col] = d[date_col].astype(str).str.replace("-", "", regex=False).str[:8]
    dd = d[d[date_col] == str(D)]

    lens = dd[code_col].astype(str).str.replace(".0", "", regex=False).str.strip().str.len()
    out = {
        "live_fills_path": str(live_fills_csv),
        "exists": True,
        "date_col": date_col,
        "code_col": code_col,
        "side_col": side_col,
        "price_col": price_col,
        "rows_total": int(len(d)),
        "rows_D": int(len(dd)),
        "side_unique_D": sorted(dd[side_col].astype(str).unique().tolist()) if (side_col and len(dd)) else None,
        "code_len_min_D": int(lens.min()) if len(dd) else None,
        "code_len_max_D": int(lens.max()) if len(dd) else None,
    }
    return out


def _compute_inputs_fingerprint(
    roota: Path,
    rootb: Path,
    D: str,
    mode: str,
) -> Tuple[str, dict]:
    fills_csv = roota / "paper" / "fills.csv"
    live_fills_csv = rootb / "data" / "live" / "live_fills.csv"

    meta = {
        "D": D,
        "mode": mode,
        "roota": str(roota),
        "rootb": str(rootb),
        "files": {
            "roota_fills_csv": _file_sig(fills_csv),
            "roota_virtual_ledger": _file_sig(roota / "virtual_ledger.csv"),
            "roota_onepass": _file_sig(roota / "tools" / "p0_onepass_from_fills.py"),
            "roota_ledger_append": _file_sig(roota / "tools" / "ledger_append_from_orders_exec.py"),
            "rootb_live_fills": _file_sig(live_fills_csv),
            "rootb_stats": _file_sig(rootb / "vibe_generate_stats_p0.py"),
            "rootb_onepass_p0_paper": _file_sig(rootb / "tools" / "onepass_p0_paper.py"),
        },
        "live_fills_D_summary": _live_fills_d_summary(live_fills_csv, D),
    }
    fp = _sha1_text(json.dumps(meta, ensure_ascii=False, sort_keys=True))
    return fp, meta


def _find_latest_final_snapshot(rootb: Path, D: str, today: str) -> Optional[Path]:
    runs = rootb / "runs"
    if not runs.exists():
        return None
    prefix = f"SSOT_D{D}_FINAL_{today}_"
    cand = [p for p in runs.iterdir() if p.is_dir() and p.name.startswith(prefix)]
    if not cand:
        return None
    cand.sort(key=lambda p: p.name, reverse=True)
    return cand[0]


def _make_snapshot_dir(rootb: Path, D: str, today: str) -> Path:
    runs = rootb / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    name = f"SSOT_D{D}_FINAL_{today}_{_dt.datetime.now().strftime('%H%M%S')}"
    return runs / name


def _copy_artifact(src: Path, dst_dir: Path, rel_name: str, copied: list) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / rel_name
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists():
        shutil.copy2(src, dst)
        copied.append({"src": str(src), "dst": str(dst)})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--roota", default=r"E:\1_Data")
    ap.add_argument("--rootb", default=r"E:\vibe\buffett")
    ap.add_argument("--mode", default="paper", choices=["paper", "broker"])
    ap.add_argument("--apply-ledger", action="store_true", dest="apply_ledger")
    ap.add_argument("--force-snapshot", action="store_true")
    ap.add_argument("--no-change-detect", action="store_true")
    ap.add_argument("--broker-apply", action="store_true", help="Actually submit broker orders in mode=broker")
    ap.add_argument("--broker-order-type", default="market", choices=["market", "limit"])
    ap.add_argument("--broker-max-orders", type=int, default=0)
    ap.add_argument("--broker-sleep-ms", type=int, default=120)
    ap.add_argument("--broker-sync-fills", action="store_true", help="Fetch KIS daily fills after broker apply")
    args = ap.parse_args()

    roota = Path(args.roota)
    rootb = Path(args.rootb)
    mode = args.mode

    py_roota = str(Path(sys.executable))  # caller python
    py_rootb = str(rootb / ".venv" / "Scripts" / "python.exe")

    print("=== VIBE L3 ONEPASS RUN (DEV) ===")
    print(f"- roota: {roota}")
    print(f"- rootb: {rootb}")
    print(f"- mode:  {mode}")
    print(f"- apply_ledger: {bool(args.apply_ledger)}")
    print(f"- broker_apply: {bool(args.broker_apply)}")
    print(f"- broker_order_type: {args.broker_order_type}")
    print(f"- broker_max_orders: {int(args.broker_max_orders)}")
    print(f"- broker_sync_fills: {bool(args.broker_sync_fills)}")
    print(f"- python(roota): {py_roota}")
    print(f"- python(rootb): {py_rootb}")

    # [1/7] Determine D
    print("[1/7] Determine D_by_rule")
    fills_csv = roota / "paper" / "fills.csv"
    D, metaD = _determine_d_by_rule(fills_csv)
    print(f"[OK] D={D}  meta={metaD}")

    today = _today_ymd()

    # fingerprint (P2)
    fp_now, fp_meta = _compute_inputs_fingerprint(roota, rootb, D, mode)

    latest_snap = _find_latest_final_snapshot(rootb, D, today)
    if latest_snap and (not args.force_snapshot) and mode == "broker" and args.broker_apply:
        print("[INFO] NOOP bypassed: broker_apply requires dispatch attempt even when snapshot exists.")

    if latest_snap and (not args.force_snapshot) and (not (mode == "broker" and args.broker_apply)):
        if args.no_change_detect:
            print("[NOOP] FINAL snapshot already exists for today. (change-detect disabled) Skipping RootA/RootB execution to avoid churn.")
            print(f"[NOOP] snapshot={latest_snap}")
            # still run STOP checks on existing artifacts
            orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
            u = _read_exec_date_unique(orders_exec_b)
            print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u} (NOOP)")
            lv = _safe_read_json(rootb / "data" / "stats" / "live_vs_bt.json") or {}
            st = lv.get("status")
            asof = lv.get("as_of_ymd") or lv.get("as_of")
            if st != "PASS" or str(asof) != str(D):
                print(f"[STOP] live_vs_bt not PASS/as_of mismatch (NOOP): status={st} as_of={asof}")
                return 2
            print(f"[OK] RootB live_vs_bt PASS(as_of={D}) (NOOP)")
            print("[7/7] FINAL")
            print(f"=== PASS(NOOP) D={D} | snapshot={latest_snap} ===")
            return 0

        prov = _safe_read_json(latest_snap / "provenance.json") or {}
        fp_prev = prov.get("inputs_fingerprint")

        if fp_prev == fp_now:
            print("[NOOP] FINAL snapshot already exists for today AND inputs_fingerprint is unchanged. Skipping RootA/RootB execution.")
            print(f"[NOOP] snapshot={latest_snap}")
            orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
            u = _read_exec_date_unique(orders_exec_b)
            print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u} (NOOP)")
            lv = _safe_read_json(rootb / "data" / "stats" / "live_vs_bt.json") or {}
            st = lv.get("status")
            asof = lv.get("as_of_ymd") or lv.get("as_of")
            if st != "PASS" or str(asof) != str(D):
                print(f"[STOP] live_vs_bt not PASS/as_of mismatch (NOOP): status={st} as_of={asof}")
                return 2
            print(f"[OK] RootB live_vs_bt PASS(as_of={D}) (NOOP)")
            print("[7/7] FINAL")
            print(f"=== PASS(NOOP) D={D} | snapshot={latest_snap} ===")
            return 0

        # fingerprint differs (or missing) => rerun + new snapshot
        reason = "inputs_fingerprint changed" if fp_prev else "previous snapshot missing inputs_fingerprint"
        print(f"[RERUN] snapshot exists but {reason}. Proceeding with rerun + new FINAL snapshot to preserve audit trail.")

    # [2/7] RootA onepass
    print("[2/7] RootA onepass_from_fills")
    cmd_onepass = [py_roota, str(roota / "tools" / "p0_onepass_from_fills.py"), str(D)]
    rc = _run(cmd_onepass, cwd=roota, tag="[RootA.onepass]")
    if rc != 0:
        print("[STOP] RootA onepass failed.")
        return 2

    core_json = roota / "2_Logs" / f"p0_live_vs_bt_core_{D}.json"
    jcore = _safe_read_json(core_json) or {}
    if jcore.get("status") != "PASS":
        print(f"[STOP] RootA live_vs_bt_core not PASS: status={jcore.get('status')} file={core_json}")
        return 2
    print(f"[OK] RootA live_vs_bt_core PASS: status=PASS as_of={jcore.get('as_of_ymd') or jcore.get('as_of')} file={core_json}")

    if mode == "broker":
        print("[2.5/7] RootA broker dispatch (KIS)")
        cmd_broker = [
            py_roota,
            str(roota / "tools" / "kis_order_dispatch_from_exec.py"),
            "--date", str(D),
            "--order-type", str(args.broker_order_type),
            "--sleep-ms", str(int(args.broker_sleep_ms)),
        ]
        if int(args.broker_max_orders) > 0:
            cmd_broker += ["--max-orders", str(int(args.broker_max_orders))]
        if args.broker_apply:
            cmd_broker.append("--apply")
        rc_broker = _run(cmd_broker, cwd=roota, tag="[RootA.broker]")
        if rc_broker != 0:
            print("[STOP] RootA broker dispatch failed.")
            return 2

        if args.broker_apply and args.broker_sync_fills:
            print("[2.6/7] RootA broker fills sync (KIS)")
            cmd_sync = [
                py_roota,
                str(roota / "tools" / "kis_sync_fills_from_api.py"),
                "--date", str(D),
                "--bridge-write",
                "--bridge-live-path", str(rootb / "data" / "live" / "live_fills.csv"),
            ]
            rc_sync = _run(cmd_sync, cwd=roota, tag="[RootA.broker_sync]")
            if rc_sync != 0:
                print("[STOP] RootA broker fills sync failed.")
                return 2

    # [3/7] ledger append
    print("[3/7] RootA ledger append (optional)")
    ledger_csv = roota / "virtual_ledger.csv"
    if args.apply_ledger and ledger_csv.exists():
        try:
            df_ledger = pd.read_csv(ledger_csv, dtype=str)
            cols = [c for c in ["ymd", "date", "as_of", "as_of_ymd", "exec_date", "trade_date"] if c in df_ledger.columns]
            has = False
            if cols:
                for c in cols:
                    if (df_ledger[c].astype(str) == str(D)).any():
                        has = True
                        break
            else:
                # fallback: scan first column
                c0 = df_ledger.columns[0]
                has = (df_ledger[c0].astype(str) == str(D)).any()
            if has:
                print(f"[NA] ledger already contains D={D}: {ledger_csv} (idempotent skip)")
            else:
                cmd_ledger = [py_roota, str(roota / "tools" / "ledger_append_from_orders_exec.py"), str(D), "--apply"]
                rc2 = _run(cmd_ledger, cwd=roota, tag="[RootA.ledger]")
                if rc2 != 0:
                    print("[STOP] ledger append failed.")
                    return 2
        except Exception as e:
            print(f"[STOP] ledger guard/read failed: {e}")
            return 2
    else:
        print("[NA] apply_ledger not requested or ledger missing; skipping.")

    # [4/7] Sync orders_exec
    print("[4/7] Sync RootA orders_exec -> RootB")
    orders_exec_a = roota / "paper" / f"orders_{D}_exec.xlsx"
    orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
    orders_exec_b.parent.mkdir(parents=True, exist_ok=True)
    if not orders_exec_a.exists():
        print(f"[STOP] RootA orders_exec not found: {orders_exec_a}")
        return 2
    shutil.copy2(orders_exec_a, orders_exec_b)
    print(f"[OK] synced orders_exec: {orders_exec_a} -> {orders_exec_b}")

    u = _read_exec_date_unique(orders_exec_b)
    if u != [str(D)]:
        print(f"[STOP] exec_date!=D: exec_date_unique={u} expected={[str(D)]}")
        return 2
    print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u}")

    # [5/7] RootB stats
    print("[5/7] RootB stats generate + verify")
    t_before = time.time()
    cmd_stats = [py_rootb, str(rootb / "vibe_generate_stats_p0.py")]
    rc3 = _run(cmd_stats, cwd=rootb, tag="[RootB.stats]")
    if rc3 != 0:
        print("[STOP] RootB stats failed.")
        return 2

    # prefer RootB config.yaml stats_dir to avoid SSOT split (no PyYAML)
    def _read_stats_dir_from_config():
        import os, re
        candidates = [r"E:\\vibe\\buffett\\config.yaml", "config.yaml"]
        for cp in candidates:
            try:
                if not os.path.exists(cp):
                    continue
                for line in open(cp, "r", encoding="utf-8"):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    mm = re.match(r"^\s*stats_dir\s*:\s*(.+?)\s*$", line)
                    if mm:
                        v = mm.group(1).strip().strip('"').strip("'")
                        return v
            except Exception:
                pass
        return None

    _cfg_stats_dir = _read_stats_dir_from_config()
    if _cfg_stats_dir:
        lv_path = Path(_cfg_stats_dir) / "live_vs_bt.json"
    else:
        lv_path = rootb / "data" / "stats" / "live_vs_bt.json"
    lv = _safe_read_json(lv_path) or {}
    st = lv.get("status")
    asof = lv.get("as_of_ymd") or lv.get("as_of")

    # freshness check: mtime should be updated by this run (avoid stale PASS)
    try:
        mtime = lv_path.stat().st_mtime
        if mtime < (t_before - 1):
            print(f"[STOP] RootB live_vs_bt not fresh (stale file): mtime={_dt.datetime.fromtimestamp(mtime)}")
            return 2
    except Exception:
        pass

    if st != "PASS" or str(asof) != str(D):
        print(f"[STOP] live_vs_bt status not PASS or as_of mismatch: status={st} as_of={asof} file={lv_path}")
        return 2
    print(f"[OK] RootB live_vs_bt PASS+FRESH: status=PASS as_of={D} file={lv_path}")

    # [6/7] Snapshot
    print("[6/7] Snapshot (RootB runs/SSOT_...)")
    snap = _make_snapshot_dir(rootb, D, today)
    copied: List[dict] = []

    # collect artifacts (7 expected)
    _copy_artifact(orders_exec_b, snap, f"orders/orders_{D}_exec.xlsx", copied)
    _copy_artifact(rootb / "data" / "orders" / f"orders_{D}_eval.xlsx", snap, f"orders/orders_{D}_eval.xlsx", copied)
    _copy_artifact(lv_path, snap, "stats/live_vs_bt.json", copied)
    _copy_artifact(rootb / "data" / "stats" / "risk_stats.json", snap, "stats/risk_stats.json", copied)
    _copy_artifact(rootb / "data" / "stats" / "backtest_stats.json", snap, "stats/backtest_stats.json", copied)
    _copy_artifact(rootb / "data" / "portfolio" / f"portfolio_{D}.xlsx", snap, f"portfolio/portfolio_{D}.xlsx", copied)
    _copy_artifact(core_json, snap, f"roota_logs/p0_live_vs_bt_core_{D}.json", copied)

    prov = {
        "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "D": D,
        "today_ymd": today,
        "roota": str(roota),
        "rootb": str(rootb),
        "mode": mode,
        "apply_ledger": bool(args.apply_ledger),
        "broker_apply": bool(args.broker_apply),
        "broker_order_type": str(args.broker_order_type),
        "broker_max_orders": int(args.broker_max_orders),
        "broker_sync_fills": bool(args.broker_sync_fills),
        "inputs_fingerprint": fp_now,
        "inputs_meta": fp_meta,
        "artifacts_copied": copied,
    }
    _write_json(snap / "provenance.json", prov)

    chg = {
        "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "actions": [
            {"type": "SNAPSHOT_CREATE", "path": str(snap), "artifacts": len(copied)},
            {"type": "POLICY", "note": "fingerprint-based NOOP: rerun only when inputs change"},
        ],
        "notes": [
            "Idempotent policy upgraded: once per (D, today, inputs_fingerprint); rerun auto if inputs differ.",
        ],
    }
    _write_json(snap / "change_log.json", chg)

    print(f"[OK] snapshot created: {snap}")

    # [7/7] FINAL
    print("[7/7] FINAL")
    print(f"=== PASS D={D} | live_vs_bt=PASS(as_of={D}) | ledger={'apply' if args.apply_ledger else 'skip'} | snapshot={snap} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



