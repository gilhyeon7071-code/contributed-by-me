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
import logging


# -------------------------
# helpers
# -------------------------


logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
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


def _run(cmd: List[str], cwd: Path, tag: str, env: Optional[dict] = None) -> int:
    _log_print(f"[RUN]{tag} {' '.join(cmd)}")
    _log_print(f"[RUN]{tag} CWD={cwd}")
    r = subprocess.run(cmd, cwd=str(cwd), env=env)
    _log_print(f"[RUN]{tag} EXIT={r.returncode}")
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
    ap.add_argument("--roota", default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument("--rootb", default=str(Path(__file__).resolve().parents[1].parent / "vibe" / "buffett"))
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

    _log_print("=== VIBE L3 ONEPASS RUN (DEV) ===")
    _log_print(f"- roota: {roota}")
    _log_print(f"- rootb: {rootb}")
    _log_print(f"- mode:  {mode}")
    _log_print(f"- apply_ledger: {bool(args.apply_ledger)}")
    _log_print(f"- broker_apply: {bool(args.broker_apply)}")
    _log_print(f"- broker_order_type: {args.broker_order_type}")
    _log_print(f"- broker_max_orders: {int(args.broker_max_orders)}")
    _log_print(f"- broker_sync_fills: {bool(args.broker_sync_fills)}")
    _log_print(f"- python(roota): {py_roota}")
    _log_print(f"- python(rootb): {py_rootb}")

    # [1/7] Determine D
    _log_print("[1/7] Determine D_by_rule")
    fills_csv = roota / "paper" / "fills.csv"
    D, metaD = _determine_d_by_rule(fills_csv)
    _log_print(f"[OK] D={D}  meta={metaD}")

    today = _today_ymd()

    # fingerprint (P2)
    fp_now, fp_meta = _compute_inputs_fingerprint(roota, rootb, D, mode)

    latest_snap = _find_latest_final_snapshot(rootb, D, today)
    if latest_snap and (not args.force_snapshot) and mode == "broker" and args.broker_apply:
        _log_print("[INFO] NOOP bypassed: broker_apply requires dispatch attempt even when snapshot exists.")

    if latest_snap and (not args.force_snapshot) and (not (mode == "broker" and args.broker_apply)):
        if args.no_change_detect:
            _log_print("[NOOP] FINAL snapshot already exists for today. (change-detect disabled) Skipping RootA/RootB execution to avoid churn.")
            _log_print(f"[NOOP] snapshot={latest_snap}")
            # still run STOP checks on existing artifacts
            orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
            u = _read_exec_date_unique(orders_exec_b)
            _log_print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u} (NOOP)")
            lv = _safe_read_json(rootb / "data" / "stats" / "live_vs_bt.json") or {}
            st = lv.get("status")
            asof = lv.get("as_of_ymd") or lv.get("as_of")
            if st != "PASS" or str(asof) != str(D):
                _log_print(f"[STOP] live_vs_bt not PASS/as_of mismatch (NOOP): status={st} as_of={asof}")
                return 2
            _log_print(f"[OK] RootB live_vs_bt PASS(as_of={D}) (NOOP)")
            _log_print("[7/7] FINAL")
            _log_print(f"=== PASS(NOOP) D={D} | snapshot={latest_snap} ===")
            return 0

        prov = _safe_read_json(latest_snap / "provenance.json") or {}
        fp_prev = prov.get("inputs_fingerprint")

        if fp_prev == fp_now:
            _log_print("[NOOP] FINAL snapshot already exists for today AND inputs_fingerprint is unchanged. Skipping RootA/RootB execution.")
            _log_print(f"[NOOP] snapshot={latest_snap}")
            orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
            u = _read_exec_date_unique(orders_exec_b)
            _log_print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u} (NOOP)")
            lv = _safe_read_json(rootb / "data" / "stats" / "live_vs_bt.json") or {}
            st = lv.get("status")
            asof = lv.get("as_of_ymd") or lv.get("as_of")
            if st != "PASS" or str(asof) != str(D):
                _log_print(f"[STOP] live_vs_bt not PASS/as_of mismatch (NOOP): status={st} as_of={asof}")
                return 2
            _log_print(f"[OK] RootB live_vs_bt PASS(as_of={D}) (NOOP)")
            _log_print("[7/7] FINAL")
            _log_print(f"=== PASS(NOOP) D={D} | snapshot={latest_snap} ===")
            return 0

        # fingerprint differs (or missing) => rerun + new snapshot
        reason = "inputs_fingerprint changed" if fp_prev else "previous snapshot missing inputs_fingerprint"
        _log_print(f"[RERUN] snapshot exists but {reason}. Proceeding with rerun + new FINAL snapshot to preserve audit trail.")

    # [2/7] RootA onepass
    _log_print("[2/7] RootA onepass_from_fills")
    cmd_onepass = [py_roota, str(roota / "tools" / "p0_onepass_from_fills.py"), str(D)]
    rc = _run(cmd_onepass, cwd=roota, tag="[RootA.onepass]")
    if rc != 0:
        _log_print("[STOP] RootA onepass failed.")
        return 2

    core_json = roota / "2_Logs" / f"p0_live_vs_bt_core_{D}.json"
    jcore = _safe_read_json(core_json) or {}
    if jcore.get("status") != "PASS":
        _log_print(f"[STOP] RootA live_vs_bt_core not PASS: status={jcore.get('status')} file={core_json}")
        return 2
    _log_print(f"[OK] RootA live_vs_bt_core PASS: status=PASS as_of={jcore.get('as_of_ymd') or jcore.get('as_of')} file={core_json}")

    if mode == "broker":
        _log_print("[2.5/7] RootA broker dispatch (KIS)")
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
            _log_print("[STOP] RootA broker dispatch failed.")
            return 2

        if args.broker_apply and args.broker_sync_fills:
            _log_print("[2.6/7] RootA broker fills sync (KIS)")
            cmd_sync = [
                py_roota,
                str(roota / "tools" / "kis_sync_fills_from_api.py"),
                "--date", str(D),
                "--bridge-write",
                "--bridge-live-path", str(rootb / "data" / "live" / "live_fills.csv"),
            ]
            rc_sync = _run(cmd_sync, cwd=roota, tag="[RootA.broker_sync]")
            if rc_sync != 0:
                _log_print("[STOP] RootA broker fills sync failed.")
                return 2

    # [3/7] ledger append
    _log_print("[3/7] RootA ledger append (optional)")
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
                _log_print(f"[NA] ledger already contains D={D}: {ledger_csv} (idempotent skip)")
            else:
                cmd_ledger = [py_roota, str(roota / "tools" / "ledger_append_from_orders_exec.py"), str(D), "--apply"]
                rc2 = _run(cmd_ledger, cwd=roota, tag="[RootA.ledger]")
                if rc2 != 0:
                    _log_print("[STOP] ledger append failed.")
                    return 2
        except Exception as e:
            _log_print(f"[STOP] ledger guard/read failed: {e}")
            return 2
    else:
        _log_print("[NA] apply_ledger not requested or ledger missing; skipping.")

    # [4/7] Sync orders_exec
    _log_print("[4/7] Sync RootA orders_exec -> RootB")
    orders_exec_a = roota / "paper" / f"orders_{D}_exec.xlsx"
    orders_exec_b = rootb / "data" / "orders" / f"orders_{D}_exec.xlsx"
    orders_exec_b.parent.mkdir(parents=True, exist_ok=True)
    if not orders_exec_a.exists():
        _log_print(f"[STOP] RootA orders_exec not found: {orders_exec_a}")
        return 2
    shutil.copy2(orders_exec_a, orders_exec_b)
    _log_print(f"[OK] synced orders_exec: {orders_exec_a} -> {orders_exec_b}")

    u = _read_exec_date_unique(orders_exec_b)
    if u != [str(D)]:
        _log_print(f"[STOP] exec_date!=D: exec_date_unique={u} expected={[str(D)]}")
        return 2
    _log_print(f"[OK] STOP(exec_date==D) PASS: exec_date_unique={u}")

    # [5/8] RootB paper ledger refresh
    _log_print("[5/8] RootB paper ledger refresh")
    paper_fills_csv = roota / "paper" / "fills.csv"
    paper_ledger_path = rootb / "data" / "ledger" / "paper_fills_ledger.csv"
    if not paper_fills_csv.exists():
        _log_print(f"[STOP] RootA fills not found: {paper_fills_csv}")
        return 2
    try:
        if str(rootb / "tools") not in sys.path:
            sys.path.insert(0, str(rootb / "tools"))
        from ledger_cost_model import (
            build_enriched_ledger,
            deduplicate_ledger,
            derive_open_positions_snapshot,
        )

        df_roota_fills = pd.read_csv(paper_fills_csv, dtype=str, encoding="utf-8-sig")
        df_roota_fills["source"] = "PAPER"
        df_roota_fills["run_id"] = _now_ts()
        df_roota_fills["as_of"] = (
            df_roota_fills.get("datetime", pd.Series(index=df_roota_fills.index, dtype=str))
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .str[:8]
        )
        rebuilt_ledger = build_enriched_ledger(df_roota_fills, root=str(rootb), default_source="PAPER")
        rebuilt_ledger = deduplicate_ledger(rebuilt_ledger)
        paper_ledger_path.parent.mkdir(parents=True, exist_ok=True)
        rebuilt_ledger.to_csv(paper_ledger_path, index=False, encoding="utf-8-sig")

        open_pos = derive_open_positions_snapshot(rebuilt_ledger, root=str(rootb), default_source="PAPER")
        portfolio_dir = rootb / "data" / "portfolio"
        portfolio_dir.mkdir(parents=True, exist_ok=True)
        portfolio_path = portfolio_dir / f"portfolio_{D}.xlsx"

        name_map: Dict[str, str] = {}
        market_map: Dict[str, str] = {}
        latest_portfolio_files = sorted(portfolio_dir.glob("portfolio_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
        for candidate in latest_portfolio_files:
            if candidate == portfolio_path:
                continue
            try:
                prev_port = pd.read_excel(candidate)
            except Exception:
                continue
            if prev_port is None or len(prev_port) == 0:
                continue
            if "code" not in prev_port.columns:
                continue
            prev_port["_code_norm"] = prev_port["code"].astype(str).str.replace(r"\D", "", regex=True).str[-6:].str.zfill(6)
            if "name" in prev_port.columns:
                name_map.update(
                    {
                        str(row["_code_norm"]): str(row["name"]).strip()
                        for _, row in prev_port[["name", "_code_norm"]].dropna(subset=["_code_norm"]).iterrows()
                        if str(row["_code_norm"]).strip()
                    }
                )
            if "market" in prev_port.columns:
                market_map.update(
                    {
                        str(row["_code_norm"]): str(row["market"]).strip()
                        for _, row in prev_port[["market", "_code_norm"]].dropna(subset=["_code_norm"]).iterrows()
                        if str(row["_code_norm"]).strip()
                    }
                )
            break

        portfolio_rows: List[Dict[str, Any]] = []
        if open_pos is not None and len(open_pos) > 0:
            for _, row in open_pos.iterrows():
                code = str(row.get("code") or "").strip().zfill(6)
                qty = pd.to_numeric(pd.Series([row.get("qty")]), errors="coerce").iloc[0]
                avg_price = pd.to_numeric(pd.Series([row.get("avg_price")]), errors="coerce").iloc[0]
                last_trade_date = str(row.get("last_trade_date") or "")
                portfolio_rows.append(
                    {
                        "code": code,
                        "name": name_map.get(code, f"LEDGER({code})"),
                        "market": market_map.get(code, ""),
                        "shares": float(qty) if pd.notna(qty) else None,
                        "entry_date": last_trade_date or D,
                        "entry_price": float(avg_price) if pd.notna(avg_price) else None,
                        "stop_level": None,
                        "last_close": None,
                        "last_low": None,
                        "MA20": None,
                        "pnl_krw": None,
                        "pnl_pct": None,
                        "exit_next_open": False,
                    }
                )

        portfolio_df = pd.DataFrame(
            portfolio_rows,
            columns=[
                "code",
                "name",
                "market",
                "shares",
                "entry_date",
                "entry_price",
                "stop_level",
                "last_close",
                "last_low",
                "MA20",
                "pnl_krw",
                "pnl_pct",
                "exit_next_open",
            ],
        )
        portfolio_df.to_excel(portfolio_path, index=False, engine="openpyxl")
    except Exception as e:
        _log_print(f"[STOP] RootB paper ledger rebuild failed: {e}")
        return 2

    try:
        df_rootb_ledger = pd.read_csv(paper_ledger_path, dtype=str, encoding="utf-8-sig")
    except Exception as e:
        _log_print(f"[STOP] RootB paper ledger read failed: {e}")
        return 2
    ledger_cols = [c for c in ["as_of", "date", "exec_date", "trade_date"] if c in df_rootb_ledger.columns]
    ledger_has_d = False
    for c in ledger_cols:
        if (df_rootb_ledger[c].astype(str).str.replace(r"\D", "", regex=True).str[:8] == str(D)).any():
            ledger_has_d = True
            break
    if not ledger_has_d:
        _log_print(f"[STOP] RootB paper ledger does not contain D={D}: {paper_ledger_path}")
        return 2
    _log_print(f"[OK] RootB paper ledger rebuilt from RootA fills for D={D}: {paper_ledger_path}")
    _log_print(f"[OK] RootB portfolio snapshot refreshed for D={D}: {portfolio_path}")

    # [6/8] RootB stats
    _log_print("[6/8] RootB stats generate + verify")
    t_before = time.time()
    cmd_stats = [py_rootb, str(rootb / "vibe_generate_stats_p0.py")]
    # [2026-08-29] RootB 통계는 자기 폴더의 최신 orders 로 as_of 를 스스로 정한다.
    #   진입이 멈춰 D 가 고정되면 as_of 만 전진해 as_of != D 로 매일 rc=2 가 됐다.
    #   D 를 명시해 같은 날을 보게 한다(핀 미지원 구버전이면 무시되고 종전대로 동작).
    #   출력도 분리한다. RootB 는 자기 D(달력 최신)로 따로 돌고, config.yaml 의
    #   stats_dir 은 RootB 의 현재 SSOT 스냅샷을 가리킨다. 거기에 D 기준 통계를 쓰면
    #   RootB 스냅샷이 오염된다(실측 2026-08-29). 검증용 산출물은 RootA 가 소유한다.
    stats_out_dir = roota / "2_Logs" / "rootb_stats_for_onepass" / str(D)
    stats_out_dir.mkdir(parents=True, exist_ok=True)
    env_stats = dict(os.environ)
    env_stats["VIBE_STATS_AS_OF"] = str(D)
    env_stats["VIBE_STATS_DIR"] = str(stats_out_dir)
    rc3 = _run(cmd_stats, cwd=rootb, tag="[RootB.stats]", env=env_stats)
    if rc3 != 0:
        _log_print("[STOP] RootB stats failed.")
        return 2

    # [2026-08-29] 종전에는 RootB config.yaml 의 stats_dir 을 읽어 그 안의 live_vs_bt.json
    #   을 봤다("SSOT split 회피"). 그런데 그 경로는 RootB 자신의 D(달력 최신) 스냅샷이라
    #   RootA 의 D(마지막 매수일)와 다른 날을 가리킨다. 둘은 하나로 합칠 수 없는 서로 다른
    #   측정이다. 위에서 D 로 핀해 생성한 산출물을 그대로 읽는다.
    lv_path = stats_out_dir / "live_vs_bt.json"
    lv = _safe_read_json(lv_path) or {}
    st = lv.get("status")
    asof = lv.get("as_of_ymd") or lv.get("as_of")

    # freshness check: mtime should be updated by this run (avoid stale PASS)
    try:
        mtime = lv_path.stat().st_mtime
        if mtime < (t_before - 1):
            _log_print(f"[STOP] RootB live_vs_bt not fresh (stale file): mtime={_dt.datetime.fromtimestamp(mtime)}")
            return 2
    except Exception:
        pass

    # [2026-08-27] status=NA 를 두 경우로 가른다.
    #   NA + rows_as_of==0 은 "그날 체결이 없어 비교할 대상이 없다" 는 뜻이지 실패가 아니다.
    #   v41.1 진입이 PAPER_EXIT_ONLY=1 로 멈춰 있는 동안 매일 이 상태가 되고,
    #   그러면 아침 파이프라인이 구조적으로 통과할 수 없다(08-26/08-27 연속 rc=2).
    #   체결이 있는데 NA 인 경우(rows_as_of>0)는 여전히 실패로 둔다.
    _lv_rows_asof = None
    try:
        _lv_sum = lv.get("summary") if isinstance(lv.get("summary"), dict) else {}
        if "rows_as_of" in _lv_sum:
            _lv_rows_asof = int(_lv_sum.get("rows_as_of") or 0)
    except Exception:
        _lv_rows_asof = None

    if st == "NA" and _lv_rows_asof == 0 and str(asof) == str(D):
        _log_print(
            f"[SKIP] live_vs_bt NA (그날 체결 0건이라 비교 대상 없음): "
            f"as_of={asof} rows_as_of=0 file={lv_path}"
        )
    elif st != "PASS" or str(asof) != str(D):
        _log_print(
            f"[STOP] live_vs_bt status not PASS or as_of mismatch: "
            f"status={st} as_of={asof} rows_as_of={_lv_rows_asof} file={lv_path}"
        )
        return 2
    else:
        _log_print(f"[OK] RootB live_vs_bt PASS+FRESH: status=PASS as_of={D} file={lv_path}")
    stats_dir_for_snapshot = lv_path.parent

    def _stats_asof(obj: dict) -> str:
        return str(obj.get("as_of_ymd") or obj.get("as_of") or "").strip()

    def _require_stats_asof(name: str) -> Path:
        p = stats_dir_for_snapshot / name
        obj = _safe_read_json(p)
        if not isinstance(obj, dict):
            _log_print(f"[STOP] stats JSON missing or unreadable: {p}")
            raise RuntimeError("stats_missing")
        got = _stats_asof(obj)
        if got != str(D):
            _log_print(f"[STOP] stats as_of mismatch: file={p} as_of={got} expected={D}")
            raise RuntimeError("stats_asof_mismatch")
        return p

    try:
        lv_copy_path = _require_stats_asof("live_vs_bt.json")
        risk_copy_path = _require_stats_asof("risk_stats.json")
        backtest_copy_path = _require_stats_asof("backtest_stats.json")
    except RuntimeError:
        return 2

    # [7/8] Snapshot
    _log_print("[7/8] Snapshot (RootB runs/SSOT_...)")
    snap = _make_snapshot_dir(rootb, D, today)
    copied: List[dict] = []

    # collect artifacts (7 expected)
    _copy_artifact(orders_exec_b, snap, f"orders/orders_{D}_exec.xlsx", copied)
    _copy_artifact(rootb / "data" / "orders" / f"orders_{D}_eval.xlsx", snap, f"orders/orders_{D}_eval.xlsx", copied)
    _copy_artifact(lv_copy_path, snap, "stats/live_vs_bt.json", copied)
    _copy_artifact(risk_copy_path, snap, "stats/risk_stats.json", copied)
    _copy_artifact(backtest_copy_path, snap, "stats/backtest_stats.json", copied)
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

    _log_print(f"[OK] snapshot created: {snap}")

    # [8/8] FINAL
    _log_print("[8/8] FINAL")
    _log_print(f"=== PASS D={D} | live_vs_bt=PASS(as_of={D}) | ledger={'apply' if args.apply_ledger else 'skip'} | snapshot={snap} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




