import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger("ledger_append_from_orders_exec")

# Ensure utils import works regardless of cwd
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from utils.pipeline_audit import log_pipeline_event, count_rows


NEED_LEDGER_COLS = ["purchase_date", "ticker", "name", "buy_price", "status", "yield", "group", "days_held"]
NEED_ORDERS_COLS = ["exec_date", "side", "code", "fill_qty", "fill_price", "is_stop"]


def stop(msg: str, code: int = 2):
    logger.error("%s", msg)
    raise SystemExit(code)


def norm_ymd(x) -> str:
    s = "" if pd.isna(x) else str(x)
    m = re.search(r"(\d{8})", s)
    return m.group(1) if m else ""


def norm_code(x) -> str:
    s = "" if pd.isna(x) else str(x)
    m = re.search(r"(\d+)", s)
    if not m:
        return ""
    try:
        return str(int(m.group(1)))
    except Exception:
        return m.group(1)


def to_bool(v) -> bool:
    s = "" if pd.isna(v) else str(v).strip().lower()
    return s in {"1", "true", "t", "y", "yes"}


def pick_col(columns, candidates):
    col_map = {str(c).strip().lower(): c for c in columns}
    for cand in candidates:
        hit = col_map.get(str(cand).strip().lower())
        if hit is not None:
            return hit
    return None


def load_name_map() -> dict:
    mapping: dict = {}
    root = Path(__file__).resolve().parents[1]
    p = root / "_cache" / "krx_listing.csv"
    if not p.exists():
        return mapping
    try:
        df = pd.read_csv(p, dtype=str)
    except Exception:
        return mapping
    if "code" not in df.columns or "name" not in df.columns:
        return mapping
    for _, r in df.iterrows():
        code = norm_code(r.get("code", ""))
        nm = str(r.get("name", "") or "").strip()
        if code and nm:
            mapping[code] = nm
    return mapping


def build_run_id() -> str:
    return f"LEDGER_APPEND_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def write_report(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[REPORT] %s", path)

def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _open_position_keys_from_state(root: Path) -> set[tuple[str, str]]:
    state_path = root / "paper" / "paper_state.json"
    state = _read_json(state_path)
    keys: set[tuple[str, str]] = set()
    for pos in state.get("open_positions") or []:
        if not isinstance(pos, dict):
            continue
        code = norm_code(pos.get("code", ""))
        entry_date = norm_ymd(pos.get("entry_date") or pos.get("signal_date") or "")
        if code and entry_date:
            keys.add((entry_date, code))
    return keys


def _sync_ledger_status_from_open_positions(ledger: pd.DataFrame, open_keys: set[tuple[str, str]]) -> tuple[pd.DataFrame, dict]:
    out = ledger.copy()
    out["purchase_date_n"] = out["purchase_date"].apply(norm_ymd)
    out["ticker_n"] = out["ticker"].apply(norm_code)
    keys = list(zip(out["purchase_date_n"].astype(str), out["ticker_n"].astype(str)))
    open_mask = pd.Series([key in open_keys for key in keys], index=out.index)
    status_u = out["status"].fillna("").astype(str).str.upper().str.strip()
    group_u = out["group"].fillna("").astype(str).str.upper().str.strip()
    managed_mask = status_u.isin({"PENDING", "BOUGHT", "OPEN"})
    # LIVE_BROKER rows are backed by actual broker positions; do not close them based on paper_state.
    live_broker_mask = group_u == "LIVE_BROKER"
    close_mask = managed_mask & (~open_mask) & (~live_broker_mask)
    before_status = out["status"].astype(str).copy()
    before_group = out["group"].astype(str).copy() if "group" in out.columns else pd.Series([""] * len(out), index=out.index)
    out.loc[open_mask & (~live_broker_mask), "status"] = "PENDING"
    out.loc[open_mask & (~live_broker_mask), "group"] = "WATCH"
    out.loc[close_mask, "status"] = "CLOSED"
    out.loc[close_mask, "group"] = "CLOSED"
    changed = (out["status"].astype(str) != before_status) | (out["group"].astype(str) != before_group)
    metrics = {
        "open_position_keys": int(len(open_keys)),
        "status_changed_rows": int(changed.sum()),
        "closed_rows": int(close_mask.sum()),
        "open_rows": int(open_mask.sum()),
    }
    return out.drop(columns=["purchase_date_n", "ticker_n"], errors="ignore"), metrics


def _load_live_fills_buys(path: Path, D: str, blocked_keys: set[tuple[str, str]]) -> pd.DataFrame:
    """Read AUTO_DISPATCH BUY rows from live_fills.csv for date D.

    Cross-checks against entry_blocked keys from orders_exec so that a blocked
    paper signal does not resurrect as a broker fill in the ledger.
    """
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df["date_n"] = df["date"].astype(str).str[:8].apply(norm_ymd)
    df["code_n"] = df["code"].astype(str).str.replace(".0", "", regex=False).str.strip().apply(norm_code)
    df["side_u"] = df["side"].astype(str).str.upper().str.strip()
    df["lineage_origin_u"] = df.get("lineage_origin", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper().str.strip()
    df["fill_qty_n"] = pd.to_numeric(df["fill_qty"], errors="coerce")
    df["fill_price_n"] = pd.to_numeric(df["fill_price"], errors="coerce")
    df["key"] = list(zip(df["date_n"].astype(str), df["code_n"].astype(str)))
    mask = (
        (df["date_n"] == D)
        & (df["side_u"] == "BUY")
        & (df["lineage_origin_u"] == "AUTO_DISPATCH")
        & (df["fill_qty_n"] > 0)
        & (~df["key"].isin(blocked_keys))
    )
    return df.loc[mask].copy()


def _apply_live_fills_sells(ledger: pd.DataFrame, path: Path, D: str) -> pd.DataFrame:
    """Close the most recent non-CLOSED open row for each AUTO_DISPATCH SELL."""
    if not path.exists():
        return ledger
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return ledger
    if df.empty:
        return ledger
    df["date_n"] = df["date"].astype(str).str[:8].apply(norm_ymd)
    df["code_n"] = df["code"].astype(str).str.replace(".0", "", regex=False).str.strip().apply(norm_code)
    df["side_u"] = df["side"].astype(str).str.upper().str.strip()
    df["lineage_origin_u"] = df.get("lineage_origin", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper().str.strip()
    sells = df[
        (df["date_n"] == D)
        & (df["side_u"] == "SELL")
        & (df["lineage_origin_u"] == "AUTO_DISPATCH")
    ].copy()
    if sells.empty:
        return ledger

    out = ledger.copy()
    out["purchase_date_n"] = out["purchase_date"].apply(norm_ymd)
    out["ticker_n"] = out["ticker"].apply(norm_code)
    out["status_u"] = out["status"].fillna("").astype(str).str.upper().str.strip()
    closed_count = 0
    for _, r in sells.iterrows():
        # norm_code() strips leading zeros on both sides; zfill here would break matching.
        code = str(r["code_n"])
        mask = (out["ticker_n"] == code) & (~out["status_u"].isin({"CLOSED"}))
        if mask.any():
            idx = out[mask].index[-1]
            out.loc[idx, "status"] = "CLOSED"
            out.loc[idx, "group"] = "CLOSED"
            closed_count += 1
    logger.info("[LIVE_SELLS] closed_rows=%s for D=%s", closed_count, D)
    return out.drop(columns=["purchase_date_n", "ticker_n", "status_u"], errors="ignore")


def _write_ledger_atomic(ledger_path: Path, out: pd.DataFrame, run_id: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = ledger_path.with_name(ledger_path.name + f".bak_{ts}")
    tmp_path = ledger_path.with_name(ledger_path.name + f".tmp_{ts}")
    out.to_csv(tmp_path, index=False, encoding="utf-8")
    reread = pd.read_csv(tmp_path, dtype=str).fillna("")
    if len(reread) != len(out):
        tmp_path.unlink(missing_ok=True)
        stop(f"STOP ledger temp write row mismatch expected={len(out)} actual={len(reread)}")
    ledger_path.replace(bak)
    try:
        tmp_path.replace(ledger_path)
    except Exception:
        if ledger_path.exists():
            ledger_path.unlink()
        bak.replace(ledger_path)
        raise
    return str(bak)


def main():
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    _audit_ymd = datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="ledger_append",
        batch_label="[11/14]",
        event="START",
        date=_audit_ymd,
        input_files={
            "virtual_ledger": str(Path(__file__).resolve().parents[1] / "virtual_ledger.csv"),
        },
    )

    def _clean_arg(a: str) -> str:
        return str(a).strip().replace("\r", "").replace("\n", "")

    raw_args = [_clean_arg(a) for a in sys.argv[1:]]
    apply = "--apply" in raw_args
    sync_state_only = "--sync-state-only" in raw_args
    from_live_fills_flag = "--from-live-fills" in raw_args
    from_live_fills_path: Optional[Path] = None
    if from_live_fills_flag:
        idx = raw_args.index("--from-live-fills")
        if idx + 1 < len(raw_args):
            from_live_fills_path = Path(raw_args[idx + 1])
        if from_live_fills_path is None or not from_live_fills_path.exists():
            stop(f"STOP --from-live-fills requires an existing PATH, got={from_live_fills_path}")

    # Extract positional arguments while skipping flags and their values.
    positional: list[str] = []
    skip = False
    for x in raw_args:
        if skip:
            skip = False
            continue
        if x in {"--apply", "--sync-state-only"}:
            continue
        if x == "--from-live-fills":
            skip = True
            continue
        positional.append(x)

    if not positional and not sync_state_only:
        stop("STOP usage: python ledger_append_from_orders_exec.py <YYYYMMDD> [--apply] [--sync-state-only] [--from-live-fills PATH]")
    D = str(positional[0]).strip() if positional else datetime.now().strftime("%Y%m%d")
    if not re.fullmatch(r"\d{8}", D):
        stop(f"STOP invalid D={D} expected YYYYMMDD")

    logger.info(
        "[ARGS] raw_argv=%s cleaned_argv=%s apply=%s sync_state_only=%s from_live_fills=%s from_live_fills_path=%s D=%s",
        sys.argv, raw_args, apply, sync_state_only, from_live_fills_flag,
        from_live_fills_path, D,
    )

    run_id = build_run_id()
    root = Path(__file__).resolve().parents[1]
    logs_dir = root / "2_Logs"
    ledger_path = root / "virtual_ledger.csv"
    orders_path = root / "paper" / f"orders_{D}_exec.xlsx"
    report_path = logs_dir / ("ledger_sync_state_report_latest.json" if sync_state_only else f"ledger_append_report_{D}.json")

    if not ledger_path.exists():
        stop(f"STOP missing ledger: {ledger_path}")

    ledger = pd.read_csv(ledger_path, dtype=str)
    missing_ledger = [c for c in NEED_LEDGER_COLS if c not in ledger.columns]
    if missing_ledger:
        stop(f"STOP ledger missing_cols={missing_ledger}")

    open_keys = _open_position_keys_from_state(root)
    ledger, state_sync_metrics = _sync_ledger_status_from_open_positions(ledger, open_keys)
    live_sell_closed_rows = 0
    if from_live_fills_path:
        ledger_before_sells = ledger.copy()
        ledger = _apply_live_fills_sells(ledger, from_live_fills_path, D)
        live_sell_closed_rows = int((ledger["status"] != ledger_before_sells["status"]).sum())
    if sync_state_only:
        backup_path = ""
        if apply and int(state_sync_metrics.get("status_changed_rows", 0)) > 0:
            backup_path = _write_ledger_atomic(ledger_path, ledger, run_id)
        payload = {
            "run_id": run_id,
            "as_of": D,
            "status": "PASS",
            "apply": bool(apply),
            "backup_path": backup_path,
            "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
            "state_path": str(root / "paper" / "paper_state.json"),
            "ledger_path": str(ledger_path),
            "metrics": state_sync_metrics,
            "reflected_scope": "virtual_ledger_status_from_paper_state_open_positions",
        }
        write_report(report_path, payload)
        logger.info("[SYNC_STATE] changed=%s apply=%s", state_sync_metrics.get("status_changed_rows"), apply)
        log_pipeline_event(
            stage="ledger_append",
            batch_label="[11/14]",
            event="END",
            date=_audit_ymd,
            output_files={
                "virtual_ledger": {"path": str(ledger_path), "rows": count_rows(ledger_path)},
            },
            metrics={
                "sync_state_only": True,
                "apply": bool(apply),
                "status_changed_rows": int(state_sync_metrics.get("status_changed_rows", 0)),
            },
            status="PASS",
        )
        return 0

    if not orders_path.exists():
        stop(f"STOP missing orders_exec: {orders_path}")

    orders = pd.read_excel(orders_path, engine="openpyxl", dtype=str)
    missing_orders = [c for c in NEED_ORDERS_COLS if c not in orders.columns]
    if missing_orders:
        stop(f"STOP orders_exec missing_cols={missing_orders}")

    exec_unique = sorted({norm_ymd(x) for x in orders["exec_date"].tolist() if norm_ymd(x)})
    if exec_unique != [D]:
        stop(f"STOP exec_date_unique={exec_unique} expected={[D]}")

    o = orders.copy()
    o["side_u"] = o["side"].astype(str).str.upper().str.strip()
    o["is_stop_b"] = o["is_stop"].apply(to_bool)
    if "entry_blocked" in o.columns:
        o["entry_blocked_b"] = o["entry_blocked"].apply(to_bool)
    else:
        o["entry_blocked_b"] = False
    o["ticker_n"] = o["code"].apply(norm_code)
    o["qty_n"] = pd.to_numeric(o["fill_qty"], errors="coerce")
    o["buy_price_n"] = pd.to_numeric(o["fill_price"], errors="coerce")
    invalid_qty_rows = int(o["qty_n"].isna().sum() + (o["qty_n"] <= 0).sum())
    if invalid_qty_rows > 0:
        stop(f"STOP invalid fill_qty rows={invalid_qty_rows} (must be > 0)")

    blocked_buy_mask = (o["side_u"] == "BUY") & (~o["is_stop_b"]) & (o["entry_blocked_b"])
    blocked_buy_rows = int(blocked_buy_mask.sum())
    blocked_buy_notional = float((o.loc[blocked_buy_mask, "qty_n"] * o.loc[blocked_buy_mask, "buy_price_n"]).sum())
    buys = o[(o["side_u"] == "BUY") & (~o["is_stop_b"]) & (~o["entry_blocked_b"])].copy()
    buys["source"] = "orders_exec"

    # Cross-check entry_blocked keys from orders_exec to filter live broker fills.
    blocked_exec_keys: set[tuple[str, str]] = set()
    if "entry_blocked_b" in o.columns and "ticker_n" in o.columns:
        blocked_mask = (o["side_u"] == "BUY") & (~o["is_stop_b"]) & (o["entry_blocked_b"])
        if blocked_mask.any():
            blocked_exec_keys = set(zip([D] * int(blocked_mask.sum()), o.loc[blocked_mask, "ticker_n"].tolist()))

    live_buys = _load_live_fills_buys(from_live_fills_path, D, blocked_exec_keys) if from_live_fills_path else pd.DataFrame()
    live_buys_added_rows = 0
    live_buys_qty_sum = 0.0
    if not live_buys.empty:
        live_buys["name_n"] = live_buys.get("name", "").astype(str).str.strip()
        name_map = load_name_map()
        if name_map:
            miss_mask = live_buys["name_n"].eq("")
            live_buys.loc[miss_mask, "name_n"] = live_buys.loc[miss_mask, "code_n"].map(lambda x: name_map.get(str(x), ""))
        live_buys["side_u"] = "BUY"
        live_buys["is_stop_b"] = False
        live_buys["entry_blocked_b"] = False
        live_buys = live_buys.rename(columns={"code_n": "ticker_n", "fill_qty_n": "qty_n", "fill_price_n": "buy_price_n"})
        live_buys["source"] = "live_broker"
        live_buys = live_buys[["ticker_n", "qty_n", "buy_price_n", "name_n", "side_u", "is_stop_b", "entry_blocked_b", "source"]].copy()
        buys = pd.concat([buys, live_buys], ignore_index=True)
        live_buys_added_rows = len(live_buys)
        live_buys_qty_sum = float(live_buys["qty_n"].sum())

    if len(buys) == 0:
        payload = {
            "run_id": run_id,
            "as_of": D,
            "status": "PASS",
            "apply": bool(apply),
            "reason": "buy_rows_zero",
            "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
            "before_snapshot": {"ledger_rows": int(len(ledger))},
            "after_snapshot": {"ledger_rows": int(len(ledger))},
            "metrics": {
                "input_buy_rows_total": blocked_buy_rows,
                "input_buy_rows": 0,
                "blocked_buy_rows": blocked_buy_rows,
                "blocked_buy_notional": blocked_buy_notional,
                "input_buy_qty_sum": 0,
                "to_append_rows": 0,
                "to_append_qty_sum": 0,
                "idempotent_reappend_rows": 0,
                "position_negative_rows": 0,
                "reflected_scope": "unblocked_buy_rows_only",
                "blocked_buy_excluded": blocked_buy_rows > 0,
            },
            "checks": {
                "blocked_buy_excluded": True,
                "position_negative_zero": True,
            },
            "avg_cost_rule": "legacy_virtual_ledger: buy_price stores entry reference; weighted avg cost is handled in downstream ledger pipeline",
        }
        write_report(report_path, payload)
        logger.info("[DRY] D=%s buy_rows=0 -> nothing to append", D)
        log_pipeline_event(
            stage="ledger_append",
            batch_label="[11/14]",
            event="END",
            date=_audit_ymd,
            output_files={
                "virtual_ledger": {"path": str(ledger_path), "rows": count_rows(ledger_path)},
            },
            metrics={
                "apply": bool(apply),
                "buy_rows_zero": True,
                "appended_rows": 0,
            },
            status="PASS",
        )
        return 0

    name_col = pick_col(o.columns, ["name", "stock_name", "item_name"])
    if name_col is not None and name_col in buys.columns:
        buys["name_n"] = buys[name_col].astype(str).fillna("").str.strip()
    else:
        buys["name_n"] = ""
    name_map = load_name_map()
    if name_map:
        miss_mask = buys["name_n"].astype(str).str.strip().eq("")
        buys.loc[miss_mask, "name_n"] = buys.loc[miss_mask, "ticker_n"].map(lambda x: name_map.get(str(x), ""))

    buys = buys.dropna(subset=["buy_price_n", "qty_n"])
    buys = buys[buys["ticker_n"].astype(str).str.len() > 0].copy()

    # Aggregate partial fills by (D, ticker) for deterministic/idempotent append.
    grouped_rows = []
    for (purchase_date, ticker), g in buys.groupby([pd.Series([D] * len(buys), index=buys.index), "ticker_n"], sort=False):
        qty_sum = float(g["qty_n"].sum())
        if qty_sum <= 0:
            continue
        vwap = float((g["buy_price_n"] * g["qty_n"]).sum() / qty_sum)
        names = [str(x).strip() for x in g["name_n"].tolist() if str(x).strip()]
        source_vals = g["source"].tolist() if "source" in g.columns else ["orders_exec"] * len(g)
        source_val = "live_broker" if all(str(s).strip() == "live_broker" for s in source_vals) else "orders_exec"
        grouped_rows.append(
            {
                "purchase_date": str(purchase_date),
                "ticker_n": str(ticker),
                "name_n": names[0] if names else "",
                "qty_sum": qty_sum,
                "buy_price_vwap": vwap,
                "fill_rows": int(len(g)),
                "source": source_val,
            }
        )
    grouped = pd.DataFrame(grouped_rows)
    if len(grouped) == 0:
        stop("STOP grouped buys empty after normalization")

    ledger_k = ledger.copy()
    ledger_k["purchase_date_n"] = ledger_k["purchase_date"].apply(norm_ymd)
    ledger_k["ticker_n"] = ledger_k["ticker"].apply(norm_code)
    existing_keys = set((ledger_k["purchase_date_n"].astype(str) + "|" + ledger_k["ticker_n"].astype(str)).tolist())
    grouped["key"] = grouped["purchase_date"].astype(str) + "|" + grouped["ticker_n"].astype(str)
    to_append = grouped[~grouped["key"].isin(existing_keys)].copy()

    add_rows = []
    for _, r in to_append.iterrows():
        group_val = "LIVE_BROKER" if str(r.get("source", "")) == "live_broker" else "WATCH"
        add_rows.append(
            {
                "purchase_date": D,
                "ticker": int(r["ticker_n"]),
                "name": str(r.get("name_n", "") or "").strip(),
                "buy_price": float(r["buy_price_vwap"]),
                "status": "PENDING",
                "yield": 0.0,
                "group": group_val,
                "days_held": 0,
            }
        )
    add = pd.DataFrame(add_rows, columns=NEED_LEDGER_COLS)

    before_snapshot = {
        "ledger_rows": int(len(ledger)),
        "ledger_keys_unique": int(
            ledger_k[["purchase_date_n", "ticker_n"]].drop_duplicates().shape[0]
        ),
    }
    metrics = {
        "input_buy_rows_total": int(((o["side_u"] == "BUY") & (~o["is_stop_b"])).sum()),
        "input_buy_rows": int(len(buys)),
        "blocked_buy_rows": blocked_buy_rows,
        "blocked_buy_notional": blocked_buy_notional,
        "input_buy_qty_sum": float(buys["qty_n"].sum()),
        "grouped_buy_rows": int(len(grouped)),
        "grouped_buy_qty_sum": float(grouped["qty_sum"].sum()),
        "to_append_rows": int(len(add)),
        "to_append_qty_sum": float(to_append["qty_sum"].sum()) if len(to_append) else 0.0,
        "duplicate_existing_hits": int(len(grouped) - len(to_append)),
        "position_negative_rows": int((buys["qty_n"] <= 0).sum()),
        "state_sync_status_changed_rows": int(state_sync_metrics.get("status_changed_rows", 0)),
        "state_sync_closed_rows": int(state_sync_metrics.get("closed_rows", 0)),
        "state_sync_open_rows": int(state_sync_metrics.get("open_rows", 0)),
        "live_buys_added_rows": live_buys_added_rows,
        "live_buys_qty_sum": live_buys_qty_sum,
        "live_sell_closed_rows": live_sell_closed_rows,
        "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
    }

    logger.info(
        "[PLAN] run_id=%s as_of=%s buys=%s grouped=%s to_append=%s ledger_rows_before=%s qty_to_append=%.2f",
        run_id,
        D,
        metrics["input_buy_rows"],
        metrics["grouped_buy_rows"],
        metrics["to_append_rows"],
        before_snapshot["ledger_rows"],
        metrics["to_append_qty_sum"],
    )

    if len(add) == 0:
        payload = {
            "run_id": run_id,
            "as_of": D,
            "status": "PASS",
            "apply": bool(apply),
            "reason": "all_duplicates",
            "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
            "before_snapshot": before_snapshot,
            "after_snapshot": before_snapshot,
            "metrics": {
                **metrics,
                "idempotent_reappend_rows": 0,
                "reflected_scope": "unblocked_buy_rows_only",
                "blocked_buy_excluded": blocked_buy_rows > 0,
            },
            "checks": {
                "append_idempotent": True,
                "position_negative_zero": metrics["position_negative_rows"] == 0,
                "blocked_buy_excluded": True,
            },
            "avg_cost_rule": "legacy_virtual_ledger: buy_price stores weighted average entry price(vwap) by (as_of,ticker) from fill_price*fill_qty",
        }
        write_report(report_path, payload)
        logger.info("[PLAN] nothing to append (all duplicates)")
        log_pipeline_event(
            stage="ledger_append",
            batch_label="[11/14]",
            event="END",
            date=_audit_ymd,
            output_files={
                "virtual_ledger": {"path": str(ledger_path), "rows": count_rows(ledger_path)},
            },
            metrics={
                "apply": bool(apply),
                "appended_rows": 0,
                "to_append_rows": int(metrics.get("to_append_rows", 0)),
            },
            status="PASS",
        )
        return 0

    if not apply:
        after_snapshot = {"ledger_rows": int(len(ledger) + len(add))}
        payload = {
            "run_id": run_id,
            "as_of": D,
            "status": "PASS",
            "apply": False,
            "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
            "before_snapshot": before_snapshot,
            "after_snapshot": after_snapshot,
            "metrics": {
                **metrics,
                "idempotent_reappend_rows": 0,
                "reflected_scope": "unblocked_buy_rows_only",
                "blocked_buy_excluded": blocked_buy_rows > 0,
            },
            "checks": {
                "append_idempotent": True,
                "position_negative_zero": metrics["position_negative_rows"] == 0,
                "blocked_buy_excluded": True,
            },
            "avg_cost_rule": "legacy_virtual_ledger: buy_price stores weighted average entry price(vwap) by (as_of,ticker) from fill_price*fill_qty",
            "preview_append_rows": add_rows,
        }
        write_report(report_path, payload)
        logger.info("[DRY] preview_append_rows:\n%s", add.to_string(index=False))
        logger.info("[DRY] run with --apply to write (will create backup)")
        log_pipeline_event(
            stage="ledger_append",
            batch_label="[11/14]",
            event="END",
            date=_audit_ymd,
            output_files={
                "virtual_ledger": {"path": str(ledger_path), "rows": count_rows(ledger_path)},
            },
            metrics={
                "apply": False,
                "appended_rows": 0,
                "to_append_rows": int(len(add)),
            },
            status="PASS",
        )
        return 0

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = ledger_path.with_name(ledger_path.name + f".bak_{ts}")
    out = pd.concat([ledger, add], ignore_index=True)

    out_k = out.copy()
    out_k["purchase_date_n"] = out_k["purchase_date"].apply(norm_ymd)
    out_k["ticker_n"] = out_k["ticker"].apply(norm_code)
    dup_after = int(out_k.duplicated(subset=["purchase_date_n", "ticker_n"], keep=False).sum())
    if dup_after > 0:
        stop(f"STOP duplicate ledger keys after append rows={dup_after}")

    tmp_path = ledger_path.with_name(ledger_path.name + f".tmp_{ts}")
    out.to_csv(tmp_path, index=False, encoding="utf-8")
    reread = pd.read_csv(tmp_path, dtype=str).fillna("")
    if len(reread) != len(out):
        tmp_path.unlink(missing_ok=True)
        stop(f"STOP ledger temp write row mismatch expected={len(out)} actual={len(reread)}")

    ledger_path.replace(bak)
    try:
        tmp_path.replace(ledger_path)
        persisted = pd.read_csv(ledger_path, dtype=str).fillna("")
        if len(persisted) != len(out):
            raise RuntimeError(f"persisted row mismatch expected={len(out)} actual={len(persisted)}")
    except Exception as e:
        failed_path = ledger_path.with_name(ledger_path.name + f".failed_{ts}")
        if ledger_path.exists():
            ledger_path.replace(failed_path)
        bak.replace(ledger_path)
        payload = {
            "run_id": run_id,
            "as_of": D,
            "status": "FAIL",
            "apply": True,
            "backup_path": str(bak),
            "rollback_attempted": True,
            "rollback_restored": ledger_path.exists(),
            "failed_path": str(failed_path),
            "error": f"{type(e).__name__}: {e}",
        }
        write_report(report_path, payload)
        stop(f"STOP ledger write failed and rollback restored: {type(e).__name__}: {e}")

    reappend_keys = set(out_k["purchase_date_n"].astype(str) + "|" + out_k["ticker_n"].astype(str))
    idempotent_reappend_rows = int((~grouped["key"].isin(reappend_keys)).sum())
    after_snapshot = {
        "ledger_rows": int(len(out)),
        "ledger_keys_unique": int(out_k[["purchase_date_n", "ticker_n"]].drop_duplicates().shape[0]),
    }

    payload = {
        "run_id": run_id,
        "as_of": D,
        "status": "PASS",
        "apply": True,
        "backup_path": str(bak),
        "from_live_fills_path": str(from_live_fills_path) if from_live_fills_path else "",
        "before_snapshot": before_snapshot,
        "after_snapshot": after_snapshot,
        "metrics": {
            **metrics,
            "appended_rows": int(len(add)),
            "idempotent_reappend_rows": idempotent_reappend_rows,
            "reflected_scope": "unblocked_buy_rows_only",
            "blocked_buy_excluded": blocked_buy_rows > 0,
        },
        "checks": {
            "append_idempotent": idempotent_reappend_rows == 0,
            "position_negative_zero": metrics["position_negative_rows"] == 0,
            "blocked_buy_excluded": True,
        },
        "avg_cost_rule": "legacy_virtual_ledger: buy_price stores weighted average entry price(vwap) by (as_of,ticker) from fill_price*fill_qty",
    }
    write_report(report_path, payload)

    logger.info("[APPLY] BACKUP=%s", bak)
    logger.info("[APPLY] WROTE=%s rows_after=%s appended=%s", ledger_path, len(out), len(add))
    log_pipeline_event(
        stage="ledger_append",
        batch_label="[11/14]",
        event="END",
        date=_audit_ymd,
        output_files={
            "virtual_ledger": {"path": str(ledger_path), "rows": count_rows(ledger_path)},
        },
        metrics={
            "apply": True,
            "appended_rows": int(len(add)),
            "ledger_rows_before": int(before_snapshot["ledger_rows"]),
            "ledger_rows_after": int(after_snapshot["ledger_rows"]),
        },
        status="PASS",
    )
    return 0


if __name__ == "__main__":
    main()
