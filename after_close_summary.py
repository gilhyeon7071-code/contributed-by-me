# -*- coding: utf-8 -*-
"""
after_close_summary.py
- After market close summary for paper trading.
- Reads:
  - paper/fills.csv
  - 2_Logs/candidates_latest_data.csv
  - 2_Logs/candidates_latest_data.filtered.csv (optional)
  - 2_Logs/liquidity_filter_daily_last.json (optional)
  - 2_Logs/p0_daily_check_*.json (latest)
  - 2_Logs/gate_daily_*.json (latest)
  - 2_Logs/survivorship_daily_last.json (optional)

Outputs:
  - 2_Logs/after_close_summary_YYYYMMDD_HHMMSS.json
  - 2_Logs/after_close_summary_YYYYMMDD_HHMMSS.csv
  - 2_Logs/after_close_summary_last.json / .csv

Console: prints a compact summary.
"""
def _attach_kill_switch_dd_source_link(after_close_obj: dict, logs_dir):
    """
    Attach dd_source_link into after_close_summary_last.json for auditability.
    - Does NOT change trading logic (risk_off/reasons).
    - Best-effort: if missing, records *_error.
    """
    try:
        import json, re, datetime
        from pathlib import Path

        if not isinstance(after_close_obj, dict):
            return

        # 1) parse reported_dd from reasons (kill_switch:MAX_DD(...))
        reasons = None
        p0 = after_close_obj.get("p0", {})
        if isinstance(p0, dict):
            reasons = p0.get("risk_off_reasons") or after_close_obj.get("risk_off_reasons") or []
        if not isinstance(reasons, list):
            reasons = []

        m = None
        for r in reasons:
            if not isinstance(r, str):
                continue
            m = re.search(r"MAX_DD\(([-+]?\d*\.?\d+)\s*<=\s*([-+]?\d*\.?\d+)\)", r)
            if m:
                break
        if not m:
            # kill_switch MAX_DD reason이 없으면 attach 불가(정상)
            return

        reported_dd = float(m.group(1))
        reported_fmt4 = float(f"{reported_dd:.4f}")

        # 2) find best dd_source (newest first, must match fmt4 + must have dd_curve_csv)
        logs_dir = Path(str(logs_dir))
        srcs = sorted(logs_dir.glob("kill_switch_dd_source_*.json"),
                      key=lambda p: p.stat().st_mtime, reverse=True)

        best = None
        for sp in srcs[:200]:
            try:
                j = json.load(open(sp, "r", encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(j, dict):
                continue

            calc = j.get("calc", {}) if isinstance(j.get("calc"), dict) else {}
            art  = j.get("artifact", {}) if isinstance(j.get("artifact"), dict) else {}
            dd_curve = art.get("dd_curve_csv") if isinstance(art.get("dd_curve_csv"), dict) else None
            if not dd_curve:
                continue
            curve_path = dd_curve.get("path")
            if not curve_path or (not Path(str(curve_path)).exists()):
                continue

            f_re = calc.get("max_dd_recomputed_fmt4")
            try:
                if f_re is None:
                    continue
                if float(f_re) != reported_fmt4:
                    continue
            except Exception:
                continue

            best = (sp, dd_curve)
            break

        if best is None:
            after_close_obj.setdefault("p0", {}).setdefault("kill_switch", {}).setdefault("metrics", {})
            after_close_obj["p0"]["kill_switch"]["metrics"]["dd_source_link_error"] = \
                f"no_matching_dd_source(found={len(srcs)}, reported_fmt4={reported_fmt4})"
            return

        sp, dd_curve = best
        ts = datetime.datetime.fromtimestamp(Path(sp).stat().st_mtime).isoformat(timespec="seconds")

        dd_link = {
            "reported_dd_fmt4": reported_fmt4,
            "dd_source_json": {"path": str(sp), "mtime": ts},
            "dd_curve_csv": dd_curve,
        }

        after_close_obj.setdefault("p0", {}).setdefault("kill_switch", {}).setdefault("metrics", {})
        after_close_obj["p0"]["kill_switch"]["metrics"]["dd_source_link"] = dd_link

    except Exception as e:
        try:
            after_close_obj.setdefault("p0", {}).setdefault("kill_switch", {}).setdefault("metrics", {})
            after_close_obj["p0"]["kill_switch"]["metrics"]["dd_source_link_error"] = f"{type(e).__name__}: {e}"
        except Exception:
            pass
import csv
import json
from pathlib import Path
from datetime import datetime
from utils.common import read_json

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "2_Logs"
PAPER_DIR = BASE_DIR / "paper"

def _latest_by_glob(glob_pat: str) -> Path | None:
    paths = list(LOG_DIR.glob(glob_pat))
    if not paths:
        return None
    paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[0]


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

def _print_table(title: str, headers: list[str], rows: list[list[str]], max_rows: int = 50) -> None:
    print("\\n" + title)
    if not rows:
        print("(none)")
        return
    rows = rows[:max_rows]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))
    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    print("-" * (sum(widths) + 2 * (len(widths) - 1)))
    for r in rows:
        print(fmt.format(*[str(v) for v in r]))

def main() -> int:
    now = datetime.now()
    ymd = now.strftime("%Y%m%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    fills_path = PAPER_DIR / "fills.csv"
    fills = _read_csv_rows(fills_path)

    # Today's executions
    todays = []
    for r in fills:
        dt = (r.get("datetime") or "").strip()
        if not dt.startswith(ymd):
            continue
        side = (r.get("side") or "").strip().upper()
        if side not in ("BUY", "SELL"):
            continue
        code = (r.get("code") or "").strip()
        todays.append({
            "datetime": dt,
            "side": side,
            "code": code,
            "qty": r.get("qty"),
            "price": r.get("price"),
            "order_id": r.get("order_id"),
            "note": r.get("note"),
        })

    buys = [r for r in todays if r["side"] == "BUY"]
    sells = [r for r in todays if r["side"] == "SELL"]

    # Candidates: filtered vs raw
    cand_raw_path = LOG_DIR / "candidates_latest_data.csv"
    cand_filt_path = LOG_DIR / "candidates_latest_data.filtered.csv"
    cand_raw = _read_csv_rows(cand_raw_path)
    cand_filt = _read_csv_rows(cand_filt_path) if cand_filt_path.exists() else []

    # Liquidity filter removal reasons (if available)
    liq_last = LOG_DIR / "liquidity_filter_daily_last.json"
    liq = read_json(liq_last) if liq_last.exists() else {}

    # P0 / gate / survivorship (latest)
    p0_path = _latest_by_glob("p0_daily_check_*.json")
    gate_path = _latest_by_glob("gate_daily_*.json") or (LOG_DIR / "gate_daily.json")
    surv_path = LOG_DIR / "survivorship_daily_last.json"

    p0 = read_json(p0_path) if p0_path and p0_path.exists() else {}
    gate = read_json(gate_path) if gate_path and gate_path.exists() else {}
    surv = read_json(surv_path) if surv_path.exists() else {}

    removed = liq.get("removed") if isinstance(liq.get("removed"), list) else []
    kept = liq.get("kept") if isinstance(liq.get("kept"), list) else []

    removed_codes = {str(x.get("code")) for x in removed if isinstance(x, dict)}
    kept_codes = set(kept) if isinstance(kept, list) else set()

    out = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "as_of_ymd": ymd,
        "paths": {
            "fills": str(fills_path),
            "candidates_raw": str(cand_raw_path),
            "candidates_filtered": str(cand_filt_path) if cand_filt_path.exists() else None,
            "liquidity_filter_last": str(liq_last) if liq_last.exists() else None,
            "p0_daily_check": str(p0_path) if p0_path else None,
            "gate_daily": str(gate_path) if gate_path else None,
            "survivorship_daily_last": str(surv_path) if surv_path.exists() else None,
        },
        "p0": {
            "risk_off": (p0.get("risk_off") or {}).get("enabled") if isinstance(p0.get("risk_off"), dict) else None,
            "risk_off_reasons": (p0.get("risk_off") or {}).get("reasons") if isinstance(p0.get("risk_off"), dict) else None,
            "market_regime": (p0.get("meta") or {}).get("market_regime") if isinstance(p0.get("meta"), dict) else p0.get("market_regime"),
            "prices_date_max": (p0.get("prices") or {}).get("date_max") if isinstance(p0.get("prices"), dict) else None,
        },
        "gate": gate,
        "survivorship": surv,
        "candidates": {
            "raw_count": len(cand_raw),
            "filtered_count": (len(cand_filt) if cand_filt else None),
            "kept_codes": kept,
            "removed": removed,
        },
        "executions": {
            "today_total": len(todays),
            "buy_count": len(buys),
            "sell_count": len(sells),
            "buys": buys,
            "sells": sells,
        },
    }

    json_out = LOG_DIR / f"after_close_summary_{ts}.json"
    json_last = LOG_DIR / "after_close_summary_last.json"
    # PATCH: attach kill_switch dd_source_link without relying on a fixed variable name
    try:
        _ac_obj = None
        # 1) common variable names first
        for _nm in ("after_close","after","summary","result","out","payload","data"):
            if _nm in locals() and isinstance(locals().get(_nm), dict):
                _ac_obj = locals().get(_nm)
                break
        # 2) last resort: pick the largest dict in locals() (usually the summary object)
        if _ac_obj is None:
            _best = None
            for _k, _v in list(locals().items()):
                if isinstance(_v, dict):
                    if (_best is None) or (len(_v) > len(_best)):
                        _best = _v
            _ac_obj = _best
        if isinstance(_ac_obj, dict):
            _attach_kill_switch_dd_source_link(_ac_obj, LOG_DIR)
    except Exception as _e:
        # never break the writer; record only
        try:
            if isinstance(_ac_obj, dict):
                _ac_obj.setdefault("p0", {}).setdefault("kill_switch", {}).setdefault("metrics", {})
                _ac_obj["p0"]["kill_switch"]["metrics"]["dd_source_link_error"] = f"{type(_e).__name__}: {_e}"
        except Exception:
            pass
    _write_json(json_out, out)
    _write_json(json_last, out)

    # CSV for today's executions + candidate status
    csv_rows = []
    for r in todays:
        code = r.get("code")
        status = "UNKNOWN"
        if code in kept_codes:
            status = "KEPT"
        elif code in removed_codes:
            status = "REMOVED"
        csv_rows.append({**r, "candidate_status": status})
    csv_fields = ["datetime","side","code","qty","price","order_id","note","candidate_status"]
    csv_out = LOG_DIR / f"after_close_summary_{ts}.csv"
    csv_last = LOG_DIR / "after_close_summary_last.csv"
    _write_csv(csv_out, csv_rows, csv_fields)
    _write_csv(csv_last, csv_rows, csv_fields)

    # Console
    print("============================================================")
    print(f"[AFTER_CLOSE] as_of={out['generated_at']} (ymd={ymd})")
    print(f"[P0] market_regime={out['p0']['market_regime']} risk_off={out['p0']['risk_off']} prices_date_max={out['p0']['prices_date_max']}")
    g0 = gate.get("gate0") if isinstance(gate, dict) else None
    g1 = gate.get("gate1") if isinstance(gate, dict) else None
    g2 = gate.get("gate2") if isinstance(gate, dict) else None
    if g0 is not None or g1 is not None or g2 is not None:
        print(f"[GATE] gate0={g0} gate1={g1} gate2={g2}")
    print(f"[CAND] raw={len(cand_raw)} filtered={(len(cand_filt) if cand_filt else 'n/a')} kept={len(kept_codes)} removed={len(removed_codes)}")
    print(f"[EXEC] today_total={len(todays)} buys={len(buys)} sells={len(sells)}")
    print(f"[OUT] {json_out}")
    print(f"[OUT] {csv_out}")
    print("============================================================")

    _print_table("[TODAY BUYS]", ["datetime","code","qty","price","order_id","candidate_status"], [
        [r["datetime"], r["code"], str(r["qty"]), str(r["price"]), r["order_id"],
         ("KEPT" if r["code"] in kept_codes else ("REMOVED" if r["code"] in removed_codes else "UNKNOWN"))]
        for r in buys
    ])
    _print_table("[TODAY SELLS]", ["datetime","code","qty","price","order_id"], [
        [r["datetime"], r["code"], str(r["qty"]), str(r["price"]), r["order_id"]]
        for r in sells
    ])

    rem_rows = []
    for x in removed:
        if not isinstance(x, dict):
            continue
        rem_rows.append([
            str(x.get("date") or ""),
            str(x.get("code") or ""),
            str(x.get("trading_value") or ""),
            str(x.get("day_ret_pct") or ""),
            ";".join(x.get("reasons") or []) if isinstance(x.get("reasons"), list) else str(x.get("reasons") or ""),
        ])
    _print_table("[COMPARE] Removed candidates (filters)", ["date","code","trading_value","day_ret_pct","reasons"], rem_rows)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

