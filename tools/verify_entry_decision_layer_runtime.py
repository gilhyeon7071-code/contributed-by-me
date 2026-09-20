from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
ROOTB = ROOT.parent / "vibe" / "buffett"
LOG_DIR = ROOT / "2_Logs"
REPORT_PATH = LOG_DIR / "entry_decision_layer_runtime_verify_latest.json"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_sig(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    st = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size": int(st.st_size),
        "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
        "sha256": _sha256(path),
    }


def _norm_ymd(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))[:8]


def _derive_d_from_fills(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"status": "FAIL", "reason": "fills_missing", "path": str(path)}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        return {"status": "FAIL", "reason": f"fills_read_failed:{type(exc).__name__}", "path": str(path)}
    if not rows:
        return {"status": "FAIL", "reason": "fills_empty", "path": str(path)}

    def row_ymd(row: Dict[str, Any]) -> str:
        for key in ("datetime", "ts", "date"):
            ymd = _norm_ymd(row.get(key, ""))
            if len(ymd) == 8:
                return ymd
        return ""

    buys = [row_ymd(r) for r in rows if str(r.get("side", "")).strip().upper() == "BUY"]
    buys = [x for x in buys if len(x) == 8]
    all_dates = [row_ymd(r) for r in rows]
    all_dates = [x for x in all_dates if len(x) == 8]
    d_rule = max(buys) if buys else (max(all_dates) if all_dates else "")
    return {
        "status": "PASS" if d_rule else "FAIL",
        "path": str(path),
        "rows": int(len(rows)),
        "buy_ymd_count": int(len(buys)),
        "d_rule": d_rule,
    }


def _compare_backup(backup_dir: Optional[Path], rel_path: str, live_path: Path) -> Dict[str, Any]:
    live = _file_sig(live_path)
    if backup_dir is None:
        return {"rel_path": rel_path, "live": live, "backup_checked": False}
    backup_name = re.sub(r'[\\/:*?"<>|]', "_", rel_path) + ".bak"
    backup_path = backup_dir / backup_name
    backup = _file_sig(backup_path)
    changed = bool(live.get("sha256") and backup.get("sha256") and live.get("sha256") != backup.get("sha256"))
    if live.get("exists") != backup.get("exists"):
        changed = True
    return {
        "rel_path": rel_path,
        "live": live,
        "backup": backup,
        "backup_checked": True,
        "changed": bool(changed),
    }


def _artifact_checks(layer_json: Dict[str, Any], d_rule: str, require_d_match: bool) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    checks.append({
        "name": "artifact_status_pass",
        "status": "PASS" if str(layer_json.get("status", "")).upper() == "PASS" else "FAIL",
        "value": layer_json.get("status", ""),
    })
    checks.append({
        "name": "policy_change_false",
        "status": "PASS" if layer_json.get("policy_change") is False else "FAIL",
        "value": layer_json.get("policy_change"),
    })
    candidate_rows = int(layer_json.get("candidate_rows") or 0)
    snapshot_rows = int(layer_json.get("snapshot_rows") or 0)
    checks.append({
        "name": "snapshot_rows_consistent",
        "status": "PASS" if snapshot_rows > 0 or candidate_rows == 0 else "FAIL",
        "value": {"candidate_rows": candidate_rows, "snapshot_rows": snapshot_rows},
    })
    d_ref = str(layer_json.get("d_ref", "") or "")
    if require_d_match:
        d_status = "PASS" if d_rule and d_ref == d_rule else "FAIL"
    else:
        d_status = "PASS" if len(d_ref) == 8 else "NA"
    checks.append({
        "name": "d_ref_matches_fills_d_rule",
        "status": d_status,
        "value": {"d_ref": d_ref, "fills_d_rule": d_rule},
    })
    return checks


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify entry decision layer runtime artifacts and chain impact.")
    ap.add_argument("--backup-dir", default="", help="Backup directory created before the runtime/batch run.")
    ap.add_argument("--require-d-match", action="store_true", help="Fail if entry layer d_ref differs from fills D rule.")
    ap.add_argument("--fail-on-chain-change", action="store_true", help="Fail when fills/trades changed versus backup.")
    args = ap.parse_args()

    backup_dir = Path(args.backup_dir) if str(args.backup_dir or "").strip() else None
    layer_json_path = LOG_DIR / "entry_decision_layers_runtime_latest.json"
    layer_csv_path = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
    layer_json = _read_json(layer_json_path)
    fills_d = _derive_d_from_fills(ROOT / "paper" / "fills.csv")
    d_rule = str(fills_d.get("d_rule", "") or "")

    comparisons = [
        _compare_backup(backup_dir, "paper\\fills.csv", ROOT / "paper" / "fills.csv"),
        _compare_backup(backup_dir, "paper\\trades.csv", ROOT / "paper" / "trades.csv"),
        _compare_backup(backup_dir, "paper\\paper_state.json", ROOT / "paper" / "paper_state.json"),
        _compare_backup(backup_dir, "2_Logs\\entry_decision_layers_runtime_latest.json", layer_json_path),
        _compare_backup(backup_dir, "2_Logs\\entry_decision_layers_runtime_latest.csv", layer_csv_path),
    ]
    rootb_signatures = [
        _file_sig(ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"),
        _file_sig(ROOTB / "data" / "orders" / "replay_orders_latest.json"),
        _file_sig(ROOTB / "data" / "live" / "live_fills.csv"),
    ]
    checks = _artifact_checks(layer_json, d_rule, bool(args.require_d_match))

    fills_cmp = next((x for x in comparisons if x.get("rel_path") == "paper\\fills.csv"), {})
    trades_cmp = next((x for x in comparisons if x.get("rel_path") == "paper\\trades.csv"), {})
    if fills_cmp.get("backup_checked"):
        checks.append({
            "name": "fills_unchanged_vs_backup",
            "status": "PASS" if not fills_cmp.get("changed") else ("FAIL" if args.fail_on_chain_change else "WARN"),
            "value": fills_cmp.get("changed"),
        })
    if trades_cmp.get("backup_checked"):
        checks.append({
            "name": "trades_unchanged_vs_backup",
            "status": "PASS" if not trades_cmp.get("changed") else ("FAIL" if args.fail_on_chain_change else "WARN"),
            "value": trades_cmp.get("changed"),
        })

    failed = [c for c in checks if c.get("status") == "FAIL"]
    warned = [c for c in checks if c.get("status") == "WARN"]
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if not failed else "FAIL",
        "backup_dir": str(backup_dir) if backup_dir else "",
        "entry_decision_layer_json": str(layer_json_path),
        "entry_decision_layer_csv": str(layer_csv_path),
        "fills_d_rule": fills_d,
        "artifact_summary": layer_json,
        "checks": checks,
        "comparisons": comparisons,
        "rootb_read_only_signatures": rootb_signatures,
        "failed_checks": failed,
        "warning_checks": warned,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ENTRY_LAYER_VERIFY] status={report['status']} report={REPORT_PATH}")
    for check in checks:
        print(f"[CHECK] {check['name']}={check['status']} value={check.get('value')}")
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
