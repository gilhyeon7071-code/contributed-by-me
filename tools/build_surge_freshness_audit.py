from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER = ROOT / "paper"
OUT_JSON = LOG_DIR / "surge_freshness_audit_latest.json"


FILES = {
    "surge_params": PAPER / "surge_params.json",
    "surge_universe": PAPER / "surge_universe.csv",
    "surge_param_proposal": LOG_DIR / "surge_param_proposal_latest.json",
    "surge_param_apply": LOG_DIR / "surge_param_apply_latest.json",
    "surge_ml_score": LOG_DIR / "surge_ml_score_latest.json",
    "surge_realtime": LOG_DIR / "surge_realtime_latest.json",
    "surge_lob": LOG_DIR / "surge_lob_latest.json",
    "surge_ev_readiness": LOG_DIR / "surge_ev_probe_readiness_latest.json",
    "surge_ev_staged": LOG_DIR / "surge_ev_paper_probe_staged_latest.json",
}


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"_read_error": f"{type(exc).__name__}:{exc}"}


def _meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "mtime": "", "age_hours": None, "size": 0}
    stat = path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime)
    age_hours = (datetime.now() - mtime).total_seconds() / 3600.0
    return {"path": str(path), "exists": True, "mtime": mtime.isoformat(timespec="seconds"), "age_hours": round(age_hours, 3), "size": int(stat.st_size)}


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    files = {name: _meta(path) for name, path in FILES.items()}
    params = _read_json(FILES["surge_params"])
    warnings = []
    if not files["surge_universe"]["exists"]:
        warnings.append("surge_universe_missing")
    if not files["surge_params"]["exists"]:
        warnings.append("surge_params_missing")
    if not files["surge_param_proposal"]["exists"]:
        warnings.append("surge_param_proposal_missing")
    if files["surge_ml_score"]["age_hours"] is not None and files["surge_ml_score"]["age_hours"] > 24:
        warnings.append("surge_ml_score_older_than_24h")
    if files["surge_realtime"]["age_hours"] is not None and files["surge_realtime"]["age_hours"] > 24:
        warnings.append("surge_realtime_older_than_24h")
    model_files = [
        p for p in (ROOT / "models").glob("*surge*") if p.is_file()
    ] if (ROOT / "models").exists() else []
    payload = {
        "ts": now,
        "status": "PASS" if not warnings else "WARN",
        "scope": "surge_freshness_audit",
        "warnings": warnings,
        "params": {
            "last_validated": params.get("_last_validated", ""),
            "param_status": params.get("_param_status", ""),
            "source": params.get("_source", ""),
        },
        "model_files": [
            {"path": str(p), "mtime": datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds"), "size": int(p.stat().st_size)}
            for p in sorted(model_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]
        ],
        "files": files,
        "risk_contract": {
            "audit_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "warnings": warnings, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
