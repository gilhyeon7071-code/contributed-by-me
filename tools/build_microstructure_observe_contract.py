from __future__ import annotations

import csv
import hashlib
import hmac
import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LOB_JSON = LOG_DIR / "surge_lob_latest.json"
LOB_CSV = LOG_DIR / "surge_lob_latest.csv"
SURGE_REALTIME_JSON = LOG_DIR / "surge_realtime_latest.json"
SHADOW_SIM_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
MICRO_SIGNAL_JSON = LOG_DIR / "micro_signal_latest.json"

OUT_PROVISIONAL_JSONL = LOG_DIR / "microstructure_provisional_events_latest.jsonl"
OUT_PROVISIONAL_CSV = LOG_DIR / "microstructure_provisional_events_latest.csv"
OUT_CORROBORATION_JSON = LOG_DIR / "microstructure_corroboration_latest.json"
OUT_SHADOW_EVAL_JSON = LOG_DIR / "microstructure_shadow_eval_latest.json"
OUT_AUTOTUNE_JSON = LOG_DIR / "microstructure_autotune_proposal_latest.json"

PUBLISHER_ID = "roota_microstructure_observe_v1"
OFI_Z_PROMOTE_MIN = float(os.getenv("MICRO_OFI_Z_PROMOTE_MIN", "2.5") or "2.5")
ADV_SPIKE_PCT_POINT_MIN = float(os.getenv("MICRO_ADV_SPIKE_PCT_POINT_MIN", "0.5") or "0.5")
TOB_MOVE_BPS_MIN = float(os.getenv("MICRO_TOB_MOVE_BPS_MIN", "5.0") or "5.0")
CORROBORATION_WINDOW_SEC = int(float(os.getenv("MICRO_CORROBORATION_WINDOW_SEC", "15") or "15"))
PROVISIONAL_TTL_SEC = int(float(os.getenv("MICRO_PROVISIONAL_TTL_SEC", "15") or "15"))
EXTRACTOR_TIMEOUT_MS = int(float(os.getenv("MICRO_EXTRACTOR_TIMEOUT_MS", "120") or "120"))
SHADOW_RETENTION_HOURS = int(float(os.getenv("MICRO_SHADOW_RETENTION_HOURS", "48") or "48"))


def _now() -> datetime:
    return datetime.now()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"_read_error": f"{type(exc).__name__}: {exc}"}


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            return [dict(row) for row in csv.DictReader(fp)]
    except Exception:
        return []


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        x = float(str(value).replace(",", "").strip())
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _canonical_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hmac_key() -> tuple[bytes, str]:
    raw = os.getenv("MICRO_AUDIT_HMAC_KEY", "")
    if raw.strip():
        return raw.encode("utf-8"), "env:MICRO_AUDIT_HMAC_KEY"
    # Observe-only fallback keeps every emit signed while forcing non-trading status.
    return b"roota_microstructure_observe_only_default_key", "default_observe_only_key"


def _hmac_sha256(text: str) -> tuple[str, str]:
    key, source = _hmac_key()
    return hmac.new(key, text.encode("utf-8"), hashlib.sha256).hexdigest(), source


def _event_id(code: str, semantic_hash: str, normalized_ts: str) -> str:
    raw = f"{semantic_hash}|{PUBLISHER_ID}|{normalized_ts}"
    return _sha256_text(raw)[:32]


def _by_code(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = "".join(ch for ch in str(row.get("code") or "") if ch.isdigit()).zfill(6)
        if len(code) == 6:
            out[code] = row
    return out


def _extractor_rows(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        rows = payload.get("alerts")
    if not isinstance(rows, list):
        return {}
    return _by_code([row for row in rows if isinstance(row, dict)])


def _shadow_rows(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {}
    return _by_code([row for row in rows if isinstance(row, dict)])


def _micro_signal_map(payload: Dict[str, Any]) -> Dict[str, Any]:
    maps = payload.get("maps")
    if isinstance(maps, dict):
        return maps
    return {}


def _confirmation_flags(
    lob_row: Dict[str, Any],
    extractor_row: Dict[str, Any],
    micro_maps: Dict[str, Any],
) -> Dict[str, Any]:
    code = str(lob_row.get("code") or "").zfill(6)
    ofi_z = _to_float(lob_row.get("ofi_z"))
    markout_bps = abs(_to_float(lob_row.get("markout_1step_bps")))
    trading_value = _to_float(extractor_row.get("trading_value"))
    adv = _to_float(extractor_row.get("adv20_trading_value"), 0.0)
    if adv <= 0:
        adv = _to_float(extractor_row.get("avg_trading_value_20d"), 0.0)
    adv_spike_pct_point = (trading_value / adv * 100.0) if trading_value > 0 and adv > 0 else 0.0
    signal_map = micro_maps.get("signal") if isinstance(micro_maps.get("signal"), dict) else {}
    source_map = micro_maps.get("source") if isinstance(micro_maps.get("source"), dict) else {}
    micro_signal = str(signal_map.get(code, "") or "")
    micro_source = str(source_map.get(code, "") or "")
    secondary_confirmed = bool(micro_signal and micro_signal.upper() not in {"NEUTRAL", "NONE", "0"})
    checks = {
        "ofi_z_confirmed": bool(ofi_z >= OFI_Z_PROMOTE_MIN),
        "adv_spike_confirmed": bool(adv_spike_pct_point >= ADV_SPIKE_PCT_POINT_MIN),
        "tob_move_confirmed": bool(markout_bps >= TOB_MOVE_BPS_MIN),
        "secondary_publisher_confirmed": secondary_confirmed,
    }
    return {
        **checks,
        "ofi_z": round(ofi_z, 6),
        "adv_spike_pct_point": round(adv_spike_pct_point, 6),
        "tob_move_bps": round(markout_bps, 6),
        "micro_signal": micro_signal,
        "micro_source": micro_source,
        "confirmed": any(checks.values()),
    }


def _build_events(now: datetime) -> List[Dict[str, Any]]:
    lob_payload = _read_json(LOB_JSON)
    realtime_payload = _read_json(SURGE_REALTIME_JSON)
    shadow_payload = _read_json(SHADOW_SIM_JSON)
    micro_payload = _read_json(MICRO_SIGNAL_JSON)
    lob_rows = _read_csv(LOB_CSV)
    extractor_by_code = _extractor_rows(realtime_payload)
    shadow_by_code = _shadow_rows(shadow_payload)
    micro_maps = _micro_signal_map(micro_payload)
    generated_at = _iso(now)
    expires_at = _iso(now + timedelta(seconds=PROVISIONAL_TTL_SEC))
    events: List[Dict[str, Any]] = []
    for row in lob_rows:
        code = "".join(ch for ch in str(row.get("code") or "") if ch.isdigit()).zfill(6)
        if len(code) != 6:
            continue
        extractor = extractor_by_code.get(code, {})
        shadow = shadow_by_code.get(code, {})
        normalized_ts = str(row.get("ts") or lob_payload.get("ts") or generated_at)
        semantic_basis = {
            "code": code,
            "lob_status": row.get("lob_status", ""),
            "orderflow_tag": row.get("orderflow_tag", ""),
            "surge_type": extractor.get("detected_surge_type", extractor.get("surge_type", "")),
        }
        semantic_hash = _sha256_text(_canonical_json(semantic_basis))[:24]
        confirmations = _confirmation_flags(row, extractor, micro_maps)
        status = "PROMOTE_CANDIDATE_RESEARCH_ONLY" if confirmations["confirmed"] else "PROVISIONAL_UNCONFIRMED"
        if not confirmations["confirmed"]:
            status = "EXPIRE_AFTER_TTL_IF_UNCONFIRMED"
        evidence = {
            "tob": {
                "ask1": _to_float(row.get("ask1")),
                "bid1": _to_float(row.get("bid1")),
                "askq1": _to_float(row.get("askq1")),
                "bidq1": _to_float(row.get("bidq1")),
                "spread_bps": _to_float(row.get("spread_bps")),
                "markout_1step_bps": _to_float(row.get("markout_1step_bps")),
            },
            "tick_stream": {
                "path": str((lob_payload.get("ws_hoga_load") or {}).get("path") or ""),
                "mode": str((lob_payload.get("ws_hoga_load") or {}).get("mode") or ""),
                "line_count": int(_to_float((lob_payload.get("ws_hoga_load") or {}).get("line_count"))),
            },
            "extractor_output": {
                "entry_decision": extractor.get("entry_decision", ""),
                "entry_blocked": extractor.get("entry_blocked", ""),
                "blocked_reasons": extractor.get("blocked_reasons", extractor.get("entry_reason", "")),
                "surge_score_final": extractor.get("surge_score_final", ""),
                "rvol20": extractor.get("rvol20", ""),
                "change_pct": extractor.get("change_pct", ""),
            },
            "shadow_output": {
                "status": shadow.get("status", ""),
                "primary_ret_pct": shadow.get("primary_ret_pct", ""),
                "exit_reason": shadow.get("exit_reason", ""),
            },
            "confirmations": confirmations,
        }
        manifest = {
            "schema_version": "microstructure_observe_event_v1",
            "generated_at": generated_at,
            "publisher_id": PUBLISHER_ID,
            "event_id_contract": "semantic_hash+publisher_id+normalized_ts",
            "extractor_timeout_ms": EXTRACTOR_TIMEOUT_MS,
            "corroboration_window_sec": CORROBORATION_WINDOW_SEC,
            "provisional_ttl_sec": PROVISIONAL_TTL_SEC,
            "shadow_retention_hours": SHADOW_RETENTION_HOURS,
            "risk_contract": {
                "policy_change": False,
                "entry_approval_changed": False,
                "live_order_allowed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "research_only": True,
                "must_not_dispatch": True,
                "purpose": "observe-only microstructure evidence bundle; no order route",
            },
        }
        event = {
            "event_id": _event_id(code, semantic_hash, normalized_ts),
            "semantic_hash": semantic_hash,
            "publisher_id": PUBLISHER_ID,
            "normalized_ts": normalized_ts,
            "code": code,
            "status": status,
            "expires_at": expires_at,
            "manifest": manifest,
            "evidence": evidence,
        }
        canonical = _canonical_json({k: v for k, v in event.items() if k not in {"sha256", "hmac_sha256", "hmac_key_source"}})
        event["sha256"] = _sha256_text(canonical)
        event["hmac_sha256"], event["hmac_key_source"] = _hmac_sha256(canonical)
        events.append(event)
    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        deduped[str(event["event_id"])] = event
    return list(deduped.values())


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _append_jsonl_unique(path: Path, rows: List[Dict[str, Any]]) -> int:
    existing_ids: set[str] = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as fp:
            for line in fp:
                try:
                    event_id = str(json.loads(line).get("event_id") or "")
                except Exception:
                    continue
                if event_id:
                    existing_ids.add(event_id)
    new_rows = [row for row in rows if str(row.get("event_id") or "") not in existing_ids]
    with path.open("a", encoding="utf-8", newline="\n") as fp:
        for row in new_rows:
            fp.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return len(new_rows)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "event_id",
        "code",
        "status",
        "normalized_ts",
        "expires_at",
        "ofi_z",
        "adv_spike_pct_point",
        "tob_move_bps",
        "confirmed",
        "sha256",
        "hmac_key_source",
        "policy_change",
        "entry_approval_changed",
        "live_order_allowed",
        "research_only",
        "must_not_dispatch",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            confirmations = ((row.get("evidence") or {}).get("confirmations") or {})
            risk = ((row.get("manifest") or {}).get("risk_contract") or {})
            writer.writerow({
                "event_id": row.get("event_id", ""),
                "code": row.get("code", ""),
                "status": row.get("status", ""),
                "normalized_ts": row.get("normalized_ts", ""),
                "expires_at": row.get("expires_at", ""),
                "ofi_z": confirmations.get("ofi_z", ""),
                "adv_spike_pct_point": confirmations.get("adv_spike_pct_point", ""),
                "tob_move_bps": confirmations.get("tob_move_bps", ""),
                "confirmed": confirmations.get("confirmed", False),
                "sha256": row.get("sha256", ""),
                "hmac_key_source": row.get("hmac_key_source", ""),
                "policy_change": risk.get("policy_change", False),
                "entry_approval_changed": risk.get("entry_approval_changed", False),
                "live_order_allowed": risk.get("live_order_allowed", False),
                "research_only": risk.get("research_only", True),
                "must_not_dispatch": risk.get("must_not_dispatch", True),
            })


def _shadow_eval(now: datetime, bundle_path: Path) -> Dict[str, Any]:
    cutoff = now - timedelta(hours=SHADOW_RETENTION_HOURS)
    matured: List[Dict[str, Any]] = []
    if bundle_path.exists():
        with bundle_path.open("r", encoding="utf-8") as fp:
            for line in fp:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                ts = str(row.get("manifest", {}).get("generated_at") or "")
                try:
                    generated_at = datetime.fromisoformat(ts)
                except Exception:
                    continue
                if generated_at <= cutoff:
                    matured.append(row)
    returns = [
        _to_float(((row.get("evidence") or {}).get("shadow_output") or {}).get("primary_ret_pct"))
        for row in matured
        if str(((row.get("evidence") or {}).get("shadow_output") or {}).get("status") or "") == "EVALUATED"
    ]
    if not returns:
        return {
            "status": "NO_MATURED_EVALUATED_BUNDLES",
            "matured_events": len(matured),
            "evaluated_returns": 0,
            "enbpi_status": "NOT_RUN_INSUFFICIENT_48H_SAMPLE",
            "median_delta_l95": None,
            "ess": 0,
        }
    returns = sorted(returns)
    idx = max(0, int(math.floor((len(returns) - 1) * 0.05)))
    l95 = returns[idx]
    return {
        "status": "EVALUATED_PROXY_BLOCK_BOOTSTRAP_PENDING_ENBPI",
        "matured_events": len(matured),
        "evaluated_returns": len(returns),
        "enbpi_status": "PENDING_DEDICATED_ENBPI_MODEL",
        "median_delta_l95": round(l95, 6),
        "ess": len(returns),
    }


def main() -> int:
    now = _now()
    events = _build_events(now)
    bundle_path = LOG_DIR / f"shadow_micro_bundle_{now.strftime('%Y%m%d')}.jsonl"
    _write_jsonl(OUT_PROVISIONAL_JSONL, events)
    _write_csv(OUT_PROVISIONAL_CSV, events)
    appended_bundle_events = _append_jsonl_unique(bundle_path, events)
    promoted = [row for row in events if row.get("status") == "PROMOTE_CANDIDATE_RESEARCH_ONLY"]
    expired = [row for row in events if row.get("status") == "EXPIRE_AFTER_TTL_IF_UNCONFIRMED"]
    confirmation_counts: Dict[str, int] = {
        "ofi_z_confirmed": 0,
        "adv_spike_confirmed": 0,
        "tob_move_confirmed": 0,
        "secondary_publisher_confirmed": 0,
    }
    for row in events:
        flags = ((row.get("evidence") or {}).get("confirmations") or {})
        for key in confirmation_counts:
            confirmation_counts[key] += int(bool(flags.get(key)))
    shadow_eval = _shadow_eval(now, bundle_path)
    common_risk = {
        "policy_change": False,
        "entry_approval_changed": False,
        "live_order_allowed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "research_only": True,
        "must_not_dispatch": True,
    }
    corroboration = {
        "ts": _iso(now),
        "status": "OK",
        "scope": "microstructure_corroboration_observe_only",
        "source_provisional_jsonl": str(OUT_PROVISIONAL_JSONL),
        "shadow_bundle_append_only": str(bundle_path),
        "summary": {
            "provisional_events": len(events),
            "shadow_bundle_appended_events": appended_bundle_events,
            "promote_candidate_research_only": len(promoted),
            "expire_after_ttl_if_unconfirmed": len(expired),
            "promotion_rate": round(len(promoted) / max(len(events), 1), 6),
            "expire_rate": round(len(expired) / max(len(events), 1), 6),
            **confirmation_counts,
        },
        "params": {
            "ofi_z_min": OFI_Z_PROMOTE_MIN,
            "adv_spike_pct_point_min": ADV_SPIKE_PCT_POINT_MIN,
            "tob_move_bps_min": TOB_MOVE_BPS_MIN,
            "corroboration_window_sec": CORROBORATION_WINDOW_SEC,
            "provisional_ttl_sec": PROVISIONAL_TTL_SEC,
            "extractor_timeout_ms": EXTRACTOR_TIMEOUT_MS,
            "shadow_retention_hours": SHADOW_RETENTION_HOURS,
        },
        "risk_contract": common_risk,
    }
    OUT_CORROBORATION_JSON.write_text(json.dumps(corroboration, ensure_ascii=False, indent=2), encoding="utf-8")
    shadow_payload = {
        "ts": _iso(now),
        "status": shadow_eval["status"],
        "scope": "microstructure_shadow_48h_eval",
        "source_shadow_bundle": str(bundle_path),
        "shadow_eval": shadow_eval,
        "risk_contract": common_risk,
    }
    OUT_SHADOW_EVAL_JSON.write_text(json.dumps(shadow_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    autotune = {
        "ts": _iso(now),
        "status": "PROPOSAL_ONLY",
        "scope": "microstructure_autotune_proposal",
        "source_shadow_eval_json": str(OUT_SHADOW_EVAL_JSON),
        "policy_change_applied": False,
        "entry_approval_changed": False,
        "thresholds_current": {
            "ofi_z_min": OFI_Z_PROMOTE_MIN,
            "adv_spike_pct_point_min": ADV_SPIKE_PCT_POINT_MIN,
            "tob_move_bps_min": TOB_MOVE_BPS_MIN,
        },
        "thresholds_proposed": {
            "ofi_z_min": OFI_Z_PROMOTE_MIN,
            "adv_spike_pct_point_min": ADV_SPIKE_PCT_POINT_MIN,
            "tob_move_bps_min": TOB_MOVE_BPS_MIN,
        },
        "proposal_reason": "no automatic threshold change; observe-only until 48h evaluated sample is sufficient",
        "risk_contract": common_risk,
    }
    OUT_AUTOTUNE_JSON.write_text(json.dumps(autotune, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "status": "OK",
        "provisional_events": len(events),
        "promote_candidate_research_only": len(promoted),
        "expire_after_ttl_if_unconfirmed": len(expired),
        "out_json": str(OUT_CORROBORATION_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
