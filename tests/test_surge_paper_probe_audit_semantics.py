from pathlib import Path
import importlib.util
import json
import sys


def _load_module():
    mod_path = Path(r"E:\1_Data\paper_engine.py")
    spec = importlib.util.spec_from_file_location("paper_engine", mod_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_surge_paper_probe_preserves_source_entry_decision(monkeypatch, tmp_path):
    mod = _load_module()

    surge_json = tmp_path / "surge_realtime_latest.json"
    active_csv = tmp_path / "surge_active_response_layer_latest.csv"
    readiness_csv = tmp_path / "surge_live_readiness_audit_latest.csv"

    monkeypatch.setattr(mod, "SURGE_REALTIME_STATUS_PATH", surge_json)
    monkeypatch.setattr(mod, "SURGE_ACTIVE_RESPONSE_LAYER_CSV_PATH", active_csv)
    monkeypatch.setattr(mod, "SURGE_LIVE_READINESS_AUDIT_CSV_PATH", readiness_csv)

    alert = {
        "date": "20260617",
        "code": "122640",
        "surge_score": 99.88,
        "surge_score_final": 82.48492599999999,
        "surge_type": "PRICE_VOL_BREAKOUT",
        "entry_allowed": False,
        "entry_blocked": True,
        "entry_decision": "ENTRY_BLOCKED",
        "entry_reason": "HIGH_REJECTION_ENTRY_BLOCK",
        "active_response_label": "PROBE_READY",
        "active_response_reason": "paper probe sample",
        "rvol20": 2.896485,
        "ret1_pct": 11.4187,
        "day_range_pct": 0.189441,
        "current_price": 32200,
        "prev_close": 28900,
        "spread_bps": 15.54001554001554,
        "markout_1step_bps": 0.0,
        "orderflow_risk_score": 0.009797,
        "orderflow_tag": "OK",
        "lob_status": "OK",
        "ask_depth_levels": 10,
        "exclude_reasons": "HIGH_REJECTION_ENTRY_BLOCK:-0.0694<=-0.0250",
        "paper_probe_allowed": True,
        "paper_probe_block_reasons": "HIGH_REJECTION_ENTRY_BLOCK",
    }
    surge_json.write_text(
        json.dumps(
            {
                "ts": "2026-06-17T13:13:22",
                "intraday_date": "20260617",
                "alerts": [alert],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    active_csv.write_text(
        "date,code,active_response_label,active_response_reason\n"
        "20260617,122640,PROBE_READY,paper probe sample\n",
        encoding="utf-8",
    )
    readiness_csv.write_text(
        "source_ts,code,paper_order_readiness,paper_order_route,live_trade_readiness\n"
        "2026-06-17T13:13:22,122640,PAPER_READY,True,BLOCKED\n",
        encoding="utf-8",
    )

    cfg = {
        "surge_entry_policy": {
            "enabled": True,
            "realtime_only": True,
            "realtime_alert_max_age_minutes": 0,
            "top_n": 1,
            "min_score_final": 75.0,
            "per_symbol_alloc_pct": 0.01,
            "total_alloc_pct": 0.01,
            "max_same_code_per_day": 1,
            "type_policy": {"enabled": False},
            "dynamic_max_new": {"enabled": False},
            "no_lob_probe": {"max_selected": 0},
        }
    }
    empty_candidates = mod.pd.DataFrame(columns=["code", "name", "signal_date"])

    out, status = mod._inject_surge_immediate_candidates(
        empty_candidates,
        cfg=cfg,
        intraday_realtime_mode=True,
        open_codes=set(),
        today_ymd="20260617",
        today_buy_code_counts={},
    )

    assert status["applied"] is True
    row = out.iloc[0].to_dict()
    assert row["code"] == "122640"
    assert row["source_entry_decision"] == "ENTRY_BLOCKED"
    assert row["source_entry_reason"] == "HIGH_REJECTION_ENTRY_BLOCK"
    assert str(row["source_entry_allowed"]) == "False"
    assert str(row["source_entry_blocked"]) == "True"
    assert row["exclude_reasons"] == "HIGH_REJECTION_ENTRY_BLOCK:-0.0694<=-0.0250"
    assert row["surge_paper_probe_allowed"] is True
    assert row["surge_paper_probe_block_reasons"] == "HIGH_REJECTION_ENTRY_BLOCK"
